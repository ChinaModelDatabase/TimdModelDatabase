pub mod auth;
pub mod pg;
pub mod mysql;

use std::sync::Arc;
use tmdb_exec::Engine;

/// Start TCP server that accepts both PG and MySQL connections.
///
/// Authentication is mandatory. Credentials are loaded before either listener
/// is bound, so a missing or unreadable configuration stops startup instead of
/// leaving a port open that accepts anything.
pub async fn start_server(engine: Arc<Engine>, pg_addr: &str, mysql_addr: &str) -> anyhow::Result<()> {
    let users = auth::load(&auth::Config::from_env()).map_err(|e| {
        anyhow::anyhow!("refusing to start without authentication: {}", e)
    })?;
    tracing::info!(
        "loaded {} user(s): {}",
        users.len(),
        users.names().join(", ")
    );

    // Start PG listener
    let pg_engine = engine.clone();
    let pg_users = users.clone();
    let pg_listener = tokio::net::TcpListener::bind(pg_addr).await?;
    tracing::info!("PG wire listening on {}", pg_addr);

    tokio::spawn(async move {
        loop {
            if let Ok((stream, addr)) = pg_listener.accept().await {
                tracing::info!("PG connection from {}", addr);
                let engine = pg_engine.clone();
                let users = pg_users.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_pg_connection(stream, engine, users).await {
                        tracing::error!("PG connection error: {}", e);
                    }
                });
            }
        }
    });

    // Start MySQL listener
    let mysql_engine = engine.clone();
    let mysql_listener = tokio::net::TcpListener::bind(mysql_addr).await?;
    tracing::info!("MySQL wire listening on {}", mysql_addr);

    tokio::spawn(async move {
        loop {
            if let Ok((stream, addr)) = mysql_listener.accept().await {
                tracing::info!("MySQL connection from {}", addr);
                let engine = mysql_engine.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_mysql_connection(stream, engine).await {
                        tracing::error!("MySQL connection error: {}", e);
                    }
                });
            }
        }
    });

    Ok(())
}

/// Protocol version 3.0, as it appears in a startup packet.
const PG_PROTOCOL_VERSION_3: i32 = 196608;
/// `80877103`: the client asking whether the server speaks TLS.
const PG_SSL_REQUEST: i32 = 80877103;
/// `80877104`: an out-of-band cancel; there is no query to attach it to here.
const PG_CANCEL_REQUEST: i32 = 80877104;

/// Handle a single PostgreSQL connection
async fn handle_pg_connection(
    mut stream: tokio::net::TcpStream,
    engine: Arc<Engine>,
    users: Arc<auth::Users>,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut conn = pg::PgConnection::with_verifier(users);
    // TCP does not preserve message boundaries, so a partial read has to be
    // carried over to the next one. Reading straight into a fixed buffer and
    // assuming one message per read desynchronises the stream as soon as a
    // message spans a segment boundary.
    let mut pending: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; 8192];

    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        pending.extend_from_slice(&chunk[..n]);

        // Drain every complete message currently buffered.
        loop {
            if pending.len() < 4 {
                break;
            }
            let len = i32::from_be_bytes([pending[0], pending[1], pending[2], pending[3]]);
            if len < 4 {
                // Nonsense length; drop the connection rather than spin.
                anyhow::bail!("bad PG message length {}", len);
            }
            let total = 4 + len as usize;
            if pending.len() < total {
                break;
            }
            let msg: Vec<u8> = pending.drain(..total).collect();
            let body = &msg[4..];

            // The first message on a connection is untyped: its body starts
            // with a request code rather than a message type byte.
            if body.len() >= 4 {
                let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                match code {
                    PG_SSL_REQUEST => {
                        // Refuse TLS; the client then continues in cleartext.
                        stream.write_all(b"N").await?;
                        continue;
                    }
                    PG_CANCEL_REQUEST => continue,
                    PG_PROTOCOL_VERSION_3 => {
                        let resp = conn.handle_startup(body);
                        stream.write_all(&resp).await?;
                        continue;
                    }
                    _ => {}
                }
            }

            let msg_type = body[0];
            match msg_type {
                b'Q' => {
                    // Simple query: body is the length then a NUL-terminated
                    // string.
                    let sql = String::from_utf8_lossy(&body[5..]).trim_end_matches('\0').to_string();
                    tracing::info!("PG query: {}", sql);
                    let resp = conn.execute_query(&sql, &engine);
                    stream.write_all(&resp).await?;
                }
                b'X' => return Ok(()),
                b'p' => {
                    // SASLInitialResponse and SASLResponse share the type byte.
                    let resp = conn.handle_sasl(body);
                    stream.write_all(&resp).await?;
                }
                other => {
                    // Everything else in the extended protocol funnels through
                    // the connection's own dispatcher, which knows the message
                    // grammar and answers unknown types with a protocol error.
                    if matches!(other, b'P' | b'B' | b'D' | b'E' | b'C' | b'S') {
                        let resp = conn.handle_extended_query(other, body, &engine);
                        stream.write_all(&resp).await?;
                    } else {
                        tracing::warn!("unsupported PG message type {:?}", other as char);
                        let resp = conn.build_error_response("unsupported message type");
                        stream.write_all(&resp).await?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Handle a single MySQL connection
async fn handle_mysql_connection(
    mut stream: tokio::net::TcpStream,
    engine: Arc<Engine>,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut conn = mysql::MysqlConnection::new();

    // Send handshake
    let handshake = conn.build_handshake();
    stream.write_all(&handshake).await?;

    let mut buf = vec![0u8; 4096];

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        if !conn.is_authenticated() {
            let resp = conn.handle_auth(&buf[..n]);
            stream.write_all(&resp).await?;
            continue;
        }

        // Parse COM_QUERY
        if n > 4 {
            let _len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            let _seq = buf[4];
            let cmd = buf[5];

            match cmd {
                0x03 => {
                    // COM_QUERY
                    let sql = String::from_utf8_lossy(&buf[6..n]).to_string();
                    tracing::info!("MySQL query: {}", sql);
                    let resp = conn.execute_query(&sql, &engine);
                    stream.write_all(&resp).await?;
                }
                0x01 => {
                    // COM_QUIT
                    break;
                }
                0x0E => {
                    // COM_PING
                    conn.begin_response();
                    let resp = conn.build_ok();
                    stream.write_all(&resp).await?;
                }
                _ => {
                    conn.begin_response();
                    let resp = conn.build_error(1047, "Unknown command");
                    stream.write_all(&resp).await?;
                }
            }
        }
    }

    Ok(())
}
