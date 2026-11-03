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
    let mut startup_done = false;

    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        pending.extend_from_slice(&chunk[..n]);

        // Drain every complete message currently buffered.
        loop {
            // Only the startup packet is untyped. Every message after it opens
            // with a type byte and carries its length behind it, so reading the
            // length from offset zero asks for four bytes no client sends.
            if !startup_done {
                if pending.len() < 4 {
                    break;
                }
                let len = i32::from_be_bytes([pending[0], pending[1], pending[2], pending[3]]);
                if len < 4 {
                    // Nonsense length; drop the connection rather than spin.
                    anyhow::bail!("bad PG message length {}", len);
                }
                // A PostgreSQL length counts itself: the four bytes of the
                // length plus everything after them.
                let total = len as usize;
                if pending.len() < total {
                    break;
                }
                let msg: Vec<u8> = pending.drain(..total).collect();
                let code = if msg.len() >= 8 {
                    i32::from_be_bytes([msg[4], msg[5], msg[6], msg[7]])
                } else {
                    0
                };

                match code {
                    PG_SSL_REQUEST => {
                        // Refuse TLS; the client then continues in cleartext,
                        // and the packet that follows is untyped too.
                        stream.write_all(b"N").await?;
                        continue;
                    }
                    PG_CANCEL_REQUEST => continue,
                    PG_PROTOCOL_VERSION_3 => {
                        startup_done = true;
                        // The startup handler reads its own length field, so it
                        // takes the whole message.
                        let resp = conn.handle_startup(&msg);
                        stream.write_all(&resp).await?;
                        continue;
                    }
                    other => anyhow::bail!("unknown startup request code {}", other),
                }
            }

            if pending.len() < 5 {
                break;
            }
            let msg_type = pending[0];
            let len = i32::from_be_bytes([pending[1], pending[2], pending[3], pending[4]]);
            if len < 4 {
                anyhow::bail!("bad PG message length {}", len);
            }
            let total = 1 + len as usize;
            if pending.len() < total {
                break;
            }
            let msg: Vec<u8> = pending.drain(..total).collect();
            // The handlers read their own length field, so they take the message
            // with only the type byte removed; the payload starts behind it.
            let frame = &msg[1..];
            let payload = &msg[5..];

            match msg_type {
                b'Q' => {
                    // Simple query: the payload is the SQL, NUL-terminated.
                    let sql = String::from_utf8_lossy(payload).trim_end_matches('\0').to_string();
                    tracing::info!("PG query: {}", sql);
                    let resp = conn.execute_query(&sql, &engine);
                    stream.write_all(&resp).await?;
                }
                b'X' => return Ok(()),
                b'p' => {
                    // SASLInitialResponse and SASLResponse share the type byte.
                    let resp = conn.handle_sasl(frame);
                    stream.write_all(&resp).await?;
                }
                other => {
                    // Everything else in the extended protocol funnels through
                    // the connection's own dispatcher, which knows the message
                    // grammar and answers unknown types with a protocol error.
                    if matches!(other, b'P' | b'B' | b'D' | b'E' | b'C' | b'S') {
                        let resp = conn.handle_extended_query(other, frame, &engine);
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

    // Packet framing, same reasoning as on the PostgreSQL side: a header is
    // three bytes of length and one of sequence id, and one read may hold a
    // whole packet, part of one, or several. Treating a read as a packet read
    // the command byte one position late (a MySQL header is four bytes) and
    // lost every packet that shared a segment with another.
    let mut pending: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; 8192];

    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        pending.extend_from_slice(&chunk[..n]);

        loop {
            if pending.len() < 4 {
                break;
            }
            let len = pending[0] as usize | (pending[1] as usize) << 8 | (pending[2] as usize) << 16;
            let total = 4 + len;
            if pending.len() < total {
                break;
            }
            let packet: Vec<u8> = pending.drain(..total).collect();

            if !conn.is_authenticated() {
                let resp = conn.handle_auth(&packet);
                stream.write_all(&resp).await?;
                continue;
            }

            let Some((cmd, args)) = packet.split_at(4).1.split_first().map(|(c, rest)| (*c, rest)) else {
                continue;
            };

            match cmd {
                0x03 => {
                    // COM_QUERY
                    let sql = String::from_utf8_lossy(args).to_string();
                    tracing::info!("MySQL query: {}", sql);
                    let resp = conn.execute_query(&sql, &engine);
                    stream.write_all(&resp).await?;
                }
                0x01 => {
                    // COM_QUIT
                    return Ok(());
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
