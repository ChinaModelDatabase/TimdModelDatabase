//! Tests that drive the real TCP server, not the protocol handlers.
//!
//! The unit tests in `src` call the handlers directly with buffers they build
//! themselves, which hides how the server cuts the byte stream into messages.
//! Every client outside this repository frames its bytes by the protocol rules,
//! so these tests do the same: standards-compliant packets over a real socket.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A standards-compliant StartupMessage: the length counts itself.
fn startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&196608i32.to_be_bytes());
    for (key, value) in [("user", user), ("database", database)] {
        payload.extend_from_slice(key.as_bytes());
        payload.push(0);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
    }
    payload.push(0);

    let mut message = ((4 + payload.len()) as i32).to_be_bytes().to_vec();
    message.extend_from_slice(&payload);
    message
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn start(pg_port: u16, mysql_port: u16) -> tempfile::TempDir {
    std::env::set_var("TMDB_USER", "tmdb");
    std::env::set_var("TMDB_PASSWORD", "s3cret");
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(tmdb_exec::Engine::new(dir.path()));
    tmdb_wire::start_server(
        engine,
        &format!("127.0.0.1:{}", pg_port),
        &format!("127.0.0.1:{}", mysql_port),
    )
    .await
    .unwrap();
    dir
}

async fn read_with_timeout(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 8192];
    let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
        .await
        .expect("the server never answered the startup message")
        .expect("read failed");
    buf[..n].to_vec()
}

/// A real client sends the length counting itself; anything else hangs the
/// connection, because the server would wait for bytes the client already sent.
#[tokio::test]
async fn a_startup_message_gets_a_sasl_challenge() {
    let pg_port = free_port();
    let _dir = start(pg_port, free_port()).await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", pg_port))
        .await
        .unwrap();
    stream
        .write_all(&startup_message("tmdb", "db"))
        .await
        .unwrap();

    let response = read_with_timeout(&mut stream).await;
    assert_eq!(response[0], b'R', "expected an authentication message");
    let code = i32::from_be_bytes([response[5], response[6], response[7], response[8]]);
    assert_eq!(code, 10, "expected SASL, got {}", code);

    // The mechanisms are a list of NUL-terminated names closed by an empty
    // string. A single NUL ends the name, not the list, and libpq refuses the
    // whole exchange rather than guessing.
    let mechanisms = &response[9..];
    assert!(
        mechanisms.ends_with(&[0, 0]),
        "mechanism list is not terminated: {:?}",
        mechanisms
    );
    let names: Vec<&[u8]> = mechanisms
        .split(|b| *b == 0)
        .filter(|name| !name.is_empty())
        .collect();
    assert_eq!(names, vec![b"SCRAM-SHA-256".as_slice()]);

    // Length counts itself, the authentication code, and every byte of the list.
    let declared =
        i32::from_be_bytes([response[1], response[2], response[3], response[4]]) as usize;
    assert_eq!(
        declared,
        4 + 4 + mechanisms.len(),
        "the length must cover the code and the mechanism list"
    );
    assert_eq!(
        response.len(),
        1 + declared,
        "message length disagrees with the bytes sent"
    );
}

/// Only the very first message is untyped. Every message after it is a type
/// byte followed by the length, and reading the length from offset zero asks
/// for a message four bytes longer than any client will ever send.
#[tokio::test]
async fn a_simple_query_framed_by_the_protocol_is_answered() {
    let pg_port = free_port();
    let _dir = start(pg_port, free_port()).await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", pg_port))
        .await
        .unwrap();
    stream
        .write_all(&startup_message("tmdb", "db"))
        .await
        .unwrap();
    let _challenge = read_with_timeout(&mut stream).await;

    let sql = b"SELECT 1\0";
    let mut query = vec![b'Q'];
    query.extend_from_slice(&((4 + sql.len()) as i32).to_be_bytes());
    query.extend_from_slice(sql);
    stream.write_all(&query).await.unwrap();

    let response = read_with_timeout(&mut stream).await;
    assert_eq!(
        response[0],
        b'E',
        "expected an error response for an unauthenticated query, got {:?}",
        response.first()
    );
}

/// The TLS probe and the startup message can arrive in one segment, so the
/// server has to split a buffer into as many messages as it actually holds.
#[tokio::test]
async fn a_tls_probe_and_a_startup_message_in_one_write_are_both_answered() {
    let pg_port = free_port();
    let _dir = start(pg_port, free_port()).await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", pg_port))
        .await
        .unwrap();
    let mut request = (8i32).to_be_bytes().to_vec();
    request.extend_from_slice(&80877103i32.to_be_bytes());
    stream.write_all(&request).await.unwrap();

    let mut refused = [0u8; 1];
    tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut refused))
        .await
        .expect("no answer to SSLRequest")
        .unwrap();
    assert_eq!(refused[0], b'N', "the server must refuse TLS, not hang");

    stream
        .write_all(&startup_message("tmdb", "db"))
        .await
        .unwrap();
    let response = read_with_timeout(&mut stream).await;
    assert_eq!(response[0], b'R');
    let code = i32::from_be_bytes([response[5], response[6], response[7], response[8]]);
    assert_eq!(code, 10);
}
