//! Real MySQL clients frame packets with a three-byte length plus a sequence
//! id, and they may put several packets in one segment. These tests speak that
//! framing over a socket rather than calling the handlers directly.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn start(mysql_port: u16) -> tempfile::TempDir {
    std::env::set_var("TMDB_USER", "tmdb");
    std::env::set_var("TMDB_PASSWORD", "s3cret");
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(tmdb_exec::Engine::new(dir.path()));
    tmdb_wire::start_server(
        engine,
        &format!("127.0.0.1:{}", free_port()),
        &format!("127.0.0.1:{}", mysql_port),
    )
    .await
    .unwrap();
    dir
}

/// The MySQL packet header is four bytes: three of length, one of sequence.
fn packet(sequence: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_le_bytes()[..3].to_vec();
    out.push(sequence);
    out.extend_from_slice(payload);
    out
}

/// A 4.1 handshake response: capabilities, max packet, charset, 23 reserved
/// bytes, then the user name.
fn handshake_response(user: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0x0008_0a01u32.to_le_bytes());
    payload.extend_from_slice(&(1u32 << 24).to_le_bytes());
    payload.push(33);
    payload.extend_from_slice(&[0u8; 23]);
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.push(0x14);
    payload.extend_from_slice(&[0u8; 20]);
    payload.push(0);
    payload.extend_from_slice(b"caching_sha2_password\0");
    packet(1, &payload)
}

async fn connect(mysql_port: u16) -> tokio::net::TcpStream {
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", mysql_port))
        .await
        .unwrap();
    stream
}

/// Read one packet, splitting the three-byte length off by the protocol rules.
async fn read_packet(stream: &mut tokio::net::TcpStream, label: &str) -> (u8, Vec<u8>) {
    let header = read_exactly(stream, 4, label).await;
    let len = header[0] as usize | (header[1] as usize) << 8 | (header[2] as usize) << 16;
    let body = read_exactly(stream, len, label).await;
    (header[3], body)
}

async fn read_exactly(stream: &mut tokio::net::TcpStream, n: usize, label: &str) -> Vec<u8> {
    let mut out = vec![0u8; n];
    tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut out))
        .await
        .unwrap_or_else(|_| panic!("no answer for {}: connection hung", label))
        .unwrap();
    out
}

#[tokio::test]
async fn a_com_query_is_executed_rather_than_rejected_as_unknown() {
    let mysql_port = free_port();
    let _dir = start(mysql_port).await;
    let mut stream = connect(mysql_port).await;

    let (_seq, greeting) = read_packet(&mut stream, "handshake").await;
    assert_eq!(greeting[0], 10, "expected a protocol 10 greeting");

    stream.write_all(&handshake_response("tmdb")).await.unwrap();
    let (_seq, auth) = read_packet(&mut stream, "auth").await;
    assert_eq!(
        auth[0],
        0x00,
        "authentication failed: {:?}",
        &auth[..auth.len().min(3)]
    );

    // The command byte sits directly after the four-byte header; reading it one
    // byte later sees the start of the SQL text and answers "Unknown command".
    stream
        .write_all(&packet(0, &{
            let mut body = vec![b'\x03'];
            body.extend_from_slice(b"CREATE TABLE t (ts TIMESTAMP, v DOUBLE)");
            body
        }))
        .await
        .unwrap();
    let (_seq, body) = read_packet(&mut stream, "create table").await;
    assert_ne!(body[0], 0xff, "server rejected COM_QUERY: {}", {
        let text = String::from_utf8_lossy(&body[3..]);
        text.trim_end_matches('\0').to_string()
    });
}

/// A client that pipelines two statements in one segment still gets two
/// answers: one TCP read is not one packet.
#[tokio::test]
async fn two_queries_in_one_segment_are_both_answered() {
    let mysql_port = free_port();
    let _dir = start(mysql_port).await;
    let mut stream = connect(mysql_port).await;

    let (_seq, _greeting) = read_packet(&mut stream, "handshake").await;
    stream.write_all(&handshake_response("tmdb")).await.unwrap();
    let (_seq, _auth) = read_packet(&mut stream, "auth").await;

    let mut first = vec![0x03];
    first.extend_from_slice(b"CREATE TABLE piped (ts TIMESTAMP, v DOUBLE)");
    let mut second = vec![0x03];
    second.extend_from_slice(b"INSERT INTO piped VALUES (1000, 1.5)");

    let mut both = packet(0, &first);
    both.extend_from_slice(&packet(1, &second));
    stream.write_all(&both).await.unwrap();

    let (_seq, first_answer) = read_packet(&mut stream, "first query").await;
    assert_ne!(
        first_answer[0],
        0xff,
        "first query rejected: {}",
        String::from_utf8_lossy(&first_answer[3..])
    );
    let (_seq, second_answer) = read_packet(&mut stream, "second query").await;
    assert_ne!(
        second_answer[0],
        0xff,
        "second query rejected: {}",
        String::from_utf8_lossy(&second_answer[3..])
    );
}
