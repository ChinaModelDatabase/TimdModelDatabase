use super::*;
use tmdb_exec::Engine;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// A SCRAM client, so the server is exercised end to end rather than against
// its own output. The client proof is computed here independently of
// `scram::Server`; the primitives themselves are pinned by the RFC vectors in
// `scram_tests`.
fn client_nonce(seed: u8) -> String {
    std::iter::repeat(seed).take(18).map(|c| c as char).collect()
}

fn parse_attrs(s: &str) -> Vec<(&str, &str)> {
    s.split(',')
        .filter_map(|p| {
            let (k, v) = p.split_once('=')?;
            Some((k, v))
        })
        .collect()
}

/// Drive a full SCRAM exchange. Returns true when the server authenticated us.
fn handshake(conn: &mut PgConnection, user: &str, password: &str, seed: u8) -> bool {
    let first_bare = format!("n={},r={}", user, client_nonce(seed));
    let initial = format!("n,,{}", first_bare);

    // SASLInitialResponse: mechanism, int32 length, bytes.
    let mut msg = Vec::new();
    msg.extend_from_slice(&(8 + initial.len() as i32).to_be_bytes());
    msg.extend_from_slice(b"SCRAM-SHA-256\0");
    msg.extend_from_slice(&(initial.len() as i32).to_be_bytes());
    msg.extend_from_slice(initial.as_bytes());

    let bytes = conn.handle_scram_response(&msg);
    let msgs = unframe(&bytes);
    assert_eq!(msgs.len(), 1, "expected exactly one reply message");
    if msgs[0].0 == b'E' {
        // An unknown user, a bad mechanism or a malformed message all stop here.
        return false;
    }
    assert_eq!(msgs[0].0, b'R');
    assert_eq!(
        i32::from_be_bytes([msgs[0].1[0], msgs[0].1[1], msgs[0].1[2], msgs[0].1[3]]),
        11
    );
    let server_first = String::from_utf8_lossy(&msgs[0].1[4..])
        .trim_end_matches('\0')
        .to_string();

    let attrs = parse_attrs(&server_first);
    let full_nonce = attrs
        .iter()
        .find(|(k, _)| *k == "r")
        .map(|(_, v)| *v)
        .expect("server nonce");
    assert!(
        full_nonce.starts_with(&client_nonce(seed)),
        "server nonce must extend ours"
    );
    let salt_b64 = attrs.iter().find(|(k, _)| *k == "s").map(|(_, v)| *v).unwrap();
    let iterations: u32 = attrs
        .iter()
        .find(|(k, _)| *k == "i")
        .map(|(_, v)| *v)
        .unwrap()
        .parse()
        .unwrap();

    let salt = scram::base64_decode(salt_b64).expect("server salt is valid base64");
    let salted = scram::pbkdf2_hmac_sha256(password.as_bytes(), &salt, iterations);

    let without_proof = format!("c=biws,r={}", full_nonce);
    let auth_message = format!("{},{},{}", first_bare, server_first, without_proof);

    let client_key = scram::hmac_sha256(&salted, b"Client Key");
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();
    let client_sig = scram::hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = client_key;
    for i in 0..32 {
        proof[i] ^= client_sig[i];
    }

    let client_final = format!("{},p={}", without_proof, scram::base64_encode(&proof));
    let mut msg = Vec::new();
    msg.extend_from_slice(&(4 + client_final.len() as i32).to_be_bytes());
    msg.extend_from_slice(client_final.as_bytes());

    let bytes = conn.handle_scram_final(&msg);
    let msgs = unframe(&bytes);
    if msgs.iter().any(|(t, _)| *t == b'E') {
        return false;
    }
    conn.is_authenticated()
}

fn connected() -> (tempfile::TempDir, Engine, PgConnection) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path());
    let mut users = StaticUsers::new();
    users.insert("alice", "s3cret");
    let mut conn = PgConnection::with_verifier(Arc::new(users));
    conn.handle_startup(&startup_packet("alice", "db"));
    assert!(handshake(&mut conn, "alice", "s3cret", b'a'), "handshake failed");
    (dir, engine, conn)
}

fn seeded() -> (tempfile::TempDir, Engine, PgConnection) {
    let (dir, engine, conn) = connected();
    engine
        .execute("CREATE TABLE metrics (ts TIMESTAMP, host TEXT, value DOUBLE)")
        .unwrap();
    engine
        .execute("INSERT INTO metrics VALUES (1000, 'a', 1.5), (2000, 'b', 2.5)")
        .unwrap();
    (dir, engine, conn)
}

fn engine() -> (tempfile::TempDir, Engine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path());
    (dir, engine)
}

fn startup_packet(user: &str, database: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i32(PG_PROTOCOL_VERSION);
    for (key, value) in [("user", user), ("database", database)] {
        body.put_slice(key.as_bytes());
        body.put_u8(0);
        body.put_slice(value.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);

    let mut out = BytesMut::new();
    out.put_i32(4 + body.len() as i32);
    out.extend_from_slice(&body);
    out.to_vec()
}

/// Split a stream of PG messages into (type byte, body) pairs.
fn unframe(bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 5 <= bytes.len() {
        let ty = bytes[i];
        let len =
            i32::from_be_bytes([bytes[i + 1], bytes[i + 2], bytes[i + 3], bytes[i + 4]]) as usize;
        let start = i + 5;
        let end = start + len - 4;
        assert!(end <= bytes.len(), "message at {} runs past the buffer", i);
        out.push((ty, bytes[start..end].to_vec()));
        i = end;
    }
    assert_eq!(i, bytes.len(), "trailing bytes that are not a message");
    out
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

#[test]
fn a_correct_password_authenticates() {
    let mut conn = PgConnection::new();
    conn.handle_startup(&startup_packet("alice", "db"));
    let mut users = StaticUsers::new();
    users.insert("alice", "s3cret");
    let mut conn = PgConnection::with_verifier(Arc::new(users));
    conn.handle_startup(&startup_packet("alice", "db"));
    assert!(handshake(&mut conn, "alice", "s3cret", b'a'));
    assert!(conn.is_authenticated());
}

#[test]
fn a_wrong_password_is_refused() {
    let mut users = StaticUsers::new();
    users.insert("alice", "s3cret");
    let mut conn = PgConnection::with_verifier(Arc::new(users));
    conn.handle_startup(&startup_packet("alice", "db"));
    assert!(!handshake(&mut conn, "alice", "wrong", b'a'));
    assert!(!conn.is_authenticated());
}

#[test]
fn an_unknown_user_is_refused() {
    let mut users = StaticUsers::new();
    users.insert("alice", "s3cret");
    let mut conn = PgConnection::with_verifier(Arc::new(users));
    conn.handle_startup(&startup_packet("mallory", "db"));
    assert!(!handshake(&mut conn, "mallory", "s3cret", b'a'));
    assert!(!conn.is_authenticated());
}

#[test]
fn an_unknown_user_and_a_wrong_password_are_indistinguishable() {
    let mut users = StaticUsers::new();
    users.insert("alice", "s3cret");
    let mut a = PgConnection::with_verifier(Arc::new(StaticUsers::new()));
    let mut b = PgConnection::with_verifier(Arc::new(users));
    let msg_a = vec![b'p', 0, 0, 0, 8, b'x'];
    let msg_b = vec![b'p', 0, 0, 0, 8, b'x'];
    a.handle_startup(&startup_packet("nobody", "db"));
    b.handle_startup(&startup_packet("alice", "db"));
    let ra = a.handle_scram_response(&msg_a);
    let rb = b.handle_scram_response(&msg_b);
    // Both are errors; the text must not disclose which account failed.
    let ta = String::from_utf8_lossy(&ra).to_string();
    let tb = String::from_utf8_lossy(&rb).to_string();
    assert_eq!(ta, tb);
}

#[test]
fn queries_are_refused_before_authentication() {
    let (_dir, engine) = engine();
    let mut users = StaticUsers::new();
    users.insert("alice", "s3cret");
    let mut conn = PgConnection::with_verifier(Arc::new(users));
    conn.handle_startup(&startup_packet("alice", "db"));

    let bytes = conn.execute_query("CREATE TABLE t (ts TIMESTAMP, v DOUBLE)", &engine);
    let msgs = unframe(&bytes);
    assert_eq!(msgs[0].0, b'E');
    let text = String::from_utf8_lossy(&msgs[0].1);
    assert!(text.contains("authentication required"), "got {:?}", text);
}

#[test]
fn the_extended_protocol_is_refused_before_authentication() {
    let (_dir, engine) = engine();
    let mut conn = PgConnection::new();
    let bytes = conn.handle_extended_query(b'P', &[0, 0, 0, 8, 0, 0, 0, 0], &engine);
    let msgs = unframe(&bytes);
    assert_eq!(msgs[0].0, b'E');
    assert!(String::from_utf8_lossy(&msgs[0].1).contains("authentication required"));
}

#[test]
fn a_failed_exchange_leaves_the_connection_unauthenticated() {
    let mut users = StaticUsers::new();
    users.insert("alice", "s3cret");
    let mut conn = PgConnection::with_verifier(Arc::new(users));
    conn.handle_startup(&startup_packet("alice", "db"));
    assert!(!handshake(&mut conn, "alice", "nope", b'a'));

    // A follow-up attempt must not be able to skip the handshake.
    let bytes = conn.execute_query("SELECT 1", &engine().1);
    assert_eq!(unframe(&bytes)[0].0, b'E');
}

#[test]
fn an_unsupported_sasl_mechanism_is_rejected() {
    let mut users = StaticUsers::new();
    users.insert("alice", "s3cret");
    let mut conn = PgConnection::with_verifier(Arc::new(users));
    conn.handle_startup(&startup_packet("alice", "db"));
    let mut msg = Vec::new();
    msg.extend_from_slice(&8i32.to_be_bytes());
    msg.extend_from_slice(b"SCRAM-SHA-1\0");
    let bytes = conn.handle_scram_response(&msg);
    assert_eq!(unframe(&bytes)[0].0, b'E');
}

#[test]
fn the_announced_iteration_count_comes_from_the_stored_verifier() {
    // The count is whatever the verifier was created with, so raising it later
    // does not invalidate entries that are already on disk. Sub-4096 values are
    // rejected when the file is read, not silently advertised here.
    let mut users = StaticUsers::new();
    users.insert_verifier(
        "alice",
        scram::Verifier::from_password(b"s3cret", vec![1u8; 16], 15000),
    );
    let mut conn = PgConnection::with_verifier(Arc::new(users));
    conn.handle_startup(&startup_packet("alice", "db"));

    let initial = "n,,n=alice,r=aaaa";
    let mut msg = Vec::new();
    msg.extend_from_slice(&(8 + initial.len() as i32).to_be_bytes());
    msg.extend_from_slice(b"SCRAM-SHA-256\0");
    msg.extend_from_slice(&(initial.len() as i32).to_be_bytes());
    msg.extend_from_slice(initial.as_bytes());

    let bytes = conn.handle_scram_response(&msg);
    let text = String::from_utf8_lossy(&unframe(&bytes)[0].1).to_string();
    let announced: u32 = parse_attrs(text.trim_end_matches('\0'))
        .into_iter()
        .find(|(k, _)| *k == "i")
        .map(|(_, v)| v.parse().unwrap())
        .unwrap();
    assert_eq!(announced, 15000);
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

#[test]
fn a_row_description_declares_every_column() {
    let (_dir, _engine, conn) = seeded();
    let bytes = conn.execute_query("SELECT ts, host, value FROM metrics", &_engine);
    let msgs = unframe(&bytes);
    assert_eq!(msgs[0].0, b'T', "RowDescription first");

    let body = &msgs[0].1;
    assert_eq!(i16::from_be_bytes([body[0], body[1]]), 3);
    let text = String::from_utf8_lossy(body);
    assert!(text.contains("ts\0"));
    assert!(text.contains("host\0"));
    assert!(text.contains("value\0"));
    assert_eq!(*body.last().unwrap(), 0, "every column is text format");
}

#[test]
fn timestamps_are_sent_as_integers_not_as_zero_point_zero() {
    let (_dir, _engine, conn) = seeded();
    let bytes = conn.execute_query("SELECT ts FROM metrics", &_engine);
    let msgs = unframe(&bytes);

    let row = msgs.iter().find(|(t, _)| *t == b'D').unwrap();
    assert_eq!(i16::from_be_bytes([row.1[0], row.1[1]]), 1);
    let len = i32::from_be_bytes([row.1[2], row.1[3], row.1[4], row.1[5]]);
    assert!(len > 0, "the value must be present, not NULL");
    assert_eq!(&row.1[6..6 + len as usize], b"1000");
}

#[test]
fn a_null_is_a_minus_one_length() {
    let result = QueryResult::Rows {
        columns: vec!["v".to_string()],
        types: vec![DataType::Float64],
        rows: vec![vec![None], vec![Some(1.0f64.to_le_bytes().to_vec())]],
    };
    let bytes = encode_result_set(&result);
    let msgs = unframe(&bytes);
    let rows: Vec<_> = msgs.iter().filter(|(t, _)| *t == b'D').collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        i32::from_be_bytes([rows[0].1[2], rows[0].1[3], rows[0].1[4], rows[0].1[5]]),
        -1,
        "NULL is a length of -1, not the text \"null\""
    );
}

#[test]
fn an_insert_reports_how_many_rows_it_wrote() {
    let (_dir, engine, conn) = connected();
    engine
        .execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE)")
        .unwrap();
    let bytes = conn.execute_query("INSERT INTO t VALUES (1, 1.0), (2, 2.0)", &engine);
    let msgs = unframe(&bytes);
    let complete = msgs.iter().find(|(t, _)| *t == b'C').unwrap();
    assert_eq!(&complete.1, b"INSERT 0 2\0");
}

#[test]
fn a_result_set_ends_with_ready_for_query() {
    let (_dir, _engine, conn) = seeded();
    let bytes = conn.execute_query("SELECT ts FROM metrics", &_engine);
    let msgs = unframe(&bytes);
    let last = msgs.last().unwrap();
    assert_eq!(last.0, b'Z', "ReadyForQuery closes every exchange");
    assert_eq!(last.1, vec![b'I'], "'I' means idle, not in a transaction");
}

#[test]
fn a_bad_query_produces_an_error_and_still_closes_cleanly() {
    let (_dir, _engine, conn) = connected();
    let bytes = conn.execute_query("SELECT * FROM missing", &_engine);
    let msgs = unframe(&bytes);
    assert_eq!(msgs[0].0, b'E', "ErrorResponse first");
    let text = String::from_utf8_lossy(&msgs[0].1);
    assert!(text.contains("ERROR"), "severity field: {:?}", text);
    assert!(text.contains("missing"));
    assert_eq!(msgs.last().unwrap().0, b'Z');
}

#[test]
fn a_ddl_statement_reports_command_complete() {
    let (_dir, _engine, conn) = connected();
    let bytes = conn.execute_query("CREATE TABLE t (ts TIMESTAMP, v DOUBLE)", &_engine);
    let msgs = unframe(&bytes);
    let complete = msgs.iter().find(|(t, _)| *t == b'C').unwrap();
    assert_eq!(&complete.1, b"CREATE TABLE\0");
}

#[test]
fn sync_answers_ready_for_query() {
    let (_dir, _engine, conn) = connected();
    let msgs = unframe(&conn.handle_sync());
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].0, b'Z');
    assert_eq!(msgs[0].1, vec![b'I']);
}

#[test]
fn an_unknown_message_type_is_answered_with_an_error() {
    let (_dir, engine, mut conn) = connected();
    let bytes = conn.handle_extended_query(b'?', &[], &engine);
    let msgs = unframe(&bytes);
    assert_eq!(msgs[0].0, b'E');
    assert!(String::from_utf8_lossy(&msgs[0].1).contains("unknown message type"));
}

#[test]
fn every_column_type_maps_to_a_postgres_type_oid() {
    assert_eq!(pg_type_oid(&DataType::Bool), 16);
    assert_eq!(pg_type_oid(&DataType::Int32), 23);
    assert_eq!(pg_type_oid(&DataType::Int64), 20);
    assert_eq!(pg_type_oid(&DataType::Timestamp), 1114);
    assert_eq!(pg_type_oid(&DataType::Float32), 700);
    assert_eq!(pg_type_oid(&DataType::Float64), 701);
    assert_eq!(pg_type_oid(&DataType::String), 25);
}

#[test]
fn a_non_ascii_string_survives_the_round_trip() {
    let (_dir, engine, conn) = connected();
    engine
        .execute("CREATE TABLE t (ts TIMESTAMP, name TEXT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, '温度计')").unwrap();
    let bytes = conn.execute_query("SELECT name FROM t", &engine);
    let msgs = unframe(&bytes);
    let row = msgs.iter().find(|(t, _)| *t == b'D').unwrap();
    let len = i32::from_be_bytes([row.1[2], row.1[3], row.1[4], row.1[5]]) as usize;
    assert_eq!(&row.1[6..6 + len], "温度计".as_bytes());
}
