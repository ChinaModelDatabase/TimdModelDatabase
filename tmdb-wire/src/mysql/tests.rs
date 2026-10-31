use super::*;
use tmdb_exec::Engine;

fn engine() -> (tempfile::TempDir, Engine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path());
    (dir, engine)
}

fn seeded() -> (tempfile::TempDir, Engine) {
    let (dir, engine) = engine();
    engine
        .execute("CREATE TABLE metrics (ts TIMESTAMP, host TEXT, value DOUBLE)")
        .unwrap();
    engine
        .execute("INSERT INTO metrics VALUES (1000, 'a', 1.5), (2000, 'b', 2.5)")
        .unwrap();
    (dir, engine)
}

/// Split a framed stream back into (sequence id, payload) pairs, so a test can
/// assert on individual packets instead of one long byte string.
fn unframe(bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 4 <= bytes.len() {
        let len = bytes[i] as usize | (bytes[i + 1] as usize) << 8 | (bytes[i + 2] as usize) << 16;
        let seq = bytes[i + 3];
        let start = i + 4;
        let end = start + len;
        assert!(end <= bytes.len(), "packet at {} runs past the buffer", i);
        out.push((seq, bytes[start..end].to_vec()));
        i = end;
    }
    assert_eq!(i, bytes.len(), "trailing bytes that are not a packet");
    out
}

#[test]
fn frame_writes_a_three_byte_length_and_a_sequence_id() {
    let p = frame(7, &[1, 2, 3]);
    assert_eq!(p, vec![3, 0, 0, 7, 1, 2, 3]);
}

#[test]
fn frame_handles_a_payload_larger_than_255_bytes() {
    let payload = vec![0xABu8; 300];
    let p = frame(0, &payload);
    let len = p[0] as usize | (p[1] as usize) << 8 | (p[2] as usize) << 16;
    assert_eq!(len, 300);
    assert_eq!(unframe(&p).len(), 1);
}

#[test]
fn the_handshake_is_framed_and_starts_at_sequence_zero() {
    let mut c = MysqlConnection::new();
    let packets = unframe(&c.build_handshake());
    assert_eq!(packets.len(), 1);
    let (seq, body) = &packets[0];
    assert_eq!(*seq, 0, "the server greeting is always sequence 0");
    assert_eq!(body[0], 10, "handshake protocol version 10");
    // Server version string, NUL terminated.
    let v_end = body[1..].iter().position(|&b| b == 0).unwrap() + 1;
    assert_eq!(&body[1..v_end], b"8.0.35-tmDB");
    assert!(body.ends_with(b"caching_sha2_password\0"));
}

/// Every logical message needs its own header. Concatenating them into one
/// buffer, as this used to, leaves the client reading a column-count packet as
/// if it were the whole result.
#[test]
fn a_result_set_is_a_sequence_of_packets_with_counting_ids() {
    let (_dir, engine) = seeded();
    let mut c = MysqlConnection::new();
    let bytes = c.execute_query("SELECT ts, host, value FROM metrics", &engine);
    let packets = unframe(&bytes);

    // count, 3 column defs, EOF, 2 rows, EOF
    assert_eq!(packets.len(), 8);
    for (i, (seq, _)) in packets.iter().enumerate() {
        // The client's command carries sequence 0, so the server's first
        // response packet is 1.
        assert_eq!(*seq as usize, i + 1, "packet {} has the wrong sequence id", i);
    }
    assert_eq!(packets[0].1[0], 3, "column count");
    assert_eq!(packets[4].1[0], 0xFE, "EOF after the column defs");
    assert_eq!(packets[7].1[0], 0xFE, "EOF after the rows");
}

#[test]
fn column_definitions_name_the_column_and_carry_its_type() {
    let (_dir, engine) = seeded();
    let mut c = MysqlConnection::new();
    let packets = unframe(&c.execute_query("SELECT ts, host, value FROM metrics", &engine));

    let name_of = |body: &[u8]| -> String {
        // catalog, schema, table and org_table are length-encoded and all zero
        // here, so the name starts after them.
        let mut i = 0usize;
        for _ in 0..4 {
            i += 1 + body[i] as usize;
        }
        let len = body[i] as usize;
        String::from_utf8_lossy(&body[i + 1..i + 1 + len]).into_owned()
    };
    assert_eq!(name_of(&packets[1].1), "ts");
    assert_eq!(name_of(&packets[2].1), "host");
    assert_eq!(name_of(&packets[3].1), "value");

    // The type code is followed by flags, decimals, filler and the ordinal:
    // seven bytes, so it sits eighth from the end.
    let type_of = |body: &[u8]| body[body.len() - 8];
    assert_eq!(type_of(&packets[1].1), 0x08, "timestamp is LONGLONG");
    assert_eq!(type_of(&packets[2].1), 0xFD, "string is VAR_STRING");
    assert_eq!(type_of(&packets[3].1), 0x05, "double is DOUBLE");
}

#[test]
fn rows_carry_decoded_text_not_raw_little_endian_bytes() {
    let (_dir, engine) = seeded();
    let mut c = MysqlConnection::new();
    let packets = unframe(&c.execute_query("SELECT ts, host, value FROM metrics", &engine));

    // Row 1: ts=1000, host='a', value=1.5
    let row = &packets[5].1;
    let mut i = 0usize;
    let mut fields = Vec::new();
    while i < row.len() {
        let len = row[i] as usize;
        fields.push(String::from_utf8_lossy(&row[i + 1..i + 1 + len]).into_owned());
        i += 1 + len;
    }
    assert_eq!(fields, vec!["1000", "a", "1.5"]);
}

#[test]
fn a_null_value_is_the_length_encoded_null_marker() {
    let mut c = MysqlConnection::new();
    let result = QueryResult::Rows {
        columns: vec!["v".to_string()],
        types: vec![DataType::Float64],
        rows: vec![
            vec![None],
            vec![Some(1.0f64.to_le_bytes().to_vec())],
        ],
    };
    c.begin_response();
    let packets = unframe(&c.encode_result_set(&result));
    // count, def, EOF, 2 rows, EOF
    assert_eq!(packets[3].1, vec![0xFB], "a NULL row is the single 0xFB byte");
    // Rust renders 1.0f64 as "1"; both wire formats accept that for a DOUBLE.
    assert_eq!(packets[4].1, vec![1, b'1']);
}

#[test]
fn booleans_render_as_one_and_zero() {
    assert_eq!(decode_text(&DataType::Bool, Some(&[1])), Some("1".into()));
    assert_eq!(decode_text(&DataType::Bool, Some(&[0])), Some("0".into()));
    assert_eq!(decode_text(&DataType::Bool, None), None);
}

#[test]
fn a_long_string_uses_the_two_byte_length_prefix() {
    let long = "x".repeat(300);
    let row = vec![Some(long.clone().into_bytes())];
    let encoded = encode_row(&row, &[DataType::String]);
    assert_eq!(encoded[0], 0xFC, "lengths >= 251 use the 0xFC form");
    let declared = u16::from_le_bytes([encoded[1], encoded[2]]) as usize;
    assert_eq!(declared, 300);
    assert_eq!(&encoded[3..], long.as_bytes());
}

#[test]
fn a_non_ascii_string_is_measured_in_bytes() {
    let s = "温度";
    let row = vec![Some(s.as_bytes().to_vec())];
    let encoded = encode_row(&row, &[DataType::String]);
    assert_eq!(
        encoded[0] as usize,
        s.len(),
        "the prefix counts bytes, not characters"
    );
    assert_eq!(&encoded[1..], s.as_bytes());
}

#[test]
fn an_ok_packet_is_framed_and_follows_the_protocol_shape() {
    let mut c = MysqlConnection::new();
    c.begin_response();
    let packets = unframe(&c.build_ok());
    assert_eq!(packets[0].0, 1);
    assert_eq!(packets[0].1[0], 0x00);
    assert_eq!(packets[0].1.len(), 7);
}

#[test]
fn an_error_packet_carries_the_code_and_sqlstate() {
    let mut c = MysqlConnection::new();
    c.begin_response();
    let packets = unframe(&c.build_error(1064, "bad syntax"));
    let (_, body) = &packets[0];
    assert_eq!(body[0], 0xFF);
    assert_eq!(u16::from_le_bytes([body[1], body[2]]), 1064);
    assert_eq!(&body[3..8], b"#HY00");
    assert!(String::from_utf8_lossy(body).contains("bad syntax"));
}

#[test]
fn a_failing_query_becomes_an_error_packet_not_a_result_set() {
    let (_dir, engine) = engine();
    let mut c = MysqlConnection::new();
    let packets = unframe(&c.execute_query("SELECT * FROM nope", &engine));
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].1[0], 0xFF);
}

#[test]
fn a_ddl_statement_is_acknowledged_with_an_ok_packet() {
    let (_dir, engine) = engine();
    let mut c = MysqlConnection::new();
    let packets = unframe(&c.execute_query("CREATE TABLE t (ts TIMESTAMP, v DOUBLE)", &engine));
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].1[0], 0x00, "DDL answers OK");
}

#[test]
fn the_result_sequence_restarts_for_every_command() {
    let (_dir, engine) = seeded();
    let mut c = MysqlConnection::new();
    let first = unframe(&c.execute_query("SELECT ts FROM metrics", &engine));
    let second = unframe(&c.execute_query("SELECT ts FROM metrics", &engine));
    assert_eq!(first[0].0, 1, "each command is answered from sequence 1");
    assert_eq!(second[0].0, 1, "the counter must not carry over");
}

