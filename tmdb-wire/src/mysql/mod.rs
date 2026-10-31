/// MySQL wire protocol implementation (v4.1+)
/// Supports: handshake v10, caching_sha2_password, COM_QUERY, COM_STMT_*, result set

use bytes::{Buf, BufMut, BytesMut};

use tmdb_exec::{Engine, QueryResult};
use tmdb_timestore::DataType;

pub const CLIENT_PROTOCOL_41: u32 = 1 << 9;
pub const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
pub const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
pub const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;

/// Prepared statement
struct PreparedStatement {
    sql: String,
    param_count: u16,
    result_columns: Vec<String>,
}

/// MySQL connection state
pub struct MysqlConnection {
    authenticated: bool,
    sequence_id: u8,
    prepared_stmts: std::collections::HashMap<u32, PreparedStatement>,
    next_stmt_id: u32,
}

impl MysqlConnection {
    pub fn new() -> Self {
        Self {
            authenticated: false,
            sequence_id: 0,
            prepared_stmts: std::collections::HashMap::new(),
            next_stmt_id: 1,
        }
    }

    /// Build handshake packet
    pub fn build_handshake(&mut self) -> Vec<u8> {
        let mut buf = BytesMut::new();

        // Protocol version
        buf.put_u8(10);

        // Server version
        buf.put_slice(b"8.0.35-tmDB\0");

        // Connection ID
        buf.put_u32_le(1);

        // Auth plugin data part 1 (8 bytes)
        buf.put_slice(&[0x41; 8]);

        // Filler
        buf.put_u8(0);

        // Capability flags
        let caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        buf.put_u16_le(caps as u16);

        // Character set
        buf.put_u8(33); // utf8mb4

        // Status flags
        buf.put_u16_le(0x0002);

        // Capability flags (upper 2 bytes)
        buf.put_u16_le((caps >> 16) as u16);

        // Auth plugin data length
        buf.put_u8(21);

        // Reserved (10 bytes)
        buf.put_slice(&[0u8; 10]);

        // Auth plugin data part 2. The protocol requires this to be at least 13
        // bytes including its own NUL terminator.
        buf.put_slice(&[0x42; 12]);
        buf.put_u8(0);

        // Auth plugin name
        buf.put_slice(b"caching_sha2_password\0");

        // The server greeting is always sequence 0; the client's reply is 1.
        self.sequence_id = 0;
        frame(self.sequence_id, &buf)
    }

    /// Start a response to a client command.
    ///
    /// The server always answers a command with sequence 1, then counts up per
    /// packet. Getting this wrong desynchronises the client's sequence check
    /// and it drops the connection mid-result.
    pub fn begin_response(&mut self) {
        self.sequence_id = 1;
    }

    /// Handle auth response
    pub fn handle_auth(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::from(data);
        if buf.remaining() < 4 {
            return self.build_ok();
        }

        let _len = buf.get_u32();
        let _seq = buf.get_u8();

        let mut username = Vec::new();
        while buf.has_remaining() {
            let b = buf.get_u8();
            if b == 0 {
                break;
            }
            username.push(b);
        }

        if buf.has_remaining() {
            let auth_len = buf.get_u8() as usize;
            let skip = auth_len.min(buf.remaining());
            buf.advance(skip);
        }

        tracing::info!("MySQL auth: user={}", String::from_utf8_lossy(&username));
        self.authenticated = true;
        self.sequence_id += 1;
        self.build_ok()
    }

    /// Execute query
    pub fn execute_query(&mut self, sql: &str, engine: &Engine) -> Vec<u8> {
        self.begin_response();
        match engine.execute(sql) {
            Ok(result) => self.encode_result_set(&result),
            Err(e) => self.build_error(1064, &e.to_string()),
        }
    }

    /// Handle COM_STMT_PREPARE
    pub fn handle_stmt_prepare(&mut self, sql: &str) -> Vec<u8> {
        let stmt_id = self.next_stmt_id;
        self.next_stmt_id += 1;

        self.prepared_stmts.insert(
            stmt_id,
            PreparedStatement {
                sql: sql.to_string(),
                param_count: 0,
                result_columns: Vec::new(),
            },
        );

        self.begin_response();
        let mut buf = BytesMut::new();
        buf.put_u8(0x00); // OK
        buf.put_u32_le(stmt_id); // statement_id
        buf.put_u16_le(0); // num_columns
        buf.put_u16_le(0); // num_params
        buf.put_u8(0); // reserved
        buf.put_u16_le(0); // warning_count
        frame(self.sequence_id, &buf)
    }

    /// Handle COM_STMT_EXECUTE
    pub fn handle_stmt_execute(&mut self, stmt_id: u32, engine: &Engine) -> Vec<u8> {
        // The sql is cloned out before the mutable borrow: `begin_response`
        // needs `&mut self`, and holding the map borrow across it would
        // conflict.
        let sql = match self.prepared_stmts.get(&stmt_id) {
            Some(stmt) => stmt.sql.clone(),
            None => {
                self.begin_response();
                return self.build_error(1045, "Unknown prepared statement");
            }
        };
        self.begin_response();
        match engine.execute(&sql) {
            Ok(result) => self.encode_result_set(&result),
            Err(e) => self.build_error(1064, &e.to_string()),
        }
    }

    /// Handle COM_STMT_CLOSE
    pub fn handle_stmt_close(&mut self, stmt_id: u32) -> Vec<u8> {
        self.prepared_stmts.remove(&stmt_id);
        self.build_ok()
    }

    /// Encode result set as a sequence of MySQL packets.
    ///
    /// The order is fixed by the protocol: column count, one definition per
    /// column, EOF, one packet per row, EOF. Each is framed separately and
    /// carries the next sequence id.
    fn encode_result_set(&mut self, result: &QueryResult) -> Vec<u8> {
        match result {
            QueryResult::Empty | QueryResult::Affected(_) => self.build_ok(),
            QueryResult::Rows { columns, types, rows } => {
                let mut out = Vec::new();
                let mut seq = self.sequence_id;

                // Column count. Past 250 columns the count itself is
                // length-encoded.
                let count = columns.len();
                let count_payload = if count < 251 {
                    vec![count as u8]
                } else {
                    let mut c = vec![0xFC];
                    c.extend_from_slice(&(count as u16).to_le_bytes());
                    c
                };
                out.extend(frame(seq, &count_payload));
                seq = seq.wrapping_add(1);

                for (i, col) in columns.iter().enumerate() {
                    let ty = types.get(i).copied().unwrap_or(DataType::String);
                    out.extend(frame(seq, &encode_column_def(i, col, &ty)));
                    seq = seq.wrapping_add(1);
                }

                out.extend(frame(seq, &eof_payload()));
                seq = seq.wrapping_add(1);

                // Rows carry rendered text, so the stored little-endian bytes
                // have to be decoded against the column type first; sending them
                // raw would show every number as mojibake.
                for row in rows {
                    out.extend(frame(seq, &encode_row(row, types)));
                    seq = seq.wrapping_add(1);
                }

                out.extend(frame(seq, &eof_payload()));

                self.sequence_id = seq.wrapping_add(1);
                out
            }
        }
    }

    pub fn build_ok(&mut self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        buf.put_u8(0x00);
        buf.put_u8(0); // affected rows
        buf.put_u8(0); // last insert id
        buf.put_u16_le(0x0002); // status: autocommit
        buf.put_u16_le(0); // warnings
        let out = frame(self.sequence_id, &buf);
        self.sequence_id = self.sequence_id.wrapping_add(1);
        out
    }

    pub fn build_error(&mut self, code: u16, message: &str) -> Vec<u8> {
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF);
        buf.put_u16_le(code);
        buf.put_slice(b"#HY000");
        buf.put_slice(message.as_bytes());
        let out = frame(self.sequence_id, &buf);
        self.sequence_id = self.sequence_id.wrapping_add(1);
        out
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }
}

/// Frame one payload as a MySQL packet.
///
/// The wire format is a 3-byte little-endian payload length, a 1-byte sequence
/// id, then the payload. Every logical message -- handshake, OK, error, each
/// column definition, each row -- is a separate packet with its own header.
/// Returning payloads without these headers, or concatenating several messages
/// into one buffer, produces a byte stream no client can parse: the client
/// reads the first four bytes as a length and then loses sync.
pub fn frame(seq: u8, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.push((len & 0xFF) as u8);
    out.push(((len >> 8) & 0xFF) as u8);
    out.push(((len >> 16) & 0xFF) as u8);
    out.push(seq);
    out.extend_from_slice(payload);
    out
}

/// Payload of an EOF_Packet.
fn eof_payload() -> Vec<u8> {
    vec![0xFE, 0x00, 0x00, 0x02, 0x00]
}

/// Render one stored value as MySQL text. `None` means SQL NULL.
fn decode_text(ty: &DataType, raw: Option<&[u8]>) -> Option<String> {
    match tmdb_exec::Value::decode(ty, raw) {
        tmdb_exec::Value::Null => None,
        // MySQL renders TINY(1) as 1/0; "true" would not parse back as a
        // boolean in a client.
        tmdb_exec::Value::Bool(b) => Some(if b { "1".to_string() } else { "0".to_string() }),
        v => Some(v.to_text()),
    }
}

/// MySQL protocol type code for a stored column type.
fn mysql_type(ty: &DataType) -> u8 {
    match ty {
        DataType::Bool => 0x01,        // TINY
        DataType::Int32 => 0x03,       // LONG
        DataType::Int64 => 0x08,       // LONGLONG
        DataType::Timestamp => 0x08,   // LONGLONG
        DataType::Float32 => 0x04,     // FLOAT
        DataType::Float64 => 0x05,     // DOUBLE
        DataType::String => 0xFD,      // VAR_STRING
    }
}

/// Declared width in bytes, which clients use to size their buffers.
fn column_length(ty: &DataType) -> u16 {
    match ty {
        DataType::Bool => 1,
        DataType::Int32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::Timestamp | DataType::Float64 => 8,
        // VAR_STRING carries no declared width.
        DataType::String => 0xFFFF,
    }
}

/// One row, as the text protocol renders it.
fn encode_row(row: &[Option<Vec<u8>>], types: &[DataType]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    for (i, val) in row.iter().enumerate() {
        let ty = types.get(i).copied().unwrap_or(DataType::String);
        match decode_text(&ty, val.as_deref()) {
            Some(text) => {
                let bytes = text.as_bytes();
                let len = bytes.len();
                if len < 251 {
                    buf.put_u8(len as u8);
                } else if len < 65536 {
                    buf.put_u8(0xFC);
                    buf.put_u16_le(len as u16);
                } else {
                    buf.put_u8(0xFD);
                    buf.put_u32_le(len as u32);
                }
                buf.extend_from_slice(bytes);
            }
            // 0xFB is the length-encoded NULL.
            None => buf.put_u8(0xFB),
        }
    }
    buf.to_vec()
}

/// One column definition packet.
fn encode_column_def(index: usize, name: &str, ty: &DataType) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u8(0x03); // lenenc "def"
    buf.put_slice(b"def");
    buf.put_u8(0); // schema
    buf.put_u8(0); // table
    buf.put_u8(0); // org_table
    buf.put_u8(name.len() as u8);
    buf.put_slice(name.as_bytes());
    buf.put_u8(0); // org_name
    buf.put_u8(0); // length of fixed fields (0x0c)
    buf.put_u16_le(0x0021); // utf8mb4_general_ci
    buf.put_u32_le(column_length(ty) as u32);
    buf.put_u8(mysql_type(ty));
    buf.put_u16_le(0); // flags
    buf.put_u8(0); // decimals
    buf.put_u16_le(0); // filler
    buf.put_u16_le(index as u16); // ordinal position
    buf.to_vec()
}

#[cfg(test)]
mod tests;