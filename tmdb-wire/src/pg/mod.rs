/// PostgreSQL wire protocol implementation (v3.0)
/// Supports: SSL negotiation, SCRAM-SHA-256, simple query, extended query, result set

use std::sync::Arc;
use bytes::{Buf, BufMut, BytesMut};

use tmdb_exec::{Engine, QueryResult};
use tmdb_timestore::DataType;

const PG_PROTOCOL_VERSION: i32 = 196608; // 3.0

/// SASL mechanism offered at authentication.
const SCRAM_MECHANISM: &[u8] = b"SCRAM-SHA-256\0";

/// The severity field every ErrorResponse starts with.
const ERROR_SEVERITY: &[u8] = b"SERROR\0";

/// Source of the password for a given user name.
///
/// SCRAM needs the plaintext password at two points: the salt and iteration
/// count are fixed when the server-first-message is built, and the proof is
/// checked against it afterwards. Keeping the lookup behind a trait lets the
/// exchange be tested without a configuration file, and lets the deployment
/// decide where credentials live.
pub trait PasswordVerifier: Send + Sync {
    /// `None` for an unknown user. Callers must not reveal which case it was.
    fn verifier(&self, user: &str) -> Option<scram::Verifier>;
}

/// In-memory user table.
#[derive(Default)]
pub struct StaticUsers {
    users: std::collections::HashMap<String, scram::Verifier>,
}

impl StaticUsers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, user: impl Into<String>, password: impl Into<String>) {
        self.insert_verifier(user, scram::Verifier::from_password(
            password.into().as_bytes(),
            crate::auth::random_salt(),
            crate::auth::DEFAULT_ITERATIONS,
        ));
    }

    pub fn insert_verifier(&mut self, user: impl Into<String>, verifier: scram::Verifier) {
        self.users.insert(user.into(), verifier);
    }
}

impl PasswordVerifier for StaticUsers {
    fn verifier(&self, user: &str) -> Option<scram::Verifier> {
        self.users.get(user).cloned()
    }
}

/// PostgreSQL connection state
pub struct PgConnection {
    authenticated: bool,
    user: String,
    database: String,
    prepared_stmts: std::collections::HashMap<String, PreparedStatement>,
    /// In-flight SCRAM exchange, if the client has sent client-first but not
    /// client-final.
    scram: Option<scram::Server>,
    verifier: Arc<dyn PasswordVerifier>,
}

/// Prepared statement
struct PreparedStatement {
    sql: String,
    param_types: Vec<i32>,
    result_columns: Vec<String>,
}

impl PgConnection {
    pub fn new() -> Self {
        Self::with_verifier(Arc::new(StaticUsers::new()))
    }

    pub fn with_verifier(verifier: Arc<dyn PasswordVerifier>) -> Self {
        Self {
            authenticated: false,
            user: String::new(),
            database: String::new(),
            prepared_stmts: std::collections::HashMap::new(),
            scram: None,
            verifier,
        }
    }

    /// Handle startup message, return auth response
    pub fn handle_startup(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::from(data);
        let _len = buf.get_u32();
        let _version = buf.get_i32();

        while buf.has_remaining() {
            let key = read_cstring(&mut buf);
            let value = read_cstring(&mut buf);
            match key.as_str() {
                "user" => self.user = value,
                "database" => self.database = value,
                _ => {}
            }
        }

        // Send AuthenticationSCRAM-SHA-256
        //
        // The length counts every byte after itself: the four-byte
        // authentication code plus the NUL-terminated mechanism name.
        let mut resp = BytesMut::new();
        resp.put_u8(b'R');
        resp.put_i32(4 + 4 + SCRAM_MECHANISM.len() as i32);
        resp.put_i32(10); // SASL
        resp.put_slice(SCRAM_MECHANISM);

        resp.to_vec()
    }

/// Handle SASLInitialResponse: the client's first SCRAM message.
    pub fn handle_scram_response(&mut self, data: &[u8]) -> Vec<u8> {
        // body: mechanism name (cstring), int32 length, client-first-message.
        let mut buf = BytesMut::from(data);
        let _len = buf.get_i32();
        let mechanism = read_cstring(&mut buf);

        if mechanism.trim_end_matches('\0') != "SCRAM-SHA-256" {
            return self.build_error_response("unsupported SASL mechanism");
        }
        if buf.remaining() < 4 {
            return self.build_error_response("SASLInitialResponse has no length");
        }
        let initial_len = buf.get_i32();
        if initial_len < 0 || initial_len as usize > buf.remaining() {
            return self.build_error_response("SASLInitialResponse length is out of range");
        }

        let client_first = String::from_utf8_lossy(&buf[..initial_len as usize]).to_string();
        let Some(verifier) = self.verifier.verifier(&self.user) else {
            return self.auth_failure("password authentication failed");
        };

        let server_nonce = generate_nonce();
        let Ok((scram, server_first)) = scram::Server::begin(&client_first, &verifier, &server_nonce)
        else {
            return self.auth_failure("password authentication failed");
        };

        self.scram = Some(scram);

        let mut resp = BytesMut::new();
        resp.put_u8(b'R');
        resp.put_i32(4 + 4 + server_first.len() as i32 + 1);
        resp.put_i32(11); // SASLContinue
        resp.put_slice(server_first.as_bytes());
        resp.put_u8(0);
        resp.to_vec()
    }

    /// Handle SASLResponse: the client's final SCRAM message.
    pub fn handle_scram_final(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::from(data);
        let _len = buf.get_i32();
        let client_final = String::from_utf8_lossy(&buf).to_string();

        let outcome = match self.scram.take() {
            Some(scram) => scram.finish(&client_final),
            None => Err(scram::Error::Malformed("no exchange in progress")),
        };

        let server_final = match outcome {
            Ok(server_final) => server_final,
            Err(_) => return self.auth_failure("password authentication failed"),
        };

        self.authenticated = true;

        let mut resp = BytesMut::new();

        // AuthenticationSASLFinal, then AuthenticationOk, then ReadyForQuery.
        let mut sasl_final = BytesMut::new();
        sasl_final.put_u8(b'R');
        sasl_final.put_i32(4 + 4 + server_final.len() as i32 + 1);
        sasl_final.put_i32(12);
        sasl_final.put_slice(server_final.as_bytes());
        sasl_final.put_u8(0);
        resp.extend_from_slice(&sasl_final);

        resp.put_u8(b'R');
        resp.put_i32(8);
        resp.put_i32(0); // AuthenticationOk

        resp.put_u8(b'Z');
        resp.put_i32(5);
        resp.put_u8(b'I');

        resp.to_vec()
    }

    /// Route a SASL message. Both SASLInitialResponse and SASLResponse use the
    /// message type `p`, so the exchange state is what distinguishes them.
    pub fn handle_sasl(&mut self, data: &[u8]) -> Vec<u8> {
        if self.scram.is_some() {
            self.handle_scram_final(data)
        } else {
            self.handle_scram_response(data)
        }
    }

    /// Answer a failed exchange with the same text an unknown user and a wrong
    /// password produce, so the reply cannot be used to enumerate accounts.
    fn auth_failure(&self, msg: &str) -> Vec<u8> {
        self.build_error_response(msg)
    }

    /// Handle extended query messages
    pub fn handle_extended_query(&mut self, msg_type: u8, data: &[u8], engine: &Engine) -> Vec<u8> {
        if !self.authenticated {
            return self.build_error_response("authentication required");
        }
        match msg_type {
            b'P' => self.handle_parse(data),
            b'B' => self.handle_bind(data),
            b'D' => self.handle_describe(data),
            b'E' => self.handle_execute(data, engine),
            b'C' => self.handle_close(data),
            b'S' => self.handle_sync(),
            _ => self.build_error_response("unknown message type"),
        }
    }

    pub fn handle_parse(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::from(data);
        // Skip length
        buf.get_i32();

        // Statement name (null-terminated)
        let stmt_name = read_cstring(&mut buf);

        // SQL (null-terminated)
        let sql = read_cstring(&mut buf);

        // Parameter types
        let param_count = buf.get_i32() as usize;
        let mut param_types = Vec::with_capacity(param_count);
        for _ in 0..param_count {
            param_types.push(buf.get_i32());
        }

        self.prepared_stmts.insert(
            stmt_name.clone(),
            PreparedStatement {
                sql,
                param_types,
                result_columns: Vec::new(),
            },
        );

        // ParseComplete
        let mut resp = BytesMut::new();
        resp.put_u8(b'1');
        resp.put_i32(4);
        resp.to_vec()
    }

    pub fn handle_bind(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::from(data);
        buf.get_i32(); // length

        // Portal name
        let _portal = read_cstring(&mut buf);

        // Statement name
        let stmt_name = read_cstring(&mut buf);

        // Parameter format codes
        let param_format_count = buf.get_i16() as usize;
        let mut param_formats = Vec::with_capacity(param_format_count);
        for _ in 0..param_format_count {
            param_formats.push(buf.get_i16());
        }

        // Parameters
        let param_count = buf.get_i16() as usize;
        let mut params = Vec::with_capacity(param_count);
        for _ in 0..param_count {
            let len = buf.get_i32();
            if len == -1 {
                params.push(None);
            } else {
                let mut param = vec![0u8; len as usize];
                let remaining = buf.remaining().min(param.len());
                param[..remaining].copy_from_slice(&buf[..remaining]);
                buf.advance(remaining);
                params.push(Some(param));
            }
        }

        // Result format codes
        let result_format_count = buf.get_i16() as usize;
        let mut result_formats = Vec::with_capacity(result_format_count);
        for _ in 0..result_format_count {
            result_formats.push(buf.get_i16());
        }

        // BindComplete
        let mut resp = BytesMut::new();
        resp.put_u8(b'2');
        resp.put_i32(4);
        resp.to_vec()
    }

    pub fn handle_describe(&self, data: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::from(data);
        buf.get_i32(); // length

        let describe_type = buf.get_u8();
        let name = read_cstring(&mut buf);

        if describe_type == b'S' {
            // Statement describe - send ParameterDescription + RowDescription
            let mut resp = BytesMut::new();

            // ParameterDescription
            resp.put_u8(b't');
            let param_count = 0i16; // TODO: get from prepared stmt
            resp.put_i32(6 + param_count as i32 * 4);
            resp.put_i16(param_count);

            // RowDescription (empty for now)
            resp.put_u8(b'T');
            resp.put_i32(6);
            resp.put_i16(0);

            resp.to_vec()
        } else {
            // Portal describe - send RowDescription
            let mut resp = BytesMut::new();
            resp.put_u8(b'T');
            resp.put_i32(6);
            resp.put_i16(0);
            resp.to_vec()
        }
    }

    pub fn handle_execute(&self, data: &[u8], engine: &Engine) -> Vec<u8> {
        let mut buf = BytesMut::from(data);
        buf.get_i32(); // length

        // Portal name
        let _portal = read_cstring(&mut buf);

        // Max rows (0 = unlimited)
        let _max_rows = buf.get_i32();

        // Get the portal's SQL from the prepared statement
        // For now, execute a simple query based on the portal
        // TODO: implement proper portal management

        // CommandComplete
        let mut resp = BytesMut::new();
        resp.put_u8(b'C');
        resp.put_slice(b"SELECT 0\0");

        resp.to_vec()
    }

    pub fn handle_close(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::from(data);
        buf.get_i32(); // length

        let close_type = buf.get_u8();
        let name = read_cstring(&mut buf);

        if close_type == b'S' {
            // Close statement
            self.prepared_stmts.remove(&name);
        }

        // CloseComplete
        let mut resp = BytesMut::new();
        resp.put_u8(b'3');
        resp.put_i32(4);
        resp.to_vec()
    }

    pub fn handle_sync(&self) -> Vec<u8> {
        let mut resp = BytesMut::new();
        resp.put_u8(b'Z');
        resp.put_i32(5);
        resp.put_u8(b'I');
        resp.to_vec()
    }

    /// Execute simple query
    pub fn execute_query(&self, sql: &str, engine: &Engine) -> Vec<u8> {
        if !self.authenticated {
            return self.build_error_response("authentication required");
        }
        match engine.execute(sql) {
            Ok(result) => encode_result_set(&result),
            Err(e) => {
                let msg = format!("ERROR: {}", e);
                let mut out = self.build_error_response(&msg);
                // Close the exchange even after a failure, or the client waits
                // for a reply that never comes.
                let mut ready = BytesMut::new();
                ready.put_u8(b'Z');
                ready.put_i32(5);
                ready.put_u8(b'I');
                out.extend_from_slice(&ready);
                out
            }
        }
    }

    pub fn build_error_response(&self, msg: &str) -> Vec<u8> {
        // An ErrorResponse is a sequence of NUL-terminated fields, so the
        // length must cover the severity field as well as the message. It used
        // to omit it, so the client read past the end of the message and lost
        // sync with the stream.
        let mut resp = BytesMut::new();
        resp.put_u8(b'E');
        resp.put_i32(4 + ERROR_SEVERITY.len() as i32 + msg.len() as i32 + 1);
        resp.put_slice(ERROR_SEVERITY);
        resp.put_slice(msg.as_bytes());
        resp.put_u8(0);
        resp.to_vec()
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn database(&self) -> &str {
        &self.database
    }
}

/// Encode query result as PG protocol messages
fn encode_result_set(result: &QueryResult) -> Vec<u8> {
    let mut resp = BytesMut::new();

    match result {
        QueryResult::Empty => {
            // The length covers itself plus the command tag and its NUL.
            let tag = b"CREATE TABLE\0";
            resp.put_u8(b'C');
            resp.put_i32(4 + tag.len() as i32);
            resp.put_slice(tag);
        }
        QueryResult::Affected(n) => {
            resp.put_u8(b'C');
            let msg = format!("INSERT 0 {}\0", n);
            resp.put_i32(4 + msg.len() as i32);
            resp.put_slice(msg.as_bytes());
        }
        QueryResult::Rows { columns, types, rows } => {
            // RowDescription
            resp.put_u8(b'T');
            let mut desc = BytesMut::new();
            desc.put_i16(columns.len() as i16);
            for (i, col) in columns.iter().enumerate() {
                let ty = types.get(i).copied().unwrap_or(DataType::String);
                desc.put_slice(col.as_bytes());
                desc.put_u8(0);
                desc.put_i32(0); // table OID
                desc.put_i16(0); // column attr
                desc.put_i32(pg_type_oid(&ty));
                desc.put_i16(-1); // varlen
                desc.put_i32(0); // format (text)
            }
            resp.put_i32(4 + desc.len() as i32);
            resp.extend_from_slice(&desc);

            // DataRow for each row. The values are sent in the text format the
            // descriptor above advertises: the stored bytes are little-endian
            // binary, so putting them on the wire raw would hand the client
            // mojibake for every number.
            for row in rows {
                resp.put_u8(b'D');
                let mut row_data = BytesMut::new();
                row_data.put_i16(row.len() as i16);
                for (i, val) in row.iter().enumerate() {
                    let ty = types.get(i).copied().unwrap_or(DataType::String);
                    match decode_text(&ty, val.as_deref()) {
                        Some(text) => {
                            row_data.put_i32(text.len() as i32);
                            row_data.extend_from_slice(text.as_bytes());
                        }
                        None => {
                            row_data.put_i32(-1);
                        }
                    }
                }
                resp.put_i32(4 + row_data.len() as i32);
                resp.extend_from_slice(&row_data);
            }

            // CommandComplete
            resp.put_u8(b'C');
            let msg = format!("SELECT {}\0", rows.len());
            resp.put_i32(4 + msg.len() as i32);
            resp.put_slice(msg.as_bytes());
        }
    }

    // ReadyForQuery
    resp.put_u8(b'Z');
    resp.put_i32(5);
    resp.put_u8(b'I');

    resp.to_vec()
}

/// PostgreSQL type OID for a stored column type.
fn pg_type_oid(ty: &DataType) -> i32 {
    match ty {
        DataType::Bool => 16,          // bool
        DataType::Int32 => 23,         // int4
        DataType::Int64 => 20,         // int8
        DataType::Timestamp => 1114,   // timestamp
        DataType::Float32 => 700,      // float4
        DataType::Float64 => 701,      // float8
        DataType::String => 25,        // text
    }
}

/// Render one stored value in the text format the descriptor advertises.
/// `None` means SQL NULL.
fn decode_text(ty: &DataType, raw: Option<&[u8]>) -> Option<String> {
    match tmdb_exec::Value::decode(ty, raw) {
        tmdb_exec::Value::Null => None,
        v => Some(v.to_text()),
    }
}

fn read_cstring(buf: &mut BytesMut) -> String {    let mut result = Vec::new();
    while buf.has_remaining() {
        let byte = buf.get_u8();
        if byte == 0 {
            break;
        }
        result.push(byte);
    }
    String::from_utf8_lossy(&result).to_string()
}

fn generate_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..24)
        .map(|_| {
            let idx = rng.gen_range(0..64);
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789./"[idx] as char
        })
        .collect()
}


pub mod scram;

#[cfg(test)]
mod scram_tests;

#[cfg(test)]
mod tests;
