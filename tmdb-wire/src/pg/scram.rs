//! SCRAM-SHA-256 (RFC 5802 / RFC 7677) server side.
//!
//! Implemented directly on `sha2` so the build needs no extra crates.

use sha2::{Digest, Sha256};
use std::fmt::Write as _;

const SHA256_LEN: usize = 32;
const BLOCK_LEN: usize = 64;

/// HMAC-SHA-256 (RFC 2104).
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; SHA256_LEN] {
    let mut block = [0u8; BLOCK_LEN];
    if key.len() > BLOCK_LEN {
        let digest = Sha256::digest(key);
        block[..SHA256_LEN].copy_from_slice(&digest);
    } else {
        block[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0x36u8; BLOCK_LEN];
    let mut outer_pad = [0x5cu8; BLOCK_LEN];
    for i in 0..BLOCK_LEN {
        inner_pad[i] ^= block[i];
        outer_pad[i] ^= block[i];
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(msg);
    let inner = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner);
    outer.finalize().into()
}

/// PBKDF2-HMAC-SHA-256, truncated to the 32-byte hash length.
///
/// Only the first output block is needed: `dkLen == hLen == 32`, so
/// `T_1` is the whole derived key.
pub fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; SHA256_LEN] {
    // U_1 = PRF(P, S || INT(32, 1))
    let mut first = Vec::with_capacity(salt.len() + 4);
    first.extend_from_slice(salt);
    first.extend_from_slice(&1u32.to_be_bytes());

    let mut u = hmac_sha256(password, &first);
    let mut out = u;

    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for i in 0..SHA256_LEN {
            out[i] ^= u[i];
        }
    }
    out
}

/// RFC 4648 base64, with padding.
pub fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(CHARS[(triple >> 18) as usize & 0x3F] as char);
        out.push(CHARS[(triple >> 12) as usize & 0x3F] as char);
        out.push(if chunk.len() > 1 {
            CHARS[(triple >> 6) as usize & 0x3F] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            CHARS[triple as usize & 0x3F] as char
        } else {
            '='
        });
    }
    out
}

/// RFC 4648 base64 decode, rejecting anything not canonical.
pub fn base64_decode(data: &str) -> Result<Vec<u8>, Error> {
    fn value(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a') as u32 + 26),
            b'0'..=b'9' => Some((c - b'0') as u32 + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = data.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err(Error::Malformed("base64 length is not a multiple of four"));
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let pad = chunk.iter().rev().take_while(|b| **b == b'=').count();
        if pad > 2 {
            return Err(Error::Malformed("too much base64 padding"));
        }
        let mut acc = 0u32;
        for (i, c) in chunk.iter().enumerate() {
            let v = if *c == b'=' {
                if i < 4 - pad {
                    return Err(Error::Malformed("padding in the middle of base64"));
                }
                0
            } else {
                value(*c).ok_or(Error::Malformed("invalid base64 character"))?
            };
            acc = (acc << 6) | v;
        }
        let triple = acc.to_be_bytes();
        out.push(triple[1]);
        if pad < 2 {
            out.push(triple[2]);
        }
        if pad < 1 {
            out.push(triple[3]);
        }
    }
    Ok(out)
}

/// Length-independent equality, so a caller cannot learn the expected proof
/// byte by byte from how long the comparison takes.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Malformed(&'static str),
    UnsupportedChannelBinding,
    UnknownUser,
    NonAsciiPassword,
    WrongPassword,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Malformed(what) => write!(f, "malformed SCRAM message: {}", what),
            Error::UnsupportedChannelBinding => write!(f, "unsupported channel binding"),
            // These three must read identically: a distinct message for an
            // unknown account would let an attacker enumerate users.
            Error::UnknownUser | Error::WrongPassword | Error::NonAsciiPassword => {
                write!(f, "authentication failed")
            }
        }
    }
}

impl std::error::Error for Error {}

/// Long-lived per-user material.
///
/// This is the salted password, not the password itself. Keeping it means a
/// connection costs one HMAC per proof instead of a full PBKDF2, and lets the
/// iteration count be raised later without invalidating stored entries.
///
/// It is not a defence against an offline dictionary attack on a stolen file:
/// PBKDF2 can still be run from `salted_password` backwards at the recorded
/// cost. What it does buy is that the file cannot be replayed as a credential
/// against anything else, and no recoverable plaintext sits on disk.
#[derive(Clone, Debug)]
pub struct Verifier {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub salted_password: [u8; SHA256_LEN],
}

impl Verifier {
    pub fn from_password(password: &[u8], salt: Vec<u8>, iterations: u32) -> Self {
        Verifier {
            salted_password: pbkdf2_hmac_sha256(password, &salt, iterations),
            salt,
            iterations,
        }
    }

    /// Derive a verifier, rejecting anything this build cannot normalise.
    ///
    /// SASLprep (RFC 4013) maps some non-ASCII code points onto others, so a
    /// client that prepares a password and a server that does not derive
    /// different keys and the exchange fails with no usable explanation. The
    /// full RFC 3454 tables are not carried here, so rather than disagree
    /// quietly this refuses non-ASCII outright and names the limitation.
    pub fn derive(
        password: &str,
        salt: Vec<u8>,
        iterations: u32,
    ) -> Result<Self, Error> {
        if !password.is_ascii() {
            return Err(Error::NonAsciiPassword);
        }
        Ok(Verifier::from_password(password.as_bytes(), salt, iterations))
    }
}

/// Server-side SCRAM exchange state, one instance per connection.
#[derive(Debug)]
pub struct Server {
    client_first_bare: String,
    server_first: String,
    server_nonce: String,
    salted_password: [u8; SHA256_LEN],
    iterations: u32,
}

impl Server {
    /// Consume the client-first-message and produce the server-first-message.
    pub fn begin(
        client_first: &str,
        verifier: &Verifier,
        server_nonce: &str,
    ) -> Result<(Self, String), Error> {
        // "n,," is the GS2 header for a channel-binding-less client.
        let client_first_bare = match client_first.strip_prefix("n,,") {
            Some(rest) => rest,
            None => match client_first.strip_prefix("y,,") {
                Some(rest) => rest,
                None => {
                    return Err(Error::UnsupportedChannelBinding);
                }
            },
        };

        let attrs = parse_scram_attributes(client_first_bare)?;
        let client_nonce = get(&attrs, "r").ok_or(Error::Malformed("missing client nonce"))?;
        if client_nonce.is_empty() {
            return Err(Error::Malformed("empty client nonce"));
        }

        let salted_password = verifier.salted_password;

        let mut server_first = String::new();
        write!(
            server_first,
            "r={}{},s={},i={}",
            client_nonce,
            server_nonce,
            base64_encode(&verifier.salt),
            verifier.iterations
        )
        .unwrap();

        Ok((
            Server {
                // AuthMessage uses the bare form: the GS2 header is excluded.
                client_first_bare: client_first_bare.to_string(),
                server_first: server_first.clone(),
                server_nonce: server_first
                    .split(',')
                    .next()
                    .and_then(|a| a.strip_prefix("r="))
                    .unwrap_or_default()
                    .to_string(),
                salted_password,
                iterations: verifier.iterations,
            },
            server_first,
        ))
    }

    /// Consume the client-final-message, verify the proof and return the
    /// server-final-message.
    pub fn finish(&self, client_final: &str) -> Result<String, Error> {
        let (proof, without_proof) = match client_final.rfind(",p=") {
            Some(idx) => (&client_final[idx + 3..], &client_final[..idx]),
            None => return Err(Error::Malformed("missing proof")),
        };
        if proof.is_empty() {
            return Err(Error::Malformed("empty proof"));
        }

        let attrs = parse_scram_attributes(without_proof)?;

        // Channel binding must match the GS2 header we accepted. Accepting any
        // other value would let a downgrade force the client to prove nothing
        // about the channel.
        let channel_binding = get(&attrs, "c").ok_or(Error::Malformed("missing channel binding"))?;
        if channel_binding != "biws" {
            return Err(Error::UnsupportedChannelBinding);
        }

        let client_nonce = get(&attrs, "r").ok_or(Error::Malformed("missing client nonce"))?;
        // A conforming client sends back the nonce we issued verbatim, so equal
        // lengths are the normal case. It may also append more, which the
        // proof still covers, but it must never shorten or replace it.
        if !client_nonce.starts_with(&self.server_nonce) {
            return Err(Error::Malformed("client nonce does not extend ours"));
        }

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );

        let client_key = hmac_sha256(&self.salted_password, b"Client Key");
        let stored_key: [u8; SHA256_LEN] = Sha256::digest(client_key).into();

        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut expected_proof = client_key;
        for i in 0..SHA256_LEN {
            expected_proof[i] ^= client_signature[i];
        }

        if !constant_time_eq(base64_encode(&expected_proof).as_bytes(), proof.as_bytes()) {
            return Err(Error::WrongPassword);
        }

        let server_key = hmac_sha256(&self.salted_password, b"Server Key");
        let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());

        Ok(format!("v={}", base64_encode(&server_signature)))
    }

    pub fn iterations(&self) -> u32 {
        self.iterations
    }
}

fn parse_scram_attributes(message: &str) -> Result<Vec<(String, String)>, Error> {
    let mut out = Vec::new();
    for part in message.split(',') {
        if part.len() < 2 || part.as_bytes()[1] != b'=' {
            return Err(Error::Malformed("attribute must be key=value"));
        }
        out.push((part[..1].to_string(), part[2..].to_string()));
    }
    if out.is_empty() {
        return Err(Error::Malformed("no attributes"));
    }
    Ok(out)
}

fn get<'a>(attrs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}
