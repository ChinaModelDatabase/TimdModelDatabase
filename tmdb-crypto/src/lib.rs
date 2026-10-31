/// SM2/SM3/SM4 national cryptography.

use sha2::{Sha256, Digest};

/// SM3-like hash (placeholder - real SM3 needs a dedicated crate)
pub fn sm3_hash(data: &[u8]) -> [u8; 32] {
    // Placeholder: use SHA-256 for now
    // Real implementation should use sm3 crate
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

/// SM4 encryption mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sm4Mode {
    Ecb,
    Cbc,
}

/// SM4 encryption (placeholder)
pub fn sm4_encrypt(key: &[u8; 16], data: &[u8], _mode: Sm4Mode) -> Vec<u8> {
    // Placeholder: XOR with key for testing
    // Real implementation should use sm4 crate
    data.iter()
        .zip(key.iter().cycle())
        .map(|(d, k)| d ^ k)
        .collect()
}

/// SM4 decryption (placeholder)
pub fn sm4_decrypt(key: &[u8; 16], data: &[u8], mode: Sm4Mode) -> Vec<u8> {
    // XOR is its own inverse
    sm4_encrypt(key, data, mode)
}

/// TLS configuration using national cryptography
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub use_sm: bool,
}

impl TlsConfig {
    pub fn new(cert_path: String, key_path: String, use_sm: bool) -> Self {
        Self {
            cert_path,
            key_path,
            use_sm,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sm3_hash() {
        let hash = sm3_hash(b"hello");
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_sm4_roundtrip() {
        let key = [0x01u8; 16];
        let data = b"hello world";
        let encrypted = sm4_encrypt(&key, data, Sm4Mode::Ecb);
        let decrypted = sm4_decrypt(&key, &encrypted, Sm4Mode::Ecb);
        assert_eq!(decrypted, data);
    }
}
