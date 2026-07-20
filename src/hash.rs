//! One-shot content hashing shared across modules.

use sha2::{Digest, Sha256};

/// Lowercase hex sha256 of arbitrary bytes.
pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
