//! One-shot content hashing shared across modules.

use sha2::{Digest, Sha256};

/// Lowercase hex sha256 of arbitrary bytes.
pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// HMAC-SHA256 (RFC 2104). Block size 64 bytes; keys longer than the block are
/// hashed down first. ~a dozen lines so we don't add an `hmac` dependency — the
/// same reason [`crate::reqsign`] hand-rolls request signing instead of pulling
/// an AWS/Azure SDK.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0u8; 64];
    let mut opad = [0u8; 64];
    for i in 0..64 {
        ipad[i] = block[i] ^ 0x36;
        opad[i] = block[i] ^ 0x5c;
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(msg)
        .finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(
        &Sha256::new()
            .chain_update(opad)
            .chain_update(inner)
            .finalize(),
    );
    out
}
