//! Stateless, short-lived install tokens.
//!
//! A token is the server's signed assertion: "I issued a credential for this
//! role, carrying this attribution, valid until `exp`." It is self-contained —
//! the expiry lives inside the token and is checked on read — so tokens never
//! touch disk and there is nothing to garbage-collect (cf. the other reserved
//! prefixes, which are all designed to avoid a time-based sweep).
//!
//! Wire shape: `pypiron-<payload>.<mac>`, where `<payload>` is base64url(JSON
//! claims) and `<mac>` is base64url(HMAC-SHA256(signing-key, payload)). It is a
//! JWT in spirit (HS256 + `exp`) but implemented as ~a dozen lines over `sha2`
//! rather than pulling in a JWT crate — unforgeability comes from the
//! operator's signing key, so no per-token randomness (and no CSPRNG dep) is
//! needed.
//!
//! The signature authenticates the *grant* (this is a real, unexpired token the
//! server minted), not the truthfulness of the self-reported attribution.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as b64url, Engine};
use serde::{Deserialize, Serialize};

/// The conventional Basic-auth username that signals token mode. Matches the
/// PyPI ecosystem convention (`__token__` / `pypi-...`) that pip/uv/twine speak
/// natively, so a client needs no special configuration to present one.
pub const TOKEN_USERNAME: &str = "__token__";

const PREFIX: &str = "pypiron-";

/// Access tier a token grants. Ordering is meaningful: admin ⊇ uploader ⊇
/// reader, so a token's role can be compared against a required minimum.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Reader,
    Uploader,
    Admin,
}

impl Role {
    pub fn parse(s: &str) -> Option<Role> {
        match s {
            "reader" => Some(Role::Reader),
            "uploader" => Some(Role::Uploader),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Reader => "reader",
            Role::Uploader => "uploader",
            Role::Admin => "admin",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What the server signs. Attribution fields are gathered freely at mint time
/// (what we collect is independent of where it is later routed); they are
/// omitted from the payload when absent to keep tokens compact.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    pub role: Role,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Issued-at, unix seconds.
    pub iat: i64,
    /// Expiry, unix seconds. Verification rejects `now >= exp`.
    pub exp: i64,
}

/// HMAC-SHA256 (RFC 2104). Block size 64 bytes; keys longer than the block are
/// hashed down first. ~a dozen lines so we don't add an `hmac` dependency.
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
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

/// Encode + sign a token. Fails only if the claims can't be serialized (they
/// can't, in practice — a struct of strings and ints — but the caller threads
/// the error rather than panicking on a request path).
pub fn mint(signing_key: &str, claims: &Claims) -> anyhow::Result<String> {
    let json = serde_json::to_vec(claims)?;
    let payload = b64url.encode(json);
    let mac = b64url.encode(hmac_sha256(signing_key.as_bytes(), payload.as_bytes()));
    Ok(format!("{PREFIX}{payload}.{mac}"))
}

/// Verify a token against the signing key and the current time. Returns the
/// claims only if the prefix, signature, encoding, and expiry all check out;
/// any failure is `None` (fail closed — a bad token never authenticates).
pub fn verify(signing_key: &str, token: &str, now_unix: i64) -> Option<Claims> {
    let body = token.strip_prefix(PREFIX)?;
    let (payload, mac) = body.split_once('.')?;
    let expected = b64url.encode(hmac_sha256(signing_key.as_bytes(), payload.as_bytes()));
    // Constant-time compare so a forged token can't be tuned byte-by-byte.
    if !crate::ct_eq(mac, &expected) {
        return None;
    }
    let claims: Claims = serde_json::from_slice(&b64url.decode(payload).ok()?).ok()?;
    if now_unix >= claims.exp {
        return None;
    }
    Some(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(role: Role, exp: i64) -> Claims {
        Claims {
            role,
            repo: Some("github.com/acme/widgets".into()),
            commit: Some("abc1234".into()),
            user: Some("bryce".into()),
            iat: 1000,
            exp,
        }
    }

    #[test]
    fn mint_then_verify_round_trips() {
        let t = mint("key", &claims(Role::Reader, 2000)).unwrap();
        let got = verify("key", &t, 1500).expect("valid token");
        assert_eq!(got, claims(Role::Reader, 2000));
        assert!(t.starts_with("pypiron-"));
    }

    #[test]
    fn wrong_key_is_rejected() {
        let t = mint("key", &claims(Role::Admin, 2000)).unwrap();
        assert!(verify("other-key", &t, 1500).is_none());
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let t = mint("key", &claims(Role::Reader, 2000)).unwrap();
        // Forge an admin payload but keep the original mac.
        let forged_payload = b64url.encode(serde_json::to_vec(&claims(Role::Admin, 2000)).unwrap());
        let mac = t.rsplit_once('.').unwrap().1;
        let forged = format!("pypiron-{forged_payload}.{mac}");
        assert!(verify("key", &forged, 1500).is_none());
    }

    #[test]
    fn expired_is_rejected() {
        let t = mint("key", &claims(Role::Reader, 2000)).unwrap();
        // exp is exclusive: at exactly exp the token is dead.
        assert!(verify("key", &t, 2000).is_none());
        assert!(verify("key", &t, 2001).is_none());
        assert!(verify("key", &t, 1999).is_some());
    }

    #[test]
    fn malformed_tokens_are_rejected() {
        assert!(verify("key", "not-a-token", 1500).is_none());
        assert!(verify("key", "pypiron-nodot", 1500).is_none());
        assert!(verify("key", "pypiron-@@@.@@@", 1500).is_none());
    }

    #[test]
    fn role_ordering_supports_minimum_checks() {
        assert!(Role::Admin > Role::Uploader);
        assert!(Role::Uploader > Role::Reader);
        assert!(Some(Role::Admin) >= Some(Role::Uploader));
        assert!(Some(Role::Reader) < Some(Role::Uploader));
        assert!(Option::<Role>::None < Some(Role::Reader));
    }

    #[test]
    fn long_signing_key_is_hashed_down() {
        let key = "x".repeat(200);
        let t = mint(&key, &claims(Role::Reader, 2000)).unwrap();
        assert!(verify(&key, &t, 1500).is_some());
    }
}
