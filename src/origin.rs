//! Origin exclusivity: every package is `private` or `mirror`, claimed at
//! first write via `packages/<pkg>/.origin`. Indexes never merge origins —
//! the dependency-confusion defense (DESIGN.md).

use anyhow::Result;

use crate::storage::Storage;
use crate::PACKAGES_PREFIX;

pub const PRIVATE: &str = "private";
pub const MIRROR: &str = "mirror";

pub fn origin_key(pkg: &str) -> String {
    format!("{PACKAGES_PREFIX}{pkg}/.origin")
}

/// The package's claimed origin, if any.
pub async fn read_origin(storage: &dyn Storage, pkg: &str) -> Option<String> {
    let bytes = storage.get_bytes(&origin_key(pkg)).await.ok()?;
    let origin = String::from_utf8(bytes).ok()?.trim().to_string();
    (!origin.is_empty()).then_some(origin)
}

/// Claim the package for an origin (caller has already checked exclusivity).
pub async fn claim_origin(storage: &dyn Storage, pkg: &str, origin: &str) -> Result<()> {
    storage
        .put_bytes(
            &origin_key(pkg),
            origin.as_bytes().to_vec(),
            Some("text/plain"),
        )
        .await
}
