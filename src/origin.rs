//! Origin exclusivity: every package is `private` or `mirror`, claimed at
//! first write via `packages/<pkg>/.origin`. Indexes never merge origins —
//! the dependency-confusion defense (DESIGN.md).

use anyhow::{anyhow, Result};

use crate::storage::{is_not_found, Storage};
use crate::PACKAGES_PREFIX;

pub const PRIVATE: &str = "private";
pub const MIRROR: &str = "mirror";

pub fn origin_key(pkg: &str) -> String {
    format!("{PACKAGES_PREFIX}{pkg}/.origin")
}

/// The package's claimed origin, if any. Storage errors propagate — treating
/// an outage as "unclaimed" would fail the exclusivity check open.
pub async fn read_origin(storage: &dyn Storage, pkg: &str) -> Result<Option<String>> {
    match storage.get_bytes(&origin_key(pkg)).await {
        Ok(bytes) => {
            let origin = String::from_utf8_lossy(&bytes).trim().to_string();
            Ok((!origin.is_empty()).then_some(origin))
        }
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Atomically claim the package for `origin`; first write wins. Returns the
/// origin that actually holds the claim — ours, or a racer's.
pub async fn claim_origin(storage: &dyn Storage, pkg: &str, origin: &str) -> Result<String> {
    let won = storage
        .put_if_absent(
            &origin_key(pkg),
            origin.as_bytes().to_vec(),
            Some("text/plain"),
        )
        .await?;
    if won {
        return Ok(origin.to_string());
    }
    read_origin(storage, pkg)
        .await?
        .ok_or_else(|| anyhow!("lost the origin claim race for '{pkg}' but no claim exists"))
}
