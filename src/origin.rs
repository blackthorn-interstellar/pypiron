//! Origin exclusivity: every package is `private` or `mirror`, claimed at
//! first write via `packages/<pkg>/.origin`. Indexes never merge origins —
//! the dependency-confusion defense (dev/DESIGN.md).

use anyhow::{anyhow, Result};
use tracing::warn;

use crate::sidecar::is_artifact;
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

/// Atomically claim the package for `origin`; first write wins. Returns
/// `(created, owner)`: `created` is true only when THIS call wrote the marker,
/// and `owner` is the origin that actually holds the claim — ours, or a racer's.
///
/// The `created` flag matters: a caller that merely read back a peer's fresh
/// claim must not believe it owns it, or it could later "release" the peer's
/// live claim out from under an in-flight download.
pub async fn claim_origin(
    storage: &dyn Storage,
    pkg: &str,
    origin: &str,
) -> Result<(bool, String)> {
    let won = storage
        .put_if_absent(
            &origin_key(pkg),
            origin.as_bytes().to_vec(),
            Some("text/plain"),
        )
        .await?;
    if won {
        return Ok((true, origin.to_string()));
    }
    let owner = read_origin(storage, pkg)
        .await?
        .ok_or_else(|| anyhow!("lost the origin claim race for '{pkg}' but no claim exists"))?;
    Ok((false, owner))
}

/// Remove our orphan `.origin` claim if the package holds no artifacts — a
/// failed first write (sync or proxy) must not block the name forever.
pub async fn release_empty_claim(storage: &dyn Storage, pkg: &str) {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    match storage.list_dir_entries(&prefix).await {
        Ok(entries) => {
            let has_artifact = entries
                .iter()
                .any(|e| e.key.strip_prefix(&prefix).is_some_and(is_artifact));
            if !has_artifact {
                let _ = storage.delete_keys(&[origin_key(pkg)]).await;
            }
        }
        Err(e) => warn!(package=%pkg, error=?e, "could not check for orphan claim"),
    }
}
