//! Origin exclusivity: every package is `private` or `mirror`, claimed at
//! first write via `packages/<pkg>/.origin`. Indexes never merge origins —
//! the dependency-confusion defense (dev/DESIGN.md).
//!
//! The claim is a **never-deleted** object with a monotone lattice of states
//! (dev/MULTIBUCKET.md §6.2). Legal transitions, all conditional (CAS):
//!
//! ```text
//!   (absent) --claim--> private          (create-if-absent)
//!   (absent) --claim--> mirror           (create-if-absent)
//!  unclaimed --claim--> private|mirror    (put-if-match sentinel)
//!     mirror --demote-> private           (put-if-match, the only demotion)
//!     mirror --release-> unclaimed        (put-if-match, orphan cleanup)
//!    private is terminal
//! ```
//!
//! The claim never transits through *absent* after creation: absent is what
//! authorizes a proxy to fill from upstream, so releasing a failed claim
//! rewrites it to the `unclaimed` sentinel (which `read_origin` folds into
//! "may claim") rather than deleting it. An unconditional delete could erase a
//! claim that went `private` mid-flight, re-opening the dependency-confusion
//! window.

use anyhow::{anyhow, Result};
use tracing::warn;

use crate::sidecar::is_artifact;
use crate::storage::{is_not_found, Storage};
use crate::PACKAGES_PREFIX;

pub const PRIVATE: &str = "private";
pub const MIRROR: &str = "mirror";
/// Sentinel written by a released claim: the object exists (so it never falls
/// back to the absent "proxy may fill" state) but claims nothing, so the next
/// writer may CAS it to a real origin.
pub const UNCLAIMED: &str = "unclaimed";

/// Bound on the sentinel-CAS retry loop in [`claim_origin`]. One or two
/// iterations is the realistic worst case (a concurrent release then re-claim);
/// the cap only exists so a pathological storm fails closed instead of spinning.
const CLAIM_ATTEMPTS: usize = 8;

pub fn origin_key(pkg: &str) -> String {
    format!("{PACKAGES_PREFIX}{pkg}/.origin")
}

/// The package's claimed origin, if any. `None` means "no claim holds this
/// name" — a missing object *or* the `unclaimed` sentinel, both of which permit
/// a proxy fill. Storage errors propagate: treating an outage as "unclaimed"
/// would fail the exclusivity check open.
pub async fn read_origin(storage: &dyn Storage, pkg: &str) -> Result<Option<String>> {
    match storage.get_bytes(&origin_key(pkg)).await {
        Ok(bytes) => Ok(claimed_owner(&bytes)),
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// The raw claim content and its etag, or `None` if the object is truly absent.
/// Unlike [`read_origin`] this does *not* fold `unclaimed` into `None` — a CAS
/// caller needs the sentinel's etag to overwrite it, and the pre-commit re-check
/// needs to distinguish "still my exact mirror claim" from "moved on".
pub async fn read_origin_versioned(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<Option<(String, String)>> {
    match storage.get_with_etag(&origin_key(pkg)).await? {
        Some((bytes, etag)) => Ok(Some((
            String::from_utf8_lossy(&bytes).trim().to_string(),
            etag,
        ))),
        None => Ok(None),
    }
}

/// Interpret raw claim bytes: the trimmed owner, or `None` for empty/unclaimed.
fn claimed_owner(bytes: &[u8]) -> Option<String> {
    let s = String::from_utf8_lossy(bytes).trim().to_string();
    (!s.is_empty() && s != UNCLAIMED).then_some(s)
}

/// The result of a claim attempt.
pub struct Claim {
    /// True only when THIS call established the claim — it created the object or
    /// rewrote the `unclaimed` sentinel. Only the establisher may later release
    /// it, so a caller that merely read back a peer's fresh claim must not.
    pub created: bool,
    /// The origin that now holds the claim: ours if `created`, else a racer's.
    pub owner: String,
    /// The claim object's etag *when this call created it* — the token
    /// [`release_empty_claim`] and the proxy's pre-commit re-check need to CAS
    /// their own exact claim. `None` when we did not create it.
    pub etag: Option<String>,
}

/// Atomically claim the package for `origin`, first write wins.
///
/// Create-if-absent for a missing object; a `put-if-match` on the `unclaimed`
/// sentinel otherwise. A real `private`/`mirror` claim already present ends the
/// call with `created = false` and that claim's owner.
pub async fn claim_origin(storage: &dyn Storage, pkg: &str, origin: &str) -> Result<Claim> {
    let key = origin_key(pkg);
    for _ in 0..CLAIM_ATTEMPTS {
        // Fast path: create the claim object if it does not exist yet.
        if let Some(etag) = storage
            .put_if_none_match(&key, origin.as_bytes().to_vec())
            .await?
        {
            return Ok(Claim {
                created: true,
                owner: origin.to_string(),
                etag: Some(etag),
            });
        }
        // The object exists. Read it with its etag to decide the transition.
        let (content, etag) = read_origin_versioned(storage, pkg)
            .await?
            .ok_or_else(|| anyhow!("origin claim for '{pkg}' vanished mid-claim"))?;
        if content == UNCLAIMED {
            // CAS the sentinel to our origin; a lost race re-reads and retries.
            match storage
                .put_if_match(&key, &etag, origin.as_bytes().to_vec())
                .await?
            {
                Some(new_etag) => {
                    return Ok(Claim {
                        created: true,
                        owner: origin.to_string(),
                        etag: Some(new_etag),
                    })
                }
                None => continue,
            }
        }
        // A real claim holds the name (ours or a racer's) — never ours to release.
        return Ok(Claim {
            created: false,
            owner: content,
            etag: None,
        });
    }
    Err(anyhow!(
        "could not settle the origin claim for '{pkg}' after {CLAIM_ATTEMPTS} attempts"
    ))
}

/// The only legal demotion (dev/MULTIBUCKET.md §6.2): CAS a `mirror` claim to
/// `private`. `expected_etag` is the mirror claim's etag; `Some(new_etag)` on
/// success, `None` if the claim has moved on (a stale demoter loses). Wired by
/// the P3 merge to make a name private when private truth arrives from a peer;
/// landed and unit-tested now so the origin lifecycle is complete and proven.
#[allow(dead_code)] // P3: consumed by the reconcile merge
pub async fn demote_mirror_to_private(
    storage: &dyn Storage,
    pkg: &str,
    expected_etag: &str,
) -> Result<Option<String>> {
    storage
        .put_if_match(&origin_key(pkg), expected_etag, PRIVATE.as_bytes().to_vec())
        .await
}

/// Release our orphan `mirror` claim if the package holds no artifacts — a
/// failed first write (sync or proxy) must not block the name forever. CAS the
/// claim back to the `unclaimed` sentinel *only if* it is still the exact claim
/// we created (`expected_etag`): a claim that went `private` (demotion) or was
/// re-claimed in the meantime no longer matches, so the stale releaser loses and
/// the live claim is preserved. Never an unconditional delete (which could erase
/// a now-private claim) and never a delete to absent (which would re-authorize a
/// proxy fill of a name we may no longer own).
pub async fn release_empty_claim(storage: &dyn Storage, pkg: &str, expected_etag: &str) {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    match storage.list_dir_entries(&prefix).await {
        Ok(entries) => {
            let has_artifact = entries
                .iter()
                .any(|e| e.key.strip_prefix(&prefix).is_some_and(is_artifact));
            if !has_artifact {
                if let Err(e) = storage
                    .put_if_match(
                        &origin_key(pkg),
                        expected_etag,
                        UNCLAIMED.as_bytes().to_vec(),
                    )
                    .await
                {
                    warn!(package=%pkg, error=?e, "could not release orphan claim");
                }
            }
        }
        Err(e) => warn!(package=%pkg, error=?e, "could not check for orphan claim"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::InMemStorage;
    use std::sync::Arc;

    fn store() -> Arc<InMemStorage> {
        Arc::new(InMemStorage::default())
    }

    #[tokio::test]
    async fn first_claim_creates_and_reports_owner() {
        let s = store();
        let claim = claim_origin(s.as_ref(), "pkg", PRIVATE).await.unwrap();
        assert!(claim.created);
        assert_eq!(claim.owner, PRIVATE);
        assert!(claim.etag.is_some());
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
    }

    #[tokio::test]
    async fn second_claim_reports_incumbent_and_yields_no_etag() {
        let s = store();
        claim_origin(s.as_ref(), "pkg", PRIVATE).await.unwrap();
        let claim = claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        assert!(!claim.created, "must not claim an already-owned name");
        assert_eq!(claim.owner, PRIVATE, "the incumbent wins");
        assert!(claim.etag.is_none(), "non-creator holds no release token");
    }

    #[tokio::test]
    async fn release_rewrites_to_unclaimed_not_absent() {
        let s = store();
        let claim = claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        release_empty_claim(s.as_ref(), "pkg", claim.etag.as_deref().unwrap()).await;
        // read_origin folds the sentinel into "unclaimed" so a proxy may fill…
        assert_eq!(read_origin(s.as_ref(), "pkg").await.unwrap(), None);
        // …but the object still exists (never absent), carrying the sentinel.
        let (content, _) = read_origin_versioned(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(content, UNCLAIMED);
    }

    #[tokio::test]
    async fn unclaimed_can_be_reclaimed_by_the_next_writer() {
        let s = store();
        let first = claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        release_empty_claim(s.as_ref(), "pkg", first.etag.as_deref().unwrap()).await;
        // A private upload now legitimately claims the released name.
        let second = claim_origin(s.as_ref(), "pkg", PRIVATE).await.unwrap();
        assert!(second.created);
        assert_eq!(second.owner, PRIVATE);
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
    }

    #[tokio::test]
    async fn release_does_not_run_when_artifacts_remain() {
        let s = store();
        let claim = claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        s.insert("packages/pkg/pkg-1.0-py3-none-any.whl", b"bytes".to_vec());
        release_empty_claim(s.as_ref(), "pkg", claim.etag.as_deref().unwrap()).await;
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(MIRROR)
        );
    }

    #[tokio::test]
    async fn stale_releaser_loses_after_demotion() {
        // The ABA the CAS closes: a proxy claims mirror, the claim is demoted to
        // private (a merge or a racing private world), and the proxy's late
        // release must NOT clobber the now-private claim.
        let s = store();
        let claim = claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        let mirror_etag = claim.etag.unwrap();
        let demoted = demote_mirror_to_private(s.as_ref(), "pkg", &mirror_etag)
            .await
            .unwrap();
        assert!(
            demoted.is_some(),
            "demotion CAS must succeed on a fresh mirror claim"
        );
        // The proxy, still holding the stale mirror etag, tries to release.
        release_empty_claim(s.as_ref(), "pkg", &mirror_etag).await;
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE),
            "the stale releaser must lose; private is terminal"
        );
    }

    #[tokio::test]
    async fn demotion_consumes_the_mirror_etag_so_a_replay_no_ops() {
        // Demotion is idempotent under CAS: the first mirror→private wins and
        // burns the mirror etag; any replay with that stale etag no-ops, so two
        // mergers racing the same demotion cannot double-apply or thrash.
        let s = store();
        let claim = claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        let mirror_etag = claim.etag.unwrap();
        assert!(demote_mirror_to_private(s.as_ref(), "pkg", &mirror_etag)
            .await
            .unwrap()
            .is_some());
        assert!(
            demote_mirror_to_private(s.as_ref(), "pkg", &mirror_etag)
                .await
                .unwrap()
                .is_none(),
            "a replay with the stale mirror etag must lose"
        );
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE),
            "private is terminal"
        );
    }

    #[tokio::test]
    async fn read_origin_propagates_storage_errors() {
        let s = store();
        claim_origin(s.as_ref(), "pkg", PRIVATE).await.unwrap();
        s.fail_next_get();
        assert!(
            read_origin(s.as_ref(), "pkg").await.is_err(),
            "an outage must not read as unclaimed"
        );
    }
}
