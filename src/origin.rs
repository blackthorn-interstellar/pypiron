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

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use tracing::warn;

use crate::storage::{is_not_found, Storage};
use crate::{DIRTY_PREFIX, PACKAGES_PREFIX};

pub const PRIVATE: &str = "private";
pub const MIRROR: &str = "mirror";
/// Sentinel written by a released claim: the object exists (so it never falls
/// back to the absent "proxy may fill" state) but claims nothing, so the next
/// writer may CAS it to a real origin.
pub const UNCLAIMED: &str = "unclaimed";

/// A parsed origin-claim state. Callers that will CAS use this together with
/// [`OriginObservation::etag`], never an etag stripped of the state it proved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OriginState {
    Unclaimed,
    Mirror,
    Private,
}

impl OriginState {
    pub fn as_str(self) -> &'static str {
        match self {
            OriginState::Unclaimed => UNCLAIMED,
            OriginState::Mirror => MIRROR,
            OriginState::Private => PRIVATE,
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            UNCLAIMED => Ok(OriginState::Unclaimed),
            MIRROR => Ok(OriginState::Mirror),
            PRIVATE => Ok(OriginState::Private),
            _ => bail!("invalid origin claim state '{value}'"),
        }
    }

    fn claim_target(value: &str) -> Result<Self> {
        match Self::parse(value)? {
            OriginState::Mirror => Ok(OriginState::Mirror),
            OriginState::Private => Ok(OriginState::Private),
            OriginState::Unclaimed => bail!("cannot claim a package as unclaimed"),
        }
    }
}

/// One state observation and the exact version token that proved it. Keeping the
/// two together prevents a caller from accidentally using a private/unclaimed
/// etag as authority for a mirror-only transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginObservation {
    pub state: OriginState,
    pub etag: String,
    /// A committed replication manifest currently owns the package. The
    /// package remains logically private, but request-path mutations must wait
    /// until that exact manifest has finished promotion.
    pub pending_manifest: Option<String>,
}

/// Input to [`claim_origin`]. Existing callers pass an origin string. A caller
/// that already observed the `unclaimed` sentinel can also pass that observation
/// and go straight to CAS, skipping a guaranteed-losing create-if-absent.
#[derive(Clone, Copy)]
pub struct ClaimRequest<'a> {
    origin: &'a str,
    unclaimed: Option<&'a OriginObservation>,
}

impl<'a> ClaimRequest<'a> {
    pub fn new(origin: &'a str, unclaimed: Option<&'a OriginObservation>) -> Self {
        Self { origin, unclaimed }
    }
}

impl<'a> From<&'a str> for ClaimRequest<'a> {
    fn from(origin: &'a str) -> Self {
        Self::new(origin, None)
    }
}

/// On-disk claim body. A fresh nonce makes every state write a distinct object
/// version even on disk, whose etags are content hashes. Without it, a
/// mirror -> unclaimed -> mirror ABA recreates the old etag and revives stale
/// CAS authority.
#[derive(Debug, Deserialize, Serialize)]
struct ClaimBody {
    origin: String,
    nonce: String,
    #[serde(
        rename = "pending-manifest",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pending_manifest: Option<String>,
}

/// Generate a 128-bit, lowercase-hex nonce without another dependency. Each
/// `RandomState` carries fresh process randomness; the clock/counter inputs also
/// make successive calls distinct even on platforms with coarse clocks.
fn fresh_nonce() -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = u64::from(std::process::id());

    let mut high = RandomState::new().build_hasher();
    high.write_u128(now);
    high.write_u64(sequence);
    high.write_u64(pid);

    let mut low = RandomState::new().build_hasher();
    low.write_u128(now);
    low.write_u64(!sequence);
    low.write_u64(pid.rotate_left(17));

    format!("{:016x}{:016x}", high.finish(), low.finish())
}

fn encode_claim(state: OriginState) -> Result<Vec<u8>> {
    encode_claim_with_pending(state, None)
}

fn encode_claim_with_pending(
    state: OriginState,
    pending_manifest: Option<&str>,
) -> Result<Vec<u8>> {
    if pending_manifest.is_some() && state != OriginState::Private {
        bail!("only a private origin claim may carry a pending manifest");
    }
    Ok(serde_json::to_vec(&ClaimBody {
        origin: state.as_str().to_string(),
        nonce: fresh_nonce(),
        pending_manifest: pending_manifest.map(str::to_string),
    })?)
}

struct DecodedClaim {
    state: OriginState,
    pending_manifest: Option<String>,
}

/// Parse the nonce-bearing JSON format and the pre-P2.5 plain-text format.
/// Malformed JSON and unknown states fail closed.
fn decode_claim(bytes: &[u8]) -> Result<DecodedClaim> {
    let text = std::str::from_utf8(bytes)?.trim();
    if text.starts_with('{') {
        let body: ClaimBody = serde_json::from_str(text)?;
        if body.nonce.len() != 32 || !body.nonce.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("origin claim nonce must be 128-bit hex");
        }
        let state = OriginState::parse(&body.origin)?;
        if body.pending_manifest.is_some() && state != OriginState::Private {
            bail!("only a private origin claim may carry a pending manifest");
        }
        Ok(DecodedClaim {
            state,
            pending_manifest: body.pending_manifest,
        })
    } else {
        Ok(DecodedClaim {
            state: OriginState::parse(text)?,
            pending_manifest: None,
        })
    }
}

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
    Ok(match read_origin_claim(storage, pkg).await? {
        Some((OriginState::Unclaimed, _)) | None => None,
        Some((state, _)) => Some(state.as_str().to_string()),
    })
}

/// Read the logical claim plus its pending owner without requiring conditional
/// storage support. Index rebuilds need the pending bit and the pre-CAS origin
/// classification, but never use this view as CAS authority.
pub async fn read_origin_claim(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<Option<(OriginState, Option<String>)>> {
    match storage.get_bytes(&origin_key(pkg)).await {
        Ok(bytes) => {
            let decoded = decode_claim(&bytes)?;
            Ok(Some((decoded.state, decoded.pending_manifest)))
        }
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// The parsed claim state and the etag that proved it, or `None` if the object is
/// truly absent. Unlike [`read_origin`], this preserves `unclaimed`: a CAS caller
/// needs both its state and version token.
pub async fn read_origin_observation(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<Option<OriginObservation>> {
    match storage.get_with_etag(&origin_key(pkg)).await? {
        Some((bytes, etag)) => {
            let decoded = decode_claim(&bytes)?;
            Ok(Some(OriginObservation {
                state: decoded.state,
                etag,
                pending_manifest: decoded.pending_manifest,
            }))
        }
        None => Ok(None),
    }
}

/// Compatibility view for callers not yet migrated to [`OriginObservation`].
/// The content is canonical (`private`, `mirror`, or `unclaimed`) even when the
/// stored body is nonce-bearing JSON.
#[cfg(test)]
pub async fn read_origin_versioned(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<Option<(String, String)>> {
    Ok(read_origin_observation(storage, pkg)
        .await?
        .map(|observation| (observation.state.as_str().to_string(), observation.etag)))
}

/// The result of a claim attempt.
#[derive(Debug)]
pub struct Claim {
    /// The origin that now holds the claim: ours when `etag` is present, else a
    /// racer's.
    pub owner: String,
    /// The claim object's etag *when this call created it* — the token
    /// [`release_empty_claim`] and the proxy's pre-commit re-check need to CAS
    /// their own exact claim. `None` when we did not create it.
    pub etag: Option<String>,
}

/// Atomically claim the package, first write wins.
///
/// Create-if-absent for a missing object; a `put-if-match` on the `unclaimed`
/// sentinel otherwise. A real `private`/`mirror` claim already present returns
/// that claim's owner and no etag. Passing a
/// [`ClaimRequest`] with an `unclaimed` observation skips the create known to
/// lose and starts with the sentinel CAS.
pub async fn claim_origin<'a>(
    storage: &dyn Storage,
    pkg: &str,
    request: impl Into<ClaimRequest<'a>>,
) -> Result<Claim> {
    let request = request.into();
    let target = OriginState::claim_target(request.origin)?;
    let key = origin_key(pkg);

    if let Some(observed) = request.unclaimed {
        if observed.state != OriginState::Unclaimed {
            bail!(
                "claim hint for '{pkg}' must be an unclaimed observation, not '{}'",
                observed.state.as_str()
            );
        }
        if let Some(etag) = storage
            .put_if_match(&key, &observed.etag, encode_claim(target)?)
            .await?
        {
            return Ok(Claim {
                owner: target.as_str().to_string(),
                etag: Some(etag),
            });
        }
        // The hint went stale. Re-read below; never assume what won the race.
    }

    for _ in 0..CLAIM_ATTEMPTS {
        // Fast path: create the claim object if it does not exist yet.
        if let Some(etag) = storage
            .put_if_none_match(&key, encode_claim(target)?)
            .await?
        {
            return Ok(Claim {
                owner: target.as_str().to_string(),
                etag: Some(etag),
            });
        }
        // The object exists. Read it with its etag to decide the transition.
        let observed = read_origin_observation(storage, pkg)
            .await?
            .ok_or_else(|| anyhow!("origin claim for '{pkg}' vanished mid-claim"))?;
        if observed.state == OriginState::Unclaimed {
            // CAS the sentinel to our origin; a lost race re-reads and retries.
            match storage
                .put_if_match(&key, &observed.etag, encode_claim(target)?)
                .await?
            {
                Some(new_etag) => {
                    return Ok(Claim {
                        owner: target.as_str().to_string(),
                        etag: Some(new_etag),
                    })
                }
                None => continue,
            }
        }
        // A real claim holds the name (ours or a racer's) — never ours to release.
        return Ok(Claim {
            owner: observed.state.as_str().to_string(),
            etag: None,
        });
    }
    Err(anyhow!(
        "could not settle the origin claim for '{pkg}' after {CLAIM_ATTEMPTS} attempts"
    ))
}

/// The only legal demotion (dev/MULTIBUCKET.md §6.2): CAS an observation proven
/// to be `mirror` to a fresh `private` body. A stale observation loses. Passing
/// any other state is a fail-closed no-op.
pub async fn demote_observed_mirror(
    storage: &dyn Storage,
    pkg: &str,
    observed: &OriginObservation,
) -> Result<Option<OriginObservation>> {
    if observed.state != OriginState::Mirror {
        return Ok(None);
    }
    Ok(storage
        .put_if_match(
            &origin_key(pkg),
            &observed.etag,
            encode_claim(OriginState::Private)?,
        )
        .await?
        .map(|etag| OriginObservation {
            state: OriginState::Private,
            etag,
            pending_manifest: None,
        }))
}

/// Atomically reserve a package for one committed replication manifest. The
/// claim is already logically private while pending, so mirrors remain locked
/// out; request-path mutations reject the pending marker until promotion
/// completes. A different manifest owns the package when this returns `None`.
pub async fn begin_private_promotion(
    storage: &dyn Storage,
    pkg: &str,
    manifest_key: &str,
) -> Result<Option<OriginObservation>> {
    let expected_prefix = format!("_staging/repl/{pkg}/manifest@");
    if !manifest_key.starts_with(&expected_prefix) {
        bail!("replication manifest for '{pkg}' has an invalid key '{manifest_key}'");
    }
    for _ in 0..CLAIM_ATTEMPTS {
        let observed = read_origin_observation(storage, pkg)
            .await?
            .ok_or_else(|| anyhow!("package '{pkg}' has no origin claim for staged promotion"))?;
        if let Some(owner) = observed.pending_manifest.as_deref() {
            return Ok((owner == manifest_key).then_some(observed));
        }
        if !matches!(observed.state, OriginState::Mirror | OriginState::Private) {
            bail!(
                "package '{pkg}' is '{}' while staged promotion requires mirror or private",
                observed.state.as_str()
            );
        }
        if let Some(etag) = storage
            .put_if_match(
                &origin_key(pkg),
                &observed.etag,
                encode_claim_with_pending(OriginState::Private, Some(manifest_key))?,
            )
            .await?
        {
            return Ok(Some(OriginObservation {
                state: OriginState::Private,
                etag,
                pending_manifest: Some(manifest_key.to_string()),
            }));
        }
    }
    bail!("could not reserve '{pkg}' for staged promotion")
}

/// Release the exact pending-manifest reservation after every staged mutation
/// has converged. A stale worker loses the CAS and leaves the current owner
/// untouched.
pub async fn finish_private_promotion(
    storage: &dyn Storage,
    pkg: &str,
    observed: &OriginObservation,
) -> Result<bool> {
    if observed.state != OriginState::Private || observed.pending_manifest.is_none() {
        return Ok(false);
    }
    Ok(storage
        .put_if_match(
            &origin_key(pkg),
            &observed.etag,
            encode_claim(OriginState::Private)?,
        )
        .await?
        .is_some())
}

/// Compatibility wrapper for callers that still carry only an etag. Re-read the
/// state and require that this exact etag currently proves `mirror` before CAS.
/// New code should retain and pass the [`OriginObservation`] directly.
#[cfg(test)]
pub async fn demote_mirror_to_private(
    storage: &dyn Storage,
    pkg: &str,
    expected_etag: &str,
) -> Result<Option<String>> {
    let Some(observed) = read_origin_observation(storage, pkg).await? else {
        return Ok(None);
    };
    if observed.etag != expected_etag {
        return Ok(None);
    }
    Ok(demote_observed_mirror(storage, pkg, &observed)
        .await?
        .map(|next| next.etag))
}

/// Release an observed orphan `mirror` claim if the package still has no
/// truth or permanent fences. The state check happens before the listing, and the CAS consumes
/// the same observation afterward. The next state is a freshly nonce'd
/// `unclaimed` object, never absence.
pub async fn release_observed_empty_mirror(
    storage: &dyn Storage,
    pkg: &str,
    observed: &OriginObservation,
) -> Result<Option<OriginObservation>> {
    if observed.state != OriginState::Mirror {
        return Ok(None);
    }
    if package_has_truth(storage, pkg).await? {
        return Ok(None);
    }
    Ok(storage
        .put_if_match(
            &origin_key(pkg),
            &observed.etag,
            encode_claim(OriginState::Unclaimed)?,
        )
        .await?
        .map(|etag| OriginObservation {
            state: OriginState::Unclaimed,
            etag,
            pending_manifest: None,
        }))
}

pub(crate) async fn package_has_truth(storage: &dyn Storage, pkg: &str) -> Result<bool> {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let claim_key = origin_key(pkg);
    Ok(storage
        .list_dir_entries(&prefix)
        .await?
        .iter()
        .any(|entry| entry.key != claim_key))
}

/// Compatibility wrapper for existing proxy callers that carry only an etag.
/// Re-read and bind it back to its state before performing the mirror-only CAS.
#[cfg(test)]
pub async fn release_empty_claim(storage: &dyn Storage, pkg: &str, expected_etag: &str) {
    let result = async {
        let Some(observed) = read_origin_observation(storage, pkg).await? else {
            return Ok::<(), anyhow::Error>(());
        };
        if observed.etag != expected_etag {
            return Ok(());
        }
        release_observed_empty_mirror(storage, pkg, &observed).await?;
        Ok(())
    }
    .await;
    if let Err(e) = result {
        warn!(package=%pkg, error=?e, "could not release orphan claim");
    }
}

/// Deliberately release an empty package for operator-directed repurposing.
/// Unlike automatic reclamation this may consume either a mirror or private
/// claim, but only through CAS and only while both truth and write-intent sets
/// are empty. A post-CAS re-list closes the list/CAS race by restoring the
/// original owner with a fresh nonce if activity appeared.
pub async fn releasable_for_repurpose(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<Option<OriginObservation>> {
    let observed = read_origin_observation(storage, pkg).await?;
    if observed
        .as_ref()
        .and_then(|value| value.pending_manifest.as_ref())
        .is_some()
    {
        bail!("package '{pkg}' still has a committed replication manifest");
    }
    let has_package_truth = package_has_truth(storage, pkg).await?;
    let dirty_prefix = format!("{DIRTY_PREFIX}{pkg}");
    let has_pending_write = storage.list_all(&dirty_prefix).await?.iter().any(|entry| {
        entry.key == dirty_prefix || entry.key.starts_with(&format!("{dirty_prefix}!"))
    });
    if has_package_truth
        || has_pending_write
        || has_committed_stage_or_replication(storage, pkg).await?
    {
        bail!(
            "package '{pkg}' still has package truth, pending write markers, or replication work"
        );
    }
    Ok(observed.filter(|value| value.state != OriginState::Unclaimed))
}

async fn has_committed_stage_or_replication(storage: &dyn Storage, pkg: &str) -> Result<bool> {
    let stage_prefix = format!("_staging/repl/{pkg}/");
    if storage.list_all(&stage_prefix).await?.iter().any(|entry| {
        entry
            .key
            .strip_prefix(&stage_prefix)
            .is_some_and(|name| name.starts_with("manifest@"))
    }) {
        return Ok(true);
    }
    Ok(storage.list_all("_repl/").await?.iter().any(|entry| {
        let Some(rest) = entry.key.strip_prefix("_repl/") else {
            return false;
        };
        let Some((_, rest)) = rest.split_once('/') else {
            return false;
        };
        rest.strip_prefix(pkg)
            .is_some_and(|suffix| suffix.starts_with('/'))
    }))
}

pub async fn release_observed_for_repurpose(
    storage: &dyn Storage,
    pkg: &str,
    observed: &OriginObservation,
) -> Result<Option<OriginObservation>> {
    if observed.state == OriginState::Unclaimed {
        return Ok(None);
    }
    // Re-check immediately before consuming the preflight observation. This is
    // cheap in the operator path and closes activity between cluster preflight
    // and this bucket's CAS.
    let Some(current) = releasable_for_repurpose(storage, pkg).await? else {
        return Ok(None);
    };
    if &current != observed {
        return Ok(None);
    }

    let Some(etag) = storage
        .put_if_match(
            &origin_key(pkg),
            &current.etag,
            encode_claim(OriginState::Unclaimed)?,
        )
        .await?
    else {
        return Ok(None);
    };
    let unclaimed = OriginObservation {
        state: OriginState::Unclaimed,
        etag,
        pending_manifest: None,
    };

    let package_prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let dirty_prefix = format!("{DIRTY_PREFIX}{pkg}");
    let claim_key = origin_key(pkg);
    let appeared = storage
        .list_dir_entries(&package_prefix)
        .await?
        .iter()
        .any(|entry| entry.key != claim_key)
        || storage.list_all(&dirty_prefix).await?.iter().any(|entry| {
            entry.key == dirty_prefix || entry.key.starts_with(&format!("{dirty_prefix}!"))
        })
        || has_committed_stage_or_replication(storage, pkg).await?;
    if appeared {
        let restored = claim_origin(
            storage,
            pkg,
            ClaimRequest::new(current.state.as_str(), Some(&unclaimed)),
        )
        .await?;
        bail!(
            "package '{pkg}' changed while its origin was released; restored owner '{}'",
            restored.owner
        );
    }
    Ok(Some(unclaimed))
}

/// Restore a release completed on an earlier bucket when the cluster-wide
/// operator command fails later. A fresh claim nonce prevents ABA. If a private
/// racer won meanwhile, that stricter terminal state is already safe.
pub async fn restore_released_for_repurpose(
    storage: &dyn Storage,
    pkg: &str,
    original: OriginState,
    unclaimed: &OriginObservation,
) -> Result<()> {
    let restored = claim_origin(
        storage,
        pkg,
        ClaimRequest::new(original.as_str(), Some(unclaimed)),
    )
    .await?;
    if restored.owner == original.as_str()
        || (original == OriginState::Mirror && restored.owner == PRIVATE)
    {
        return Ok(());
    }
    if original == OriginState::Private && restored.owner == MIRROR {
        let mirror = read_origin_observation(storage, pkg)
            .await?
            .filter(|value| value.state == OriginState::Mirror)
            .ok_or_else(|| anyhow!("mirror rollback claim for '{pkg}' changed"))?;
        if demote_observed_mirror(storage, pkg, &mirror)
            .await?
            .is_some()
        {
            return Ok(());
        }
    }
    bail!(
        "could not restore package '{pkg}' origin '{}' after partial release; current owner is '{}'",
        original.as_str(),
        restored.owner
    )
}

#[cfg(test)]
pub async fn release_for_repurpose(storage: &dyn Storage, pkg: &str) -> Result<bool> {
    let Some(observed) = releasable_for_repurpose(storage, pkg).await? else {
        return Ok(false);
    };
    Ok(release_observed_for_repurpose(storage, pkg, &observed)
        .await?
        .is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::InMemStorage;
    use std::sync::Arc;

    fn store() -> Arc<InMemStorage> {
        Arc::new(InMemStorage::default())
    }

    async fn stored_body(storage: &InMemStorage, pkg: &str) -> ClaimBody {
        let bytes = storage.get_bytes(&origin_key(pkg)).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn every_claim_write_has_a_fresh_128_bit_nonce() {
        let s = store();
        claim_origin(s.as_ref(), "one", PRIVATE).await.unwrap();
        claim_origin(s.as_ref(), "two", MIRROR).await.unwrap();

        let one = stored_body(s.as_ref(), "one").await;
        let two = stored_body(s.as_ref(), "two").await;
        assert_eq!(one.origin, PRIVATE);
        assert_eq!(two.origin, MIRROR);
        for nonce in [&one.nonce, &two.nonce] {
            assert_eq!(nonce.len(), 32);
            assert!(nonce
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        }
        assert_ne!(one.nonce, two.nonce);
    }

    #[tokio::test]
    async fn legacy_plain_text_claims_still_parse_into_typed_observations() {
        let s = store();
        s.insert(&origin_key("private-pkg"), b"private\n".to_vec());
        s.insert(&origin_key("mirror-pkg"), b"mirror".to_vec());
        s.insert(&origin_key("free-pkg"), b"unclaimed".to_vec());

        assert_eq!(
            read_origin(s.as_ref(), "private-pkg")
                .await
                .unwrap()
                .as_deref(),
            Some(PRIVATE)
        );
        assert_eq!(
            read_origin_observation(s.as_ref(), "mirror-pkg")
                .await
                .unwrap()
                .unwrap()
                .state,
            OriginState::Mirror
        );
        assert_eq!(
            read_origin_observation(s.as_ref(), "free-pkg")
                .await
                .unwrap()
                .unwrap()
                .state,
            OriginState::Unclaimed
        );
        assert_eq!(read_origin(s.as_ref(), "free-pkg").await.unwrap(), None);
    }

    #[tokio::test]
    async fn malformed_nonce_bodies_fail_closed() {
        let s = store();
        s.insert(
            &origin_key("pkg"),
            br#"{"origin":"private","nonce":"short"}"#.to_vec(),
        );
        assert!(read_origin(s.as_ref(), "pkg").await.is_err());
        assert!(read_origin_observation(s.as_ref(), "pkg").await.is_err());
    }

    #[tokio::test]
    async fn first_claim_creates_and_reports_owner() {
        let s = store();
        let claim = claim_origin(s.as_ref(), "pkg", PRIVATE).await.unwrap();
        assert!(claim.etag.is_some());
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
        assert!(second.etag.is_some());
        assert_eq!(second.owner, PRIVATE);
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
    }

    #[tokio::test]
    async fn observed_unclaimed_hint_claims_directly_and_rejects_other_states() {
        let s = store();
        let first = claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        release_empty_claim(s.as_ref(), "pkg", first.etag.as_deref().unwrap()).await;
        let unclaimed = read_origin_observation(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unclaimed.state, OriginState::Unclaimed);

        let claimed = claim_origin(
            s.as_ref(),
            "pkg",
            ClaimRequest::new(PRIVATE, Some(&unclaimed)),
        )
        .await
        .unwrap();
        assert!(claimed.etag.is_some());
        assert_eq!(claimed.owner, PRIVATE);

        let private = read_origin_observation(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        let err = claim_origin(s.as_ref(), "pkg", ClaimRequest::new(MIRROR, Some(&private)))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must be an unclaimed observation"));
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
    async fn pending_manifest_is_a_cas_owned_private_barrier() {
        let s = store();
        claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        let first = "_staging/repl/pkg/manifest@one.json";
        let pending = begin_private_promotion(s.as_ref(), "pkg", first)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.state, OriginState::Private);
        assert_eq!(pending.pending_manifest.as_deref(), Some(first));
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );

        assert!(
            begin_private_promotion(s.as_ref(), "pkg", "_staging/repl/pkg/manifest@two.json")
                .await
                .unwrap()
                .is_none()
        );
        assert!(finish_private_promotion(s.as_ref(), "pkg", &pending)
            .await
            .unwrap());
        let settled = read_origin_observation(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.state, OriginState::Private);
        assert_eq!(settled.pending_manifest, None);
    }

    #[tokio::test]
    async fn stale_promotion_finisher_cannot_clear_a_newer_pending_owner() {
        let s = store();
        claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        let first_key = "_staging/repl/pkg/manifest@one.json";
        let first = begin_private_promotion(s.as_ref(), "pkg", first_key)
            .await
            .unwrap()
            .unwrap();
        assert!(finish_private_promotion(s.as_ref(), "pkg", &first)
            .await
            .unwrap());

        let second_key = "_staging/repl/pkg/manifest@two.json";
        let second = begin_private_promotion(s.as_ref(), "pkg", second_key)
            .await
            .unwrap()
            .unwrap();
        assert!(!finish_private_promotion(s.as_ref(), "pkg", &first)
            .await
            .unwrap());

        let current = read_origin_observation(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.etag, second.etag);
        assert_eq!(current.pending_manifest.as_deref(), Some(second_key));
    }

    #[tokio::test]
    async fn operator_release_rejects_committed_replication_work() {
        for key in [
            "_staging/repl/pkg/manifest@one.json",
            "_repl/1/pkg/pkg-1.whl!nonce",
        ] {
            let s = store();
            claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
            s.insert(key, b"{}".to_vec());
            let error = releasable_for_repurpose(s.as_ref(), "pkg")
                .await
                .unwrap_err();
            assert!(error.to_string().contains("replication"));
        }
    }

    #[tokio::test]
    async fn mirror_only_transitions_reject_non_mirror_observations() {
        let s = store();
        claim_origin(s.as_ref(), "pkg", PRIVATE).await.unwrap();
        let private = read_origin_observation(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();

        assert!(demote_observed_mirror(s.as_ref(), "pkg", &private)
            .await
            .unwrap()
            .is_none());
        assert!(release_observed_empty_mirror(s.as_ref(), "pkg", &private)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );
    }

    #[tokio::test]
    async fn nonces_prevent_mirror_claim_aba_from_reviving_a_stale_etag() {
        let s = store();
        let first = claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
        let stale_mirror_etag = first.etag.unwrap();
        release_empty_claim(s.as_ref(), "pkg", &stale_mirror_etag).await;

        let unclaimed = read_origin_observation(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        let second = claim_origin(
            s.as_ref(),
            "pkg",
            ClaimRequest::new(MIRROR, Some(&unclaimed)),
        )
        .await
        .unwrap();
        let current_mirror_etag = second.etag.unwrap();
        assert_ne!(stale_mirror_etag, current_mirror_etag);

        assert!(
            demote_mirror_to_private(s.as_ref(), "pkg", &stale_mirror_etag)
                .await
                .unwrap()
                .is_none()
        );
        release_empty_claim(s.as_ref(), "pkg", &stale_mirror_etag).await;
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(MIRROR)
        );
    }

    #[tokio::test]
    async fn operator_release_is_cas_to_unclaimed_and_requires_empty_truth() {
        let s = store();
        claim_origin(s.as_ref(), "pkg", PRIVATE).await.unwrap();
        s.insert("packages/pkg/pkg-1.0.tar.gz", b"truth".to_vec());
        assert!(release_for_repurpose(s.as_ref(), "pkg").await.is_err());
        assert_eq!(
            read_origin(s.as_ref(), "pkg").await.unwrap().as_deref(),
            Some(PRIVATE)
        );

        s.delete_keys(&["packages/pkg/pkg-1.0.tar.gz".to_string()])
            .await
            .unwrap();
        for retained in [
            "packages/pkg/pkg-1.0.tar.gz.meta.json",
            "packages/pkg/pkg-1.0.tar.gz.tombstone",
            "packages/pkg/pkg-1.0.tar.gz.frozen",
            "packages/pkg/.project-status.json",
        ] {
            s.insert(retained, b"truth fence".to_vec());
            assert!(release_for_repurpose(s.as_ref(), "pkg").await.is_err());
            s.delete_keys(&[retained.to_string()]).await.unwrap();
        }
        assert!(release_for_repurpose(s.as_ref(), "pkg").await.unwrap());
        assert_eq!(
            read_origin_observation(s.as_ref(), "pkg")
                .await
                .unwrap()
                .unwrap()
                .state,
            OriginState::Unclaimed
        );
    }

    #[tokio::test]
    async fn operator_release_rollback_restores_the_original_owner_with_a_new_nonce() {
        let s = store();
        claim_origin(s.as_ref(), "pkg", PRIVATE).await.unwrap();
        let original = releasable_for_repurpose(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        let released = release_observed_for_repurpose(s.as_ref(), "pkg", &original)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(released.state, OriginState::Unclaimed);

        restore_released_for_repurpose(s.as_ref(), "pkg", original.state, &released)
            .await
            .unwrap();
        let restored = read_origin_observation(s.as_ref(), "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.state, OriginState::Private);
        assert_ne!(restored.etag, original.etag);
        assert_ne!(restored.etag, released.etag);
    }

    #[tokio::test]
    async fn operator_release_detects_legacy_and_nonce_dirty_markers() {
        for marker in ["_dirty/pkg", "_dirty/pkg!nonce.commit"] {
            let s = store();
            claim_origin(s.as_ref(), "pkg", MIRROR).await.unwrap();
            s.insert(marker, Vec::new());
            let error = releasable_for_repurpose(s.as_ref(), "pkg")
                .await
                .unwrap_err();
            assert!(error.to_string().contains("pending write markers"));
        }
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
