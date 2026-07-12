//! Index rebuild worker: dirty markers, not a queue.
//!
//! Markers are unique, create-only event keys:
//! `_dirty/<pkg>!<nonce>.intent` written *before* a writer touches truth, and
//! `_dirty/<pkg>!<nonce>.commit` written *after*. Because every event is its
//! own key, the worker can rebuild FIRST and then delete exactly the keys it
//! observed — a concurrent writer's new marker is a new key and survives
//! untouched, and a crash mid-rebuild leaves the keys in place for the next
//! tick. At-least-once processing is free: rebuilds derive views from current
//! truth, so duplicates converge.
//!
//! The intent/commit pair is what makes a crashed writer heal without any
//! sweep: a commit (or an intent whose pair arrived) rebuilds immediately; an
//! unpaired intent younger than the grace period means a writer is still in
//! flight, so the package is skipped this tick; an unpaired intent older than
//! the grace period is a crashed writer — rebuild and consume it. Markers are
//! never deleted unprocessed, so no event is ever lost.
//!
//! Legacy flat markers (`_dirty/<pkg>`, no `!`) are treated as commits so an
//! upgraded node drains what an old node wrote.

use std::{
    collections::{hash_map::RandomState, HashSet},
    hash::{BuildHasher, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, bail, Result};
use sha2::{Digest, Sha256};
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};

use crate::lease::LeaseManager;
use crate::names::infer_version_from_filename;
use crate::render::{
    pep503_global_html, pep503_package_html, pep691_global_json, pep691_package_json, FileMetadata,
    SIMPLE_HTML_CONTENT_TYPE, SIMPLE_JSON_CONTENT_TYPE,
};
use crate::sidecar::{
    is_artifact, sidecar_key, Sidecar, Yanked, FROZEN_SUFFIX, METADATA_SUFFIX, PROVENANCE_SUFFIX,
    SIDECAR_SUFFIX, TOMBSTONE_SUFFIX,
};
use crate::storage::{is_not_found, FileEntry, ObjectMeta, Storage};
use crate::{AppState, DIRTY_PREFIX, PACKAGES_PREFIX, SIMPLE_PREFIX};

/// Bounded fan-out for storage round-trips during rebuilds and sweeps.
/// High enough to collapse per-file latency, low enough to never matter
/// against S3 request limits or this process's memory. 64 sidecar reads in
/// flight took a 5,000-file package rebuild from 17 s to a few seconds;
/// sidecars are sub-KB objects, far below any S3 prefix limit.
const SIDECAR_READ_CONCURRENCY: usize = 64;
const PACKAGE_SWEEP_CONCURRENCY: usize = 8;
/// Bound health-only storage calls independently of the object-store client's
/// retry budget. A blackholed bucket must contribute one failure per worker
/// cycle, not one failure after minutes of SDK retries.
const BUCKET_HEALTH_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

const INTENT_SUFFIX: &str = ".intent";
const COMMIT_SUFFIX: &str = ".commit";

/// Unique per-event marker id: wall nanos + pid + process-local counter +
/// per-call randomized entropy. The deterministic fields make logs useful;
/// entropy prevents two processes with the same pid and clock from claiming
/// the same correctness-critical marker identity. Shared with the replicator's
/// `_repl/` markers (src/replicate.rs), which reuse the idiom.
pub(crate) fn marker_nonce() -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let pid = std::process::id();
    let entropy = |domain: u64| {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(nanos);
        hasher.write_u32(pid);
        hasher.write_u64(seq);
        hasher.write_u64(domain);
        hasher.finish()
    };
    format!("{nanos}-{pid}-{seq}-{:016x}{:016x}", entropy(0), entropy(1))
}

/// Declare "I am about to change truth for `pkg`". Returns the nonce the
/// writer must commit with. If the writer dies, the intent goes stale and the
/// worker rebuilds anyway after the grace period.
pub async fn mark_intent(storage: &dyn Storage, pkg: &str) -> Result<String> {
    let nonce = marker_nonce();
    mark_intent_with_nonce(storage, pkg, &nonce).await?;
    Ok(nonce)
}

pub(crate) async fn mark_intent_with_nonce(
    storage: &dyn Storage,
    pkg: &str,
    nonce: &str,
) -> Result<()> {
    put_marker(storage, pkg, nonce, INTENT_SUFFIX).await
}

/// Declare "truth changed for `pkg`": rebuild as soon as possible.
pub async fn mark_commit(storage: &dyn Storage, pkg: &str, nonce: &str) -> Result<()> {
    put_marker(storage, pkg, nonce, COMMIT_SUFFIX).await
}

pub(crate) async fn clear_intent(storage: &dyn Storage, pkg: &str, nonce: &str) -> Result<()> {
    storage
        .delete_keys(&[format!("{DIRTY_PREFIX}{pkg}!{nonce}{INTENT_SUFFIX}")])
        .await
}

/// Write an empty event marker `_dirty/<pkg>!<nonce><suffix>`.
async fn put_marker(storage: &dyn Storage, pkg: &str, nonce: &str, suffix: &str) -> Result<()> {
    storage
        .put_bytes(
            &format!("{DIRTY_PREFIX}{pkg}!{nonce}{suffix}"),
            Vec::new(),
            None,
        )
        .await
}

/// Mark a package as needing an index rebuild (an unpaired commit event, for
/// callers whose truth change already happened).
pub async fn mark_dirty(storage: &dyn Storage, pkg: &str) -> Result<()> {
    mark_commit(storage, pkg, &marker_nonce()).await
}

/// One parsed `_dirty/` entry.
struct Marker {
    key: String,
    nonce: Option<String>,
    is_commit: bool,
    /// Storage last-modified — staleness comes from the storage clock.
    written_at: Option<time::OffsetDateTime>,
}

struct DirtyWork {
    package: String,
    keys: Vec<String>,
    stale_intents: u64,
}

/// Split a marker key into (package, marker). Legacy `_dirty/<pkg>` keys
/// parse as nonce-less commits.
fn parse_marker(entry: &FileEntry) -> Option<(String, Marker)> {
    let rest = entry.key.strip_prefix(DIRTY_PREFIX)?;
    let written_at = entry.last_modified.as_deref().and_then(|ts| {
        time::OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339).ok()
    });
    let Some((pkg, event)) = rest.split_once('!') else {
        return Some((
            rest.to_string(),
            Marker {
                key: entry.key.clone(),
                nonce: None,
                is_commit: true,
                written_at,
            },
        ));
    };
    let (nonce, is_commit) = if let Some(n) = event.strip_suffix(COMMIT_SUFFIX) {
        (n, true)
    } else if let Some(n) = event.strip_suffix(INTENT_SUFFIX) {
        (n, false)
    } else {
        // Unknown suffix: treat as a commit so nothing rots in the prefix.
        (event, true)
    };
    Some((
        pkg.to_string(),
        Marker {
            key: entry.key.clone(),
            nonce: Some(nonce.to_string()),
            is_commit,
            written_at,
        },
    ))
}

/// Group dirty events by package and select exactly the markers safe to
/// consume now. Both the selected-bucket tick and warm-bucket drain use this
/// transaction-log rule: any fresh unpaired intent defers the whole package;
/// otherwise commits and paired intents are ready immediately, while stale
/// unpaired intents heal a crashed writer.
fn consumable_dirty_work(
    entries: &[FileEntry],
    now: time::OffsetDateTime,
    intent_grace: time::Duration,
) -> Vec<DirtyWork> {
    let mut per_pkg: std::collections::HashMap<String, Vec<Marker>> =
        std::collections::HashMap::new();
    for entry in entries {
        if let Some((pkg, marker)) = parse_marker(entry) {
            per_pkg.entry(pkg).or_default().push(marker);
        }
    }

    let mut work = Vec::new();
    for (package, markers) in per_pkg {
        let commit_nonces: HashSet<&str> = markers
            .iter()
            .filter(|marker| marker.is_commit)
            .filter_map(|marker| marker.nonce.as_deref())
            .collect();
        let mut stale_intents = 0;
        let mut fresh_unpaired = false;
        for marker in markers.iter().filter(|marker| !marker.is_commit) {
            let paired = marker
                .nonce
                .as_deref()
                .is_some_and(|nonce| commit_nonces.contains(nonce));
            if paired {
                continue;
            }
            let stale = marker
                .written_at
                .is_some_and(|written_at| now - written_at >= intent_grace);
            if stale {
                stale_intents += 1;
            } else {
                fresh_unpaired = true;
                break;
            }
        }
        if fresh_unpaired {
            continue;
        }
        work.push(DirtyWork {
            package,
            keys: markers.into_iter().map(|marker| marker.key).collect(),
            stale_intents,
        });
    }
    work
}

fn topology_availability_error(_index: usize, error: &anyhow::Error) -> bool {
    crate::bucket_health::classify(crate::observed_storage::signal_for_error(error))
        == crate::bucket_health::SignalClass::AvailabilityFailure
}

/// Probe every bucket concurrently on the dedicated health loop. Probing the
/// selected bucket is what bounds idle-node failover even when the ordinary
/// worker is busy in a long index/counter operation; this loop is multi-only.
async fn probe_buckets(state: &AppState) {
    let probes = state
        .buckets
        .handles()
        .iter()
        .enumerate()
        .map(|(index, handle)| {
            async move {
                match timeout(
                    BUCKET_HEALTH_IO_TIMEOUT,
                    // S3 reports a missing object and a missing bucket as the
                    // same body-less 404 to HEAD. GET the tiny, guaranteed
                    // multi-bucket topology stamp so NoSuchBucket retains its
                    // typed response body and cannot look healthy.
                    handle
                        .storage
                        .get_with_etag(crate::buckets::TOPOLOGY_STAMP_KEY),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        let signal = crate::observed_storage::signal_for_error(&error);
                        match crate::bucket_health::classify(signal) {
                            crate::bucket_health::SignalClass::AvailabilityFailure => {
                                warn!(bucket=%handle.name, error=?error, "bucket health probe failed")
                            }
                            crate::bucket_health::SignalClass::Ignored => {
                                error!(bucket=%handle.name, error=?error, "bucket health probe alarm (selection unchanged)")
                            }
                            crate::bucket_health::SignalClass::Healthy => {}
                        }
                    }
                    Err(_) => {
                        if let Some(health) = &state.bucket_health {
                            let _ = health.observe(
                                index,
                                crate::bucket_health::BucketSignal::Timeout,
                            );
                        }
                        warn!(bucket=%handle.name, timeout_ms=BUCKET_HEALTH_IO_TIMEOUT.as_millis(), "bucket health probe timed out");
                    }
                }
            }
        });
    futures::future::join_all(probes).await;
}

/// Availability selection has its own loop so no unrelated worker I/O can
/// delay it. Requests continue pinning the switched `BucketSet` immediately;
/// the index worker notices the generation on its next safe boundary.
async fn run_bucket_health_until(
    state: Arc<AppState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    // Only the storage probe is traffic-gated; selection maintenance runs every
    // tick so a switch applies the instant an observation (a probe, or the
    // worker's own I/O on the selected bucket) marks a bucket unhealthy.
    let mut last_probe: Option<Instant> = None;
    loop {
        // Traffic-gated cadence (dev/MULTIBUCKET.md §Health): probe at full
        // speed while there is recent request traffic OR any bucket is unhealthy
        // or recovering — re-probing an unhealthy bucket is the only way it heals
        // back, so that is never gated off. Otherwise decay to the idle cadence,
        // accepting that the first request after idle may pay one bounded
        // discovery timeout before failover.
        let full_cadence =
            state.recent_request_traffic() || state.any_bucket_unhealthy_or_recovering();
        let probe_due =
            full_cadence || last_probe.is_none_or(|t| t.elapsed() >= crate::IDLE_PROBE_INTERVAL);
        if probe_due {
            probe_buckets(&state).await;
            last_probe = Some(Instant::now());
        }
        maintain_bucket_selection(&state).await;
        tokio::select! {
            _ = sleep(state.worker_interval) => {}
            _ = shutdown.changed() => break,
        }
    }
}

/// Check a bucket-local lease without inheriting a cloud SDK's multi-minute
/// retry budget. `ObservedStorage` records completed calls; a cancelled timeout
/// is recorded here because the wrapper never sees a result.
async fn bounded_lease_check(state: &AppState, bucket: usize, lease: &LeaseManager) -> bool {
    match timeout(BUCKET_HEALTH_IO_TIMEOUT, lease.is_leader()).await {
        Ok(is_leader) => is_leader,
        Err(_) => {
            if let Some(health) = &state.bucket_health {
                let _ = health.observe(bucket, crate::bucket_health::BucketSignal::Timeout);
            }
            warn!(
                bucket = %state.buckets.handles()[bucket].name,
                timeout_ms = BUCKET_HEALTH_IO_TIMEOUT.as_millis(),
                "bucket lease check timed out"
            );
            false
        }
    }
}

async fn wait_until_bucket_ineligible(state: &AppState, index: usize) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if state
            .bucket_health
            .as_ref()
            .is_some_and(|health| !health.bucket_eligible(index).unwrap_or(false))
        {
            return;
        }
    }
}

async fn wait_until_generation_changes(state: &AppState, generation: u64) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if state.pin().generation != generation {
            return;
        }
    }
}

/// Validate recovered buckets' topology and apply at most one coalesced
/// selection change. Entirely dormant in single-bucket mode.
async fn maintain_bucket_selection(state: &Arc<AppState>) -> bool {
    let Some(health) = &state.bucket_health else {
        return false;
    };
    let mut snapshot = health.worker_tick();
    for (index, alarms) in snapshot.alarms.iter().copied().enumerate() {
        if alarms > 0 {
            error!(bucket=%state.buckets.handles()[index].name, alarms, "bucket configuration/auth alarms (selection unchanged)");
        }
    }

    // A selection candidate is usable only after its topology has been
    // revalidated. Availability failures may recover on a later tick; a
    // topology mismatch additionally raises the sticky write fence, but must
    // also keep reads and presigns on the last validated bucket.
    let mut selection_blocked = HashSet::new();
    for index in &snapshot.topology_revalidation {
        let verification = timeout(
            BUCKET_HEALTH_IO_TIMEOUT,
            state
                .buckets
                .verify_topology_index_with(*index, topology_availability_error),
        )
        .await;
        match verification {
            Err(_) => {
                let _ = health.observe(*index, crate::bucket_health::BucketSignal::Timeout);
                selection_blocked.insert(*index);
                warn!(bucket=%state.buckets.handles()[*index].name, "bucket topology revalidation timed out");
            }
            Ok(Ok(crate::buckets::TopologyIndexStatus::Unreachable)) => {
                selection_blocked.insert(*index);
            }
            Ok(Ok(_)) => {
                if let Err(error) = health.topology_revalidated(*index) {
                    error!(bucket=*index, error=%error, "could not acknowledge topology validation");
                } else {
                    // A bucket just crossed unhealthy→healthy. Fire the `_repl/`
                    // sweep at once so drain starts seconds after heal instead of
                    // waiting out the periodic backstop (dev/MULTIBUCKET.md
                    // §Repair and convergence). The worker loop owns the sweep;
                    // set the request flag and wake it.
                    state
                        .repl_sweep_requested
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    state.worker_nudge.notify_one();
                }
            }
            Ok(Err(error)) => {
                // A healed partition exposed a differently stamped deployment.
                // Reads stay available, but accepting new writes would turn a
                // configuration error into continuous divergence.
                state
                    .writes_fenced
                    .store(true, std::sync::atomic::Ordering::Release);
                selection_blocked.insert(*index);
                error!(bucket=*index, error=?error, "runtime bucket topology mismatch; writes fenced");
            }
        }
    }

    let mut changed = false;
    if let Some(change) = snapshot.selection_change {
        if !selection_blocked.contains(&change.to) {
            let next = state.buckets.switch(change.to);
            if let Err(error) = health.selection_applied(change.to) {
                error!(bucket=change.to, error=%error, "could not acknowledge bucket selection");
            } else {
                // These caches describe the selected bucket, not the process.
                // Index and presign caches clear through their generation tags;
                // reset the remaining selected-bucket views explicitly.
                *state.global_names.lock().await = None;
                *state.inventory.lock().await = InventoryMap::default();
                state.empty_origin_observations.lock().await.clear();
                info!(
                    from = %state.buckets.handles()[change.from].name,
                    to = %state.buckets.handles()[change.to].name,
                    generation = next.generation,
                    "selected bucket changed"
                );
                changed = true;
            }
        }
    }
    let applied = state.pin();
    snapshot.selected_index = applied.index;
    let names: Vec<String> = state
        .buckets
        .handles()
        .iter()
        .map(|handle| handle.name.clone())
        .collect();
    state.metrics.update_bucket_health(
        &snapshot,
        &names,
        applied.generation,
        state
            .writes_fenced
            .load(std::sync::atomic::Ordering::Acquire),
    );
    changed
}

/// Whether `pkg` has an unpaired intent still inside the storage-clock grace
/// window. A missing/malformed storage timestamp is conservatively live. The
/// key prefix keeps this O(markers-for-one-package), not O(all markers).
async fn has_live_intent(state: &AppState, storage: &dyn Storage, pkg: &str) -> Result<bool> {
    Ok(stale_unpaired_intents(state, storage, pkg).await?.is_none())
}

/// Whether any writer intent still lacks its matching commit, regardless of
/// age. Callers use this when stale work must be healed before classifying the
/// package's current truth.
#[allow(dead_code)] // Base writer-intent primitive retained for repair callers.
pub(crate) async fn has_unpaired_intent_ignoring(
    storage: &dyn Storage,
    pkg: &str,
    ignored_nonce: Option<&str>,
) -> Result<bool> {
    let prefix = format!("{DIRTY_PREFIX}{pkg}!");
    let entries = storage.list_all(&prefix).await?;
    let commits: HashSet<String> = entries
        .iter()
        .filter_map(|entry| {
            entry
                .key
                .strip_prefix(&prefix)?
                .strip_suffix(COMMIT_SUFFIX)
                .map(str::to_string)
        })
        .collect();
    Ok(entries.iter().any(|entry| {
        entry
            .key
            .strip_prefix(&prefix)
            .and_then(|event| event.strip_suffix(INTENT_SUFFIX))
            .is_some_and(|nonce| {
                !intent_is_ignored(nonce, ignored_nonce) && !commits.contains(nonce)
            })
    }))
}

/// Classify every unpaired intent by storage `last_modified`. `None` means at
/// least one writer is still fresh (or lacks a trustworthy storage timestamp
/// and is therefore conservatively live). `Some(keys)` contains only stale
/// intent keys safe to consume while healing the crash shape. Storage time, not
/// a writer nonce clock, keeps skewed nodes safe.
pub(crate) async fn stale_unpaired_intents(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
) -> Result<Option<Vec<String>>> {
    stale_unpaired_intents_ignoring(state, storage, pkg, None).await
}

pub(crate) async fn stale_unpaired_intents_ignoring(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    ignored_nonce: Option<&str>,
) -> Result<Option<Vec<String>>> {
    let prefix = format!("{DIRTY_PREFIX}{pkg}!");
    let entries = storage.list_dir_entries(DIRTY_PREFIX).await?;
    let commits: HashSet<String> = entries
        .iter()
        .filter_map(|entry| {
            entry
                .key
                .strip_prefix(&prefix)?
                .strip_suffix(COMMIT_SUFFIX)
                .map(str::to_string)
        })
        .collect();
    let now = time::OffsetDateTime::now_utc();
    let mut stale = Vec::new();
    for entry in &entries {
        let Some(nonce) = entry
            .key
            .strip_prefix(&prefix)
            .and_then(|event| event.strip_suffix(INTENT_SUFFIX))
        else {
            continue;
        };
        if intent_is_ignored(nonce, ignored_nonce) {
            continue;
        }
        if commits.contains(nonce) {
            continue;
        }
        let Some(written) = entry.last_modified.as_deref().and_then(|raw| {
            time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339).ok()
        }) else {
            return Ok(None);
        };
        if now - written < state.intent_grace {
            return Ok(None);
        }
        stale.push(entry.key.clone());
    }
    Ok(Some(stale))
}

fn intent_is_ignored(nonce: &str, ignored_nonce: Option<&str>) -> bool {
    ignored_nonce == Some(nonce)
}

/// Reclaim an empty mirror claim only after two audit observations of the same
/// nonce-bearing claim version separated by the intent grace. The proxy failure
/// path deliberately does not release claims: only the leader audit has enough
/// evidence to distinguish an orphan from a slow live writer.
async fn reclaim_empty_mirror_claim(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    generation: u64,
) -> Result<()> {
    let key = (generation, pkg.to_string());
    let Some(observed) = crate::origin::read_origin_observation(storage, pkg).await? else {
        state.empty_origin_observations.lock().await.remove(&key);
        return Ok(());
    };
    if observed.state != crate::origin::OriginState::Mirror {
        state.empty_origin_observations.lock().await.remove(&key);
        return Ok(());
    }
    if crate::origin::package_has_truth(storage, pkg).await?
        || has_live_intent(state, storage, pkg).await?
    {
        state.empty_origin_observations.lock().await.remove(&key);
        return Ok(());
    }

    let now = Instant::now();
    let grace = std::time::Duration::from_secs(state.intent_grace.whole_seconds().max(0) as u64);
    let ready = {
        let mut observations = state.empty_origin_observations.lock().await;
        match observations.get(&key) {
            Some((etag, first)) if etag == &observed.etag => now.duration_since(*first) >= grace,
            _ => {
                observations.insert(key.clone(), (observed.etag.clone(), now));
                false
            }
        }
    };
    if !ready {
        return Ok(());
    }

    // Re-check intents immediately before consuming the exact claim version.
    if has_live_intent(state, storage, pkg).await? {
        state.empty_origin_observations.lock().await.remove(&key);
        return Ok(());
    }
    let released = crate::origin::release_observed_empty_mirror(storage, pkg, &observed).await?;
    state.empty_origin_observations.lock().await.remove(&key);
    let Some(unclaimed) = released else {
        return Ok(());
    };

    // A writer can appear between the final list and the CAS. Re-list after the
    // release; if anything appeared, immediately reclaim mirror with a fresh
    // nonce. A concurrent private claim still wins and is never overwritten.
    let appeared = crate::origin::package_has_truth(storage, pkg).await?
        || has_live_intent(state, storage, pkg).await?;
    if appeared {
        let claim = crate::origin::claim_origin(
            storage,
            pkg,
            crate::origin::ClaimRequest::new(crate::origin::MIRROR, Some(&unclaimed)),
        )
        .await?;
        if claim.owner != crate::origin::MIRROR {
            warn!(package=%pkg, owner=%claim.owner, "empty mirror claim changed while audit reverted a raced release");
        }
    }
    Ok(())
}

pub async fn run_worker_until(
    state: Arc<AppState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let health_task = state
        .buckets
        .is_multi()
        .then(|| tokio::spawn(run_bucket_health_until(state.clone(), shutdown.clone())));
    // Only the index writer is singular, and only as a cost optimization:
    // rebuilds are idempotent, so the lease is sloppy. Disk is single-node
    // and skips leasing entirely.
    // Leadership is bucket-local. A selection generation change releases the
    // old lease and constructs a manager on the new bucket before any leader
    // work proceeds; authority never crosses buckets.
    let mut lease: Option<LeaseManager> = None;
    let mut warm_leases: Vec<Option<LeaseManager>> =
        (0..state.buckets.len()).map(|_| None).collect();
    let mut authority_generation = None;

    // Markers are the primary freshness mechanism; the audit is the safety
    // net for what events cannot see (restores, out-of-band storage changes,
    // a peer that died without committing). The first leader audit runs
    // immediately (unless --audit-on-boot=false), so a restored backup heals
    // without waiting an interval. The audit runs on its own task: a deep
    // pass over a large corpus takes minutes of storage round-trips, and
    // running it inline starved dirty-marker processing for its whole
    // duration. Concurrent audit + tick rebuilds of the same package are
    // safe — rebuilds are idempotent.
    let mut last_audit: Option<Instant> = if state.audit_on_boot {
        None
    } else {
        Some(Instant::now())
    };
    // Adaptive spacing: never spend more than ~1/10th of wall time auditing,
    // no matter how the interval is configured relative to corpus size.
    let last_audit_secs = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sweep_running = Arc::new(AtomicBool::new(false));
    let reconcile_running = Arc::new(AtomicBool::new(false));
    let replication_running = Arc::new(AtomicBool::new(false));
    let warm_running = Arc::new(AtomicBool::new(false));
    // Clears the in-flight flag on drop — including a panic unwind inside the
    // spawned audit. Without it, a panicking sweep leaves the flag stuck `true`
    // and no further sweep is ever scheduled, silently disabling self-healing.
    struct SweepGuard(Arc<AtomicBool>);
    impl Drop for SweepGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    let mut last_inventory_refresh: Option<Instant> = None;
    let mut last_counter_flush: Option<Instant> = None;
    let mut last_counter_compact: Option<Instant> = None;
    let mut last_bucket_maintenance: Option<Instant> = None;
    // The `_repl/` sweep runs on its own slow backstop (repl_sweep_interval),
    // decoupled from the 1 s worker tick, plus immediately on a bucket heal
    // (repl_sweep_requested). `None` fires one sweep on boot to drain any notes
    // a predecessor left. (dev/MULTIBUCKET.md §Repair and convergence.)
    let mut last_repl_sweep: Option<Instant> = None;
    let mut last_reconcile: Option<Instant> = if state.audit_on_boot {
        None
    } else {
        Some(Instant::now())
    };
    loop {
        // Nudges make selected-bucket indexes visible quickly, but must not
        // multiply the documented multi-bucket probe/LIST cadence. All periodic
        // bucket maintenance shares this independent minimum interval.
        let bucket_maintenance_due = state.buckets.is_multi()
            && last_bucket_maintenance.is_none_or(|t| t.elapsed() >= state.worker_interval);
        if bucket_maintenance_due {
            last_bucket_maintenance = Some(Instant::now());
        }
        let selected = state.pin();
        if authority_generation != Some(selected.generation) {
            if authority_generation.is_some() {
                // The leader on the newly selected bucket must audit its own
                // views and refresh bucket-local inventory.
                last_audit = None;
                last_reconcile = None;
                last_inventory_refresh = None;
            }
            if let Some(old) = lease.take() {
                tokio::spawn(async move { old.release().await });
            }
            for slot in &mut warm_leases {
                if let Some(old) = slot.take() {
                    tokio::spawn(async move { old.release().await });
                }
            }
            authority_generation = Some(selected.generation);
            if selected.storage.supports_leases() {
                lease = Some(LeaseManager::new(
                    selected.storage.clone(),
                    state.lease_ttl,
                    selected.generation,
                ));
            }
            for (index, handle) in state.buckets.handles().iter().enumerate() {
                if index != selected.index && handle.storage.supports_leases() {
                    warm_leases[index] = Some(LeaseManager::new(
                        handle.storage.clone(),
                        state.lease_ttl,
                        selected.generation,
                    ));
                }
            }
        }
        let is_leader = match &lease {
            None => true,
            Some(lm) => bounded_lease_check(&state, selected.index, lm).await,
        };
        // Refresh the in-memory inventory from the persisted view ONCE on boot
        // (a starting display, including a restart's last-known value), then
        // continuously only on followers — that's how the leader's published
        // counts reach them. The leader does NOT keep refreshing: it is the
        // authority, and reading back its own (possibly stale-on-failed-persist)
        // file would revert its fresh atomics.
        let due_refresh = last_inventory_refresh
            .is_none_or(|t| !is_leader && t.elapsed() >= INVENTORY_REFRESH_INTERVAL);
        if due_refresh {
            last_inventory_refresh = Some(Instant::now());
            tokio::select! {
                _ = refresh_inventory(&state) => {}
                _ = wait_until_generation_changes(&state, selected.generation) => {}
            }
        }
        // Counter flush (EVERY node, not leader-gated): drain the in-memory
        // download buffer to this node's own immutable segment. Best-effort;
        // fires on the interval or early when the buffer hits its high-water mark.
        if last_counter_flush.is_none_or(|t| t.elapsed() >= state.counters.flush_interval())
            || state.counters.flush_due()
        {
            last_counter_flush = Some(Instant::now());
            tokio::select! {
                _ = state.counters.flush() => {}
                _ = wait_until_generation_changes(&state, selected.generation) => {}
            }
        }

        // Replication is correctness work, not selected-bucket leader work.
        // Every node may attempt it; all writes are conditional/idempotent. A
        // node that can reach a destination must not sit idle merely because a
        // different node holds the selected bucket's index lease.
        let repl_sweep_forced = state.repl_sweep_requested.load(Ordering::SeqCst);
        let repl_sweep_due = state.buckets.is_multi()
            && (repl_sweep_forced
                || last_repl_sweep.is_none_or(|t| t.elapsed() >= state.repl_sweep_interval));
        if repl_sweep_due && !replication_running.swap(true, Ordering::SeqCst) {
            // Consume the heal request only once a sweep actually starts, so a
            // request arriving while a sweep is already in flight is not lost.
            state.repl_sweep_requested.store(false, Ordering::SeqCst);
            last_repl_sweep = Some(Instant::now());
            let state = state.clone();
            let guard = SweepGuard(replication_running.clone());
            tokio::spawn(async move {
                let _guard = guard;
                if let Err(e) = crate::replicate::sweep_all_markers(&state).await {
                    error!(error=?e, "replicate: marker sweep failed");
                }
            });
        }
        // Rebuild each warm copy's own indexes. Each bucket-local lease keeps
        // duplicate work cheap, but selected-bucket leadership is irrelevant:
        // another node may be the only one with a working path to this bucket.
        if bucket_maintenance_due && !warm_running.swap(true, Ordering::SeqCst) {
            let state = state.clone();
            let leases = warm_leases.clone();
            let selected_index = selected.index;
            let guard = SweepGuard(warm_running.clone());
            tokio::spawn(async move {
                let _guard = guard;
                let jobs = state
                    .buckets
                    .handles()
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| *idx != selected_index)
                    .filter(|(idx, _)| {
                        state.bucket_health.as_ref().is_none_or(|health| {
                            health.bucket_eligible(*idx).unwrap_or(false)
                        })
                    })
                    .map(|(idx, handle)| {
                        let lease = leases[idx].clone();
                        let job_state = state.clone();
                        async move {
                            if job_state.mutations_fenced() {
                                return;
                            }
                            if let Some(lease) = &lease {
                                if !bounded_lease_check(&job_state, idx, lease).await {
                                    return;
                                }
                            }
                            let result = tokio::select! {
                                result = drain_dirty_uncached(&job_state, handle.storage.as_ref()) => result,
                                _ = wait_until_bucket_ineligible(&job_state, idx) => {
                                    Err(anyhow!("warm bucket became topology-ineligible"))
                                }
                            };
                            if let Err(e) = result {
                                error!(bucket=%handle.name, error=?e, "replicate: destination drain failed");
                            }
                        }
                    });
                futures::future::join_all(jobs).await;
            });
        }

        // The lost-marker backstop is likewise safe to duplicate. Run it on
        // every node so one lease holder's asymmetric network path cannot make
        // another node's healthy path useless.
        let reconcile_due = state.buckets.is_multi()
            && last_reconcile.is_none_or(|t| t.elapsed() >= state.reconcile_interval);
        if reconcile_due && !reconcile_running.swap(true, Ordering::SeqCst) {
            last_reconcile = Some(Instant::now());
            let state = state.clone();
            let pinned = selected.clone();
            let guard = SweepGuard(reconcile_running.clone());
            tokio::spawn(async move {
                let _guard = guard;
                if let Err(e) = crate::replicate::reconcile(&state, &pinned).await {
                    error!(error=?e, "reconcile failed");
                }
            });
        }
        if is_leader {
            let spacing = state.reconcile_interval.max(std::time::Duration::from_secs(
                last_audit_secs.load(Ordering::Relaxed) * 10,
            ));
            let due = last_audit.is_none_or(|t| t.elapsed() >= spacing);
            if due && !sweep_running.swap(true, Ordering::SeqCst) {
                last_audit = Some(Instant::now());
                let state = state.clone();
                // This is the exact bucket/generation whose lease authorized the
                // audit. The health task may switch selection before the spawned
                // task starts; never re-pin onto that new bucket under the old
                // bucket's lease.
                let pinned = selected.clone();
                let duration_out = last_audit_secs.clone();
                let guard = SweepGuard(sweep_running.clone());
                tokio::spawn(async move {
                    // Held for the task's lifetime; its Drop clears the flag on
                    // normal return or panic. Bound to a name (not `_`) so it
                    // isn't dropped immediately.
                    let _guard = guard;
                    let started = Instant::now();
                    tokio::select! {
                        result = audit(&state, &pinned, false) => {
                            if let Err(e) = result {
                                error!(error=?e, "audit failed");
                            }
                        }
                        _ = wait_until_generation_changes(&state, pinned.generation) => {
                            warn!(generation=pinned.generation, "audit cancelled after bucket selection changed");
                        }
                    }
                    duration_out.store(started.elapsed().as_secs(), Ordering::Relaxed);
                });
            }
            // tick + compact can run many seconds on S3 with a backlog. Race
            // them against the shutdown signal: a graceful SIGTERM must never be
            // stuck behind a slow batch, or the worker is aborted before it
            // releases the lease — and a skipped release is a lease-TTL write
            // outage on the successor (the very thing release() exists to avoid).
            // Abandoning a rebuild mid-flight is safe: rebuilds are idempotent
            // and the next leader redoes the work.
            let leader_work = async {
                if let Err(e) = tick(&state, &selected).await {
                    error!(error=?e, "worker tick failed");
                }
                // Counter compaction (LEADER only): freeze finished days into one
                // file per shard, write summaries, prune past retention. Cheap most
                // ticks (a list with nothing closeable); gated to the rollup cadence.
                // Inline is fine at the hourly default; spawn it like the audit if a
                // very large corpus ever makes it head-of-line-block the tick.
                if last_counter_compact
                    .is_none_or(|t| t.elapsed() >= state.counters.rollup_interval())
                {
                    last_counter_compact = Some(Instant::now());
                    state.counters.compact().await;
                }
            };
            tokio::select! {
                _ = leader_work => {}
                _ = wait_until_generation_changes(&state, selected.generation) => {}
                _ = shutdown.changed() => break,
            }
        }
        tokio::select! {
            _ = sleep(state.worker_interval) => {}
            _ = state.worker_nudge.notified() => {}
            _ = state.counters.flush_signal() => {}
            _ = shutdown.changed() => break,
        }
    }
    // Flush any buffered counts before exit so a graceful restart loses at most
    // the events of the final partial interval.
    state.counters.flush().await;
    // Graceful exit: hand leadership over instead of leaving successors to
    // wait out the lease TTL (a restart used to be a TTL-long write outage).
    if let Some(lm) = &lease {
        lm.release().await;
    }
    for lease in warm_leases.into_iter().flatten() {
        lease.release().await;
    }
    if let Some(task) = health_task {
        task.abort();
        let _ = task.await;
    }
}

/// Fingerprint shards live here, one JSON map per [`SHARD_CHARS`] character:
/// package → hash of the (key, size, etag) listing its views were built
/// from. They are views of views — regenerable, never trusted over truth. A
/// lost shard merely means its packages rebuild once.
const STATE_PREFIX: &str = "_state/";

/// How often each node refreshes its in-memory inventory from the persisted
/// view. Followers never rebuild, so this is how the leader's value reaches
/// them; a few seconds of homepage lag is fine for a glanceable stat.
const INVENTORY_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Storage key for the registry-inventory view (option 4): a tiny regenerable
/// aggregate every node reads, so followers stay current without a sweep.
fn inventory_key() -> String {
    format!("{STATE_PREFIX}inventory.json")
}

/// Publish the aggregate to `_state/inventory.json` (so other nodes see it) and
/// into the local metrics atomics (so this node's homepage and `/metrics` show
/// it now). Returns whether the persist landed: the local atomics always update,
/// but a storage-write failure returns `false` so the caller can retry rather
/// than drop the change. Best-effort — it must never strand markers or fail a
/// rebuild.
async fn publish_inventory(
    state: &AppState,
    storage: &dyn Storage,
    inv: crate::metrics::Inventory,
) -> bool {
    state
        .metrics
        .set_inventory(inv.projects, inv.releases, inv.files, inv.bytes);
    let bytes = match serde_json::to_vec(&inv) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(error=?e, "inventory: serialize failed");
            return false;
        }
    };
    match storage
        .put_bytes(&inventory_key(), bytes, Some("application/json"))
        .await
    {
        Ok(()) => true,
        Err(e) => {
            warn!(error=?e, "inventory: persist failed (followers will lag)");
            false
        }
    }
}

/// Refresh this node's in-memory inventory atomics from the persisted view.
/// Runs on every node (leader and follower) so the published value propagates.
/// A missing or unparseable object is left alone — the homepage shows nothing
/// until the first sweep writes it (never panics; no `unwrap` on the worker
/// path).
async fn refresh_inventory(state: &AppState) {
    let storage = state.pin().storage.clone();
    if let Ok(bytes) = storage.get_bytes(&inventory_key()).await {
        if let Ok(inv) = serde_json::from_slice::<crate::metrics::Inventory>(&bytes) {
            state
                .metrics
                .set_inventory(inv.projects, inv.releases, inv.files, inv.bytes);
        }
    }
}

/// Audit sweep: detect-and-repair with cost proportional to *churn*, not
/// corpus size. One flat listing per shard (1,000 keys per S3 request)
/// covers truth and views; a package whose listing fingerprint matches the
/// one stored at its last rebuild is provably unchanged — zero reads. Only
/// the diff gets the deep treatment (sidecar reads, view rewrite, sidecar
/// backfill, orphan pruning). `force_deep` ignores stored fingerprints and
/// rebuilds everything — that is `pypiron rebuild-index`.
pub async fn audit(
    state: &AppState,
    pinned: &crate::buckets::Pinned,
    force_deep: bool,
) -> Result<()> {
    let storage = pinned.storage.as_ref();
    let generation = pinned.generation;
    let started = Instant::now();
    let mut live: Vec<String> = Vec::new();
    let mut dead: Vec<String> = Vec::new();
    let mut failures = 0usize;
    let mut rebuilt = 0usize;
    let mut skipped = 0usize;
    let mut pkg_stats: Vec<(String, PkgStat)> = Vec::new();

    // Shards enumerate in parallel — that is what the sharding is for. The
    // bound keeps peak memory at a few shards' worth of listings (a shard is
    // ~1/36th of the corpus).
    const SHARD_CONCURRENCY: usize = 6;
    for chunk in crate::storage::SHARD_CHARS.chunks(SHARD_CONCURRENCY) {
        let audits = chunk
            .iter()
            .map(|shard| audit_shard(state, storage, generation, *shard, force_deep));
        for (shard, result) in chunk.iter().zip(futures::future::join_all(audits).await) {
            match result {
                Ok(result) => {
                    live.extend(result.live);
                    dead.extend(result.dead);
                    rebuilt += result.rebuilt;
                    skipped += result.skipped;
                    failures += result.failures;
                    pkg_stats.extend(result.pkg_stats);
                }
                Err(e) => {
                    error!(shard=%shard, error=?e, "audit: shard failed");
                    failures += 1;
                }
            }
        }
    }

    live.sort();
    live.dedup();
    // Delta + CAS, not a blind overwrite: a package born mid-audit (its name
    // added by the tick) must not be clobbered by our older observation.
    require_generation(state, generation)?;
    update_global_index(state, storage, &live, &dead).await?;
    if failures > 0 {
        return Err(anyhow!("audit finished with {failures} failure(s)"));
    }
    let duration_secs = started.elapsed().as_secs_f64();
    let m = &state.metrics;
    m.reconcile_sweeps
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    m.audit_packages_rebuilt
        .fetch_add(rebuilt as u64, std::sync::atomic::Ordering::Relaxed);
    m.audit_packages_skipped
        .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
    m.set_audit_duration(duration_secs);
    // Authoritatively re-baseline the in-memory map from truth (heals any
    // between-sweep delta drift, restores it after a restart), then publish the
    // map's own totals — the exact value a subsequent tick flush would publish,
    // so the displayed counts never flicker between the audit and tick paths.
    // (`projects` = packages with stored artifacts, not the global name set's
    // `live`, which conservatively keeps failed-rebuild names listed.)
    let totals = {
        let mut inv = state.inventory.lock().await;
        inv.pkgs = pkg_stats.into_iter().collect();
        inv.ready = true;
        inv.dirty = false;
        inv.totals()
    };
    publish_inventory(state, storage, totals).await;
    info!(
        packages = live.len(),
        rebuilt, skipped, duration_secs, "reconcile: sweep complete"
    );
    Ok(())
}

struct ShardAudit {
    live: Vec<String>,
    /// Observed with no artifacts: must not be listed globally.
    dead: Vec<String>,
    rebuilt: usize,
    /// Provably unchanged (fingerprint hit): zero reads spent.
    skipped: usize,
    failures: usize,
    /// Per-package inventory derived from the shard listing (no extra reads):
    /// artifact files (sidecars excluded), bytes, and distinct releases, for
    /// every package with at least one artifact. Summed to re-baseline the
    /// in-memory inventory map authoritatively each sweep.
    pkg_stats: Vec<(String, PkgStat)>,
}

/// Audit every package whose name starts with `shard`.
async fn audit_shard(
    state: &AppState,
    storage: &dyn Storage,
    generation: u64,
    shard: char,
    force_deep: bool,
) -> Result<ShardAudit> {
    let (truth, views) = futures::future::try_join(
        storage.list_all(&format!("{PACKAGES_PREFIX}{shard}")),
        storage.list_all(&format!("{SIMPLE_PREFIX}{shard}")),
    )
    .await?;

    // Group listings by package; the global index files ("index.json" under
    // simple/i...) have no '/' and are skipped — they are handled globally.
    let mut by_pkg: std::collections::BTreeMap<String, (Vec<&ObjectMeta>, Vec<&ObjectMeta>)> =
        std::collections::BTreeMap::new();
    for obj in &truth {
        if let Some(pkg) = key_package(&obj.key, PACKAGES_PREFIX) {
            by_pkg.entry(pkg.to_string()).or_default().0.push(obj);
        }
    }
    for obj in &views {
        if let Some(pkg) = key_package(&obj.key, SIMPLE_PREFIX) {
            by_pkg.entry(pkg.to_string()).or_default().1.push(obj);
        }
    }

    let fp_key = format!("{STATE_PREFIX}fp-{shard}.json");
    let stored: std::collections::HashMap<String, String> = if force_deep {
        Default::default()
    } else {
        match storage.get_bytes(&fp_key).await {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => Default::default(),
        }
    };

    let mut out = ShardAudit {
        live: Vec::new(),
        dead: Vec::new(),
        rebuilt: 0,
        skipped: 0,
        failures: 0,
        pkg_stats: Vec::new(),
    };
    let mut fresh: std::collections::HashMap<String, String> = Default::default();
    let mut packages: Vec<(String, String, bool, bool, Vec<String>)> =
        Vec::with_capacity(by_pkg.len());
    for (pkg, (t, v)) in by_pkg {
        let fp = fingerprint(&t, &v);
        // Count artifacts and distinct versions straight off the listing — the
        // same bytes the fingerprint already walked, so the inventory is free.
        let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
        let members: HashSet<&str> = t
            .iter()
            .filter_map(|obj| obj.key.strip_prefix(&prefix))
            .collect();
        let mut versions: HashSet<String> = HashSet::new();
        let mut file_count = 0u32;
        let mut pkg_bytes = 0u64;
        // A live artifact body sitting beside its own `.tombstone` is a delete
        // that crashed between the tombstone write and the body removal: the
        // index hides it, but the bytes stay downloadable by direct URL. Detect
        // it here from the listing already in hand (zero extra reads — the
        // single-bucket audit stays free) and finish the delete below.
        let mut interrupted_deletes: Vec<String> = Vec::new();
        for obj in &t {
            if let Some(filename) = obj.key.strip_prefix(&prefix) {
                if is_artifact(filename) {
                    file_count += 1;
                    pkg_bytes += obj.size;
                    if let Some(version) = infer_version_from_filename(filename) {
                        versions.insert(version);
                    }
                    // A `.frozen` artifact deliberately retains its canonical
                    // body beside a tombstone as evidence; only a bare tombstone
                    // (no freeze) beside a live body is a crashed delete.
                    if members.contains(format!("{filename}{TOMBSTONE_SUFFIX}").as_str())
                        && !members.contains(format!("{filename}{FROZEN_SUFFIX}").as_str())
                    {
                        interrupted_deletes.push(filename.to_string());
                    }
                }
            }
        }
        if file_count > 0 {
            out.pkg_stats.push((
                pkg.clone(),
                PkgStat {
                    files: file_count,
                    releases: versions.len() as u32,
                    bytes: pkg_bytes,
                },
            ));
        }
        let has_package_view = v.iter().any(|object| {
            object.key.ends_with("/index.json") || object.key.ends_with("/index.html")
        });
        packages.push((
            pkg,
            fp,
            file_count > 0,
            has_package_view,
            interrupted_deletes,
        ));
    }

    for chunk in packages.chunks(PACKAGE_SWEEP_CONCURRENCY) {
        let jobs = chunk
            .iter()
            .map(|(pkg, fp, has_artifacts, has_package_view, interrupted_deletes)| {
            let fingerprint_unchanged =
                stored.get(pkg.as_str()) == Some(fp) && interrupted_deletes.is_empty();
            let (pkg, fp, has_artifacts, has_package_view, interrupted_deletes) = (
                pkg.clone(),
                fp.clone(),
                *has_artifacts,
                *has_package_view,
                interrupted_deletes.clone(),
            );
            async move {
                if let Err(e) = require_generation(state, generation) {
                    error!(package=%pkg, error=?e, "audit: selection changed before package batch");
                    return (pkg, None, has_artifacts, false, true);
                }
                if state.buckets.is_multi() {
                    match crate::origin::read_origin_observation(storage, &pkg).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!(package=%pkg, error=?e, "audit: origin unreadable; deferring maintenance");
                            return (pkg, None, has_artifacts, false, true);
                        }
                    }
                }
                let mut maintenance_failed = false;
                let mut maintenance_changed = false;
                // Complete any delete that crashed after its tombstone but before
                // its body was removed (single- and multi-bucket alike). Runs
                // only for the rare package the listing flagged, so the common
                // path pays nothing.
                if !interrupted_deletes.is_empty() {
                    match crate::tombstone::complete_interrupted_deletes(
                        storage,
                        &pkg,
                        &interrupted_deletes,
                    )
                    .await
                    {
                        Ok(0) => {}
                        Ok(count) => {
                            maintenance_changed = true;
                            error!(package=%pkg, count, "audit: completed interrupted delete(s) — orphaned body dropped");
                        }
                        Err(e) => {
                            maintenance_failed = true;
                            error!(package=%pkg, error=?e, "audit: completing interrupted delete failed");
                        }
                    }
                }
                if has_artifacts {
                    state
                        .empty_origin_observations
                        .lock()
                        .await
                        .remove(&(generation, pkg.clone()));
                    if state.buckets.is_multi() {
                        match crate::replicate::quarantine_mirror_artifacts(storage, &pkg).await {
                            Ok(0) => {}
                            Ok(count) => {
                                maintenance_changed = true;
                                error!(package=%pkg, count, "audit: quarantined mirror artifacts under private claim");
                            }
                            Err(e) => {
                                maintenance_failed = true;
                                error!(package=%pkg, error=?e, "audit: private-claim quarantine failed");
                            }
                        }
                    }
                } else if let Err(e) =
                    reclaim_empty_mirror_claim(state, storage, &pkg, generation).await
                {
                    error!(package=%pkg, error=?e, "audit: empty mirror claim proof failed");
                    maintenance_failed = true;
                }
                let unchanged = fingerprint_unchanged && !maintenance_changed;
                if unchanged {
                    // Provably unchanged since the fingerprint was written.
                    // Logical liveness follows the materialized package view,
                    // not physical artifacts: frozen/quarantined canonical
                    // evidence deliberately stays in `packages/` while dead.
                    return (
                        pkg,
                        Some(fp),
                        has_package_view,
                        false,
                        maintenance_failed,
                    );
                }
                match rebuild_package(state, storage, &pkg).await {
                    Ok(live_now) => {
                        // Fingerprint what the rebuild actually saw/wrote, not
                        // the pre-rebuild listing — two cheap per-package lists.
                        let new_fp = package_fingerprint(storage, &pkg).await.ok();
                        (pkg, new_fp, live_now, true, maintenance_failed)
                    }
                    Err(e) => {
                        // Conservative on failure: keep the package listed and
                        // its views rather than pruning on a bad observation.
                        error!(package=%pkg, error=?e, "audit: package rebuild failed");
                        (pkg, None, has_artifacts, false, true)
                    }
                }
            }
        });
        for (pkg, fp, live_now, was_rebuilt, failed) in futures::future::join_all(jobs).await {
            if let Some(fp) = fp {
                fresh.insert(pkg.clone(), fp);
            }
            if live_now || failed {
                out.live.push(pkg);
            } else {
                out.dead.push(pkg);
            }
            out.rebuilt += was_rebuilt as usize;
            out.skipped += (!was_rebuilt && !failed) as usize;
            out.failures += failed as usize;
        }
    }

    // `fresh` now holds exactly the packages that exist; anything left in
    // `stored` is gone and simply drops out of the rewritten shard.
    require_generation(state, generation)?;
    let bytes = serde_json::to_vec(&std::collections::BTreeMap::from_iter(fresh.iter()))?;
    put_if_changed(state, storage, &fp_key, bytes, "application/json").await?;
    Ok(out)
}

fn require_generation(state: &AppState, expected: u64) -> Result<()> {
    if state.mutations_fenced() {
        bail!("bucket topology mismatch; audit mutations are fenced");
    }
    let current = state.pin().generation;
    if current != expected {
        bail!("bucket selection generation changed from {expected} to {current}");
    }
    Ok(())
}

/// The package a key belongs to: first path segment after `prefix`.
fn key_package<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
    key.strip_prefix(prefix)?.split_once('/').map(|(p, _)| p)
}

/// Hash of everything a package's views are derived from, as observed in a
/// flat listing: truth objects (artifacts decide membership, sidecar etags
/// carry yank/metadata changes) plus the view objects themselves (so
/// out-of-band view deletion or tampering is also caught).
fn fingerprint(truth: &[&ObjectMeta], views: &[&ObjectMeta]) -> String {
    let mut hasher = Sha256::new();
    for obj in truth.iter().chain(views.iter()) {
        hasher.update(&obj.key);
        hasher.update(obj.size.to_le_bytes());
        hasher.update(&obj.etag);
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

/// Re-derive one package's fingerprint from fresh listings (post-rebuild).
async fn package_fingerprint(storage: &dyn Storage, pkg: &str) -> Result<String> {
    let (truth, views) = futures::future::try_join(
        storage.list_all(&format!("{PACKAGES_PREFIX}{pkg}/")),
        storage.list_all(&format!("{SIMPLE_PREFIX}{pkg}/")),
    )
    .await?;
    Ok(fingerprint(
        &truth.iter().collect::<Vec<_>>(),
        &views.iter().collect::<Vec<_>>(),
    ))
}

async fn tick(state: &Arc<AppState>, pinned: &crate::buckets::Pinned) -> Result<()> {
    // The caller passes the exact pin whose bucket-local lease authorized this
    // tick. Selection may change concurrently, but this operation stays on that
    // handle; the per-package rebuilds spawn with an owned clone of it.
    let storage = pinned.storage.as_ref();
    let entries = storage.list_dir_entries(DIRTY_PREFIX).await?;
    if entries.is_empty() {
        return Ok(());
    }

    let work = consumable_dirty_work(
        &entries,
        time::OffsetDateTime::now_utc(),
        state.intent_grace,
    );
    if work.is_empty() {
        return Ok(());
    }
    info!(
        packages = work.len(),
        markers = entries.len(),
        "worker: processing dirty markers"
    );

    // Packages drain with bounded concurrency: rebuilds are idempotent, so
    // parallelism across packages is free. A semaphore (not chunked join_all)
    // so one slow 5,000-file rebuild never head-of-line blocks the tiny
    // rebuilds behind it — that stall showed up as a 73s visibility p99 for
    // unrelated packages. One failing package must not starve the namespace.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(PACKAGE_SWEEP_CONCURRENCY));
    let mut handles = Vec::with_capacity(work.len());
    for DirtyWork {
        package,
        keys,
        stale_intents,
    } in work
    {
        let state = state.clone();
        let semaphore = semaphore.clone();
        // Move an owned handle into the task — the tick's pin, so every rebuild
        // this tick spawns writes to the same bucket it captured at entry.
        let storage = pinned.storage.clone();
        handles.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await;
            let rebuilt = match rebuild_package(&state, storage.as_ref(), &package).await {
                Ok(has_artifacts) => Some(has_artifacts),
                Err(e) => {
                    error!(package=%package, error=?e, "rebuild failed; markers retained for retry");
                    None
                }
            };
            (package, keys, stale_intents, rebuilt)
        }));
    }
    let mut failures = 0usize;
    let mut healed = 0u64;
    let (mut adds, mut removes) = (Vec::new(), Vec::new());
    let mut consumed: Vec<String> = Vec::new();
    for handle in handles {
        match handle.await {
            Ok((pkg, keys, stale_intents, Some(live_now))) => {
                // Markers for this package are now consumed; the stale intents
                // among them are healed crashed writers.
                healed += stale_intents;
                if live_now {
                    adds.push(pkg);
                } else {
                    removes.push(pkg);
                }
                consumed.extend(keys);
            }
            _ => failures += 1,
        }
    }
    if healed > 0 {
        state
            .metrics
            .stale_intents_healed
            .fetch_add(healed, std::sync::atomic::Ordering::Relaxed);
    }
    // One batched global-index pass per tick: mass ingest of N new packages
    // rewrites the (corpus-sized) global views once, not N times.
    update_global_index(state, storage, &adds, &removes).await?;
    // Flush the inventory once per tick if this tick's rebuilds changed it
    // (and a sweep has set the full baseline — before that the map is partial,
    // so we let the audit publish). Batched like the global index: one write
    // per tick, never one per package.
    let pending = {
        let mut inv = state.inventory.lock().await;
        if inv.ready && inv.dirty {
            inv.dirty = false;
            Some(inv.totals())
        } else {
            None
        }
    };
    if let Some(totals) = pending {
        if !publish_inventory(state, storage, totals).await {
            // Persist failed; re-arm dirty so the next tick retries instead of
            // waiting for the next change or sweep. (A concurrent rebuild may
            // have re-set it already — harmless, the flush is idempotent.)
            state.inventory.lock().await.dirty = true;
        }
    }
    // Markers are consumed LAST — they are the transaction log, and must
    // outlive every write they announce (package views above, global index
    // here). Rebuild-then-delete is race-free because keys are unique: an
    // event arriving during the rebuild is a new key and survives. A crash
    // anywhere before this line replays the whole tick — idempotent, so the
    // only cost is repeated work, never a lost update.
    if let Err(e) = storage.delete_keys(&consumed).await {
        warn!(error=?e, "could not consume markers; rebuilds will repeat");
    }
    if failures > 0 {
        return Err(anyhow!("{failures} package(s) failed this tick"));
    }
    Ok(())
}

/// Regenerate one package's indexes from a storage listing.
/// Returns whether the package still has artifacts; with none, its indexes
/// are removed (index first, per the ordering invariant).
pub async fn rebuild_package(state: &AppState, storage: &dyn Storage, pkg: &str) -> Result<bool> {
    rebuild_package_excluding(state, storage, pkg, None).await
}

/// Like `rebuild_package`, but omitting one filename from the views. Deletion
/// uses this to drop the file from the index *before* removing the artifact —
/// views may lag truth but never lead it.
pub async fn rebuild_package_excluding(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    omit: Option<&str>,
) -> Result<bool> {
    let (live, raw) = rebuild_package_indexes(state, storage, pkg, omit).await?;
    // Maintain the in-memory inventory. This is the one choke point every
    // rebuild against the *selected* bucket — tick, audit, delete — flows
    // through, and `upsert` is an idempotent absolute set, so concurrent or
    // repeated rebuilds never double-count. The audit re-baselines the whole
    // map periodically.
    state
        .inventory
        .lock()
        .await
        .upsert(pkg, PkgStat::from_raw(&raw));
    Ok(live)
}

/// Rebuild only a package's index views from `storage`'s own truth, touching no
/// node-local inventory. `rebuild_package_excluding` layers the selected
/// bucket's inventory on top; the replicator (src/replicate.rs) calls this
/// directly against a *non-selected* destination bucket, where mixing that
/// bucket's counts into the node's inventory (or its name cache) would be wrong.
/// Returns `(still_live, raw_artifacts)`.
pub async fn rebuild_package_indexes(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    omit: Option<&str>,
) -> Result<(bool, Vec<(String, u64)>)> {
    if !state.buckets.is_multi() {
        return rebuild_package_indexes_inner(state, storage, pkg, omit).await;
    }

    // Package view generation reads several independently-versioned objects.
    // Join the writer-intent fence across that read set and the final index PUT.
    // This maintenance intent is removed after the derived view is complete; a
    // process crash leaves it behind for the ordinary stale-intent healer.
    let nonce = mark_intent(storage, pkg).await?;
    let result = rebuild_package_indexes_inner(state, storage, pkg, omit).await;
    match result {
        Ok(value) => {
            clear_intent(storage, pkg, &nonce).await?;
            Ok(value)
        }
        Err(error) => {
            // The failed rebuild may already have changed one of the derived
            // views. Pair the intent so the ordinary worker retries it; never
            // erase the only crash-recovery event on an error path.
            let _ = mark_commit(storage, pkg, &nonce).await;
            Err(error)
        }
    }
}

async fn rebuild_package_indexes_inner(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    omit: Option<&str>,
) -> Result<(bool, Vec<(String, u64)>)> {
    state
        .metrics
        .index_rebuilds
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (mut files, mut raw) = list_artifacts_for_claim(storage, pkg, true).await?;
    if let Some(omit) = omit {
        files.retain(|f| f.filename != omit);
        raw.retain(|(filename, _)| filename != omit);
    }
    // Index membership keys on the *renderable* files; inventory keys on the
    // *stored* files (raw). They differ only for a corrupt-sidecar file, which
    // is dropped from the index but still counted as stored — matching the
    // audit, so the two inventory writers can't disagree.
    let live = !files.is_empty();
    if live {
        write_pkg_indexes(state, storage, pkg, &files).await?;
    } else {
        let keys = [
            format!("{SIMPLE_PREFIX}{pkg}/index.html"),
            format!("{SIMPLE_PREFIX}{pkg}/index.json"),
        ];
        storage.delete_keys(&keys).await?;
        for key in &keys {
            state.index_cache.invalidate(key);
        }
    }
    Ok((live, raw))
}

/// One package's contribution to the registry inventory: artifact files in
/// storage and their bytes, with distinct inferred-version releases. `u32`
/// counts are ample (thousands of files per package at most); bytes need `u64`.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(crate) struct PkgStat {
    files: u32,
    releases: u32,
    bytes: u64,
}

impl PkgStat {
    /// Stats for a package's `(filename, size)` artifacts as they exist in
    /// storage — counted straight off the raw listing, the audit's exact method
    /// ([`audit_shard`]). Counting the *stored* files (not the index-renderable
    /// subset) is what keeps the tick and audit writers in agreement even for a
    /// file whose sidecar is corrupt: the audit counts it off the listing, so
    /// this must too. `omit` (a being-deleted filename) is already applied by
    /// the caller.
    fn from_raw(artifacts: &[(String, u64)]) -> Self {
        let mut versions: HashSet<String> = HashSet::new();
        let mut bytes = 0u64;
        for (filename, size) in artifacts {
            bytes += size;
            if let Some(v) = infer_version_from_filename(filename) {
                versions.insert(v);
            }
        }
        PkgStat {
            files: artifacts.len() as u32,
            releases: versions.len() as u32,
            bytes,
        }
    }
}

/// In-memory per-package inventory, the truth-faithful source of "old" for the
/// between-sweep delta. Maintained on the leader by every rebuild (idempotent
/// absolute set), re-baselined wholesale by each audit, and summed to publish
/// `_state/inventory.json`. `ready` gates publishing until the first audit has
/// established the full baseline; `dirty` marks an unpublished change.
#[derive(Default)]
pub(crate) struct InventoryMap {
    pkgs: std::collections::HashMap<String, PkgStat>,
    ready: bool,
    dirty: bool,
}

impl InventoryMap {
    /// Idempotent: set this package's absolute stats and flag a change. An
    /// empty package (no artifacts) is removed. Safe from any caller — two
    /// rebuilds of the same package converge to the same map.
    fn upsert(&mut self, pkg: &str, stat: PkgStat) {
        let changed = if stat.files == 0 {
            self.pkgs.remove(pkg).is_some()
        } else {
            self.pkgs.insert(pkg.to_string(), stat) != Some(stat)
        };
        self.dirty |= changed;
    }

    /// Current aggregate, summed off the map. O(projects); only paid when we
    /// publish (a changed tick or an audit), not per request.
    fn totals(&self) -> crate::metrics::Inventory {
        let mut inv = crate::metrics::Inventory {
            projects: self.pkgs.len() as u64,
            ..Default::default()
        };
        for s in self.pkgs.values() {
            inv.releases += s.releases as u64;
            inv.files += s.files as u64;
            inv.bytes += s.bytes;
        }
        inv
    }
}

/// The in-memory copy of the global index's name set, pinned to the ETag of
/// the materialized JSON it was loaded from (None on backends without ETags).
/// At 780k names a membership check against storage costs a 45 MB GET +
/// parse; against this it costs a hash lookup.
pub(crate) struct GlobalNames {
    etag: Option<String>,
    names: HashSet<String>,
}

/// All hosted package names, sorted — the human package browser's listing.
/// Loads the global name set into memory on first use, so a freshly booted node
/// still answers.
pub async fn global_package_names(state: &AppState, storage: &dyn Storage) -> Result<Vec<String>> {
    let mut guard = state.global_names.lock().await;
    if guard.is_none() {
        *guard = Some(load_global_names(storage).await?);
    }
    let mut names: Vec<String> = guard
        .as_ref()
        .expect("just loaded")
        .names
        .iter()
        .cloned()
        .collect();
    names.sort();
    Ok(names)
}

/// The global index only changes when the *set of package names* changes —
/// check membership in memory first; the common case (an upload to a known
/// package) costs nothing. Real changes are applied as a delta and written
/// back under CAS (`If-Match`) where the backend supports it, so two nodes
/// adding different names can never clobber each other: the loser reloads
/// and reapplies. Deltas batch per worker tick, so mass ingest rewrites the
/// (large) global index once per tick, not once per package.
async fn update_global_index(
    state: &AppState,
    storage: &dyn Storage,
    adds: &[String],
    removes: &[String],
) -> Result<()> {
    if adds.is_empty() && removes.is_empty() {
        return Ok(());
    }
    let mut guard = state.global_names.lock().await;
    // Once we lose a CAS we have already written an optimistic HTML for a name
    // set that lost. If the reload then makes our delta a no-op (the winner
    // already added our name), `changed` is false and we would return leaving
    // that stale HTML as the final write — a drift nothing else heals (the
    // audit reaches the same `changed` gate). So on that path, reconcile HTML
    // to the now-canonical set before returning.
    let mut wrote_optimistic_html = false;
    for _attempt in 0..4 {
        if guard.is_none() {
            *guard = Some(load_global_names(storage).await?);
        }
        let cached = guard.as_mut().expect("just loaded");
        let mut changed = false;
        for pkg in adds {
            changed |= cached.names.insert(pkg.clone());
        }
        for pkg in removes {
            changed |= cached.names.remove(pkg);
        }
        if !changed {
            if wrote_optimistic_html {
                let mut packages: Vec<String> = cached.names.iter().cloned().collect();
                packages.sort();
                put_if_changed(
                    state,
                    storage,
                    &format!("{SIMPLE_PREFIX}index.html"),
                    pep503_global_html(&packages).into_bytes(),
                    SIMPLE_HTML_CONTENT_TYPE,
                )
                .await?;
            }
            return Ok(());
        }
        let mut packages: Vec<String> = cached.names.iter().cloned().collect();
        packages.sort();
        match write_global_indexes_cas(state, storage, &packages, &cached.etag.clone()).await? {
            CasOutcome::Won(new_etag) => {
                if let Some(cached) = guard.as_mut() {
                    // Pin the ETag the conditional write itself returned, not one
                    // from a follow-up GET — a peer could land a write between the
                    // two and we'd pin its ETag against our stale name set.
                    cached.etag = new_etag;
                }
                return Ok(());
            }
            CasOutcome::Lost => {}
        }
        // Lost the CAS to a peer: another node updated the name set under us.
        // We already wrote an optimistic HTML this iteration; remember that so
        // a subsequent no-op reload still reconciles it. Count the conflict
        // (operators watch this to confirm dual leadership converges rather
        // than corrupts), then drop the cache, reload, reapply the delta.
        wrote_optimistic_html = true;
        state
            .metrics
            .global_cas_conflicts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        warn!(
            adds = adds.len(),
            removes = removes.len(),
            "global index CAS lost to a peer; reloading and retrying"
        );
        *guard = None;
    }
    bail!("global index CAS retries exhausted")
}

/// Consume the `_dirty/` markers the replicator dropped in a *destination*
/// bucket and rebuild that bucket's own indexes from its own truth (design §4:
/// indexes are per-bucket derived views, never written cross-bucket). This is
/// what keeps a warm copy's indexes fresh. Cache-free on purpose: it must
/// not disturb the node-local name/inventory caches, which describe the
/// *selected* bucket. A package that fails to rebuild keeps its markers for the
/// next pass; a package whose global membership flips (first file in, or last
/// file out) is threaded into the destination's own global index.
pub async fn drain_dirty_uncached(state: &AppState, storage: &dyn Storage) -> Result<()> {
    let entries = storage.list_dir_entries(DIRTY_PREFIX).await?;
    if entries.is_empty() {
        return Ok(());
    }
    let work = consumable_dirty_work(
        &entries,
        time::OffsetDateTime::now_utc(),
        state.intent_grace,
    );
    let mut adds = Vec::new();
    let mut removes = Vec::new();
    let mut consumed = Vec::new();
    let mut healed = 0;
    for DirtyWork {
        package,
        keys,
        stale_intents,
    } in work
    {
        // A cheap HEAD decides whether global membership can flip, so the common
        // "another file added to a package already listed" case never pays the
        // full global-index read below.
        let existed = storage
            .head_exists(&format!("{SIMPLE_PREFIX}{package}/index.json"))
            .await
            .unwrap_or(false);
        match rebuild_package_indexes(state, storage, &package, None).await {
            Ok((live, _)) => {
                if live && !existed {
                    adds.push(package);
                } else if !live && existed {
                    removes.push(package);
                }
                healed += stale_intents;
                consumed.extend(keys);
            }
            Err(e) => {
                error!(package=%package, error=?e, "replicate: destination rebuild failed; markers retained")
            }
        }
    }
    if healed > 0 {
        state
            .metrics
            .stale_intents_healed
            .fetch_add(healed, std::sync::atomic::Ordering::Relaxed);
    }
    update_global_index_uncached(state, storage, &adds, &removes).await?;
    if let Err(e) = storage.delete_keys(&consumed).await {
        warn!(error=?e, "replicate: could not consume destination dirty markers");
    }
    Ok(())
}

/// Apply a package-name delta to a bucket's own global index, reading and
/// writing that bucket's `simple/index.{json,html}` directly — never through the
/// node-local name cache, which is pinned to the *selected* bucket. The
/// replicator uses this for destination buckets on a genuine membership flip
/// only. Single-writer in P3 (one node rebuilds every warm copy), so a plain
/// read-modify-write is safe; P4's per-bucket leaders drive each bucket's own
/// cached CAS path instead.
pub async fn update_global_index_uncached(
    state: &AppState,
    storage: &dyn Storage,
    adds: &[String],
    removes: &[String],
) -> Result<()> {
    if adds.is_empty() && removes.is_empty() {
        return Ok(());
    }
    let mut names = load_global_names(storage).await?.names;
    let mut changed = false;
    for pkg in adds {
        changed |= names.insert(pkg.clone());
    }
    for pkg in removes {
        changed |= names.remove(pkg);
    }
    if !changed {
        return Ok(());
    }
    let mut packages: Vec<String> = names.into_iter().collect();
    packages.sort();
    write_global_indexes(state, storage, &packages).await
}

/// Load the global name set (and its ETag) from the materialized JSON.
async fn load_global_names(storage: &dyn Storage) -> Result<GlobalNames> {
    let key = format!("{SIMPLE_PREFIX}index.json");
    let (bytes, etag) = if storage.supports_leases() {
        match storage.get_with_etag(&key).await? {
            Some((bytes, etag)) => (bytes, Some(etag)),
            None => (Vec::new(), None),
        }
    } else {
        // A missing index is an empty set (no packages yet); any other read
        // error must propagate. Swallowing a transient I/O error to empty here
        // would let the caller write back a near-empty global index, truncating
        // the package list off a phantom "zero packages" observation.
        match storage.get_bytes(&key).await {
            Ok(bytes) => (bytes, None),
            Err(e) if is_not_found(&e) => (Vec::new(), None),
            Err(e) => return Err(e),
        }
    };
    #[derive(serde::Deserialize)]
    struct Global {
        projects: Vec<Project>,
    }
    #[derive(serde::Deserialize)]
    struct Project {
        name: String,
    }
    let names = match serde_json::from_slice::<Global>(&bytes) {
        Ok(g) => g.projects.into_iter().map(|p| p.name).collect(),
        Err(_) => HashSet::new(),
    };
    Ok(GlobalNames { etag, names })
}

/// Outcome of a global-index conditional write.
enum CasOutcome {
    /// Won; carries the authoritative new ETag (`None` on non-CAS disk backends).
    Won(Option<String>),
    /// Lost the conditional write to a concurrent leader; caller should reload.
    Lost,
}

/// Write both global views. The canonical JSON — the one `changed` detection
/// reloads from — is written LAST, under CAS where supported, so that a crash
/// between the two writes is healed by replay: stale JSON re-detects the change
/// and rewrites both. (JSON-first stranded a stale HTML that the name-set-change
/// gate never revisited without an audit; the disk path already orders it this
/// way.) HTML is last-writer-wins; a racing loser rewrites it on its retry, so
/// the iteration whose JSON CAS finally wins always left a matching HTML.
/// Returns `CasOutcome::Lost` when the conditional write lost the race, else
/// `CasOutcome::Won` with the ETag the put itself returned.
async fn write_global_indexes_cas(
    state: &AppState,
    storage: &dyn Storage,
    packages: &[String],
    expected_etag: &Option<String>,
) -> Result<CasOutcome> {
    let json_key = format!("{SIMPLE_PREFIX}index.json");
    let json = pep691_global_json(packages).into_bytes();
    if storage.supports_leases() {
        // HTML first: derived from the same list, unconditional, idempotent.
        let html_key = format!("{SIMPLE_PREFIX}index.html");
        storage
            .put_bytes(
                &html_key,
                pep503_global_html(packages).into_bytes(),
                Some(SIMPLE_HTML_CONTENT_TYPE),
            )
            .await?;
        state.index_cache.invalidate(&html_key);
        // Canonical JSON last, under CAS: its success is what consumes markers.
        let outcome = match expected_etag {
            Some(etag) => storage.put_if_match(&json_key, etag, json).await?,
            None => storage.put_if_none_match(&json_key, json).await?,
        };
        let Some(new_etag) = outcome else {
            return Ok(CasOutcome::Lost);
        };
        state.index_cache.invalidate(&json_key);
        return Ok(CasOutcome::Won(Some(new_etag)));
    }
    write_global_indexes(state, storage, packages).await?;
    Ok(CasOutcome::Won(None))
}

/// List a package's artifacts with metadata from sidecars — O(files), no hashing.
/// Artifacts without a sidecar (legacy files) get one backfilled, hashing once.
/// Also returns the *raw* `(filename, size)` of every artifact in storage — the
/// inventory counts off this, not the metadata `Vec`, because a corrupt-sidecar
/// file is dropped from the index (`load_file_metadata` returns `None`) yet
/// still occupies storage, and the audit counts it off the listing.
pub async fn list_artifacts(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<(Vec<FileMetadata>, Vec<(String, u64)>)> {
    list_artifacts_for_claim(storage, pkg, true).await
}

/// Request-path project rendering in multi-bucket mode never mutates truth. The
/// background worker owns legacy sidecar backfill; this view simply omits an
/// untyped artifact for now.
pub async fn list_artifacts_readonly(
    storage: &dyn Storage,
    pkg: &str,
) -> Result<(Vec<FileMetadata>, Vec<(String, u64)>)> {
    list_artifacts_for_claim(storage, pkg, false).await
}

async fn list_artifacts_for_claim(
    storage: &dyn Storage,
    pkg: &str,
    backfill_missing: bool,
) -> Result<(Vec<FileMetadata>, Vec<(String, u64)>)> {
    let pkg_origin = match crate::origin::read_origin_claim(storage, pkg).await? {
        Some(crate::origin::OriginState::Private) => Some(crate::origin::PRIVATE),
        Some(crate::origin::OriginState::Mirror) => Some(crate::origin::MIRROR),
        Some(crate::origin::OriginState::Unclaimed) | None => None,
    };
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let entries = storage.list_dir_entries(&prefix).await?;
    let names: HashSet<&str> = entries
        .iter()
        .filter_map(|e| e.key.strip_prefix(&prefix))
        .collect();
    // Tombstoned filenames leave every index (dev/MULTIBUCKET.md §6.4): a delete
    // that crashed after the tombstone but before the artifact removal converges
    // to "gone" here, rather than resurrecting the file.
    let tombstoned: HashSet<&str> = names
        .iter()
        .filter_map(|f| f.strip_suffix(TOMBSTONE_SUFFIX))
        .collect();
    // Frozen filenames are likewise suppressed (dev/MULTIBUCKET.md §6.3): a
    // byte conflict moved both bodies to `_quarantine/` and dropped a `.frozen`
    // marker; the name must not resolve on any bucket until a human resolves it.
    let frozen: HashSet<&str> = names
        .iter()
        .filter_map(|f| f.strip_suffix(FROZEN_SUFFIX))
        .collect();
    // Sidecar reads fan out with bounded concurrency: a 2,000-file package
    // costs 2,000 GETs, and doing them serially put rebuilds at minutes of
    // wall clock on S3. Chunked join_all keeps listing order — index output
    // must stay deterministic.
    let raw: Vec<(String, u64)> = entries
        .iter()
        .filter_map(|entry| {
            let filename = entry.key.strip_prefix(&prefix)?;
            is_artifact(filename).then_some((filename.to_string(), entry.size))
        })
        .collect();
    let artifacts: Vec<(&FileEntry, &str)> = entries
        .iter()
        .filter_map(|entry| {
            let filename = entry.key.strip_prefix(&prefix)?;
            (is_artifact(filename) && !tombstoned.contains(filename) && !frozen.contains(filename))
                .then_some((entry, filename))
        })
        .collect();
    // Read the package claim once. Besides typing a legacy sidecar backfill, it
    // suppresses a typed mirror record that finished after the claim became
    // private. Such bytes remain inert until replication quarantines them; they
    // must never be rendered or backfilled as fabricated private truth.
    let mut metadata = Vec::with_capacity(artifacts.len());
    for chunk in artifacts.chunks(SIDECAR_READ_CONCURRENCY) {
        let loaded = futures::future::join_all(chunk.iter().map(|(entry, filename)| {
            load_file_metadata(
                storage,
                entry,
                filename,
                &names,
                pkg_origin,
                backfill_missing,
            )
        }))
        .await;
        metadata.extend(loaded.into_iter().flatten());
    }
    Ok((metadata, raw))
}

/// Load one artifact's index entry from its sidecar (backfilling if absent).
/// None means "leave it out of the index" — reasons logged inside.
async fn load_file_metadata(
    storage: &dyn Storage,
    entry: &FileEntry,
    filename: &str,
    names: &HashSet<&str>,
    pkg_origin: Option<&str>,
    backfill_missing: bool,
) -> Option<FileMetadata> {
    let has_sidecar = names.contains(format!("{filename}{SIDECAR_SUFFIX}").as_str());
    let mirror_quarantined =
        names.contains(format!("{filename}{}", crate::sidecar::MIRROR_QUARANTINED_SUFFIX).as_str());
    if mirror_quarantined && !has_sidecar {
        warn!(key=%entry.key, "quarantined mirror artifact has no sidecar; omitting from index");
        return None;
    }
    let sc = if has_sidecar {
        match read_sidecar(storage, &entry.key).await {
            Ok(sc) => sc,
            Err(e) => {
                // A present-but-unreadable sidecar is corruption, not a
                // legacy file. Backfilling would fabricate fresh metadata
                // over it — silently resetting a security yank to false.
                // Leave the file out of the index until an operator looks.
                error!(error=?e, key=%entry.key, "corrupt sidecar; omitting file from index (will not fabricate metadata)");
                return None;
            }
        }
    } else if backfill_missing {
        match backfill_sidecar(storage, entry, filename, pkg_origin).await {
            Ok(sc) => sc,
            Err(e) => {
                warn!(error=?e, key=%entry.key, "could not backfill sidecar; skipping file");
                return None;
            }
        }
    } else {
        return None;
    };
    if pkg_origin == Some(crate::origin::PRIVATE)
        && sc.origin.as_deref() == Some(crate::origin::MIRROR)
    {
        warn!(key=%entry.key, "mirror artifact under private package claim; omitting from index");
        return None;
    }
    if mirror_quarantined && sc.origin.as_deref() != Some(crate::origin::PRIVATE) {
        warn!(key=%entry.key, "quarantined mirror artifact remains non-private; omitting from index");
        return None;
    }
    let core_metadata = names.contains(format!("{filename}{METADATA_SUFFIX}").as_str());
    let provenance = names.contains(format!("{filename}{PROVENANCE_SUFFIX}").as_str());
    Some(FileMetadata::from_sidecar(
        filename,
        sc,
        core_metadata,
        provenance,
    ))
}

async fn read_sidecar(storage: &dyn Storage, artifact_key: &str) -> Result<Sidecar> {
    let bytes = storage.get_bytes(&sidecar_key(artifact_key)).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Hash-once-and-backfill for files that predate write-time sidecars.
/// Storage last-modified is the upload-time fallback (correct by construction
/// for direct uploads — filenames are immutable, so written exactly once).
///
/// Create-only, never overwrite: "missing" was observed in a listing that may
/// already be stale, and a concurrent upload's real sidecar (true timestamp,
/// yank state) must always beat this fabricated one. Losing the race means
/// the real sidecar exists — read and use it.
async fn backfill_sidecar(
    storage: &dyn Storage,
    entry: &FileEntry,
    filename: &str,
    pkg_origin: Option<&str>,
) -> Result<Sidecar> {
    let bytes = storage.get_bytes(&entry.key).await?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let sc = Sidecar {
        sha256: format!("{:x}", hasher.finalize()),
        size: entry.size,
        version: infer_version_from_filename(filename).unwrap_or_default(),
        upload_time: entry.last_modified.clone().unwrap_or_default(),
        requires_python: None,
        yanked: Yanked::Flag(false),
        // Fill the per-artifact origin from the package-level claim (§4); a
        // legacy artifact predates the field, so the claim is the truth.
        origin: pkg_origin.map(str::to_string),
        upload_epoch_ms: None,
        yank_epoch: 0,
    };
    let created = storage
        .put_if_absent(
            &sidecar_key(&entry.key),
            serde_json::to_vec(&sc)?,
            Some("application/json"),
        )
        .await?;
    if !created {
        return read_sidecar(storage, &entry.key).await;
    }
    info!(key=%entry.key, "backfilled sidecar");
    Ok(sc)
}

/// Write only if the stored object differs — idempotent rebuilds shouldn't
/// touch storage (or bump mtimes/ETags) when nothing changed. A real write
/// invalidates the in-process index cache so same-node reads are fresh
/// immediately (other nodes are bounded by the cache TTL).
async fn put_if_changed(
    state: &AppState,
    storage: &dyn Storage,
    key: &str,
    bytes: Vec<u8>,
    ct: &str,
) -> Result<()> {
    if let Ok(current) = storage.get_bytes(key).await {
        if current == bytes {
            return Ok(());
        }
    }
    storage.put_bytes(key, bytes, Some(ct)).await?;
    state.index_cache.invalidate(key);
    Ok(())
}

async fn write_pkg_indexes(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    files: &[FileMetadata],
) -> Result<()> {
    // Status is per-project truth (PEP 792). A read error propagates — we
    // re-render against the prior index rather than assume `active` and, say,
    // re-expose links for a project that should be quarantined.
    let status = crate::status::read_status(storage, pkg).await?;
    // Quarantine omits file links; the delete-vs-render decision upstream still
    // keys on the real artifact count, so a quarantined project keeps a
    // status-bearing (link-free) page instead of 404ing.
    let render_files: &[FileMetadata] = if status.status.blocks_downloads() {
        &[]
    } else {
        files
    };
    let html = pep503_package_html(pkg, render_files, &status);
    let json = pep691_package_json(pkg, render_files, &status);

    let base = format!("{SIMPLE_PREFIX}{pkg}/");
    put_if_changed(
        state,
        storage,
        &format!("{base}index.html"),
        html.into_bytes(),
        SIMPLE_HTML_CONTENT_TYPE,
    )
    .await?;
    put_if_changed(
        state,
        storage,
        &format!("{base}index.json"),
        json.into_bytes(),
        SIMPLE_JSON_CONTENT_TYPE,
    )
    .await?;
    Ok(())
}

async fn write_global_indexes(
    state: &AppState,
    storage: &dyn Storage,
    packages: &[String],
) -> Result<()> {
    let html = pep503_global_html(packages);
    let json = pep691_global_json(packages);

    put_if_changed(
        state,
        storage,
        &format!("{SIMPLE_PREFIX}index.html"),
        html.into_bytes(),
        SIMPLE_HTML_CONTENT_TYPE,
    )
    .await?;
    put_if_changed(
        state,
        storage,
        &format!("{SIMPLE_PREFIX}index.json"),
        json.into_bytes(),
        SIMPLE_JSON_CONTENT_TYPE,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buckets::{BucketHandle, BucketSet, Pinned};
    use crate::storage::test_support::InMemStorage;
    use crate::{AccessLogFormat, ArtifactDelivery};
    use axum::body::Body;
    use http::Response;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool as StdAtomicBool;
    use std::sync::Mutex;
    use std::time::Duration;

    fn raw(items: &[(&str, u64)]) -> Vec<(String, u64)> {
        items.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    #[test]
    fn marker_nonces_carry_randomized_process_entropy() {
        let first = marker_nonce();
        let second = marker_nonce();
        assert_ne!(first, second);
        for nonce in [first, second] {
            let entropy = nonce.rsplit('-').next().unwrap();
            assert_eq!(entropy.len(), 32);
            assert!(entropy.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn missing_intent_timestamp_defers_the_whole_package() {
        let entries = vec![
            FileEntry {
                key: format!("{DIRTY_PREFIX}pkg!unknown.intent"),
                size: 0,
                last_modified: None,
            },
            FileEntry {
                key: format!("{DIRTY_PREFIX}pkg!other.commit"),
                size: 0,
                last_modified: Some("2026-01-01T00:00:00Z".into()),
            },
        ];
        assert!(consumable_dirty_work(
            &entries,
            time::OffsetDateTime::now_utc(),
            time::Duration::ZERO,
        )
        .is_empty());
    }

    fn seed_private_artifact(storage: &InMemStorage, pkg: &str, filename: &str) {
        let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        storage.insert(&key, b"artifact".to_vec());
        storage.insert(
            &sidecar_key(&key),
            serde_json::to_vec(&Sidecar {
                sha256: "ab".repeat(32),
                size: 8,
                version: "1.0".to_string(),
                upload_time: "2026-01-01T00:00:00Z".to_string(),
                requires_python: None,
                yanked: Yanked::Flag(false),
                origin: Some(crate::origin::PRIVATE.to_string()),
                upload_epoch_ms: None,
                yank_epoch: 0,
            })
            .unwrap(),
        );
    }

    fn state_switched_after_pin(
        first: Arc<InMemStorage>,
        second: Arc<InMemStorage>,
    ) -> (Arc<AppState>, Arc<Pinned>) {
        let mut state = AppState::headless(first.clone());
        state.buckets = Arc::new(BucketSet::new(vec![
            BucketHandle {
                storage: first,
                name: "first".to_string(),
            },
            BucketHandle {
                storage: second,
                name: "second".to_string(),
            },
        ]));
        let leased = state.pin();
        state.buckets.switch(1);
        (Arc::new(state), leased)
    }

    #[test]
    fn pkgstat_counts_files_distinct_versions_and_bytes() {
        // Two versions of one project: 3 files, 2 releases, summed bytes.
        let stat = PkgStat::from_raw(&raw(&[
            ("six-1.16.0-py2.py3-none-any.whl", 100),
            ("six-1.16.0.tar.gz", 50),
            ("six-1.15.0-py2.py3-none-any.whl", 80),
        ]));
        assert_eq!(stat.files, 3);
        assert_eq!(stat.releases, 2);
        assert_eq!(stat.bytes, 230);
        assert_eq!(PkgStat::from_raw(&[]), PkgStat::default());
    }

    #[test]
    fn inventory_map_upsert_is_idempotent_and_totals_sum() {
        let mut inv = InventoryMap::default();
        let a = PkgStat::from_raw(&raw(&[("a-1.0-py3-none-any.whl", 10)]));
        inv.upsert("a", a);
        assert!(inv.dirty);
        inv.dirty = false;
        // Re-applying the same absolute stat is a no-op (the audit re-running, or
        // a duplicate marker, must not double-count).
        inv.upsert("a", a);
        assert!(!inv.dirty, "identical upsert must not mark dirty");

        inv.upsert(
            "b",
            PkgStat::from_raw(&raw(&[
                ("b-1.0-py3-none-any.whl", 20),
                ("b-2.0-py3-none-any.whl", 5),
            ])),
        );
        let t = inv.totals();
        assert_eq!((t.projects, t.releases, t.files, t.bytes), (2, 3, 3, 35));

        // Going to zero artifacts removes the project (a delete that empties it).
        inv.upsert("a", PkgStat::default());
        let t = inv.totals();
        assert_eq!((t.projects, t.releases, t.files, t.bytes), (1, 2, 2, 25));
    }

    #[tokio::test]
    async fn single_bucket_audit_reclaims_an_abandoned_empty_mirror_claim() {
        let storage = Arc::new(InMemStorage::default());
        storage.insert(
            &crate::origin::origin_key("abandoned"),
            br#"{"origin":"mirror","nonce":"00000000000000000000000000000000"}"#.to_vec(),
        );
        let mut state = AppState::headless(storage.clone());
        state.intent_grace = time::Duration::ZERO;

        reclaim_empty_mirror_claim(&state, storage.as_ref(), "abandoned", 0)
            .await
            .unwrap();
        reclaim_empty_mirror_claim(&state, storage.as_ref(), "abandoned", 0)
            .await
            .unwrap();

        assert_eq!(
            crate::origin::read_origin_observation(storage.as_ref(), "abandoned")
                .await
                .unwrap()
                .unwrap()
                .state,
            crate::origin::OriginState::Unclaimed
        );
    }

    #[tokio::test]
    async fn leader_tick_stays_on_the_lease_matched_pin_after_a_switch() {
        let first = Arc::new(InMemStorage::default());
        let second = Arc::new(InMemStorage::default());
        let (state, leased) = state_switched_after_pin(first.clone(), second.clone());
        let pkg = "leasepin";
        let filename = "leasepin-1.0-py3-none-any.whl";
        let artifact = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        first.insert(&artifact, b"leased bucket bytes".to_vec());
        first.insert(
            &sidecar_key(&artifact),
            serde_json::to_vec(&Sidecar {
                sha256: "ab".repeat(32),
                size: 19,
                version: "1.0".to_string(),
                upload_time: "2026-01-01T00:00:00Z".to_string(),
                requires_python: None,
                yanked: Yanked::Flag(false),
                origin: Some(crate::origin::PRIVATE.to_string()),
                upload_epoch_ms: None,
                yank_epoch: 0,
            })
            .unwrap(),
        );
        mark_dirty(first.as_ref(), pkg).await.unwrap();

        tick(&state, &leased).await.unwrap();

        let index = format!("{SIMPLE_PREFIX}{pkg}/index.json");
        assert!(first.head_exists(&index).await.unwrap());
        assert!(
            !second.head_exists(&index).await.unwrap(),
            "a lease on the first bucket must not authorize a tick on the newly selected bucket"
        );
    }

    #[tokio::test]
    async fn warm_drain_defers_a_whole_package_with_a_fresh_intent() {
        let storage = Arc::new(InMemStorage::default());
        let pkg = "writing";
        seed_private_artifact(storage.as_ref(), pkg, "writing-1.0.whl");
        let intent = format!("{DIRTY_PREFIX}{pkg}!fresh.intent");
        let commit = format!("{DIRTY_PREFIX}{pkg}!unrelated.commit");
        storage.insert(&intent, Vec::new());
        storage.insert(&commit, Vec::new());
        let mut state = AppState::headless(storage.clone());
        // InMemStorage's fixed test timestamp remains fresh under this window.
        state.intent_grace = time::Duration::weeks(10_000);

        drain_dirty_uncached(&state, storage.as_ref())
            .await
            .unwrap();

        assert!(storage.head_exists(&intent).await.unwrap());
        assert!(storage.head_exists(&commit).await.unwrap());
        assert!(!storage
            .head_exists(&format!("{SIMPLE_PREFIX}{pkg}/index.json"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn selected_tick_defers_a_whole_package_with_a_fresh_intent() {
        let storage = Arc::new(InMemStorage::default());
        let pkg = "writing";
        seed_private_artifact(storage.as_ref(), pkg, "writing-1.0.whl");
        let intent = format!("{DIRTY_PREFIX}{pkg}!fresh.intent");
        let commit = format!("{DIRTY_PREFIX}{pkg}!unrelated.commit");
        storage.insert(&intent, Vec::new());
        storage.insert(&commit, Vec::new());
        let mut state = AppState::headless(storage.clone());
        state.intent_grace = time::Duration::weeks(10_000);
        let state = Arc::new(state);
        let pinned = state.pin();

        tick(&state, &pinned).await.unwrap();

        assert!(storage.head_exists(&intent).await.unwrap());
        assert!(storage.head_exists(&commit).await.unwrap());
        assert!(!storage
            .head_exists(&format!("{SIMPLE_PREFIX}{pkg}/index.json"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn pending_recovery_uses_storage_time_not_a_skewed_nonce_clock() {
        let storage = Arc::new(InMemStorage::default());
        storage.insert("_dirty/pkg!0-1-0.intent", Vec::new());
        let mut state = AppState::headless(storage.clone());
        // The nonce claims 1970, while InMemStorage's authoritative object
        // timestamp is 2026. A huge grace keeps that storage timestamp fresh.
        state.intent_grace = time::Duration::weeks(10_000);

        assert!(stale_unpaired_intents(&state, storage.as_ref(), "pkg")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn audit_fast_path_keeps_retained_frozen_evidence_logically_dead() {
        let storage = Arc::new(InMemStorage::default());
        let pkg = "frozenpkg";
        let filename = "frozenpkg-1.0.whl";
        seed_private_artifact(storage.as_ref(), pkg, filename);
        let artifact = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        storage.insert(&format!("{artifact}{FROZEN_SUFFIX}"), b"{}".to_vec());
        storage.insert(&format!("{artifact}{TOMBSTONE_SUFFIX}"), b"{}".to_vec());
        let fp = package_fingerprint(storage.as_ref(), pkg).await.unwrap();
        storage.insert(
            &format!("{STATE_PREFIX}fp-f.json"),
            serde_json::to_vec(&std::collections::HashMap::from([(pkg.to_string(), fp)])).unwrap(),
        );
        let state = AppState::headless(storage.clone());

        let audit = audit_shard(&state, storage.as_ref(), 0, 'f', false)
            .await
            .unwrap();

        assert_eq!(audit.live, Vec::<String>::new());
        assert_eq!(audit.dead, vec![pkg.to_string()]);
        assert_eq!(audit.skipped, 1);
    }

    #[tokio::test]
    async fn leader_audit_rejects_a_stale_lease_pin_instead_of_repinning() {
        let first = Arc::new(InMemStorage::default());
        let second = Arc::new(InMemStorage::default());
        let (state, leased) = state_switched_after_pin(first, second.clone());

        let error = audit(&state, &leased, true).await.unwrap_err();

        assert!(error
            .to_string()
            .contains("bucket selection generation changed"));
        assert!(
            second.list_all("").await.unwrap().is_empty(),
            "an audit authorized on the old bucket must not re-pin and mutate the new bucket"
        );
    }

    /// Storage stub whose `list_all("packages/...")` never returns — an audit
    /// sweep that takes forever. Everything else is a tiny in-memory object map.
    struct SweepStallsStorage {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        sweep_entered: StdAtomicBool,
    }

    #[async_trait::async_trait]
    impl Storage for SweepStallsStorage {
        async fn head_exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        async fn serve_artifact(&self, _key: &str, _range: Option<&str>) -> Result<Response<Body>> {
            bail!("not used in this test")
        }
        async fn presign_get(
            &self,
            _key: &str,
            _expires: std::time::Duration,
        ) -> Result<Option<String>> {
            Ok(None)
        }
        async fn put_bytes(
            &self,
            key: &str,
            bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<()> {
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
            Ok(())
        }
        async fn put_if_absent(
            &self,
            key: &str,
            bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bool> {
            let mut map = self.objects.lock().unwrap();
            if map.contains_key(key) {
                return Ok(false);
            }
            map.insert(key.to_string(), bytes);
            Ok(true)
        }
        async fn put_file_if_absent(
            &self,
            key: &str,
            path: &std::path::Path,
            content_type: Option<&str>,
        ) -> Result<bool> {
            let bytes = std::fs::read(path)?;
            self.put_if_absent(key, bytes, content_type).await
        }
        async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| crate::storage::NotFound(key.to_string()).into())
        }
        async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
            let map = self.objects.lock().unwrap();
            let mut out: Vec<FileEntry> = map
                .iter()
                .filter(|(k, _)| k.starts_with(dir_prefix) && !k[dir_prefix.len()..].contains('/'))
                .map(|(k, v)| FileEntry {
                    key: k.clone(),
                    size: v.len() as u64,
                    last_modified: Some("2026-01-01T00:00:00Z".to_string()),
                })
                .collect();
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        }
        async fn list_all(&self, prefix: &str) -> Result<Vec<crate::storage::ObjectMeta>> {
            if prefix.starts_with(PACKAGES_PREFIX) {
                // Same stall for the flat-enumeration path the audit uses.
                self.sweep_entered.store(true, Ordering::SeqCst);
                futures::future::pending::<()>().await;
            }
            Ok(Vec::new())
        }
        async fn delete_keys(&self, keys: &[String]) -> Result<()> {
            let mut map = self.objects.lock().unwrap();
            for k in keys {
                map.remove(k);
            }
            Ok(())
        }
    }

    /// Regression for write-visibility latency: a dirty marker dropped while
    /// the worker is parked in its sleep must be processed via the nudge in
    /// far less than the tick interval (10s here; the nudge makes it ~ms).
    #[tokio::test]
    async fn nudge_wakes_worker_before_tick() {
        let pkg = "fastpkg";
        let wheel = "fastpkg-1.0-py3-none-any.whl";
        let mut objects = HashMap::new();
        objects.insert(
            format!("{PACKAGES_PREFIX}{pkg}/{wheel}"),
            b"not-a-real-wheel".to_vec(),
        );
        objects.insert(
            format!("{PACKAGES_PREFIX}{pkg}/{wheel}{SIDECAR_SUFFIX}"),
            serde_json::to_vec(&Sidecar {
                sha256: "ab".repeat(32),
                size: 16,
                version: "1.0".into(),
                upload_time: "2026-01-01T00:00:00Z".into(),
                requires_python: None,
                yanked: Yanked::Flag(false),
                origin: None,
                upload_epoch_ms: None,
                yank_epoch: 0,
            })
            .unwrap(),
        );
        objects.insert(
            format!("{SIMPLE_PREFIX}index.json"),
            br#"{"projects":[{"name":"fastpkg"}]}"#.to_vec(),
        );
        let storage = Arc::new(SweepStallsStorage {
            objects: Mutex::new(objects),
            sweep_entered: StdAtomicBool::new(false),
        });
        let state = Arc::new(AppState {
            buckets: Arc::new(crate::buckets::BucketSet::single(storage.clone())),
            bucket_health: None,
            writes_fenced: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            uploader_user: None,
            uploader_pass: None,
            admin_user: None,
            admin_pass: None,
            read_user: None,
            read_pass: None,
            token_signing_key: None,
            private_prefix: None,
            artifact_delivery: ArtifactDelivery::Auto,
            metrics_project_labels: false,
            access_log: false,
            access_log_format: AccessLogFormat::Structured,
            worker_interval: Duration::from_secs(10),
            reconcile_interval: Duration::from_secs(3600),
            repl_sweep_interval: Duration::from_secs(300),
            repl_sweep_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_request_unix: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            fanout_grace: Duration::from_secs(30),
            intent_grace: time::Duration::seconds(900),
            audit_on_boot: true,
            wait_on_upload: false,
            wait_on_upload_timeout: Duration::from_secs(1),
            lease_ttl: Duration::from_secs(30),
            index_cache: Arc::new(crate::cache::IndexCache::new(crate::cache::INDEX_CACHE_TTL)),
            presign_cache: Arc::new(crate::cache::PresignCache::new(
                crate::cache::PRESIGN_CACHE_TTL,
            )),
            spool_dir: std::env::temp_dir(),
            global_names: Arc::new(tokio::sync::Mutex::new(None)),
            inventory: Arc::new(tokio::sync::Mutex::new(InventoryMap::default())),
            worker_nudge: Arc::new(tokio::sync::Notify::new()),
            empty_origin_observations: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            metrics: Arc::new(crate::metrics::Metrics::new()),
            counters: Arc::new(crate::counters::Counters::disabled()),
            download_board: Arc::new(std::sync::Mutex::new(None)),
            proxy: None,
            started: std::time::Instant::now(),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(run_worker_until(state.clone(), shutdown_rx));
        // Let the first (empty) tick pass; the worker parks in a 10s sleep.
        tokio::time::sleep(Duration::from_millis(300)).await;

        storage
            .objects
            .lock()
            .unwrap()
            .insert(format!("{DIRTY_PREFIX}{pkg}"), Vec::new());
        state.worker_nudge.notify_one();

        let index_key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut rebuilt = false;
        while Instant::now() < deadline {
            if storage.objects.lock().unwrap().contains_key(&index_key) {
                rebuilt = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        worker.abort();
        assert!(
            rebuilt,
            "nudged marker not processed within 2s — visibility is stuck on the 10s tick"
        );
    }

    /// Regression: a long reconcile sweep must not starve dirty-marker
    /// processing. Before the fix, the sweep ran inline ahead of tick() —
    /// uploads stayed invisible (and sync uploads timed out) for the whole
    /// sweep. Here the sweep literally never finishes, and the marker must
    /// still be processed.
    #[tokio::test]
    async fn dirty_markers_processed_while_sweep_runs() {
        let pkg = "fastpkg";
        let wheel = "fastpkg-1.0-py3-none-any.whl";
        let mut objects = HashMap::new();
        objects.insert(format!("{DIRTY_PREFIX}{pkg}"), Vec::new());
        objects.insert(
            format!("{PACKAGES_PREFIX}{pkg}/{wheel}"),
            b"not-a-real-wheel".to_vec(),
        );
        objects.insert(
            format!("{PACKAGES_PREFIX}{pkg}/{wheel}{SIDECAR_SUFFIX}"),
            serde_json::to_vec(&Sidecar {
                sha256: "ab".repeat(32),
                size: 16,
                version: "1.0".into(),
                upload_time: "2026-01-01T00:00:00Z".into(),
                requires_python: None,
                yanked: Yanked::Flag(false),
                origin: None,
                upload_epoch_ms: None,
                yank_epoch: 0,
            })
            .unwrap(),
        );
        // Global index already lists the package, so the tick path skips the
        // global rebuild (which would also hit the stalled list_all).
        objects.insert(
            format!("{SIMPLE_PREFIX}index.json"),
            br#"{"projects":[{"name":"fastpkg"}]}"#.to_vec(),
        );

        let storage = Arc::new(SweepStallsStorage {
            objects: Mutex::new(objects),
            sweep_entered: StdAtomicBool::new(false),
        });
        let state = Arc::new(AppState {
            buckets: Arc::new(crate::buckets::BucketSet::single(storage.clone())),
            bucket_health: None,
            writes_fenced: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            uploader_user: None,
            uploader_pass: None,
            admin_user: None,
            admin_pass: None,
            read_user: None,
            read_pass: None,
            token_signing_key: None,
            private_prefix: None,
            artifact_delivery: ArtifactDelivery::Auto,
            metrics_project_labels: false,
            access_log: false,
            access_log_format: AccessLogFormat::Structured,
            worker_interval: Duration::from_millis(10),
            reconcile_interval: Duration::from_secs(3600),
            repl_sweep_interval: Duration::from_secs(300),
            repl_sweep_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_request_unix: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            fanout_grace: Duration::from_secs(30),
            intent_grace: time::Duration::seconds(900),
            audit_on_boot: true,
            wait_on_upload: false,
            wait_on_upload_timeout: Duration::from_secs(1),
            lease_ttl: Duration::from_secs(30),
            index_cache: Arc::new(crate::cache::IndexCache::new(crate::cache::INDEX_CACHE_TTL)),
            presign_cache: Arc::new(crate::cache::PresignCache::new(
                crate::cache::PRESIGN_CACHE_TTL,
            )),
            spool_dir: std::env::temp_dir(),
            global_names: Arc::new(tokio::sync::Mutex::new(None)),
            inventory: Arc::new(tokio::sync::Mutex::new(InventoryMap::default())),
            worker_nudge: Arc::new(tokio::sync::Notify::new()),
            empty_origin_observations: Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            metrics: Arc::new(crate::metrics::Metrics::new()),
            counters: Arc::new(crate::counters::Counters::disabled()),
            download_board: Arc::new(std::sync::Mutex::new(None)),
            proxy: None,
            started: std::time::Instant::now(),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(run_worker_until(state, shutdown_rx));

        let index_key = format!("{SIMPLE_PREFIX}{pkg}/index.json");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut rebuilt = false;
        while Instant::now() < deadline {
            if storage.objects.lock().unwrap().contains_key(&index_key) {
                rebuilt = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        worker.abort();

        assert!(
            storage.sweep_entered.load(Ordering::SeqCst),
            "test setup broken: sweep never started"
        );
        assert!(
            rebuilt,
            "dirty marker was not processed while the sweep was running — sweep starves the event path"
        );
    }
}
