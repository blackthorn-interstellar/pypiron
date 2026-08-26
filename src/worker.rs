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
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Instant,
};

use anyhow::{anyhow, bail, Context as _, Result};
use sha2::{Digest, Sha256};
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};

use crate::app::{AppState, DIRTY_PREFIX, PACKAGES_PREFIX, SIMPLE_PREFIX};
use crate::hash::sha256_hex;
use crate::lease::LeaseManager;
use crate::markers::{
    clear_intent, mark_commit, mark_dirty, mark_intent, parse_marker, Marker, COMMIT_SUFFIX,
    INTENT_SUFFIX,
};
use crate::names::infer_version_from_filename;
use crate::render::{
    pep503_global_html, pep503_project_html, pep691_global_json, pep691_project_json, FileMetadata,
    SIMPLE_HTML_CONTENT_TYPE, SIMPLE_JSON_CONTENT_TYPE,
};
use crate::sidecar::{
    is_artifact, sidecar_key, Sidecar, Yanked, FROZEN_SUFFIX, METADATA_SUFFIX, PROVENANCE_SUFFIX,
    SIDECAR_SUFFIX, SUPERSEDING_SUFFIX, TOMBSTONE_SUFFIX,
};
use crate::storage::{is_not_found, FileEntry, ObjectMeta, Storage};
use crate::transparency::FileShas;

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

/// The longest wall clock one worker cycle can span, and therefore the oldest a
/// "the region bucket caught up" verdict can be by the time the read-return gate
/// consumes it. A cycle serially awaits one health probe per bucket
/// ([`probe_buckets`]), the caught-up LIST against each peer
/// ([`region_bucket_caught_up`]), and topology verification for a recovered
/// bucket ([`maintain_bucket_selection`]) — each bounded by
/// [`BUCKET_HEALTH_IO_TIMEOUT`].
fn worst_case_cycle_span(bucket_count: usize) -> std::time::Duration {
    BUCKET_HEALTH_IO_TIMEOUT * (2 * bucket_count as u32 + 1)
}

/// The cycle span a read-return window fails to clear, or `None` when it clears
/// it. A window at or below one worst-case cycle lets a region bucket fail,
/// recover, and mature the whole window inside a single cycle, so reads can be
/// admitted back to it on a caught-up verdict up to a cycle stale. The cost is
/// bounded: a read that beats a just-missed file to the region bucket is served
/// from the write home by read-through — latency, never a 404 — so startup warns
/// (`src/app.rs`) and boots. Single-bucket nodes have no read affinity and no
/// gate, so they never qualify.
pub(crate) fn read_return_window_under_floor(
    bucket_count: usize,
    return_healthy: std::time::Duration,
) -> Option<std::time::Duration> {
    let floor = worst_case_cycle_span(bucket_count);
    (bucket_count > 1 && return_healthy <= floor).then_some(floor)
}

/// The per-project PEP 792 status sidecar as it appears in a `packages/<pkg>/`
/// listing (prefix stripped). Presence in the listing is the cheap gate before
/// the sweep pays a status read to derive the quarantined-project set (rung 5).
const PROJECT_STATUS_FILE: &str = ".project-status.json";

pub struct DirtyWork {
    pub package: String,
    pub keys: Vec<String>,
    pub stale_intents: u64,
}

/// Group dirty events by package and select exactly the markers safe to
/// consume now. Both the selected-bucket tick and warm-bucket drain use this
/// transaction-log rule: any fresh unpaired intent defers the whole package;
/// otherwise commits and paired intents are ready immediately, while stale
/// unpaired intents heal a crashed writer.
pub fn consumable_dirty_work(
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
    // Deterministic work order: the per-package grouping above iterates a
    // HashMap, whose order varies per process. Sorting costs nothing at these
    // sizes and makes tick behavior a pure function of the listing — which
    // the deterministic simulator replays by seed.
    work.sort_by(|a, b| a.package.cmp(&b.package));
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
        // Traffic-gated cadence: probe at full
        // speed while there is recent request traffic OR any bucket is unhealthy
        // or recovering — re-probing an unhealthy bucket is the only way it heals
        // back, so that is never gated off. Otherwise decay to the idle cadence,
        // accepting that the first request after idle may pay one bounded
        // discovery timeout before failover.
        let full_cadence =
            state.recent_request_traffic() || state.any_bucket_unhealthy_or_recovering();
        let probe_due = full_cadence
            || last_probe.is_none_or(|t| t.elapsed() >= crate::app::IDLE_PROBE_INTERVAL);
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
async fn maintain_bucket_selection(state: &AppState) -> bool {
    let Some(health) = &state.bucket_health else {
        return false;
    };
    // Read affinity only: when the region bucket is healthy but not yet the read
    // pin, a return is pending — confirm it holds no undrained repair notes
    // before the tick may move reads back to it. Never runs on the request path,
    // and only while a return is actually pending.
    if health.has_read_preference() {
        match health.read_return_pending() {
            Some(region) => {
                let caught_up = region_bucket_caught_up(state, region).await;
                health.set_region_caught_up(caught_up);
            }
            None => health.set_region_caught_up(false),
        }
    }
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
                // Topology revalidated — but a bucket that healed at a newer
                // storage format must not be selected and written blind (latent
                // at CURRENT_FORMAT=1, catastrophic after a bump). Re-gate the
                // format on the single healed handle: verify_format returns Ok
                // only when it is reachable and at a supported format; a mismatch
                // or a still-unreachable read blocks selection this tick, and the
                // next tick retries.
                let format_ok = match state.buckets.handles().get(*index) {
                    Some(handle) => matches!(
                        timeout(
                            BUCKET_HEALTH_IO_TIMEOUT,
                            crate::format::verify_format(
                                std::slice::from_ref(handle),
                                topology_availability_error,
                            ),
                        )
                        .await,
                        Ok(Ok(_))
                    ),
                    None => false,
                };
                if !format_ok {
                    selection_blocked.insert(*index);
                    warn!(bucket=*index, "bucket storage-format not verified on recovery; selection blocked this tick");
                } else if let Err(error) = health.topology_revalidated(*index) {
                    error!(bucket=*index, error=%error, "could not acknowledge topology validation");
                } else {
                    // A bucket just crossed unhealthy→healthy. Fire the `_repl/`
                    // sweep at once so drain starts seconds after heal instead of
                    // waiting out the periodic backstop. The worker loop owns the sweep;
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

    // Apply the read-pin switch through the same topology gating. A read switch
    // only bumps the shared generation (clearing the generation-tagged index and
    // presign caches); the write-scoped views cleared above are untouched.
    if let Some(read_change) = snapshot.read_selection_change {
        if !selection_blocked.contains(&read_change.to) {
            let next = state.buckets.switch_read(read_change.to);
            if let Err(error) = health.read_selection_applied(read_change.to) {
                error!(bucket=read_change.to, error=%error, "could not acknowledge read bucket selection");
            } else {
                info!(
                    from = %state.buckets.handles()[read_change.from].name,
                    to = %state.buckets.handles()[read_change.to].name,
                    generation = next.generation,
                    "read bucket changed"
                );
            }
        }
    }

    let applied = state.pin();
    snapshot.selected_index = applied.index;
    snapshot.read_selected_index = state.buckets.read_pin().index;
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

/// Whether the region bucket `region` has no repair still owed to it: every
/// other bucket's `_repl/<region>/` tree is empty. Conservative — an unreachable
/// peer or any error defers the return, keeping reads on the write bucket
/// (slower, never wrong). Bounded single-key LISTs; only called when a read
/// return is pending, never on the request path.
async fn region_bucket_caught_up(state: &AppState, region: usize) -> bool {
    for (index, handle) in state.buckets.handles().iter().enumerate() {
        if index == region {
            continue;
        }
        match timeout(
            BUCKET_HEALTH_IO_TIMEOUT,
            crate::replicate::has_undrained_repl_notes_for(handle.storage.as_ref(), region),
        )
        .await
        {
            Ok(Ok(false)) => {}
            Ok(Ok(true)) => return false,
            Ok(Err(error)) => {
                warn!(bucket=%handle.name, target=region, error=?error, "could not confirm region bucket caught up; deferring read return");
                return false;
            }
            Err(_) => {
                warn!(bucket=%handle.name, target=region, "region caught-up check timed out; deferring read return");
                return false;
            }
        }
    }
    true
}

/// Whether `pkg` has an unpaired intent still inside the storage-clock grace
/// window. A missing/malformed storage timestamp is conservatively live. Not
/// cheap: the listing underneath reads the whole `_dirty/` prefix and filters
/// to `pkg` in memory, so this is O(all markers) — call it once a package, past
/// the gates that can answer without it.
pub(crate) async fn has_live_intent(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
) -> Result<bool> {
    Ok(stale_unpaired_intents(state, storage, pkg).await?.is_none())
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
    let now = crate::clock::now_utc();
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
    // Which selection generation the selected-bucket denylist reconcile last ran
    // for. Runs once per leadership acquisition (a restart with a changed
    // `--exclude-package` set delists/relists the affected names on boot); a
    // selection change re-runs it against the newly selected bucket.
    let mut excludes_reconciled_generation: Option<u64> = None;

    // Markers are the primary freshness mechanism; the audit is the safety
    // net for what events cannot see (restores, out-of-band storage changes,
    // a peer that died without committing). The first leader audit runs
    // immediately (unless --audit-on-boot=false), so a restored backup heals
    // without waiting an interval. The audit runs on its own task: a deep
    // pass over a large corpus takes minutes of storage round-trips, and
    // running it inline starved dirty-marker processing for its whole
    // duration. Concurrent audit + tick rebuilds of the same package are
    // safe — rebuilds are idempotent, and where they straddle a mutation and
    // the staler one lands last, the next audit repairs the view it left
    // (dev/DESIGN.md, "Split-brain is harmless"; the VOPR classifies that
    // outcome as class 3). Note that no lease separates these two: this
    // concurrency exists on a single, undisputed leader.
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
    let counter_compact_running = Arc::new(AtomicBool::new(false));
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
    // a predecessor left.
    let mut last_repl_sweep: Option<Instant> = None;
    // Advisory refresh is always-on (never gated on multi-bucket): the malware
    // block set and audit index are global truth-cache. `None` fires one refresh
    // on the first tick; the memo carries the leader's conditional-GET etag and
    // failing/unfed state (warn on transition, not per attempt). It is spawned off
    // the loop's critical path (a full 30 MB refetch must not head-of-line-block
    // index/marker work), so the memo is shared and an in-flight flag prevents
    // overlap.
    let advisory_enabled = state.advisory_feed.is_some() || state.malware_block;
    let mut last_advisory_refresh: Option<Instant> = None;
    let advisory_memo = Arc::new(tokio::sync::Mutex::new(
        crate::advisories::RefreshMemo::default(),
    ));
    let advisory_running = Arc::new(AtomicBool::new(false));
    // Malware probe (every node): block a newly-published MAL-* advisory within
    // minutes, ahead of the daily feed. Inert unless blocking is armed, the
    // interval is nonzero, and the feed is the OSV `all.zip` URL (its CSV and
    // per-advisory siblings are what the probe polls). Spawned like the advisory
    // tick so a slow fetch can't stall the loop; the memo is per-node (no shared
    // state), an in-flight flag serializes runs.
    let probe_enabled = state.malware_block
        && !state.malware_probe.is_zero()
        && state
            .advisory_feed
            .as_deref()
            .and_then(crate::advisories::probe_base)
            .is_some();
    let mut last_probe: Option<Instant> = None;
    let probe_memo = Arc::new(tokio::sync::Mutex::new(
        crate::advisories::ProbeMemo::default(),
    ));
    let probe_running = Arc::new(AtomicBool::new(false));
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
            // Drop the per-package /stats cache now that this node's own writes are
            // in the store, so a same-node poll reads its own just-flushed counts
            // (the TTL bounds other nodes). Off the hot path, once per flush.
            state.invalidate_package_stats();
        }

        // Advisory refresh (EVERY node): the leader refetches the source and
        // persists changed bytes; every node reloads from storage when FEED_KEY's
        // etag moves. On the reconcile cadence, or immediately when a PUT set
        // `advisory_reload_asap`. Spawned like the audit sweep so a slow OSV
        // refetch never stalls the tick; the in-flight flag serializes runs and
        // the asap flag is consumed only once a run actually starts. Never
        // disarmed by a bucket switch — the snapshot is global truth-cache.
        if advisory_enabled {
            let forced = state.advisory_reload_asap.load(Ordering::SeqCst);
            let due = forced
                || last_advisory_refresh.is_none_or(|t| t.elapsed() >= state.reconcile_interval);
            if due && !advisory_running.swap(true, Ordering::SeqCst) {
                last_advisory_refresh = Some(Instant::now());
                if forced {
                    state.advisory_reload_asap.store(false, Ordering::SeqCst);
                }
                let job_state = state.clone();
                let pinned = selected.clone();
                let memo = advisory_memo.clone();
                let guard = SweepGuard(advisory_running.clone());
                let leader = is_leader;
                tokio::spawn(async move {
                    let _guard = guard;
                    let mut memo = memo.lock().await;
                    // Peers the leader-authored control singletons replicate onto.
                    let replicas = job_state.singleton_replicas(pinned.index);
                    crate::advisories::refresh(
                        crate::advisories::RefreshCtx {
                            storage: pinned.storage.as_ref(),
                            slot: &job_state.advisories,
                            metrics: &job_state.metrics,
                            replicas: &replicas,
                        },
                        job_state.advisory_feed.as_deref(),
                        leader,
                        &mut memo,
                    )
                    .await;
                });
            }
        }

        // Malware probe tick (EVERY node): poll OSV's per-advisory feed to block a
        // fresh MAL-* advisory ahead of the daily snapshot. Spawned off the loop
        // like the advisory refresh; the in-flight flag serializes runs.
        if probe_enabled {
            let due = last_probe.is_none_or(|t| t.elapsed() >= state.malware_probe);
            if due && !probe_running.swap(true, Ordering::SeqCst) {
                last_probe = Some(Instant::now());
                let job_state = state.clone();
                let pinned = selected.clone();
                let memo = probe_memo.clone();
                let guard = SweepGuard(probe_running.clone());
                if let Some(feed) = job_state.advisory_feed.clone() {
                    tokio::spawn(async move {
                        let _guard = guard;
                        let mut memo = memo.lock().await;
                        crate::advisories::probe(
                            crate::advisories::RefreshCtx {
                                storage: pinned.storage.as_ref(),
                                slot: &job_state.advisories,
                                metrics: &job_state.metrics,
                                // The probe only mutates the in-memory overlay; it
                                // writes no control singleton, so it needs no peers.
                                replicas: &[],
                            },
                            &feed,
                            &mut memo,
                        )
                        .await;
                    });
                }
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
                            // Reconcile this warm copy's own indexes with the live
                            // denylist before draining: a config-only change reaches
                            // a warm bucket no other way (no artifact moved, so
                            // nothing replicates or re-fingerprints it). Marks the
                            // affected names dirty on this bucket; the drain below
                            // rebuilds them the same pass. Its own bucket-local
                            // stamp makes it a one-GET no-op when unchanged.
                            if let Err(e) =
                                reconcile_excludes(&job_state, handle.storage.as_ref()).await
                            {
                                error!(bucket=%handle.name, error=?e, "replicate: destination denylist reconcile failed");
                            }
                            let result = tokio::select! {
                                result = drain_dirty_uncached(&job_state, handle.storage.as_ref()) => result,
                                _ = crate::replicate::wait_until_bucket_ineligible(&job_state, idx) => {
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
            // Once per leadership: reconcile the selected bucket's stored indexes
            // with the live denylist before the tick runs, so an exclude change
            // made across this restart is delisted (or relisted) on boot. Marks
            // only affected names dirty; the tick below rebuilds them the same
            // pass. Cheap no-op (one GET) when the config is unchanged.
            if excludes_reconciled_generation != Some(selected.generation) {
                match reconcile_excludes(&state, selected.storage.as_ref()).await {
                    Ok(()) => excludes_reconciled_generation = Some(selected.generation),
                    Err(e) => {
                        error!(error=?e, "worker: denylist reconcile failed; will retry next tick")
                    }
                }
            }
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
            // Counter compaction (LEADER only): freeze finished days into one file
            // per shard, write summaries, prune past retention, then converge every
            // bucket on the union of the frozen rollups. Every healthy bucket is
            // compacted from its own segments: a node's tallies land on whichever
            // bucket it had pinned, so freezing only the write pin would sweep the
            // others' share of the day uncounted. A bucket that is unreachable now
            // is left entirely alone and frozen by a later pass.
            //
            // Spawned off the loop like the audit sweep, never inline. The pass
            // costs one LIST per configured bucket, and a bucket that has gone dark
            // holds that LIST for the object-store client's whole request budget —
            // an hour, deliberately, so a slow cloud is never mistaken for a dead
            // one (see storage::FAILOVER_REQUEST_TIMEOUT). Inline, that single round
            // trip froze the entire worker loop, and with it every job the loop
            // schedules — the `_repl/` marker sweep first, which is exactly the work
            // that routes around the dark bucket. Health drops an unreachable bucket
            // from the peer list within a cycle, so this only bites in the window
            // before it notices; spawning removes the window instead of guessing how
            // long a legitimate pass may take. The in-flight flag serializes passes,
            // and abandoning one at shutdown is safe: compaction recomputes from
            // immutable segments and is idempotent.
            let compact_due = last_counter_compact
                .is_none_or(|t| t.elapsed() >= state.counters.rollup_interval());
            if compact_due && !counter_compact_running.swap(true, Ordering::SeqCst) {
                last_counter_compact = Some(Instant::now());
                let job_state = state.clone();
                let primary_index = selected.index;
                let guard = SweepGuard(counter_compact_running.clone());
                tokio::spawn(async move {
                    let _guard = guard;
                    let peers = job_state.counter_rollup_peers(primary_index);
                    job_state.counters.compact(&peers).await;
                });
            }
            // The tick can run many seconds on S3 with a backlog. Race it against
            // the shutdown signal: a graceful SIGTERM must never be stuck behind a
            // slow batch, or the worker is aborted before it releases the lease —
            // and a skipped release is a lease-TTL write outage on the successor
            // (the very thing release() exists to avoid). Abandoning a rebuild
            // mid-flight is safe: rebuilds are idempotent and the next leader
            // redoes the work.
            tokio::select! {
                result = tick(&state, &selected) => {
                    if let Err(e) = result {
                        error!(error=?e, "worker tick failed");
                    }
                }
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
    // Graceful exit: hand leadership over FIRST, then flush. The hand-off is
    // the availability-critical half — without it a successor waits out the
    // lease TTL before it can write at all — and the caller only waits
    // WORKER_STOP_GRACE for this whole tail (src/app.rs). Counters are
    // best-effort by construction, so they must never be what spends that
    // budget: a restart used to be a TTL-long write outage, and a flush that
    // pays its key-verification round trips is long enough to bring that back.
    if let Some(lm) = &lease {
        lm.release().await;
    }
    for lease in warm_leases.into_iter().flatten() {
        lease.release().await;
    }
    // Flush any buffered counts before exit so a graceful restart loses at most
    // the events of the final partial interval — but bounded, since the flush
    // may verify keys against storage. Losing the last window's counts is the
    // declared, acceptable loss; a slow exit is not.
    let _ = tokio::time::timeout(FINAL_FLUSH_BUDGET, state.counters.flush()).await;
    if let Some(task) = health_task {
        task.abort();
        let _ = task.await;
    }
}

/// Fingerprint shards live here, one JSON map per [`SHARD_CHARS`] character:
/// package → hash of the (key, size, etag) listing its views were built
/// from. They are views of views — regenerable, never trusted over truth. A
/// lost shard merely means its packages rebuild once.
pub(crate) const STATE_PREFIX: &str = "_state/";

/// How often each node refreshes its in-memory inventory from the persisted
/// view. Followers never rebuild, so this is how the leader's value reaches
/// them; a few seconds of homepage lag is fine for a glanceable stat.
const INVENTORY_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// How long a shutting-down process waits for the worker's exit tail — the
/// lease hand-off and the final counter flush. Past it the process exits
/// anyway and the successor waits out the lease TTL instead, so everything in
/// that tail is ordered most-important-first and individually bounded.
pub(crate) const WORKER_STOP_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// The slice of [`WORKER_STOP_GRACE`] the last counter flush may use. It runs
/// after the leases are handed over and can pay storage round trips to verify
/// keys, so it gets a small budget and the loss if it overruns is one partial
/// flush window of download counts — already the declared loss on any crash.
const FINAL_FLUSH_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

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
    // Accumulated across every shard — the quarantined set is fleet-wide truth, so
    // it may only be published once the whole corpus has been swept (below).
    let mut quarantined_names: Vec<String> = Vec::new();
    let mut deltas: Vec<(String, FileShas)> = Vec::new();
    // Advisory-report inventory: `(package, filenames)` for advisory-matched names,
    // accumulated across shards and joined against the audit index in the tail.
    let mut inventory: Vec<(String, Vec<String>)> = Vec::new();
    // Capture the advisory snapshot once, so the shard walk's inventory filter and
    // the report tail's join both use the same db and stamp the same feed sha —
    // the materialized report is then consistent with what the walk gathered.
    let advisory_snapshot = state.advisory_snapshot();
    let advisory_db = advisory_snapshot.db.clone();

    // Tamper-evident checkpoints. On an empty chain the first pass commits the
    // whole corpus (genesis), so unchanged packages must surface their shas
    // once; steady state commits only churn. Deciding this up front tells the
    // shard pass whether to read unchanged packages' sidecars.
    //
    // `None` = the chain head could not be read this pass. We then skip the
    // checkpoint write entirely, rather than risk a *partial* genesis: treating
    // the error as "not genesis" while a later succeeding read inside
    // `write_chain_link` sees the empty chain would write seq 0 committing only
    // the churned packages, permanently omitting never-churned artifacts with no
    // re-genesis path. Skipping leaves the empty chain intact so the next pass
    // (whose read succeeds) performs a true whole-corpus genesis.
    let genesis_state: Option<bool> = if state.transparency {
        match crate::transparency::read_head(storage).await {
            Ok(head) => Some(head.is_none()),
            Err(e) => {
                warn!(error=?e, "transparency: chain head unreadable; checkpoint deferred this pass");
                None
            }
        }
    } else {
        None
    };
    let genesis = genesis_state == Some(true);

    // Shards enumerate in parallel — that is what the sharding is for. The
    // bound keeps peak memory at a few shards' worth of listings (a shard is
    // ~1/36th of the corpus).
    const SHARD_CONCURRENCY: usize = 6;
    for chunk in crate::storage::SHARD_CHARS.chunks(SHARD_CONCURRENCY) {
        let audits = chunk.iter().map(|shard| {
            audit_shard(
                state,
                storage,
                generation,
                *shard,
                force_deep,
                state.transparency,
                genesis,
                advisory_db.as_deref(),
            )
        });
        for (shard, result) in chunk.iter().zip(futures::future::join_all(audits).await) {
            match result {
                Ok(result) => {
                    live.extend(result.live);
                    dead.extend(result.dead);
                    rebuilt += result.rebuilt;
                    skipped += result.skipped;
                    failures += result.failures;
                    pkg_stats.extend(result.pkg_stats);
                    quarantined_names.extend(result.quarantined);
                    deltas.extend(result.deltas);
                    inventory.extend(result.inventory);
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
    // The fleet-wide quarantined set derived this sweep. Shared by the byte-gate
    // publish below and the report's `blocked` flag.
    let quarantined_set: HashSet<String> = quarantined_names.iter().cloned().collect();
    // A clean full cycle: publish the quarantined set for the byte gate,
    // independent of `--malware-block` — PEP 792 quarantine refusal is a separate
    // guarantee from OSV blocking (a compromised project PyPI froze stays refused
    // even with malware blocking off). `publish_quarantined` persists on change
    // only, so an empty set (no quarantined project) writes nothing; a non-empty
    // one is written write-through to every healthy bucket and swaps this leader's
    // own in-memory set immediately.
    let set: std::collections::BTreeSet<String> = quarantined_names.into_iter().collect();
    let replicas = state.singleton_replicas(pinned.index);
    if let Err(e) =
        crate::advisories::publish_quarantined(storage, &replicas, &state.advisories, set).await
    {
        warn!(error=?e, "audit: publishing quarantined set failed; serving last set");
    }
    // Materialize the org audit report: the walked inventory joined with the audit
    // index and 30-day counters. Leader-only by construction (the audit runs under
    // the leader gate). Built whenever a snapshot is loaded (feed set ⇒ audit
    // exists), independent of the blocking toggle. Best-effort — a failure keeps
    // the last report and never fails the sweep; the write is conditioned on
    // change, so an unchanged corpus/feed doesn't churn the key.
    if let Some(db) = advisory_db.as_deref() {
        let feed_sha = advisory_snapshot.zip_sha256.as_deref().unwrap_or_default();
        if let Err(e) =
            build_advisory_report(state, storage, db, inventory, &quarantined_set, feed_sha).await
        {
            warn!(error=?e, "audit: building advisory report failed; keeping last report");
        }
    }
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
    // Append the tamper-evident checkpoint last, once views and inventory are
    // settled. Never fails the audit itself: a checkpoint that cannot be landed
    // warns, counts `pypiron_chain_checkpoint_deferrals_total`, and rides to the
    // next pass.
    // Only write when we could determine genesis-vs-incremental this pass; a
    // `None` (unreadable head) is deferred above to avoid a partial genesis.
    if let Some(is_genesis) = genesis_state {
        let mut delta: crate::transparency::Delta = deltas.into_iter().collect();
        if is_genesis {
            // Genesis has no prior state to remove from, so an empty map is pure
            // noise; keep only packages that actually hold committable files.
            delta.retain(|_, files| !files.is_empty());
        }
        write_chain_link(state, pinned, generation, delta).await;
    } else {
        // The chain head was unreadable this pass, so the checkpoint is deferred
        // above — but the per-package fingerprint shards were already written
        // unconditionally, so a dropped delta is never re-derived: the chain would
        // keep committing the old sha over bytes storage has already replaced.
        // Carry it against this bucket so the next pass (whose head read succeeds)
        // merges it, exactly as the CAS-loss path does. A false `genesis_state`
        // (transparency off) collects no deltas, so this no-ops.
        let delta: crate::transparency::Delta = deltas.into_iter().collect();
        if !delta.is_empty() {
            carry_unlanded(&pinned.storage, delta);
        }
    }
    // Publish completion metrics only after the sweep's last storage write:
    // the bench harness's `wait_swept` treats `audit_last_duration_seconds` as
    // "the sweep is finished", so nothing may land after these publish.
    let duration_secs = started.elapsed().as_secs_f64();
    let m = &state.metrics;
    m.reconcile_sweeps
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    m.audit_packages_rebuilt
        .fetch_add(rebuilt as u64, std::sync::atomic::Ordering::Relaxed);
    m.audit_packages_skipped
        .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
    m.set_audit_duration(duration_secs);
    info!(
        packages = live.len(),
        rebuilt, skipped, duration_secs, "reconcile: sweep complete"
    );
    Ok(())
}

/// Read a package's current committed file→sha256 map straight from its
/// sidecars — the renderable set (tombstoned/frozen/mirror-quarantined excluded,
/// exactly as the index is built). Used only to seed the genesis checkpoint for
/// a package the incremental audit did not rebuild; steady-state passes never
/// call it, so the churn-sized audit cost is preserved.
async fn current_package_shas(storage: &dyn Storage, pkg: &str) -> Result<FileShas> {
    // `false` = do not backfill missing sidecars, deliberately asymmetric with
    // the rebuild path's `true`. A legacy artifact with no sidecar is simply
    // omitted from this genesis commitment; a later rebuild backfills its
    // sidecar and commits it as ordinary churn. An uncommitted file never
    // triggers a verify-chain violation, so the omission is safe — and this
    // keeps genesis read-only, doing no writes of its own.
    let (files, _raw) = list_artifacts_for_claim(storage, pkg, false).await?;
    Ok(files.into_iter().map(|f| (f.filename, f.sha256)).collect())
}

/// Deltas an append could not land, held against the bucket handle they were
/// observed on and merged into that bucket's next checkpoint.
///
/// Leader memory on purpose. A package's audit fingerprint is written with its
/// shard, long before the checkpoint is appended, so a delta dropped here is never
/// re-derived: the chain would go on committing the old sha while storage holds
/// the new bytes, and `verify-chain` would report a tamper that clears only if
/// that package happens to churn again. Holding it costs a few keys and makes the
/// next pass whole. A crash loses it — the accepted residual (see
/// `crate::transparency`).
///
/// Keyed by the handle itself, as a `Weak`: a carried delta rejoins the chain it
/// was observed against (surviving a pin switch away and back), a dropped bucket
/// takes its carry with it, and nothing is handed to whatever later allocation
/// reuses the address — the deterministic simulator runs many fleets, under the
/// same bucket names, in one process.
type Unlanded = Vec<(std::sync::Weak<dyn Storage>, crate::transparency::Delta)>;
static UNLANDED_DELTAS: OnceLock<Mutex<Unlanded>> = OnceLock::new();

fn unlanded_deltas() -> &'static Mutex<Unlanded> {
    UNLANDED_DELTAS.get_or_init(Mutex::default)
}

/// Is `held` the very handle `pin` points at? Address only: the vtable half of a
/// trait-object pointer is not reliably unique.
fn same_handle(held: &std::sync::Weak<dyn Storage>, pin: &Arc<dyn Storage>) -> bool {
    held.upgrade()
        .is_some_and(|storage| std::ptr::addr_eq(Arc::as_ptr(&storage), Arc::as_ptr(pin)))
}

/// Take back whatever this bucket could not land last pass, under `delta`; this
/// pass's observation of a package is the fresher one and wins. Carries whose
/// bucket is gone are dropped in the same sweep.
fn merge_unlanded(pin: &Arc<dyn Storage>, delta: &mut crate::transparency::Delta) {
    let mut carried = unlanded_deltas()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    carried.retain(|(held, _)| held.strong_count() > 0);
    if let Some(position) = carried.iter().position(|(held, _)| same_handle(held, pin)) {
        let (_, previous) = carried.swap_remove(position);
        for (pkg, files) in previous {
            delta.entry(pkg).or_insert(files);
        }
    }
}

/// Hold a delta no bucket accepted, for this bucket's next pass.
fn carry_unlanded(pin: &Arc<dyn Storage>, delta: crate::transparency::Delta) {
    let mut carried = unlanded_deltas()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match carried.iter_mut().find(|(held, _)| same_handle(held, pin)) {
        Some((_, kept)) => kept.extend(delta),
        None => carried.push((Arc::downgrade(pin), delta)),
    }
}

/// Append a hash-chained transparency checkpoint committing this pass's changed
/// packages. Never fails the audit: anything that stops the append warns, counts
/// a `chain_checkpoint_deferrals`, and carries the delta to the next audit, which
/// re-attempts. Deferral is the *designed* outcome whenever the fleet's head
/// cannot be vouched for, so that counter — not the absence of one — is what an
/// operator watches; a value climbing every audit means the chain has stopped
/// advancing. Leader-gated by construction — called only from
/// `audit`, after the fingerprint and global-index writes, on the same pin and
/// generation.
///
/// Reconcile first, append second. Catching the fleet up before writing is what
/// keeps a failover leader from spending a seq a peer already used under different
/// bytes: once the peers agree, this bucket's head *is* the fleet head. The
/// reconcile runs every audit regardless of churn, so an idle fleet still
/// converges, and it doubles as the write-through backstop for a bucket that
/// missed a link or was added later.
async fn write_chain_link(
    state: &AppState,
    pinned: &crate::buckets::Pinned,
    generation: u64,
    mut delta: crate::transparency::Delta,
) {
    let handles = state.buckets.handles();
    if handles.get(pinned.index).is_none() {
        error!(
            bucket = pinned.index,
            "transparency: pinned bucket has no handle; checkpoint skipped"
        );
        return;
    }
    // The chain fleet in config order: the write pin plus every eligible peer.
    // Config order is what makes the append arbiter below the same bucket on every
    // node, which is what a CAS can arbitrate.
    let peers = state.singleton_replicas(pinned.index);
    let mut fleet: Vec<crate::layout::ReplicaTarget<'_>> = Vec::with_capacity(peers.len() + 1);
    let mut primary = 0usize;
    for (index, handle) in handles.iter().enumerate() {
        if index == pinned.index {
            primary = fleet.len();
            fleet.push(crate::layout::ReplicaTarget {
                storage: pinned.storage.as_ref(),
                name: handle.name.as_str(),
            });
        } else if let Some(peer) = peers.iter().find(|peer| peer.name == handle.name) {
            fleet.push(crate::layout::ReplicaTarget {
                storage: peer.storage,
                name: peer.name,
            });
        }
    }
    let synced = crate::transparency::catch_up_fleet(&fleet, primary).await;
    // Append only on churn — but a delta a previous pass could not land is churn
    // that has not been committed yet, so it rides along on the next pass even if
    // this one is idle.
    merge_unlanded(&pinned.storage, &mut delta);
    if delta.is_empty() {
        return;
    }
    if synced.in_sync.is_empty() {
        warn!(
            "transparency: no bucket could arbitrate the checkpoint; carrying it to the next audit"
        );
        defer_checkpoint(state, pinned, delta);
        return;
    }
    if let Some(unlanded) =
        append_chain_link(state, &fleet, primary, synced, generation, delta).await
    {
        defer_checkpoint(state, pinned, unlanded);
    }
}

/// Hold a checkpoint this pass could not land, and say so on the one surface an
/// operator can watch. Every fail-closed exit in the chain path lands here, so
/// the counter is the alarm for "the chain has stopped advancing" — the warns
/// name which bucket, the counter says it kept happening.
fn defer_checkpoint(
    state: &AppState,
    pinned: &crate::buckets::Pinned,
    delta: crate::transparency::Delta,
) {
    state
        .metrics
        .chain_checkpoint_deferrals
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    carry_unlanded(&pinned.storage, delta);
}

/// Append one hash-chained link committing `delta` and mirror it across the
/// fleet. Returns the delta when it landed nowhere, so the caller can carry it
/// into the next pass.
///
/// The seq is decided by one create-if-absent CAS on the **arbiter**: the first
/// bucket, in config order, whose chain this pass read cleanly (`synced`). Every
/// leader picks the same bucket, so two of them racing the same seq are resolved
/// by the store instead of by whoever wrote last. The link itself chains onto this
/// bucket's head, which the catch-up just made the fleet head. One head read
/// confirms that immediately before the CAS, and it is the arbiter's *own* head
/// that decides: at or past this seq, or holding other bytes at seq-1, means the
/// bucket moved since the catch-up and this pass carries its delta instead of
/// racing it. Only *behind* is allowed through — a peer left short by a failed
/// copy stays an arbiter candidate on purpose (one transient write error must not
/// move the arbiter), and it can take this link and be backfilled under it. That
/// is a real cost, not a free pass: until the next catch-up fills the gap, that
/// bucket's chain has a hole, and `verify-chain` faults it (`gap`, and
/// `broken-link` at the hole's upper edge) with exit 1 the whole time. A
/// checkpoint that waits is the cheaper failure, which is why every other
/// mismatch defers.
///
/// With the pre-check in place, losing the CAS means a leader wrote the arbiter
/// in the window between that head read and this write — narrow, and no longer
/// the ordinary way two leaders meet. It still must not abandon the link: the
/// loser adopts the winner (pulling it onto this bucket) and appends at the next
/// seq in the same pass, because dropping the delta would leave the chain
/// committing an old sha over bytes storage has already replaced, which
/// `verify-chain` reports as a tamper that never clears. Two attempts, then
/// carry: a racing leader can only take the seq we just read, so a second loss
/// means to stop spinning, not to give up.
async fn append_chain_link(
    state: &AppState,
    fleet: &[crate::layout::ReplicaTarget<'_>],
    primary: usize,
    synced: crate::transparency::FleetChain,
    generation: u64,
    delta: crate::transparency::Delta,
) -> Option<crate::transparency::Delta> {
    let mut synced = synced;
    let Some(pin) = fleet.get(primary) else {
        return Some(delta);
    };
    for attempt in 0..2 {
        let Some(&decider) = synced.in_sync.first() else {
            break;
        };
        let Some(arbiter) = fleet.get(decider) else {
            break;
        };
        if let Err(e) = require_generation(state, generation) {
            warn!(error=?e, "transparency: generation changed; checkpoint carried to the next audit");
            return Some(delta);
        }
        let (seq, prev_sha256) = match crate::transparency::read_head(pin.storage).await {
            Ok(Some((head_seq, bytes))) => (head_seq + 1, sha256_hex(&bytes)),
            Ok(None) => (0, String::new()),
            Err(e) => {
                error!(error=?e, "transparency: could not read chain head; checkpoint carried");
                return Some(delta);
            }
        };
        // The seq and `prev-sha256` come from the pin, but the CAS lands on the
        // arbiter, so the arbiter must not already be past the seq we are about
        // to spend, and where it stands at seq-1 it must hold the very bytes we
        // are chaining onto. The catch-up just made both true; one read confirms
        // it rather than trusting a peer that may have moved since. Behind is
        // allowed and deliberately not "corrected" by deriving the seq from the
        // arbiter instead: a peer left behind by a failed copy stays an arbiter
        // candidate on purpose, and numbering off its short chain would re-issue
        // a seq the pin already holds — forking the other way.
        if decider != primary {
            match crate::transparency::read_head(arbiter.storage).await {
                Ok(head) => {
                    let chains_on = match &head {
                        Some((head_seq, head_bytes)) => {
                            *head_seq < seq
                                && (*head_seq + 1 != seq || sha256_hex(head_bytes) == prev_sha256)
                        }
                        None => true,
                    };
                    if !chains_on {
                        warn!(
                            seq,
                            bucket = %arbiter.name,
                            arbiter_head = ?head.as_ref().map(|(s, _)| *s),
                            "transparency: the arbiter's chain moved out from under this pass; checkpoint carried to the next audit"
                        );
                        return Some(delta);
                    }
                }
                Err(e) => {
                    error!(error=?e, bucket = %arbiter.name, "transparency: could not read the arbiter's chain head; checkpoint carried");
                    return Some(delta);
                }
            }
        }
        let created =
            match crate::clock::now_utc().format(&time::format_description::well_known::Rfc3339) {
                Ok(created) => created,
                Err(e) => {
                    error!(error=?e, "transparency: timestamp format failed; checkpoint carried");
                    return Some(delta);
                }
            };
        let link = crate::transparency::ChainLink {
            seq,
            prev_sha256,
            created,
            packages: delta.clone(),
        };
        let bytes = match crate::transparency::link_bytes(&link) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(error=?e, "transparency: serialize failed; checkpoint carried");
                return Some(delta);
            }
        };
        match arbiter
            .storage
            .put_if_none_match(&crate::transparency::chain_key(seq), bytes.clone())
            .await
        {
            Ok(Some(_)) => {
                info!(
                    seq,
                    packages = link.packages.len(),
                    bucket = %arbiter.name,
                    "transparency: checkpoint written"
                );
                // Mirror the immutable link to every bucket the catch-up found
                // consistent with this chain. A forked or unreadable bucket is not
                // in that set and is deliberately left untouched.
                for position in &synced.in_sync {
                    if *position == decider {
                        continue;
                    }
                    let Some(bucket) = fleet.get(*position) else {
                        continue;
                    };
                    if let Err(e) = crate::transparency::copy_link(bucket, seq, &bytes).await {
                        warn!(bucket = %bucket.name, seq, error = ?e, "transparency: mirroring the checkpoint to a peer failed; retries next audit");
                    }
                }
                return None;
            }
            Ok(None) => {
                if attempt == 1 {
                    warn!(
                        seq,
                        "transparency: lost the checkpoint CAS twice; carrying it to the next audit"
                    );
                    break;
                }
                // Adopt the winner instead of dropping our delta: the catch-up
                // pulls its link onto this bucket, and the next attempt chains
                // onto it at the following seq.
                synced = crate::transparency::catch_up_fleet(fleet, primary).await;
            }
            Err(e) => {
                error!(error=?e, seq, bucket = %arbiter.name, "transparency: checkpoint write failed; carried to the next audit");
                return Some(delta);
            }
        }
    }
    Some(delta)
}

struct ShardAudit {
    live: Vec<String>,
    /// Observed with no artifacts: must not be listed globally.
    dead: Vec<String>,
    rebuilt: usize,
    /// Provably unchanged (fingerprint hit): zero reads spent.
    skipped: usize,
    failures: usize,
    /// Packages in this shard whose PEP 792 status blocks downloads
    /// (`quarantined`), derived from the sidecars the listing flagged. Merged
    /// across shards into the fleet quarantined set once a full sweep completes.
    quarantined: Vec<String>,
    /// Per-package inventory derived from the shard listing (no extra reads):
    /// artifact files (sidecars excluded), bytes, and distinct releases, for
    /// every package with at least one artifact. Summed to re-baseline the
    /// in-memory inventory map authoritatively each sweep.
    pkg_stats: Vec<(String, PkgStat)>,
    /// Transparency-checkpoint deltas: each package that changed this pass (or,
    /// at genesis, every package) → its complete current file→sha256 map. Empty
    /// when `--transparency` is off. An empty inner map means the package went
    /// away (a removal on chain replay).
    deltas: Vec<(String, FileShas)>,
    /// Advisory-report inventory: `(package, artifact filenames)` for every
    /// package this shard holds whose name is in the audit index. Collected from
    /// the listing already in hand (no extra reads), and only for advisory-matched
    /// names, so it stays proportional to the corpus ∩ OSV, not corpus size. The
    /// sweep tail resolves versions and joins downloads to materialize the report.
    inventory: Vec<(String, Vec<String>)>,
}

/// One package's audit inputs, derived from the flat listing in a single pass:
/// `(name, fingerprint, has-live-artifacts, has-materialized-view,
/// interrupted-deletes, orphaned-companions)`. The two trailing lists are the
/// debris the listing flagged for repair (a crashed delete's remnants, and a
/// companion stranded beside a vanished artifact).
type PackageAudit = (String, String, bool, bool, Vec<String>, Vec<String>);

/// Audit every package whose name starts with `shard`. `collect_deltas` gathers
/// transparency-checkpoint deltas (off when `--transparency` is disabled);
/// `need_full_shas` additionally reads unchanged packages' sidecars so the
/// genesis checkpoint can commit the whole corpus — a one-time cost only when
/// the chain is empty.
#[allow(clippy::too_many_arguments)]
async fn audit_shard(
    state: &AppState,
    storage: &dyn Storage,
    generation: u64,
    shard: char,
    force_deep: bool,
    collect_deltas: bool,
    need_full_shas: bool,
    advisory_db: Option<&crate::advisories::AdvisoryDb>,
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
        quarantined: Vec::new(),
        deltas: Vec::new(),
        inventory: Vec::new(),
    };
    let mut fresh: std::collections::HashMap<String, String> = Default::default();
    // Packages whose listing shows the PEP 792 sidecar: the rare set that pays a
    // status read below to derive the quarantined set. Collected independent of
    // `--malware-block` — quarantine refusal is a separate guarantee from OSV
    // blocking — but only for projects that actually carry a status marker, so a
    // deployment with none does zero extra work.
    let mut status_pkgs: Vec<String> = Vec::new();
    let mut packages: Vec<PackageAudit> = Vec::with_capacity(by_pkg.len());
    for (pkg, (t, v)) in by_pkg {
        let fp = fingerprint(&t, &v);
        // Count artifacts and distinct versions straight off the listing — the
        // same bytes the fingerprint already walked, so the inventory is free.
        let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
        let members: HashSet<&str> = t
            .iter()
            .filter_map(|obj| obj.key.strip_prefix(&prefix))
            .collect();
        if members.contains(PROJECT_STATUS_FILE) {
            status_pkgs.push(pkg.clone());
        }
        let mut versions: HashSet<String> = HashSet::new();
        let mut file_count = 0u32;
        let mut pkg_bytes = 0u64;
        // Collect artifact filenames for the advisory report, but only for names
        // the audit index carries — the corpus ∩ OSV, a tiny set — so the walk
        // stays free for everything else. Version resolution happens in the tail.
        let advisory_matched = advisory_db.is_some_and(|db| db.audit_has_name(&pkg));
        let mut advisory_filenames: Vec<String> = Vec::new();
        for obj in &t {
            if let Some(filename) = obj.key.strip_prefix(&prefix) {
                if is_artifact(filename) {
                    file_count += 1;
                    pkg_bytes += obj.size;
                    if let Some(version) = infer_version_from_filename(filename) {
                        versions.insert(version);
                    }
                    if advisory_matched {
                        advisory_filenames.push(filename.to_string());
                    }
                }
            }
        }
        if advisory_matched && !advisory_filenames.is_empty() {
            out.inventory.push((pkg.clone(), advisory_filenames));
        }
        // Any record object sitting beside its own bare `.tombstone` is a
        // delete that crashed mid-removal: a live body (crash before the
        // artifact delete — the bytes stay downloadable by direct URL) or an
        // orphaned sidecar/companion (crash between the artifact delete and
        // the companion deletes — permanent debris the merge would otherwise
        // re-visit forever). A `.frozen` marker deliberately retains its
        // record as evidence and is skipped. Detected from the listing
        // already in hand, so the single-bucket audit stays free.
        let mut interrupted_deletes: Vec<String> = Vec::new();
        for member in &members {
            let Some(filename) = member.strip_suffix(TOMBSTONE_SUFFIX) else {
                continue;
            };
            if members.contains(format!("{filename}{FROZEN_SUFFIX}").as_str()) {
                continue;
            }
            let remnants = [
                filename.to_string(),
                format!("{filename}{SIDECAR_SUFFIX}"),
                format!("{filename}{METADATA_SUFFIX}"),
                format!("{filename}{PROVENANCE_SUFFIX}"),
            ];
            if remnants.iter().any(|key| members.contains(key.as_str())) {
                interrupted_deletes.push(filename.to_string());
            }
        }
        // `members` is a HashSet: sort so the repair order (and therefore the
        // storage-op order a deterministic simulation replays) is stable.
        interrupted_deletes.sort();
        // A companion (sidecar/metadata/provenance) sitting beside NO artifact
        // and NO deliberate marker is stranded debris — the mirror image of the
        // orphaned-artifact backfill above. A failed upload's own rollback
        // deleting bytes the audit had already fabricated a sidecar for, or an
        // interrupted sidecar-first replication copy, leaves it. With no
        // artifact to list and no tombstone to trigger the interrupted-delete
        // sweep, it hides from every rebuild; and the cross-bucket merge reads
        // sidecar-without-artifact as `Absent`, so the tree diff never converges
        // it away. Flag the base for a re-verified drop; quarantined mirror
        // bytes keep their sidecar as canonical evidence and are excluded.
        let mut orphan_companions: Vec<String> = Vec::new();
        for member in &members {
            let Some(filename) = member
                .strip_suffix(SIDECAR_SUFFIX)
                .or_else(|| member.strip_suffix(METADATA_SUFFIX))
                .or_else(|| member.strip_suffix(PROVENANCE_SUFFIX))
            else {
                continue;
            };
            if !is_artifact(filename) {
                continue;
            }
            let anchored = members.contains(filename)
                || members.contains(format!("{filename}{TOMBSTONE_SUFFIX}").as_str())
                || members.contains(format!("{filename}{FROZEN_SUFFIX}").as_str())
                || members.contains(
                    format!("{filename}{}", crate::sidecar::MIRROR_QUARANTINED_SUFFIX).as_str(),
                );
            if !anchored {
                orphan_companions.push(filename.to_string());
            }
        }
        orphan_companions.sort();
        orphan_companions.dedup();
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
            orphan_companions,
        ));
    }

    for chunk in packages.chunks(PACKAGE_SWEEP_CONCURRENCY) {
        let jobs = chunk
            .iter()
            .map(|(pkg, fp, has_artifacts, has_package_view, interrupted_deletes, orphan_companions)| {
            let fingerprint_unchanged = stored.get(pkg.as_str()) == Some(fp)
                && interrupted_deletes.is_empty()
                && orphan_companions.is_empty();
            let (pkg, fp, has_artifacts, has_package_view, interrupted_deletes, orphan_companions) = (
                pkg.clone(),
                fp.clone(),
                *has_artifacts,
                *has_package_view,
                interrupted_deletes.clone(),
                orphan_companions.clone(),
            );
            async move {
                if let Err(e) = require_generation(state, generation) {
                    error!(package=%pkg, error=?e, "audit: selection changed before package batch");
                    return (pkg, None, has_artifacts, false, true, None);
                }
                if state.buckets.is_multi() {
                    match crate::origin::read_origin_observation(storage, &pkg).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!(package=%pkg, error=?e, "audit: origin unreadable; deferring maintenance");
                            return (pkg, None, has_artifacts, false, true, None);
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
                // Drop companions stranded beside a vanished artifact with no
                // marker — debris the tree diff would otherwise carry forever.
                // Same rare-package gating as the interrupted-delete sweep.
                if !orphan_companions.is_empty() {
                    match crate::tombstone::drop_orphan_companions(
                        state,
                        storage,
                        &pkg,
                        &orphan_companions,
                    )
                    .await
                    {
                        Ok(0) => {}
                        Ok(count) => {
                            maintenance_changed = true;
                            error!(package=%pkg, count, "audit: dropped orphaned sidecar/companion(s) with no artifact");
                        }
                        Err(e) => {
                            maintenance_failed = true;
                            error!(package=%pkg, error=?e, "audit: dropping orphaned companion failed");
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
                    // Only the genesis pass needs this package's shas (its
                    // rebuild didn't run); steady state contributes no delta.
                    let delta = if collect_deltas && need_full_shas && !maintenance_failed {
                        current_package_shas(storage, &pkg).await.ok()
                    } else {
                        None
                    };
                    return (
                        pkg,
                        Some(fp),
                        has_package_view,
                        false,
                        maintenance_failed,
                        delta,
                    );
                }
                match rebuild_package_excluding(state, storage, &pkg, None).await {
                    Ok((live_now, shas)) => {
                        // Fingerprint what the rebuild actually saw/wrote, not
                        // the pre-rebuild listing — two cheap per-package lists.
                        let new_fp = package_fingerprint(storage, &pkg).await.ok();
                        // The rebuild's own sidecar reads produced `shas`; commit
                        // them (an empty map = the package became dead).
                        let delta = collect_deltas.then_some(shas);
                        (pkg, new_fp, live_now, true, maintenance_failed, delta)
                    }
                    Err(e) => {
                        // Conservative on failure: keep the package listed and
                        // its views rather than pruning on a bad observation.
                        error!(package=%pkg, error=?e, "audit: package rebuild failed");
                        (pkg, None, has_artifacts, false, true, None)
                    }
                }
            }
        });
        for (pkg, fp, live_now, was_rebuilt, failed, delta) in futures::future::join_all(jobs).await
        {
            if let Some(fp) = fp {
                fresh.insert(pkg.clone(), fp);
            }
            if let Some(shas) = delta {
                out.deltas.push((pkg.clone(), shas));
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

    // Derive this shard's quarantined-project set: a status read for each of the
    // (rare) sidecar-bearing packages, bounded like the rebuild fan-out. A read
    // failure is a shard failure — `audit` then refuses to publish a set that
    // might be missing a still-quarantined project (a partial set flaps
    // dequarantines), so the last-published set stands.
    for chunk in status_pkgs.chunks(PACKAGE_SWEEP_CONCURRENCY) {
        let reads = chunk
            .iter()
            .map(|pkg| async move { (pkg, crate::status::read_status(storage, pkg).await) });
        for (pkg, result) in futures::future::join_all(reads).await {
            match result {
                Ok(doc) if doc.status.blocks_downloads() => out.quarantined.push(pkg.clone()),
                Ok(_) => {}
                Err(e) => {
                    error!(package=%pkg, error=?e, "audit: quarantined-set status read failed");
                    out.failures += 1;
                }
            }
        }
    }
    Ok(out)
}

/// Materialize the org audit report from the walked inventory. For each
/// advisory-matched package: read its origin claim (the row's label; the pure
/// join drops private-origin rows), resolve each artifact's version, and roll up
/// its 30-day download counts. The join then produces the ranked rows, written on
/// change. Matched packages are the corpus ∩ OSV — a tiny set — so the per-package
/// origin read and counter query here are cheap. No `unwrap`/`panic` on this
/// worker path; a per-package read failure skips that package, never the report.
async fn build_advisory_report(
    state: &AppState,
    storage: &dyn Storage,
    db: &crate::advisories::AdvisoryDb,
    inventory: Vec<(String, Vec<String>)>,
    quarantined: &HashSet<String>,
    feed_sha256: &str,
) -> Result<()> {
    let to = time::OffsetDateTime::now_utc().date();
    let from = to.saturating_sub(time::Duration::days(29));
    let mut entries: Vec<crate::advisories::AuditInventory> = Vec::new();
    for (pkg, filenames) in inventory {
        let origin = match crate::origin::read_origin_claim(storage, &pkg).await {
            Ok(Some(claim)) => claim.as_str(),
            Ok(None) => crate::origin::UNCLAIMED,
            Err(e) => {
                warn!(package=%pkg, error=?e, "audit report: origin read failed; skipping package");
                continue;
            }
        };
        // 30-day downloads for this package, rolled up filename → version.
        let series = state
            .counters
            .query_package("downloads", &pkg, from, to)
            .await;
        let mut downloads: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        for files in series.values() {
            for (filename, count) in files {
                if let Some(version) = infer_version_from_filename(filename) {
                    *downloads.entry(version).or_insert(0) += count;
                }
            }
        }
        for version in resolve_report_versions(storage, &pkg, &filenames).await {
            let downloads_30d = downloads.get(&version).copied().unwrap_or(0);
            entries.push(crate::advisories::AuditInventory {
                package: pkg.clone(),
                version,
                origin: origin.to_string(),
                downloads_30d,
            });
        }
    }
    let generated_unix = time::OffsetDateTime::now_utc().unix_timestamp().max(0) as u64;
    let report =
        crate::advisories::build_report(&entries, db, quarantined, generated_unix, feed_sha256);
    crate::advisories::write_report_if_changed(storage, &report).await
}

/// Resolve a matched package's artifact filenames to a sorted, deduped version
/// set: infer from the filename, fall back to the sidecar's version for a name
/// PEP 440 can't read, and skip a file whose version stays unknown. The fallback
/// reads only unparseable names (a rare legacy shape), bounded like every sweep.
async fn resolve_report_versions(
    storage: &dyn Storage,
    pkg: &str,
    filenames: &[String],
) -> Vec<String> {
    let mut versions: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut unparseable: Vec<&String> = Vec::new();
    for filename in filenames {
        match infer_version_from_filename(filename) {
            Some(version) => {
                versions.insert(version);
            }
            None => unparseable.push(filename),
        }
    }
    for chunk in unparseable.chunks(SIDECAR_READ_CONCURRENCY) {
        let reads = chunk.iter().map(|filename| {
            let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
            async move { storage.get_bytes(&sidecar_key(&key)).await }
        });
        for bytes in futures::future::join_all(reads).await.into_iter().flatten() {
            if let Ok(sc) = serde_json::from_slice::<Sidecar>(&bytes) {
                if !sc.version.is_empty() {
                    versions.insert(sc.version);
                }
            }
        }
    }
    versions.into_iter().collect()
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

/// Publish the tally of crashed-writer intents healed during a drain. Shared by
/// the selected-bucket [`tick`] and the destination [`drain_dirty_uncached`] —
/// the two schedulers are otherwise separate, but both heal a stale intent by
/// re-arming its marker and then report the same counter identically.
fn record_stale_intents_healed(state: &AppState, healed: u64) {
    if healed > 0 {
        state
            .metrics
            .stale_intents_healed
            .fetch_add(healed, std::sync::atomic::Ordering::Relaxed);
    }
}

pub async fn tick(state: &Arc<AppState>, pinned: &crate::buckets::Pinned) -> Result<()> {
    // The caller passes the exact pin whose bucket-local lease authorized this
    // tick. Selection may change concurrently, but this operation stays on that
    // handle; the per-package rebuilds spawn with an owned clone of it.
    let storage = pinned.storage.as_ref();
    let entries = storage.list_dir_entries(DIRTY_PREFIX).await?;
    if entries.is_empty() {
        return Ok(());
    }

    let work = consumable_dirty_work(&entries, crate::clock::now_utc(), state.intent_grace);
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
                // A stale unpaired intent is *presumed* crashed — but the
                // writer may merely be paused past the grace and can still
                // mutate truth between this rebuild's listing and the marker
                // consumption below, then die before its commit. Consuming its
                // intent would erase the only signal covering those writes, so
                // re-arm a fresh dirty marker first: the next tick re-derives
                // from post-mutation truth (a no-op when nothing changed). If
                // the re-arm cannot be written, retain the originals instead.
                if stale_intents > 0 {
                    if let Err(e) = mark_dirty(pinned.storage.as_ref(), &pkg).await {
                        error!(package=%pkg, error=?e, "could not re-arm dirty marker after healing stale intent; markers retained");
                        failures += 1;
                        continue;
                    }
                }
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
    record_stale_intents_healed(state, healed);
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
    Ok(rebuild_package_excluding(state, storage, pkg, None)
        .await?
        .0)
}

/// Like `rebuild_package`, but omitting one filename from the views. Deletion
/// uses this to drop the file from the index *before* removing the artifact —
/// views may lag truth but never lead it.
pub async fn rebuild_package_excluding(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    omit: Option<&str>,
) -> Result<(bool, FileShas)> {
    let RebuiltPackage {
        still_live: live,
        raw_artifacts: raw,
        file_shas: shas,
    } = rebuild_package_indexes(state, storage, pkg, omit).await?;
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
    // `shas` is the renderable file→sha256 map the transparency audit commits;
    // the delete/tick callers discard it, the audit threads it into the chain.
    Ok((live, shas))
}

/// The outcome of rebuilding a package's derived index views (see
/// [`rebuild_package_indexes`]).
pub(crate) struct RebuiltPackage {
    /// Whether the package still has renderable artifacts (`false` means the
    /// index views were removed).
    still_live: bool,
    /// The raw `(filename, size)` listing — the inventory writer's input.
    raw_artifacts: Vec<(String, u64)>,
    /// The renderable file→sha256 map (the transparency chain's commitment).
    file_shas: FileShas,
}

/// Rebuild only a package's index views from `storage`'s own truth, touching no
/// node-local inventory. `rebuild_package_excluding` layers the selected
/// bucket's inventory on top; the replicator (src/replicate.rs) calls this
/// directly against a *non-selected* destination bucket, where mixing that
/// bucket's counts into the node's inventory (or its name cache) would be wrong.
pub(crate) async fn rebuild_package_indexes(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    omit: Option<&str>,
) -> Result<RebuiltPackage> {
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
) -> Result<RebuiltPackage> {
    state
        .metrics
        .index_rebuilds
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (mut files, mut raw) = list_artifacts_for_claim(storage, pkg, true).await?;
    if let Some(omit) = omit {
        files.retain(|f| f.filename != omit);
        raw.retain(|(filename, _)| filename != omit);
    }
    // Denylist scrub: drop `--exclude-package` matches from the *renderable* view
    // so installers can't resolve them, exactly as the malware scrub does — a
    // fully-denied name empties `files`, so the index is deleted and the name
    // leaves the global list below. `raw` (the inventory input) is left intact:
    // the bytes are only delisted, not deleted, so they still count as stored and
    // stay fetchable by direct `/files/` URL. Unblocking (removing the exclude)
    // relists the package on its next rebuild with no re-download.
    if let Some(denylist) = state.denylist.as_ref() {
        files.retain(|f| !denylist.file_denied(pkg, &f.filename));
    }
    // The renderable files carry the sha256 each one's sidecar just yielded —
    // the transparency chain's commitment, captured here at no extra reads.
    let shas: FileShas = files
        .iter()
        .map(|f| (f.filename.clone(), f.sha256.clone()))
        .collect();
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
    // Drop the package's cached human `/project/` pages the same way the simple
    // indexes are invalidated above: this runs only when a package actually
    // changed (its fingerprint/dirty marker moved), so a same-node reader sees
    // the upload immediately instead of waiting out the cache TTL.
    state.project_cache.invalidate_package(pkg);
    Ok(RebuiltPackage {
        still_live: live,
        raw_artifacts: raw,
        file_shas: shas,
    })
}

/// The denylist state a bucket's stored indexes were last built against: a small
/// bucket-local `_state/` sidecar (like the fingerprint shards) the startup
/// reconcile diffs the live `--exclude-package` config against. A lost stamp only
/// costs one extra reconcile pass.
fn enforced_excludes_key() -> String {
    format!("{STATE_PREFIX}enforced-excludes.json")
}

/// The denylist a bucket's stored indexes were last built against, or `None` when
/// no stamp exists yet (first run). A *not-found* is the ordinary first-run case;
/// any other read error propagates rather than masquerading as "nothing enforced"
/// — collapsing a transient (or persistent) read fault to empty would spin a full
/// re-delist every pass instead of surfacing, the way [`invalidate_fingerprints`]
/// already distinguishes the two. A present-but-unparsable body reads as empty (a
/// harmless full re-delist), only truly missing bytes are `None`.
pub(crate) async fn read_enforced_excludes(
    storage: &dyn Storage,
) -> Result<Option<std::collections::BTreeMap<String, Vec<String>>>> {
    match storage.get_bytes(&enforced_excludes_key()).await {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).unwrap_or_default())),
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e).context("reading the enforced-denylist stamp"),
    }
}

/// The denylist the offline maintenance commands (`verify-index`, `rebuild-index`)
/// must rule through: whatever `serve` last enforced, read from the persisted
/// stamp so they agree with the running server regardless of which channel
/// (flag / `PYPIRON_EXCLUDE_PACKAGE` / config) set the excludes. With no stamp yet
/// — a store `serve` never enforced against — fall back to the config the command
/// resolved, so a first-ever offline pass still honors `pypiron.toml`.
pub(crate) async fn enforced_denylist(
    storage: &dyn Storage,
    fallback: &crate::denylist::Denylist,
) -> Result<crate::denylist::Denylist> {
    match read_enforced_excludes(storage).await? {
        Some(canonical) => crate::denylist::Denylist::from_canonical(&canonical),
        None => Ok(fallback.clone()),
    }
}

/// Drop the given packages from their audit fingerprint shards so the next audit
/// rebuilds them instead of skipping on a fingerprint that a denylist change left
/// stale. Grouped by shard (a normalized name's first character) so each shard
/// file is rewritten at most once. An absent shard file needs no work.
async fn invalidate_fingerprints(storage: &dyn Storage, packages: &[&str]) -> Result<()> {
    let mut by_shard: std::collections::BTreeMap<char, Vec<&str>> =
        std::collections::BTreeMap::new();
    for pkg in packages {
        if let Some(c) = pkg.chars().next() {
            by_shard.entry(c).or_default().push(pkg);
        }
    }
    for (shard, names) in by_shard {
        let key = format!("{STATE_PREFIX}fp-{shard}.json");
        let mut stored: std::collections::BTreeMap<String, String> =
            match storage.get_bytes(&key).await {
                Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
                Err(e) if is_not_found(&e) => continue,
                Err(e) => return Err(e),
            };
        let mut changed = false;
        for name in names {
            changed |= stored.remove(name).is_some();
        }
        if changed {
            let bytes = serde_json::to_vec(&stored)?;
            storage
                .put_bytes(&key, bytes, Some("application/json"))
                .await?;
        }
    }
    Ok(())
}

/// Reconcile one bucket's stored indexes with the live `--exclude-package`
/// denylist. The denylist is startup config, so a change only lands across a
/// restart; the audit can't catch it (a config-only change moves no artifact, so
/// the package fingerprint is unchanged and the audit skips the rebuild). This
/// bridges the gap: it diffs the live denylist against the stamp the stored
/// indexes were last built against and marks only the names whose entry changed —
/// added (delist), removed (relist), or a moved version pin (re-filter) — dirty,
/// so the ordinary tick rebuilds their per-package index and the global name list.
/// Never a whole-corpus rebuild, and relisting rebuilds from the artifacts already
/// on disk (no re-download). A no-op — one small GET — when the config is
/// unchanged. Runs per bucket; each keeps its own bucket-local stamp and worklist.
pub(crate) async fn reconcile_excludes(state: &AppState, storage: &dyn Storage) -> Result<()> {
    let Some(denylist) = state.denylist.as_ref() else {
        return Ok(());
    };
    let current = denylist.canonical();
    // Not-found → first run (empty); a real read fault propagates so a persistent
    // stamp error surfaces instead of silently re-delisting the whole set forever.
    let previous = read_enforced_excludes(storage).await?.unwrap_or_default();
    if current == previous {
        return Ok(());
    }
    // A name whose entry differs between the two sets has stale visibility in the
    // stored indexes and must be rebuilt; a name identical in both is untouched.
    let mut changed: std::collections::BTreeSet<&String> = std::collections::BTreeSet::new();
    for (name, specs) in &current {
        if previous.get(name) != Some(specs) {
            changed.insert(name);
        }
    }
    for (name, specs) in &previous {
        if current.get(name) != Some(specs) {
            changed.insert(name);
        }
    }
    let names: Vec<&str> = changed.iter().map(|s| s.as_str()).collect();
    let count = names.len();
    // Invalidate the affected packages' audit fingerprints first. A denylist
    // change moves no artifact, so a package's cached fingerprint still matches
    // truth — and the leader's boot audit, which trusts that fingerprint to skip
    // an "unchanged" package, would then reassert the *pre-change* visibility
    // from the stored view and clobber the rebuild the markers below trigger.
    // Dropping the fingerprint forces the audit to rebuild these names too, so it
    // agrees with the tick (both apply the live denylist). This runs before the
    // audit is spawned (the leader loop awaits the reconcile), so it is race-free.
    invalidate_fingerprints(storage, &names).await?;
    for name in &names {
        mark_dirty(storage, name).await?;
    }
    // Persist the newly enforced set only after every marker is down: a crash
    // between the two re-runs the reconcile (the diff still fires) rather than
    // recording enforcement that never happened.
    write_enforced_excludes(state, storage, &current).await?;
    if count > 0 {
        info!(
            packages = count,
            "worker: denylist changed since last run; marked affected packages for reindex"
        );
    }
    Ok(())
}

/// Persist the enforced-denylist stamp for a bucket. `rebuild-index` calls this
/// after its audit (which already applied the live denylist to every package),
/// so a later `serve` boot's reconcile sees the stamp already agrees with the
/// config and doesn't re-flag work the offline pass already did.
pub(crate) async fn write_enforced_excludes(
    state: &AppState,
    storage: &dyn Storage,
    canonical: &std::collections::BTreeMap<String, Vec<String>>,
) -> Result<()> {
    let bytes = serde_json::to_vec(canonical)?;
    put_if_changed(
        state,
        storage,
        &enforced_excludes_key(),
        bytes,
        "application/json",
    )
    .await
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
pub struct InventoryMap {
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
pub struct GlobalNames {
    etag: Option<String>,
    names: HashSet<String>,
    /// The stored `simple/index.html`'s ETag as of this load. Every HTML write
    /// is conditional on it, so a node holding a stale view of the HTML cannot
    /// clobber a fresher one — its write fails and it reloads. `None` when no
    /// HTML has been published yet, or on a single-writer backend with no
    /// conditional writes (disk), where there are no peers to race.
    html_etag: Option<String>,
    /// Whether this cache has already *proved* `simple/index.html` renders
    /// exactly `names` — as of `html_etag`, and no longer than that. The proof
    /// comes from storage (the durable stamp at [`global_html_stamp_key`]),
    /// never from optimism: a node's belief that it left the HTML current is
    /// only ever true of its own last write, and a peer's write invalidates it
    /// silently. So the memo is scoped to the object it was proved against —
    /// the no-op gate re-observes the HTML's ETag in the listing it already
    /// makes, and any movement drops the flag. Without that scoping a CAS
    /// winner never re-checked the pair, and a peer that crashed between its
    /// two writes left this node serving an HTML the JSON did not back.
    html_current: bool,
    /// An HTML view exists but the canonical JSON does not — a crash landed
    /// between the two writes. The name set below was derived from the
    /// per-package views rather than read from the JSON, so a delta that dedups
    /// against it proves nothing: the JSON is still missing and no `changed`
    /// will ever be computed to create it. Such a cache must write once
    /// regardless, which materializes the authority and clears this.
    stranded: bool,
}

/// Where the durable proof of global-HTML currency lives. Deliberately in the
/// per-bucket coordination area rather than under `simple/`: it describes one
/// bucket's derived view, is never served, and must never replicate.
fn global_html_stamp_key() -> String {
    format!("{STATE_PREFIX}global-html.json")
}

/// The proof that a specific stored `simple/index.html` renders a specific name
/// set. `html_etag` is what makes it trustworthy: HTML writes are conditional,
/// so any later write moves the ETag and this stamp stops matching — a stamp can
/// never speak for an HTML object it did not itself produce.
#[derive(serde::Serialize, serde::Deserialize)]
struct GlobalHtmlStamp {
    html_etag: String,
    names: String,
}

/// Identity of a rendered global index: a digest over the sorted name list.
/// Content-addressed rather than ETag-addressed so it means the same thing in
/// every bucket and on every backend.
fn global_names_digest(packages: &[String]) -> String {
    crate::hash::sha256_hex(packages.join("\n").as_bytes())
}

/// Read the currency stamp, if one was ever written. A missing or unparsable
/// stamp is simply "unproven" — it forces the full reconcile, never an error:
/// the stamp is an optimization over reading the HTML body, not an authority.
async fn read_global_html_stamp(storage: &dyn Storage) -> Option<GlobalHtmlStamp> {
    match storage.get_bytes(&global_html_stamp_key()).await {
        Ok(bytes) => serde_json::from_slice(&bytes).ok(),
        Err(_) => None,
    }
}

/// Record that the HTML at `html_etag` renders `packages`. Written *after* the
/// HTML it describes, so a crash in between leaves the stamp stale — which
/// costs one redundant reconcile and never a false claim of currency.
async fn write_global_html_stamp(
    storage: &dyn Storage,
    html_etag: &Option<String>,
    packages: &[String],
) -> Result<()> {
    let Some(html_etag) = html_etag else {
        return Ok(()); // No conditional writes: no peers, nothing to prove.
    };
    let stamp = GlobalHtmlStamp {
        html_etag: html_etag.clone(),
        names: global_names_digest(packages),
    };
    storage
        .put_bytes(
            &global_html_stamp_key(),
            serde_json::to_vec(&stamp)?,
            Some("application/json"),
        )
        .await
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
    // Invariant: the cached name set must never outlive a write we could not
    // prove landed. `update_global_index_locked` absorbs the delta into the
    // cache *before* the conditional write; if that write (or its currency
    // probe) errors, the delta is already applied against the old pinned ETag. A
    // surviving cache makes the retry compute `changed = false`, the ETag probe
    // still matches (nothing moved), and the tick returns Ok — consuming the
    // dirty markers and dropping the delta until the audit (a dead package left
    // listed globally; the sim caught this as seed 19026). Drop the cache on
    // every error so the retry reloads from storage and re-detects the delta.
    let result = update_global_index_locked(state, storage, adds, removes, &mut guard).await;
    if result.is_err() {
        *guard = None;
    }
    result
}

/// The CAS loop for [`update_global_index`], operating on the held cache guard.
/// Split out so the wrapper can invalidate the cache on any error exit (see the
/// invariant there). On success the cache stays pinned to the ETag the write
/// returned; never add an error path here that leaves the cache mutated.
async fn update_global_index_locked(
    state: &AppState,
    storage: &dyn Storage,
    adds: &[String],
    removes: &[String],
    guard: &mut Option<GlobalNames>,
) -> Result<()> {
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
        if !changed && !cached.stranded {
            // The delta landed entirely inside the cached set. On a backend
            // with peer writers that is NOT yet proof of a no-op: a peer may
            // have changed the stored set since this cache was pinned (add a
            // name this node never saw, so this node's remove of it dedups
            // against thin air — leaving a dead package listed globally until
            // the audit, a divergence the deterministic simulator caught).
            // One bounded LIST validates currency; a moved ETag reloads and
            // reapplies. Single-writer backends (disk) skip the probe — no
            // peers, and their listing/CAS etags live in different spaces.
            if storage.supports_leases() {
                let (json_etag, html_etag) = global_index_etags(storage).await?;
                // Two absent JSONs are not the same state. `None` reads
                // identically whether nothing was ever published — the cold
                // start this cache may have been pinned in, names legitimately
                // empty — or a peer published the HTML and died before the
                // canonical JSON. Only [`load_global_names`] tells them apart,
                // and an equal-ETag probe never takes one, so the second case
                // survived every tick: the reconcile below rewrote the live
                // HTML down to this cache's stale (empty) name set, declared the
                // pair current, and consumed the markers, leaving the canonical
                // JSON absent until the tier-3 audit (vopr seed
                // 13792606396100784374). A published HTML standing over an
                // absent JSON is exactly that stranded pair — drop the cache so
                // the reload derives the real set from the per-package views and
                // its `stranded` flag forces the JSON to be materialized.
                // Only that tear is discriminated. With BOTH views absent under
                // a cache that saw names — an operator wiping a mature bucket's
                // global index, which no crash produces and the simulator
                // therefore never draws — this still reads as a cold start, and
                // the next delta writes the truncated view back until the tier-3
                // audit rebuilds it.
                if json_etag != cached.etag || (json_etag.is_none() && html_etag.is_some()) {
                    *guard = None;
                    continue;
                }
                // The same listing carries the HTML's ETag, and it has to be
                // consulted: `html_current` is a claim about a fleet-shared
                // object, so it stays true only while that object has not
                // moved. A peer that publishes the HTML and dies before the
                // canonical JSON moves exactly one of the two, which is
                // precisely what the JSON check above cannot see — and a memo
                // pinned by this node's own CAS win would never look again,
                // making the drift permanent here until the tier-3 audit
                // (vopr seeds 60000037578 / 61000075134, crash-only, one
                // bucket). Re-pin the observed ETag and let the stamp below
                // decide what the moved object now renders.
                if html_etag != cached.html_etag {
                    cached.html_etag = html_etag;
                    cached.html_current = false;
                }
            }
            // Currency of the *JSON* is not currency of the HTML: a crash
            // between the two writes leaves the HTML ahead, and returning here
            // would make that the final word — a drift nothing heals, since the
            // audit reaches this same gate. Prove currency from the durable
            // stamp rather than assuming it: one metadata-sized read, no body,
            // so the check costs the same whether the index holds four names or
            // 780,000. Only an unproven pair pays the byte compare, and only
            // once per observed HTML ETag.
            if !cached.html_current {
                let mut packages: Vec<String> = cached.names.iter().cloned().collect();
                packages.sort();
                if !global_html_proved_current(storage, &cached.html_etag, &packages).await {
                    match reconcile_global_html(state, storage, &packages, &cached.html_etag)
                        .await?
                    {
                        HtmlReconcile::Current => {}
                        HtmlReconcile::Rewrote(html_etag) => {
                            write_global_html_stamp(storage, &html_etag, &packages).await?;
                            cached.html_etag = html_etag;
                        }
                        HtmlReconcile::Lost => {
                            // A peer moved the HTML under us, so this cache's
                            // name set may be stale too. Reload and re-evaluate
                            // rather than fight over the object.
                            *guard = None;
                            continue;
                        }
                    }
                }
                if let Some(cached) = guard.as_mut() {
                    cached.html_current = true;
                }
            }
            return Ok(());
        }
        let mut packages: Vec<String> = cached.names.iter().cloned().collect();
        packages.sort();
        let expected_json = cached.etag.clone();
        let expected_html = cached.html_etag.clone();
        match write_global_indexes_cas(state, storage, &packages, &expected_json, &expected_html)
            .await?
        {
            CasOutcome::Won {
                json_etag,
                html_etag,
            } => {
                if let Some(cached) = guard.as_mut() {
                    // Pin the ETags the conditional writes themselves returned,
                    // not ones from a follow-up GET — a peer could land a write
                    // between the two and we'd pin its ETag against our stale
                    // name set.
                    cached.etag = json_etag;
                    cached.html_etag = html_etag;
                    cached.html_current = true;
                    cached.stranded = false;
                }
                return Ok(());
            }
            CasOutcome::Lost => {}
        }
        // Lost a CAS to a peer: another node updated the global index under us.
        // If the HTML write lost, nothing was published at all. If the JSON
        // write lost, we did move the HTML to a set that then lost — but no
        // stamp was written for it, so the next load cannot prove currency and
        // reconciles. Either way dropping the cache is what heals it: the reload
        // clears `html_current`, so even a delta that turns into a no-op
        // re-establishes the pair above. Count the conflict first (operators
        // watch this to confirm dual leadership converges rather than corrupts).
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
/// what keeps a warm copy's indexes fresh. It never reads or mutates the
/// node-local name and inventory caches — those describe the *selected* bucket
/// and a destination's name set would corrupt them. The two by-key caches on the
/// shared publish path (the served index bodies, the rendered `/projects/` page)
/// are dropped, not written: each re-derives from the selected bucket on the next
/// read, so a spurious drop costs one re-render and can never serve a
/// destination's view. A package that fails to rebuild keeps its markers for the
/// next pass; every package that does rebuild asserts its membership into the
/// destination's own global index, which dedups the assertion against the name
/// set it loads.
pub async fn drain_dirty_uncached(state: &AppState, storage: &dyn Storage) -> Result<()> {
    let entries = storage.list_dir_entries(DIRTY_PREFIX).await?;
    if entries.is_empty() {
        return Ok(());
    }
    let work = consumable_dirty_work(&entries, crate::clock::now_utc(), state.intent_grace);
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
        match rebuild_package_indexes(state, storage, &package, None).await {
            Ok(RebuiltPackage {
                still_live: live, ..
            }) => {
                // Same paused-writer hazard as the tick: a stale intent's
                // writer may still mutate after this rebuild's listing, so
                // re-arm before consuming or retain the originals.
                if stale_intents > 0 {
                    if let Err(e) = mark_dirty(storage, &package).await {
                        error!(package=%package, error=?e, "replicate: could not re-arm dirty marker after healing stale intent; markers retained");
                        continue;
                    }
                }
                // State what this rebuild found — member or not — and let
                // `update_global_index_uncached` dedup it against the global
                // name set it loads, exactly as the selected-bucket tick does.
                // Never diff it here against a probe of the package's *own*
                // index view: that view is this function's own output, so a
                // pass whose global write failed left the next pass reading its
                // own leftovers, concluding the membership already matched, and
                // dropping the delta for good — while consuming the markers
                // that were the only remaining signal. An assertion has no such
                // history: repeat it as often as you like and the first pass
                // that reaches the global index converges it.
                if live {
                    adds.push(package);
                } else {
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
    record_stale_intents_healed(state, healed);
    update_global_index_uncached(state, storage, &adds, &removes).await?;
    if let Err(e) = storage.delete_keys(&consumed).await {
        warn!(error=?e, "replicate: could not consume destination dirty markers");
    }
    Ok(())
}

/// Apply a package-name delta to a bucket's own global index, reading and
/// writing that bucket's `simple/index.{json,html}` directly — never through the
/// node-local name cache, which is pinned to the *selected* bucket. `adds` and
/// `removes` are assertions, not a pre-computed diff — the loaded name set is
/// the only thing that decides whether one is a change, and dedupping here is
/// what makes a repeated pass idempotent.
///
/// Not a single-writer path, whatever the comment here used to claim: every node
/// that can reach a destination drains it, and the bucket-local lease only makes
/// the duplicate work cheap — it expires under a slow drain like any other. So
/// the publish goes through the same conditional writer the selected-bucket path
/// uses, and a stale view loses instead of clobbering.
pub(crate) async fn update_global_index_uncached(
    state: &AppState,
    storage: &dyn Storage,
    adds: &[String],
    removes: &[String],
) -> Result<()> {
    if adds.is_empty() && removes.is_empty() {
        return Ok(());
    }
    for _attempt in 0..4 {
        let loaded = load_global_names(storage).await?;
        let loaded_json_etag = loaded.etag;
        let loaded_html_etag = loaded.html_etag;
        let stranded = loaded.stranded;
        let mut names = loaded.names;
        let mut changed = false;
        for pkg in adds {
            changed |= names.insert(pkg.clone());
        }
        for pkg in removes {
            changed |= names.remove(pkg);
        }
        let mut packages: Vec<String> = names.into_iter().collect();
        packages.sort();
        // A stranded load (HTML published, canonical JSON absent) derived its names
        // from the per-package views, so a delta that dedups against them is not a
        // no-op: the JSON still has to be materialized. Same rule the cached path
        // states — without it the HTML serves a set no canonical index backs.
        if !changed && !stranded {
            // Every call here loads fresh, so it knows the JSON and nothing about
            // the HTML — same hazard as the cached path (a crash between the two
            // writes strands the HTML ahead of the JSON). Prove currency from the
            // stamp first; only an unproven pair reads the body back.
            if global_html_proved_current(storage, &loaded_html_etag, &packages).await {
                return Ok(());
            }
            match reconcile_global_html(state, storage, &packages, &loaded_html_etag).await? {
                HtmlReconcile::Current => {}
                HtmlReconcile::Rewrote(html_etag) => {
                    write_global_html_stamp(storage, &html_etag, &packages).await?;
                }
                // A peer published a fresher HTML while we looked, so this
                // load's name set is suspect too: reload and re-evaluate rather
                // than let the caller consume its markers against a view we
                // never re-checked. The sibling cached path does the same.
                HtmlReconcile::Lost => continue,
            }
            return Ok(());
        }
        // Conditional, and the stamp comes from the ETag the write itself returned.
        // Writing blind and then re-probing the ETag to stamp it — what this did —
        // meant a peer's page that landed in between was stamped as this node's own
        // render: a durable false proof that the stored HTML shows this name set,
        // which nothing re-examines while the object sits still. The browsable
        // `/simple/` root then stayed wrong indefinitely (per-package pages, and so
        // installs, are never involved).
        match write_global_indexes_cas(
            state,
            storage,
            &packages,
            &loaded_json_etag,
            &loaded_html_etag,
        )
        .await?
        {
            CasOutcome::Won { .. } => return Ok(()),
            CasOutcome::Lost => {
                state
                    .metrics
                    .global_cas_conflicts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                warn!(
                    adds = adds.len(),
                    removes = removes.len(),
                    "destination global index CAS lost to a peer; reloading and retrying"
                );
            }
        }
    }
    bail!("destination global index CAS retries exhausted")
}

/// What a reconcile had to do to `simple/index.html`.
enum HtmlReconcile {
    /// Already renders `packages`, or was never published — nothing to do.
    Current,
    /// Rewritten; carries the ETag the write returned.
    Rewrote(Option<String>),
    /// A peer moved it since `expected_etag` was observed; the caller's view of
    /// the name set is stale too and it must reload.
    Lost,
}

/// Rewrite `simple/index.html` when it disagrees with `packages`. This is the
/// slow path — it reads the body — and runs only when the stamp could not prove
/// currency. Unlike [`put_if_changed`] it never *creates* the view: the HTML is
/// always written before the canonical JSON, so an absent one means the bucket
/// has published no global index at all. Materializing one here would be pure
/// churn — and a view whose bytes move at quiescence is exactly what the
/// simulator flags as a premature marker consumption.
async fn reconcile_global_html(
    state: &AppState,
    storage: &dyn Storage,
    packages: &[String],
    expected_etag: &Option<String>,
) -> Result<HtmlReconcile> {
    let key = format!("{SIMPLE_PREFIX}index.html");
    let current = match storage.get_bytes(&key).await {
        Ok(bytes) => bytes,
        Err(e) if is_not_found(&e) => return Ok(HtmlReconcile::Current),
        Err(e) => return Err(e),
    };
    if current == pep503_global_html(packages).into_bytes() {
        return Ok(HtmlReconcile::Current);
    }
    // Conditional for the same reason the publish path is: a node reconciling
    // from a stale view must lose rather than clobber a fresher render.
    match write_global_html_cas(state, storage, packages, expected_etag).await? {
        HtmlWrite::Wrote(etag) => Ok(HtmlReconcile::Rewrote(etag)),
        HtmlWrite::Lost => Ok(HtmlReconcile::Lost),
    }
}

/// Load the global name set (and its ETag) from the materialized JSON.
async fn load_global_names(storage: &dyn Storage) -> Result<GlobalNames> {
    let key = format!("{SIMPLE_PREFIX}index.json");
    let (body, etag) = if storage.supports_leases() {
        match storage.get_with_etag(&key).await? {
            Some((bytes, etag)) => (Some(bytes), Some(etag)),
            None => (None, None),
        }
    } else {
        // A missing index means "never published"; any other read error must
        // propagate. Swallowing a transient I/O error would let the caller write
        // back a near-empty global index, truncating the package list off a
        // phantom "zero packages" observation.
        match storage.get_bytes(&key).await {
            Ok(bytes) => (Some(bytes), None),
            Err(e) if is_not_found(&e) => (None, None),
            Err(e) => return Err(e),
        }
    };
    let html_etag = current_global_html_etag(storage).await?;
    // An ABSENT canonical JSON is not necessarily an empty name set. Reading it
    // as one is how a crash between the two global writes turns into a wrong
    // answer: the next delta dedups against nothing, `changed` is false, and the
    // empty set becomes the authority that rewrites a live HTML down to nothing.
    //
    // That only matters when an HTML survives to be wrongly shrunk. With no
    // global view published at all — the cold start every fresh bucket begins
    // in — empty is simply the truth, and deriving it would buy nothing while
    // spending a sharded listing (dozens of ops) on the busiest path there is.
    // So spend one HEAD to tell the two apart, and derive only for the genuinely
    // stranded case, from the same per-package views the audit trusts.
    let Some(bytes) = body else {
        let html_key = format!("{SIMPLE_PREFIX}index.html");
        let stranded = storage.head_exists(&html_key).await?;
        let names = if stranded {
            derive_global_names(storage).await?
        } else {
            HashSet::new()
        };
        return Ok(GlobalNames {
            etag,
            names,
            html_etag,
            html_current: false,
            stranded,
        });
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
    Ok(GlobalNames {
        etag,
        names,
        html_etag,
        html_current: false,
        stranded: false,
    })
}

/// The stored ETags of the global index pair — `(json, html)` — from ONE
/// bounded listing.
///
/// Currency of the JSON is not currency of the HTML, and the drift that matters
/// moves only one of them: a peer that publishes the HTML and dies before the
/// canonical JSON. So the no-op gate has to see both.
///
/// Both come from HEADs, concurrently, not from the one bounded LIST over the
/// shared `simple/index.` prefix that would answer for the pair at once. A
/// listing reports no object version, so on GCS its ETag is neither equal to
/// what the cache pinned from its own write nor usable as the precondition of
/// the next one ([`Storage::head_etag`]). Nothing is lost by the trade: a
/// listing is the dearer request class on S3 and GCS both, so two HEADs are
/// cheaper than the one LIST they replace, and issuing them together keeps the
/// probe at a single round-trip.
async fn global_index_etags(storage: &dyn Storage) -> Result<(Option<String>, Option<String>)> {
    let prefix = format!("{SIMPLE_PREFIX}index.");
    let (json, html) = futures::future::join(
        storage.head_etag(&format!("{prefix}json")),
        storage.head_etag(&format!("{prefix}html")),
    )
    .await;
    Ok((json?, html?))
}

/// The conditional-write token of the stored global HTML, or `None` when it has
/// never been written (or the backend has no conditional writes). A
/// metadata-only probe: it never reads the body, so it costs the same against a
/// four-name index and a 780k-name one.
async fn current_global_html_etag(storage: &dyn Storage) -> Result<Option<String>> {
    if !storage.supports_leases() {
        return Ok(None);
    }
    storage
        .head_etag(&format!("{SIMPLE_PREFIX}index.html"))
        .await
}

/// Rebuild the global name set from this bucket's materialized per-package
/// views: a package is globally listed exactly when `simple/<pkg>/index.json`
/// exists, which is the rule the audit applies. Used only when the canonical
/// global JSON is absent and there is therefore nothing authoritative to read.
async fn derive_global_names(storage: &dyn Storage) -> Result<HashSet<String>> {
    const SHARD_CONCURRENCY: usize = 6;
    let mut names = HashSet::new();
    let shards: Vec<String> = crate::storage::SHARD_CHARS
        .iter()
        .map(|c| format!("{SIMPLE_PREFIX}{c}"))
        .collect();
    for chunk in shards.chunks(SHARD_CONCURRENCY) {
        let lists = chunk.iter().map(|shard| storage.list_all(shard));
        for listed in futures::future::join_all(lists).await {
            for obj in listed? {
                if obj.key.ends_with("/index.json") {
                    if let Some(pkg) = key_package(&obj.key, SIMPLE_PREFIX) {
                        names.insert(pkg.to_string());
                    }
                }
            }
        }
    }
    Ok(names)
}

/// Outcome of a global-index conditional write.
enum CasOutcome {
    /// Won; carries the authoritative new ETags the writes themselves returned
    /// (`None` on non-CAS disk backends).
    Won {
        json_etag: Option<String>,
        html_etag: Option<String>,
    },
    /// Lost a conditional write to a concurrent leader; caller should reload.
    Lost,
}

/// Write both global views. BOTH are conditional. The canonical JSON is written
/// last, under CAS, because its success is the serialization point that consumes
/// markers — but the derived HTML is written under a conditional write too, and
/// that is what keeps the pair honest. Writing it blind (as this did until the
/// stale-global-index fix) meant a node that went on to *lose* the JSON CAS had
/// already published HTML for a name set that lost, and since op completion order
/// is not issue order, that loser's HTML could land after the winner's. The
/// winner had no way to notice: it only knew what it had written itself.
/// Conditioning the HTML on the ETag this cache loaded removes the case
/// entirely — a stale writer's HTML write fails, and it reloads instead.
///
/// A crash between the two writes still strands the HTML ahead of the JSON;
/// that is what the durable stamp at [`global_html_stamp_key`] is for, and
/// [`update_global_index_locked`] proves currency from it once per cache load.
///
/// Returns `CasOutcome::Lost` when either conditional write lost its race, else
/// `CasOutcome::Won` with the ETags the puts themselves returned.
async fn write_global_indexes_cas(
    state: &AppState,
    storage: &dyn Storage,
    packages: &[String],
    expected_etag: &Option<String>,
    expected_html_etag: &Option<String>,
) -> Result<CasOutcome> {
    let json_key = format!("{SIMPLE_PREFIX}index.json");
    let json = pep691_global_json(packages).into_bytes();
    if storage.supports_leases() {
        // HTML first so that a crash strands the derived view rather than the
        // authority — but conditionally, so a stale writer loses instead of
        // clobbering. Losing here means a peer moved the HTML under us: reload.
        let html_etag =
            match write_global_html_cas(state, storage, packages, expected_html_etag).await? {
                HtmlWrite::Wrote(etag) => etag,
                HtmlWrite::Lost => return Ok(CasOutcome::Lost),
            };
        // Canonical JSON last, under CAS: its success is what consumes markers.
        let outcome = match expected_etag {
            Some(etag) => storage.put_if_match(&json_key, etag, json).await?,
            None => storage.put_if_none_match(&json_key, json).await?,
        };
        let Some(new_etag) = outcome else {
            return Ok(CasOutcome::Lost);
        };
        state.index_cache.invalidate(&json_key);
        // Both views now agree: record the proof so a later cache load can
        // establish currency without reading the HTML body back.
        write_global_html_stamp(storage, &html_etag, packages).await?;
        // The `/projects/` browser is another render of this same name set, so
        // drop its cached page alongside the global simple index.
        state.invalidate_projects_page();
        return Ok(CasOutcome::Won {
            json_etag: Some(new_etag),
            html_etag,
        });
    }
    write_global_indexes(state, storage, packages).await?;
    state.invalidate_projects_page();
    Ok(CasOutcome::Won {
        json_etag: None,
        html_etag: None,
    })
}

/// Conditionally publish the global HTML for `packages`. `Ok(None)` means a peer
/// moved it since `expected_etag` was observed — the caller must reload rather
/// than overwrite, since its own name set is by definition stale.
async fn write_global_html_cas(
    state: &AppState,
    storage: &dyn Storage,
    packages: &[String],
    expected_etag: &Option<String>,
) -> Result<HtmlWrite> {
    let key = format!("{SIMPLE_PREFIX}index.html");
    let bytes = pep503_global_html(packages).into_bytes();
    if !storage.supports_leases() {
        // Single-writer backend: no peers to lose to, and no ETag space to
        // condition on. A plain write is the whole protocol here.
        storage
            .put_bytes(&key, bytes, Some(SIMPLE_HTML_CONTENT_TYPE))
            .await?;
        state.index_cache.invalidate(&key);
        return Ok(HtmlWrite::Wrote(None));
    }
    let outcome = match expected_etag {
        Some(etag) => storage.put_if_match(&key, etag, bytes).await?,
        None => storage.put_if_none_match(&key, bytes).await?,
    };
    match outcome {
        Some(etag) => {
            state.index_cache.invalidate(&key);
            Ok(HtmlWrite::Wrote(Some(etag)))
        }
        None => Ok(HtmlWrite::Lost),
    }
}

/// Outcome of a conditional global-HTML publish.
enum HtmlWrite {
    /// Published; carries the ETag the write returned (`None` on disk).
    Wrote(Option<String>),
    /// A peer moved the object since `expected_etag` was observed.
    Lost,
}

/// Prove the stored HTML renders exactly `packages`, using only metadata-sized
/// reads: the stamp names both the name set it rendered and the exact HTML
/// object it wrote, and HTML writes are conditional, so an ETag match means the
/// bytes are the ones that stamp describes. "Unproven" is not "wrong" — it only
/// means the caller must fall back to comparing bytes.
async fn global_html_proved_current(
    storage: &dyn Storage,
    html_etag: &Option<String>,
    packages: &[String],
) -> bool {
    let Some(html_etag) = html_etag.as_deref() else {
        return false;
    };
    let Some(stamp) = read_global_html_stamp(storage).await else {
        return false;
    };
    stamp.html_etag == html_etag && stamp.names == global_names_digest(packages)
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
    let mut entries = storage.list_dir_entries(&prefix).await?;
    // A `.superseding` marker is a replication supersede that wrote a body and
    // died before publishing the sidecar naming it, so this bucket is serving
    // bytes its own index contradicts — and nothing else here would ever notice,
    // because every other reader compares sidecars and none re-hashes a body.
    // Finish it before deriving anything, or this rebuild renders the stale
    // digest and calls the package converged.
    //
    // Detected from the listing already in hand, so a package with no torn
    // record pays one `strip_suffix` per name; the repair's O(bytes) re-hash and
    // its single re-listing are reached only by the rare package that has one.
    // Gated on `backfill_missing` for the same reason the sidecar backfill is:
    // that flag is what separates the truth-authoring rebuild from the
    // request-path render, which must never mutate.
    if backfill_missing {
        let mut torn: Vec<String> = entries
            .iter()
            .filter_map(|entry| {
                entry
                    .key
                    .strip_prefix(&prefix)?
                    .strip_suffix(SUPERSEDING_SUFFIX)
            })
            .filter(|filename| is_artifact(filename))
            .map(str::to_string)
            .collect();
        if !torn.is_empty() {
            torn.sort();
            crate::replicate::finish_interrupted_supersedes(storage, pkg, &torn).await?;
            entries = storage.list_dir_entries(&prefix).await?;
        }
    }
    let names: HashSet<&str> = entries
        .iter()
        .filter_map(|e| e.key.strip_prefix(&prefix))
        .collect();
    // Tombstoned filenames leave every index: a delete
    // that crashed after the tombstone but before the artifact removal converges
    // to "gone" here, rather than resurrecting the file.
    let tombstoned: HashSet<&str> = names
        .iter()
        .filter_map(|f| f.strip_suffix(TOMBSTONE_SUFFIX))
        .collect();
    // Frozen filenames are likewise suppressed: a
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
        // An availability error on any file fails the whole listing: a rebuild
        // must not derive a view from a partial read. Dropping a file it could
        // not confirm would look like a deletion and delete the view, consuming
        // the markers that were the only retry signal. Only a deliberate
        // `Ok(None)` omission is skipped.
        for file in loaded {
            metadata.extend(file?);
        }
    }
    Ok((metadata, raw))
}

/// Load one artifact's index entry from its sidecar (backfilling if absent).
/// `Ok(None)` means "leave it out of the index" — a deliberate omission whose
/// reason is logged inside. `Err` is an availability failure reading storage: it
/// must fail the whole rebuild so the package's `_dirty/` markers are retained
/// and the next tick retries, rather than deriving a wrong (often empty) view
/// from a transient error and letting the caller consume the markers.
async fn load_file_metadata(
    storage: &dyn Storage,
    entry: &FileEntry,
    filename: &str,
    names: &HashSet<&str>,
    pkg_origin: Option<&str>,
    backfill_missing: bool,
) -> Result<Option<FileMetadata>> {
    let has_sidecar = names.contains(format!("{filename}{SIDECAR_SUFFIX}").as_str());
    let mirror_quarantined =
        names.contains(format!("{filename}{}", crate::sidecar::MIRROR_QUARANTINED_SUFFIX).as_str());
    if mirror_quarantined && !has_sidecar {
        warn!(key=%entry.key, "quarantined mirror artifact has no sidecar; omitting from index");
        return Ok(None);
    }
    let sc = if has_sidecar {
        match read_listed_sidecar(storage, &entry.key).await? {
            Some(sc) => sc,
            None => return Ok(None),
        }
    } else if backfill_missing {
        match backfill_sidecar(storage, entry, filename, pkg_origin).await? {
            Some(sc) => sc,
            None => return Ok(None),
        }
    } else {
        return Ok(None);
    };
    if pkg_origin == Some(crate::origin::PRIVATE)
        && sc.origin.as_deref() == Some(crate::origin::MIRROR)
    {
        warn!(key=%entry.key, "mirror artifact under private package claim; omitting from index");
        return Ok(None);
    }
    if mirror_quarantined && sc.origin.as_deref() != Some(crate::origin::PRIVATE) {
        warn!(key=%entry.key, "quarantined mirror artifact remains non-private; omitting from index");
        return Ok(None);
    }
    let core_metadata = names.contains(format!("{filename}{METADATA_SUFFIX}").as_str());
    let provenance = names.contains(format!("{filename}{PROVENANCE_SUFFIX}").as_str());
    Ok(Some(FileMetadata::from_sidecar(
        filename,
        sc,
        core_metadata,
        provenance,
    )))
}

/// Read a sidecar a listing said exists, distinguishing the outcomes a rebuild
/// must treat differently. `Ok(Some)` is the record. `Ok(None)` is a deliberate
/// omission: parse failure = corruption (never fabricate metadata over it — that
/// could silently reset a security yank to false), or a not-found = the sidecar
/// vanished between listing and read (a concurrent delete, whose own `_dirty/`
/// marker reconverges a later rebuild). `Err` = an availability failure: fail
/// the rebuild so its markers retry, never laundering a transient read error
/// into an authoritative "omit".
async fn read_listed_sidecar(storage: &dyn Storage, artifact_key: &str) -> Result<Option<Sidecar>> {
    let key = sidecar_key(artifact_key);
    let bytes = match storage.get_bytes(&key).await {
        Ok(bytes) => bytes,
        Err(e) if is_not_found(&e) => {
            warn!(key=%artifact_key, "sidecar vanished before read; omitting file from index");
            return Ok(None);
        }
        Err(e) => return Err(e).with_context(|| format!("reading sidecar {key}")),
    };
    match serde_json::from_slice(&bytes) {
        Ok(sc) => Ok(Some(sc)),
        Err(e) => {
            error!(error=?e, key=%artifact_key, "corrupt sidecar; omitting file from index (will not fabricate metadata)");
            Ok(None)
        }
    }
}

/// Hash-once-and-backfill for files that predate write-time sidecars.
/// Storage last-modified is the upload-time fallback (correct by construction
/// for direct uploads — filenames are immutable, so written exactly once).
///
/// Create-only, never overwrite: "missing" was observed in a listing that may
/// already be stale, and a concurrent upload's real sidecar (true timestamp,
/// yank state) must always beat this fabricated one. Losing the race means
/// the real sidecar exists — read and use it.
///
/// `Ok(Some)` is the record to index. `Ok(None)` is a deliberate omission: the
/// artifact vanished before we could hash it, or it changed/vanished under the
/// fabrication (which we retract) — both owned by the concurrent mutator's own
/// `_dirty/` marker, so a later rebuild converges. `Err` is an availability
/// failure anywhere in the sequence: propagate it so the rebuild fails and its
/// markers retry, never a fabricated observation of "changed".
async fn backfill_sidecar(
    storage: &dyn Storage,
    entry: &FileEntry,
    filename: &str,
    pkg_origin: Option<&str>,
) -> Result<Option<Sidecar>> {
    let bytes = match storage.get_bytes(&entry.key).await {
        Ok(bytes) => bytes,
        // Vanished between the listing and this read (a concurrent delete or a
        // crashed publish clearing unacked debris). Its deleter holds the marker;
        // omit the file and let a later rebuild converge.
        Err(e) if is_not_found(&e) => {
            warn!(key=%entry.key, "artifact vanished before backfill; omitting file from index");
            return Ok(None);
        }
        Err(e) => {
            return Err(e).with_context(|| format!("reading {} to backfill its sidecar", entry.key))
        }
    };
    let sc = Sidecar {
        sha256: sha256_hex(&bytes),
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
        // A backfilled sidecar cannot prove it was a `sync --to` snapshot, so its
        // provenance stays "cache" until a real sync re-stamps snapshot=true.
        // (Both replicate; the bit is provenance only.)
        snapshot: false,
        // Backfill re-reads the body to recover its sha256; a provider checksum
        // would need a native metadata read, so it stays size-only for now.
        store_checksum: None,
    };
    let fabricated = serde_json::to_vec(&sc)?;
    let created = storage
        .put_if_absent(
            &sidecar_key(&entry.key),
            fabricated.clone(),
            Some("application/json"),
        )
        .await?;
    if !created {
        // Lost the create race: a real sidecar exists. Read and use it — an
        // unparseable/absent winner is the racing writer's to own (`Ok(None)`),
        // an availability error propagates.
        return read_listed_sidecar(storage, &entry.key).await;
    }
    // The hash read above and the create are not atomic: a failed publish
    // deleting its own unacked debris can free the immutable name in between,
    // and a later upload would then inherit a sidecar describing dead bytes.
    // Confirm the body still holds the hashed bytes; otherwise retract the
    // fabrication — comparing first, so a real sidecar that already replaced
    // ours is never the casualty.
    let confirmed = match storage.get_bytes(&entry.key).await {
        Ok(now) => sha256_hex(&now) == sc.sha256,
        // Genuinely vanished after we hashed it — the exact hazard this confirm
        // guards; retract.
        Err(e) if is_not_found(&e) => false,
        // State unknown: never fabricate a "changed" observation from a transient
        // error. Propagate and let the whole rebuild retry.
        Err(e) => {
            return Err(e)
                .with_context(|| format!("confirming {} after backfilling its sidecar", entry.key))
        }
    };
    if !confirmed {
        let retract = match storage.get_bytes(&sidecar_key(&entry.key)).await {
            Ok(current) => current == fabricated,
            // Already gone — nothing of ours to retract.
            Err(e) if is_not_found(&e) => false,
            // Unknown state; propagate rather than guess, and retry the rebuild.
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "reading back fabricated sidecar for {} before retraction",
                        entry.key
                    )
                })
            }
        };
        if retract {
            storage
                .delete_keys(&[sidecar_key(&entry.key)])
                .await
                .with_context(|| format!("retracting fabricated sidecar for {}", entry.key))?;
        }
        // The artifact changed or vanished under us; any fabrication of ours is
        // retracted. Omit the file — the mutator holds its own marker.
        warn!(key=%entry.key, "artifact changed or vanished while backfilling its sidecar; retracted, omitting file");
        return Ok(None);
    }
    info!(key=%entry.key, "backfilled sidecar");
    Ok(Some(sc))
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

/// The renderable files with MAL-blocked ones removed, or `None` when no scrub
/// applies — blocking disarmed/unfed, no file blocked, or a package that isn't
/// mirror-origin (a private package sharing a MAL name keeps listing, and the
/// byte gate is the guarantee either way). The origin read is paid only on a
/// blocked-file hit; the common package is a pure hash probe. Fail-open on the
/// origin read: a listing we couldn't prove mirror is left intact, and the gate
/// still 403s the download.
async fn malware_scrubbed_files(
    state: &AppState,
    storage: &dyn Storage,
    pkg: &str,
    files: &[FileMetadata],
) -> Option<Vec<FileMetadata>> {
    if !state.malware_block {
        return None;
    }
    let snap = state.advisory_snapshot();
    let db = snap.db.as_ref()?;
    let is_blocked = |file: &FileMetadata| {
        let version = infer_version_from_filename(&file.filename);
        !crate::advisories::blocking_advisories(db, pkg, version.as_deref()).is_empty()
    };
    if !files.iter().any(&is_blocked) {
        return None; // common path: no hit, no origin read
    }
    match crate::origin::read_origin_claim(storage, pkg).await {
        Ok(Some(crate::origin::OriginState::Mirror)) => Some(
            files
                .iter()
                .filter(|file| !is_blocked(file))
                .cloned()
                .collect(),
        ),
        _ => None,
    }
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
    // Malware scrub: drop individual MAL-blocked files from the rendered view
    // (the byte gate is the guarantee; a scrubbed listing is hygiene). Only
    // mirror-origin packages are filtered, so a private package sharing a MAL
    // name keeps listing (origin exclusivity, the gate's exemption). No-op unless
    // blocking is armed and a snapshot is loaded.
    let scrubbed = malware_scrubbed_files(state, storage, pkg, files).await;
    let files: &[FileMetadata] = scrubbed.as_deref().unwrap_or(files);
    // Quarantine omits file links; the delete-vs-render decision upstream still
    // keys on the real artifact count, so a quarantined project keeps a
    // status-bearing (link-free) page instead of 404ing.
    let render_files: &[FileMetadata] = if status.status.blocks_downloads() {
        &[]
    } else {
        files
    };
    let html = pep503_project_html(pkg, render_files, &status);
    let json = pep691_project_json(pkg, render_files, &status);

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
    use crate::app::{AccessLogFormat, ArtifactDelivery};
    use crate::buckets::{BucketHandle, BucketSet, Pinned};
    use crate::storage::test_support::InMemStorage;
    use axum::body::Body;
    use http::Response;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool as StdAtomicBool;
    use std::sync::Mutex;
    use std::time::Duration;

    fn raw(items: &[(&str, u64)]) -> Vec<(String, u64)> {
        items.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    /// A minimal private/mirror sidecar for a seeded artifact — only the fields
    /// worker tests vary (`size` to match the bytes, `origin`) are parameters;
    /// the rest are fixed, mirroring replicate's `sc` fixture.
    fn test_sidecar(size: u64, origin: Option<&str>) -> Sidecar {
        Sidecar {
            sha256: "ab".repeat(32),
            size,
            version: "1.0".to_string(),
            upload_time: "2026-01-01T00:00:00Z".to_string(),
            requires_python: None,
            yanked: Yanked::Flag(false),
            origin: origin.map(str::to_string),
            upload_epoch_ms: None,
            yank_epoch: 0,
            snapshot: false,
            store_checksum: None,
        }
    }

    /// The single-bucket headless `AppState` the worker end-to-end tests run
    /// against. The two call sites differed only in `worker_interval`, so that is
    /// the one override.
    fn test_app_state(storage: Arc<dyn Storage>, worker_interval: Duration) -> Arc<AppState> {
        Arc::new(AppState {
            buckets: Arc::new(crate::buckets::BucketSet::single(storage)),
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
            trusted_proxy: false,
            login_throttle: Default::default(),
            worker_interval,
            reconcile_interval: Duration::from_secs(3600),
            repl_sweep_interval: Duration::from_secs(300),
            repl_sweep_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_request_unix: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            fanout_grace: Duration::from_secs(30),
            intent_grace: time::Duration::seconds(900),
            audit_on_boot: true,
            transparency: true,
            wait_on_upload: false,
            wait_on_upload_timeout: Duration::from_secs(1),
            allow_legacy_versions: false,
            lease_ttl: Duration::from_secs(30),
            index_cache: Arc::new(crate::cache::IndexCache::new(crate::cache::INDEX_CACHE_TTL)),
            project_cache: Arc::new(crate::project_cache::ProjectCache::new(
                crate::cache::INDEX_CACHE_TTL,
            )),
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
            summary_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            package_stats_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            projects_page_cache: Arc::new(std::sync::Mutex::new(None)),
            proxy: None,
            denylist: None,
            proxy_stream_threshold: None,
            started: std::time::Instant::now(),
            shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            advisory_feed: None,
            malware_block: false,
            malware_probe: Duration::ZERO,
            advisories: Arc::new(std::sync::RwLock::new(Arc::new(
                crate::advisories::AdvisoryState::default(),
            ))),
            advisory_reload_asap: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
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

    /// The floor is one worst-case cycle — probe every bucket, LIST every peer,
    /// verify one topology — so it grows with the topology, and the boundary is
    /// inclusive: a window exactly one cycle long can still mature inside one.
    #[test]
    fn a_read_return_window_clears_the_floor_only_above_a_worst_case_cycle() {
        // Two buckets: 2*2+1 = 5 s.
        assert_eq!(
            read_return_window_under_floor(2, Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
            "a window equal to the cycle span is still short enough to mature inside one"
        );
        assert_eq!(
            read_return_window_under_floor(2, Duration::from_secs(6)),
            None
        );
        // Three buckets: 2*3+1 = 7 s, so a window that cleared two buckets does
        // not clear three.
        assert_eq!(
            read_return_window_under_floor(3, Duration::from_secs(6)),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            read_return_window_under_floor(3, Duration::from_secs(8)),
            None
        );
        // The shipped default clears every plausible topology.
        assert_eq!(
            read_return_window_under_floor(9, Duration::from_secs(300)),
            None
        );
        // One bucket has no read affinity and no return gate at all.
        assert_eq!(read_return_window_under_floor(1, Duration::ZERO), None);
    }

    fn seed_private_artifact(storage: &InMemStorage, pkg: &str, filename: &str) {
        let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        storage.insert(&key, b"artifact".to_vec());
        storage.insert(
            &sidecar_key(&key),
            serde_json::to_vec(&test_sidecar(8, Some(crate::origin::PRIVATE))).unwrap(),
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
            serde_json::to_vec(&test_sidecar(19, Some(crate::origin::PRIVATE))).unwrap(),
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

        let audit = audit_shard(&state, storage.as_ref(), 0, 'f', false, false, false, None)
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
            serde_json::to_vec(&test_sidecar(16, None)).unwrap(),
        );
        objects.insert(
            format!("{SIMPLE_PREFIX}index.json"),
            br#"{"projects":[{"name":"fastpkg"}]}"#.to_vec(),
        );
        let storage = Arc::new(SweepStallsStorage {
            objects: Mutex::new(objects),
            sweep_entered: StdAtomicBool::new(false),
        });
        let state = test_app_state(storage.clone(), Duration::from_secs(10));

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
            serde_json::to_vec(&test_sidecar(16, None)).unwrap(),
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
        let state = test_app_state(storage.clone(), Duration::from_millis(10));

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

    async fn global_html(storage: &InMemStorage) -> String {
        let bytes = storage
            .get_bytes(&format!("{SIMPLE_PREFIX}index.html"))
            .await
            .unwrap();
        String::from_utf8(bytes).unwrap()
    }

    /// Crash residue, HTML *ahead*: the global views are written HTML-first,
    /// canonical-JSON-last, so a crash in between leaves an HTML listing a name
    /// the JSON never got. The absent JSON then reads back as the empty set, a
    /// removals-only delta dedups against nothing, and the no-change gate used
    /// to return leaving the stranded HTML as the final word (vopr seed 28024).
    #[tokio::test]
    async fn a_no_change_delta_reconciles_a_global_html_stranded_by_a_crash() {
        let storage = Arc::new(InMemStorage::default());
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.html"),
            pep503_global_html(&["ghost".to_string()]).into_bytes(),
        );
        let state = AppState::headless(storage.clone());

        update_global_index(&state, storage.as_ref(), &[], &["ghost".to_string()])
            .await
            .unwrap();

        assert_eq!(
            global_html(&storage).await,
            pep503_global_html(&[]),
            "a dedupping delta must still reconcile an HTML that a crashed write stranded"
        );
    }

    /// The sibling of the stranded-HTML bug, and the reason the HTML write is
    /// conditional (vopr seeds 96078376 / 230058708, crash-only, one bucket).
    /// Two nodes update the global index at once; the one that loses the JSON CAS
    /// has *already* published HTML for the set that lost. Completion order is
    /// not issue order, so that loser's HTML can land after the winner's — and
    /// the winner can never notice, because its only evidence is what it wrote
    /// itself. Conditioning the HTML write on the ETag the writer loaded removes
    /// the case: a stale writer loses instead of clobbering.
    #[tokio::test]
    async fn a_stale_writer_cannot_clobber_the_global_html() {
        let storage = Arc::new(InMemStorage::default());
        let state = AppState::headless(storage.clone());

        update_global_index(&state, storage.as_ref(), &["alpha".to_string()], &[])
            .await
            .unwrap();
        // What a node that loaded here — and then stalled — would hold.
        let stale = current_global_html_etag(storage.as_ref()).await.unwrap();
        assert!(
            stale.is_some(),
            "the publish must leave an ETag to go stale"
        );

        // A peer moves the global index on.
        update_global_index(&state, storage.as_ref(), &["beta".to_string()], &[])
            .await
            .unwrap();
        let fresher = global_html(&storage).await;

        // The stalled node finally issues its write. It must lose.
        let outcome =
            write_global_html_cas(&state, storage.as_ref(), &["alpha".to_string()], &stale)
                .await
                .unwrap();
        assert!(
            matches!(outcome, HtmlWrite::Lost),
            "a writer holding a stale ETag must lose the conditional write"
        );
        assert_eq!(
            global_html(&storage).await,
            fresher,
            "the fresher render must survive a stale writer's late write"
        );
    }

    /// A crash between the two global writes leaves an HTML with no canonical
    /// JSON. Reading that absent JSON as the *empty set* is how a reconcile came
    /// to wipe a live listing to nothing — the empty set became the authority.
    /// With no authority to read, the name set is derived from the per-package
    /// views instead (the source the audit trusts), and the stranded cache must
    /// write once so the missing JSON is materialized rather than left to 404
    /// while the HTML happily serves.
    #[tokio::test]
    async fn a_stranded_global_html_reconciles_from_the_views_instead_of_wiping() {
        let storage = Arc::new(InMemStorage::default());
        // `alpha` is genuinely live: it has a materialized per-package view.
        storage.insert(&format!("{SIMPLE_PREFIX}alpha/index.json"), b"{}".to_vec());
        // The crash residue: an HTML listing a live name and a dead one, and no
        // canonical JSON at all.
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.html"),
            pep503_global_html(&["alpha".to_string(), "ghost".to_string()]).into_bytes(),
        );
        let state = AppState::headless(storage.clone());

        update_global_index(&state, storage.as_ref(), &[], &["ghost".to_string()])
            .await
            .unwrap();

        assert_eq!(
            global_html(&storage).await,
            pep503_global_html(&["alpha".to_string()]),
            "the dead name goes and the live one stays — an absent JSON is not an empty index"
        );
        assert!(
            storage
                .head_exists(&format!("{SIMPLE_PREFIX}index.json"))
                .await
                .unwrap(),
            "the absent canonical JSON must be materialized, not left 404ing while the HTML serves"
        );
    }

    /// Crash residue, HTML *behind*: a node that loses the CAS has already
    /// written an optimistic HTML for the set that lost; crashing before its
    /// reconcile leaves the HTML older than the JSON. A later delta that dedups
    /// against that (correct) JSON must still republish the HTML — and having
    /// done so once, must not re-read it on every subsequent no-op tick.
    #[tokio::test]
    async fn a_no_change_delta_republishes_a_global_html_left_behind_the_json() {
        let storage = Arc::new(InMemStorage::default());
        let live = vec!["alpha".to_string()];
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.json"),
            pep691_global_json(&live).into_bytes(),
        );
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.html"),
            pep503_global_html(&[]).into_bytes(),
        );
        let state = AppState::headless(storage.clone());

        update_global_index(&state, storage.as_ref(), &live, &[])
            .await
            .unwrap();
        assert_eq!(
            global_html(&storage).await,
            pep503_global_html(&live),
            "an HTML left behind the canonical JSON must be republished"
        );

        let reads = storage.get_count();
        update_global_index(&state, storage.as_ref(), &live, &[])
            .await
            .unwrap();
        assert_eq!(
            storage.get_count(),
            reads,
            "the reconcile is once per cache load; a steady-state no-op delta must not read the global HTML"
        );
    }

    /// Crash residue on a *peer*, seen from the node that last won the CAS
    /// (vopr seeds 60000037578 / 61000075134, `--rotate`, crash-only, ONE
    /// bucket, no faults). `html_current` is a claim about a fleet-shared
    /// object, so it can only be trusted while that object has not moved. A
    /// peer that publishes the HTML and dies before the canonical JSON moves
    /// the HTML's ETag and *not* the JSON's — the one drift the JSON currency
    /// probe cannot see. Pinning the memo on a CAS win made that permanent on
    /// this node: every later delta that dedups returned without looking, and
    /// `simple/index.html` served a package `simple/index.json` did not, until
    /// the tier-3 audit stumbled on it.
    #[tokio::test]
    async fn a_peers_stranded_global_html_is_seen_by_the_node_that_won_the_last_cas() {
        let storage = Arc::new(InMemStorage::default());
        let state = AppState::headless(storage.clone());
        let live = vec!["alpha".to_string()];

        // This node publishes and wins: cache pinned, HTML proved current.
        update_global_index(&state, storage.as_ref(), &live, &[])
            .await
            .unwrap();

        // A peer publishes an HTML for a wider set, then dies before the
        // canonical JSON. Only the HTML's ETag moves.
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.html"),
            pep503_global_html(&["alpha".to_string(), "beta".to_string()]).into_bytes(),
        );

        // A delta that dedups entirely against this node's cache.
        update_global_index(&state, storage.as_ref(), &live, &[])
            .await
            .unwrap();

        assert_eq!(
            global_html(&storage).await,
            pep503_global_html(&live),
            "a CAS winner must re-check the pair once the HTML object it proved has moved"
        );

        // ...and having re-proved it, the steady state must stay free: no
        // further body read, and a fixed metadata cost per no-op delta.
        let reads = storage.get_count();
        let lists = storage.list_count();
        let heads = storage.head_count();
        update_global_index(&state, storage.as_ref(), &live, &[])
            .await
            .unwrap();
        assert_eq!(
            storage.get_count(),
            reads,
            "a steady-state no-op delta must not read the global HTML back"
        );
        assert_eq!(
            storage.head_count(),
            heads + 2,
            "the currency probe must stay ONE metadata read per global view per no-op delta"
        );
        assert_eq!(
            storage.list_count(),
            lists,
            "a listed ETag carries no version, so the probe must never reach for one"
        );
    }

    /// The other half of the peer's tear, and the one the JSON currency probe
    /// is blind to (vopr seed 13792606396100784374, `--rotate`). A cache pinned
    /// at cold start holds `etag: None`, and an ABSENT canonical JSON also
    /// probes as `None` — so a peer that published the HTML and died before the
    /// JSON read as "JSON unchanged" on every later tick. No reload meant no
    /// `load_global_names`, which is the only thing that sets `stranded` and
    /// materializes the JSON; worse, the HTML reconcile below then rewrote the
    /// live HTML down to this cache's stale (empty) set and declared the pair
    /// current, retiring the only signal that covered the tear. A published
    /// HTML standing over an absent JSON is that stranded pair, not a cold
    /// start.
    #[tokio::test]
    async fn an_absent_json_under_a_published_html_is_not_read_as_a_cold_start() {
        let storage = Arc::new(InMemStorage::default());
        let state = AppState::headless(storage.clone());

        // Pin this node's cache in the cold start it really is: nothing
        // published, names empty, both ETags `None`.
        update_global_index(&state, storage.as_ref(), &[], &["ghost".to_string()])
            .await
            .unwrap();

        // A peer publishes `alpha` — per-package view, then the global HTML —
        // and dies before the canonical JSON. Only the HTML's ETag moves, off
        // `None`; the JSON's stays absent.
        let live = vec!["alpha".to_string()];
        storage.insert(&format!("{SIMPLE_PREFIX}alpha/index.json"), b"{}".to_vec());
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.html"),
            pep503_global_html(&live).into_bytes(),
        );

        // A delta that dedups entirely against this node's cached (empty) set.
        update_global_index(&state, storage.as_ref(), &[], &["ghost".to_string()])
            .await
            .unwrap();

        assert!(
            storage
                .head_exists(&format!("{SIMPLE_PREFIX}index.json"))
                .await
                .unwrap(),
            "a dedupping delta must materialize the canonical JSON a peer's tear left absent"
        );
        assert_eq!(
            global_html(&storage).await,
            pep503_global_html(&live),
            "the reconcile must reload the peer's set, not wipe the live HTML to this cache's stale empty one"
        );
    }

    /// One chain link's exact bytes, tagged so two runs under different tags
    /// differ byte for byte at every seq — two branches of one history.
    fn chain_link_bytes(seq: u64, prev: &str, tag: &str) -> Vec<u8> {
        let mut packages = crate::transparency::Delta::new();
        packages.insert(
            tag.to_string(),
            [("a-1.0-py3-none-any.whl".to_string(), tag.to_string())]
                .into_iter()
                .collect(),
        );
        crate::transparency::link_bytes(&crate::transparency::ChainLink {
            seq,
            prev_sha256: prev.to_string(),
            created: "2026-08-24T00:00:00Z".to_string(),
            packages,
        })
        .expect("serializing a test chain link")
    }

    /// A two-bucket fleet whose write pin is the *second* bucket, so the append
    /// arbiter — first in config order — is a different bucket than the pin.
    /// That is the only shape in which the arbiter pre-check runs at all.
    fn fleet_pinned_second(
        arbiter: Arc<InMemStorage>,
        pin: Arc<InMemStorage>,
    ) -> (Arc<AppState>, Arc<Pinned>) {
        let mut state = AppState::headless(arbiter.clone());
        state.buckets = Arc::new(BucketSet::new(vec![
            BucketHandle {
                storage: arbiter,
                name: "arbiter".to_string(),
            },
            BucketHandle {
                storage: pin,
                name: "pin".to_string(),
            },
        ]));
        state.buckets.switch(1);
        let pinned = state.pin();
        (Arc::new(state), pinned)
    }

    fn one_package_delta() -> crate::transparency::Delta {
        let mut delta = crate::transparency::Delta::new();
        delta.insert(
            "six".to_string(),
            [("six-1.0-py3-none-any.whl".to_string(), "aa".to_string())]
                .into_iter()
                .collect(),
        );
        delta
    }

    /// The arbiter, not the pin, is what the CAS lands against — and it can move
    /// between the catch-up that vouched for it and the write. An arbiter that
    /// has reached the seq this pass computed off the pin has been written by
    /// somebody else since: carry, and let the next audit reconcile onto its
    /// link. (Racing it is not catastrophic here — the CAS would lose and adopt
    /// — but the same read is what catches the case below, which is.)
    #[tokio::test]
    async fn an_arbiter_already_at_our_seq_carries_the_checkpoint() {
        let arbiter = Arc::new(InMemStorage::default());
        let pin = Arc::new(InMemStorage::default());
        let genesis = chain_link_bytes(0, "", "shared");
        let second = chain_link_bytes(1, &sha256_hex(&genesis), "shared");
        // The pin is at head 0, so this pass computes seq 1 — which the arbiter
        // already holds.
        pin.insert(&crate::transparency::chain_key(0), genesis.clone());
        arbiter.insert(&crate::transparency::chain_key(0), genesis);
        arbiter.insert(&crate::transparency::chain_key(1), second.clone());
        let (state, pinned) = fleet_pinned_second(arbiter.clone(), pin.clone());
        let fleet = vec![
            crate::layout::ReplicaTarget {
                storage: arbiter.as_ref(),
                name: "arbiter",
            },
            crate::layout::ReplicaTarget {
                storage: pin.as_ref(),
                name: "pin",
            },
        ];

        let carried = append_chain_link(
            &state,
            &fleet,
            1,
            crate::transparency::FleetChain {
                in_sync: vec![0, 1],
            },
            pinned.generation,
            one_package_delta(),
        )
        .await;

        assert_eq!(
            carried,
            Some(one_package_delta()),
            "an arbiter that already spent our seq must send the delta to the next audit"
        );
        assert_eq!(
            arbiter
                .get_bytes(&crate::transparency::chain_key(1))
                .await
                .unwrap(),
            second,
            "the link already at that seq must stand"
        );
    }

    /// The half the CAS cannot catch. The arbiter sits at seq-1 holding
    /// *different* bytes — a branch — so create-if-absent at our seq genuinely
    /// succeeds: the seq really is free there. The link then chains onto a
    /// prefix that bucket does not have, which is a fork written by the append
    /// itself. Only comparing what the arbiter's head actually is sees it.
    #[tokio::test]
    async fn an_arbiter_holding_other_bytes_at_the_previous_seq_carries_the_checkpoint() {
        let arbiter = Arc::new(InMemStorage::default());
        let pin = Arc::new(InMemStorage::default());
        pin.insert(
            &crate::transparency::chain_key(0),
            chain_link_bytes(0, "", "left"),
        );
        let rival = chain_link_bytes(0, "", "right");
        arbiter.insert(&crate::transparency::chain_key(0), rival.clone());
        let (state, pinned) = fleet_pinned_second(arbiter.clone(), pin.clone());
        let fleet = vec![
            crate::layout::ReplicaTarget {
                storage: arbiter.as_ref(),
                name: "arbiter",
            },
            crate::layout::ReplicaTarget {
                storage: pin.as_ref(),
                name: "pin",
            },
        ];

        let carried = append_chain_link(
            &state,
            &fleet,
            1,
            crate::transparency::FleetChain {
                in_sync: vec![0, 1],
            },
            pinned.generation,
            one_package_delta(),
        )
        .await;

        assert_eq!(
            carried,
            Some(one_package_delta()),
            "a link must never be written onto a head it does not chain onto"
        );
        assert!(
            !arbiter
                .head_exists(&crate::transparency::chain_key(1))
                .await
                .unwrap(),
            "writing here is the fork: a free seq over somebody else's prefix"
        );
        assert_eq!(
            arbiter
                .get_bytes(&crate::transparency::chain_key(0))
                .await
                .unwrap(),
            rival,
            "the arbiter's own branch must be left exactly as it stands"
        );
    }

    /// Guards the carry mechanism itself — the pre-existing fail-closed exit
    /// (the pin's own chain listing failing) that every new defer arm reuses, so
    /// this kills none of those arms directly. What it does pin is what they all
    /// depend on: a pass that appends nothing must not lose the delta it was
    /// holding. Dropping it leaves the chain committing an old sha over bytes
    /// storage has already replaced, which `verify-chain` then reports as a
    /// tamper that clears only if that package happens to churn again.
    #[tokio::test]
    async fn a_deferred_checkpoint_carries_its_delta_into_the_next_pass() {
        let storage = Arc::new(InMemStorage::default());
        let state = AppState::headless(storage.clone());
        let pinned = state.pin();
        let mut delta = crate::transparency::Delta::new();
        delta.insert(
            "six".to_string(),
            [("six-1.0-py3-none-any.whl".to_string(), "aa".to_string())]
                .into_iter()
                .collect(),
        );

        // Pass one: the chain cannot be listed, so no head can be vouched for.
        storage.fail_lists_of(crate::transparency::CHAIN_PREFIX);
        write_chain_link(&state, &pinned, pinned.generation, delta.clone()).await;
        storage.heal_lists();
        assert!(
            storage
                .list_all(crate::transparency::CHAIN_PREFIX)
                .await
                .unwrap()
                .is_empty(),
            "a pass that could not read the chain must not have written to it"
        );

        // Pass two has nothing of its own to commit: the carried delta is the
        // churn, and riding along is the whole point of holding it.
        write_chain_link(
            &state,
            &pinned,
            pinned.generation,
            crate::transparency::Delta::new(),
        )
        .await;

        let bytes = storage
            .get_bytes(&crate::transparency::chain_key(0))
            .await
            .expect("the next pass must land the checkpoint it carried");
        let link: crate::transparency::ChainLink =
            serde_json::from_slice(&bytes).expect("a chain link this process just wrote");
        assert_eq!(
            link.packages, delta,
            "the carried delta must be exactly what the next pass committed"
        );
    }

    /// A destination's global index is published conditionally, like the
    /// selected bucket's — and so without reading either global body back. That
    /// read-back is what the old blind write needed: it re-probed the HTML's
    /// ETag afterwards to stamp it, and that ETag is exactly what a peer's
    /// concurrent publish moves. The stamp then certified the peer's page as
    /// this node's render — a durable false proof of currency that left the
    /// browsable root wrong for as long as the object sat still. Every node that
    /// can reach a destination drains it, so the window is real.
    #[tokio::test]
    async fn a_destination_publish_is_conditional_and_reads_no_global_body() {
        let storage = Arc::new(InMemStorage::default());
        let state = AppState::headless(storage.clone());
        let listed = vec!["alpha".to_string()];
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.json"),
            pep691_global_json(&listed).into_bytes(),
        );
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.html"),
            pep503_global_html(&listed).into_bytes(),
        );

        let bodies_before = storage.get_count();
        update_global_index_uncached(&state, storage.as_ref(), &["beta".to_string()], &[])
            .await
            .unwrap();
        assert_eq!(
            storage.get_count(),
            bodies_before,
            "a conditional publish knows its own ETag; reading the global body back is the \
             misattribution window"
        );

        let published = vec!["alpha".to_string(), "beta".to_string()];
        assert_eq!(global_html(&storage).await, pep503_global_html(&published));
        let stamp = read_global_html_stamp(storage.as_ref())
            .await
            .expect("a publish records its own proof of currency");
        let stored = storage
            .head_etag(&format!("{SIMPLE_PREFIX}index.html"))
            .await
            .unwrap()
            .expect("the HTML this call published");
        assert_eq!(
            stamp.html_etag, stored,
            "the stamp must name the page this call itself wrote"
        );
        assert_eq!(stamp.names, global_names_digest(&published));
    }

    /// The reconcile must not *create* views: a bucket that has never published
    /// a global index is empty by construction, and materializing one on a no-op
    /// delta is churn the audit oracle reads as a premature marker consumption.
    #[tokio::test]
    async fn a_no_change_delta_does_not_publish_a_global_index_that_never_existed() {
        let storage = Arc::new(InMemStorage::default());
        let state = AppState::headless(storage.clone());

        update_global_index(&state, storage.as_ref(), &[], &["ghost".to_string()])
            .await
            .unwrap();

        assert!(
            !storage
                .head_exists(&format!("{SIMPLE_PREFIX}index.html"))
                .await
                .unwrap(),
            "a no-op delta must not materialize a global index into a bucket that has none"
        );
    }

    /// A destination drain's membership delta must be an *assertion*, never a
    /// diff against the package's own index view — the view this same function
    /// writes and deletes. Pass one here rebuilds `alpha` to nothing (its views
    /// go) and then fails on the global index, so the markers are retained for
    /// the retry the protocol promises. Pass two used to HEAD the view its own
    /// predecessor had just deleted, read "already absent" as "already not a
    /// member", compute no delta — and consume the markers anyway, leaving a
    /// dead package listed in that bucket's global index with no signal left to
    /// heal it. That is the partitioned lane's dominant finding
    /// (AUDIT_PREMATURE_CONSUMPTION), reached whenever a fault lands between the
    /// per-package rebuild and the global write.
    #[tokio::test]
    async fn a_destination_drain_reasserts_membership_a_failed_pass_left_unapplied() {
        let storage = Arc::new(InMemStorage::default());
        let state = AppState::headless(storage.clone());
        // A warm copy that published `alpha`, whose last artifact has since been
        // replicated away: no truth under `packages/alpha/`, both views still
        // listing it, and a `_dirty/` marker announcing the change.
        let listed = vec!["alpha".to_string()];
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.json"),
            pep691_global_json(&listed).into_bytes(),
        );
        storage.insert(
            &format!("{SIMPLE_PREFIX}index.html"),
            pep503_global_html(&listed).into_bytes(),
        );
        storage.insert(&format!("{SIMPLE_PREFIX}alpha/index.json"), b"{}".to_vec());
        storage.insert(&format!("{SIMPLE_PREFIX}alpha/index.html"), b"<!>".to_vec());
        mark_dirty(storage.as_ref(), "alpha").await.unwrap();

        // Pass one: the per-package views go, then the global index is
        // unreachable. The error must reach the caller with the markers intact.
        storage.fail_reads_of(&format!("{SIMPLE_PREFIX}index.json"));
        drain_dirty_uncached(&state, storage.as_ref())
            .await
            .unwrap_err();
        storage.heal_reads();
        assert!(
            !storage
                .head_exists(&format!("{SIMPLE_PREFIX}alpha/index.json"))
                .await
                .unwrap(),
            "the rebuild must have removed the dead package's own view before failing"
        );
        assert!(
            !storage
                .list_dir_entries(DIRTY_PREFIX)
                .await
                .unwrap()
                .is_empty(),
            "a global write that failed must retain the markers it never applied"
        );

        // Pass two, with storage healthy again.
        drain_dirty_uncached(&state, storage.as_ref())
            .await
            .unwrap();

        assert_eq!(
            global_html(&storage).await,
            pep503_global_html(&[]),
            "the retry must still drop the dead package from the global index"
        );
        let json = storage
            .get_bytes(&format!("{SIMPLE_PREFIX}index.json"))
            .await
            .unwrap();
        assert!(
            !String::from_utf8(json).unwrap().contains("alpha"),
            "the canonical global JSON must not keep listing a package with no artifacts"
        );
    }

    /// A sidecar read that fails for *availability* must fail the whole rebuild,
    /// never omit the file. Omission is only ever right for a parse failure
    /// (corruption we must not overwrite) or a not-found (a concurrent delete,
    /// which holds its own marker). A single-file package is where laundering
    /// the third case into the first two is fatal: "omit the file" and "the
    /// package is dead" become the same observation, so the rebuild derives an
    /// empty view, DELETES a live package's index, reports success — and the
    /// tick consumes the only signal that would have retried. Originally found
    /// as vopr seeds 7384 / 19900 (one bucket, two topologies) and 47843 (the
    /// same poisoning under two buckets).
    #[tokio::test]
    async fn a_transient_sidecar_read_error_fails_the_rebuild_instead_of_burying_the_package() {
        const FILE: &str = "alpha-1.0-py3-none-any.whl";
        let storage = Arc::new(InMemStorage::default());
        let state = test_app_state(storage.clone(), Duration::from_secs(3600));
        let pinned = state.pin();
        let view = format!("{SIMPLE_PREFIX}alpha/index.json");
        seed_private_artifact(&storage, "alpha", FILE);

        // A healthy tick first: the package is live and listed everywhere.
        mark_dirty(storage.as_ref(), "alpha").await.unwrap();
        tick(&state, &pinned).await.unwrap();
        assert!(
            storage.head_exists(&view).await.unwrap(),
            "test setup broken: the healthy tick never built the package view"
        );

        // Now one sidecar read blips while a fresh marker is outstanding.
        mark_dirty(storage.as_ref(), "alpha").await.unwrap();
        storage.fail_reads_of(&sidecar_key(&format!("{PACKAGES_PREFIX}alpha/{FILE}")));
        tick(&state, &pinned).await.unwrap_err();
        storage.heal_reads();

        assert!(
            storage.head_exists(&view).await.unwrap(),
            "a live package's index may not be deleted because one sidecar read failed"
        );
        assert!(
            !storage
                .list_dir_entries(DIRTY_PREFIX)
                .await
                .unwrap()
                .is_empty(),
            "a rebuild that failed must retain the markers that are its only retry signal"
        );
        let global = storage
            .get_bytes(&format!("{SIMPLE_PREFIX}index.json"))
            .await
            .unwrap();
        assert!(
            String::from_utf8(global).unwrap().contains("alpha"),
            "a live package must stay in the global index across a transient read error"
        );
    }

    /// A global-index write that ERRORS must not leave its delta absorbed in the
    /// cached name set. The delta is applied in memory before the conditional
    /// write, so a surviving cache pins the OLD ETag over a MUTATED set: every
    /// retry then computes `changed = false`, the currency probe agrees (nothing
    /// moved — nothing was written), and the tick returns Ok and consumes its
    /// dirty markers. The delta is gone until the audit, with a dead package
    /// still listed globally. Originally found as vopr seed 19026.
    #[tokio::test]
    async fn a_failed_global_index_write_does_not_leave_the_delta_absorbed_in_the_cache() {
        let storage = Arc::new(InMemStorage::default());
        let state = AppState::headless(storage.clone());
        update_global_index(&state, storage.as_ref(), &["alpha".to_string()], &[])
            .await
            .unwrap();

        // The removal cannot be published: the conditional HTML write is the
        // first object the CAS path touches, so nothing lands at all.
        storage.fail_writes_of(&format!("{SIMPLE_PREFIX}index.html"));
        update_global_index(&state, storage.as_ref(), &[], &["alpha".to_string()])
            .await
            .unwrap_err();
        storage.heal_writes();

        // The identical delta, retried. It is still a change: nothing landed.
        update_global_index(&state, storage.as_ref(), &[], &["alpha".to_string()])
            .await
            .unwrap();

        let json = storage
            .get_bytes(&format!("{SIMPLE_PREFIX}index.json"))
            .await
            .unwrap();
        assert!(
            !String::from_utf8(json).unwrap().contains("alpha"),
            "the retry of a delta whose write failed must still remove the name"
        );
        assert_eq!(
            global_html(&storage).await,
            pep503_global_html(&[]),
            "and must still publish the HTML for the set it wrote"
        );
    }

    /// The backfill's hash read and its create are not atomic, and the immutable
    /// filename can come free in between — a failed publish clearing its own
    /// unacked debris. A sidecar fabricated over bytes that are no longer there
    /// is a torn record (live body, wrong sha256) that the NEXT upload of that
    /// filename inherits, so the backfill re-reads the body after the create and
    /// retracts its own fabrication. Staged on a paused clock: the delete lands
    /// inside the artificial latency of the hash read, before the confirm read
    /// looks. Originally found by the vopr soak alongside seeds 1784486481 /
    /// 1784817003 / 1784521773 (commit e860792).
    #[tokio::test(start_paused = true)]
    async fn a_backfill_retracts_the_sidecar_it_fabricated_over_vanished_bytes() {
        const FILE: &str = "alpha-1.0-py3-none-any.whl";
        let storage = Arc::new(InMemStorage::default());
        let key = format!("{PACKAGES_PREFIX}alpha/{FILE}");
        storage.insert(&key, b"artifact".to_vec());
        storage.set_get_delay(Duration::from_millis(50));

        let backfill = {
            let storage = storage.clone();
            let key = key.clone();
            tokio::spawn(async move {
                let entry = FileEntry {
                    key,
                    size: 8,
                    last_modified: Some("2026-01-01T00:00:00Z".to_string()),
                };
                backfill_sidecar(storage.as_ref(), &entry, FILE, Some(crate::origin::PRIVATE)).await
            })
        };
        // The hash read returns at t=50ms and the confirm read looks at t=100ms.
        // The publisher that never acked clears its debris in between.
        tokio::time::sleep(Duration::from_millis(75)).await;
        storage
            .delete_keys(std::slice::from_ref(&key))
            .await
            .unwrap();

        assert!(
            backfill.await.unwrap().unwrap().is_none(),
            "a backfill whose body vanished under it indexes nothing"
        );
        assert!(
            !storage.head_exists(&sidecar_key(&key)).await.unwrap(),
            "the fabricated sidecar must be retracted, not left for the next upload \
             of this immutable filename to inherit as a torn record"
        );
    }
}
