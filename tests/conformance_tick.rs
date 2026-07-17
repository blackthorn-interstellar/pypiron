//! Conformance: the real worker tick vs the pure marker-selection rule
//! (dev/MOONSHOT.md rung 2). The stateright event-protocol model binds its
//! worker transition to `worker::consumable_dirty_work`; this suite closes the
//! remaining gap by driving the REAL `worker::tick` — listing, rebuild, global
//! index CAS, and marker consumption — against a deterministic in-memory
//! bucket, and asserting the tick's observable effects match what the pure
//! selection rule licenses:
//!
//!   - exactly the selected marker keys are consumed, none other;
//!   - a selected package's views are (re)built from truth, or removed when
//!     truth is empty;
//!   - a deferred package (fresh unpaired intent) is left untouched — markers,
//!     views, and global index alike.
//!
//! Marker age is controlled by writing markers under a backdated `SimClock`
//! (staleness compares the storage's last-modified against the worker's
//! clock), so the intent-grace paths run without touching the process-global
//! simulated clock — keeping these tests safe under parallel execution.

use std::sync::Arc;

use pypiron::sim::{single_bucket_state, SimClock, SimStorage};
use pypiron::storage::{FileEntry, Storage};
use pypiron::worker;
use pypiron::AppState;

const PKG: &str = "p0";
const FILE: &str = "p0-1.0-py3-none-any.whl";

fn artifact_key() -> String {
    format!("packages/{PKG}/{FILE}")
}

fn sidecar_json(bytes: &[u8]) -> Vec<u8> {
    use sha2::digest::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let sha = format!("{:x}", hasher.finalize());
    serde_json::to_vec(&serde_json::json!({
        "sha256": sha,
        "size": bytes.len(),
        "version": "1.0",
        "upload-time": "2026-01-01T00:00:00Z",
        "yanked": false,
        "origin": "private",
    }))
    .expect("sidecar serializes")
}

/// A clock 30 minutes behind the real one: markers written under it are
/// already stale for the default 900 s intent grace when the tick compares
/// them against the real current time.
fn backdated_clock() -> Arc<SimClock> {
    SimClock::new(time::OffsetDateTime::now_utc() - time::Duration::minutes(30))
}

fn seed_artifact(storage: &SimStorage) {
    let key = artifact_key();
    storage.insert(&key, b"wheel-bytes".to_vec());
    storage.insert(&format!("{key}.meta.json"), sidecar_json(b"wheel-bytes"));
}

fn state_over(storage: Arc<SimStorage>) -> Arc<AppState> {
    Arc::new(single_bucket_state(storage))
}

/// What the pure selection rule licenses for the current `_dirty/` listing,
/// evaluated exactly the way the tick evaluates it.
async fn licensed_consumption(state: &AppState, storage: &dyn Storage) -> Vec<String> {
    let entries: Vec<FileEntry> = storage
        .list_dir_entries("_dirty/")
        .await
        .expect("list markers");
    let mut keys: Vec<String> = worker::consumable_dirty_work(
        &entries,
        time::OffsetDateTime::now_utc(),
        state.intent_grace,
    )
    .into_iter()
    .flat_map(|work| work.keys)
    .collect();
    keys.sort();
    keys
}

async fn run_tick(state: &Arc<AppState>) {
    let pinned = state.pin();
    worker::tick(state, &pinned).await.expect("tick succeeds");
}

fn remaining_markers(storage: &SimStorage) -> Vec<String> {
    storage
        .dump()
        .into_keys()
        .filter(|key| key.starts_with("_dirty/"))
        .collect()
}

#[tokio::test]
async fn paired_intent_and_commit_rebuild_and_consume() {
    let clock = backdated_clock();
    let storage = SimStorage::new(clock);
    seed_artifact(&storage);
    storage.insert(&format!("_dirty/{PKG}!n1.intent"), Vec::new());
    storage.insert(&format!("_dirty/{PKG}!n1.commit"), Vec::new());
    let state = state_over(storage.clone());

    let licensed = licensed_consumption(state.as_ref(), storage.as_ref()).await;
    assert_eq!(licensed.len(), 2, "pair must be selected: {licensed:?}");
    run_tick(&state).await;

    let dump = storage.dump();
    assert!(remaining_markers(&storage).is_empty(), "pair consumed");
    assert!(
        dump.contains_key(&format!("simple/{PKG}/index.html")),
        "package view built"
    );
    let json = dump
        .get(&format!("simple/{PKG}/index.json"))
        .expect("package JSON view built");
    assert!(
        String::from_utf8_lossy(json).contains(FILE),
        "view lists the artifact"
    );
    let global = dump.get("simple/index.json").expect("global index built");
    assert!(
        String::from_utf8_lossy(global).contains(PKG),
        "global index lists the package"
    );
}

#[tokio::test]
async fn fresh_unpaired_intent_defers_the_package() {
    // A live clock: the intent is seconds old when the tick judges it.
    let clock = SimClock::new(time::OffsetDateTime::now_utc());
    let storage = SimStorage::new(clock);
    seed_artifact(&storage);
    let intent = format!("_dirty/{PKG}!n1.intent");
    storage.insert(&intent, Vec::new());
    let state = state_over(storage.clone());

    let licensed = licensed_consumption(state.as_ref(), storage.as_ref()).await;
    assert!(licensed.is_empty(), "fresh intent defers: {licensed:?}");
    run_tick(&state).await;

    let dump = storage.dump();
    assert_eq!(remaining_markers(&storage), vec![intent], "marker survives");
    assert!(
        !dump.contains_key(&format!("simple/{PKG}/index.html")),
        "no view written while a writer is in flight"
    );
    assert!(
        !dump.contains_key("simple/index.json"),
        "global index untouched"
    );
}

#[tokio::test]
async fn stale_unpaired_intent_heals_the_crashed_writer() {
    let clock = backdated_clock();
    let storage = SimStorage::new(clock);
    seed_artifact(&storage);
    storage.insert(&format!("_dirty/{PKG}!n1.intent"), Vec::new());
    let state = state_over(storage.clone());

    let licensed = licensed_consumption(state.as_ref(), storage.as_ref()).await;
    assert_eq!(licensed.len(), 1, "stale intent selected: {licensed:?}");
    run_tick(&state).await;

    assert!(
        remaining_markers(&storage).is_empty(),
        "stale intent healed"
    );
    assert!(
        storage
            .dump()
            .contains_key(&format!("simple/{PKG}/index.html")),
        "package rebuilt anyway"
    );
}

#[tokio::test]
async fn legacy_flat_marker_is_a_commit() {
    let clock = backdated_clock();
    let storage = SimStorage::new(clock);
    seed_artifact(&storage);
    storage.insert(&format!("_dirty/{PKG}"), Vec::new());
    let state = state_over(storage.clone());

    run_tick(&state).await;

    assert!(
        remaining_markers(&storage).is_empty(),
        "legacy key consumed"
    );
    assert!(
        storage
            .dump()
            .contains_key(&format!("simple/{PKG}/index.json")),
        "package rebuilt from the legacy marker"
    );
}

#[tokio::test]
async fn commit_for_an_empty_package_removes_its_views() {
    let clock = backdated_clock();
    let storage = SimStorage::new(clock);
    // Stale views exist, but truth is empty (the artifact was deleted).
    storage.insert(&format!("simple/{PKG}/index.html"), b"<stale/>".to_vec());
    storage.insert(&format!("simple/{PKG}/index.json"), b"{}".to_vec());
    storage.insert(&format!("_dirty/{PKG}!n9.commit"), Vec::new());
    let state = state_over(storage.clone());

    run_tick(&state).await;

    let dump = storage.dump();
    assert!(remaining_markers(&storage).is_empty(), "commit consumed");
    assert!(
        !dump.contains_key(&format!("simple/{PKG}/index.html"))
            && !dump.contains_key(&format!("simple/{PKG}/index.json")),
        "views of an empty package are removed (views may lag, never lead)"
    );
    if let Some(global) = dump.get("simple/index.json") {
        assert!(
            !String::from_utf8_lossy(global).contains(&format!("\"{PKG}\"")),
            "global index does not list the dead package"
        );
    }
}

/// The tick consumes exactly what the selection rule licenses even when
/// packages mix: one ready package, one deferred package, in the same tick.
#[tokio::test]
async fn mixed_listing_consumes_exactly_the_licensed_keys() {
    let clock = backdated_clock();
    let storage = SimStorage::new(clock.clone());
    seed_artifact(&storage);
    storage.insert(&format!("_dirty/{PKG}!n1.commit"), Vec::new());
    // Second package: artifact present, but its writer is still in flight —
    // written NOW (clock caught up to real time), so the intent is fresh.
    clock.advance(std::time::Duration::from_secs(30 * 60));
    let other = "q1-1.0-py3-none-any.whl";
    storage.insert(&format!("packages/q1/{other}"), b"other".to_vec());
    storage.insert(
        &format!("packages/q1/{other}.meta.json"),
        sidecar_json(b"other"),
    );
    storage.insert("_dirty/q1!n2.intent", Vec::new());
    let state = state_over(storage.clone());

    let licensed = licensed_consumption(state.as_ref(), storage.as_ref()).await;
    run_tick(&state).await;

    let mut remaining = remaining_markers(&storage);
    remaining.sort();
    assert_eq!(
        remaining,
        vec!["_dirty/q1!n2.intent".to_string()],
        "only the licensed keys were consumed (licensed: {licensed:?})"
    );
    let dump = storage.dump();
    assert!(
        dump.contains_key(&format!("simple/{PKG}/index.json")),
        "ready package rebuilt"
    );
    assert!(
        !dump.contains_key("simple/q1/index.json"),
        "deferred package untouched"
    );
}
