//! The VOPR: deterministic simulation testing for the pypiron event protocol
//! (dev/MOONSHOT.md rung 1, FoundationDB/TigerBeetle lineage).
//!
//! An entire multi-node fleet — N server nodes, M shared buckets, concurrent
//! writers, the rebuild worker, replication fan-out, note sweep, and tree
//! diff — runs inside ONE single-threaded tokio runtime with paused virtual
//! time. Every wall-clock read routes through `pypiron::clock`'s simulated
//! override, every storage op goes through an in-memory `SimStorage` behind a
//! per-node fault view, and every scheduling choice derives from one 8-byte
//! seed. A failing seed reproduces exactly: `vopr --seed N`.
//!
//! What the fault plan injects, all seed-derived:
//!   - per-op virtual latency (unique nanosecond deadlines — total order, so
//!     the paused runtime's wakeups are deterministic);
//!   - storage failures (availability errors surfaced to the protocol);
//!   - node crashes at storage-op boundaries: the op future parks forever and
//!     the driver aborts the node's tasks — a power cut between two storage
//!     operations (in-flight work is lost, storage state survives). One
//!     honest caveat: an op parked inside `bounded_artifact_write`'s timeout
//!     fires that timeout first, so a "crash" there degrades to a timed-out
//!     write followed by the park — which is itself a legal real-world
//!     schedule (client-side timeout, then the node dies);
//!   - node restarts with cold caches (a fresh AppState over the same bucket);
//!   - clock jumps past the intent grace so crashed-writer healing runs.
//!
//! What is checked once the fleet quiesces (faults healed, workers drained):
//!   - DURABILITY: every acknowledged upload's bytes, sidecar, package-view
//!     entry, and global-index entry exist — on every bucket;
//!   - VIEWS == TRUTH: each bucket's materialized indexes equal a fresh
//!     derivation from its truth tree (views may lag, never lead, and at
//!     quiescence they may not even lag);
//!   - CONVERGENCE: all buckets hold identical truth and views;
//!   - NO LEAKS: no unconsumed `_dirty/` markers, no undrained `_repl/` notes;
//!   - CONSERVATION: acknowledged bytes are never silently lost — a file is
//!     live, or its filename was deleted/frozen by an authorized operation;
//!   - LIVENESS (bounded): the fleet reaches quiescence within the drain
//!     budget — no livelock.
//!
//! Determinism is self-checked, not assumed: every run whose seed is a
//! multiple of `--recheck-every` executes twice and must produce an identical
//! storage-op trace hash.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use pypiron::buckets::{BucketHandle, BucketSet};
use pypiron::sim::{SimClock, SimStorage};
use pypiron::storage::{FileEntry, ObjectMeta, Storage};
use pypiron::{replicate, worker, AppState};

const PACKAGES: [&str; 2] = ["vopr-alpha", "vopr-beta"];
const FILES_PER_PKG: u8 = 2;
const SIM_START: &str = "2026-01-01T00:00:00Z";

fn filename(pkg: &str, file: u8) -> String {
    // Wheel filenames escape dashes in the distribution segment; a dashed
    // package name here would parse as distribution "vopr", version "alpha".
    format!("{}-{}.0-py3-none-any.whl", pkg.replace('-', "_"), file + 1)
}

fn body_bytes(pkg: &str, file: u8, variant: u8) -> Vec<u8> {
    format!("artifact:{pkg}:{file}:{variant}").into_bytes()
}

// ---------------------------------------------------------------------------
// Seeded PRNG: SplitMix64 — tiny, deterministic, no dependency.
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_add(0x9E3779B97F4A7C15))
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

// ---------------------------------------------------------------------------
// Fault plan + per-node storage view.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum OpFate {
    /// Proceed after a virtual delay.
    Ok,
    /// Surface an availability error after a virtual delay.
    Fail,
    /// The node loses power at this op boundary: park forever (the driver
    /// aborts the task) and mark the node crashed.
    Crash,
}

/// Seed-derived fault schedule plus the shared trace. One per run.
struct FaultPlan {
    /// Per-op randomness, pre-derived from the seed so fate depends only on
    /// the global op sequence number (deterministic under deterministic
    /// scheduling, which the trace hash then verifies).
    op_seq: AtomicU64,
    fail_percent: u64,
    /// Node crash points: op-sequence numbers at which the issuing node dies.
    crash_at: Mutex<BTreeMap<u64, ()>>,
    crashed: Vec<AtomicBool>,
    /// While true, no faults fire (the heal/drain phase).
    healing: AtomicBool,
    rng_stream: Mutex<Rng>,
    trace: Mutex<TraceHasher>,
}

struct TraceHasher {
    hash: u64,
    events: u64,
    /// Full event log, kept only while diagnosing a determinism violation.
    log: Option<Vec<String>>,
}

impl TraceHasher {
    fn record(&mut self, parts: &[&str]) {
        // FNV-1a over the event tuple: cheap, stable, order-sensitive.
        for part in parts {
            for byte in part.as_bytes() {
                self.hash ^= u64::from(*byte);
                self.hash = self.hash.wrapping_mul(0x100000001B3);
            }
            self.hash ^= 0x1F;
            self.hash = self.hash.wrapping_mul(0x100000001B3);
        }
        self.events += 1;
        if let Some(log) = &mut self.log {
            log.push(parts.join(" "));
        }
    }
}

impl FaultPlan {
    fn new(seed: u64, nodes: usize, fail_percent: u64) -> Arc<Self> {
        Arc::new(FaultPlan {
            op_seq: AtomicU64::new(0),
            fail_percent,
            crash_at: Mutex::new(BTreeMap::new()),
            crashed: (0..nodes).map(|_| AtomicBool::new(false)).collect(),
            healing: AtomicBool::new(false),
            rng_stream: Mutex::new(Rng::new(seed ^ 0xFA_11_7E_57)),
            trace: Mutex::new(TraceHasher {
                hash: 0xcbf29ce484222325,
                events: 0,
                log: std::env::var_os("VOPR_TRACE").map(|_| Vec::new()),
            }),
        })
    }

    fn heal(&self) {
        self.healing.store(true, Ordering::SeqCst);
    }

    fn schedule_crash_soon(&self, node: usize, within_ops: u64) {
        let at = self.op_seq.load(Ordering::SeqCst)
            + 1
            + self.rng_stream.lock().expect("rng lock").below(within_ops);
        self.crash_at
            .lock()
            .expect("crash lock")
            .insert(at | ((node as u64) << 56), ());
    }

    fn restart(&self, node: usize) {
        self.crashed[node].store(false, Ordering::SeqCst);
        let mut pending = self.crash_at.lock().expect("crash lock");
        pending.retain(|key, _| (key >> 56) as usize != node);
    }

    fn node_crashed(&self, node: usize) -> bool {
        self.crashed[node].load(Ordering::SeqCst)
    }

    /// Decide this op's fate and unique virtual latency.
    fn admit(&self, node: usize, op: &str, key: &str) -> (OpFate, std::time::Duration) {
        let seq = self.op_seq.fetch_add(1, Ordering::SeqCst);
        let jitter = {
            let mut rng = self.rng_stream.lock().expect("rng lock");
            rng.below(1000)
        };
        // Unique deadline per op: total order over wakeups.
        let delay = std::time::Duration::from_nanos(jitter * 1_000 + seq + 1);
        self.trace.lock().expect("trace lock").record(&[
            &node.to_string(),
            op,
            key,
            &seq.to_string(),
        ]);
        if self.node_crashed(node) {
            return (OpFate::Crash, delay);
        }
        if self.healing.load(Ordering::SeqCst) {
            return (OpFate::Ok, delay);
        }
        let crash_due = {
            let mut pending = self.crash_at.lock().expect("crash lock");
            let due: Vec<u64> = pending
                .keys()
                .copied()
                .filter(|k| (k >> 56) as usize == node && (k & 0x00FF_FFFF_FFFF_FFFF) <= seq)
                .collect();
            for k in &due {
                pending.remove(k);
            }
            !due.is_empty()
        };
        if crash_due {
            self.crashed[node].store(true, Ordering::SeqCst);
            return (OpFate::Crash, delay);
        }
        let fail = {
            let mut rng = self.rng_stream.lock().expect("rng lock");
            rng.chance(self.fail_percent)
        };
        if fail {
            (OpFate::Fail, delay)
        } else {
            (OpFate::Ok, delay)
        }
    }

    fn trace_hash(&self) -> (u64, u64) {
        let trace = self.trace.lock().expect("trace lock");
        (trace.hash, trace.events)
    }

    fn take_log(&self) -> Option<Vec<String>> {
        self.trace.lock().expect("trace lock").log.take()
    }
}

/// One node's view of one bucket: every op consults the fault plan first.
struct FaultView {
    inner: Arc<SimStorage>,
    node: usize,
    plan: Arc<FaultPlan>,
}

impl FaultView {
    async fn gate(&self, op: &'static str, key: &str) -> Result<()> {
        let (fate, delay) = self.plan.admit(self.node, op, key);
        tokio::time::sleep(delay).await;
        match fate {
            OpFate::Ok => Ok(()),
            OpFate::Fail => Err(anyhow!("vopr: injected storage failure ({op} {key})")),
            OpFate::Crash => {
                // Power cut at an op boundary: never resume. The driver
                // aborts this task; nothing after this await runs.
                std::future::pending::<()>().await;
                unreachable!("parked op resumed after node crash")
            }
        }
    }
}

#[async_trait::async_trait]
impl Storage for FaultView {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        self.gate("head", key).await?;
        self.inner.head_exists(key).await
    }
    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        self.gate("size", key).await?;
        self.inner.stored_size(key).await
    }
    async fn serve_artifact(
        &self,
        key: &str,
        range: Option<&str>,
    ) -> Result<axum::response::Response<axum::body::Body>> {
        self.gate("serve", key).await?;
        self.inner.serve_artifact(key, range).await
    }
    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>> {
        self.inner.presign_get(key, expires).await
    }
    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, ct: Option<&str>) -> Result<()> {
        if key == "simple/index.json" {
            let body = String::from_utf8_lossy(&bytes).into_owned();
            self.gate("put", &format!("{key} => {body}")).await?;
        } else {
            self.gate("put", key).await?;
        }
        self.inner.put_bytes(key, bytes, ct).await
    }
    async fn put_if_absent(&self, key: &str, bytes: Vec<u8>, ct: Option<&str>) -> Result<bool> {
        self.gate("put_if_absent", key).await?;
        self.inner.put_if_absent(key, bytes, ct).await
    }
    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        ct: Option<&str>,
    ) -> Result<bool> {
        self.gate("put_file", key).await?;
        self.inner.put_file_if_absent(key, path, ct).await
    }
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.gate("get", key).await?;
        self.inner.get_bytes(key).await
    }
    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        self.gate("list_dir", dir_prefix).await?;
        self.inner.list_dir_entries(dir_prefix).await
    }
    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.gate("list_all", prefix).await?;
        self.inner.list_all(prefix).await
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        self.gate("list_page", prefix).await?;
        self.inner.list_page(prefix, after, limit).await
    }
    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        let label = keys.first().map(String::as_str).unwrap_or("");
        self.gate("delete", label).await?;
        self.inner.delete_keys(keys).await
    }
    fn supports_leases(&self) -> bool {
        true
    }
    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.gate("get_etag", key).await?;
        self.inner.get_with_etag(key).await
    }
    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        self.gate("put_inm", key).await?;
        self.inner.put_if_none_match(key, bytes).await
    }
    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        if key == "simple/index.json" {
            let body = String::from_utf8_lossy(&bytes).into_owned();
            self.gate("put_im", &format!("{key} if={etag} => {body}"))
                .await?;
        } else {
            self.gate("put_im", key).await?;
        }
        let outcome = self.inner.put_if_match(key, etag, bytes).await;
        if key == "simple/index.json" {
            if let Ok(result) = &outcome {
                self.plan.trace.lock().expect("trace lock").record(&[
                    "cas-outcome",
                    key,
                    if result.is_some() { "won" } else { "lost" },
                ]);
            }
        }
        outcome
    }
}

// ---------------------------------------------------------------------------
// The fleet: N nodes over M shared buckets.
// ---------------------------------------------------------------------------

struct Node {
    state: Arc<AppState>,
    /// Live op tasks; aborted on crash, drained on restart.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

struct Fleet {
    buckets: Vec<Arc<SimStorage>>,
    nodes: Vec<Node>,
    plan: Arc<FaultPlan>,
    clock: Arc<SimClock>,
    ledger: Arc<Mutex<Ledger>>,
    /// One rebuild worker per bucket at a time — the serialization the bucket
    /// lease provides in production. The sloppy dual-leadership window (two
    /// live ticks on one bucket) is deliberately out of scope here: the
    /// event-protocol model covers it exhaustively and documents that only
    /// the audit heals its stale-rebuild clobber.
    tick_lock: Vec<Arc<tokio::sync::Mutex<()>>>,
}

#[derive(Default)]
struct Ledger {
    /// Acknowledged uploads: (pkg, filename) -> body bytes.
    acked: BTreeMap<(String, String), Vec<u8>>,
    /// Filenames an acknowledged (204) delete removed.
    deleted: std::collections::BTreeSet<(String, String)>,
    published_attempts: u64,
    delete_attempts: u64,
}

fn build_node_state(
    buckets: &[Arc<SimStorage>],
    node: usize,
    plan: &Arc<FaultPlan>,
) -> Arc<AppState> {
    let handles: Vec<BucketHandle> = buckets
        .iter()
        .enumerate()
        .map(|(idx, bucket)| BucketHandle {
            storage: Arc::new(FaultView {
                inner: bucket.clone(),
                node,
                plan: plan.clone(),
            }) as Arc<dyn Storage>,
            name: format!("bucket-{idx}"),
        })
        .collect();
    let mut state = AppState::headless(handles[0].storage.clone());
    if handles.len() > 1 {
        state.buckets = Arc::new(BucketSet::new(handles));
    }
    // Tight grace keeps crashed-writer healing inside short simulated runs.
    state.intent_grace = time::Duration::seconds(60);
    state.fanout_grace = std::time::Duration::from_secs(5);
    Arc::new(state)
}

impl Fleet {
    fn new(
        seed: u64,
        nodes: usize,
        bucket_count: usize,
        fail_percent: u64,
        clock: Arc<SimClock>,
    ) -> Fleet {
        let plan = FaultPlan::new(seed, nodes, fail_percent);
        let buckets: Vec<Arc<SimStorage>> = (0..bucket_count)
            .map(|_| SimStorage::new(clock.clone()))
            .collect();
        let nodes = (0..nodes)
            .map(|node| Node {
                state: build_node_state(&buckets, node, &plan),
                tasks: Vec::new(),
            })
            .collect();
        let tick_lock = (0..bucket_count)
            .map(|_| Arc::new(tokio::sync::Mutex::new(())))
            .collect();
        Fleet {
            buckets,
            nodes,
            plan,
            clock,
            ledger: Arc::new(Mutex::new(Ledger::default())),
            tick_lock,
        }
    }

    fn spawn_on(&mut self, node: usize, fut: impl Future<Output = ()> + 'static) {
        let handle = tokio::task::spawn_local(fut);
        self.nodes[node].tasks.push(handle);
    }

    fn crash_node(&mut self, node: usize) {
        // The plan already (or will) park the node's ops; abort everything it
        // had in flight — in-memory state dies with the process.
        for task in self.nodes[node].tasks.drain(..) {
            task.abort();
        }
    }

    fn restart_node(&mut self, node: usize) {
        self.crash_node(node);
        self.plan.restart(node);
        // Cold caches: a fresh AppState over the same buckets.
        let plan = self.plan.clone();
        self.nodes[node].state = build_node_state(&self.buckets, node, &plan);
    }
}

// ---------------------------------------------------------------------------
// Workload ops.
// ---------------------------------------------------------------------------

async fn op_publish(
    state: Arc<AppState>,
    ledger: Arc<Mutex<Ledger>>,
    clock: Arc<SimClock>,
    pkg: String,
    file: u8,
    variant: u8,
) {
    use sha2::digest::Digest;
    let body = body_bytes(&pkg, file, variant);
    let fname = filename(&pkg, file);
    ledger.lock().expect("ledger lock").published_attempts += 1;
    let sha256 = {
        let mut hasher = sha2::Sha256::new();
        hasher.update(&body);
        format!("{:x}", hasher.finalize())
    };
    let req = pypiron::PublishRequest {
        pkg: pkg.clone(),
        filename: fname.clone(),
        sha256,
        size: body.len() as u64,
        version: format!("{}.0", file + 1),
        requires_python: None,
        is_mirror: false,
        upload_time: clock.now_rfc3339(),
        yanked: pypiron::sidecar::Yanked::Flag(false),
        wheel_metadata: None,
        is_wheel: false,
        provenance: None,
        body: pypiron::PublishBody::Bytes(body.clone()),
    };
    let pinned = state.pin();
    if pypiron::publish_record(&state, &pinned, req).await.is_ok() {
        ledger
            .lock()
            .expect("ledger lock")
            .acked
            .insert((pkg, fname), body);
    }
}

async fn op_delete(state: Arc<AppState>, ledger: Arc<Mutex<Ledger>>, pkg: String, file: u8) {
    let fname = filename(&pkg, file);
    ledger.lock().expect("ledger lock").delete_attempts += 1;
    let pinned = state.pin();
    if pypiron::delete_record(&state, &pinned, &pkg, &fname)
        .await
        .is_ok()
    {
        ledger
            .lock()
            .expect("ledger lock")
            .deleted
            .insert((pkg, fname));
    }
}

async fn op_tick(state: Arc<AppState>, lease: Arc<tokio::sync::Mutex<()>>) {
    let pinned = state.pin();
    let _guard = lease.lock().await;
    let _ = worker::tick(&state, &pinned).await;
}

async fn op_sweep(state: Arc<AppState>) {
    let _ = replicate::sweep_all_markers(&state).await;
}

async fn op_reconcile(state: Arc<AppState>) {
    let pinned = state.pin();
    let _ = replicate::reconcile(&state, &pinned).await;
}

// ---------------------------------------------------------------------------
// One seeded run.
// ---------------------------------------------------------------------------

struct RunOutcome {
    trace_hash: u64,
    trace_events: u64,
    trace_log: Option<Vec<String>>,
    acked: usize,
    audit_view_repairs: u64,
    violations: Vec<String>,
}

async fn run_seed(
    seed: u64,
    nodes: usize,
    buckets: usize,
    ops: u64,
    fail_percent: u64,
) -> RunOutcome {
    let start =
        time::OffsetDateTime::parse(SIM_START, &time::format_description::well_known::Rfc3339)
            .expect("valid sim start");
    let clock = SimClock::new(start);
    let _guard = clock.install_global();
    let mut fleet = Fleet::new(seed, nodes, buckets, fail_percent, clock.clone());
    let mut rng = Rng::new(seed);

    // ---- Chaos phase: seed-driven interleaving of ops, faults, and time.
    for _ in 0..ops {
        let node = rng.below(nodes as u64) as usize;
        if fleet.plan.node_crashed(node) && !rng.chance(30) {
            // A crashed node mostly stays down this step; sometimes restarts.
            continue;
        }
        if fleet.plan.node_crashed(node) {
            fleet.restart_node(node);
            continue;
        }
        let state = fleet.nodes[node].state.clone();
        let ledger = fleet.ledger.clone();
        match rng.below(100) {
            0..=39 => {
                let pkg = PACKAGES[rng.below(PACKAGES.len() as u64) as usize].to_string();
                let file = rng.below(u64::from(FILES_PER_PKG)) as u8;
                let variant = rng.below(2) as u8;
                let clock = fleet.clock.clone();
                fleet.spawn_on(node, op_publish(state, ledger, clock, pkg, file, variant));
            }
            40..=49 => {
                let pkg = PACKAGES[rng.below(PACKAGES.len() as u64) as usize].to_string();
                let file = rng.below(u64::from(FILES_PER_PKG)) as u8;
                fleet.spawn_on(node, op_delete(state, ledger, pkg, file));
            }
            50..=74 => {
                let lease = fleet.tick_lock[0].clone(); // every pin selects bucket 0
                fleet.spawn_on(node, op_tick(state, lease));
            }
            75..=81 => {
                if buckets > 1 {
                    fleet.spawn_on(node, op_sweep(state));
                }
            }
            82..=85 => {
                if buckets > 1 {
                    fleet.spawn_on(node, op_reconcile(state));
                }
            }
            86..=90 => {
                // Jump past the intent grace: crashed writers become healable.
                clock.advance(std::time::Duration::from_secs(90));
            }
            91..=95 => {
                fleet.plan.schedule_crash_soon(node, 12);
                // Give the doomed node's in-flight work a chance to hit the
                // crash point before the driver aborts it.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                if fleet.plan.node_crashed(node) {
                    fleet.crash_node(node);
                }
            }
            _ => {
                clock.advance(std::time::Duration::from_secs(1));
            }
        }
        // Let the paused runtime interleave whatever is in flight.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // ---- Heal phase: stop faults, restart everyone, then drive the fleet
    // the way production does — worker ticks, note sweeps, warm drains, the
    // pairwise tree diff, and the leader audit — until storage reaches a
    // fixpoint with no protocol debris. The audit matters: the event-protocol
    // model proves concurrent unleased rebuilds can strand a stale view until
    // the audit's fingerprint diff repairs it, so the system-level claim the
    // VOPR checks is "ticks + sweeps + reconcile + audit converge".
    let mut violations_pre: Vec<String> = Vec::new();
    fleet.plan.heal();
    for node in 0..nodes {
        fleet.restart_node(node);
    }
    clock.advance(std::time::Duration::from_secs(120)); // all intents stale
    let mut quiesced = false;
    let mut audit_view_repairs: u64 = 0;
    let mut last_fingerprint: Option<Vec<BTreeMap<String, Vec<u8>>>> = None;
    for round in 0..12 {
        // Drain markers across every node until none remain (bounded).
        for _ in 0..20 {
            for node in 0..nodes {
                let state = fleet.nodes[node].state.clone();
                let pinned = state.pin();
                let _ = worker::tick(&state, &pinned).await;
                if buckets > 1 {
                    let _ = replicate::sweep_all_markers(&state).await;
                    for (idx, handle) in state.buckets.handles().iter().enumerate() {
                        if idx != pinned.index {
                            let _ =
                                worker::drain_dirty_uncached(&state, handle.storage.as_ref()).await;
                        }
                    }
                }
            }
            let markers_left = fleet.buckets.iter().any(|bucket| {
                bucket
                    .dump()
                    .keys()
                    .any(|k| k.starts_with("_dirty/") || k.starts_with("_repl/"))
            });
            if !markers_left {
                break;
            }
            clock.advance(std::time::Duration::from_secs(90));
        }
        // The lost-copy backstop first: the tree diff converges truth a
        // crashed fan-out left behind and brackets its work in markers...
        let leader = fleet.nodes[0].state.clone();
        let pinned = leader.pin();
        if buckets > 1 {
            let _ = replicate::reconcile(&leader, &pinned).await;
            // ...which this drain pass consumes, rebuilding the affected
            // views, before the audits are allowed to look.
            for _ in 0..3 {
                let _ = worker::tick(&leader, &pinned).await;
                let _ = replicate::sweep_all_markers(&leader).await;
                for (idx, handle) in leader.buckets.handles().iter().enumerate() {
                    if idx != pinned.index {
                        let _ =
                            worker::drain_dirty_uncached(&leader, handle.storage.as_ref()).await;
                    }
                }
                clock.advance(std::time::Duration::from_secs(90));
            }
        }
        // With ticks lease-serialized and the diff drained, the marker path
        // must have converged every view on its own — snapshot them so an
        // audit that "helpfully" repaired a view is visible as the signal-loss
        // bug it would otherwise hide.
        let views_before_audit: Vec<BTreeMap<String, Vec<u8>>> = fleet
            .buckets
            .iter()
            .map(|bucket| {
                bucket
                    .dump()
                    .into_iter()
                    .filter(|(k, _)| k.starts_with("simple/"))
                    .collect()
            })
            .collect();
        if let Err(e) = worker::audit(&leader, &pinned, false).await {
            violations_pre.push(format!(
                "AUDIT: leader audit failed on round {round}: {e:?}"
            ));
        }
        // Every warm bucket gets the audit it receives in production before it
        // ever serves (on boot and on every selection switch): run it through
        // a throwaway single-bucket state so the leader's selected-bucket name
        // cache is never polluted with another bucket's namespace.
        for bucket in fleet.buckets.iter().skip(1) {
            let audit_state = Arc::new(pypiron::sim::single_bucket_state(
                bucket.clone() as Arc<dyn Storage>
            ));
            let audit_pin = audit_state.pin();
            if let Err(e) = worker::audit(&audit_state, &audit_pin, false).await {
                violations_pre.push(format!(
                    "AUDIT: warm-bucket audit failed on round {round}: {e:?}"
                ));
            }
        }
        clock.advance(std::time::Duration::from_secs(90));
        let views_after_audit: Vec<BTreeMap<String, Vec<u8>>> = fleet
            .buckets
            .iter()
            .map(|bucket| {
                bucket
                    .dump()
                    .into_iter()
                    .filter(|(k, _)| k.starts_with("simple/"))
                    .collect()
            })
            .collect();
        if views_before_audit != views_after_audit {
            // The audit repaired views the tick/sweep path had not converged.
            // Under injected storage failures this is within the system's
            // contract — markers are retained on failure and the audit is the
            // documented safety net — so it is a reported statistic there. In
            // a crash-only run (--fail-percent 0) there is no such excuse:
            // markers alone must converge the tick path over every crash
            // schedule, so an audit repair IS a protocol violation.
            audit_view_repairs += 1;
            if fail_percent == 0 {
                let changed: Vec<String> = views_before_audit
                    .iter()
                    .zip(views_after_audit.iter())
                    .enumerate()
                    .flat_map(|(idx, (before, after))| {
                        before
                            .keys()
                            .chain(after.keys())
                            .filter(|k| before.get(*k) != after.get(*k))
                            .map(move |k| {
                                format!(
                                    "bucket{idx}:{k} before={:?} after={:?}",
                                    before
                                        .get(k)
                                        .map(|v| String::from_utf8_lossy(v).into_owned()),
                                    after
                                        .get(k)
                                        .map(|v| String::from_utf8_lossy(v).into_owned()),
                                )
                            })
                    })
                    .collect();
                violations_pre.push(format!(
                    "AUDIT_REPAIRED_VIEWS: crash-only run needed the audit to converge views \
                     — the marker protocol failed to self-heal: {changed:#?}"
                ));
            }
        }
        // Fixpoint: no markers and no storage change over a full round.
        let snapshot: Vec<BTreeMap<String, Vec<u8>>> =
            fleet.buckets.iter().map(|bucket| bucket.dump()).collect();
        let markers_left = snapshot.iter().any(|dump| {
            dump.keys()
                .any(|k| k.starts_with("_dirty/") || k.starts_with("_repl/"))
        });
        if !markers_left && last_fingerprint.as_ref() == Some(&snapshot) {
            quiesced = true;
            break;
        }
        last_fingerprint = Some(snapshot);
    }

    // ---- Invariants.
    let ledger = fleet.ledger.lock().expect("ledger lock");
    let mut violations = violations_pre;
    if !quiesced {
        let leftovers: Vec<String> = fleet
            .buckets
            .iter()
            .enumerate()
            .flat_map(|(idx, bucket)| {
                bucket
                    .dump()
                    .into_keys()
                    .filter(|k| k.starts_with("_dirty/") || k.starts_with("_repl/"))
                    .map(move |k| format!("bucket{idx}:{k}"))
            })
            .collect();
        violations.push(format!(
            "LIVENESS: fleet did not quiesce within the drain budget; leftover markers: {leftovers:?}"
        ));
    }
    let dumps: Vec<BTreeMap<String, Vec<u8>>> =
        fleet.buckets.iter().map(|bucket| bucket.dump()).collect();

    // Convergence: identical truth + views across buckets.
    if buckets > 1 {
        let project = |dump: &BTreeMap<String, Vec<u8>>| -> BTreeMap<String, Vec<u8>> {
            let mut projected: BTreeMap<String, Vec<u8>> = dump
                .iter()
                .filter(|(k, _)| k.starts_with("packages/") || k.starts_with("simple/"))
                .filter(|(k, _)| {
                    // The global views are compared as a parsed name set below:
                    // an absent global index and an empty one both derive "no
                    // packages" from truth (the HTML is the same pure function
                    // of that set). Per-package views stay byte-strict.
                    *k != "simple/index.json" && *k != "simple/index.html"
                })
                .map(|(k, v)| {
                    if k.ends_with("/.origin") {
                        // Claims are equal when their *state* is equal; the
                        // nonce is a fresh ABA guard on every write by design.
                        let state = serde_json::from_slice::<serde_json::Value>(v)
                            .ok()
                            .and_then(|doc| {
                                doc.get("origin")
                                    .and_then(|o| o.as_str())
                                    .map(str::to_string)
                            })
                            .unwrap_or_else(|| String::from_utf8_lossy(v).into_owned());
                        (k.clone(), state.into_bytes())
                    } else {
                        (k.clone(), v.clone())
                    }
                })
                .collect();
            let names: Vec<String> = dump
                .get("simple/index.json")
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
                .and_then(|doc| {
                    doc.get("projects").and_then(|p| p.as_array()).map(|arr| {
                        let mut names: Vec<String> = arr
                            .iter()
                            .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
                            .map(str::to_string)
                            .collect();
                        names.sort();
                        names
                    })
                })
                .unwrap_or_default();
            projected.insert(
                "simple/index.json#names".to_string(),
                names.join(",").into_bytes(),
            );
            projected
        };
        let first = project(&dumps[0]);
        for (idx, dump) in dumps.iter().enumerate().skip(1) {
            let other = project(dump);
            if other != first {
                let diff: Vec<&String> = first
                    .keys()
                    .chain(other.keys())
                    .filter(|k| first.get(*k) != other.get(*k))
                    .collect();
                let detail: Vec<String> = diff
                    .iter()
                    .take(4)
                    .map(|k| {
                        format!(
                            "{k}: b0={:?} b{idx}={:?}",
                            first
                                .get(*k)
                                .map(|v| String::from_utf8_lossy(v).into_owned()),
                            other
                                .get(*k)
                                .map(|v| String::from_utf8_lossy(v).into_owned()),
                        )
                    })
                    .collect();
                violations.push(format!(
                    "CONVERGENCE: bucket 0 and bucket {idx} differ on {} keys: {detail:#?}\nb0 keys: {:?}\nb{idx} keys: {:?}",
                    diff.len(),
                    dumps[0].keys().collect::<Vec<_>>(),
                    dumps[idx].keys().collect::<Vec<_>>(),
                ));
                break;
            }
        }
    }

    for (bucket_idx, dump) in dumps.iter().enumerate() {
        // Durability of acknowledged uploads.
        for ((pkg, fname), body) in &ledger.acked {
            let akey = format!("packages/{pkg}/{fname}");
            let tomb = dump.contains_key(&format!("{akey}.tombstone"));
            let frozen = dump.contains_key(&format!("{akey}.frozen"));
            if ledger.deleted.contains(&(pkg.clone(), fname.clone())) || tomb || frozen {
                continue; // authorized removal or conflict freeze
            }
            match dump.get(&akey) {
                None => violations.push(format!(
                    "DURABILITY: acked {akey} missing on bucket {bucket_idx}"
                )),
                Some(stored) if stored != body => violations.push(format!(
                    "DURABILITY: acked {akey} byte-corrupt on bucket {bucket_idx}"
                )),
                Some(_) => {
                    if !dump.contains_key(&format!("{akey}.meta.json")) {
                        violations.push(format!(
                            "DURABILITY: acked {akey} lost its sidecar on bucket {bucket_idx}"
                        ));
                    }
                    let view = dump
                        .get(&format!("simple/{pkg}/index.json"))
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default();
                    if !view.contains(fname.as_str()) {
                        violations.push(format!(
                            "VISIBILITY: acked {akey} not listed in bucket {bucket_idx}'s view"
                        ));
                    }
                }
            }
        }

        // Views == a fresh derivation from truth (membership form).
        for pkg in PACKAGES {
            let prefix = format!("packages/{pkg}/");
            let live: Vec<String> = dump
                .keys()
                .filter_map(|k| k.strip_prefix(&prefix))
                .filter(|name| pypiron::sidecar::is_artifact(name))
                .filter(|name| {
                    !dump.contains_key(&format!("{prefix}{name}.tombstone"))
                        && !dump.contains_key(&format!("{prefix}{name}.frozen"))
                        && !dump.contains_key(&format!("{prefix}{name}.mirror-quarantined"))
                })
                .map(str::to_string)
                .collect();
            let view = dump
                .get(&format!("simple/{pkg}/index.json"))
                .map(|b| String::from_utf8_lossy(b).into_owned());
            match (&view, live.is_empty()) {
                (None, true) => {}
                (None, false) => violations.push(format!(
                    "VIEW: bucket {bucket_idx} has live files for {pkg} but no view"
                )),
                (Some(_), true) => violations.push(format!(
                    "VIEW: bucket {bucket_idx} lists empty package {pkg} (view leads truth)"
                )),
                (Some(json), false) => {
                    for name in &live {
                        if !json.contains(name.as_str()) {
                            violations.push(format!(
                                "VIEW: bucket {bucket_idx} view of {pkg} misses live {name}"
                            ));
                        }
                    }
                    let global = dump
                        .get("simple/index.json")
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default();
                    if !global.contains(pkg) {
                        violations.push(format!(
                            "GLOBAL: bucket {bucket_idx} global index misses live {pkg}"
                        ));
                    }
                }
            }
        }

        // Conservation: acked bytes must exist somewhere unless an authorized
        // delete or freeze removed them. The tombstone is the point of no
        // return, so storage evidence (a tombstone/freeze marker on any
        // bucket) exempts — an interrupted delete that crashed after its
        // tombstone but before its 204 has still legitimately destroyed.
        for ((pkg, fname), body) in &ledger.acked {
            let akey = format!("packages/{pkg}/{fname}");
            let removal_authorized = ledger.deleted.contains(&(pkg.clone(), fname.clone()))
                || dumps.iter().any(|d| {
                    d.contains_key(&format!("{akey}.tombstone"))
                        || d.contains_key(&format!("{akey}.frozen"))
                });
            if removal_authorized {
                continue;
            }
            let survives = dump.values().any(|stored| stored == body);
            if !survives && bucket_idx == 0 {
                // Checked once (bucket 0): quarantine may hold it on either
                // bucket, so scan them all before declaring loss.
                let anywhere = dumps
                    .iter()
                    .any(|d| d.values().any(|stored| stored == body));
                if !anywhere {
                    violations.push(format!(
                        "CONSERVATION: acked bytes of packages/{pkg}/{fname} vanished from every bucket"
                    ));
                }
            }
        }
    }

    let (trace_hash, trace_events) = fleet.plan.trace_hash();
    RunOutcome {
        trace_hash,
        trace_events,
        trace_log: fleet.plan.take_log(),
        acked: ledger.acked.len(),
        audit_view_repairs,
        violations,
    }
}

// ---------------------------------------------------------------------------
// Entry point.
// ---------------------------------------------------------------------------

struct Args {
    seeds: u64,
    start_seed: u64,
    nodes: usize,
    buckets: usize,
    ops: u64,
    recheck_every: u64,
    fail_percent: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        seeds: 25,
        start_seed: 1,
        nodes: 2,
        buckets: 2,
        ops: 120,
        recheck_every: 10,
        fail_percent: 3,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut grab = || {
            it.next()
                .unwrap_or_else(|| panic!("missing value for {flag}"))
                .parse::<u64>()
                .unwrap_or_else(|e| panic!("bad value for {flag}: {e}"))
        };
        match flag.as_str() {
            "--seeds" => args.seeds = grab(),
            "--seed" => {
                args.start_seed = grab();
                args.seeds = 1;
            }
            "--start-seed" => args.start_seed = grab(),
            "--nodes" => args.nodes = grab() as usize,
            "--buckets" => args.buckets = grab() as usize,
            "--ops" => args.ops = grab(),
            "--recheck-every" => args.recheck_every = grab(),
            "--fail-percent" => args.fail_percent = grab(),
            "--verbose" => {}
            other => panic!("unknown flag {other} (see examples/vopr.rs)"),
        }
    }
    args
}

fn dump_trace(outcome: &RunOutcome) {
    if let (Some(path), Some(log)) = (std::env::var_os("VOPR_TRACE_FILE"), &outcome.trace_log) {
        let _ = std::fs::write(path, log.join("\n"));
    }
}

fn run_once(seed: u64, args: &Args) -> RunOutcome {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("build paused runtime");
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(run_seed(
        seed,
        args.nodes,
        args.buckets,
        args.ops,
        args.fail_percent,
    )))
}

fn main() {
    if std::env::args().any(|a| a == "--verbose") {
        tracing_subscriber::fmt()
            .with_env_filter(
                std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,pypiron=warn".into()),
            )
            .init();
    }
    let args = parse_args();
    let started = std::time::Instant::now();
    let mut total_events: u64 = 0;
    let mut total_acked: usize = 0;
    let mut total_audit_repairs: u64 = 0;
    for i in 0..args.seeds {
        let seed = args.start_seed + i;
        let outcome = run_once(seed, &args);
        dump_trace(&outcome);
        total_events += outcome.trace_events;
        total_acked += outcome.acked;
        total_audit_repairs += outcome.audit_view_repairs;
        if args.recheck_every > 0 && seed.is_multiple_of(args.recheck_every) {
            let again = run_once(seed, &args);
            if again.trace_hash != outcome.trace_hash {
                eprintln!(
                    "vopr: DETERMINISM VIOLATION seed={seed}: trace {:#x} vs {:#x}",
                    outcome.trace_hash, again.trace_hash
                );
                if let (Some(a), Some(b)) = (&outcome.trace_log, &again.trace_log) {
                    let split = a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count();
                    eprintln!(
                        "first divergence at event {split} of {}/{}",
                        a.len(),
                        b.len()
                    );
                    for i in split.saturating_sub(3)..(split + 4).min(a.len().min(b.len())) {
                        eprintln!("  [{i}] A: {}\n  [{i}] B: {}", a[i], b[i]);
                    }
                } else {
                    eprintln!("rerun with VOPR_TRACE=1 to diff the traces");
                }
                std::process::exit(3);
            }
        }
        if !outcome.violations.is_empty() {
            eprintln!(
                "vopr: seed {seed} FAILED ({} violations):",
                outcome.violations.len()
            );
            for violation in &outcome.violations {
                eprintln!("  {violation}");
            }
            eprintln!("reproduce: cargo run --release --example vopr -- --seed {seed} --nodes {} --buckets {} --ops {}", args.nodes, args.buckets, args.ops);
            std::process::exit(2);
        }
    }
    println!(
        "vopr: {} seeds explored, {} storage-op interleavings, {} acked uploads verified, {} audit view repairs, nodes={} buckets={} ops/run={} in {:?} — all invariants held",
        args.seeds, total_events, total_acked, total_audit_repairs, args.nodes, args.buckets,
        args.ops, started.elapsed()
    );
}
