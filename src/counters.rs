//! Distributed, S3-backed counter store. **Self-contained on purpose**: it
//! depends only on the two tiny store traits below, `time`, and `serde` — never
//! on `AppState`, `html`, or `crate::storage`. Lift it into its own crate by
//! copying this one file and providing store implementations for your backend.
//!
//! ## Model (truth = immutable files, views = recomputations — the repo's bias)
//! - **Record** (every node, hot path): bump a bounded in-memory map keyed by
//!   `(metric, UTC day, shard, intra-day bucket, key)`. No I/O.
//! - **Flush** (every node): write the buffered *deltas* as one immutable,
//!   uniquely-named segment per `(metric, day, shard)`, then clear. Plain PUT,
//!   no read-before-write, no CAS — segments never collide (unique incarnation
//!   id + sequence), so summing all of a `(day, shard)`'s segments is the total.
//! - **Compact** (leader only): once a day is safely past (`grace`), sum each
//!   shard's segments into one frozen `day/<day>/<shard>.json`, then delete the
//!   segments. A frozen file **always wins** over the segment dir at read time,
//!   so a crash mid-compaction can neither double-count nor shrink a total.
//!   Retention then deletes frozen days older than the window.
//! - **Query**: per day, prefer the frozen shard file; else sum the open day's
//!   segments. Filter to one key-prefix (a package) for cheap per-package reads.
//!
//! Sharding mirrors the package tree's first-character fan-out (`0-9a-z`, plus a
//! `_` catch-all), so a package's counters live in one shard and the leader can
//! compact shards in parallel. Cost and object-count scale with *days*, not with
//! resolution or download volume; only key *cardinality* (distinct keys/day)
//! grows the per-shard files, which is why the in-memory map is hard-capped.
//!
//! ## Fleet shape (the two top-level prefixes)
//! The key space splits the moment-of-write role above the metric so a
//! multi-bucket fleet can classify a key by a static prefix (`src/layout.rs`):
//! - `_counters/day/…` — the **rollup**: frozen per-shard day totals and their
//!   summaries. Leader-authored, immutable once written, and the durable truth a
//!   dashboard/audit reads. The engine offers [`Counters::reseed_rollups`] so an
//!   embedder can mirror this subtree to every bucket (copy-if-absent), keeping a
//!   bucket failover from zeroing history. The engine itself stays single-store.
//! - `_counters/seg/…` — the **live tallies**: per-node open-day delta segments,
//!   not yet rolled up. These are never mirrored; losing a bucket loses at most
//!   its share of the current day (the declared, bounded loss window). To keep an
//!   open day whole across a mid-day selection change, a query sums its segments
//!   across every bucket [`ObjectStoreSelector::reachable_peers`] adds, best-effort.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime};
use tracing::warn;

/// Everything this store writes lives under one top-level prefix, excluded from
/// index rebuilds like every other `_`-prefixed key. See dev/DESIGN.md.
pub const PREFIX: &str = "_counters/";

/// The rollup subtree: frozen per-shard day totals + summaries. Leader-authored,
/// immutable, the replicated truth a failover bucket must hold. See
/// [`Counters::reseed_rollups`] and the layout manifest.
pub const DAY_PREFIX: &str = "_counters/day/";

/// The live-tally subtree: per-node open-day delta segments, never replicated —
/// their loss is the declared ≤1-day window. See the layout manifest.
pub const SEG_PREFIX: &str = "_counters/seg/";

/// In-memory keys past the cap fold into this catch-all so a flood of distinct
/// (or hostile) keys can never grow a node's memory without bound.
pub(crate) const OVERFLOW_KEY: &str = "_overflow";

const SUMMARY_FILE: &str = "_summary.json";

/// Select one stable store handle for a complete flush, compaction, or query.
/// The engine calls this exactly once at each public operation boundary and
/// threads the returned handle through the whole call graph.
pub trait ObjectStoreSelector: Send + Sync {
    fn pin(&self) -> Box<dyn ObjectStore>;

    /// The *other* reachable stores (excluding the [`pin`](Self::pin)ned primary)
    /// a best-effort read of the *current* day's un-rolled-up live segments should
    /// also sum, so an open day stays whole when a mid-day selection change split
    /// its segments across buckets. Default: none — a single-bucket read uses only
    /// the pinned store and is byte-for-byte unchanged. A multi-bucket embedder
    /// returns the eligible peer buckets; a store that is down is simply absent,
    /// and its share of the current day is the declared loss. Never consulted for
    /// rolled-up (`day/`) history — that is replicated truth, read from the pin.
    fn reachable_peers(&self) -> Vec<Box<dyn ObjectStore>> {
        Vec::new()
    }
}

/// The minimal object-store surface used after an operation is pinned. Map
/// these onto any backend (the pypiron adapter wraps `crate::storage::Storage`).
/// `get` returns `Ok(None)` for a genuinely-absent object and `Err` only for a
/// *transient* failure — the engine relies on that distinction to never freeze a
/// day from a failed read.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>>;
    async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()>;
    /// Every key under `prefix`, recursively.
    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>>;
    /// Best-effort delete; missing keys are not an error.
    async fn delete(&self, keys: &[String]) -> anyhow::Result<()>;
}

/// Tunables. `resolution_secs` is the intra-day bucket width; it must be a
/// whole number of minutes that divides a day (validated by [`Config::checked`]).
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub resolution_secs: u32,
    pub flush_interval: Duration,
    pub rollup_interval: Duration,
    pub retention_days: i64,
    /// Days a finished day waits before it is compacted+frozen, covering clock
    /// skew, stragglers, and in-flight requests. A day `D` closes once today is
    /// `> D + grace_days`.
    pub grace_days: i64,
    /// Hard cap on distinct in-memory keys before new ones fold into
    /// [`OVERFLOW_KEY`]. Bounds per-node memory regardless of cardinality.
    pub max_keys: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            resolution_secs: 86_400,
            flush_interval: Duration::from_secs(300),
            rollup_interval: Duration::from_secs(3600),
            retention_days: 90,
            grace_days: 1,
            max_keys: 500_000,
        }
    }
}

impl Config {
    /// Validate the operator-facing knobs and clamp the internal cap to
    /// something sane. Returns an error string so the caller can fail closed at
    /// startup rather than silently coercing a typo'd `0` (e.g. a `0` retention
    /// would prune all history on the next compaction).
    pub fn checked(self) -> Result<Self, String> {
        let r = self.resolution_secs;
        if !(60..=86_400).contains(&r) || !r.is_multiple_of(60) || !86_400u32.is_multiple_of(r) {
            return Err(format!(
                "resolution must be a whole number of minutes dividing a day (60..=86400 s), got {r}s"
            ));
        }
        if self.flush_interval.is_zero() {
            return Err("flush-interval must be at least 1 second".into());
        }
        if self.rollup_interval.is_zero() {
            return Err("rollup-interval must be at least 1 second".into());
        }
        if self.retention_days < 1 {
            return Err(format!(
                "retention-days must be at least 1, got {}",
                self.retention_days
            ));
        }
        Ok(Self {
            max_keys: self.max_keys.max(1_000),
            ..self
        })
    }
}

/// `bucket(HH:MM) -> key -> count`. `BTreeMap` for deterministic bytes, so a
/// recompute of the same inputs yields the identical object (idempotent freeze).
type BucketMap = BTreeMap<String, BTreeMap<String, u64>>;

/// The on-disk shape of both a flushed segment and a frozen day-shard file.
#[derive(Serialize, Deserialize, Default)]
struct Segment {
    /// Resolution the buckets were written at — recorded so a later resolution
    /// change is non-destructive (old files keep their granularity).
    #[serde(default)]
    resolution_secs: u32,
    #[serde(default)]
    buckets: BucketMap,
}

/// A compacted day's headline view (one tiny object per day): grand total plus
/// the busiest keys, so a dashboard never has to read the whole registry.
#[derive(Serialize, Deserialize, Default)]
pub struct DaySummary {
    pub total: u64,
    /// `key -> count`, the top-N by count.
    pub top: BTreeMap<String, u64>,
}

#[derive(Default)]
struct Pending {
    segs: BTreeMap<(String, String, char), BucketMap>,
    n_keys: usize,
}

/// The store. Construct enabled with [`Counters::new`], or [`Counters::disabled`]
/// for a no-op instance (single-node tests, `--download-stats=false`).
pub struct Counters {
    store: Option<Box<dyn ObjectStoreSelector>>,
    cfg: Config,
    /// Unique per process incarnation (`pid-nanos`), so two nodes — even two
    /// sharing a hostname — never write the same segment key.
    incarnation: String,
    seq: AtomicU64,
    pending: Mutex<Pending>,
    flush_wake: tokio::sync::Notify,
    flush_due: AtomicBool,
}

impl Counters {
    pub fn new(store: Box<dyn ObjectStoreSelector>, cfg: Config) -> Self {
        Self {
            store: Some(store),
            cfg,
            incarnation: incarnation_id(),
            seq: AtomicU64::new(0),
            pending: Mutex::new(Pending::default()),
            flush_wake: tokio::sync::Notify::new(),
            flush_due: AtomicBool::new(false),
        }
    }

    /// A no-op store: `record`/`flush`/`compact` do nothing and `query` is empty.
    pub fn disabled() -> Self {
        Self {
            store: None,
            cfg: Config::default(),
            incarnation: incarnation_id(),
            seq: AtomicU64::new(0),
            pending: Mutex::new(Pending::default()),
            flush_wake: tokio::sync::Notify::new(),
            flush_due: AtomicBool::new(false),
        }
    }

    pub fn enabled(&self) -> bool {
        self.store.is_some()
    }
    pub fn flush_interval(&self) -> Duration {
        self.cfg.flush_interval
    }
    pub fn rollup_interval(&self) -> Duration {
        self.cfg.rollup_interval
    }
    /// True when a memory high-water mark was crossed since the last flush — the
    /// worker uses it (with [`Counters::flush_signal`]) to flush early under load.
    pub fn flush_due(&self) -> bool {
        self.flush_due.load(Ordering::Relaxed)
    }
    /// Resolves the next time the in-memory buffer crosses its high-water mark.
    pub async fn flush_signal(&self) {
        self.flush_wake.notified().await;
    }

    /// Count one event against `(metric, key)` at the current instant. Hot path:
    /// a couple of map lookups under a short mutex, no I/O, never blocks.
    pub fn record(&self, metric: &str, key: &str) {
        self.record_n(metric, key, 1);
    }

    pub(crate) fn record_n(&self, metric: &str, key: &str, n: u64) {
        if self.store.is_none() || n == 0 {
            return;
        }
        let now = OffsetDateTime::now_utc();
        let (day, bucket) = day_and_bucket(now, self.cfg.resolution_secs);
        let shard = shard_of(key);

        let over = {
            let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            let Pending { segs, n_keys } = &mut *guard;
            let leaf = segs
                .entry((metric.to_string(), day, shard))
                .or_default()
                .entry(bucket)
                .or_default();
            if let Some(c) = leaf.get_mut(key) {
                *c += n;
            } else if *n_keys >= self.cfg.max_keys {
                *leaf.entry(OVERFLOW_KEY.to_string()).or_insert(0) += n;
            } else {
                leaf.insert(key.to_string(), n);
                *n_keys += 1;
            }
            *n_keys >= self.cfg.max_keys.saturating_mul(8) / 10
        };
        if over && !self.flush_due.swap(true, Ordering::Relaxed) {
            self.flush_wake.notify_one();
        }
    }

    /// Write the buffered deltas as immutable segments, then clear the buffer.
    /// Best-effort: a failed segment is re-buffered for the next flush.
    pub async fn flush(&self) {
        let Some(selector) = self.store.as_deref() else {
            return;
        };
        let store = selector.pin();
        let taken = {
            let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            self.flush_due.store(false, Ordering::Relaxed);
            std::mem::take(&mut *guard)
        };
        for ((metric, day, shard), buckets) in taken.segs {
            if buckets.is_empty() {
                continue;
            }
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let key = format!(
                "{SEG_PREFIX}{metric}/{day}/{shard}/{}-{seq}.json",
                self.incarnation
            );
            let seg = Segment {
                resolution_secs: self.cfg.resolution_secs,
                buckets,
            };
            let bytes = serde_json::to_vec(&seg).unwrap_or_default();
            if let Err(e) = store.put(&key, bytes).await {
                warn!(error=?e, %key, "counter flush failed; re-buffering deltas");
                self.rebuffer(&metric, &day, shard, seg.buckets);
            }
        }
    }

    fn rebuffer(&self, metric: &str, day: &str, shard: char, buckets: BucketMap) {
        let mut guard = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let Pending { segs, n_keys } = &mut *guard;
        let dest = segs
            .entry((metric.to_string(), day.to_string(), shard))
            .or_default();
        for (bucket, keys) in buckets {
            let leaf = dest.entry(bucket).or_default();
            for (k, v) in keys {
                if !leaf.contains_key(&k) {
                    *n_keys += 1;
                }
                *leaf.entry(k).or_insert(0) += v;
            }
        }
    }

    /// Leader-only: freeze every closeable `(metric, day, shard)`, write per-day
    /// summaries, and apply retention. Idempotent and crash-safe (recompute from
    /// immutable segments; frozen file is the sentinel; deletes are best-effort).
    pub async fn compact(&self) {
        let Some(selector) = self.store.as_deref() else {
            return;
        };
        let store = selector.pin();
        let keys = match store.list(PREFIX).await {
            Ok(k) => k,
            Err(e) => {
                warn!(error=?e, "counter compaction: list failed; will retry");
                return;
            }
        };
        let today = OffsetDateTime::now_utc().date();
        let close_cutoff = day_str(today.saturating_sub(time::Duration::days(self.cfg.grace_days)));
        let retain_cutoff =
            day_str(today.saturating_sub(time::Duration::days(self.cfg.retention_days)));

        let layout = Layout::parse(&keys);

        // Freeze closeable day-shards; collect each frozen day for its summary.
        let mut to_summarize: BTreeMap<(String, String), ()> = BTreeMap::new();
        for ((metric, day, shard), seg_keys) in &layout.segments {
            if day >= &close_cutoff {
                continue; // still open (or within grace)
            }
            if layout
                .frozen
                .contains(&(metric.clone(), day.clone(), *shard))
            {
                // Already frozen: a crash left stragglers — sweep, never recompute.
                let _ = store.delete(seg_keys).await;
                continue;
            }
            match sum_segments(store.as_ref(), seg_keys).await {
                Some(buckets) => {
                    let frozen_key = format!("{DAY_PREFIX}{metric}/{day}/{shard}.json");
                    let seg = Segment {
                        resolution_secs: self.cfg.resolution_secs,
                        buckets,
                    };
                    let bytes = serde_json::to_vec(&seg).unwrap_or_default();
                    if store.put(&frozen_key, bytes).await.is_ok() {
                        let _ = store.delete(seg_keys).await;
                        to_summarize.insert((metric.clone(), day.clone()), ());
                    }
                }
                None => {
                    // Transient read error mid-day — skip; next cycle retries.
                    // Never freeze from a partial read.
                }
            }
        }

        // Backfill any already-frozen day still missing its _summary.json: a
        // prior cycle's best-effort write_summary failed transiently. Without
        // this it never retries — a frozen day with swept segments never
        // re-enters the loop above — and the global dashboard undercounts it
        // forever. Recomputing from the surviving frozen shard files is
        // idempotent, so skip days that already have a summary (no churn).
        let have_summary: std::collections::HashSet<(&str, &str)> = keys
            .iter()
            .filter_map(|k| match key_parts(k)?[..] {
                ["day", metric, day, file] if file == SUMMARY_FILE => Some((metric, day)),
                _ => None,
            })
            .collect();
        for (metric, day, _shard) in &layout.frozen {
            if day.as_str() >= retain_cutoff.as_str()
                && !have_summary.contains(&(metric.as_str(), day.as_str()))
            {
                to_summarize.insert((metric.clone(), day.clone()), ());
            }
        }

        // Recompute each pending day's summary from its frozen shard files.
        for (metric, day) in to_summarize.into_keys() {
            self.write_summary(store.as_ref(), &metric, &day).await;
        }

        // Retention: drop frozen days (and any leftover segments) past the window.
        let mut stale: Vec<String> = Vec::new();
        for k in &keys {
            if let Some(day) = layout.day_of(k) {
                if day < retain_cutoff.as_str() {
                    stale.push(k.clone());
                }
            }
        }
        if !stale.is_empty() {
            let _ = store.delete(&stale).await;
        }
    }

    async fn write_summary(&self, store: &dyn ObjectStore, metric: &str, day: &str) {
        let prefix = format!("{DAY_PREFIX}{metric}/{day}/");
        let keys = match store.list(&prefix).await {
            Ok(k) => k,
            Err(_) => return,
        };
        let mut totals: BTreeMap<String, u64> = BTreeMap::new();
        let mut total: u64 = 0;
        for k in &keys {
            if k.ends_with(SUMMARY_FILE) {
                continue;
            }
            let Ok(Some(bytes)) = store.get(k).await else {
                return; // transient: skip writing a partial summary
            };
            let seg: Segment = serde_json::from_slice(&bytes).unwrap_or_default();
            fold_buckets(&seg.buckets, &mut totals, &mut total);
        }
        let summary = rank_summary(totals, total);
        let key = format!("{prefix}{SUMMARY_FILE}");
        let _ = store
            .put(&key, serde_json::to_vec(&summary).unwrap_or_default())
            .await;
    }

    /// Per-package daily series: `day -> sub-key -> count`, where `sub-key` is
    /// `key` with the `"<pkg>/"` prefix stripped (a filename, for downloads).
    /// Reads only the package's shard, preferring the frozen file per day.
    pub async fn query_package(
        &self,
        metric: &str,
        pkg: &str,
        from: Date,
        to: Date,
    ) -> BTreeMap<String, BTreeMap<String, u64>> {
        let mut out = BTreeMap::new();
        let Some(selector) = self.store.as_deref() else {
            return out;
        };
        let store = selector.pin();
        let peers = selector.reachable_peers();
        let shard = shard_of(pkg);
        let prefix = format!("{pkg}/");
        let mut day = from;
        loop {
            let ds = day_str(day);
            if let Some(buckets) = self
                .read_day_shard(store.as_ref(), &peers, metric, &ds, shard)
                .await
            {
                let mut per_key: BTreeMap<String, u64> = BTreeMap::new();
                for keys_at in buckets.values() {
                    for (key, c) in keys_at {
                        if let Some(sub) = key.strip_prefix(&prefix) {
                            *per_key.entry(sub.to_string()).or_insert(0) += c;
                        }
                    }
                }
                if !per_key.is_empty() {
                    out.insert(ds, per_key);
                }
            }
            if day >= to {
                break;
            }
            day = match day.next_day() {
                Some(d) => d,
                None => break,
            };
        }
        out
    }

    /// Recent per-day summaries: `day -> DaySummary`, for a dashboard's
    /// totals/top-N. A frozen `_summary.json` is one tiny GET per day; a day that
    /// isn't frozen yet (today and anything within `grace_days`) has no summary,
    /// so it is aggregated live across shards on read — that way the global view
    /// is never days behind the per-package one (which already reads open-day
    /// segments). Older days with no summary are genuinely empty (or
    /// retention-pruned), so they cost nothing beyond the missing-summary GET.
    pub async fn query_summaries(
        &self,
        metric: &str,
        from: Date,
        to: Date,
    ) -> BTreeMap<String, DaySummary> {
        let mut out = BTreeMap::new();
        let Some(selector) = self.store.as_deref() else {
            return out;
        };
        let store = selector.pin();
        // Peer buckets summed only for the open day's live segments (below); the
        // rolled-up history is replicated, so a frozen day reads from the pin.
        let peers = selector.reachable_peers();
        // Mirror of `compact`'s freeze gate: a day at or after this cutoff cannot
        // be frozen yet, so its absent summary means "still open", not "empty" —
        // those are the only days worth a live cross-shard scan.
        let close_cutoff = day_str(
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(self.cfg.grace_days)),
        );
        let mut day = from;
        loop {
            let ds = day_str(day);
            let key = format!("{DAY_PREFIX}{metric}/{ds}/{SUMMARY_FILE}");
            match store.get(&key).await {
                Ok(Some(bytes)) => {
                    if let Ok(s) = serde_json::from_slice::<DaySummary>(&bytes) {
                        out.insert(ds, s); // frozen summary wins, always
                    }
                }
                Ok(None) if ds >= close_cutoff => {
                    if let Some(s) = self
                        .summarize_open_day(store.as_ref(), &peers, metric, &ds)
                        .await
                    {
                        out.insert(ds, s);
                    }
                }
                Ok(None) => {} // closed day, no summary => no data: skip the scan
                Err(_) => {}   // transient: skip; the next refresh retries
            }
            if day >= to {
                break;
            }
            day = match day.next_day() {
                Some(d) => d,
                None => break,
            };
        }
        out
    }

    /// Build a [`DaySummary`] for `day` from live state — the same shape
    /// [`Counters::write_summary`] freezes, but computed on read. The caller only
    /// reaches here for an *open* day (`compact` never froze it), so instead of
    /// probing all shards this lists the day's two prefixes once, still letting a
    /// frozen shard file that raced in win over that shard's open segments. Open
    /// segments are summed across the pinned `primary` and every `peer` bucket, so
    /// a day split over buckets by a mid-day selection change stays whole; a peer
    /// that is down contributes nothing (the declared loss). `None` when no shard
    /// on any reachable bucket has data for the day.
    async fn summarize_open_day(
        &self,
        primary: &dyn ObjectStore,
        peers: &[Box<dyn ObjectStore>],
        metric: &str,
        day: &str,
    ) -> Option<DaySummary> {
        let mut totals: BTreeMap<String, u64> = BTreeMap::new();
        let mut total: u64 = 0;
        let mut any = false;

        // Frozen shard files are near-always absent for an open day, but honor
        // any that raced in — a leader ahead of this clock-behind querier whose
        // best-effort summary write failed — so it still wins over stragglers.
        // Replicated, so the primary's copy stands in for the whole fleet; one
        // prefix LIST replaces the 37 blind per-shard frozen GETs.
        let mut frozen_shards: std::collections::HashSet<char> = std::collections::HashSet::new();
        let day_prefix = format!("{DAY_PREFIX}{metric}/{day}/");
        for k in primary.list(&day_prefix).await.unwrap_or_default() {
            let Some(shard) = frozen_shard_of(&k) else {
                continue; // _summary.json or a stray key
            };
            if let Ok(Some(bytes)) = primary.get(&k).await {
                let seg: Segment = serde_json::from_slice(&bytes).unwrap_or_default();
                fold_buckets(&seg.buckets, &mut totals, &mut total);
                frozen_shards.insert(shard);
                any = true;
            }
        }

        // Open segments for every not-yet-frozen shard, summed in one pass per
        // reachable bucket. A per-bucket read failure drops only that bucket's
        // share of the open day (the declared loss window), never the whole day.
        // Each segment key lives on exactly one bucket (segments are never
        // replicated), so deduping by key makes the sum robust to the same bucket
        // appearing twice — a selection switch racing between the pin and the peer
        // enumeration — which would otherwise double-count.
        let seg_prefix = format!("{SEG_PREFIX}{metric}/{day}/");
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for store in std::iter::once(primary).chain(peers.iter().map(|p| p.as_ref())) {
            let listed = match store.list(&seg_prefix).await {
                Ok(l) => l,
                Err(_) => continue,
            };
            let open: Vec<String> = listed
                .into_iter()
                .filter(|k| seg_shard_of(k).is_some_and(|s| !frozen_shards.contains(&s)))
                .filter(|k| seen.insert(k.clone()))
                .collect();
            if open.is_empty() {
                continue;
            }
            if let Some(buckets) = sum_segments(store, &open).await {
                fold_buckets(&buckets, &mut totals, &mut total);
                any = true;
            }
        }

        any.then(|| rank_summary(totals, total))
    }

    /// Frozen file wins; otherwise sum the open day's live segments. A rolled-up
    /// (frozen) shard is replicated, so the `primary` pin holds it and stands in
    /// for the fleet. An open day's segments are summed across `primary` and every
    /// `peer` bucket so a day split by a mid-day selection change stays whole; a
    /// down peer contributes nothing (the declared loss). `None` means no data for
    /// that day-shard on any reachable bucket.
    async fn read_day_shard(
        &self,
        primary: &dyn ObjectStore,
        peers: &[Box<dyn ObjectStore>],
        metric: &str,
        day: &str,
        shard: char,
    ) -> Option<BucketMap> {
        let frozen_key = format!("{DAY_PREFIX}{metric}/{day}/{shard}.json");
        if let Ok(Some(bytes)) = primary.get(&frozen_key).await {
            let seg: Segment = serde_json::from_slice(&bytes).unwrap_or_default();
            return Some(seg.buckets);
        }
        let seg_prefix = format!("{SEG_PREFIX}{metric}/{day}/{shard}/");
        let mut acc: BucketMap = BTreeMap::new();
        let mut any = false;
        // Dedup keys across buckets: each segment lives on exactly one bucket, so a
        // key seen twice is the same bucket read twice (a selection switch racing
        // the pin/peer enumeration) — sum it once, never double.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for store in std::iter::once(primary).chain(peers.iter().map(|p| p.as_ref())) {
            let seg_keys = match store.list(&seg_prefix).await {
                Ok(k) => k,
                Err(_) => continue,
            };
            let fresh: Vec<String> = seg_keys
                .into_iter()
                .filter(|k| seen.insert(k.clone()))
                .collect();
            if fresh.is_empty() {
                continue;
            }
            if let Some(buckets) = sum_segments(store, &fresh).await {
                merge_bucketmap(&mut acc, buckets);
                any = true;
            }
        }
        any.then_some(acc)
    }

    /// Leader-only, multi-bucket: mirror every rolled-up counter file (the frozen
    /// per-shard day totals and their summaries under `_counters/day/`) present on
    /// the pinned (write) bucket onto each `peer` that lacks it. Copy-if-absent —
    /// a frozen rollup is immutable, so a peer that already holds the key holds the
    /// right bytes (newest-wins is trivially satisfied). This is both the
    /// write-through for a freshly-frozen day and the backstop for a peer that was
    /// down when it froze or joined the fleet later, so a failover to any bucket
    /// finds the whole history. The live `_counters/seg/` tallies are never copied
    /// (the declared ≤1-day loss window). A no-op when `peers` is empty
    /// (single-bucket) or the store is disabled. Best-effort per peer: one
    /// unreachable peer never blocks healing the others; the next cycle retries.
    pub async fn reseed_rollups(&self, peers: &[Box<dyn ObjectStore>]) {
        if peers.is_empty() {
            return;
        }
        let Some(selector) = self.store.as_deref() else {
            return;
        };
        let primary = selector.pin();
        let rollups = match primary.list(DAY_PREFIX).await {
            Ok(keys) => keys,
            Err(e) => {
                warn!(error=?e, "counter rollup reseed: listing the primary failed; retries next cycle");
                return;
            }
        };
        for peer in peers {
            let present: std::collections::HashSet<String> = match peer.list(DAY_PREFIX).await {
                Ok(keys) => keys.into_iter().collect(),
                Err(e) => {
                    warn!(error=?e, "counter rollup reseed: listing a peer failed; retries next cycle");
                    continue;
                }
            };
            for key in &rollups {
                if present.contains(key) {
                    continue; // immutable rollup already mirrored
                }
                match primary.get(key).await {
                    Ok(Some(bytes)) => {
                        if let Err(e) = peer.put(key, bytes).await {
                            warn!(error=?e, %key, "counter rollup reseed: writing a peer failed; retries next cycle");
                        }
                    }
                    // Listed then vanished (raced retention): nothing to copy.
                    Ok(None) => {}
                    Err(e) => {
                        warn!(error=?e, %key, "counter rollup reseed: reading the primary failed; retries next cycle");
                    }
                }
            }
        }
    }
}

/// Fold one storage bucket's [`BucketMap`] into a cross-bucket accumulator,
/// summing the counts of any `(time-bucket, key)` the two share.
fn merge_bucketmap(acc: &mut BucketMap, src: BucketMap) {
    for (bucket, keys) in src {
        let dest = acc.entry(bucket).or_default();
        for (key, c) in keys {
            *dest.entry(key).or_insert(0) += c;
        }
    }
}

/// Flatten a shard's bucket map into a running per-key `totals` and grand
/// `total`, dropping the (time-)bucket dimension — the shape a day summary needs.
fn fold_buckets(buckets: &BucketMap, totals: &mut BTreeMap<String, u64>, total: &mut u64) {
    for keys_at in buckets.values() {
        for (key, c) in keys_at {
            *totals.entry(key.clone()).or_insert(0) += c;
            *total += c;
        }
    }
}

/// Split a `_counters/` key into its `/`-separated components past `PREFIX`, or
/// `None` when the key is not under the prefix.
fn key_parts(key: &str) -> Option<Vec<&str>> {
    Some(key.strip_prefix(PREFIX)?.split('/').collect())
}

/// The shard of a frozen day-shard key `day/<metric>/<day>/<shard>.json`.
/// `None` for the day's `_summary.json` or any non-shard key — so the `_`
/// catch-all shard is never confused with the `_summary` file's leading `_`.
fn frozen_shard_of(key: &str) -> Option<char> {
    match key_parts(key)?[..] {
        ["day", _metric, _day, file] if file != SUMMARY_FILE => {
            file.strip_suffix(".json").and_then(first_char)
        }
        _ => None,
    }
}

/// The shard of an open segment key `seg/<metric>/<day>/<shard>/<file>`.
fn seg_shard_of(key: &str) -> Option<char> {
    match key_parts(key)?[..] {
        ["seg", _metric, _day, shard, _file] => first_char(shard),
        _ => None,
    }
}

/// Sum a set of segment objects into one [`BucketMap`]. Returns `None` on any
/// transient read failure, so a caller never acts on a partial view.
async fn sum_segments(store: &dyn ObjectStore, seg_keys: &[String]) -> Option<BucketMap> {
    let mut acc: BucketMap = BTreeMap::new();
    for k in seg_keys {
        match store.get(k).await {
            Ok(Some(bytes)) => {
                let seg: Segment = serde_json::from_slice(&bytes).unwrap_or_default();
                for (bucket, keys) in seg.buckets {
                    let dest = acc.entry(bucket).or_default();
                    for (key, c) in keys {
                        *dest.entry(key).or_insert(0) += c;
                    }
                }
            }
            Ok(None) => {} // listed then vanished (raced a delete): treat as 0
            Err(_) => return None,
        }
    }
    Some(acc)
}

/// Parsed view of the `_counters/` key space for one compaction pass.
struct Layout {
    /// `(metric, day, shard) -> segment keys`.
    segments: BTreeMap<(String, String, char), Vec<String>>,
    /// `(metric, day, shard)` that already have a frozen file.
    frozen: std::collections::HashSet<(String, String, char)>,
}

impl Layout {
    fn parse(keys: &[String]) -> Self {
        let mut segments: BTreeMap<(String, String, char), Vec<String>> = BTreeMap::new();
        let mut frozen = std::collections::HashSet::new();
        for k in keys {
            let Some(parts) = key_parts(k) else {
                continue;
            };
            // seg/<metric>/<day>/<shard>/<file>   |   day/<metric>/<day>/<shard>.json
            match parts.as_slice() {
                ["seg", metric, day, shard, _file] => {
                    if let Some(s) = first_char(shard) {
                        segments
                            .entry((metric.to_string(), day.to_string(), s))
                            .or_default()
                            .push(k.clone());
                    }
                }
                ["day", metric, day, file] if *file != SUMMARY_FILE => {
                    if let Some(s) = file.strip_suffix(".json").and_then(first_char) {
                        frozen.insert((metric.to_string(), day.to_string(), s));
                    }
                }
                _ => {}
            }
        }
        Self { segments, frozen }
    }

    /// The `<day>` component of any counter key, for retention.
    fn day_of<'a>(&self, key: &'a str) -> Option<&'a str> {
        let parts = key_parts(key)?;
        match parts.as_slice() {
            ["seg", _metric, day, _shard, _file] => Some(day),
            ["day", _metric, day, _file] => Some(day),
            _ => None,
        }
    }
}

fn first_char(s: &str) -> Option<char> {
    s.chars().next()
}

/// Shard a key by its first character (`0-9a-z`), folding anything else into a
/// single `_` shard. Matches the package tree's first-char fan-out.
fn shard_of(key: &str) -> char {
    match key.chars().next() {
        Some(c) if c.is_ascii_alphanumeric() => c.to_ascii_lowercase(),
        _ => '_',
    }
}

/// Total + top-50 (count desc, then key asc) — the on-disk [`DaySummary`] shape,
/// shared by the freeze path ([`Counters::write_summary`]) and the live read
/// fallback ([`Counters::summarize_day_live`]) so both produce identical bytes.
fn rank_summary(totals: BTreeMap<String, u64>, total: u64) -> DaySummary {
    let mut ranked: Vec<(String, u64)> = totals.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked.truncate(50);
    DaySummary {
        total,
        top: ranked.into_iter().collect(),
    }
}

/// `(YYYY-MM-DD, HH:MM)` for an instant, with the time floored to the bucket.
fn day_and_bucket(now: OffsetDateTime, resolution_secs: u32) -> (String, String) {
    let res_min = (resolution_secs / 60).max(1);
    let mins = now.hour() as u32 * 60 + now.minute() as u32;
    let floored = mins - (mins % res_min);
    (
        day_str(now.date()),
        format!("{:02}:{:02}", floored / 60, floored % 60),
    )
}

fn day_str(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day())
}

fn incarnation_id() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Default, Clone)]
    struct MemStore {
        objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    }
    impl MemStore {
        fn len(&self) -> usize {
            self.objects.lock().unwrap().len()
        }
    }
    impl ObjectStoreSelector for MemStore {
        fn pin(&self) -> Box<dyn ObjectStore> {
            Box::new(self.clone())
        }
    }
    #[async_trait]
    impl ObjectStore for MemStore {
        async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }
        async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
            Ok(())
        }
        async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
        async fn delete(&self, keys: &[String]) -> anyhow::Result<()> {
            let mut o = self.objects.lock().unwrap();
            for k in keys {
                o.remove(k);
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    struct SwitchingStore {
        stores: [MemStore; 2],
        pins: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ObjectStoreSelector for SwitchingStore {
        fn pin(&self) -> Box<dyn ObjectStore> {
            let index = self.pins.fetch_add(1, Ordering::SeqCst) % self.stores.len();
            Box::new(self.stores[index].clone())
        }
    }

    /// A multi-bucket fleet: a fixed primary plus peer buckets exposed through
    /// [`ObjectStoreSelector::reachable_peers`], so a query sums an open day's
    /// live segments across every bucket.
    #[derive(Clone)]
    struct FleetStore {
        primary: MemStore,
        peers: Vec<MemStore>,
    }
    impl ObjectStoreSelector for FleetStore {
        fn pin(&self) -> Box<dyn ObjectStore> {
            Box::new(self.primary.clone())
        }
        fn reachable_peers(&self) -> Vec<Box<dyn ObjectStore>> {
            self.peers
                .iter()
                .map(|p| Box::new(p.clone()) as Box<dyn ObjectStore>)
                .collect()
        }
    }

    /// One day-shard segment holding a single `key -> count` at bucket `00:00`.
    fn seg_bytes(key: &str, count: u64) -> Vec<u8> {
        serde_json::to_vec(&Segment {
            resolution_secs: 86_400,
            buckets: BTreeMap::from([(
                "00:00".to_string(),
                BTreeMap::from([(key.to_string(), count)]),
            )]),
        })
        .unwrap()
    }

    fn engine(store: MemStore, cfg: Config) -> Counters {
        Counters::new(Box::new(store), cfg)
    }

    #[test]
    fn config_rejects_bad_resolution() {
        assert!(Config {
            resolution_secs: 90,
            ..Default::default()
        }
        .checked()
        .is_err()); // not a whole minute
        assert!(Config {
            resolution_secs: 3600,
            ..Default::default()
        }
        .checked()
        .is_ok());
        assert!(Config {
            resolution_secs: 1800,
            ..Default::default()
        }
        .checked()
        .is_ok());
        assert!(Config {
            resolution_secs: 50,
            ..Default::default()
        }
        .checked()
        .is_err());
    }

    #[test]
    fn config_rejects_zero_intervals_and_retention() {
        // A typo'd 0 must fail closed, not silently coerce to 1 (a 0 retention
        // would prune every finished day on the next compaction).
        assert!(Config {
            flush_interval: Duration::ZERO,
            ..Default::default()
        }
        .checked()
        .is_err());
        assert!(Config {
            rollup_interval: Duration::ZERO,
            ..Default::default()
        }
        .checked()
        .is_err());
        for days in [0, -1] {
            assert!(Config {
                retention_days: days,
                ..Default::default()
            }
            .checked()
            .is_err());
        }
        assert!(Config::default().checked().is_ok());
    }

    #[test]
    fn buckets_floor_to_resolution() {
        let t = time::macros::datetime!(2026-06-20 14:37:12 UTC);
        assert_eq!(
            day_and_bucket(t, 86_400),
            ("2026-06-20".into(), "00:00".into())
        );
        assert_eq!(
            day_and_bucket(t, 3600),
            ("2026-06-20".into(), "14:00".into())
        );
        assert_eq!(
            day_and_bucket(t, 1800),
            ("2026-06-20".into(), "14:30".into())
        );
    }

    #[test]
    fn shards_by_first_char() {
        assert_eq!(shard_of("requests/x.whl"), 'r');
        assert_eq!(shard_of("Flask/x"), 'f');
        assert_eq!(shard_of("0/x"), '0');
        assert_eq!(shard_of("/weird"), '_');
    }

    #[tokio::test]
    async fn flush_writes_segments_summing_to_total() {
        let store = MemStore::default();
        let c = engine(store.clone(), Config::default());
        c.record("downloads", "requests/requests-2.31.0-py3-none-any.whl");
        c.record("downloads", "requests/requests-2.31.0-py3-none-any.whl");
        c.record("downloads", "flask/flask-3.0.0-py3-none-any.whl");
        c.flush().await;
        // Two shards touched ('r','f') => two segment objects.
        assert_eq!(store.len(), 2);
        // A second flush with new deltas writes new (unique) segments.
        c.record("downloads", "requests/requests-2.31.0-py3-none-any.whl");
        c.flush().await;
        assert_eq!(store.len(), 3);

        let today = OffsetDateTime::now_utc().date();
        let series = c.query_package("downloads", "requests", today, today).await;
        let day = day_str(today);
        assert_eq!(series[&day]["requests-2.31.0-py3-none-any.whl"], 3);
    }

    #[tokio::test]
    async fn overflow_bounds_memory() {
        let store = MemStore::default();
        let cfg = Config {
            max_keys: 4,
            ..Default::default()
        };
        let c = engine(store, cfg);
        for i in 0..100 {
            c.record("m", &format!("pkg/{i}.whl"));
        }
        let guard = c.pending.lock().unwrap();
        assert!(guard.n_keys <= 4, "distinct keys capped at max_keys");
        // The overflow bucket still accrues the dropped events.
        let has_overflow = guard
            .segs
            .values()
            .any(|bm| bm.values().any(|leaf| leaf.contains_key(OVERFLOW_KEY)));
        assert!(has_overflow);
    }

    #[tokio::test]
    async fn compaction_freezes_deletes_and_is_idempotent() {
        let store = MemStore::default();
        // grace_days 0 so "yesterday" is immediately closeable in the test.
        let cfg = Config {
            grace_days: 0,
            ..Default::default()
        };
        let c = engine(store.clone(), cfg);

        // Hand-place a segment for a day that is already in the past.
        let yest = day_str(
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(3)),
        );
        let seg = Segment {
            resolution_secs: 86_400,
            buckets: BTreeMap::from([(
                "00:00".to_string(),
                BTreeMap::from([("requests/r-1.0.whl".to_string(), 5u64)]),
            )]),
        };
        store
            .put(
                &format!("{SEG_PREFIX}downloads/{yest}/r/inc-0.json"),
                serde_json::to_vec(&seg).unwrap(),
            )
            .await
            .unwrap();

        c.compact().await;
        // Segment gone, frozen file written, summary written.
        let frozen = format!("{DAY_PREFIX}downloads/{yest}/r.json");
        assert!(store.objects.lock().unwrap().contains_key(&frozen));
        assert!(store
            .objects
            .lock()
            .unwrap()
            .contains_key(&format!("{DAY_PREFIX}downloads/{yest}/{SUMMARY_FILE}")));
        let remaining_segs = store
            .list(&format!("{SEG_PREFIX}downloads/{yest}/"))
            .await
            .unwrap();
        assert!(remaining_segs.is_empty(), "segments deleted after freeze");

        // Idempotent: re-running compaction changes nothing and never double-counts.
        let before = store.objects.lock().unwrap().clone();
        c.compact().await;
        assert_eq!(*store.objects.lock().unwrap(), before);

        // Query reads the frozen value.
        let from = OffsetDateTime::now_utc()
            .date()
            .saturating_sub(time::Duration::days(4));
        let to = OffsetDateTime::now_utc().date();
        let series = c.query_package("downloads", "requests", from, to).await;
        assert_eq!(series[&yest]["r-1.0.whl"], 5);

        // Summary reflects the total.
        let sums = c.query_summaries("downloads", from, to).await;
        assert_eq!(sums[&yest].total, 5);
    }

    #[tokio::test]
    async fn operations_pin_one_store_for_their_whole_call_graph() {
        let first = MemStore::default();
        let second = MemStore::default();
        let pins = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = Counters::new(
            Box::new(SwitchingStore {
                stores: [first.clone(), second.clone()],
                pins: pins.clone(),
            }),
            Config {
                grace_days: 0,
                ..Config::default()
            },
        );

        let old_day = day_str(
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(3)),
        );
        let segment_key = format!("{SEG_PREFIX}downloads/{old_day}/r/inc-0.json");
        first
            .put(
                &segment_key,
                serde_json::to_vec(&Segment {
                    resolution_secs: 86_400,
                    buckets: BTreeMap::from([(
                        "00:00".to_string(),
                        BTreeMap::from([("requests/r-1.0.whl".to_string(), 5)]),
                    )]),
                })
                .unwrap(),
            )
            .await
            .unwrap();

        c.compact().await;
        assert_eq!(pins.load(Ordering::SeqCst), 1);
        assert!(!first.objects.lock().unwrap().contains_key(&segment_key));
        assert!(first
            .objects
            .lock()
            .unwrap()
            .contains_key(&format!("{DAY_PREFIX}downloads/{old_day}/r.json")));
        assert_eq!(second.len(), 0, "compaction never crossed into next store");

        let today = OffsetDateTime::now_utc().date();
        let _ = c.query_package("downloads", "requests", today, today).await;
        assert_eq!(pins.load(Ordering::SeqCst), 2);
        let _ = c.query_summaries("downloads", today, today).await;
        assert_eq!(pins.load(Ordering::SeqCst), 3);
        c.record("downloads", "requests/r-2.0.whl");
        c.flush().await;
        assert_eq!(pins.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn compaction_backfills_a_frozen_day_missing_its_summary() {
        let store = MemStore::default();
        let c = engine(store.clone(), Config::default());

        // A prior cycle froze this past day's shard but its best-effort
        // write_summary failed: the frozen file exists, its segments are gone,
        // and there is no _summary.json. The day never re-enters the freeze
        // loop (no segments), so without backfill it stays summary-less forever
        // and the global dashboard undercounts it.
        let day = day_str(
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(3)),
        );
        let frozen = Segment {
            resolution_secs: 86_400,
            buckets: BTreeMap::from([(
                "00:00".to_string(),
                BTreeMap::from([("requests/r-1.0.whl".to_string(), 7u64)]),
            )]),
        };
        store
            .put(
                &format!("{DAY_PREFIX}downloads/{day}/r.json"),
                serde_json::to_vec(&frozen).unwrap(),
            )
            .await
            .unwrap();
        let summary_key = format!("{DAY_PREFIX}downloads/{day}/{SUMMARY_FILE}");
        assert!(!store.objects.lock().unwrap().contains_key(&summary_key));

        c.compact().await;

        // Recomputed from the surviving frozen shard file.
        assert!(store.objects.lock().unwrap().contains_key(&summary_key));
        let from = OffsetDateTime::now_utc()
            .date()
            .saturating_sub(time::Duration::days(4));
        let to = OffsetDateTime::now_utc().date();
        let sums = c.query_summaries("downloads", from, to).await;
        assert_eq!(sums[&day].total, 7);

        // A day that already has a summary is not rewritten on the next pass.
        let before = store.objects.lock().unwrap().clone();
        c.compact().await;
        assert_eq!(*store.objects.lock().unwrap(), before);
    }

    #[tokio::test]
    async fn frozen_file_wins_over_straggler_segments() {
        let store = MemStore::default();
        let c = engine(store.clone(), Config::default());
        let day = "2026-01-01";
        // A frozen file with the authoritative value...
        let frozen = Segment {
            resolution_secs: 86_400,
            buckets: BTreeMap::from([(
                "00:00".to_string(),
                BTreeMap::from([("requests/r-1.0.whl".to_string(), 10u64)]),
            )]),
        };
        store
            .put(
                &format!("{DAY_PREFIX}downloads/{day}/r.json"),
                serde_json::to_vec(&frozen).unwrap(),
            )
            .await
            .unwrap();
        // ...and a straggler segment that must be IGNORED by readers.
        let straggler = Segment {
            resolution_secs: 86_400,
            buckets: BTreeMap::from([(
                "00:00".to_string(),
                BTreeMap::from([("requests/r-1.0.whl".to_string(), 99u64)]),
            )]),
        };
        store
            .put(
                &format!("{SEG_PREFIX}downloads/{day}/r/late-0.json"),
                serde_json::to_vec(&straggler).unwrap(),
            )
            .await
            .unwrap();

        let d = time::macros::date!(2026 - 01 - 01);
        let series = c.query_package("downloads", "requests", d, d).await;
        assert_eq!(
            series[day]["r-1.0.whl"], 10,
            "frozen file wins; straggler is not double-counted"
        );
    }

    #[tokio::test]
    async fn disabled_is_a_noop() {
        let c = Counters::disabled();
        c.record("downloads", "requests/x.whl");
        c.flush().await;
        c.compact().await;
        let d = OffsetDateTime::now_utc().date();
        assert!(c
            .query_package("downloads", "requests", d, d)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn query_summaries_includes_open_day_live() {
        // The 2-day-delay fix: today's downloads must surface in the GLOBAL
        // summary after a flush, without waiting for a day to freeze/compact.
        let store = MemStore::default();
        let c = engine(store, Config::default());
        c.record("downloads", "requests/requests-2.31.0-py3-none-any.whl");
        c.record("downloads", "requests/requests-2.31.0-py3-none-any.whl");
        c.record("downloads", "flask/flask-3.0.0-py3-none-any.whl");
        c.flush().await; // note: NO compact() — today is never frozen.

        let today = OffsetDateTime::now_utc().date();
        let day = day_str(today);
        let sums = c.query_summaries("downloads", today, today).await;
        assert_eq!(
            sums[&day].total, 3,
            "open day aggregated live across shards"
        );
        assert_eq!(
            sums[&day].top["requests/requests-2.31.0-py3-none-any.whl"],
            2
        );
        assert_eq!(sums[&day].top["flask/flask-3.0.0-py3-none-any.whl"], 1);
    }

    #[tokio::test]
    async fn query_summaries_frozen_summary_wins_over_live() {
        // A frozen _summary.json short-circuits the live fallback, so straggler
        // segments left behind after a freeze can never inflate the total.
        let store = MemStore::default();
        let c = engine(store.clone(), Config::default());
        let today = day_str(OffsetDateTime::now_utc().date());
        store
            .put(
                &format!("{DAY_PREFIX}downloads/{today}/{SUMMARY_FILE}"),
                serde_json::to_vec(&DaySummary {
                    total: 10,
                    top: BTreeMap::from([("requests/r-1.0.whl".to_string(), 10u64)]),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        // A straggler segment for the same day that must be IGNORED.
        store
            .put(
                &format!("{SEG_PREFIX}downloads/{today}/r/late-0.json"),
                serde_json::to_vec(&Segment {
                    resolution_secs: 86_400,
                    buckets: BTreeMap::from([(
                        "00:00".to_string(),
                        BTreeMap::from([("requests/r-1.0.whl".to_string(), 99u64)]),
                    )]),
                })
                .unwrap(),
            )
            .await
            .unwrap();

        let d = OffsetDateTime::now_utc().date();
        let sums = c.query_summaries("downloads", d, d).await;
        assert_eq!(
            sums[&today].total, 10,
            "frozen summary wins; straggler ignored"
        );
    }

    #[tokio::test]
    async fn query_summaries_mixes_frozen_and_live_and_skips_empty() {
        let store = MemStore::default();
        let c = engine(store.clone(), Config::default());
        let today = OffsetDateTime::now_utc().date();
        let today_s = day_str(today);

        // An old, frozen day represented only by its summary file.
        let old = day_str(today.saturating_sub(time::Duration::days(5)));
        store
            .put(
                &format!("{DAY_PREFIX}downloads/{old}/{SUMMARY_FILE}"),
                serde_json::to_vec(&DaySummary {
                    total: 7,
                    top: BTreeMap::from([("flask/f-1.0.whl".to_string(), 7u64)]),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        // Today: live segments only (flushed, not compacted).
        c.record("downloads", "requests/r-2.0.whl");
        c.flush().await;

        let from = today.saturating_sub(time::Duration::days(10));
        let sums = c.query_summaries("downloads", from, today).await;
        assert_eq!(sums[&old].total, 7, "frozen day served from its summary");
        assert_eq!(
            sums[&today_s].total, 1,
            "open day served from live segments"
        );
        // An in-range day that is closed but has no summary is genuinely empty:
        // absent from the result, and paid no cross-shard scan.
        let empty = day_str(today.saturating_sub(time::Duration::days(3)));
        assert!(!sums.contains_key(&empty));
    }

    #[tokio::test]
    async fn reseed_rollups_mirrors_frozen_days_but_never_live_segments() {
        // The multi-bucket contract: a frozen day's rollup (shard totals +
        // summary) fans out to every peer so a failover finds the history; the
        // current day's live segments stay put (the declared ≤1-day loss window).
        let primary = MemStore::default();
        let peer = MemStore::default();
        // grace_days 0 so a past day is immediately closeable.
        let c = engine(
            primary.clone(),
            Config {
                grace_days: 0,
                ..Default::default()
            },
        );

        let today = OffsetDateTime::now_utc().date();
        let past = day_str(today.saturating_sub(time::Duration::days(3)));
        let today_s = day_str(today);
        // A closeable past-day segment (freezes into a rollup)...
        primary
            .put(
                &format!("{SEG_PREFIX}downloads/{past}/r/inc-0.json"),
                seg_bytes("requests/r-1.0.whl", 5),
            )
            .await
            .unwrap();
        // ...and today's live segment, which must never be replicated.
        primary
            .put(
                &format!("{SEG_PREFIX}downloads/{today_s}/f/inc-0.json"),
                seg_bytes("flask/f-1.0.whl", 3),
            )
            .await
            .unwrap();

        c.compact().await; // freezes the past day; today's segment survives as-is
        let frozen = format!("{DAY_PREFIX}downloads/{past}/r.json");
        let summary = format!("{DAY_PREFIX}downloads/{past}/{SUMMARY_FILE}");
        assert!(primary.objects.lock().unwrap().contains_key(&frozen));
        assert!(primary.objects.lock().unwrap().contains_key(&summary));
        assert!(!peer.objects.lock().unwrap().contains_key(&frozen));

        let peers: Vec<Box<dyn ObjectStore>> = vec![Box::new(peer.clone())];
        c.reseed_rollups(&peers).await;

        // The frozen rollup and its summary are mirrored to the peer...
        assert!(peer.objects.lock().unwrap().contains_key(&frozen));
        assert!(peer.objects.lock().unwrap().contains_key(&summary));
        // ...but no live segment (of any day) ever is.
        assert!(peer
            .objects
            .lock()
            .unwrap()
            .keys()
            .all(|k| !k.starts_with(SEG_PREFIX)));

        // Copy-if-absent is idempotent: a second reseed changes nothing.
        let before = peer.objects.lock().unwrap().clone();
        c.reseed_rollups(&peers).await;
        assert_eq!(*peer.objects.lock().unwrap(), before);

        // A node that fails over to the peer reads the full frozen history — the
        // audit ranking / stats survive the loss of the primary bucket.
        let survivor = engine(peer.clone(), Config::default());
        let from = today.saturating_sub(time::Duration::days(4));
        let sums = survivor.query_summaries("downloads", from, today).await;
        assert_eq!(sums[&past].total, 5, "frozen history survived on the peer");
        // Today's tally was only ever on the primary: it is the declared loss.
        assert!(!sums.contains_key(&today_s));
    }

    #[tokio::test]
    async fn open_day_sums_live_segments_across_reachable_buckets() {
        // A mid-day selection change split today's segments over two buckets. A
        // stats/report read must sum both so the open day stays whole.
        let primary = MemStore::default();
        let peer = MemStore::default();
        let today = OffsetDateTime::now_utc().date();
        let today_s = day_str(today);
        primary
            .put(
                &format!("{SEG_PREFIX}downloads/{today_s}/r/inc-0.json"),
                seg_bytes("requests/r-1.0.whl", 2),
            )
            .await
            .unwrap();
        peer.put(
            &format!("{SEG_PREFIX}downloads/{today_s}/f/inc-0.json"),
            seg_bytes("flask/f-1.0.whl", 3),
        )
        .await
        .unwrap();

        let c = Counters::new(
            Box::new(FleetStore {
                primary: primary.clone(),
                peers: vec![peer.clone()],
            }),
            Config::default(),
        );

        let sums = c.query_summaries("downloads", today, today).await;
        assert_eq!(
            sums[&today_s].total, 5,
            "open day summed across both buckets"
        );
        assert_eq!(sums[&today_s].top["requests/r-1.0.whl"], 2);
        assert_eq!(sums[&today_s].top["flask/f-1.0.whl"], 3);

        // A package whose shard's segments live only on the peer is still counted.
        let series = c.query_package("downloads", "flask", today, today).await;
        assert_eq!(series[&today_s]["f-1.0.whl"], 3);

        // Losing the peer degrades to only the primary's live tallies (declared
        // loss) — never an error, never the frozen history.
        let solo = engine(primary.clone(), Config::default());
        let sums = solo.query_summaries("downloads", today, today).await;
        assert_eq!(
            sums[&today_s].total, 2,
            "peer down: its share of today is lost"
        );
    }

    #[tokio::test]
    async fn open_day_read_dedups_a_bucket_seen_as_both_pin_and_peer() {
        // A selection switch racing the pin/peer enumeration can surface the same
        // bucket twice; its segments must be summed once, not doubled.
        let primary = MemStore::default();
        let today = OffsetDateTime::now_utc().date();
        let today_s = day_str(today);
        primary
            .put(
                &format!("{SEG_PREFIX}downloads/{today_s}/r/inc-0.json"),
                seg_bytes("requests/r-1.0.whl", 4),
            )
            .await
            .unwrap();
        let c = Counters::new(
            Box::new(FleetStore {
                primary: primary.clone(),
                peers: vec![primary.clone()], // the racing duplicate
            }),
            Config::default(),
        );

        let sums = c.query_summaries("downloads", today, today).await;
        assert_eq!(sums[&today_s].total, 4, "same bucket twice is not doubled");
        let series = c.query_package("downloads", "requests", today, today).await;
        assert_eq!(series[&today_s]["r-1.0.whl"], 4);
    }
}
