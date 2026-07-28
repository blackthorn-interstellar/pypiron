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
//!   shard's segments into one frozen `day/<day>/<shard>@<bucket>.json`, then
//!   delete the segments. A frozen file **always wins** over the segment dir at
//!   read time, so a crash mid-compaction can neither double-count nor shrink a
//!   total. Retention then deletes frozen days older than the window.
//! - **Query**: per day, prefer the frozen shard files; else sum the open day's
//!   segments. Filter to one key-prefix (a package) for cheap per-package reads.
//!
//! ## Why a rollup names its bucket
//! A segment lives on exactly the bucket the node that wrote it had pinned, so a
//! `(metric, day, shard)` can have segments on several buckets at once — one
//! mid-day selection change is enough. The rollup therefore carries the identity
//! of the bucket whose segments it summed (`<shard>@<bucket>.json`), each bucket
//! is compacted from its own segments in isolation, and a read sums the variants.
//! Without that, one bucket's freeze would claim the whole day: the shared key
//! reseeds to every peer as truth, and a later leader on another bucket reads it
//! as "already frozen" and sweeps that bucket's segments **without summing them**
//! — a permanently short day. The identity is in the *filename*, not a new path
//! component, so every key keeps its arity for `src/layout.rs`'s classifier and
//! retention. A bucket unreachable during a pass is skipped whole and frozen by a
//! later one, because the frozen sentinel is now per-bucket.
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
//!   summaries, one variant per bucket. Leader-authored, immutable once written,
//!   and the durable truth a dashboard/audit reads. The same [`Counters::compact`]
//!   pass that freezes them converges every bucket on the union of the variants
//!   (copy-if-absent), so a failover cannot zero history. The engine reaches a
//!   bucket only through the handles the embedder hands it — the pin, and the
//!   peers passed to [`Counters::compact`].
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
/// [`Counters::compact`] and the layout manifest.
pub const DAY_PREFIX: &str = "_counters/day/";

/// The live-tally subtree: per-node open-day delta segments, never replicated —
/// their loss is the declared ≤1-day window. See the layout manifest.
pub const SEG_PREFIX: &str = "_counters/seg/";

/// In-memory keys past the cap fold into this catch-all so a flood of distinct
/// (or hostile) keys can never grow a node's memory without bound.
pub(crate) const OVERFLOW_KEY: &str = "_overflow";

/// Filename stem of a day's summary, distinguishing it from the `_` catch-all
/// shard's own rollup (`_@<bucket>.json`).
const SUMMARY_STEM: &str = "_summary";

/// A key-safe, restart-stable identifier for one bucket, stamped into every
/// rollup filename so a frozen day names the bucket whose segments it summed.
/// Anything outside `[A-Za-z0-9._-]` folds to `-` (a scheme's `://`, most
/// obviously); an unnamed store — the single-bucket default — is `local`. It is
/// derived from the bucket's configured identity and never from its position in
/// the list, so reordering the configuration cannot rename a key.
pub fn bucket_tag(name: &str) -> String {
    let tag: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    if tag.is_empty() {
        "local".to_string()
    } else {
        tag
    }
}

/// Select one stable store handle for a complete flush, compaction, or query.
/// The engine calls this exactly once at each public operation boundary and
/// threads the returned handle through the whole call graph.
pub trait ObjectStoreSelector: Send + Sync {
    fn pin(&self) -> Box<dyn ObjectStore>;

    /// The [`ObjectStore::bucket`] tag of every *configured* bucket, reachable or
    /// not — the set of rollup variants a read must sum. It must include the
    /// pinned bucket's own tag. Reachability is deliberately not consulted: the
    /// rollups are replicated, so the pin holds a down bucket's variant too, and
    /// dropping it would silently shorten a finished day. Default: the pinned
    /// bucket alone, exactly right for a single-bucket embedder.
    fn bucket_tags(&self) -> Vec<String> {
        vec![self.pin().bucket().to_string()]
    }

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
    /// This handle's bucket identity — see [`bucket_tag`]. Stable across
    /// restarts: it is written into every rollup key this store authors.
    fn bucket(&self) -> &str;
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

    /// Leader-only: one rollup pass over the whole fleet. Freeze every closeable
    /// `(metric, day, shard)`, write per-day summaries, apply retention — **each
    /// bucket independently**, from its own segments into its own
    /// `<shard>@<bucket>.json` rollup, sweeping only its own segments — and then
    /// converge every bucket on the union of the rollups so a failover to any of
    /// them finds the whole history. `peers` are the other write-eligible buckets
    /// (the caller's health/fence gate); a single-bucket fleet passes an empty
    /// slice and behaves exactly as before, with one variant and no mirroring.
    ///
    /// A bucket that is unreachable during a pass is skipped **whole** — nothing
    /// of its own is read, frozen, or deleted — and a later pass picks it up, since
    /// the frozen sentinel names its bucket and so another bucket's finished day
    /// can never stand in for it. Idempotent and crash-safe (recompute from
    /// immutable segments; the bucket's own frozen file is the sentinel; deletes
    /// are best-effort).
    ///
    /// Freeze and mirror are one pass on purpose: it costs exactly **one LIST per
    /// bucket** for both, so a bucket that has gone dark stalls the leader's tick
    /// for one hung round trip rather than two.
    pub async fn compact(&self, peers: &[Box<dyn ObjectStore>]) {
        let Some(selector) = self.store.as_deref() else {
            return;
        };
        let store = selector.pin();
        let today = OffsetDateTime::now_utc().date();
        let close_cutoff = day_str(today.saturating_sub(time::Duration::days(self.cfg.grace_days)));
        let retain_cutoff =
            day_str(today.saturating_sub(time::Duration::days(self.cfg.retention_days)));

        // Each reachable bucket and the rollup keys it holds when its own
        // compaction is done. Dedup by identity: a selection switch racing the
        // peer enumeration can surface the pinned bucket again as a peer, and
        // compacting it twice in one pass would re-list a tree it has swept.
        let mut mesh: Vec<(&dyn ObjectStore, std::collections::HashSet<String>)> = Vec::new();
        let mut done: std::collections::HashSet<String> = std::collections::HashSet::new();
        for bucket in std::iter::once(store.as_ref()).chain(peers.iter().map(|p| p.as_ref())) {
            if !done.insert(bucket.bucket().to_string()) {
                continue;
            }
            if let Some(rollups) = self
                .compact_bucket(bucket, &close_cutoff, &retain_cutoff)
                .await
            {
                mesh.push((bucket, rollups));
            }
        }
        reseed_rollups(&mesh, &retain_cutoff).await;
    }

    /// One bucket's whole compaction, in isolation: its segments, its rollups,
    /// its summaries, its retention. Never touches another bucket. Returns the
    /// `_counters/day/` keys it holds afterwards — the reseed's view of it — or
    /// `None` when the bucket could not be listed and is skipped whole.
    async fn compact_bucket(
        &self,
        store: &dyn ObjectStore,
        close_cutoff: &str,
        retain_cutoff: &str,
    ) -> Option<std::collections::HashSet<String>> {
        let name = store.bucket();
        let keys = match store.list(PREFIX).await {
            Ok(k) => k,
            Err(e) => {
                warn!(error=?e, bucket=%name, "counter compaction: list failed; this bucket is skipped whole and frozen by a later pass");
                return None;
            }
        };
        // Only this bucket's OWN rollups count as frozen: a peer's variant sitting
        // here from a reseed says nothing about whether these segments were summed.
        let layout = Layout::parse(&keys, name);

        // What this bucket holds under `day/`, kept current as the pass writes and
        // prunes, so the reseed below needs no second LIST.
        let mut rollups: std::collections::HashSet<String> = keys
            .iter()
            .filter(|k| k.starts_with(DAY_PREFIX))
            .cloned()
            .collect();

        // Freeze closeable day-shards; collect each frozen day for its summary.
        let mut to_summarize: BTreeMap<(String, String), ()> = BTreeMap::new();
        for ((metric, day, shard), seg_keys) in &layout.segments {
            if day.as_str() >= close_cutoff {
                continue; // still open (or within grace)
            }
            if layout
                .frozen
                .contains(&(metric.clone(), day.clone(), *shard))
            {
                // Already frozen here: a crash left stragglers — sweep, never recompute.
                let _ = store.delete(seg_keys).await;
                continue;
            }
            match sum_segments(store, seg_keys).await {
                Some(buckets) => {
                    let key = frozen_key(metric, day, *shard, name);
                    let seg = Segment {
                        resolution_secs: self.cfg.resolution_secs,
                        buckets,
                    };
                    let bytes = serde_json::to_vec(&seg).unwrap_or_default();
                    if store.put(&key, bytes).await.is_ok() {
                        let _ = store.delete(seg_keys).await;
                        rollups.insert(key);
                        to_summarize.insert((metric.clone(), day.clone()), ());
                    }
                }
                None => {
                    // Transient read error mid-day — skip; next cycle retries.
                    // Never freeze from a partial read.
                }
            }
        }

        // Backfill any already-frozen day still missing this bucket's summary: a
        // prior cycle's best-effort write_summary failed transiently. Without
        // this it never retries — a frozen day with swept segments never
        // re-enters the loop above — and the global dashboard undercounts it
        // forever. Recomputing from the surviving frozen shard files is
        // idempotent, so skip days that already have a summary (no churn).
        for (metric, day, _shard) in &layout.frozen {
            if day.as_str() >= retain_cutoff
                && !layout.summaries.contains(&(metric.clone(), day.clone()))
            {
                to_summarize.insert((metric.clone(), day.clone()), ());
            }
        }

        // Recompute each pending day's summary from this bucket's frozen shards.
        for (metric, day) in to_summarize.into_keys() {
            if let Some(key) = self.write_summary(store, &metric, &day).await {
                rollups.insert(key);
            }
        }

        // Retention: drop days (any bucket's rollup, plus leftover segments) past
        // the window. Uniform across variants — every bucket prunes what it holds.
        let stale: Vec<String> = keys
            .iter()
            .filter(|k| day_of(k).is_some_and(|day| day < retain_cutoff))
            .cloned()
            .collect();
        if !stale.is_empty() {
            let _ = store.delete(&stale).await;
            for k in &stale {
                rollups.remove(k);
            }
        }
        Some(rollups)
    }

    /// Rewrite `day`'s summary for `store`'s **own** rollups — a peer's variants
    /// mirrored here belong to that peer's summary, and double-counting them
    /// would inflate the day the moment a reseed lands. A read sums the variants.
    /// Returns the key it wrote, or `None` when a transient read made it skip.
    async fn write_summary(
        &self,
        store: &dyn ObjectStore,
        metric: &str,
        day: &str,
    ) -> Option<String> {
        let name = store.bucket();
        let prefix = format!("{DAY_PREFIX}{metric}/{day}/");
        let keys = store.list(&prefix).await.ok()?;
        let mut totals: BTreeMap<String, u64> = BTreeMap::new();
        let mut total: u64 = 0;
        for k in &keys {
            if frozen_shard_of(k).is_none_or(|(_, bucket)| bucket != name) {
                continue; // a summary, another bucket's rollup, or a stray key
            }
            let Ok(Some(bytes)) = store.get(k).await else {
                return None; // transient: skip writing a partial summary
            };
            let seg: Segment = serde_json::from_slice(&bytes).unwrap_or_default();
            fold_buckets(&seg.buckets, &mut totals, &mut total);
        }
        let summary = rank_summary(totals, total);
        let key = summary_key(metric, day, name);
        store
            .put(&key, serde_json::to_vec(&summary).unwrap_or_default())
            .await
            .ok()
            .map(|()| key)
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
        let tags = selector.bucket_tags();
        let shard = shard_of(pkg);
        let prefix = format!("{pkg}/");
        let mut day = from;
        loop {
            let ds = day_str(day);
            if let Some(buckets) = self
                .read_day_shard(store.as_ref(), &peers, &tags, metric, &ds, shard)
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
    /// totals/top-N. A frozen day is one tiny GET per configured bucket (the
    /// summary is per-bucket, so the day's total is their sum); a day that isn't
    /// frozen yet (today and anything within `grace_days`) has no summary, so it
    /// is aggregated live across shards on read — that way the global view is
    /// never days behind the per-package one (which already reads open-day
    /// segments). Older days with no summary are genuinely empty (or
    /// retention-pruned), so they cost nothing beyond the missing-summary GETs —
    /// deliberately N exact GETs against the pin rather than a prefix LIST, which
    /// would turn every empty day into a scan.
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
        // rolled-up history is replicated, so every bucket's frozen variant reads
        // from the pin — including a bucket that is currently down.
        let peers = selector.reachable_peers();
        let tags = selector.bucket_tags();
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
            // Sum every bucket's variant: each summarizes only the segments that
            // bucket held, so the day's total is their sum, never either half.
            // The total is exact; the merged top-N inherits the per-variant
            // truncation each summary already applied, same as it always has.
            let mut totals: BTreeMap<String, u64> = BTreeMap::new();
            let mut total: u64 = 0;
            let mut frozen = false;
            for tag in &tags {
                if let Ok(Some(bytes)) = store.get(&summary_key(metric, &ds, tag)).await {
                    if let Ok(s) = serde_json::from_slice::<DaySummary>(&bytes) {
                        total += s.total;
                        for (key, c) in s.top {
                            *totals.entry(key).or_insert(0) += c;
                        }
                        frozen = true;
                    }
                }
            }
            if frozen {
                out.insert(ds, rank_summary(totals, total)); // frozen summaries win, always
            } else if ds >= close_cutoff {
                if let Some(s) = self
                    .summarize_open_day(store.as_ref(), &peers, metric, &ds)
                    .await
                {
                    out.insert(ds, s);
                }
            }
            // A closed day with no summary on any bucket has no data: skip the scan.
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
        // best-effort summary write failed — so they still win over stragglers.
        // Replicated, so the primary's copy stands in for the whole fleet; one
        // prefix LIST replaces the 37 blind per-shard frozen GETs. A rollup is
        // frozen per `(shard, bucket)`, so that pair is what it retires.
        let mut frozen: std::collections::HashSet<(char, String)> =
            std::collections::HashSet::new();
        let day_prefix = format!("{DAY_PREFIX}{metric}/{day}/");
        for k in primary.list(&day_prefix).await.unwrap_or_default() {
            let Some((shard, bucket)) = frozen_shard_of(&k) else {
                continue; // a _summary variant or a stray key
            };
            let bucket = bucket.to_string();
            if let Ok(Some(bytes)) = primary.get(&k).await {
                let seg: Segment = serde_json::from_slice(&bytes).unwrap_or_default();
                fold_buckets(&seg.buckets, &mut totals, &mut total);
                frozen.insert((shard, bucket));
                any = true;
            }
        }

        // Open segments for every shard this bucket has not frozen, summed in one
        // pass per reachable bucket. A per-bucket read failure drops only that
        // bucket's share of the open day (the declared loss window), never the
        // whole day. Segments are never replicated, so deduping by bucket identity
        // makes the sum robust to the same bucket appearing twice — a selection
        // switch racing between the pin and the peer enumeration — which would
        // otherwise double-count.
        let seg_prefix = format!("{SEG_PREFIX}{metric}/{day}/");
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for store in std::iter::once(primary).chain(peers.iter().map(|p| p.as_ref())) {
            if !seen.insert(store.bucket().to_string()) {
                continue;
            }
            let listed = match store.list(&seg_prefix).await {
                Ok(l) => l,
                Err(_) => continue,
            };
            let open: Vec<String> = listed
                .into_iter()
                .filter(|k| {
                    seg_shard_of(k)
                        .is_some_and(|s| !frozen.contains(&(s, store.bucket().to_string())))
                })
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

    /// Sum the day-shard across every bucket: a bucket's frozen rollup wins over
    /// its own live segments, and the buckets that have not frozen it yet still
    /// contribute theirs. Rollups are replicated, so all `tags`' variants are read
    /// from the `primary` pin — one exact GET each, no LIST — which is what keeps
    /// a bucket that is currently down counted. Live segments are only ever on the
    /// bucket that wrote them, so they come from `primary` and each reachable
    /// `peer`; a down peer contributes nothing (the declared loss). `None` means
    /// no data for that day-shard anywhere reachable.
    async fn read_day_shard(
        &self,
        primary: &dyn ObjectStore,
        peers: &[Box<dyn ObjectStore>],
        tags: &[String],
        metric: &str,
        day: &str,
        shard: char,
    ) -> Option<BucketMap> {
        let mut acc: BucketMap = BTreeMap::new();
        let mut any = false;
        let mut frozen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for tag in tags {
            if let Ok(Some(bytes)) = primary.get(&frozen_key(metric, day, shard, tag)).await {
                let seg: Segment = serde_json::from_slice(&bytes).unwrap_or_default();
                merge_bucketmap(&mut acc, seg.buckets);
                frozen.insert(tag.as_str());
                any = true;
            }
        }
        let seg_prefix = format!("{SEG_PREFIX}{metric}/{day}/{shard}/");
        // Dedup by bucket identity: a bucket seen twice is a selection switch
        // racing the pin/peer enumeration — sum its segments once, never double.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for store in std::iter::once(primary).chain(peers.iter().map(|p| p.as_ref())) {
            if frozen.contains(store.bucket()) || !seen.insert(store.bucket().to_string()) {
                continue; // this bucket's total is already the frozen one
            }
            let seg_keys = match store.list(&seg_prefix).await {
                Ok(k) => k,
                Err(_) => continue,
            };
            if seg_keys.is_empty() {
                continue;
            }
            if let Some(buckets) = sum_segments(store, &seg_keys).await {
                merge_bucketmap(&mut acc, buckets);
                any = true;
            }
        }
        any.then_some(acc)
    }
}

/// Converge every bucket in `mesh` on the **union** of the rolled-up counter
/// files it collectively holds (the frozen per-shard day totals and their
/// summaries under `_counters/day/`), copy-if-absent. `mesh` pairs each reachable
/// bucket with the rollup keys it holds — the view [`Counters::compact`] already
/// built while freezing, so this costs no extra LIST.
///
/// A union rather than a push from the write pin because each bucket authors its
/// own variants: a rollup frozen on a peer has to reach the pin too, or a read
/// there would count only the pin's share of a finished day. Copy-if-absent is
/// sound because every key names its author and is immutable once written, so two
/// buckets never hold different bytes for one key. This is both the write-through
/// for a freshly-frozen day and the backstop for a bucket that was down when
/// another froze or that joined the fleet later, so a failover to any bucket
/// finds the whole history. The live `_counters/seg/` tallies are never copied
/// (the declared ≤1-day loss window). A no-op for a single-bucket fleet; an
/// unreachable bucket is simply absent from the mesh and the next pass retries.
async fn reseed_rollups(
    mesh: &[(&dyn ObjectStore, std::collections::HashSet<String>)],
    retain_cutoff: &str,
) {
    if mesh.len() < 2 {
        return; // single bucket, or nothing to converge against this cycle
    }
    // The union, minus anything already past retention: a bucket returning from a
    // long outage must not resurrect history the rest of the fleet has pruned
    // (every later pass would prune it again).
    let union: std::collections::BTreeSet<&String> = mesh
        .iter()
        .flat_map(|(_, keys)| keys.iter())
        .filter(|k| day_of(k).is_some_and(|day| day >= retain_cutoff))
        .collect();
    for key in union {
        if mesh.iter().all(|(_, keys)| keys.contains(key)) {
            continue; // immutable rollup already everywhere
        }
        let Some((source, _)) = mesh.iter().find(|(_, keys)| keys.contains(key)) else {
            continue;
        };
        let bytes = match source.get(key).await {
            Ok(Some(bytes)) => bytes,
            // Listed then vanished (raced retention): nothing to copy.
            Ok(None) => continue,
            Err(e) => {
                warn!(error=?e, %key, bucket=%source.bucket(), "counter rollup reseed: reading a bucket failed; retries next cycle");
                continue;
            }
        };
        for (dest, _) in mesh.iter().filter(|(_, keys)| !keys.contains(key)) {
            if let Err(e) = dest.put(key, bytes.clone()).await {
                warn!(error=?e, %key, bucket=%dest.bucket(), "counter rollup reseed: writing a bucket failed; retries next cycle");
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

/// `_counters/day/<metric>/<day>/<shard>@<bucket>.json` — one bucket's frozen
/// total for that day-shard, summed from the segments that bucket held.
fn frozen_key(metric: &str, day: &str, shard: char, bucket: &str) -> String {
    format!("{DAY_PREFIX}{metric}/{day}/{shard}@{bucket}.json")
}

/// `_counters/day/<metric>/<day>/_summary@<bucket>.json` — one bucket's headline
/// view of the day, summed from its own frozen shards.
fn summary_key(metric: &str, day: &str, bucket: &str) -> String {
    format!("{DAY_PREFIX}{metric}/{day}/{SUMMARY_STEM}@{bucket}.json")
}

/// Split a rollup filename `<stem>@<bucket>.json` into its two halves. A bucket
/// tag can never contain `@` ([`bucket_tag`] folds it away), so the first `@`
/// splits unambiguously — including for the `_` catch-all shard, whose file is
/// `_@<bucket>.json` and is never confused with `_summary@<bucket>.json`.
fn split_rollup_file(file: &str) -> Option<(&str, &str)> {
    let (stem, bucket) = file.strip_suffix(".json")?.split_once('@')?;
    (!stem.is_empty() && !bucket.is_empty()).then_some((stem, bucket))
}

/// The `(shard, bucket)` a frozen day-shard key belongs to. `None` for a day's
/// `_summary@<bucket>.json` or any non-rollup key.
fn frozen_shard_of(key: &str) -> Option<(char, &str)> {
    let parts = key_parts(key)?;
    let ["day", _metric, _day, file] = parts[..] else {
        return None;
    };
    let (stem, bucket) = split_rollup_file(file)?;
    if stem == SUMMARY_STEM {
        return None;
    }
    Some((first_char(stem)?, bucket))
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

/// Parsed view of ONE bucket's `_counters/` key space for a compaction pass.
/// Rollups authored by another bucket and mirrored here are deliberately absent
/// from [`frozen`](Self::frozen) and [`summaries`](Self::summaries): they say
/// nothing about whether *this* bucket's segments have been summed, and treating
/// them as this bucket's own is exactly how a day gets swept uncounted.
struct Layout {
    /// `(metric, day, shard) -> segment keys`.
    segments: BTreeMap<(String, String, char), Vec<String>>,
    /// `(metric, day, shard)` this bucket has already frozen.
    frozen: std::collections::HashSet<(String, String, char)>,
    /// `(metric, day)` for which this bucket has already written its summary.
    summaries: std::collections::HashSet<(String, String)>,
}

impl Layout {
    fn parse(keys: &[String], bucket: &str) -> Self {
        let mut segments: BTreeMap<(String, String, char), Vec<String>> = BTreeMap::new();
        let mut frozen = std::collections::HashSet::new();
        let mut summaries = std::collections::HashSet::new();
        for k in keys {
            let Some(parts) = key_parts(k) else {
                continue;
            };
            // seg/<metric>/<day>/<shard>/<file>  |  day/<metric>/<day>/<stem>@<bucket>.json
            match parts.as_slice() {
                ["seg", metric, day, shard, _file] => {
                    if let Some(s) = first_char(shard) {
                        segments
                            .entry((metric.to_string(), day.to_string(), s))
                            .or_default()
                            .push(k.clone());
                    }
                }
                ["day", metric, day, file] => {
                    let Some((stem, owner)) = split_rollup_file(file) else {
                        continue;
                    };
                    if owner != bucket {
                        continue; // another bucket's rollup, mirrored here
                    }
                    if stem == SUMMARY_STEM {
                        summaries.insert((metric.to_string(), day.to_string()));
                    } else if let Some(s) = first_char(stem) {
                        frozen.insert((metric.to_string(), day.to_string(), s));
                    }
                }
                _ => {}
            }
        }
        Self {
            segments,
            frozen,
            summaries,
        }
    }
}

/// The `<day>` component of any counter key, for retention.
fn day_of(key: &str) -> Option<&str> {
    let parts = key_parts(key)?;
    match parts.as_slice() {
        ["seg", _metric, day, _shard, _file] => Some(day),
        ["day", _metric, day, _file] => Some(day),
        _ => None,
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

    /// One in-memory bucket. `up` models an unreachable bucket: every operation
    /// fails transiently, exactly as a real outage looks to the engine (never a
    /// genuine `Ok(None)` miss, which would license a freeze from a partial read).
    #[derive(Clone)]
    struct MemStore {
        objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
        name: String,
        up: Arc<AtomicBool>,
    }
    impl Default for MemStore {
        fn default() -> Self {
            Self::named("")
        }
    }
    impl MemStore {
        fn named(name: &str) -> Self {
            Self {
                objects: Arc::new(Mutex::new(BTreeMap::new())),
                name: bucket_tag(name),
                up: Arc::new(AtomicBool::new(true)),
            }
        }
        fn len(&self) -> usize {
            self.objects.lock().unwrap().len()
        }
        fn set_up(&self, up: bool) {
            self.up.store(up, Ordering::SeqCst);
        }
        fn down(&self) -> anyhow::Result<()> {
            if self.up.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(anyhow::anyhow!("bucket {} is unreachable", self.name))
            }
        }
        fn peer(&self) -> Box<dyn ObjectStore> {
            Box::new(self.clone())
        }
    }
    impl ObjectStoreSelector for MemStore {
        fn pin(&self) -> Box<dyn ObjectStore> {
            Box::new(self.clone())
        }
    }
    #[async_trait]
    impl ObjectStore for MemStore {
        fn bucket(&self) -> &str {
            &self.name
        }
        async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.down()?;
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }
        async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
            self.down()?;
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
            Ok(())
        }
        async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            self.down()?;
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
            self.down()?;
            let mut o = self.objects.lock().unwrap();
            for k in keys {
                o.remove(k);
            }
            Ok(())
        }
    }

    /// A multi-bucket fleet: a fixed primary plus peer buckets exposed through
    /// [`ObjectStoreSelector::reachable_peers`], so a query sums an open day's
    /// live segments across every bucket. [`ObjectStoreSelector::bucket_tags`]
    /// lists every *configured* bucket, healthy or not.
    #[derive(Clone)]
    struct FleetStore {
        primary: MemStore,
        peers: Vec<MemStore>,
    }
    impl ObjectStoreSelector for FleetStore {
        fn pin(&self) -> Box<dyn ObjectStore> {
            Box::new(self.primary.clone())
        }
        fn bucket_tags(&self) -> Vec<String> {
            std::iter::once(&self.primary)
                .chain(&self.peers)
                .map(|s| s.name.clone())
                .collect()
        }
        fn reachable_peers(&self) -> Vec<Box<dyn ObjectStore>> {
            self.peers
                .iter()
                .filter(|p| p.up.load(Ordering::SeqCst))
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

        c.compact(&[]).await;
        // Segment gone, frozen file written, summary written — both naming the
        // one bucket that held the segments, exactly as a fleet of one should.
        let frozen = frozen_key("downloads", &yest, 'r', "local");
        assert!(store.objects.lock().unwrap().contains_key(&frozen));
        assert!(store.objects.lock().unwrap().contains_key(&summary_key(
            "downloads",
            &yest,
            "local"
        )));
        let remaining_segs = store
            .list(&format!("{SEG_PREFIX}downloads/{yest}/"))
            .await
            .unwrap();
        assert!(remaining_segs.is_empty(), "segments deleted after freeze");

        // Idempotent: re-running compaction changes nothing and never double-counts.
        let before = store.objects.lock().unwrap().clone();
        c.compact(&[]).await;
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

    /// A fleet whose two buckets each hold part of the SAME `(metric, day, shard)`
    /// — one mid-day selection change is all it takes. Returns the engine, the
    /// past day, and the two buckets.
    async fn split_day_fleet(a_count: u64, b_count: u64) -> (Counters, String, MemStore, MemStore) {
        let a = MemStore::named("s3://a");
        let b = MemStore::named("s3://b");
        let past = day_str(
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(3)),
        );
        let seg = format!("{SEG_PREFIX}downloads/{past}/r/inc-0.json");
        a.put(&seg, seg_bytes("requests/r-1.0.whl", a_count))
            .await
            .unwrap();
        b.put(&seg, seg_bytes("requests/r-1.0.whl", b_count))
            .await
            .unwrap();
        let c = Counters::new(
            Box::new(FleetStore {
                primary: a.clone(),
                peers: vec![b.clone()],
            }),
            Config {
                grace_days: 0,
                ..Config::default()
            },
        );
        (c, past, a, b)
    }

    #[tokio::test]
    async fn each_bucket_is_frozen_from_its_own_segments_and_read_as_the_union() {
        // The invariant that replaced "a compaction pins exactly one store": a
        // bucket's segments are read, frozen, and swept through THAT bucket's own
        // store, into a rollup that names it — and a read returns the sum, not
        // whichever half the leader happened to be pinned to.
        let (c, past, a, b) = split_day_fleet(5, 3).await;
        c.compact(&[b.peer()]).await;

        // Each bucket froze its own share into its own key, on itself.
        let (key_a, key_b) = (
            frozen_key("downloads", &past, 'r', "s3---a"),
            frozen_key("downloads", &past, 'r', "s3---b"),
        );
        // Each bucket authored its own variant and swept only its own segments;
        // the same pass then converged both on the union, which is what puts the
        // peer's variant on the pin — reads of a rolled-up day go to the pin.
        for bucket in [&a, &b] {
            let has_both = {
                let held = bucket.objects.lock().unwrap();
                held.contains_key(&key_a) && held.contains_key(&key_b)
            };
            assert!(has_both, "both variants converged onto every bucket");
            assert!(bucket
                .list(&format!("{SEG_PREFIX}downloads/{past}/"))
                .await
                .unwrap()
                .is_empty());
        }

        let (from, to) = (
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(4)),
            OffsetDateTime::now_utc().date(),
        );
        let series = c.query_package("downloads", "requests", from, to).await;
        assert_eq!(series[&past]["r-1.0.whl"], 8, "union of both buckets");
        let sums = c.query_summaries("downloads", from, to).await;
        assert_eq!(sums[&past].total, 8);
        assert_eq!(sums[&past].top["requests/r-1.0.whl"], 8);

        // Both buckets converged on the union, so losing either still serves the
        // whole finished day from the survivor.
        for (pin, dead) in [(&a, &b), (&b, &a)] {
            dead.set_up(false);
            let survivor = Counters::new(
                Box::new(FleetStore {
                    primary: pin.clone(),
                    peers: vec![dead.clone()],
                }),
                Config::default(),
            );
            let sums = survivor.query_summaries("downloads", from, to).await;
            assert_eq!(sums[&past].total, 8, "the survivor holds both variants");
            dead.set_up(true);
        }
    }

    #[tokio::test]
    async fn a_bucket_unreachable_during_a_pass_is_frozen_by_the_next() {
        // The regression test. Bucket B is down when the leader compacts, so its
        // share of the finished day is neither frozen NOR swept. The reseed then
        // puts A's rollup on B — and that must not read as "B is already frozen"
        // when B comes back, which is precisely how the shared `<shard>.json` key
        // deleted a bucket's segments without ever summing them.
        let (c, past, a, b) = split_day_fleet(5, 3).await;
        let seg = format!("{SEG_PREFIX}downloads/{past}/r/inc-0.json");

        b.set_up(false);
        c.compact(&[b.peer()]).await;
        assert!(
            a.objects
                .lock()
                .unwrap()
                .contains_key(&frozen_key("downloads", &past, 'r', "s3---a")),
            "the reachable bucket still freezes"
        );
        b.set_up(true);
        assert!(
            b.objects.lock().unwrap().contains_key(&seg),
            "an unreachable bucket is skipped whole: its segments survive, uncounted"
        );
        assert!(
            !b.objects
                .lock()
                .unwrap()
                .contains_key(&frozen_key("downloads", &past, 'r', "s3---b")),
            "and nothing was frozen there from a failed read"
        );

        // Second pass, B reachable. Its segments are summed into ITS rollup, and
        // A's rollup — mirrored onto B by this very pass, the failover guarantee —
        // does not stand in for B's own and get them deleted under cover of it.
        c.compact(&[b.peer()]).await;
        {
            let held = b.objects.lock().unwrap();
            assert!(held.contains_key(&frozen_key("downloads", &past, 'r', "s3---a")));
            assert!(held.contains_key(&frozen_key("downloads", &past, 'r', "s3---b")));
            assert!(!held.contains_key(&seg), "swept only after it was counted");
        }

        let (from, to) = (
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(4)),
            OffsetDateTime::now_utc().date(),
        );
        let sums = c.query_summaries("downloads", from, to).await;
        assert_eq!(sums[&past].total, 8, "the late bucket's share is counted");
        let series = c.query_package("downloads", "requests", from, to).await;
        assert_eq!(series[&past]["r-1.0.whl"], 8);
    }

    #[tokio::test]
    async fn a_mirrored_rollup_never_passes_for_a_bucket_s_own_frozen_day() {
        // The other half of the same bug, and the sharpest statement of it. Bucket
        // A froze a day nobody else had segments for, and the reseed mirrored
        // `r@a` onto B — that is the failover guarantee working. When B later
        // holds its own segments for that same (metric, day, shard), reading the
        // mirrored copy as "B has already frozen this" would sweep them WITHOUT
        // EVER SUMMING THEM, permanently. Only B's own `r@b` may retire them.
        let a = MemStore::named("s3://a");
        let b = MemStore::named("s3://b");
        let past = day_str(
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(3)),
        );
        let seg = format!("{SEG_PREFIX}downloads/{past}/r/inc-0.json");
        a.put(&seg, seg_bytes("requests/r-1.0.whl", 5))
            .await
            .unwrap();
        let c = Counters::new(
            Box::new(FleetStore {
                primary: a.clone(),
                peers: vec![b.clone()],
            }),
            Config {
                grace_days: 0,
                ..Config::default()
            },
        );

        c.compact(&[b.peer()]).await;
        let (key_a, key_b) = (
            frozen_key("downloads", &past, 'r', "s3---a"),
            frozen_key("downloads", &past, 'r', "s3---b"),
        );
        assert!(
            b.objects.lock().unwrap().contains_key(&key_a),
            "A's rollup is mirrored onto B"
        );
        assert!(!b.objects.lock().unwrap().contains_key(&key_b));

        // A straggler node flushed to B after the day closed.
        b.put(&seg, seg_bytes("requests/r-1.0.whl", 3))
            .await
            .unwrap();
        c.compact(&[b.peer()]).await;

        assert!(
            b.objects.lock().unwrap().contains_key(&key_b),
            "B froze its own share rather than deferring to A's mirrored copy"
        );
        assert!(!b.objects.lock().unwrap().contains_key(&seg));
        let (from, to) = (
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(4)),
            OffsetDateTime::now_utc().date(),
        );
        assert_eq!(
            c.query_summaries("downloads", from, to).await[&past].total,
            8
        );
    }

    #[tokio::test]
    async fn a_finished_day_reads_whole_while_one_bucket_is_still_unfrozen() {
        // Between the two passes above the day is short but never wrong-shaped:
        // the frozen bucket serves its rollup, the not-yet-frozen one still serves
        // its live segments, and neither is double-counted.
        let (c, past, _a, b) = split_day_fleet(5, 3).await;
        b.set_up(false);
        c.compact(&[b.peer()]).await;
        b.set_up(true);

        let (from, to) = (
            OffsetDateTime::now_utc()
                .date()
                .saturating_sub(time::Duration::days(4)),
            OffsetDateTime::now_utc().date(),
        );
        let series = c.query_package("downloads", "requests", from, to).await;
        assert_eq!(
            series[&past]["r-1.0.whl"], 8,
            "A's frozen rollup plus B's still-live segments"
        );
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
                &frozen_key("downloads", &day, 'r', "local"),
                serde_json::to_vec(&frozen).unwrap(),
            )
            .await
            .unwrap();
        let summary = summary_key("downloads", &day, "local");
        assert!(!store.objects.lock().unwrap().contains_key(&summary));

        c.compact(&[]).await;

        // Recomputed from the surviving frozen shard file.
        assert!(store.objects.lock().unwrap().contains_key(&summary));
        let from = OffsetDateTime::now_utc()
            .date()
            .saturating_sub(time::Duration::days(4));
        let to = OffsetDateTime::now_utc().date();
        let sums = c.query_summaries("downloads", from, to).await;
        assert_eq!(sums[&day].total, 7);

        // A day that already has a summary is not rewritten on the next pass.
        let before = store.objects.lock().unwrap().clone();
        c.compact(&[]).await;
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
                &frozen_key("downloads", day, 'r', "local"),
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
        c.compact(&[]).await;
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
                &summary_key("downloads", &today, "local"),
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
                &summary_key("downloads", &old, "local"),
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
        let primary = MemStore::named("s3://a");
        let peer = MemStore::named("s3://b");
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

        // Only the pin has segments; the peer is compacted too and finds none.
        // The same pass mirrors what the pin froze onto the peer.
        c.compact(&[peer.peer()]).await; // freezes the past day; today's segment survives
        let frozen = frozen_key("downloads", &past, 'r', "s3---a");
        let summary = summary_key("downloads", &past, "s3---a");
        assert!(primary.objects.lock().unwrap().contains_key(&frozen));
        assert!(primary.objects.lock().unwrap().contains_key(&summary));

        // The frozen rollup and its summary reached the peer...
        assert!(peer.objects.lock().unwrap().contains_key(&frozen));
        assert!(peer.objects.lock().unwrap().contains_key(&summary));
        // ...but no live segment (of any day) ever is.
        assert!(peer
            .objects
            .lock()
            .unwrap()
            .keys()
            .all(|k| !k.starts_with(SEG_PREFIX)));

        // Copy-if-absent is idempotent: a second pass changes nothing.
        let before = peer.objects.lock().unwrap().clone();
        c.compact(&[peer.peer()]).await;
        assert_eq!(*peer.objects.lock().unwrap(), before);

        // A node that fails over to the peer reads the full frozen history — the
        // audit ranking / stats survive the loss of the primary bucket. The
        // rollup variant still names the bucket that authored it, and the pin
        // holds the copy, so a dead bucket's frozen share is still counted.
        primary.set_up(false);
        let survivor = Counters::new(
            Box::new(FleetStore {
                primary: peer.clone(),
                peers: vec![primary.clone()],
            }),
            Config::default(),
        );
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
        let primary = MemStore::named("s3://a");
        let peer = MemStore::named("s3://b");
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
