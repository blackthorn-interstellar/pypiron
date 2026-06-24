//! Distributed, S3-backed counter store. **Self-contained on purpose**: it
//! depends only on the [`ObjectStore`] trait below, `time`, and `serde` — never
//! on `AppState`, `web`, or `crate::storage`. Lift it into its own crate by
//! copying this one file and providing an [`ObjectStore`] impl for your backend.
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

/// In-memory keys past the cap fold into this catch-all so a flood of distinct
/// (or hostile) keys can never grow a node's memory without bound.
pub const OVERFLOW_KEY: &str = "_overflow";

const SUMMARY_FILE: &str = "_summary.json";

/// The minimal object-store surface the engine needs. Map these onto any
/// backend (the pypiron adapter wraps `crate::storage::Storage`). `get` returns
/// `Ok(None)` for a genuinely-absent object and `Err` only for a *transient*
/// failure — the engine relies on that distinction to never freeze a day from a
/// failed read.
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
    /// Validate the resolution (a whole-minute divisor of a day) and clamp the
    /// cap to something sane. Returns an error string for a bad resolution so a
    /// caller can fail closed at startup.
    pub fn checked(self) -> Result<Self, String> {
        let r = self.resolution_secs;
        if !(60..=86_400).contains(&r) || !r.is_multiple_of(60) || !86_400u32.is_multiple_of(r) {
            return Err(format!(
                "resolution must be a whole number of minutes dividing a day (60..=86400 s), got {r}s"
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
    store: Option<Box<dyn ObjectStore>>,
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
    pub fn new(store: Box<dyn ObjectStore>, cfg: Config) -> Self {
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

    pub fn record_n(&self, metric: &str, key: &str, n: u64) {
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
        let Some(store) = self.store.as_deref() else {
            return;
        };
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
                "{PREFIX}{metric}/seg/{day}/{shard}/{}-{seq}.json",
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
        let Some(store) = self.store.as_deref() else {
            return;
        };
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
            match sum_segments(store, seg_keys).await {
                Some(buckets) => {
                    let frozen_key = format!("{PREFIX}{metric}/day/{day}/{shard}.json");
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
            .filter_map(
                |k| match k.strip_prefix(PREFIX)?.split('/').collect::<Vec<_>>()[..] {
                    [metric, "day", day, file] if file == SUMMARY_FILE => Some((metric, day)),
                    _ => None,
                },
            )
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
            self.write_summary(store, &metric, &day).await;
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
        let prefix = format!("{PREFIX}{metric}/day/{day}/");
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
            for keys_at in seg.buckets.values() {
                for (key, c) in keys_at {
                    *totals.entry(key.clone()).or_insert(0) += c;
                    total += c;
                }
            }
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
        let Some(store) = self.store.as_deref() else {
            return out;
        };
        let shard = shard_of(pkg);
        let prefix = format!("{pkg}/");
        let mut day = from;
        loop {
            let ds = day_str(day);
            if let Some(buckets) = self.read_day_shard(store, metric, &ds, shard).await {
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
        let Some(store) = self.store.as_deref() else {
            return out;
        };
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
            let key = format!("{PREFIX}{metric}/day/{ds}/{SUMMARY_FILE}");
            match store.get(&key).await {
                Ok(Some(bytes)) => {
                    if let Ok(s) = serde_json::from_slice::<DaySummary>(&bytes) {
                        out.insert(ds, s); // frozen summary wins, always
                    }
                }
                Ok(None) if ds >= close_cutoff => {
                    if let Some(s) = self.summarize_day_live(store, metric, &ds).await {
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
    /// [`Counters::write_summary`] freezes, but computed on read by summing every
    /// shard via [`Counters::read_day_shard`] (so a shard's frozen file still wins
    /// over its open segments). `None` when no shard has data for the day.
    async fn summarize_day_live(
        &self,
        store: &dyn ObjectStore,
        metric: &str,
        day: &str,
    ) -> Option<DaySummary> {
        let mut totals: BTreeMap<String, u64> = BTreeMap::new();
        let mut total: u64 = 0;
        let mut any = false;
        for shard in all_shards() {
            let Some(buckets) = self.read_day_shard(store, metric, day, shard).await else {
                continue;
            };
            any = true;
            for keys_at in buckets.values() {
                for (key, c) in keys_at {
                    *totals.entry(key.clone()).or_insert(0) += c;
                    total += c;
                }
            }
        }
        any.then(|| rank_summary(totals, total))
    }

    /// Frozen file wins; otherwise sum the open day's live segments. `None` means
    /// no data for that day-shard.
    async fn read_day_shard(
        &self,
        store: &dyn ObjectStore,
        metric: &str,
        day: &str,
        shard: char,
    ) -> Option<BucketMap> {
        let frozen_key = format!("{PREFIX}{metric}/day/{day}/{shard}.json");
        if let Ok(Some(bytes)) = store.get(&frozen_key).await {
            let seg: Segment = serde_json::from_slice(&bytes).unwrap_or_default();
            return Some(seg.buckets);
        }
        let seg_prefix = format!("{PREFIX}{metric}/seg/{day}/{shard}/");
        let seg_keys = store.list(&seg_prefix).await.ok()?;
        if seg_keys.is_empty() {
            return None;
        }
        sum_segments(store, &seg_keys).await
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
            let Some(rest) = k.strip_prefix(PREFIX) else {
                continue;
            };
            let parts: Vec<&str> = rest.split('/').collect();
            // <metric>/seg/<day>/<shard>/<file>   |   <metric>/day/<day>/<shard>.json
            match parts.as_slice() {
                [metric, "seg", day, shard, _file] => {
                    if let Some(s) = first_char(shard) {
                        segments
                            .entry((metric.to_string(), day.to_string(), s))
                            .or_default()
                            .push(k.clone());
                    }
                }
                [metric, "day", day, file] if *file != SUMMARY_FILE => {
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
        let rest = key.strip_prefix(PREFIX)?;
        let parts: Vec<&str> = rest.split('/').collect();
        match parts.as_slice() {
            [_metric, "seg", day, _shard, _file] => Some(day),
            [_metric, "day", day, _file] => Some(day),
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

/// Every shard label in deterministic order — the inverse of [`shard_of`]: the
/// package tree's first-character fan-out (`0-9`, `a-z`) plus the `_` catch-all.
fn all_shards() -> impl Iterator<Item = char> {
    ('0'..='9').chain('a'..='z').chain(std::iter::once('_'))
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
                &format!("{PREFIX}downloads/seg/{yest}/r/inc-0.json"),
                serde_json::to_vec(&seg).unwrap(),
            )
            .await
            .unwrap();

        c.compact().await;
        // Segment gone, frozen file written, summary written.
        let frozen = format!("{PREFIX}downloads/day/{yest}/r.json");
        assert!(store.objects.lock().unwrap().contains_key(&frozen));
        assert!(store
            .objects
            .lock()
            .unwrap()
            .contains_key(&format!("{PREFIX}downloads/day/{yest}/{SUMMARY_FILE}")));
        let remaining_segs = store
            .list(&format!("{PREFIX}downloads/seg/{yest}/"))
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
                &format!("{PREFIX}downloads/day/{day}/r.json"),
                serde_json::to_vec(&frozen).unwrap(),
            )
            .await
            .unwrap();
        let summary_key = format!("{PREFIX}downloads/day/{day}/{SUMMARY_FILE}");
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
                &format!("{PREFIX}downloads/day/{day}/r.json"),
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
                &format!("{PREFIX}downloads/seg/{day}/r/late-0.json"),
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
                &format!("{PREFIX}downloads/day/{today}/{SUMMARY_FILE}"),
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
                &format!("{PREFIX}downloads/seg/{today}/r/late-0.json"),
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
                &format!("{PREFIX}downloads/day/{old}/{SUMMARY_FILE}"),
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
}
