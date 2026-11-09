//! On-demand mirroring: serve unknown packages from an upstream simple index,
//! caching artifacts in storage on first download.
//!
//! This is `sync`, made lazy. The same rules hold: the origin model is the
//! dependency-confusion defense, so a name claimed `private` (or inside
//! `--private-prefix` / `--private-pattern`) never falls through to upstream, and the first
//! upstream artifact write claims the name `mirror` — atomically, exactly as
//! `sync` does. Artifacts are immutable, so caching them is trivially
//! correct; only the package *listing* needs freshness, and it is fetched
//! from the upstream PEP 691 API (which carries PEP 700 upload times, so
//! `--exclude-newer` stays historically correct) and cached for
//! [`LISTING_TTL`]. Concurrent misses for one package share one fetch. A
//! failed fetch is remembered only for [`LISTING_ERROR_TTL`], and the last good
//! listing keeps being served through an outage for at most
//! [`LISTING_MAX_STALE`].
//!
//! Package pages are rendered from the upstream listing with our own
//! `/files/` URLs; artifact GETs download-verify-commit through the upload
//! spool (bounded memory, whatever the wheel size), then fall through to the
//! normal serving path. PEP 658 companions for not-yet-cached wheels are
//! passed through from upstream without writing anything — a resolver
//! probing dozens of candidate wheels must not stampede gigabytes into
//! storage. When upstream is down, callers fall back to the local
//! materialized index: already-cached packages keep installing, and a package
//! with nothing cached answers a retryable 503, never a final 404.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context as _, Result};
use futures::stream::{self, StreamExt};
use pep440_rs::VersionSpecifiers;
use reqwest::Client;
use tracing::{debug, info, warn};

use crate::app::{AppState, PACKAGES_PREFIX};
use crate::deps::{self, DepsMemo, Want};
use crate::hash::sha256_hex;
use crate::names::infer_version_from_filename;
use crate::origin;
use crate::render::{self, FileMetadata};
use crate::sidecar::{
    frozen_key, metadata_key, provenance_key, sidecar_key, Sidecar, FROZEN_SUFFIX, METADATA_SUFFIX,
    MIRROR_QUARANTINED_SUFFIX, PROVENANCE_SUFFIX, TOMBSTONE_SUFFIX,
};
use crate::simple::{self, SimpleFile};
use crate::ssrf::{self, Guard, SsrfGuardResolver};
use crate::storage::Storage;
use crate::sync::{is_yanked, matches_mirror_listing, ResolvedMirror};
use crate::upload::{FinishedSpool, UploadSpool};

/// How long an upstream package listing (or its absence) is reused before
/// refetching. Bounds the lag for "a new release appeared upstream"; the
/// artifacts themselves are immutable and cached forever.
const LISTING_TTL: Duration = Duration::from_secs(60);
/// How long a failed upstream fetch is remembered when there is no usable last
/// good listing. Long enough that a dead upstream isn't probed on every request,
/// short enough that a recovered one is used again almost at once.
pub(crate) const LISTING_ERROR_TTL: Duration = Duration::from_secs(5);
/// The oldest last good listing still served while upstream keeps failing. Past
/// it the package falls back to local-only, so an upstream yank or quarantine
/// can't go unseen for longer than this.
const LISTING_MAX_STALE: Duration = Duration::from_secs(3600);
/// Hard ceiling on cached listings. Each `/simple/:pkg` miss against a proxy
/// upstream inserts one entry (including negative `Missing` ones for 404s), and
/// there are unbounded distinct normalized names, so without a cap a stream of
/// nonexistent-package requests grows the map until OOM.
const MAX_LISTINGS: usize = 8192;
/// Listing and metadata fetches are small; bound them hard.
const SMALL_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
/// With `include-dependencies`: how often the dependency scope is re-read with
/// nothing prompting it. A listing fetch that turns up an unread release
/// prompts a refresh at once; this pass retries releases whose metadata could
/// not be read.
const FOLLOW_INTERVAL: Duration = Duration::from_secs(600);
/// How long a request for a name outside the included packages waits on a
/// pending dependency refresh before a retryable 503.
const FOLLOW_WAIT: Duration = Duration::from_secs(20);
/// Projects, and metadata files within each, read at once while following.
const FOLLOW_CONCURRENCY: usize = 8;
/// What the dependency follower has read, so a restart reads only releases
/// published since. Derived and disposable, like `_sync/cursors.json`.
pub(crate) const FOLLOW_KEY: &str = "_state/proxy-dependencies.json";
/// Same retry budget as `sync`: at CDN scale, transient errors are routine.
const DOWNLOAD_ATTEMPTS: u32 = 3;
/// Fixed head start every artifact download gets before its size-derived budget
/// begins: connect, TLS, upstream think-time, and the whole of a small wheel.
const DOWNLOAD_GRACE: Duration = Duration::from_secs(60);
/// The slowest sustained transfer worth waiting on, 64 KiB/s. Below it an
/// upstream is not delivering a file, it is occupying a single-flight slot.
const DOWNLOAD_FLOOR_BYTES_PER_SEC: u64 = 64 * 1024;
/// Ceiling on one download attempt whatever the declared size — an hour is well
/// past any wheel PyPI serves, and it is what an unsized body gets.
const DOWNLOAD_MAX: Duration = Duration::from_secs(3600);
/// Spool ceiling for an artifact whose upstream listing declares no size. The
/// per-chunk overrun check has nothing to compare against there, and the sha256
/// gate only runs on a *finished* spool — so without this an unsized body is
/// bounded only by [`DOWNLOAD_MAX`], an hour of disk at whatever rate upstream
/// can push. 1 GiB is PyPI's hard per-file limit, so nothing a real index serves
/// is refused; PEP 700 indexes declare a size and never reach this at all.
const DOWNLOAD_UNSIZED_MAX_BYTES: u64 = 1024 * 1024 * 1024;
/// How long a positive "already cached locally" observation is trusted before a
/// re-verifying HEAD. Artifacts are immutable, so present→absent only happens on
/// a delete/prune (rare) or a bucket switch (handled by the generation key), and
/// a stale hit can only ever skip an upstream fetch and serve local bytes (or a
/// local 404) — never trigger a fetch — so the bound is about freshness, not
/// safety. Matches [`LISTING_TTL`].
const PRESENCE_TTL: Duration = Duration::from_secs(60);
/// Hard ceiling on remembered artifact keys, mirroring [`MAX_LISTINGS`]: each
/// distinct proxied download inserts one entry, so without a cap a stream of
/// distinct downloads grows the map until OOM.
const MAX_PRESENCE: usize = 65_536;
/// How much of a teed artifact is withheld until the sha256 check passes. The
/// client gets byte one at upstream speed, but the last [`TEE_HOLDBACK`] bytes
/// only exist after verification — so a corrupt or truncated upstream can never
/// produce a *complete* body, only a short read on an aborted connection.
pub(crate) const TEE_HOLDBACK: u64 = 64 * 1024;
/// Read size for one poll of a teed body: the same 64 KiB the disk artifact
/// streamer uses (see `range::read_capacity`).
const TEE_CHUNK: u64 = 64 * 1024;

/// Ceiling on cold-miss fills running detached from the request that started
/// them. A detached fill deliberately outlives its client — that is what stops
/// pip's retries from livelocking on a big wheel — so a client disconnect no
/// longer bounds anything. Without a ceiling, one anonymous caller can GET a
/// hundred distinct large wheels, hang up on every one, and leave the node
/// committed to a hundred concurrent multi-hour downloads spooling into the
/// spool dir (often a tmpfs). Past the ceiling the fill runs INLINE on the
/// request task instead, where a disconnect cancels it exactly as it did before
/// fills were detached: the overflow case degrades to the old behavior rather
/// than to no behavior.
const MAX_DETACHED_FILLS: usize = 32;
static DETACHED_FILLS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(MAX_DETACHED_FILLS);

/// A package page rendered from the upstream listing, ETag precomputed.
#[derive(Clone)]
pub struct RenderedIndex {
    pub body: bytes::Bytes,
    pub etag: Arc<str>,
}

fn rendered(body: String) -> RenderedIndex {
    RenderedIndex {
        etag: crate::cache::quoted_sha256(body.as_bytes()),
        body: bytes::Bytes::from(body),
    }
}

/// Upstream listing, filtered and pre-rendered. Rendering happens once per
/// fill, so the per-request cost of a proxied page is a map lookup.
struct Found {
    files: Vec<SimpleFile>,
    html: RenderedIndex,
    json: RenderedIndex,
    status: crate::status::ProjectStatusDoc,
}

/// What upstream said about a package: its listing (cached as `Arc<Found>`),
/// or one of the two ways of having none, which callers must tell apart.
#[derive(Clone)]
pub enum Listing<T> {
    Found(T),
    /// Upstream said 404 — cached as hard as a hit, or a stampede of typo'd
    /// installs becomes an upstream hammer.
    Missing,
    /// Upstream failed and there was nothing fresh enough to reuse. Served
    /// local-only like `Missing`, but held for [`LISTING_ERROR_TTL`], never a
    /// full TTL, and a page with no local copy is a retryable 503, not a 404:
    /// an upstream blip must not read as "no such package".
    Unreachable,
}

impl<T> Listing<T> {
    fn found(self) -> Option<T> {
        match self {
            Listing::Found(found) => Some(found),
            Listing::Missing | Listing::Unreachable => None,
        }
    }
}

struct CacheEntry {
    listing: Listing<Arc<Found>>,
    /// When upstream last really answered. Never moved by a failed refresh, so
    /// it is what [`LISTING_MAX_STALE`] measures.
    fetched: Instant,
    /// Serve from cache until this instant.
    expires: Instant,
}

impl CacheEntry {
    fn served(&self) -> Listing<Arc<Found>> {
        self.listing.clone()
    }
}

/// What a failed refresh leaves in the cache: the last good listing, if it is
/// younger than [`LISTING_MAX_STALE`] (and never served past that age), else a
/// short-lived [`Listing::Unreachable`] — held at least as long as the failed
/// fetch `took`, so a stalled upstream is not probed more often than it answers.
fn after_error(prev: Option<CacheEntry>, now: Instant, took: Duration) -> CacheEntry {
    match prev {
        Some(CacheEntry {
            listing: Listing::Found(found),
            fetched,
            ..
        }) if now.saturating_duration_since(fetched) < LISTING_MAX_STALE => CacheEntry {
            listing: Listing::Found(found),
            fetched,
            expires: (now + LISTING_TTL).min(fetched + LISTING_MAX_STALE),
        },
        _ => CacheEntry {
            listing: Listing::Unreachable,
            fetched: now,
            expires: now + LISTING_ERROR_TTL.max(took),
        },
    }
}

pub struct Proxy {
    upstream: String,
    mirror: ResolvedMirror,
    /// The package scope as a fast name → version-constraints lookup, derived
    /// from `mirror.include_packages`. `None` means no scope is configured (serve
    /// any non-private name — the open-proxy default). A present map is a
    /// fail-closed allowlist: a name absent from it never falls through, and a
    /// name's constraints gate which versions are served. A name may carry
    /// several constraints (duplicate list entries); a version passes if any
    /// allows it, matching `sync`'s union semantics. With `include-dependencies`
    /// the follower widens it to the included packages' dependency closure.
    scope: RwLock<Option<Arc<ScopeMap>>>,
    /// The dependency follower, when `include-dependencies` is on.
    follow: Option<Follow>,
    /// The `--exclude-package` denylist, shared (via `Arc`) with `AppState` so the
    /// index rebuild, the `/project/` page, and the reconcile all rule through the
    /// same predicate the proxy's own scope check does — no drift.
    denylist: Arc<crate::denylist::Denylist>,
    client: Client,
    /// The SSRF allow-list shared by the client's DNS resolver and the pre-flight
    /// literal check in [`ssrf::guarded_get`].
    guard: Arc<Guard>,
    listings: Mutex<HashMap<String, CacheEntry>>,
    /// Single-flight guard: at most one in-flight download per artifact key.
    /// Without it, N concurrent GETs for the same uncached wheel each stream a
    /// full copy into N separate spool files — an anonymous client could
    /// amplify one request for a large wheel into N full-size downloads
    /// (disk-fill + upstream bandwidth). The map self-prunes (see
    /// [`DownloadSlot`]), so it stays bounded by the fills actually in flight —
    /// detached ones included, and [`MAX_DETACHED_FILLS`] bounds those — not by
    /// the number of distinct artifacts ever proxied.
    inflight: InflightMap,
    /// Warm-hit accelerator: artifact keys proven present in local storage, so a
    /// repeat proxied download skips its existence HEAD. See [`PresenceCache`].
    presence: PresenceCache,
}

type ScopeMap = HashMap<String, Vec<Option<VersionSpecifiers>>>;

/// `include-dependencies` on the proxy: the same closure `sync` mirrors, kept
/// current in the background by [`Proxy::follow_dependencies`] so a name is in
/// scope by the time an installer asks for it.
struct Follow {
    /// The included names; they keep their own constraints and never wait.
    listed: HashSet<String>,
    known: Mutex<Known>,
    /// The closure last published into the scope.
    published: Mutex<BTreeMap<String, Want>>,
    /// Refreshes asked for so far; startup asks the first.
    asked: AtomicU64,
    wake: tokio::sync::Notify,
    /// The `asked` count the last finished refresh started from: every ask up
    /// to it is reflected in the scope.
    answered: tokio::sync::watch::Sender<u64>,
}

#[derive(Default)]
struct Known {
    /// project → what its servable releases require.
    memo: HashMap<String, DepsMemo>,
    /// Releases whose metadata fetch failed; not re-asked for until the next
    /// [`FOLLOW_INTERVAL`] pass retries them.
    failed: HashSet<(String, String)>,
    /// `memo` changed since it was last saved.
    dirty: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SavedFollow {
    /// The memo is only good for the upstream it was read from.
    upstream: String,
    projects: BTreeMap<String, DepsMemo>,
}

impl Follow {
    fn ask(&self) {
        self.asked.fetch_add(1, Ordering::AcqRel);
        self.wake.notify_one();
    }

    /// A refresh asked for has not finished yet.
    fn pending(&self) -> bool {
        *self.answered.borrow() < self.asked.load(Ordering::Acquire)
    }

    /// Wait until every refresh asked for so far has finished; false if that
    /// took longer than [`FOLLOW_WAIT`].
    async fn settled(&self) -> bool {
        let target = self.asked.load(Ordering::Acquire);
        let mut rx = self.answered.subscribe();
        tokio::time::timeout(FOLLOW_WAIT, rx.wait_for(|&answered| answered >= target))
            .await
            .is_ok_and(|r| r.is_ok())
    }

    fn closure(
        &self,
        mirror: &ResolvedMirror,
        private: &crate::names::PrivateNames,
    ) -> BTreeMap<String, Want> {
        let targets = crate::sync::dependency_targets(mirror);
        let known = self.known.lock().unwrap_or_else(|e| e.into_inner());
        deps::closure(&mirror.include_packages, &known.memo, private, &targets)
    }
}

/// One artifact key's single-flight state: nothing but the slot mutex. Fill
/// progress does not live here — the leader owns a fresh channel per fill (see
/// [`Proxy::ensure_artifact_cached`]), which is why one fill can never be
/// mistaken for the next on the same key.
type InflightMap = Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>;

/// Held for the whole download-verify-commit of one artifact key. Dropping it
/// removes the map entry once no other task is waiting on that key, keeping
/// [`Proxy::inflight`] bounded.
struct DownloadSlot {
    inflight: InflightMap,
    key: String,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for DownloadSlot {
    fn drop(&mut self) {
        let mut map = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = map.get(&self.key) {
            // We still hold `_guard` (one strong ref) and the map holds one.
            // Any count beyond that is a task already waiting on this key, so
            // only collapse the entry when we are its last user.
            if Arc::strong_count(entry) <= 2 {
                map.remove(&self.key);
            }
        }
    }
}

/// What a running fill publishes for whoever is teeing its bytes to a client.
///
/// One value, overwritten in place — a tee only ever needs the latest position,
/// never a history. The channel belongs to exactly one fill, so the only
/// splice hazard left is that fill's own retries, and `attempt` is what makes
/// that safe: a tee binds to the attempt it attached on and treats any other
/// attempt as a failure rather than splicing a restarted download onto a body
/// that already carries a prefix of the dead one.
#[derive(Clone)]
struct FillProgress {
    /// Which of the fill's up-to-[`DOWNLOAD_ATTEMPTS`] attempts published this.
    attempt: u32,
    /// The attempt's spool file, published once it exists — which is also once
    /// upstream has answered 200, since the spool is opened after the response.
    spool: Option<Arc<std::path::PathBuf>>,
    /// Bytes flushed to that spool. Safe to read: the fill flushes before it
    /// publishes, so every counted byte is readable through a second handle.
    written: u64,
    state: FillState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FillState {
    Running,
    /// sha256 matched and the origin fence still holds — the tail may be released.
    Verified,
    /// This attempt is dead. The fill may retry for the cache's sake; the tee
    /// bound to the attempt must abort its connection.
    Failed,
}

impl Default for FillProgress {
    fn default() -> Self {
        // The channel's initial value: the fill exists but has not opened a
        // spool yet, so there is nothing to attach to and nothing has failed.
        Self {
            attempt: 0,
            spool: None,
            written: 0,
            state: FillState::Running,
        }
    }
}

/// The publishing half of [`FillProgress`], owned by a fill that has a tee
/// attached. Absent (`None` at the call sites) when nothing is streaming, so the
/// buffered path pays nothing — it never even allocates the channel.
struct Progress {
    tx: tokio::sync::watch::Sender<FillProgress>,
}

impl Progress {
    /// A new attempt has an open spool: republish from byte zero under the new
    /// attempt number. A tee bound to the previous attempt sees the change and
    /// aborts rather than splicing.
    fn started(&self, attempt: u32, spool: &std::path::Path) {
        let spool = Arc::new(spool.to_path_buf());
        self.tx.send_modify(|p| {
            p.attempt = attempt;
            p.spool = Some(spool);
            p.written = 0;
            p.state = FillState::Running;
        });
    }

    fn wrote(&self, written: u64) {
        self.tx.send_modify(|p| p.written = written);
    }

    /// Release the tail: the bytes on the spool hash to what upstream declared
    /// and the name is still ours. Published before the storage commit, so a
    /// client never waits on an S3 upload for the last 64 KiB.
    fn verified(&self, written: u64) {
        self.tx.send_modify(|p| {
            p.written = written;
            p.state = FillState::Verified;
        });
    }

    fn failed(&self) {
        self.tx.send_modify(|p| p.state = FillState::Failed);
    }

    fn is_verified(&self) -> bool {
        self.tx.borrow().state == FillState::Verified
    }
}

/// What [`Proxy::ensure_artifact_cached`] left its caller to do.
pub enum FillOutcome {
    /// Nothing is in flight any more; serve the artifact from storage as usual.
    /// `filled` says THIS call committed a fresh cache fill, so the caller
    /// schedules the off-request-path peer replication notes.
    Done { filled: bool },
    /// A cold-miss fill past the streaming threshold is running: serve its bytes
    /// to the client as they land, instead of waiting for the commit.
    Streaming(Box<StreamingFill>),
}

/// A cold-miss fill big enough to serve while it downloads.
///
/// The promise this keeps is narrow and absolute: a *complete* body is only ever
/// delivered for bytes pypiron verified. The last [`TEE_HOLDBACK`] bytes do not
/// leave until the fill's sha256 check and origin fence have both passed, so a
/// corrupt, truncated, or hung upstream can only ever produce a short read on an
/// aborted connection — never a whole file the client would trust.
pub struct StreamingFill {
    size: u64,
    attempt: u32,
    /// This tee's own handle on the spool, opened while the fill still owns the
    /// file. It stays valid across the commit's rename and the spool's unlink.
    file: tokio::fs::File,
    rx: tokio::sync::watch::Receiver<FillProgress>,
    fill: Option<tokio::task::JoinHandle<Result<bool>>>,
    metrics: Arc<crate::metrics::Metrics>,
}

impl StreamingFill {
    /// Upstream's declared size — this response's `Content-Length`.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The fill task, so the caller can react to the commit (peer replication
    /// notes) off the request path. The tee never touches it: a client that
    /// disconnects drops the response, and the fill must survive that.
    pub fn take_fill(&mut self) -> Option<tokio::task::JoinHandle<Result<bool>>> {
        self.fill.take()
    }

    /// The response body. `on_delivered` runs once, when the verified tail has
    /// been handed to the client — the teed path's delivery exit, where the
    /// buffered path bumps its download counter.
    pub fn into_body(self, on_delivered: Option<Box<dyn FnOnce() + Send>>) -> axum::body::Body {
        let tee = Tee {
            size: self.size,
            attempt: self.attempt,
            file: self.file,
            rx: self.rx,
            sent: 0,
            available: 0,
            verified: false,
            done: false,
            on_delivered,
            metrics: self.metrics,
        };
        axum::body::Body::from_stream(futures::stream::unfold(tee, |mut tee| async move {
            tee.next().await.map(|chunk| (chunk, tee))
        }))
    }
}

/// The live position of the fill a tee is bound to.
enum Observed {
    /// Still downloading; this many bytes are readable from the spool.
    Live(u64),
    /// Verified, with the spool's final length — the tail may go out.
    Verified(u64),
    /// This attempt is dead, or a different fill now owns the entry.
    Lost,
}

struct Tee {
    size: u64,
    attempt: u32,
    file: tokio::fs::File,
    rx: tokio::sync::watch::Receiver<FillProgress>,
    sent: u64,
    available: u64,
    verified: bool,
    done: bool,
    on_delivered: Option<Box<dyn FnOnce() + Send>>,
    metrics: Arc<crate::metrics::Metrics>,
}

impl Tee {
    /// The next chunk, `None` at a clean end of body, `Some(Err)` to abort the
    /// connection mid-body. Never `None` before `size` bytes have gone out: a
    /// short body under a declared `Content-Length` would be a silent truncation
    /// on some clients, where an aborted connection is one nobody mistakes.
    async fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        if self.done {
            return None;
        }
        loop {
            if !self.verified {
                match observe(&mut self.rx, self.attempt) {
                    Observed::Live(written) => self.available = written,
                    Observed::Verified(written) => {
                        self.verified = true;
                        self.available = written;
                    }
                    Observed::Lost => return Some(Err(self.abort("upstream fill failed"))),
                }
            }
            // The verified body is not the length this response promised. Short
            // is an upstream that under-delivered; long means the two listing
            // lookups behind this download (the tee's size and the fill's) saw
            // different metadata across a listing refresh. Either way the
            // Content-Length is already out, so abort rather than pad or truncate.
            if self.verified && self.available != self.size {
                return Some(Err(
                    self.abort("upstream body was not the length it declared")
                ));
            }
            let limit = if self.verified {
                self.size
            } else {
                self.available.min(self.size.saturating_sub(TEE_HOLDBACK))
            };
            if self.sent >= limit {
                if self.verified {
                    self.done = true;
                    return None; // the tail went out on the previous poll
                }
                if let Some(err) = self.wait().await {
                    return Some(Err(err));
                }
                continue;
            }
            let want = usize::try_from((limit - self.sent).min(TEE_CHUNK)).unwrap_or(0);
            let mut buf = vec![0u8; want];
            match tokio::io::AsyncReadExt::read(&mut self.file, &mut buf).await {
                Ok(0) if self.verified => {
                    return Some(Err(self.abort("verified spool ended early")))
                }
                Ok(0) => {
                    // The published count ran ahead of what this handle can see;
                    // wait for the fill to move rather than spin on EOF.
                    if let Some(err) = self.wait().await {
                        return Some(Err(err));
                    }
                }
                Ok(n) => {
                    buf.truncate(n);
                    self.sent += n as u64;
                    if self.verified && self.sent >= self.size {
                        self.deliver();
                    }
                    return Some(Ok(bytes::Bytes::from(buf)));
                }
                Err(e) => {
                    return Some(Err(
                        anyhow::Error::from(e).context("read the proxy fill spool")
                    ))
                }
            }
        }
    }

    /// Block until the fill publishes again. `Some(err)` when it published its
    /// last: a fill that exited without verifying can never complete this body.
    async fn wait(&mut self) -> Option<anyhow::Error> {
        if self.rx.changed().await.is_ok() {
            return None;
        }
        // The fill task is gone, but its final value is still readable — a tail
        // released just before it exited still counts.
        match observe(&mut self.rx, self.attempt) {
            Observed::Verified(written) => {
                self.verified = true;
                self.available = written;
                None
            }
            _ => Some(self.abort("upstream fill ended without verifying")),
        }
    }

    fn abort(&mut self, why: &str) -> anyhow::Error {
        self.done = true;
        // The middleware logged a 200 the moment the headers went out, so an
        // abort here is invisible in the status-code metrics. Count it where an
        // operator will find it, next to the other proxy artifact failures.
        self.metrics
            .proxy_stream_aborts
            .fetch_add(1, Ordering::Relaxed);
        warn!(
            sent = self.sent,
            size = self.size,
            "proxy: aborting a partially streamed artifact — {why}"
        );
        anyhow!("proxy stream aborted after {} bytes: {why}", self.sent)
    }

    fn deliver(&mut self) {
        if let Some(on_delivered) = self.on_delivered.take() {
            on_delivered();
        }
    }
}

fn observe(rx: &mut tokio::sync::watch::Receiver<FillProgress>, attempt: u32) -> Observed {
    let progress = rx.borrow_and_update();
    if progress.attempt != attempt {
        return Observed::Lost;
    }
    match progress.state {
        FillState::Failed => Observed::Lost,
        FillState::Verified => Observed::Verified(progress.written),
        FillState::Running => Observed::Live(progress.written),
    }
}

/// Where a tee attaches: the attempt it binds to, and that attempt's spool.
struct TeeAnchor {
    attempt: u32,
    spool: Arc<std::path::PathBuf>,
}

enum Peek {
    Anchor(TeeAnchor),
    Wait,
    Gone,
}

fn peek(rx: &mut tokio::sync::watch::Receiver<FillProgress>) -> Peek {
    let progress = rx.borrow_and_update();
    match progress.state {
        // Ended before it ever opened a spool — an upstream 404/500, a yank or
        // malware refusal, a name gone private. There is nothing to stream, and
        // the buffered path gives the client the right answer.
        FillState::Failed | FillState::Verified => Peek::Gone,
        FillState::Running => match progress.spool.clone() {
            Some(spool) => Peek::Anchor(TeeAnchor {
                attempt: progress.attempt,
                spool,
            }),
            None => Peek::Wait,
        },
    }
}

/// Wait for the fill to open a spool, which is also the moment upstream has
/// answered 200 and every pre-download refusal has had its say. `None` means the
/// fill ended first, or died — either way, no tee.
async fn tee_anchor(rx: &mut tokio::sync::watch::Receiver<FillProgress>) -> Option<TeeAnchor> {
    loop {
        match peek(rx) {
            Peek::Anchor(anchor) => return Some(anchor),
            Peek::Gone => return None,
            Peek::Wait => {}
        }
        rx.changed().await.ok()?;
    }
}

/// A bounded, generation-keyed set of artifact keys proven to exist in local
/// storage, so the warm-hit download path can skip its existence HEAD. A proxied
/// `GET /files/...` for an already-cached artifact needs no upstream work at all;
/// a positive hit here lets it serve the local bytes directly.
///
/// Safe by construction: an entry only ever *elides a HEAD and serves whatever is
/// on disk*. It can never authorize an upstream fetch — that stays gated by
/// [`eligible`] on the miss path — so no stale entry can breach the origin fence.
/// A bucket switch bumps `generation` and clears the map; a delete/prune is
/// bounded by [`PRESENCE_TTL`] (a stale hit serves a local 404, which the next
/// post-TTL GET heals by re-HEADing).
struct PresenceCache {
    ttl: Duration,
    max: usize,
    inner: Mutex<PresenceInner>,
}

#[derive(Default)]
struct PresenceInner {
    seen: HashMap<String, Instant>,
    generation: u64,
}

impl PresenceInner {
    /// Adopt `generation`, clearing every prior observation if it changed — a
    /// bucket switch invalidates existence proofs taken against the old bucket.
    fn reconcile(&mut self, generation: u64) {
        if self.generation != generation {
            self.seen.clear();
            self.generation = generation;
        }
    }
}

impl PresenceCache {
    fn new(ttl: Duration, max: usize) -> Self {
        Self {
            ttl,
            max,
            inner: Mutex::new(PresenceInner::default()),
        }
    }

    /// True when `key` was proven present within the TTL under this generation.
    fn present(&self, key: &str, generation: u64) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.reconcile(generation);
        inner
            .seen
            .get(key)
            .is_some_and(|seen| seen.elapsed() < self.ttl)
    }

    /// Remember that `key` exists locally under `generation`. Stays bounded like
    /// the listings cache: at the cap, drop expired entries, then clear outright
    /// if the live set alone still fills it (a re-HEAD per hot key, once).
    fn record(&self, key: &str, generation: u64) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.reconcile(generation);
        if inner.seen.len() >= self.max && !inner.seen.contains_key(key) {
            let ttl = self.ttl;
            inner.seen.retain(|_, seen| seen.elapsed() < ttl);
            if inner.seen.len() >= self.max {
                inner.seen.clear();
            }
        }
        inner.seen.insert(key.to_string(), Instant::now());
    }

    /// Forget `key`'s existence proof after its artifact is deleted or pruned, so
    /// the next proxied download re-HEADs (and re-mirrors) instead of serving a
    /// stale "present" that now points at a local 404. A hard drop, like the
    /// sibling caches' invalidate: this positive-only cache makes it a plain
    /// remove, and TTL is only the fallback for removals that can't reach here.
    fn invalidate(&self, key: &str) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .seen
            .remove(key);
    }
}

/// Is `pkg`'s `filename` condemned by a MAL advisory in the live snapshot?
/// Proxy listings are mirror-origin by definition, so this never reads origin —
/// the block applies unconditionally. A no-op (`false`) unless blocking is armed
/// and a snapshot is loaded; the common file is a pure hash probe.
fn advisory_blocks(state: &AppState, pkg: &str, filename: &str) -> bool {
    if !state.malware_block {
        return false;
    }
    let snap = state.advisory_snapshot();
    if !snap.has_block_data() {
        return false;
    }
    let version = infer_version_from_filename(filename);
    !snap.blocking(pkg, version.as_deref()).is_empty()
}

/// The dependency scope is still being read and does not yet include this
/// name: answer a retryable outage, never "no such package".
#[derive(Debug)]
pub(crate) struct ScopePending;

impl std::fmt::Display for ScopePending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("still reading the included packages' dependencies; retry shortly")
    }
}

impl std::error::Error for ScopePending {}

/// May this package be served from upstream at all? Private names, the reserved
/// private names, and (when a scope is configured) names outside the allowlist
/// never fall through — that is the entire defense.
pub async fn eligible(state: &AppState, storage: &dyn Storage, pkg: &str) -> Result<bool> {
    if state.private.matches(pkg) {
        return Ok(false);
    }
    // The package allowlist is fail-closed and gates before the origin read, so
    // an unapproved name never touches storage — except when the check would
    // wait on a dependency refresh: a private name is answered first, so a slow
    // upstream can't hold a private package's page hostage.
    if let Some(proxy) = &state.proxy {
        if proxy.name_fully_denied(pkg) {
            return Ok(false);
        }
        if proxy.scope_would_wait(pkg) && is_private(storage, pkg).await? {
            return Ok(false);
        }
        if !proxy.in_scope(pkg).await? {
            return Ok(false);
        }
    }
    Ok(!is_private(storage, pkg).await?)
}

async fn is_private(storage: &dyn Storage, pkg: &str) -> Result<bool> {
    Ok(origin::read_origin(storage, pkg)
        .await?
        .is_some_and(|owner| owner == origin::PRIVATE))
}

impl Proxy {
    pub fn new(
        upstream: &str,
        mirror: ResolvedMirror,
        allow_insecure: bool,
        allow_hosts: &[String],
        allow_cidrs: &[String],
    ) -> Result<Self> {
        let upstream = upstream.trim_end_matches('/').to_string();
        // Plaintext http:// lets a network MITM forge both the artifact bytes and
        // the sha256 we check them against (they arrive over the same channel), so
        // the hash stops being a control. Refuse it unless explicitly overridden.
        if upstream.starts_with("http://") {
            if !allow_insecure {
                bail!(
                    "--proxy-upstream is plaintext http://, which lets a network MITM forge \
                     artifact hashes; pass --allow-insecure-upstream to override, got '{upstream}'"
                );
            }
        } else if !upstream.starts_with("https://") {
            bail!("--proxy-upstream must be an https URL, got '{upstream}'");
        }
        // The upstream host is operator-chosen and trusted, so it is exempt from
        // the SSRF guard below — an internal/self-hosted mirror on a private range
        // must still work. Listing-derived targets get no such pass unless the
        // operator allow-lists them explicitly (--proxy-allow-host / -cidr).
        let guard = Arc::new(Guard::new(&upstream, allow_hosts, allow_cidrs)?);
        // Derive the request-time allowlist index once. Empty scope → None, so
        // name_in_scope() short-circuits to "allow all" (the open-proxy default).
        let scope = (!mirror.include_packages.is_empty()).then(|| {
            let mut map: ScopeMap = HashMap::new();
            for spec in &mirror.include_packages {
                map.entry(spec.name.clone())
                    .or_default()
                    .push(spec.specifiers.clone());
            }
            Arc::new(map)
        });
        let follow = (mirror.include_dependencies && scope.is_some()).then(|| Follow {
            listed: mirror
                .include_packages
                .iter()
                .map(|s| s.name.clone())
                .collect(),
            known: Mutex::default(),
            published: Mutex::default(),
            asked: AtomicU64::new(1),
            wake: tokio::sync::Notify::new(),
            answered: tokio::sync::watch::Sender::new(0),
        });
        let denylist = Arc::new(crate::denylist::Denylist::from_specs(
            &mirror.exclude_packages,
        ));
        Ok(Self {
            upstream,
            mirror,
            scope: RwLock::new(scope),
            follow,
            denylist,
            client: crate::upstream_tls::apply(
                Client::builder()
                    .user_agent(
                        "pypiron-proxy/0.1 (+https://github.com/blackthorn-interstellar/pypiron)",
                    )
                    .connect_timeout(Duration::from_secs(10))
                    // Inactivity timeout between reads, reset on each chunk: an
                    // upstream that connects then stalls mid-stream can't hang a
                    // client-facing request forever. Does NOT bound large
                    // downloads that keep streaming. download_verified's retry
                    // loop turns the resulting error into a clean retry.
                    .read_timeout(Duration::from_secs(30))
                    // Refuse to connect to private/loopback/link-local addresses.
                    // A malicious or MITM'd upstream listing can point a companion
                    // URL (.metadata/.provenance — unlike artifacts, not
                    // hash-gated) at an internal endpoint and read the response
                    // back through us. The resolver catches name targets (and
                    // DNS-rebind on redirects); IP-literal targets are caught by
                    // the pre-flight in guarded_get, which is why redirects are
                    // followed manually below.
                    .dns_resolver(Arc::new(SsrfGuardResolver::new(guard.clone())))
                    // Honor ambient HTTPS_PROXY/HTTP_PROXY/ALL_PROXY/NO_PROXY so
                    // the proxy works behind a corporate forward proxy. With a
                    // proxy the resolver no longer sees name targets (the proxy
                    // resolves them), so name-based SSRF enforcement moves to the
                    // proxy egress ACL; the IP-literal pre-flight in guarded_get
                    // still blocks every internal literal on every hop. See the
                    // `ssrf` module docs.
                    .redirect(reqwest::redirect::Policy::none()),
            )
            .build()?,
            guard,
            listings: Mutex::new(HashMap::new()),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            presence: PresenceCache::new(PRESENCE_TTL, MAX_PRESENCE),
        })
    }

    /// Acquire the single-flight slot for an artifact key, waiting if another
    /// task is already downloading it. Held until the returned guard drops.
    async fn acquire_download_slot(&self, key: &str) -> DownloadSlot {
        let guard = self.slot_entry(key).lock_owned().await;
        DownloadSlot {
            inflight: self.inflight.clone(),
            key: key.to_string(),
            _guard: guard,
        }
    }

    /// Take the single-flight slot only if it is free. `None` means another task
    /// is already filling this key, so the caller is a waiter and must block on
    /// [`acquire_download_slot`] instead. Splitting the two lets the *leader*
    /// hand its slot to a spawned fill (which outlives the request) while a
    /// waiter keeps waiting on the request's own task, where its wait costs
    /// nothing if the client goes away.
    fn try_acquire_download_slot(&self, key: &str) -> Option<DownloadSlot> {
        let guard = self.slot_entry(key).try_lock_owned().ok()?;
        Some(DownloadSlot {
            inflight: self.inflight.clone(),
            key: key.to_string(),
            _guard: guard,
        })
    }

    /// The per-key mutex, created on first use. Taken under the map lock, which
    /// [`DownloadSlot::drop`] also holds while it decides whether to prune, so a
    /// clone taken here is never an entry that is about to be dropped.
    fn slot_entry(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key.to_string())
            .or_default()
            .clone()
    }

    /// Is this (PEP 503-normalized) name allowed to fall through to upstream?
    /// True when no scope is configured; otherwise true only if the name is on
    /// the allowlist. The version axis is enforced separately, per file, in the
    /// listing.
    pub(crate) fn name_in_scope(&self, pkg: &str) -> bool {
        self.scope().is_none_or(|m| m.contains_key(pkg))
    }

    fn scope(&self) -> Option<Arc<ScopeMap>> {
        self.scope.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// [`Self::name_in_scope`], once the dependency scope has caught up with
    /// every refresh asked for: an installer asks for a new dependency right
    /// after the listing that revealed it. A dependency's versions can widen
    /// too, so every name but an included one waits. A refresh that outlasts
    /// [`FOLLOW_WAIT`] (a stuck upstream) leaves a name already in scope served
    /// on its last-known versions; one not yet in scope is [`ScopePending`].
    async fn in_scope(&self, pkg: &str) -> Result<bool> {
        if let Some(follow) = self.follow.as_ref().filter(|f| !f.listed.contains(pkg)) {
            if !follow.settled().await && !self.name_in_scope(pkg) {
                return Err(ScopePending.into());
            }
        }
        Ok(self.name_in_scope(pkg))
    }

    /// Would [`Self::in_scope`] wait on a dependency refresh for this name?
    fn scope_would_wait(&self, pkg: &str) -> bool {
        self.follow
            .as_ref()
            .is_some_and(|f| !f.listed.contains(pkg) && f.pending())
    }

    /// True only for a bare (whole-name) exclude — what the pull gate
    /// ([`Proxy::eligible`]) keys on to keep a fully-denied name from ever being
    /// fetched upstream.
    pub(crate) fn name_fully_denied(&self, pkg: &str) -> bool {
        self.denylist.name_fully_denied(pkg)
    }

    /// The shared exclude denylist, so `AppState` (and thence the index rebuild,
    /// the `/project/` page, and the reconcile) rules through the same predicate.
    pub(crate) fn denylist(&self) -> Arc<crate::denylist::Denylist> {
        self.denylist.clone()
    }

    /// Does this file satisfy the name's version constraints? Allowed when no
    /// scope is configured; otherwise the name's constraints gate the version.
    /// A name in scope is reached here only after [`name_in_scope`], so a miss
    /// in the map can't normally happen — treated fail-closed if it does. The
    /// denylist subtracts on top: an excluded file never serves even if in scope.
    fn version_in_scope(&self, pkg: &str, filename: &str) -> bool {
        let allowed = match self.scope() {
            None => true,
            Some(map) => map
                .get(pkg)
                .is_some_and(|constraints| crate::denylist::version_allowed(constraints, filename)),
        };
        allowed && !self.denylist.file_denied(pkg, filename)
    }

    pub fn upstream(&self) -> &str {
        &self.upstream
    }

    /// Drop a deleted/pruned artifact's presence proof so the warm-hit path stops
    /// eliding its HEAD. Without this a delete inside [`PRESENCE_TTL`] would keep
    /// a stale "present" that serves a local 404 where a re-mirror should happen;
    /// the admin delete path calls this the same place it invalidates the presign
    /// cache. `key` is the artifact storage key (`packages/<pkg>/<file>`).
    pub fn invalidate_presence(&self, key: &str) {
        self.presence.invalidate(key);
    }

    /// The package page rendered from the upstream listing. `Missing` and
    /// `Unreachable` both mean "serve the local index instead"; with no local
    /// index, `Unreachable` is a retryable outage rather than a 404.
    pub async fn package_index(
        &self,
        state: &AppState,
        storage: &dyn Storage,
        pkg: &str,
        json: bool,
    ) -> Result<Listing<RenderedIndex>> {
        let found = match self.listing(state, pkg).await {
            Listing::Found(found) => found,
            Listing::Missing => return Ok(Listing::Missing),
            Listing::Unreachable => return Ok(Listing::Unreachable),
        };
        if !state.buckets.is_multi() {
            return Ok(Listing::Found(if json {
                found.json.clone()
            } else {
                found.html.clone()
            }));
        }
        // Fetch first, then observe local fences. A freeze/delete that landed
        // during the slow upstream request is therefore suppressed in this
        // response instead of advertising a URL the file route rejects.
        let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
        let names: std::collections::HashSet<String> = storage
            .list_dir_entries(&prefix)
            .await?
            .into_iter()
            .filter_map(|entry| entry.key.strip_prefix(&prefix).map(str::to_string))
            .collect();
        let visible: Vec<FileMetadata> = found
            .files
            .iter()
            .filter(|file| {
                ![TOMBSTONE_SUFFIX, FROZEN_SUFFIX, MIRROR_QUARANTINED_SUFFIX]
                    .iter()
                    .any(|suffix| names.contains(&format!("{}{suffix}", file.filename)))
            })
            // Re-scrub blocked files here too: `found` is cached for a TTL, so a
            // MAL advisory that arrived after the listing was fetched is caught at
            // render time even against a stale cache entry.
            .filter(|file| !advisory_blocks(state, pkg, &file.filename))
            .map(SimpleFile::as_file_metadata)
            .collect();
        let render_files: &[FileMetadata] = if found.status.status.blocks_downloads() {
            &[]
        } else {
            &visible
        };
        Ok(Listing::Found(if json {
            rendered(render::pep691_project_json(
                pkg,
                render_files,
                &found.status,
            ))
        } else {
            rendered(render::pep503_project_html(
                pkg,
                render_files,
                &found.status,
            ))
        }))
    }

    /// Warm-hit fast path for a proxied artifact download. `Ok(true)` means the
    /// artifact is already in local storage and can be served as-is — the caller
    /// must NOT run the eligibility fence, because serving already-local bytes is
    /// always safe (the fence gates *fetching* from upstream, never local
    /// delivery). `Ok(false)` means "not known present"; the caller runs the full
    /// [`eligible`] → [`Proxy::ensure_artifact_cached`] path.
    ///
    /// A presence-cache hit costs zero storage ops; a miss pays one HEAD and,
    /// when the file is present, records it so the next hit is free. This is what
    /// turns a warm proxied download from three storage ops (origin read +
    /// existence HEAD + serve) into one (serve).
    pub async fn artifact_cached_locally(
        &self,
        storage: &dyn Storage,
        key: &str,
        generation: u64,
    ) -> Result<bool> {
        if self.presence.present(key, generation) {
            return Ok(true);
        }
        if storage.head_exists(key).await? {
            self.presence.record(key, generation);
            return Ok(true);
        }
        Ok(false)
    }

    /// Would `exclude-yanked` refuse to pull this file from upstream? The gate
    /// is on the *fill*, not the listing: a file yanked upstream stays listed
    /// (PEP 592) so an exact pin resolves against bytes already cached, while
    /// nothing new is fetched for it — not the artifact, and not its PEP 658 /
    /// PEP 740 companions, which would otherwise pass through even though the
    /// artifact they annotate is refused. Callers return their "nothing here"
    /// value, which falls through to the local 404.
    fn fill_refused_by_yank(&self, file: &SimpleFile) -> bool {
        self.mirror.exclude_yanked && is_yanked(&file.yanked)
    }

    /// Download-verify-commit one artifact on a local miss.
    ///
    /// [`FillOutcome::Done`] with `filled: false` falls through to normal
    /// serving with nothing newly written — the file was already cached, isn't
    /// upstream (the local 404 is the right answer), or the fill lost a race.
    /// `filled: true` means THIS call committed a fresh cache fill, so the caller
    /// schedules the off-request-path peer replication notes. `Err` is a hard
    /// failure (storage outage, exhausted verification retries).
    ///
    /// [`FillOutcome::Streaming`] means the fill is big enough to tee: it is
    /// still running, and the caller should stream its spool to the client
    /// instead of waiting for the commit. `tee_allowed` is the caller's half of
    /// that decision (a plain whole-file GET); the size threshold is this side's.
    pub async fn ensure_artifact_cached(
        self: &Arc<Self>,
        state: &Arc<AppState>,
        storage: &Arc<dyn Storage>,
        pkg: &str,
        filename: &str,
        tee_allowed: bool,
    ) -> Result<FillOutcome> {
        if state.mutations_fenced() {
            bail!("bucket topology mismatch; proxy cache writes are fenced");
        }
        let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        // On the cold-miss path this repeats the HEAD `artifact_cached_locally`
        // just did (its miss is what routed us here) — accepted redundancy: it
        // only costs on the rare miss, and keeping it lets this function be called
        // on its own and stay correct (the slot racer below re-checks here too).
        if storage.head_exists(&key).await? {
            return Ok(FillOutcome::Done { filled: false });
        }
        // Serialize concurrent fetches of the *same* artifact (distinct files
        // still download in parallel). A racer that loses the race waits for the
        // slot on this task, re-checks, and finds the file already cached instead
        // of downloading its own copy. If the leader's fill failed there is
        // nothing cached to find, so the waiter inherits the slot and fills it
        // itself — exactly what it did when the fill ran inline.
        let (slot, leader) = match self.try_acquire_download_slot(&key) {
            Some(slot) => (slot, true),
            None => {
                let slot = self.acquire_download_slot(&key).await;
                if storage.head_exists(&key).await? {
                    return Ok(FillOutcome::Done { filled: false });
                }
                // A waiter that inherits the slot after a failed leader takes the
                // buffered path: teeing is the uncontended leader's privilege, so
                // there is exactly one tee per fill and never a second one racing
                // onto an attempt already in flight.
                (slot, false)
            }
        };
        // Detaching the fill is what stops pip's retries from livelocking, but it
        // also removed the only thing that used to bound concurrent downloads: a
        // client hanging up. [`MAX_DETACHED_FILLS`] is that bound. At the ceiling
        // the fill runs inline below, cancelled by a disconnect exactly as it was
        // before fills were detached.
        let detached = DETACHED_FILLS.try_acquire();
        // A tee needs the fill running *beside* the response, so an inline
        // overflow fill takes the buffered path — degraded, never wrong.
        let tee = match (&detached, tee_allowed && leader) {
            (Ok(_), true) => self.tee_size(state, pkg, filename).await,
            _ => None,
        };
        // One channel per fill, created here and moved into it. Nothing outlives
        // the fill that publishes on it, so a tee can never observe the previous
        // fill's position on this key.
        let (progress, rx) = match tee {
            Some(_) => {
                let (tx, rx) = tokio::sync::watch::channel(FillProgress::default());
                (Some(Progress { tx }), Some(rx))
            }
            None => (None, None),
        };
        let fill = Arc::clone(self).fill_artifact(
            Arc::clone(state),
            Arc::clone(storage),
            pkg.to_string(),
            filename.to_string(),
            slot,
            progress,
        );
        let Ok(permit) = detached else {
            debug!(
                %pkg, %filename, max = MAX_DETACHED_FILLS,
                "proxy: detached-fill ceiling reached; filling inline on the request"
            );
            return fill.await.map(|filled| FillOutcome::Done { filled });
        };
        // The fill runs in its own task, holding the slot for its whole life. A
        // client disconnect (pip's default timeout is ~15s; a large wheel from a
        // slow upstream is not) drops this handle, which detaches the task rather
        // than cancelling it — so the download finishes and the retry that pip
        // issues finds the artifact cached. Cancelling it was a livelock: every
        // retry restarted from byte zero and none ever completed.
        //
        // The permit rides with the task, so the ceiling counts fills that are
        // actually running, not requests that are still connected.
        //
        // Awaiting the handle keeps a connected client's semantics identical: the
        // same `Ok(bool)`, the same errors, in the same order.
        let fill = tokio::spawn(async move {
            let _permit = permit;
            fill.await
        });
        if let (Some(size), Some(mut rx)) = (tee, rx) {
            // Hold the response until the fill has an open spool — which is also
            // the moment upstream has answered 200, the origin is claimed mirror,
            // and every pre-download refusal (yank, malware, out of scope, name
            // gone private) has already had its say. Only then is a 200 with a
            // Content-Length honest. Anything else falls back to the buffered
            // path below, which is byte-for-byte today's behavior.
            if let Some(anchor) = tee_anchor(&mut rx).await {
                match tokio::fs::File::open(anchor.spool.as_path()).await {
                    Ok(file) => {
                        return Ok(FillOutcome::Streaming(Box::new(StreamingFill {
                            size,
                            attempt: anchor.attempt,
                            file,
                            rx,
                            fill: Some(fill),
                            metrics: Arc::clone(&state.metrics),
                        })))
                    }
                    Err(e) => {
                        warn!(%pkg, %filename, error=?e, "proxy: could not follow the fill spool; buffering instead")
                    }
                }
            }
        }
        match fill.await {
            Ok(filled) => filled.map(|filled| FillOutcome::Done { filled }),
            Err(join) => Err(anyhow::Error::new(join))
                .with_context(|| format!("proxy cache fill for '{pkg}/{filename}' did not finish")),
        }
    }

    /// The expected size of a cold-miss artifact when it is big enough to stream
    /// while it downloads, or `None` to serve it the buffered way. The listing
    /// was just fetched to render this package's page, so this is a map lookup on
    /// every path a real client takes.
    async fn tee_size(&self, state: &AppState, pkg: &str, filename: &str) -> Option<u64> {
        let threshold = state.proxy_stream_threshold?;
        let found = self.listing(state, pkg).await.found()?;
        let file = found.files.iter().find(|f| f.filename == filename)?;
        // No declared size, no Content-Length, no tee: a body of unknown length
        // cannot be promised to the client up front.
        file.size.filter(|size| *size >= threshold)
    }

    /// The cold-miss fill: download → verify → commit, holding the single-flight
    /// `slot` until it returns. Usually spawned by [`ensure_artifact_cached`],
    /// which awaits the handle, so every early return here is one the request
    /// sees; past the detached-fill ceiling it is awaited inline instead.
    /// `progress` is present only when a tee is following this fill.
    async fn fill_artifact(
        self: Arc<Self>,
        state: Arc<AppState>,
        storage: Arc<dyn Storage>,
        pkg: String,
        filename: String,
        slot: DownloadSlot,
        progress: Option<Progress>,
    ) -> Result<bool> {
        let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        let out = self
            .fill_and_commit(&state, &storage, &pkg, &filename, &key, progress.as_ref())
            .await;
        // Anything that ended this fill short of releasing the tail is a failure
        // for whoever is teeing: an early `Ok(false)` served no bytes at all, and
        // an `Err` died mid-body. A tee that already saw `Verified` has its tail
        // and has stopped listening, so a post-verification `Ok(false)` (a
        // demotion race losing the commit) leaves its finished body alone.
        if let Some(progress) = &progress {
            if !progress.is_verified() {
                progress.failed();
            }
        }
        // Explicit: the slot — and with it this key's single-flight exclusion —
        // is held until the commit is done, not until the download is.
        drop(slot);
        out
    }

    /// The fill proper. Split from [`fill_artifact`] only so every one of its
    /// early returns publishes a terminal state to an attached tee without each
    /// one having to remember to.
    async fn fill_and_commit(
        &self,
        state: &AppState,
        owned_storage: &Arc<dyn Storage>,
        pkg: &str,
        filename: &str,
        key: &str,
        progress: Option<&Progress>,
    ) -> Result<bool> {
        let storage = owned_storage.as_ref();
        if storage.head_exists(key).await? {
            return Ok(false);
        }
        // An unreachable upstream fails the fill rather than reading as "not
        // upstream": the request answers a retryable 503, not a 404.
        let found = match self.listing(state, pkg).await {
            Listing::Found(found) => found,
            Listing::Missing => return Ok(false),
            Listing::Unreachable => bail!("upstream listing for '{pkg}' is unreachable"),
        };
        let Some(file) = found.files.iter().find(|f| f.filename == filename) else {
            return Ok(false); // not upstream, or filtered out
        };
        if filename.len() > crate::sidecar::MAX_NEW_ARTIFACT_FILENAME_BYTES {
            warn!(%pkg, %filename, "proxy: refusing to cache a filename too long to store");
            return Ok(false);
        }

        if self.fill_refused_by_yank(file) {
            debug!(%pkg, %filename, "proxy: refusing to cache a file yanked upstream");
            return Ok(false);
        }

        // Malware fill refusal: never download-and-cache a version OSV condemns —
        // the server-side twin of uv's pre-sync check, so malware in the feed
        // before anyone here requests it never lands in storage. Origin is mirror
        // by definition on this path, so no origin read. The refused GET then
        // falls through to the byte gate (unclaimed origin → 403); nothing is
        // written here — no artifact, no sidecar, no origin claim.
        if state.malware_block {
            let snap = state.advisory_snapshot();
            if snap.has_block_data() {
                let version = infer_version_from_filename(filename);
                let ids = snap.blocking(pkg, version.as_deref());
                if !ids.is_empty() {
                    warn!(%pkg, %filename, advisories = ?ids, "proxy: refused malware fill");
                    return Ok(false);
                }
            }
        }

        // Claim before writing, exactly like sync: atomically, so a racing
        // first private upload can't merge worlds. Capture the claim's exact
        // identity (etag) so the pre-commit re-check below can prove the name is
        // still ours after a slow download. Losing to a private claim means this
        // name is no longer ours to serve.
        let claim_observation = match origin::read_origin_observation(storage, pkg).await? {
            Some(observed) if observed.state == origin::OriginState::Mirror => observed,
            Some(observed) if observed.state == origin::OriginState::Private => return Ok(false),
            // A caller that already saw the sentinel passes it through so the
            // claim goes straight to CAS instead of a guaranteed-losing create.
            observed => {
                let request = origin::ClaimRequest::new(
                    origin::MIRROR,
                    observed
                        .as_ref()
                        .filter(|value| value.state == origin::OriginState::Unclaimed),
                );
                let claim = origin::claim_origin(storage, pkg, request).await?;
                if claim.owner != origin::MIRROR {
                    return Ok(false);
                }
                match claim.etag {
                    Some(etag) => origin::OriginObservation {
                        state: origin::OriginState::Mirror,
                        etag,
                    },
                    None => origin::read_origin_observation(storage, pkg)
                        .await?
                        .filter(|value| value.state == origin::OriginState::Mirror)
                        .ok_or_else(|| anyhow!("mirror claim for '{pkg}' vanished after claim"))?,
                }
            }
        };

        info!(%pkg, %filename, upstream = %self.upstream, "proxy: caching artifact");
        let spool = match self.download_verified(state, pkg, file, progress).await {
            Ok(spool) => spool,
            Err(e) => {
                state
                    .metrics
                    .proxy_artifact_errors
                    .fetch_add(1, Ordering::Relaxed);
                // Empty-claim reclamation is deliberately audit-owned. A
                // failure-path release cannot distinguish a slow live writer
                // from an orphan and used to reopen names mid-publish.
                return Err(e);
            }
        };

        // Intent before truth, commit after (see worker.rs): a crash between
        // the artifact landing and the commit marker heals via stale intent.
        let intent = if state.buckets.is_multi() {
            crate::markers::Intent::open(state, owned_storage, pkg).await?
        } else {
            crate::markers::Intent::open_best_effort(state, owned_storage, pkg).await
        };

        let sidecar = Sidecar {
            // Upstream's digest, verified against the downloaded bytes.
            sha256: spool.sha256.clone(),
            size: spool.size,
            version: infer_version_from_filename(filename).unwrap_or_default(),
            // Upstream's true upload time: what keeps --exclude-newer honest.
            upload_time: file.upload_time.clone().unwrap_or_default(),
            requires_python: file.requires_python.clone(),
            yanked: file.yanked.clone(),
            // Proxy fills are mirror truth (§4/§6.2).
            origin: Some(origin::MIRROR.to_string()),
            yank_epoch: 0,
            upload_time_revision: 0,
            upload_epoch_ms: None,
            // A proxy fill is a cache, not a `sync --to` snapshot — but it still
            // replicates, asynchronously via a post-serve `_repl/` note (the
            // handler schedules them once this fill commits). The bit is only
            // provenance now, not a replication carve-out.
            snapshot: false,
            // A proxy cache fill replicates asynchronously (a `_repl/` note),
            // whose copy leg keeps its size-only check; capturing the provider
            // checksum here is a later refinement.
            store_checksum: None,
        };
        // Type the record before its artifact can exist. An orphan sidecar is
        // inert; an orphan artifact would be backfilled from a later private
        // package claim and launder public bytes into private truth. A sidecar
        // already standing (a crashed or refused earlier fill, a peer's copy) is
        // adopted only when it names these exact mirror bytes.
        let sidecar_key = sidecar_key(key);
        let sidecar_bytes = serde_json::to_vec(&sidecar)?;
        if !storage
            .put_if_absent(
                &sidecar_key,
                sidecar_bytes.clone(),
                Some("application/json"),
            )
            .await?
            && !adopt_standing_sidecar(storage, &sidecar_key, &sidecar, &sidecar_bytes).await?
        {
            intent.commit().await?;
            return Ok(false);
        }

        // Pre-commit re-check: a slow download can
        // straddle a mirror→private demotion (or a re-claim). Re-read the claim;
        // if it is no longer the exact MIRROR claim this fill started against,
        // abandon the fill and fall back to the private-name behavior — never
        // commit mirror bytes into a name that went private mid-flight. One
        // extra GET per first-time cache fill.
        // This is intentionally the final storage operation before the
        // conditional artifact PUT. The redundant tombstone HEAD that used to
        // sit here widened the fencing window and bought nothing: a mirror
        // claim and a private tombstone cannot legally coexist.
        match origin::read_origin_observation(storage, pkg).await? {
            Some(observed) if observed == claim_observation => {}
            _ => {
                info!(%pkg, %filename, "proxy: origin claim changed mid-download; abandoning fill");
                intent.commit().await?;
                return Ok(false);
            }
        }

        // Release a teed client's withheld tail here, and only here: the bytes
        // hash to what upstream declared AND the name is still the mirror claim
        // this fill started against. Deliberately before the storage commit —
        // the last 64 KiB must not wait on an S3 upload — so a commit that later
        // loses a race leaves the cache cold while the client keeps a body that
        // was verified before its first withheld byte moved.
        //
        // The one thing a download long enough to stream is long enough to
        // straddle is a *refusal* arriving mid-flight: a MAL advisory landing in
        // the feed, an upstream quarantine, an operator freeze. The buffered path
        // re-gates after the fill returns (serve.rs `advisory_byte_gate`); this
        // is the teed path's copy, and it makes the teed path at least as strict.
        // Refusing here aborts the tee mid-body (the tail never exists) and
        // leaves nothing but the inert sidecar behind.
        if let Some(progress) = progress {
            if self
                .release_refused(state, storage, pkg, filename, key)
                .await?
            {
                intent.commit().await?;
                return Ok(false);
            }
            progress.verified(spool.size);
        }

        // Sidecar is durable and the exact package claim is still ours. Publish
        // the artifact last; this read-to-PUT edge is the origin fence.
        let created = storage
            .put_file_if_absent(key, spool.path.path(), Some("application/octet-stream"))
            .await?;
        if !created {
            // Another mirror writer won the immutable filename. Its own
            // sidecar protocol completes the record; never overwrite it with
            // metadata paired to our stale observation.
            intent.commit().await?;
            return Ok(false);
        }
        // Multi-bucket demotion can still win after the pre-PUT read. The
        // record is already typed mirror, so never delete through a cross-object
        // race; private precedence will quarantine and suppress it. A lone
        // bucket cannot demote concurrently and skips this extra origin GET.
        if !crate::publish::post_publish_mirror_claim_is_current(
            state,
            storage,
            pkg,
            &claim_observation,
        )
        .await
        .context("re-check mirror claim after artifact publish")?
        {
            intent.commit().await?;
            info!(%pkg, %filename, "proxy: origin changed after artifact publish; leaving typed mirror loser");
            return Ok(false);
        }
        if state.buckets.is_multi() && storage.head_exists(&frozen_key(key)).await? {
            // The marker suppresses the filename immediately. Leave the typed
            // body for freeze recovery; deleting here would create another
            // cross-object race with staged private promotion.
            intent.commit().await?;
            return Ok(false);
        }
        if filename.ends_with(".whl") && file.has_core_metadata() {
            // Best-effort, like sync: a missing companion only costs the
            // resolver a wheel download.
            if let Some(md) = self.fetch_metadata_url(pkg, file).await {
                let _ = storage
                    .put_if_absent(
                        &metadata_key(key),
                        md.to_vec(),
                        Some("text/plain; charset=utf-8"),
                    )
                    .await;
            }
        }
        if let Some(prov_url) = &file.provenance {
            // PEP 740 provenance, relayed verbatim alongside the artifact.
            // Best-effort like metadata: a missing companion only drops the
            // supply-chain signal, never the artifact.
            if let Some(prov) = self.fetch_provenance_url(pkg, prov_url).await {
                let _ = storage
                    .put_if_absent(
                        &provenance_key(key),
                        prov.to_vec(),
                        Some("application/json"),
                    )
                    .await;
            }
        }
        intent.commit().await?;
        state
            .metrics
            .proxy_artifacts_cached
            .fetch_add(1, Ordering::Relaxed);
        // A fresh fill committed: signal the caller to schedule the peer
        // replication notes off the request's critical path.
        Ok(true)
    }

    /// The last chance to refuse a teed body, run immediately before the withheld
    /// tail is released. `true` means abort: the download outlived the decision
    /// that let it start.
    ///
    /// Three refusals can land mid-download, and each is judged the way the byte
    /// gate judges it: an OSV MAL advisory for this version (armed by
    /// `--malware-block`), an upstream PEP 792 quarantine of the project
    /// (enforced whatever the malware toggle says), and a freeze marker on the
    /// filename. Origin is mirror by definition on this path, so none of them
    /// needs the byte gate's private-origin read.
    async fn release_refused(
        &self,
        state: &AppState,
        storage: &dyn Storage,
        pkg: &str,
        filename: &str,
        key: &str,
    ) -> Result<bool> {
        let snap = state.advisory_snapshot();
        if snap.is_quarantined(pkg) {
            warn!(%pkg, %filename, "proxy: project quarantined mid-download; aborting the streamed body");
            return Ok(true);
        }
        if state.malware_block {
            let version = infer_version_from_filename(filename);
            let ids = snap.blocking(pkg, version.as_deref());
            if !ids.is_empty() {
                warn!(%pkg, %filename, advisories = ?ids, "proxy: malware advisory landed mid-download; aborting the streamed body");
                return Ok(true);
            }
        }
        if state.buckets.is_multi() && storage.head_exists(&frozen_key(key)).await? {
            warn!(%pkg, %filename, "proxy: filename frozen mid-download; aborting the streamed body");
            return Ok(true);
        }
        Ok(false)
    }

    /// PEP 658 companion for a not-yet-cached wheel, fetched from upstream
    /// and served without storage writes. `None` falls back to a local 404.
    pub async fn fetch_metadata(
        &self,
        state: &AppState,
        pkg: &str,
        metadata_filename: &str,
    ) -> Option<bytes::Bytes> {
        let base = metadata_filename.strip_suffix(METADATA_SUFFIX)?;
        let found = self.listing(state, pkg).await.found()?;
        let file = found.files.iter().find(|f| f.filename == base)?;
        if self.fill_refused_by_yank(file) {
            debug!(%pkg, %base, "proxy: refusing the companion of a file yanked upstream");
            return None;
        }
        if !file.has_core_metadata() {
            return None;
        }
        self.fetch_metadata_url(pkg, file).await
    }

    async fn fetch_metadata_url(&self, pkg: &str, file: &SimpleFile) -> Option<bytes::Bytes> {
        let url = match self.resolve_url(pkg, &file.url) {
            Ok(base) => match reqwest::Url::parse(&format!("{base}{METADATA_SUFFIX}")) {
                Ok(url) => url,
                Err(e) => {
                    warn!(%pkg, error=?e, "proxy: unresolvable upstream metadata URL");
                    return None;
                }
            },
            Err(e) => {
                warn!(%pkg, error=?e, "proxy: unresolvable upstream file URL");
                return None;
            }
        };
        let resp = match ssrf::guarded_get(
            &self.client,
            &self.guard,
            url.clone(),
            Some(SMALL_FETCH_TIMEOUT),
        )
        .await
        .and_then(|r| r.error_for_status().map_err(Into::into))
        {
            Ok(resp) => resp,
            Err(e) => {
                warn!(%url, error=?e, "proxy: upstream metadata fetch refused/failed");
                return None;
            }
        };
        let body = read_capped(resp, crate::wheel::MAX_METADATA_BYTES, url.as_str()).await?;
        // Defense-in-depth (Claim 10): if the listing carried a PEP 714/658
        // core-metadata digest, the fetched bytes must match it. Fail closed on
        // mismatch — a hostile or MITM'd listing can otherwise reflect arbitrary
        // bytes to the client as this wheel's `.metadata`.
        if let Some(expected) = file.core_metadata_sha256() {
            let actual = sha256_hex(&body);
            if !actual.eq_ignore_ascii_case(expected) {
                warn!(%url, expected, actual, "proxy: .metadata sha256 mismatch; refusing");
                return None;
            }
        }
        Some(body)
    }

    /// PEP 740 provenance for a not-yet-cached file, fetched from upstream and
    /// served without storage writes. `None` falls back to a local 404.
    pub async fn fetch_provenance(
        &self,
        state: &AppState,
        pkg: &str,
        provenance_filename: &str,
    ) -> Option<bytes::Bytes> {
        let base = provenance_filename.strip_suffix(PROVENANCE_SUFFIX)?;
        let found = self.listing(state, pkg).await.found()?;
        let file = found.files.iter().find(|f| f.filename == base)?;
        if self.fill_refused_by_yank(file) {
            debug!(%pkg, %base, "proxy: refusing the companion of a file yanked upstream");
            return None;
        }
        let prov_url = file.provenance.as_ref()?;
        self.fetch_provenance_url(pkg, prov_url).await
    }

    async fn fetch_provenance_url(&self, pkg: &str, prov_url: &str) -> Option<bytes::Bytes> {
        // The upstream provenance URL is authoritative (absolute on PyPI), but
        // resolve relative ones against the index page just like file URLs.
        let url = match self.resolve_url(pkg, prov_url) {
            Ok(url) => url,
            Err(e) => {
                warn!(%pkg, error=?e, "proxy: unresolvable upstream provenance URL");
                return None;
            }
        };
        let resp = match ssrf::guarded_get(
            &self.client,
            &self.guard,
            url.clone(),
            Some(SMALL_FETCH_TIMEOUT),
        )
        .await
        .and_then(|r| r.error_for_status().map_err(Into::into))
        {
            Ok(resp) => resp,
            Err(e) => {
                warn!(%url, error=?e, "proxy: upstream provenance fetch refused/failed");
                return None;
            }
        };
        // No listing-supplied digest exists for provenance (PEP 740), so it stays
        // trust-on-fetch; the SSRF guard above is what bounds where it came from.
        read_capped(resp, crate::wheel::MAX_METADATA_BYTES, url.as_str()).await
    }

    /// The filtered upstream listing for `pkg`, served from cache within
    /// [`LISTING_TTL`]. Concurrent misses share one fetch. On an upstream error
    /// the last good listing is reused (already-resolved installs keep working
    /// through blips) until it is [`LISTING_MAX_STALE`] old; with nothing to
    /// reuse the package is served local-only for [`LISTING_ERROR_TTL`], so a
    /// dead upstream isn't a per-request timeout and a recovered one is used
    /// again within seconds.
    async fn listing(&self, state: &AppState, pkg: &str) -> Listing<Arc<Found>> {
        if let Some(cached) = self.cached_listing(pkg) {
            return cached;
        }
        // Single-flight on the fill slot map; artifact keys start `packages/`,
        // so this key can't collide. A waiter that already has an answer (an
        // expired entry still inside the stale bound) serves it rather than
        // queue behind a refresh that may run the whole fetch timeout; only a
        // cold miss waits, then rechecks and takes the leader's result. Held
        // through the status persist and the cache insert.
        let key = format!("listing:{pkg}");
        let _slot = match self.try_acquire_download_slot(&key) {
            Some(slot) => slot,
            None => {
                if let Some(stale) = self.stale_listing(pkg) {
                    return stale;
                }
                self.acquire_download_slot(&key).await
            }
        };
        if let Some(cached) = self.cached_listing(pkg) {
            return cached;
        }
        state
            .metrics
            .proxy_listing_fetches
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let fetched = self.fetch_listing(state, pkg).await;
        if let Ok(Listing::Found(found)) = &fetched {
            // Persist the observed upstream status durably. The in-memory listing
            // renders the freeze, but a lockfile-pinned `/files/...` download of an
            // already-cached artifact never re-fetches the listing — only the
            // durable `.project-status.json`, folded into the byte gate's
            // quarantined set by the audit sweep, blocks those bytes.
            self.persist_observed_status(state, pkg, &found.status)
                .await;
        }
        let mut map = self.listings.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let entry = match fetched {
            Ok(listing) => CacheEntry {
                listing,
                fetched: now,
                expires: now + LISTING_TTL,
            },
            Err(e) => {
                state
                    .metrics
                    .proxy_listing_errors
                    .fetch_add(1, Ordering::Relaxed);
                warn!(%pkg, upstream = %self.upstream, error=?e, "proxy: upstream listing fetch failed");
                after_error(map.remove(pkg), now, now.saturating_duration_since(started))
            }
        };
        let served = entry.served();
        if map.len() >= MAX_LISTINGS && !map.contains_key(pkg) {
            evict_listings(&mut map, now);
        }
        map.insert(pkg.to_string(), entry);
        served
    }

    /// Cached listing lookup: `None` when absent or expired.
    fn cached_listing(&self, pkg: &str) -> Option<Listing<Arc<Found>>> {
        let map = self.listings.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map.get(pkg)?;
        (Instant::now() < entry.expires).then(|| entry.served())
    }

    /// What a waiter may serve while another task refreshes `pkg`: any entry,
    /// expired or not, except a listing past [`LISTING_MAX_STALE`]. `None` when
    /// there is nothing to serve, so the caller waits for the refresh.
    fn stale_listing(&self, pkg: &str) -> Option<Listing<Arc<Found>>> {
        let map = self.listings.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map.get(pkg)?;
        let too_old = matches!(entry.listing, Listing::Found(_))
            && Instant::now().saturating_duration_since(entry.fetched) >= LISTING_MAX_STALE;
        (!too_old).then(|| entry.served())
    }

    /// Persist the upstream PEP 792 status so the byte gate blocks cached bytes
    /// of an upstream-quarantined project (see the call site). Mirror-origin
    /// only; an operator-authored (private-origin) status is never clobbered —
    /// re-judged on every CAS attempt (see [`reconcile_observed_status`]).
    /// Best-effort: a write failure only leaves the durable status one poll
    /// stale — the in-memory listing still renders the freeze.
    ///
    /// A read-triggered write, so the HTTP method gate never sees it: a node
    /// whose writes are fenced records nothing durable here and only holds the
    /// observation in its own byte gate, which fails closed until restart.
    async fn persist_observed_status(
        &self,
        state: &AppState,
        pkg: &str,
        observed: &crate::status::ProjectStatusDoc,
    ) {
        if state.mutations_fenced() {
            crate::advisories::hold_fenced_quarantine(
                &state.advisories,
                pkg,
                observed.status.blocks_downloads(),
            );
            return;
        }
        let pinned = state.pin();
        match reconcile_observed_status(pinned.storage.as_ref(), pkg, observed).await {
            // Only a real status change touches the shared quarantined set: a poll
            // that found upstream unchanged must not republish it on every listing
            // refresh. On a change, arm this node's byte gate and hand the
            // increment to the rest of the fleet — the audit sweep is repair, not
            // propagation.
            //
            // A second dirty marker follows the publish, whatever it returned. The
            // first (written with the status) keeps the index crash-safe; this one
            // is the retry for a failed publish, and it is what keeps a delayed
            // relay harmless: when this publish lands after an operator's newer
            // event and the rebuild for it, the worker drains this marker next,
            // re-derives the name from the durable status and publishes it last.
            Ok(true) => {
                let replicas = state.singleton_replicas(pinned.index);
                if let Err(e) = crate::advisories::arm_quarantine(
                    pinned.storage.as_ref(),
                    &replicas,
                    &state.advisories,
                    pkg,
                    observed.status.blocks_downloads(),
                )
                .await
                {
                    warn!(%pkg, error=?e, "proxy: publishing the upstream status to the fleet's quarantined set failed; the worker retries from the status");
                }
                if let Err(e) = crate::markers::mark_dirty(pinned.storage.as_ref(), pkg).await {
                    warn!(%pkg, error=?e, "proxy: could not mark the package dirty after publishing its status; the audit sweep repairs the set");
                }
            }
            Ok(false) => {}
            Err(e) => warn!(%pkg, error=?e, "proxy: failed to persist upstream project status"),
        }
    }

    /// Keep the dependency scope current for as long as the server runs: the
    /// included packages plus everything their servable releases require,
    /// exactly what `sync --include-dependencies` would mirror. A no-op unless
    /// `include-dependencies` is on.
    pub fn follow_dependencies(self: &Arc<Self>, state: Arc<AppState>) {
        if self.follow.is_none() {
            return;
        }
        let proxy = self.clone();
        tokio::spawn(async move { proxy.follow_loop(&state).await });
    }

    async fn follow_loop(&self, state: &AppState) {
        let Some(follow) = &self.follow else {
            return;
        };
        // What an earlier run read answers startup's ask at once; this
        // refresh then only catches up on what was published since.
        if self.load_followed(state, follow).await {
            let wants = follow.closure(&self.mirror, &state.private);
            self.publish_scope(follow, &wants);
            follow.answered.send_replace(1);
        }
        let mut retried = Instant::now();
        loop {
            if retried.elapsed() >= FOLLOW_INTERVAL {
                follow
                    .known
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failed
                    .clear();
                retried = Instant::now();
            }
            let upto = follow.asked.load(Ordering::Acquire);
            self.refresh_dependencies(state, follow).await;
            follow.answered.send_if_modified(|answered| {
                let newer = upto > *answered;
                *answered = (*answered).max(upto);
                newer
            });
            if follow.asked.load(Ordering::Acquire) > upto {
                continue;
            }
            let _ = tokio::time::timeout(FOLLOW_INTERVAL, follow.wake.notified()).await;
        }
    }

    /// Visit, round by round, every project not yet visited for the versions
    /// now wanted of it, reading the releases not read before, until nothing
    /// changes (see [`deps::pending`]).
    async fn refresh_dependencies(&self, state: &AppState, follow: &Follow) {
        let mut done: HashSet<(String, Want)> = HashSet::new();
        loop {
            let wants = follow.closure(&self.mirror, &state.private);
            self.publish_scope(follow, &wants);
            let round: Vec<(String, Want)> = wants
                .into_iter()
                .filter(|visit| !done.contains(visit))
                .collect();
            if round.is_empty() {
                break;
            }
            let names: Vec<String> = round.iter().map(|(name, _)| name.clone()).collect();
            stream::iter(names)
                .map(|name| async move { self.follow_project(state, follow, &name).await })
                .buffer_unordered(FOLLOW_CONCURRENCY)
                .collect::<()>()
                .await;
            done.extend(round);
        }
        self.save_followed(state, follow).await;
    }

    /// Read the requirements of `pkg`'s servable releases not read before. The
    /// files are exactly what the proxy would serve: the listing is already
    /// filtered by the mirror filters and the published scope.
    async fn follow_project(&self, state: &AppState, follow: &Follow, pkg: &str) {
        // Never ask upstream about a name the server owns, nor one excluded.
        if self.name_fully_denied(pkg) {
            return;
        }
        match origin::read_origin(state.pin().storage.as_ref(), pkg).await {
            Ok(Some(owner)) if owner == origin::PRIVATE => return,
            Ok(_) => {}
            Err(e) => {
                warn!(%pkg, error=?e, "proxy: could not read the origin of a dependency; skipping it this pass");
                return;
            }
        }
        let Some(found) = self.listing(state, pkg).await.found() else {
            debug!(%pkg, "proxy: a dependency has no upstream listing");
            return;
        };
        let unread = {
            let mut known = follow.known.lock().unwrap_or_else(|e| e.into_inner());
            let known = &mut *known;
            let files = || found.files.iter().filter(|f| !self.fill_refused_by_yank(f));
            let memo = known.memo.entry(pkg.to_string()).or_default();
            // Releases no longer served drop out, and what only they required.
            known.dirty |= memo.keep_only(files());
            let unread = memo.unread(files());
            // Owned, so the reads below hold no borrow across an await.
            unread
                .into_iter()
                .map(|(version, key, file)| (version, key, file.clone()))
                .collect::<Vec<_>>()
        };
        // `Err` = the fetch failed; `Ok(None)` = no metadata file to read.
        let read: Vec<_> = stream::iter(unread)
            .map(|(version, key, file)| async move {
                let requires = if file.has_core_metadata() {
                    self.fetch_metadata_url(pkg, &file)
                        .await
                        .map(|bytes| Some(crate::coremeta::parse(&bytes).requires_dist))
                        .ok_or(())
                } else {
                    Ok(None)
                };
                (version, key, requires)
            })
            .buffer_unordered(FOLLOW_CONCURRENCY)
            .collect()
            .await;
        let mut known = follow.known.lock().unwrap_or_else(|e| e.into_inner());
        let known = &mut *known;
        for (version, key, requires) in read {
            match requires {
                Ok(requires) => {
                    known
                        .memo
                        .entry(pkg.to_string())
                        .or_default()
                        .record(version, key, requires);
                    known.dirty = true;
                }
                Err(()) => {
                    known.failed.insert((pkg.to_string(), version));
                }
            }
        }
    }

    /// Make `wants` the scope. A project whose versions changed loses its
    /// cached listing, which was filtered by the old scope.
    fn publish_scope(&self, follow: &Follow, wants: &BTreeMap<String, Want>) {
        let mut published = follow.published.lock().unwrap_or_else(|e| e.into_inner());
        if *published == *wants {
            return;
        }
        let map: ScopeMap = wants
            .iter()
            .map(|(name, want)| {
                let constraints = match deps::specifiers(want) {
                    None => vec![None],
                    Some(specs) => specs.into_iter().map(Some).collect(),
                };
                (name.clone(), constraints)
            })
            .collect();
        *self.scope.write().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(map));
        {
            let mut listings = self.listings.lock().unwrap_or_else(|e| e.into_inner());
            for (name, want) in wants {
                if published.get(name) != Some(want) {
                    listings.remove(name);
                }
            }
        }
        info!(
            projects = wants.len(),
            dependencies = wants.len().saturating_sub(follow.listed.len()),
            "proxy: dependency scope widened"
        );
        *published = wants.clone();
    }

    /// A fetched listing with a release the follower has not read yet asks
    /// for a refresh, so its new dependencies come into scope.
    fn notice_releases(&self, pkg: &str, files: &[SimpleFile]) {
        let Some(follow) = &self.follow else {
            return;
        };
        let unread = {
            let known = follow.known.lock().unwrap_or_else(|e| e.into_inner());
            let none = DepsMemo::default();
            known
                .memo
                .get(pkg)
                .unwrap_or(&none)
                .unread(files.iter().filter(|f| !self.fill_refused_by_yank(f)))
                .into_iter()
                .any(|(v, _, _)| !known.failed.contains(&(pkg.to_string(), v)))
        };
        if unread {
            follow.ask();
        }
    }

    async fn load_followed(&self, state: &AppState, follow: &Follow) -> bool {
        let bytes = match state.pin().storage.get_bytes(FOLLOW_KEY).await {
            Ok(bytes) => bytes,
            Err(e) if crate::storage::is_not_found(&e) => return false,
            Err(e) => {
                warn!(error=?e, "proxy: could not read what was followed before; reading every release again");
                return false;
            }
        };
        match serde_json::from_slice::<SavedFollow>(&bytes) {
            Ok(saved) if saved.upstream == self.upstream => {
                let mut known = follow.known.lock().unwrap_or_else(|e| e.into_inner());
                known.memo = saved.projects.into_iter().collect();
                true
            }
            Ok(_) => false,
            Err(e) => {
                warn!(error=?e, "proxy: unreadable dependency memo; reading every release again");
                false
            }
        }
    }

    /// Best-effort: a lost save only means the next start reads more.
    async fn save_followed(&self, state: &AppState, follow: &Follow) {
        if state.mutations_fenced() {
            return;
        }
        let bytes = {
            let mut known = follow.known.lock().unwrap_or_else(|e| e.into_inner());
            if !std::mem::take(&mut known.dirty) {
                return;
            }
            serde_json::to_vec(&SavedFollow {
                upstream: self.upstream.clone(),
                projects: known
                    .memo
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            })
        };
        let result = match bytes {
            Ok(bytes) => {
                state
                    .pin()
                    .storage
                    .put_bytes(FOLLOW_KEY, bytes, Some("application/json"))
                    .await
            }
            Err(e) => Err(e.into()),
        };
        if let Err(e) = result {
            warn!(error=?e, "proxy: could not save the dependency memo");
        }
    }

    async fn fetch_listing(&self, state: &AppState, pkg: &str) -> Result<Listing<Arc<Found>>> {
        let Some(index) =
            simple::fetch_index(&self.client, &self.upstream, pkg, Some(SMALL_FETCH_TIMEOUT))
                .await?
        else {
            return Ok(Listing::Missing);
        };
        // Relay the upstream PEP 792 status verbatim (default active). An
        // upstream-quarantined project returns no files anyway, so the marker
        // rides along with a naturally empty listing.
        let status = index.project_status.clone().unwrap_or_default();
        let fixes = self
            .mirror
            .fix_lifts_cooldown()
            .then(|| state.advisory_snapshot());
        let now = crate::clock::now_utc();
        let security_fix = |filename: &str| {
            fixes.as_ref().is_some_and(|snap| {
                infer_version_from_filename(filename)
                    .is_some_and(|v| snap.security_fix(pkg, &v, now, self.mirror.fix_settle))
            })
        };
        let files: Vec<SimpleFile> = index
            .files
            .into_iter()
            // No digest, no service: every artifact we hand out is verifiable.
            .filter(|f| f.sha256().is_some())
            // Every mirror gate except the yank one: a file yanked upstream stays
            // listed (with `data-yanked`, PEP 592) so an exact pin against bytes
            // we already cached still resolves. `exclude-yanked` gates the fill
            // instead — see `fill_refused_by_yank`. A release a settled
            // vulnerability advisory names as its fix skips the cooldown.
            .filter(|f| matches_mirror_listing(f, &self.mirror, security_fix(&f.filename)))
            // The scope's version axis: a pinned/ranged allowlist entry serves
            // only matching versions, exactly as `sync` mirrors only matching
            // versions. No scope → kept.
            .filter(|f| self.version_in_scope(pkg, &f.filename))
            // Malware scrub: a proxy listing is mirror-origin by definition, so a
            // MAL-blocked version is dropped unconditionally (no origin read).
            // Best-effort — the byte gate and fill refusal are the guarantees.
            .filter(|f| !advisory_blocks(state, pkg, &f.filename))
            .collect();
        self.notice_releases(pkg, &files);
        let metas: Vec<FileMetadata> = files.iter().map(SimpleFile::as_file_metadata).collect();
        let render_metas: &[FileMetadata] = if status.status.blocks_downloads() {
            &[]
        } else {
            &metas
        };
        Ok(Listing::Found(Arc::new(Found {
            html: rendered(render::pep503_project_html(pkg, render_metas, &status)),
            json: rendered(render::pep691_project_json(pkg, render_metas, &status)),
            files,
            status,
        })))
    }

    /// Stream the artifact to the upload spool (hashing on the way) and
    /// verify it against the upstream digest; same retry budget as sync —
    /// a truncated body and a flaky CDN look identical.
    async fn download_verified(
        &self,
        state: &AppState,
        pkg: &str,
        file: &SimpleFile,
        progress: Option<&Progress>,
    ) -> Result<FinishedSpool> {
        let expected = file
            .sha256()
            .ok_or_else(|| anyhow!("no upstream sha256 for {}", file.filename))?;
        let url = self.resolve_url(pkg, &file.url)?;
        let mut last_err = None;
        for attempt in 1..=DOWNLOAD_ATTEMPTS {
            match self
                .download_once(state, &url, file, progress, attempt)
                .await
            {
                Ok(spool) if spool.sha256.eq_ignore_ascii_case(expected) => return Ok(spool),
                Ok(spool) => {
                    last_err = Some(anyhow!(
                        "sha256 mismatch for {} (expected {expected}, got {})",
                        file.filename,
                        spool.sha256
                    ));
                }
                // A guard refusal is deterministic — the target is forbidden and
                // no retry changes that, so fail fast instead of backing off 6s.
                Err(e) if e.downcast_ref::<ssrf::Blocked>().is_some() => {
                    if let Some(progress) = progress {
                        progress.failed();
                    }
                    return Err(e);
                }
                Err(e) => last_err = Some(e),
            }
            // This attempt's bytes are dead. A tee bound to it aborts its
            // connection now — the retry below is for the cache, and its bytes
            // start again at zero, so they can never be spliced onto a body that
            // already carries a prefix of this attempt.
            if let Some(progress) = progress {
                progress.failed();
            }
            if attempt < DOWNLOAD_ATTEMPTS {
                warn!(file=%file.filename, attempt, "proxy: download failed; retrying");
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
            }
        }
        Err(last_err.expect("at least one attempt"))
    }

    async fn download_once(
        &self,
        state: &AppState,
        url: &reqwest::Url,
        file: &SimpleFile,
        progress: Option<&Progress>,
        attempt: u32,
    ) -> Result<FinishedSpool> {
        // Total budget for this attempt. The client's 30s inactivity timeout only
        // catches an upstream that stops; one that trickles resets it forever and
        // would hold this artifact's single-flight slot for hours.
        let deadline = download_deadline(file.size, download_grace());
        // Absolute byte ceiling for this attempt: the upstream-declared size when
        // there is one, else the unsized fallback. Without the fallback an
        // unsized body has no size gate at all — the read timeout bounds only
        // inactivity, and the digest check only runs once the spool is finished.
        let (ceiling, limit) = match file.size {
            Some(max) => (max, "declared size"),
            None => (DOWNLOAD_UNSIZED_MAX_BYTES, "unsized-artifact ceiling"),
        };
        let resp = ssrf::guarded_get(&self.client, &self.guard, url.clone(), Some(deadline))
            .await?
            .error_for_status()?;
        // Cheapest possible check first: an upstream that admits the body is over
        // the ceiling is refused before a single byte reaches the spool.
        if let Some(declared) = resp.content_length() {
            if declared > ceiling {
                bail!(
                    "{} declares Content-Length {declared}, over its {limit} ({ceiling} bytes)",
                    file.filename
                );
            }
        }
        let mut spool = UploadSpool::new(&state.spool_dir).await?;
        // Upstream answered 200 and there is a file to follow: this is where a
        // waiting tee attaches, which is why nothing before it can 200 a client.
        if let Some(progress) = progress {
            progress.started(attempt, spool.path());
        }
        let mut stream = resp.bytes_stream();
        let mut published = 0u64;
        while let Some(chunk) = stream.next().await {
            spool.write_chunk(&chunk?).await?;
            // Abort a body that overruns its ceiling before it can fill the disk
            // (the read timeout bounds time, not size, and an overrun fails the
            // sha check anyway) — a lying or absent Content-Length is caught here.
            if spool.size() > ceiling {
                bail!(
                    "{} overran its {limit} ({} > {ceiling} bytes)",
                    file.filename,
                    spool.size()
                );
            }
            // Publish per [`TEE_CHUNK`], not per reqwest chunk: upstream hands us
            // 8-16 KiB at a time, and flushing on each one is tens of thousands of
            // blocking-pool round trips on a large wheel for no benefit — the tee
            // reads in TEE_CHUNK units anyway. The tail is published unconditionally
            // when the body ends, so nothing is left unannounced.
            if let Some(progress) = progress {
                if spool.size() - published >= TEE_CHUNK {
                    // Flush first: `write_chunk` returns once tokio has *queued*
                    // the write, and a tee reading a second handle must never be
                    // told about bytes the OS has not seen yet.
                    spool.flush().await?;
                    published = spool.size();
                    progress.wrote(published);
                }
            }
        }
        if let Some(progress) = progress {
            spool.flush().await?;
            progress.wrote(spool.size());
        }
        spool.finish(true).await
    }

    /// PEP 691 file URLs may be absolute or relative; relative ones resolve
    /// against the index page URL (RFC 3986), which `Url::join` implements.
    fn resolve_url(&self, pkg: &str, raw: &str) -> Result<reqwest::Url> {
        let base = reqwest::Url::parse(&format!("{}/simple/{pkg}/", self.upstream))?;
        Ok(base.join(raw)?)
    }
}

/// Bring the durable `.project-status.json` into line with an observed upstream
/// status. Writes only when [`relay_replaces`] allows it, judged against every
/// CAS read rather than one earlier snapshot, so an operator event that lands
/// mid-relay is never overwritten. A change marks the package dirty so its own
/// leader rebuilds the (now file-less, if quarantined) index. Returns whether it
/// wrote, so the caller only touches the fleet's quarantined set on a real change.
async fn reconcile_observed_status(
    storage: &dyn Storage,
    pkg: &str,
    observed: &crate::status::ProjectStatusDoc,
) -> Result<bool> {
    let wrote = crate::status::advance_status_if(
        storage,
        pkg,
        observed,
        Some(crate::replicate::Origin::Mirror),
        |current| relay_replaces(current, observed),
    )
    .await?;
    if wrote.is_none() {
        return Ok(false);
    }
    crate::markers::mark_dirty(storage, pkg).await?;
    Ok(true)
}

/// May an observed upstream status replace `current`? Never over an
/// operator-authored (private-origin) event, never as a no-op rewrite, and never
/// to record the active default where no marker exists (absence already means
/// active). An untagged or mirror event yields: upstream is authoritative for a
/// mirror.
fn relay_replaces(
    current: Option<&crate::status::VersionedProjectStatus>,
    observed: &crate::status::ProjectStatusDoc,
) -> bool {
    match current {
        Some(cur) if cur.origin == Some(crate::replicate::Origin::Private) => false,
        Some(cur) => cur.doc != *observed,
        None => *observed != crate::status::ProjectStatusDoc::default(),
    }
}

/// Read an upstream companion body into memory with a hard ceiling. The local
/// wheel extractor already bounds `.metadata` at 16 MiB; the passthrough/cache
/// paths must too, or a hostile/huge upstream `.metadata`/`.provenance` body
/// (the timeout bounds time, not size) OOMs the node. `None` on overflow or a
/// read error — both fall back to a local 404, which is the existing contract.
async fn read_capped(resp: reqwest::Response, max: u64, url: &str) -> Option<bytes::Bytes> {
    let declared = resp.content_length();
    if declared.is_some_and(|len| len > max) {
        warn!(%url, max, "proxy: upstream body exceeds cap (Content-Length)");
        return None;
    }
    let mut buf: Vec<u8> = Vec::with_capacity(declared.map_or(0, |l| l.min(max)) as usize);
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                warn!(%url, error=?e, "proxy: upstream body read failed");
                return None;
            }
        };
        if buf.len() as u64 + chunk.len() as u64 > max {
            warn!(%url, max, "proxy: upstream body exceeds cap");
            return None;
        }
        buf.extend_from_slice(&chunk);
    }
    Some(bytes::Bytes::from(buf))
}

/// How long one attempt at an artifact of `expected_size` bytes may take in
/// total: `grace`, plus the time that many bytes need at
/// [`DOWNLOAD_FLOOR_BYTES_PER_SEC`], capped at [`DOWNLOAD_MAX`]. An unknown size
/// gets the cap — nothing to derive a budget from, and the mid-flight overrun
/// check has nothing to bite on either.
///
/// This is a *liveness* bound, not a bandwidth policy: the size term is sized so
/// that any upstream a real client could install from finishes far inside it,
/// while one that trickles is cut off instead of holding the artifact's
/// single-flight slot indefinitely.
fn download_deadline(expected_size: Option<u64>, grace: Duration) -> Duration {
    let Some(size) = expected_size else {
        return DOWNLOAD_MAX;
    };
    let transfer = Duration::from_secs(size.div_ceil(DOWNLOAD_FLOOR_BYTES_PER_SEC));
    grace.saturating_add(transfer).min(DOWNLOAD_MAX)
}

/// [`DOWNLOAD_GRACE`], unless the chaos suite shrank it. Test-only fault
/// injection in the style of `PYPIRON_FAULT_ABORT_AFTER_WRITES` (see
/// storage.rs): a blackbox test cannot afford to wait out a real 60s grace, and
/// the deadline is worth proving *fires*, not just proving it computes.
fn download_grace() -> Duration {
    std::env::var("PYPIRON_FAULT_DOWNLOAD_GRACE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(DOWNLOAD_GRACE, Duration::from_secs)
}

/// May a fill proceed over a sidecar that already stands at `key`? A standing
/// sidecar naming the same mirror bytes (sha256 and size) is the same record
/// seen at another time — a leftover from a crashed, cancelled or refused fill,
/// or a peer's copy whose body never landed — so it merges by the same-sha rule
/// replication uses (DESIGN §6.5), under a conditional write, and the fill goes
/// on. Anything else (different bytes, private or untyped, unparseable) is a
/// conflict this fill does not own: refuse, and leave it untouched.
async fn adopt_standing_sidecar(
    storage: &dyn Storage,
    key: &str,
    ours: &Sidecar,
    ours_bytes: &[u8],
) -> Result<bool> {
    let Some((bytes, etag)) = storage.get_with_etag(key).await? else {
        return Ok(false); // swept meanwhile; the next request refills
    };
    if bytes == ours_bytes {
        return Ok(true);
    }
    let Ok(current) = serde_json::from_slice::<Sidecar>(&bytes) else {
        return Ok(false);
    };
    if current.origin.as_deref() != Some(origin::MIRROR)
        || current.sha256 != ours.sha256
        || current.size != ours.size
    {
        return Ok(false);
    }
    let mirror = Some(crate::replicate::Origin::Mirror);
    if crate::replicate::sidecar_merge(ours, mirror, &current, mirror)
        != crate::replicate::MergeChoice::A
    {
        return Ok(true); // the standing sidecar ranks at least as high
    }
    Ok(storage
        .put_if_match(key, &etag, ours_bytes.to_vec())
        .await?
        .is_some())
}

/// Keep the listings cache bounded: first drop everything expired (those
/// would be re-fetched anyway), and if that didn't free a slot, evict the
/// soonest-expiring entries down to half the cap so this stays amortized O(1)
/// per insert.
fn evict_listings(map: &mut HashMap<String, CacheEntry>, now: Instant) {
    map.retain(|_, e| e.expires > now);
    if map.len() < MAX_LISTINGS {
        return;
    }
    let mut by_age: Vec<(String, Instant)> =
        map.iter().map(|(k, e)| (k.clone(), e.expires)).collect();
    by_age.sort_by_key(|(_, expires)| *expires);
    for (k, _) in by_age.into_iter().take(MAX_LISTINGS / 2) {
        map.remove(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn found_entry(fetched: Instant) -> CacheEntry {
        CacheEntry {
            listing: Listing::Found(Arc::new(Found {
                files: Vec::new(),
                html: rendered(String::new()),
                json: rendered(String::new()),
                status: crate::status::ProjectStatusDoc::default(),
            })),
            fetched,
            expires: fetched + LISTING_TTL,
        }
    }

    fn is_unreachable(entry: &CacheEntry, now: Instant) -> bool {
        matches!(entry.listing, Listing::Unreachable)
            && entry.fetched == now
            && entry.expires == now + LISTING_ERROR_TTL
    }

    #[test]
    fn a_failed_fetch_with_nothing_to_reuse_is_remembered_briefly() {
        let now = Instant::now();
        assert!(is_unreachable(&after_error(None, now, Duration::ZERO), now));
        // A cached 404 that then errors is not re-armed for a full TTL either.
        let missing = CacheEntry {
            listing: Listing::Missing,
            fetched: now,
            expires: now + LISTING_TTL,
        };
        let later = now + Duration::from_secs(61);
        assert!(is_unreachable(
            &after_error(Some(missing), later, Duration::ZERO),
            later
        ));
        // A fetch that stalled to its timeout is not retried sooner than it took.
        let took = Duration::from_secs(30);
        assert_eq!(after_error(None, now, took).expires, now + took);
    }

    #[test]
    fn a_failed_fetch_reuses_the_last_good_listing_without_aging_it() {
        let t0 = Instant::now();
        let now = t0 + Duration::from_secs(600);
        let kept = after_error(Some(found_entry(t0)), now, Duration::ZERO);
        assert!(kept.served().found().is_some());
        assert_eq!(kept.fetched, t0);
        assert_eq!(kept.expires, now + LISTING_TTL);
        // A second error later still measures from the real fetch.
        let again = now + LISTING_TTL;
        let kept = after_error(Some(kept), again, Duration::ZERO);
        assert_eq!(kept.fetched, t0);
        assert_eq!(kept.expires, again + LISTING_TTL);
    }

    #[test]
    fn the_last_good_listing_is_never_served_past_max_stale() {
        let t0 = Instant::now();
        // Revived just before the cap: the expiry is clamped to the cap.
        let edge = t0 + LISTING_MAX_STALE - Duration::from_secs(1);
        let kept = after_error(Some(found_entry(t0)), edge, Duration::ZERO);
        assert!(kept.served().found().is_some());
        assert_eq!(kept.expires, t0 + LISTING_MAX_STALE);
        // Past the cap: dropped.
        let past = t0 + LISTING_MAX_STALE + Duration::from_secs(1);
        assert!(is_unreachable(
            &after_error(Some(found_entry(t0)), past, Duration::ZERO),
            past
        ));
        assert!(is_unreachable(
            &after_error(Some(kept), past, Duration::ZERO),
            past
        ));
    }

    #[test]
    fn relative_and_absolute_upstream_urls_resolve() {
        let proxy = Proxy::new(
            "https://pypi.org/",
            ResolvedMirror::default(),
            false,
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(proxy.upstream(), "https://pypi.org");
        let abs = proxy
            .resolve_url("six", "https://files.pythonhosted.org/p/six.whl")
            .unwrap();
        assert_eq!(abs.as_str(), "https://files.pythonhosted.org/p/six.whl");
        let host_rel = proxy.resolve_url("six", "/files/six/six.whl").unwrap();
        assert_eq!(host_rel.as_str(), "https://pypi.org/files/six/six.whl");
        let page_rel = proxy.resolve_url("six", "six.whl").unwrap();
        assert_eq!(page_rel.as_str(), "https://pypi.org/simple/six/six.whl");
    }

    #[test]
    fn download_deadline_scales_with_the_declared_size() {
        // A small wheel is all grace: 3 KiB needs a second at the floor rate.
        assert_eq!(
            download_deadline(Some(3 * 1024), DOWNLOAD_GRACE),
            Duration::from_secs(61)
        );
        // 64 MiB at 64 KiB/s is 1024s of transfer on top of the grace.
        assert_eq!(
            download_deadline(Some(64 * 1024 * 1024), DOWNLOAD_GRACE),
            Duration::from_secs(60 + 1024)
        );
        // A zero-byte body still gets the grace, and the grace alone.
        assert_eq!(download_deadline(Some(0), DOWNLOAD_GRACE), DOWNLOAD_GRACE);
        // No declared size: nothing to derive a budget from, so the cap.
        assert_eq!(download_deadline(None, DOWNLOAD_GRACE), DOWNLOAD_MAX);
        // The cap holds against an absurd (or hostile) declared size, and against
        // a grace that would overflow the addition on its own.
        assert_eq!(
            download_deadline(Some(u64::MAX), DOWNLOAD_GRACE),
            DOWNLOAD_MAX
        );
        assert_eq!(download_deadline(Some(1), Duration::MAX), DOWNLOAD_MAX);
        // The chaos suite's shrunken grace still bounds a tiny file tightly.
        assert_eq!(
            download_deadline(Some(3 * 1024), Duration::from_secs(3)),
            Duration::from_secs(4)
        );
    }

    #[test]
    fn non_http_upstream_is_rejected() {
        let err = Proxy::new("ftp://mirror", ResolvedMirror::default(), false, &[], &[])
            .map(|_| ())
            .unwrap_err();
        assert!(err.to_string().contains("https"));
    }

    #[test]
    fn plaintext_http_upstream_needs_opt_in() {
        // Off by default: http:// lets a MITM forge the hashes we verify against.
        let err = Proxy::new(
            "http://mirror.internal",
            ResolvedMirror::default(),
            false,
            &[],
            &[],
        )
        .map(|_| ())
        .unwrap_err();
        assert!(err.to_string().contains("allow-insecure-upstream"));
        // Explicit opt-in accepts it.
        Proxy::new(
            "http://mirror.internal",
            ResolvedMirror::default(),
            true,
            &[],
            &[],
        )
        .unwrap();
    }

    fn spec(name: &str, specifiers: Option<&str>) -> crate::sync::PackageSpec {
        crate::sync::PackageSpec {
            name: name.to_string(),
            specifiers: specifiers.map(|s| VersionSpecifiers::from_str(s).unwrap()),
        }
    }

    #[test]
    fn empty_scope_allows_every_name_and_version() {
        let proxy = Proxy::new(
            "https://pypi.org",
            ResolvedMirror::default(),
            false,
            &[],
            &[],
        )
        .unwrap();
        assert!(proxy.name_in_scope("anything"));
        assert!(proxy.version_in_scope("anything", "anything-1.0.0.tar.gz"));
    }

    #[test]
    fn scope_gates_names_fail_closed() {
        let filter = ResolvedMirror {
            include_packages: vec![spec("requests", Some(">=2.20,<3")), spec("numpy", None)],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", filter, false, &[], &[]).unwrap();
        assert!(proxy.name_in_scope("requests"));
        assert!(proxy.name_in_scope("numpy"));
        assert!(
            !proxy.name_in_scope("flask"),
            "unapproved name must be denied"
        );
    }

    #[test]
    fn scope_gates_versions_like_sync() {
        let filter = ResolvedMirror {
            include_packages: vec![spec("requests", Some(">=2.20,<3")), spec("numpy", None)],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", filter, false, &[], &[]).unwrap();
        // Pinned name: only versions inside the range pass.
        assert!(proxy.version_in_scope("requests", "requests-2.31.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("requests", "requests-2.10.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("requests", "requests-3.0.0-py3-none-any.whl"));
        // Unparseable version under a constraint can't be proven to match → dropped.
        assert!(!proxy.version_in_scope("requests", "requests-garbage.whl"));
        // Bare (unpinned) name: every version passes, even an unparseable one.
        assert!(proxy.version_in_scope("numpy", "numpy-1.26.0-cp311-cp311-linux_x86_64.whl"));
        assert!(proxy.version_in_scope("numpy", "numpy-whatever.tar.gz"));
    }

    #[test]
    fn duplicate_entries_union_their_ranges() {
        // Two constrained entries for one name: a version matching either passes.
        let mirror = ResolvedMirror {
            include_packages: vec![spec("foo", Some("==1.0")), spec("foo", Some("==3.0"))],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", mirror, false, &[], &[]).unwrap();
        assert!(proxy.version_in_scope("foo", "foo-1.0-py3-none-any.whl"));
        assert!(proxy.version_in_scope("foo", "foo-3.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("foo", "foo-2.0-py3-none-any.whl"));
    }

    #[test]
    fn empty_scope_with_bare_deny_excludes_that_name_only() {
        let mirror = ResolvedMirror {
            exclude_packages: vec![spec("blocked", None)],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", mirror, false, &[], &[]).unwrap();
        assert!(proxy.name_in_scope("blocked"));
        assert!(proxy.name_fully_denied("blocked"));
        assert!(!proxy.version_in_scope("blocked", "blocked-1.0-py3-none-any.whl"));
        assert!(!proxy.name_fully_denied("allowed"));
        assert!(proxy.version_in_scope("allowed", "allowed-1.0-py3-none-any.whl"));
    }

    #[test]
    fn version_pinned_deny_drops_old_versions_only() {
        let mirror = ResolvedMirror {
            exclude_packages: vec![spec("demo", Some("<2"))],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", mirror, false, &[], &[]).unwrap();
        assert!(!proxy.name_fully_denied("demo"));
        assert!(!proxy.version_in_scope("demo", "demo-1.9-py3-none-any.whl"));
        assert!(proxy.version_in_scope("demo", "demo-2.0-py3-none-any.whl"));
        assert!(proxy.version_in_scope("demo", "demo-garbage.whl"));
    }

    #[test]
    fn deny_wins_over_allow() {
        let mirror = ResolvedMirror {
            include_packages: vec![spec("demo", None), spec("pinned", Some(">=1"))],
            exclude_packages: vec![spec("demo", None), spec("pinned", Some("<2"))],
            ..Default::default()
        };
        let proxy = Proxy::new("https://pypi.org", mirror, false, &[], &[]).unwrap();
        assert!(proxy.name_in_scope("demo"));
        assert!(proxy.name_fully_denied("demo"));
        assert!(!proxy.version_in_scope("demo", "demo-3.0-py3-none-any.whl"));
        assert!(!proxy.version_in_scope("pinned", "pinned-1.5-py3-none-any.whl"));
        assert!(proxy.version_in_scope("pinned", "pinned-2.0-py3-none-any.whl"));
    }

    #[test]
    fn presence_cache_records_then_serves_from_ram() {
        let cache = PresenceCache::new(Duration::from_secs(60), 4);
        assert!(!cache.present("packages/p/a.whl", 0));
        cache.record("packages/p/a.whl", 0);
        assert!(cache.present("packages/p/a.whl", 0));
    }

    #[test]
    fn presence_cache_generation_switch_invalidates() {
        // A bucket switch bumps the generation; an existence proof taken against
        // the old bucket must not be trusted for the new one.
        let cache = PresenceCache::new(Duration::from_secs(60), 4);
        cache.record("packages/p/a.whl", 0);
        assert!(cache.present("packages/p/a.whl", 0));
        assert!(
            !cache.present("packages/p/a.whl", 1),
            "old-generation observation must not survive a switch"
        );
        cache.record("packages/p/a.whl", 1);
        assert!(cache.present("packages/p/a.whl", 1));
    }

    #[test]
    fn presence_cache_expires_after_ttl() {
        let cache = PresenceCache::new(Duration::from_millis(10), 4);
        cache.record("packages/p/a.whl", 0);
        assert!(cache.present("packages/p/a.whl", 0));
        std::thread::sleep(Duration::from_millis(25));
        assert!(
            !cache.present("packages/p/a.whl", 0),
            "a stale observation must be re-verified, not trusted"
        );
    }

    #[test]
    fn presence_cache_stays_bounded() {
        // A flood of distinct downloads must not grow the map past its cap.
        let cache = PresenceCache::new(Duration::from_secs(60), 8);
        for i in 0..10_000 {
            cache.record(&format!("packages/p/f{i}.whl"), 0);
        }
        let len = cache.inner.lock().unwrap().seen.len();
        assert!(
            len <= 8,
            "presence cache grew to {len} entries past its cap"
        );
    }

    fn quarantined(reason: &str) -> crate::status::ProjectStatusDoc {
        crate::status::ProjectStatusDoc {
            status: crate::status::ProjectStatus::Quarantined,
            reason: Some(reason.into()),
        }
    }

    #[test]
    fn relay_yields_only_to_operator_events_and_no_ops() {
        use crate::replicate::Origin::{Mirror, Private};
        use crate::status::{ProjectStatusDoc, VersionedProjectStatus};
        let active = ProjectStatusDoc::default();
        let frozen = quarantined("upstream");
        let stored = |doc: &ProjectStatusDoc, origin| VersionedProjectStatus {
            doc: doc.clone(),
            epoch: 3,
            origin,
            etag: "etag".into(),
        };
        // (stored event, upstream observation, may the relay write?)
        let cases = [
            (Some(stored(&frozen, Some(Private))), &active, false),
            (Some(stored(&active, Some(Private))), &frozen, false),
            (Some(stored(&frozen, Some(Mirror))), &active, true),
            // Untagged yields: upstream is authoritative for a mirror.
            (Some(stored(&frozen, None)), &active, true),
            (Some(stored(&frozen, Some(Mirror))), &frozen, false),
            (None, &active, false),
            (None, &frozen, true),
        ];
        for (current, observed, want) in cases {
            assert_eq!(
                relay_replaces(current.as_ref(), observed),
                want,
                "stored {current:?} observed {observed:?}"
            );
        }
    }

    #[tokio::test]
    async fn relay_never_clobbers_a_private_status_that_lands_mid_cas() {
        use crate::replicate::Origin::{Mirror, Private};
        use crate::status::{encode_status, read_status_versioned, status_key};
        let storage = crate::storage::test_support::InMemStorage::default();
        crate::status::advance_status(&storage, "pkg", &quarantined("upstream"), Some(Mirror))
            .await
            .unwrap();
        // The operator's private quarantine lands right after the relay's read.
        let operator = quarantined("operator");
        storage.write_after_read(
            &status_key("pkg"),
            encode_status(&operator, 2, Some(Private)).unwrap(),
        );

        let wrote =
            reconcile_observed_status(&storage, "pkg", &crate::status::ProjectStatusDoc::default())
                .await
                .unwrap();

        assert!(
            !wrote,
            "a relay that lost to an operator event must not arm"
        );
        let stored = read_status_versioned(&storage, "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (stored.doc, stored.epoch, stored.origin),
            (operator, 2, Some(Private))
        );
        assert!(
            !storage
                .write_log()
                .iter()
                .any(|key| key.starts_with(crate::app::DIRTY_PREFIX)),
            "a declined relay marks nothing dirty"
        );
    }

    #[tokio::test]
    async fn a_relayed_status_change_marks_dirty_after_its_quarantine_publish() {
        // A delayed relay's publish can land after an operator's newer event and
        // the worker's rebuild for it. Only a marker written AFTER the publish
        // makes the worker re-derive the name and publish last.
        let storage = Arc::new(crate::storage::test_support::InMemStorage::default());
        let state = AppState::headless(storage.clone());
        let proxy = Proxy::new(
            "https://pypi.org",
            ResolvedMirror::default(),
            false,
            &[],
            &[],
        )
        .unwrap();
        // An existing set, so the publish is a logged conditional replace.
        let other = crate::advisories::QuarantinedEdit::One {
            name: "other".into(),
            blocked: true,
        };
        crate::advisories::publish_quarantined(storage.as_ref(), &[], &state.advisories, &other)
            .await
            .unwrap();
        proxy
            .persist_observed_status(&state, "pkg", &quarantined("upstream"))
            .await;
        let log = storage.write_log();
        let publish = log
            .iter()
            .position(|key| key == crate::advisories::QUARANTINED_KEY)
            .expect("the change was published");
        assert!(
            log[publish..]
                .iter()
                .any(|key| key.starts_with(crate::app::DIRTY_PREFIX)),
            "no dirty marker follows the publish: {log:?}"
        );
    }

    #[tokio::test]
    async fn relay_writes_a_real_change_once() {
        let storage = crate::storage::test_support::InMemStorage::default();
        let frozen = quarantined("upstream");
        assert!(reconcile_observed_status(&storage, "pkg", &frozen)
            .await
            .unwrap());
        // The same observation again is a no-op, not a second event.
        assert!(!reconcile_observed_status(&storage, "pkg", &frozen)
            .await
            .unwrap());
        let stored = crate::status::read_status_versioned(&storage, "pkg")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((stored.doc, stored.epoch), (frozen, 1));
    }
}
