//! On-demand mirroring: serve unknown packages from an upstream simple index,
//! caching artifacts in storage on first download.
//!
//! This is `sync`, made lazy. The same rules hold: the origin model is the
//! dependency-confusion defense, so a name claimed `private` (or inside
//! `--private-prefix`) never falls through to upstream, and the first
//! upstream artifact write claims the name `mirror` — atomically, exactly as
//! `sync` does. Artifacts are immutable, so caching them is trivially
//! correct; only the package *listing* needs freshness, and it is fetched
//! from the upstream PEP 691 API (which carries PEP 700 upload times, so
//! `--exclude-newer` stays historically correct) and cached for
//! [`LISTING_TTL`].
//!
//! Package pages are rendered from the upstream listing with our own
//! `/files/` URLs; artifact GETs download-verify-commit through the upload
//! spool (bounded memory, whatever the wheel size), then fall through to the
//! normal serving path. PEP 658 companions for not-yet-cached wheels are
//! passed through from upstream without writing anything — a resolver
//! probing dozens of candidate wheels must not stampede gigabytes into
//! storage. When upstream is down, callers fall back to the local
//! materialized index: already-cached packages keep installing.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context as _, Result};
use futures::StreamExt;
use pep440_rs::VersionSpecifiers;
use reqwest::Client;
use tracing::{debug, info, warn};

use crate::app::{AppState, PACKAGES_PREFIX};
use crate::hash::sha256_hex;
use crate::names::{infer_version_from_filename, matches_prefix};
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
/// Hard ceiling on cached listings. Each `/simple/:pkg` miss against a proxy
/// upstream inserts one entry (including negative `Missing` ones for 404s), and
/// there are unbounded distinct normalized names, so without a cap a stream of
/// nonexistent-package requests grows the map until OOM.
const MAX_LISTINGS: usize = 8192;
/// Listing and metadata fetches are small; bound them hard.
const SMALL_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
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
const TEE_HOLDBACK: u64 = 64 * 1024;
/// Read size for one poll of a teed body: the same 64 KiB the disk artifact
/// streamer uses (see `range::read_capacity`).
const TEE_CHUNK: u64 = 64 * 1024;

/// Distinguishes one fill's progress publications from the next fill's on the
/// same inflight entry — the entry outlives a fill whenever a waiter is queued
/// behind it. Never zero, so a never-published entry can't be mistaken for one.
static FILL_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn next_fill_epoch() -> u64 {
    FILL_EPOCH.fetch_add(1, Ordering::Relaxed) + 1
}

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

enum Listing {
    Found(Arc<Found>),
    /// Upstream said 404 — cached as hard as a hit, or a stampede of typo'd
    /// installs becomes an upstream hammer.
    Missing,
}

struct CacheEntry {
    listing: Listing,
    fetched: Instant,
}

pub struct Proxy {
    upstream: String,
    mirror: ResolvedMirror,
    /// The package scope as a fast name → version-constraints lookup, derived
    /// once from `mirror.include_packages`. `None` means no scope is configured (serve
    /// any non-private name — the open-proxy default). A present map is a
    /// fail-closed allowlist: a name absent from it never falls through, and a
    /// name's constraints gate which versions are served. A name may carry
    /// several constraints (duplicate list entries); a version passes if any
    /// allows it, matching `sync`'s union semantics.
    scope: Option<HashMap<String, Vec<Option<VersionSpecifiers>>>>,
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
    /// [`DownloadSlot`]), so it stays bounded by live concurrency, not by the
    /// number of distinct artifacts ever proxied.
    inflight: InflightMap,
    /// Warm-hit accelerator: artifact keys proven present in local storage, so a
    /// repeat proxied download skips its existence HEAD. See [`PresenceCache`].
    presence: PresenceCache,
}

type InflightMap = Arc<Mutex<HashMap<String, Inflight>>>;

/// One artifact key's single-flight state: the slot mutex, and the channel the
/// fill holding that slot publishes its progress on. A streaming tee reads the
/// channel; nothing else does, so a fill with no tee attached only ever writes
/// into a watch nobody reads.
#[derive(Clone, Default)]
struct Inflight {
    lock: Arc<tokio::sync::Mutex<()>>,
    progress: Arc<tokio::sync::watch::Sender<FillProgress>>,
}

/// Held for the whole download-verify-commit of one artifact key. Dropping it
/// removes the map entry once no other task is waiting on that key, keeping
/// [`Proxy::inflight`] bounded.
struct DownloadSlot {
    inflight: InflightMap,
    key: String,
    /// The progress channel of the entry this slot owns. Handed to the fill so
    /// it can publish, and cloned by the leader so it can subscribe a tee.
    progress: Arc<tokio::sync::watch::Sender<FillProgress>>,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for DownloadSlot {
    fn drop(&mut self) {
        let mut map = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = map.get(&self.key) {
            // We still hold `_guard` (one strong ref) and the map holds one.
            // Any count beyond that is a task already waiting on this key, so
            // only collapse the entry when we are its last user. The progress
            // channel rides on the same entry and is never counted: a tee holds
            // a `Receiver`, which is not a strong ref to anything here.
            if Arc::strong_count(&entry.lock) <= 2 {
                map.remove(&self.key);
            }
        }
    }
}

/// What a running fill publishes for whoever is teeing its bytes to a client.
///
/// One value, overwritten in place — a tee only ever needs the latest position,
/// never a history. `epoch` and `attempt` are what make that safe: a tee binds
/// to the (epoch, attempt) pair it attached on, and treats any other pair as a
/// failure rather than splicing bytes from a different download into its body.
#[derive(Clone)]
struct FillProgress {
    /// Which fill published this (see [`next_fill_epoch`]). Zero = none ever did.
    epoch: u64,
    /// Which of that fill's up-to-[`DOWNLOAD_ATTEMPTS`] attempts.
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
        // A fresh entry has no fill behind it, so it is nobody's live download.
        Self {
            epoch: 0,
            attempt: 0,
            spool: None,
            written: 0,
            state: FillState::Failed,
        }
    }
}

/// The publishing half of [`FillProgress`], owned by a fill that has a tee
/// attached. Absent (`None` at the call sites) when nothing is streaming, so the
/// buffered path pays nothing.
struct Progress {
    tx: Arc<tokio::sync::watch::Sender<FillProgress>>,
    epoch: u64,
}

impl Progress {
    /// A new attempt has an open spool: republish from byte zero under the new
    /// attempt number. A tee bound to the previous attempt sees the change and
    /// aborts rather than splicing.
    fn started(&self, attempt: u32, spool: &std::path::Path) {
        let spool = Arc::new(spool.to_path_buf());
        self.tx.send_modify(|p| {
            p.epoch = self.epoch;
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
    epoch: u64,
    attempt: u32,
    /// This tee's own handle on the spool, opened while the fill still owns the
    /// file. It stays valid across the commit's rename and the spool's unlink.
    file: tokio::fs::File,
    rx: tokio::sync::watch::Receiver<FillProgress>,
    fill: Option<tokio::task::JoinHandle<Result<bool>>>,
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
            epoch: self.epoch,
            attempt: self.attempt,
            file: self.file,
            rx: self.rx,
            sent: 0,
            available: 0,
            verified: false,
            done: false,
            on_delivered,
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
    epoch: u64,
    attempt: u32,
    file: tokio::fs::File,
    rx: tokio::sync::watch::Receiver<FillProgress>,
    sent: u64,
    available: u64,
    verified: bool,
    done: bool,
    on_delivered: Option<Box<dyn FnOnce() + Send>>,
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
                match observe(&mut self.rx, self.epoch, self.attempt) {
                    Observed::Live(written) => self.available = written,
                    Observed::Verified(written) => {
                        self.verified = true;
                        self.available = written;
                    }
                    Observed::Lost => return Some(Err(self.abort("upstream fill failed"))),
                }
            }
            // Verified against a body shorter than the listing declared: the
            // Content-Length is already promised, so abort rather than pad.
            if self.verified && self.available < self.size {
                return Some(Err(self.abort("upstream body was shorter than it declared")));
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
        match observe(&mut self.rx, self.epoch, self.attempt) {
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

fn observe(
    rx: &mut tokio::sync::watch::Receiver<FillProgress>,
    epoch: u64,
    attempt: u32,
) -> Observed {
    let progress = rx.borrow_and_update();
    if progress.epoch != epoch || progress.attempt != attempt {
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

fn peek(rx: &mut tokio::sync::watch::Receiver<FillProgress>, epoch: u64) -> Peek {
    let progress = rx.borrow_and_update();
    if progress.epoch != epoch {
        return Peek::Wait; // not ours yet: the fill hasn't published
    }
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
async fn tee_anchor(
    rx: &mut tokio::sync::watch::Receiver<FillProgress>,
    epoch: u64,
) -> Option<TeeAnchor> {
    loop {
        match peek(rx, epoch) {
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

/// May this package be served from upstream at all? Private names, the reserved
/// prefix, and (when a scope is configured) names outside the allowlist never
/// fall through — that is the entire defense.
pub async fn eligible(state: &AppState, storage: &dyn Storage, pkg: &str) -> Result<bool> {
    if let Some(prefix) = &state.private_prefix {
        if matches_prefix(pkg, prefix) {
            return Ok(false);
        }
    }
    // The package allowlist is fail-closed and pure (no I/O), so it gates before
    // the origin read — an unapproved name never even touches storage.
    if let Some(proxy) = &state.proxy {
        if proxy.name_fully_denied(pkg) {
            return Ok(false);
        }
        if !proxy.name_in_scope(pkg) {
            return Ok(false);
        }
    }
    match origin::read_origin(storage, pkg).await? {
        Some(owner) if owner == origin::PRIVATE => Ok(false),
        _ => Ok(true),
    }
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
            let mut map: HashMap<String, Vec<Option<VersionSpecifiers>>> = HashMap::new();
            for spec in &mirror.include_packages {
                map.entry(spec.name.clone())
                    .or_default()
                    .push(spec.specifiers.clone());
            }
            map
        });
        let denylist = Arc::new(crate::denylist::Denylist::from_specs(
            &mirror.exclude_packages,
        ));
        Ok(Self {
            upstream,
            mirror,
            scope,
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
        let entry = self.slot_entry(key);
        let guard = entry.lock.lock_owned().await;
        DownloadSlot {
            inflight: self.inflight.clone(),
            key: key.to_string(),
            progress: entry.progress,
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
        let entry = self.slot_entry(key);
        let guard = entry.lock.try_lock_owned().ok()?;
        Some(DownloadSlot {
            inflight: self.inflight.clone(),
            key: key.to_string(),
            progress: entry.progress,
            _guard: guard,
        })
    }

    /// The per-key mutex and progress channel, created on first use. Taken under
    /// the map lock, which [`DownloadSlot::drop`] also holds while it decides
    /// whether to prune, so a clone taken here is never an entry that is about
    /// to be dropped.
    fn slot_entry(&self, key: &str) -> Inflight {
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
        self.scope.as_ref().is_none_or(|m| m.contains_key(pkg))
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
        let allowed = match self.scope.as_ref() {
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

    /// The package page rendered from the upstream listing; `None` means
    /// "serve the local index instead" (upstream 404 or unreachable).
    pub async fn package_index(
        &self,
        state: &AppState,
        storage: &dyn Storage,
        pkg: &str,
        json: bool,
    ) -> Result<Option<RenderedIndex>> {
        let Some(found) = self.listing(state, pkg).await else {
            return Ok(None);
        };
        if !state.buckets.is_multi() {
            return Ok(Some(if json {
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
        Ok(Some(if json {
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
        let tee_size = if tee_allowed && leader {
            self.tee_size(state, pkg, filename).await
        } else {
            None
        };
        let progress = slot.progress.clone();
        let epoch = tee_size.map(|_| next_fill_epoch());
        // The fill itself runs in its own task, holding the slot for its whole
        // life. A client disconnect (pip's default timeout is ~15s; a large wheel
        // from a slow upstream is not) drops this handle, which detaches the task
        // rather than cancelling it — so the download finishes and the retry that
        // pip issues finds the artifact cached. Cancelling it was a livelock:
        // every retry restarted from byte zero and none ever completed.
        //
        // Awaiting the handle keeps a connected client's semantics identical: the
        // same `Ok(bool)`, the same errors, in the same order.
        //
        // Subscribe before spawning, so no publication can be missed. `subscribe`
        // starts at the current version, so a stale value left by an earlier fill
        // on this entry is never mistaken for a change either.
        let mut rx = progress.subscribe();
        let fill = tokio::spawn(Arc::clone(self).fill_artifact(
            Arc::clone(state),
            Arc::clone(storage),
            pkg.to_string(),
            filename.to_string(),
            slot,
            epoch,
        ));
        if let (Some(size), Some(epoch)) = (tee_size, epoch) {
            // Hold the response until the fill has an open spool — which is also
            // the moment upstream has answered 200, the origin is claimed mirror,
            // and every pre-download refusal (yank, malware, out of scope, name
            // gone private) has already had its say. Only then is a 200 with a
            // Content-Length honest. Anything else falls back to the buffered
            // path below, which is byte-for-byte today's behavior.
            if let Some(anchor) = tee_anchor(&mut rx, epoch).await {
                match tokio::fs::File::open(anchor.spool.as_path()).await {
                    Ok(file) => {
                        return Ok(FillOutcome::Streaming(Box::new(StreamingFill {
                            size,
                            epoch,
                            attempt: anchor.attempt,
                            file,
                            rx,
                            fill: Some(fill),
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
        let found = self.listing(state, pkg).await?;
        let file = found.files.iter().find(|f| f.filename == filename)?;
        // No declared size, no Content-Length, no tee: a body of unknown length
        // cannot be promised to the client up front.
        file.size.filter(|size| *size >= threshold)
    }

    /// The cold-miss fill: download → verify → commit, holding the single-flight
    /// `slot` until it returns. Spawned by [`ensure_artifact_cached`], never
    /// awaited on its own, so every early return here is one the request sees.
    async fn fill_artifact(
        self: Arc<Self>,
        state: Arc<AppState>,
        storage: Arc<dyn Storage>,
        pkg: String,
        filename: String,
        slot: DownloadSlot,
        epoch: Option<u64>,
    ) -> Result<bool> {
        let key = format!("{PACKAGES_PREFIX}{pkg}/{filename}");
        let progress = epoch.map(|epoch| Progress {
            tx: slot.progress.clone(),
            epoch,
        });
        let out = self
            .fill_and_commit(
                &state,
                storage.as_ref(),
                &pkg,
                &filename,
                &key,
                progress.as_ref(),
            )
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
        storage: &dyn Storage,
        pkg: &str,
        filename: &str,
        key: &str,
        progress: Option<&Progress>,
    ) -> Result<bool> {
        if storage.head_exists(key).await? {
            return Ok(false);
        }
        let Some(found) = self.listing(state, pkg).await else {
            return Ok(false);
        };
        let Some(file) = found.files.iter().find(|f| f.filename == filename) else {
            return Ok(false); // not upstream, or filtered out
        };

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
        let intent_nonce = if state.buckets.is_multi() {
            Some(crate::markers::mark_intent(storage, pkg).await?)
        } else {
            crate::markers::mark_intent(storage, pkg).await.ok()
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
        // package claim and launder public bytes into private truth.
        let sidecar_key = sidecar_key(key);
        let sidecar_bytes = serde_json::to_vec(&sidecar)?;
        if !storage
            .put_if_absent(
                &sidecar_key,
                sidecar_bytes.clone(),
                Some("application/json"),
            )
            .await?
        {
            let existing = storage.get_bytes(&sidecar_key).await?;
            if existing != sidecar_bytes {
                crate::markers::commit_marker(state, storage, pkg, intent_nonce).await?;
                return Ok(false);
            }
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
                crate::markers::commit_marker(state, storage, pkg, intent_nonce).await?;
                return Ok(false);
            }
        }

        // Release a teed client's withheld tail here, and only here: the bytes
        // hash to what upstream declared AND the name is still the mirror claim
        // this fill started against. Deliberately before the storage commit —
        // the last 64 KiB must not wait on an S3 upload — so a commit that later
        // loses a race leaves the cache cold while the client keeps a body that
        // was verified before its first withheld byte moved.
        if let Some(progress) = progress {
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
            crate::markers::commit_marker(state, storage, pkg, intent_nonce).await?;
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
            crate::markers::commit_marker(state, storage, pkg, intent_nonce).await?;
            info!(%pkg, %filename, "proxy: origin changed after artifact publish; leaving typed mirror loser");
            return Ok(false);
        }
        if state.buckets.is_multi() && storage.head_exists(&frozen_key(key)).await? {
            // The marker suppresses the filename immediately. Leave the typed
            // body for freeze recovery; deleting here would create another
            // cross-object race with staged private promotion.
            crate::markers::commit_marker(state, storage, pkg, intent_nonce).await?;
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
        crate::markers::commit_marker(state, storage, pkg, intent_nonce).await?;
        state
            .metrics
            .proxy_artifacts_cached
            .fetch_add(1, Ordering::Relaxed);
        // A fresh fill committed: signal the caller to schedule the peer
        // replication notes off the request's critical path.
        Ok(true)
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
        let found = self.listing(state, pkg).await?;
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
        let found = self.listing(state, pkg).await?;
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
    /// [`LISTING_TTL`]. On upstream errors a stale entry is reused for one
    /// more TTL (already-resolved installs keep working through blips);
    /// with nothing to reuse the package is treated as missing for one TTL,
    /// so a dead upstream degrades to local-only instead of a per-request
    /// timeout.
    async fn listing(&self, state: &AppState, pkg: &str) -> Option<Arc<Found>> {
        if let Some(cached) = self.cached_listing(pkg, false) {
            return cached;
        }
        state
            .metrics
            .proxy_listing_fetches
            .fetch_add(1, Ordering::Relaxed);
        let listing = match self.fetch_listing(state, pkg).await {
            Ok(listing) => listing,
            Err(e) => {
                state
                    .metrics
                    .proxy_listing_errors
                    .fetch_add(1, Ordering::Relaxed);
                warn!(%pkg, upstream = %self.upstream, error=?e, "proxy: upstream listing fetch failed");
                if let Some(stale) = self.cached_listing(pkg, true) {
                    return stale;
                }
                Listing::Missing
            }
        };
        let result = match &listing {
            Listing::Found(found) => Some(found.clone()),
            Listing::Missing => None,
        };
        // Persist the observed upstream status durably. The in-memory listing
        // renders the freeze, but a lockfile-pinned `/files/...` download of an
        // already-cached artifact never re-fetches the listing — only the
        // durable `.project-status.json`, folded into the byte gate's
        // quarantined set by the audit sweep, blocks those bytes.
        if let Listing::Found(found) = &listing {
            self.persist_observed_status(state, pkg, &found.status)
                .await;
        }
        let mut map = self.listings.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= MAX_LISTINGS && !map.contains_key(pkg) {
            evict_listings(&mut map);
        }
        map.insert(
            pkg.to_string(),
            CacheEntry {
                listing,
                fetched: Instant::now(),
            },
        );
        result
    }

    /// Cached listing lookup. `revive` refreshes the entry's timestamp and
    /// ignores expiry — the stale-on-upstream-error path.
    fn cached_listing(&self, pkg: &str, revive: bool) -> Option<Option<Arc<Found>>> {
        let mut map = self.listings.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map.get_mut(pkg)?;
        if revive {
            entry.fetched = Instant::now();
        } else if entry.fetched.elapsed() >= LISTING_TTL {
            return None;
        }
        Some(match &entry.listing {
            Listing::Found(found) => Some(found.clone()),
            Listing::Missing => None,
        })
    }

    /// Persist the upstream PEP 792 status so the byte gate blocks cached bytes
    /// of an upstream-quarantined project (see the call site). Mirror-origin
    /// only; an operator-authored (private-origin) status is never clobbered.
    /// Best-effort: a write failure only leaves the durable status one poll
    /// stale — the in-memory listing still renders the freeze.
    async fn persist_observed_status(
        &self,
        state: &AppState,
        pkg: &str,
        observed: &crate::status::ProjectStatusDoc,
    ) {
        let pinned = state.pin();
        if let Err(e) = reconcile_observed_status(pinned.storage.as_ref(), pkg, observed).await {
            warn!(%pkg, error=?e, "proxy: failed to persist upstream project status");
        }
    }

    async fn fetch_listing(&self, state: &AppState, pkg: &str) -> Result<Listing> {
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
        let files: Vec<SimpleFile> = index
            .files
            .into_iter()
            // No digest, no service: every artifact we hand out is verifiable.
            .filter(|f| f.sha256().is_some())
            // Every mirror gate except the yank one: a file yanked upstream stays
            // listed (with `data-yanked`, PEP 592) so an exact pin against bytes
            // we already cached still resolves. `exclude-yanked` gates the fill
            // instead — see `fill_refused_by_yank`.
            .filter(|f| matches_mirror_listing(f, &self.mirror))
            // The scope's version axis: a pinned/ranged allowlist entry serves
            // only matching versions, exactly as `sync` mirrors only matching
            // versions. No scope → kept.
            .filter(|f| self.version_in_scope(pkg, &f.filename))
            // Malware scrub: a proxy listing is mirror-origin by definition, so a
            // MAL-blocked version is dropped unconditionally (no origin read).
            // Best-effort — the byte gate and fill refusal are the guarantees.
            .filter(|f| !advisory_blocks(state, pkg, &f.filename))
            .collect();
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
        let resp = ssrf::guarded_get(&self.client, &self.guard, url.clone(), Some(deadline))
            .await?
            .error_for_status()?;
        let mut spool = UploadSpool::new(&state.spool_dir).await?;
        // Upstream answered 200 and there is a file to follow: this is where a
        // waiting tee attaches, which is why nothing before it can 200 a client.
        if let Some(progress) = progress {
            progress.started(attempt, spool.path());
        }
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            spool.write_chunk(&chunk?).await?;
            // Mirror sync::download_once: abort a body that overruns its
            // upstream-declared size before it can fill the disk (the read
            // timeout bounds time, not size, and an overrun fails the sha
            // check anyway). No declared size → no cap, same as sync.
            if let Some(max) = file.size {
                if spool.size() > max {
                    bail!(
                        "{} overran its declared size ({} > {max} bytes)",
                        file.filename,
                        spool.size()
                    );
                }
            }
            if let Some(progress) = progress {
                // Flush first: `write_chunk` returns once tokio has *queued* the
                // write, and a tee reading a second handle must never be told
                // about bytes the OS has not seen yet.
                spool.flush().await?;
                progress.wrote(spool.size());
            }
        }
        spool.finish().await
    }

    /// PEP 691 file URLs may be absolute or relative; relative ones resolve
    /// against the index page URL (RFC 3986), which `Url::join` implements.
    fn resolve_url(&self, pkg: &str, raw: &str) -> Result<reqwest::Url> {
        let base = reqwest::Url::parse(&format!("{}/simple/{pkg}/", self.upstream))?;
        Ok(base.join(raw)?)
    }
}

/// Bring the durable `.project-status.json` into line with an observed upstream
/// status. Writes only on a real change (idempotent under repeated polls), never
/// clobbers an operator-authored private-origin status, and skips the write when
/// upstream is the active default and no marker exists. A change marks the
/// package dirty so its own leader rebuilds the (now file-less, if quarantined)
/// index; the audit sweep separately folds a quarantine into the byte gate's set.
async fn reconcile_observed_status(
    storage: &dyn Storage,
    pkg: &str,
    observed: &crate::status::ProjectStatusDoc,
) -> Result<()> {
    match crate::status::read_status_versioned(storage, pkg).await? {
        // An operator-authored (private-origin) status is authoritative; the
        // proxy never overwrites it with a relayed upstream observation.
        Some(cur) if cur.origin == Some(crate::replicate::Origin::Private) => return Ok(()),
        // Durable status already reflects upstream: nothing to write.
        Some(cur) if &cur.doc == observed => return Ok(()),
        Some(_) => {}
        // No marker yet and upstream is the active default: absence already
        // means active, so a write would only add noise.
        None if observed == &crate::status::ProjectStatusDoc::default() => return Ok(()),
        None => {}
    }
    crate::status::advance_status(
        storage,
        pkg,
        observed,
        Some(crate::replicate::Origin::Mirror),
    )
    .await?;
    crate::markers::mark_dirty(storage, pkg).await?;
    Ok(())
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

/// Keep the listings cache bounded: first drop everything past its TTL (those
/// would be re-fetched anyway), and if that didn't free a slot, evict the
/// oldest entries down to half the cap so this stays amortized O(1) per insert.
fn evict_listings(map: &mut HashMap<String, CacheEntry>) {
    map.retain(|_, e| e.fetched.elapsed() < LISTING_TTL);
    if map.len() < MAX_LISTINGS {
        return;
    }
    let mut by_age: Vec<(String, Instant)> =
        map.iter().map(|(k, e)| (k.clone(), e.fetched)).collect();
    by_age.sort_by_key(|(_, fetched)| *fetched);
    for (k, _) in by_age.into_iter().take(MAX_LISTINGS / 2) {
        map.remove(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

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
}
