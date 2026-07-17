//! Advisory snapshot: the OSV PyPI export parsed into two in-memory views —
//! a malware **block set** (`MAL-*` ids only) and an **audit index**
//! (everything). One feed, two features: enforcement where bytes are served
//! and an org-level audit. See [dev/ADVISORIES.md](../dev/ADVISORIES.md).
//!
//! Parsing and matching are pure and unit-tested here; the fetch/persist/reload
//! plumbing (the rest of this file) is exercised blackbox. The snapshot is a
//! global truth-cache: it is carried verbatim to `_advisories/osv-pypi.zip`,
//! never authored, and a bucket failover must never disarm it.

use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use pep440_rs::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use zip::ZipArchive;

use crate::metrics::Metrics;
use crate::names::normalize_pkg_name;
use crate::ssrf::{Guard, SsrfGuardResolver};
use crate::storage::{self, Storage};

/// The verbatim snapshot, written by whichever source delivered it (leader OSV
/// pull, local-path ferry, or an admin `PUT`). A derived/carried view under the
/// reserved `_advisories/` prefix — deletable and regenerable, never truth.
pub const FEED_KEY: &str = "_advisories/osv-pypi.zip";

/// Worker-derived set of PEP 792 `quarantined` projects, persisted for the
/// byte-gate probe (rung 5). Defined now; unused until then.
pub const QUARANTINED_KEY: &str = "_advisories/quarantined.json";

/// Materialized org audit report (rung 7). Defined now; unused until then.
pub const REPORT_KEY: &str = "_advisories/report.json";

/// The OSV bulk export for the PyPI ecosystem — the same database `uv audit`
/// queries live, so advisory ids shown by pypiron and by a laptop always agree.
/// No auth, ETag'd, regenerated continuously.
pub const DEFAULT_FEED_URL: &str =
    "https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip";

/// Hard ceiling on a fetched/read feed. The real export is ~32 MB; 256 MB is
/// generous headroom that still refuses a hostile or runaway body.
const MAX_FEED_BYTES: u64 = 256 * 1024 * 1024;

/// Per-entry ceiling inside the zip. OSV records are KBs; this only refuses a
/// decompression bomb hiding in one member.
const MAX_ENTRY_BYTES: u64 = 8 * 1024 * 1024;

/// OSV's ecosystem string for PyPI (matched case-insensitively for tolerance).
const PYPI_ECOSYSTEM: &str = "PyPI";

// ---------------------------------------------------------------------------
// Parsed views (pure)
// ---------------------------------------------------------------------------

/// How an advisory's affected clause selects versions of one package.
#[derive(Debug, Clone)]
pub enum VersionScope {
    /// Every version (an `introduced: "0"` range with no fix, or a bare package
    /// clause). Matches even an unparseable requested version — fail-closed, so
    /// malware can't slip through on a version string pep440 can't read.
    AllVersions,
    /// An explicit affected-version list. Compared as parsed PEP 440 versions,
    /// falling back to raw string equality when a side won't parse.
    Exact(Vec<String>),
    /// Half-open `[introduced, fixed)` ranges; `fixed = None` is open-ended.
    Ranges(Vec<(Version, Option<Version>)>),
}

impl VersionScope {
    /// Does `version` (a raw requested version string) fall in this scope?
    pub fn matches(&self, version: &str) -> bool {
        match self {
            VersionScope::AllVersions => true,
            VersionScope::Exact(versions) => versions.iter().any(|v| version_eq(v, version)),
            VersionScope::Ranges(ranges) => match version.parse::<Version>() {
                Ok(v) => ranges
                    .iter()
                    .any(|(intro, fixed)| v >= *intro && fixed.as_ref().is_none_or(|f| v < *f)),
                // A requested version pep440 can't read never matches a numeric
                // range — a range is a claim about ordering it can't satisfy.
                Err(_) => false,
            },
        }
    }
}

/// One malware-block rule: a `MAL-*` id and the versions it condemns.
#[derive(Debug, Clone)]
pub struct MalRule {
    pub id: String,
    pub scope: VersionScope,
}

/// One advisory record for the audit index — informational, never blocking
/// (unless its id is also `MAL-*`, which additionally lands in the block set).
#[derive(Debug, Clone)]
pub struct AdvisoryRecord {
    pub id: String,
    pub summary: String,
    /// Severity verbatim from the feed (`database_specific.severity`, else the
    /// first `severity[].score`, else empty). Never rewritten.
    pub severity: String,
    pub fixed_in: Vec<String>,
    pub matcher: VersionScope,
}

/// The parsed snapshot: normalized name → rules/records.
#[derive(Debug, Default)]
pub struct AdvisoryDb {
    block: HashMap<String, Vec<MalRule>>,
    audit: HashMap<String, Vec<AdvisoryRecord>>,
}

impl AdvisoryDb {
    /// Distinct package names carrying at least one malware-block rule.
    pub fn block_names(&self) -> usize {
        self.block.len()
    }

    /// Total advisory records in the audit index.
    pub fn audit_records(&self) -> usize {
        self.audit.values().map(Vec::len).sum()
    }
}

/// The `MAL-*` ids that condemn `name_norm` at `version` (empty = not blocked).
/// `name_norm` must already be [`normalize_pkg_name`]-normalized.
pub fn blocking_advisories<'a>(db: &'a AdvisoryDb, name_norm: &str, version: &str) -> Vec<&'a str> {
    db.block
        .get(name_norm)
        .map(|rules| {
            rules
                .iter()
                .filter(|rule| rule.scope.matches(version))
                .map(|rule| rule.id.as_str())
                .collect()
        })
        .unwrap_or_default()
}

/// The advisory records affecting `name_norm` at `version` (for the audit).
pub fn advisories_for<'a>(
    db: &'a AdvisoryDb,
    name_norm: &str,
    version: &str,
) -> Vec<&'a AdvisoryRecord> {
    db.audit
        .get(name_norm)
        .map(|records| {
            records
                .iter()
                .filter(|record| record.matcher.matches(version))
                .collect()
        })
        .unwrap_or_default()
}

/// Compare two raw version strings for equality: parsed PEP 440 when both read,
/// else raw string equality (legacy/malformed versions still compare exactly).
fn version_eq(a: &str, b: &str) -> bool {
    match (a.parse::<Version>(), b.parse::<Version>()) {
        (Ok(va), Ok(vb)) => va == vb,
        _ => a == b,
    }
}

// ---- OSV wire structs (tolerant: every field defaulted, unknowns ignored) ---

#[derive(Deserialize)]
struct OsvAdvisory {
    #[serde(default)]
    id: String,
    #[serde(default)]
    summary: String,
    /// Present (non-null) iff the advisory was withdrawn — skip those.
    #[serde(default)]
    withdrawn: Option<String>,
    #[serde(default)]
    affected: Vec<OsvAffected>,
    #[serde(default)]
    severity: Vec<OsvSeverity>,
    #[serde(default)]
    database_specific: OsvDatabaseSpecific,
}

#[derive(Deserialize, Default)]
struct OsvDatabaseSpecific {
    #[serde(default)]
    severity: Option<String>,
}

#[derive(Deserialize, Default)]
struct OsvAffected {
    #[serde(default)]
    package: OsvPackage,
    #[serde(default)]
    ranges: Vec<OsvRange>,
    #[serde(default)]
    versions: Vec<String>,
}

#[derive(Deserialize, Default)]
struct OsvPackage {
    #[serde(default)]
    ecosystem: String,
    #[serde(default)]
    name: String,
}

#[derive(Deserialize, Default)]
struct OsvRange {
    #[serde(default)]
    events: Vec<OsvEvent>,
}

#[derive(Deserialize, Default)]
struct OsvEvent {
    #[serde(default)]
    introduced: Option<String>,
    #[serde(default)]
    fixed: Option<String>,
}

#[derive(Deserialize, Default)]
struct OsvSeverity {
    #[serde(default)]
    score: String,
}

/// Parse the OSV PyPI export (a zip of flat `<id>.json` members) into the two
/// in-memory views. Tolerant by design: a withdrawn, malformed, or
/// non-PyPI-ecosystem entry is skipped and counted, never fatal — one truncated
/// record must not blind the whole fleet.
pub fn parse_feed(zip_bytes: &[u8]) -> Result<AdvisoryDb> {
    let mut zip = ZipArchive::new(Cursor::new(zip_bytes)).context("advisory feed is not a zip")?;
    let names: Vec<String> = zip
        .file_names()
        .filter(|n| n.ends_with(".json"))
        .map(str::to_string)
        .collect();

    let mut db = AdvisoryDb::default();
    let mut skipped = 0usize;
    for name in names {
        let Ok(entry) = zip.by_name(&name) else {
            skipped += 1;
            continue;
        };
        let mut buf = Vec::new();
        if entry
            .take(MAX_ENTRY_BYTES + 1)
            .read_to_end(&mut buf)
            .is_err()
            || buf.len() as u64 > MAX_ENTRY_BYTES
        {
            skipped += 1;
            continue;
        }
        match serde_json::from_slice::<OsvAdvisory>(&buf) {
            Ok(adv) if ingest_advisory(&mut db, &adv) => {}
            _ => skipped += 1,
        }
    }
    if skipped > 0 {
        debug!(
            skipped,
            "advisory feed: skipped withdrawn/malformed/non-PyPI entries"
        );
    }
    Ok(db)
}

/// Fold one advisory into the db. Returns false (→ skip count) when the entry is
/// withdrawn or names no PyPI package. Never rewrites the id (AC9: byte-equal).
fn ingest_advisory(db: &mut AdvisoryDb, adv: &OsvAdvisory) -> bool {
    if adv.withdrawn.is_some() || adv.id.is_empty() {
        return false;
    }
    let is_malware = adv.id.starts_with("MAL-");
    let severity = adv
        .database_specific
        .severity
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            adv.severity
                .iter()
                .map(|s| s.score.clone())
                .find(|s| !s.is_empty())
        })
        .unwrap_or_default();

    let mut matched_pypi = false;
    for affected in &adv.affected {
        if !affected
            .package
            .ecosystem
            .eq_ignore_ascii_case(PYPI_ECOSYSTEM)
        {
            continue;
        }
        let Some(name) = checked_osv_name(&affected.package.name) else {
            continue;
        };
        matched_pypi = true;
        let scope = scope_for(affected);
        if is_malware {
            db.block.entry(name.clone()).or_default().push(MalRule {
                id: adv.id.clone(),
                scope: scope.clone(),
            });
        }
        db.audit.entry(name).or_default().push(AdvisoryRecord {
            id: adv.id.clone(),
            summary: adv.summary.clone(),
            severity: severity.clone(),
            fixed_in: fixed_versions(affected),
            matcher: scope,
        });
    }
    matched_pypi
}

/// Normalize an OSV package name to a servable PEP 503 name, or `None` if it
/// isn't one (hostile/garbage names never become map keys).
fn checked_osv_name(raw: &str) -> Option<String> {
    let name = normalize_pkg_name(raw);
    (!name.is_empty()).then_some(name)
}

/// Determine the version scope of one PyPI affected clause. Always yields a
/// scope (never `None`) — a clause we can't pin down blocks all versions
/// (fail-closed: malware is blocked, never negotiated).
fn scope_for(affected: &OsvAffected) -> VersionScope {
    if is_all_versions(affected) {
        return VersionScope::AllVersions;
    }
    let ranges = parse_ranges(&affected.ranges);
    if !ranges.is_empty() {
        return VersionScope::Ranges(ranges);
    }
    if !affected.versions.is_empty() {
        return VersionScope::Exact(affected.versions.clone());
    }
    VersionScope::AllVersions
}

/// True when the clause condemns every version: a lone `introduced: "0"` range
/// with no fix, or a bare package clause with neither ranges nor versions.
fn is_all_versions(affected: &OsvAffected) -> bool {
    if affected.ranges.is_empty() && affected.versions.is_empty() {
        return true;
    }
    affected.ranges.iter().any(|range| {
        range.events.len() == 1
            && range.events[0].introduced.as_deref() == Some("0")
            && range.events[0].fixed.is_none()
    })
}

/// Fold OSV range events into half-open `[introduced, fixed)` pairs. Events are
/// a flat sequence: each `introduced` opens a range that the next `fixed`
/// closes. Unparseable `introduced` events drop that range; an unparseable
/// `fixed` leaves the range open-ended rather than dropping it.
fn parse_ranges(ranges: &[OsvRange]) -> Vec<(Version, Option<Version>)> {
    let mut pairs = Vec::new();
    for range in ranges {
        let mut open: Option<Version> = None;
        for event in &range.events {
            if let Some(intro) = &event.introduced {
                if let Some(prev) = open.take() {
                    pairs.push((prev, None));
                }
                open = intro.parse::<Version>().ok();
            } else if let Some(fixed) = &event.fixed {
                if let Some(intro) = open.take() {
                    pairs.push((intro, fixed.parse::<Version>().ok()));
                }
            }
        }
        if let Some(intro) = open.take() {
            pairs.push((intro, None));
        }
    }
    pairs
}

/// The fixed-in versions named across a clause's ranges (raw, for display).
fn fixed_versions(affected: &OsvAffected) -> Vec<String> {
    affected
        .ranges
        .iter()
        .flat_map(|range| range.events.iter().filter_map(|e| e.fixed.clone()))
        .collect()
}

/// Hex sha256 of the verbatim feed bytes — the snapshot's content identity and
/// the `GET /advisories/feed` ETag.
pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

// ---------------------------------------------------------------------------
// Live snapshot state + refresh plumbing (blackbox-tested)
// ---------------------------------------------------------------------------

/// The live snapshot every request-path probe reads. Held behind
/// `Arc<RwLock<Arc<AdvisoryState>>>`: a reader takes the lock, clones the inner
/// `Arc`, and drops the guard immediately, so probes never contend with a
/// refresh swap. A poisoned lock is recovered, never a panic path.
#[derive(Default)]
pub struct AdvisoryState {
    /// The parsed views, or `None` until the first snapshot loads (armed but
    /// unfed — nothing to block yet, degraded not fatal).
    pub db: Option<AdvisoryDb>,
    /// Content identity of the loaded snapshot (hex sha256 of the zip).
    pub zip_sha256: Option<String>,
    /// The storage `ObjectMeta.etag` the loaded snapshot was read at, so the
    /// follower reload skips the 32 MB GET when the key hasn't moved.
    pub storage_etag: Option<String>,
    /// Worker-derived `quarantined` projects (rung 5); carried across reloads.
    pub quarantined: HashSet<String>,
    /// Unix seconds of the load (0 = never loaded).
    pub loaded_unix: u64,
}

impl AdvisoryState {
    /// Read the current snapshot: lock, clone the inner `Arc`, drop the guard.
    /// Recovers a poisoned lock instead of panicking (never a request-path panic).
    pub fn read(slot: &RwLock<Arc<AdvisoryState>>) -> Arc<AdvisoryState> {
        slot.read().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn is_url(feed: &str) -> bool {
    feed.starts_with("http://") || feed.starts_with("https://")
}

/// One poll of a configured feed source (a URL or a local path).
enum RawFetch {
    /// URL conditional GET returned 304 — the source is unchanged and free.
    NotModified,
    /// Fresh bytes, plus the URL's ETag to remember for the next conditional GET.
    Bytes {
        bytes: Vec<u8>,
        http_etag: Option<String>,
    },
}

/// A resolved feed source. Built once per process (the URL client and its SSRF
/// guard are reused across polls); a local path holds no client.
pub struct FeedSource {
    feed: String,
    url_client: Option<(reqwest::Client, Arc<Guard>)>,
}

impl FeedSource {
    /// Build the source for `feed`. A URL gets an SSRF-guarded client that —
    /// unlike sync/proxy — honors `HTTP(S)_PROXY`, because corporate egress and
    /// the hermetic tests both reach OSV through a forward proxy.
    pub fn new(feed: &str) -> Result<Self> {
        let url_client = if is_url(feed) {
            let guard = Arc::new(Guard::new(feed, &[], &[])?);
            let client = reqwest::Client::builder()
                .user_agent(
                    "pypiron-advisory/0.1 (+https://github.com/blackthorn-interstellar/pypiron)",
                )
                .dns_resolver(Arc::new(SsrfGuardResolver::new(guard.clone())))
                .connect_timeout(std::time::Duration::from_secs(10))
                .read_timeout(std::time::Duration::from_secs(300))
                .build()
                .context("building advisory feed HTTP client")?;
            Some((client, guard))
        } else {
            None
        };
        Ok(Self {
            feed: feed.to_string(),
            url_client,
        })
    }

    async fn poll(&self, http_etag: Option<&str>) -> Result<RawFetch> {
        match &self.url_client {
            Some((client, _guard)) => fetch_url(client, &self.feed, http_etag).await,
            None => {
                let bytes = read_path_capped(&self.feed).await?;
                Ok(RawFetch::Bytes {
                    bytes,
                    http_etag: None,
                })
            }
        }
    }
}

/// Conditional GET of a URL feed, size-capped. A `304` short-circuits with no
/// body; otherwise the ETag rides back for the next poll.
async fn fetch_url(
    client: &reqwest::Client,
    url: &str,
    if_none_match: Option<&str>,
) -> Result<RawFetch> {
    use futures::StreamExt;

    let mut req = client.get(url);
    if let Some(tag) = if_none_match {
        req = req.header(reqwest::header::IF_NONE_MATCH, tag);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?;
    if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(RawFetch::NotModified);
    }
    let resp = resp
        .error_for_status()
        .with_context(|| format!("fetching {url}"))?;
    if resp
        .content_length()
        .is_some_and(|len| len > MAX_FEED_BYTES)
    {
        bail!("advisory feed exceeds {MAX_FEED_BYTES} bytes (Content-Length)");
    }
    let http_etag = resp
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let mut bytes = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("reading {url}"))?;
        if bytes.len() as u64 + chunk.len() as u64 > MAX_FEED_BYTES {
            bail!("advisory feed exceeds {MAX_FEED_BYTES} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(RawFetch::Bytes { bytes, http_etag })
}

/// Read a local-path feed with the same ceiling as the URL path.
async fn read_path_capped(path: &str) -> Result<Vec<u8>> {
    let meta = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("stat advisory feed {path}"))?;
    if meta.len() > MAX_FEED_BYTES {
        bail!("advisory feed {path} exceeds {MAX_FEED_BYTES} bytes");
    }
    tokio::fs::read(path)
        .await
        .with_context(|| format!("reading advisory feed {path}"))
}

/// Parse feed bytes off the async runtime (a full export is CPU-bound).
async fn parse_off_thread(bytes: Vec<u8>) -> Result<(AdvisoryDb, String)> {
    tokio::task::spawn_blocking(move || {
        let db = parse_feed(&bytes)?;
        Ok((db, sha256_hex(&bytes)))
    })
    .await
    .context("advisory parse task panicked")?
}

/// Read `FEED_KEY`'s current storage etag (via a 1-key LIST, no body), or `None`
/// when no snapshot exists yet.
async fn feed_storage_etag(storage: &dyn Storage) -> Result<Option<String>> {
    let page = storage.list_page(FEED_KEY, None, 1).await?;
    Ok(page.into_iter().find(|m| m.key == FEED_KEY).map(|m| m.etag))
}

/// Load the persisted snapshot from storage, or `None` when the key is absent.
async fn load_from_storage(storage: &dyn Storage) -> Result<Option<AdvisoryState>> {
    let Some(etag) = feed_storage_etag(storage).await? else {
        return Ok(None);
    };
    let bytes = storage.get_bytes(FEED_KEY).await?;
    let (db, sha) = parse_off_thread(bytes).await?;
    Ok(Some(AdvisoryState {
        db: Some(db),
        zip_sha256: Some(sha),
        storage_etag: Some(etag),
        quarantined: HashSet::new(),
        loaded_unix: unix_now(),
    }))
}

/// Obtain the startup snapshot: try the source (URL/path), and on any failure
/// fall back to a previously delivered `_advisories/` copy in storage. `None`
/// means neither yielded a snapshot — the caller decides bail (explicit) vs
/// warn-and-continue (implicit). A source feed that parses is persisted verbatim
/// so followers and later restarts load the same bytes.
pub async fn obtain_at_startup(storage: &dyn Storage, feed: Option<&str>) -> Option<AdvisoryState> {
    if let Some(feed) = feed {
        if let Some(state) = obtain_from_source(storage, feed).await {
            return Some(state);
        }
    }
    match load_from_storage(storage).await {
        Ok(state) => state,
        Err(e) => {
            warn!(error = %e, "advisory feed: reading stored snapshot failed");
            None
        }
    }
}

/// Fetch/read the source, validate, persist verbatim, and load. `None` on any
/// failure so the caller falls back to a stored `_advisories/` copy.
async fn obtain_from_source(storage: &dyn Storage, feed: &str) -> Option<AdvisoryState> {
    let source = match FeedSource::new(feed) {
        Ok(source) => source,
        Err(e) => {
            warn!(error = %e, "advisory feed: could not build source; trying stored snapshot");
            return None;
        }
    };
    let bytes = match source.poll(None).await {
        Ok(RawFetch::Bytes { bytes, .. }) => bytes,
        // No If-None-Match was sent, so a 304 is the source misbehaving.
        Ok(RawFetch::NotModified) => return None,
        Err(e) => {
            warn!(error = %e, "advisory feed: source unreachable at startup; trying stored snapshot");
            return None;
        }
    };
    // Parse once — this both validates the bytes (never seed a bad snapshot with
    // an error page or truncated body) and yields the db to load.
    let (db, sha) = match parse_off_thread(bytes.clone()).await {
        Ok(pair) => pair,
        Err(e) => {
            warn!(error = %e, "advisory feed: source did not parse; trying stored snapshot");
            return None;
        }
    };
    if let Err(e) = storage
        .put_bytes(FEED_KEY, bytes, Some("application/zip"))
        .await
    {
        warn!(error = %e, "advisory feed: persisting snapshot failed; loading in-memory");
    }
    Some(AdvisoryState {
        db: Some(db),
        zip_sha256: Some(sha),
        storage_etag: feed_storage_etag(storage).await.unwrap_or(None),
        quarantined: HashSet::new(),
        loaded_unix: unix_now(),
    })
}

/// Per-node handle the worker's advisory tick refreshes.
pub struct RefreshCtx<'a> {
    pub storage: &'a dyn Storage,
    pub slot: &'a RwLock<Arc<AdvisoryState>>,
    pub metrics: &'a Metrics,
}

/// Leader-only in-memory refresh memo, owned by the worker loop (never in shared
/// state, so a failover starts a fresh conditional cycle harmlessly).
#[derive(Default)]
pub struct RefreshMemo {
    /// The URL feed's last-seen ETag, for the next `If-None-Match`.
    http_etag: Option<String>,
    /// Whether the source was failing on the previous tick (warn on transition).
    source_failing: bool,
}

/// One advisory refresh cycle. The leader half (leader with a source feed)
/// refetches the source and persists changed bytes verbatim; the every-node half
/// reloads from storage when `FEED_KEY`'s etag moves. Failures keep serving the
/// last snapshot; the staleness gauge resets only on this node's own successful
/// refresh (source poll for a leader-with-source, storage check otherwise), so a
/// failing source is visible as rising staleness.
pub async fn refresh(
    ctx: RefreshCtx<'_>,
    feed: Option<&str>,
    is_leader: bool,
    memo: &mut RefreshMemo,
) {
    let has_source = feed.is_some();
    let leads_source = is_leader && has_source;
    let mut refreshed_ok = false;

    if leads_source {
        if let Some(feed) = feed {
            match poll_and_persist(&ctx, feed, memo).await {
                Ok(()) => {
                    if memo.source_failing {
                        info!("advisory feed: source reachable again");
                        memo.source_failing = false;
                    }
                    refreshed_ok = true;
                }
                Err(e) => {
                    if !memo.source_failing {
                        warn!(error = %e, "advisory feed: source unreachable; serving last snapshot");
                        memo.source_failing = true;
                    }
                }
            }
        }
    }

    match reload(&ctx).await {
        Ok(loaded) => {
            if loaded {
                let snap = AdvisoryState::read(ctx.slot);
                if let Some(db) = &snap.db {
                    info!(
                        block_names = db.block_names(),
                        audit_records = db.audit_records(),
                        "advisory snapshot loaded"
                    );
                }
                // A freshly loaded snapshot is always a successful refresh — this
                // is how a sync/ferry-delivered snapshot self-arms even on a
                // leader whose live source is unreachable (AC7 implicit).
                refreshed_ok = true;
            } else if !leads_source {
                // Follower (or a leader whose only source is the stored snapshot):
                // a clean storage check confirms the loaded snapshot is current.
                // A leader with a live source resets its gauge on the source poll
                // above, so a failing source shows as rising staleness.
                refreshed_ok = true;
            }
        }
        Err(e) => debug!(error = %e, "advisory feed: storage reload failed; serving last snapshot"),
    }

    // Arm the gauge only once a snapshot is actually loaded — an armed-but-unfed
    // node has nothing to age.
    if refreshed_ok && AdvisoryState::read(ctx.slot).db.is_some() {
        ctx.metrics.advisory_refresh_ok();
    }
}

/// Leader half: poll the source and persist verbatim only when the content
/// actually changed (sha differs from the loaded snapshot), so an unchanged feed
/// never bumps the storage etag and stampedes every follower into a reload.
async fn poll_and_persist(ctx: &RefreshCtx<'_>, feed: &str, memo: &mut RefreshMemo) -> Result<()> {
    // FeedSource is cheap for a path; for a URL it rebuilds the client each tick,
    // which is negligible at the reconcile cadence.
    let source = FeedSource::new(feed)?;
    let fetched = source.poll(memo.http_etag.as_deref()).await?;
    let (bytes, http_etag) = match fetched {
        RawFetch::NotModified => return Ok(()),
        RawFetch::Bytes { bytes, http_etag } => (bytes, http_etag),
    };
    memo.http_etag = http_etag;
    let loaded_sha = AdvisoryState::read(ctx.slot).zip_sha256.clone();
    let sha = sha256_hex(&bytes);
    if Some(&sha) == loaded_sha.as_ref() {
        return Ok(());
    }
    // Validate before persisting — the storage copy is what every node loads.
    parse_off_thread(bytes.clone()).await?;
    ctx.storage
        .put_bytes(FEED_KEY, bytes, Some("application/zip"))
        .await
        .context("persisting advisory snapshot")?;
    Ok(())
}

/// Every-node half: reload the in-memory snapshot when `FEED_KEY`'s storage etag
/// has moved. Returns whether a new snapshot was swapped in.
async fn reload(ctx: &RefreshCtx<'_>) -> Result<bool> {
    let Some(etag) = feed_storage_etag(ctx.storage).await? else {
        return Ok(false);
    };
    if Some(&etag) == AdvisoryState::read(ctx.slot).storage_etag.as_ref() {
        return Ok(false);
    }
    let bytes = ctx.storage.get_bytes(FEED_KEY).await?;
    let (db, sha) = parse_off_thread(bytes).await?;
    let quarantined = AdvisoryState::read(ctx.slot).quarantined.clone();
    let next = Arc::new(AdvisoryState {
        db: Some(db),
        zip_sha256: Some(sha),
        storage_etag: Some(etag),
        quarantined,
        loaded_unix: unix_now(),
    });
    *ctx.slot.write().unwrap_or_else(|p| p.into_inner()) = next;
    Ok(true)
}

/// Read the persisted snapshot bytes for `GET /advisories/feed`, or `None` when
/// no snapshot exists.
pub async fn stored_feed_bytes(storage: &dyn Storage) -> Result<Option<Vec<u8>>> {
    match storage.get_bytes(FEED_KEY).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if storage::is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::write::SimpleFileOptions;

    /// Build an OSV-shaped zip from `(filename, json)` members.
    fn osv_zip(entries: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        for (name, json) in entries {
            zip.start_file(*name, opts).unwrap();
            zip.write_all(json.to_string().as_bytes()).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    fn mal_exact(id: &str, name: &str, version: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "summary": "malicious code",
            "affected": [{"package": {"ecosystem": "PyPI", "name": name}, "versions": [version]}],
        })
    }

    fn mal_all(id: &str, name: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "affected": [{
                "package": {"ecosystem": "PyPI", "name": name},
                "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}]}],
            }],
        })
    }

    fn pysec_fixed(id: &str, name: &str, fixed: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "summary": "a vulnerability",
            "severity": [{"type": "CVSS_V3", "score": "7.5"}],
            "affected": [{
                "package": {"ecosystem": "PyPI", "name": name},
                "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}, {"fixed": fixed}]}],
            }],
        })
    }

    #[test]
    fn exact_version_blocks_only_that_version() {
        let zip = osv_zip(&[(
            "MAL-1.json",
            mal_exact("MAL-2024-0001", "Evil.Pkg", "1.0.0"),
        )]);
        let db = parse_feed(&zip).unwrap();
        assert_eq!(
            blocking_advisories(&db, "evil-pkg", "1.0.0"),
            ["MAL-2024-0001"]
        );
        assert!(blocking_advisories(&db, "evil-pkg", "1.0.1").is_empty());
        // Name normalization applies on the query side too.
        assert!(blocking_advisories(&db, "notevil", "1.0.0").is_empty());
    }

    #[test]
    fn all_versions_blocks_everything_including_unparseable() {
        let zip = osv_zip(&[("MAL-2.json", mal_all("MAL-2024-0002", "badpkg"))]);
        let db = parse_feed(&zip).unwrap();
        assert_eq!(blocking_advisories(&db, "badpkg", "0.1"), ["MAL-2024-0002"]);
        assert_eq!(
            blocking_advisories(&db, "badpkg", "99.99.99"),
            ["MAL-2024-0002"]
        );
        // An unparseable version must still be blocked (fail-closed sentinel).
        assert_eq!(
            blocking_advisories(&db, "badpkg", "not-a-version"),
            ["MAL-2024-0002"]
        );
    }

    #[test]
    fn range_matches_below_fixed_boundary() {
        let zip = osv_zip(&[("P.json", pysec_fixed("PYSEC-2024-42", "widget", "2.20.0"))]);
        let db = parse_feed(&zip).unwrap();
        // Vulnerable strictly below the fix; safe at and above it.
        assert_eq!(advisories_for(&db, "widget", "2.19.0").len(), 1);
        assert_eq!(advisories_for(&db, "widget", "0").len(), 1);
        assert!(advisories_for(&db, "widget", "2.20.0").is_empty());
        assert!(advisories_for(&db, "widget", "2.21.0").is_empty());
        // A PYSEC advisory never blocks — it is audit-only.
        assert!(blocking_advisories(&db, "widget", "2.19.0").is_empty());
    }

    #[test]
    fn record_carries_verbatim_id_severity_and_fixed_in() {
        let zip = osv_zip(&[("P.json", pysec_fixed("PYSEC-2024-42", "widget", "2.20.0"))]);
        let db = parse_feed(&zip).unwrap();
        let recs = advisories_for(&db, "widget", "1.0");
        assert_eq!(recs[0].id, "PYSEC-2024-42"); // byte-equal (AC9)
        assert_eq!(recs[0].severity, "7.5");
        assert_eq!(recs[0].fixed_in, ["2.20.0"]);
    }

    #[test]
    fn database_specific_severity_wins_over_score() {
        let adv = serde_json::json!({
            "id": "GHSA-xxxx",
            "database_specific": {"severity": "HIGH"},
            "severity": [{"type": "CVSS_V3", "score": "9.8"}],
            "affected": [{"package": {"ecosystem": "PyPI", "name": "widget"}, "versions": ["1.0"]}],
        });
        let db = parse_feed(&osv_zip(&[("G.json", adv)])).unwrap();
        assert_eq!(advisories_for(&db, "widget", "1.0")[0].severity, "HIGH");
    }

    #[test]
    fn withdrawn_and_non_pypi_entries_are_skipped() {
        let withdrawn = serde_json::json!({
            "id": "MAL-2024-0003",
            "withdrawn": "2024-06-01T00:00:00Z",
            "affected": [{"package": {"ecosystem": "PyPI", "name": "widget"}, "versions": ["1.0"]}],
        });
        let npm = serde_json::json!({
            "id": "MAL-2024-0004",
            "affected": [{"package": {"ecosystem": "npm", "name": "widget"}, "versions": ["1.0"]}],
        });
        let db = parse_feed(&osv_zip(&[("w.json", withdrawn), ("n.json", npm)])).unwrap();
        assert!(blocking_advisories(&db, "widget", "1.0").is_empty());
        assert!(advisories_for(&db, "widget", "1.0").is_empty());
    }

    #[test]
    fn malformed_entry_does_not_sink_the_feed() {
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip.start_file("junk.json", opts).unwrap();
        zip.write_all(b"{not json").unwrap();
        zip.start_file("MAL.json", opts).unwrap();
        zip.write_all(
            mal_exact("MAL-2024-0005", "goodpkg", "1.0.0")
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        let bytes = zip.finish().unwrap().into_inner();
        let db = parse_feed(&bytes).unwrap();
        assert_eq!(
            blocking_advisories(&db, "goodpkg", "1.0.0"),
            ["MAL-2024-0005"]
        );
    }

    #[test]
    fn exact_match_is_version_normalized() {
        // The feed says 1.0; a request for 1.0.0 is the same PEP 440 version.
        let zip = osv_zip(&[("M.json", mal_exact("MAL-2024-0006", "pkg", "1.0"))]);
        let db = parse_feed(&zip).unwrap();
        assert_eq!(blocking_advisories(&db, "pkg", "1.0.0"), ["MAL-2024-0006"]);
        assert_eq!(blocking_advisories(&db, "pkg", "1.0"), ["MAL-2024-0006"]);
    }

    #[test]
    fn mal_advisory_is_both_blocked_and_audited() {
        let zip = osv_zip(&[("M.json", mal_exact("MAL-2024-0007", "pkg", "1.0.0"))]);
        let db = parse_feed(&zip).unwrap();
        assert_eq!(blocking_advisories(&db, "pkg", "1.0.0"), ["MAL-2024-0007"]);
        // Malware also shows in the audit index (with a blocked flag downstream).
        assert_eq!(advisories_for(&db, "pkg", "1.0.0")[0].id, "MAL-2024-0007");
        assert_eq!(db.block_names(), 1);
        assert_eq!(db.audit_records(), 1);
    }
}
