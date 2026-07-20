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
use std::time::Duration;

use anyhow::{bail, Context, Result};
use pep440_rs::Version;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tracing::{debug, info, warn};
use zip::ZipArchive;

use crate::clock::unix_now_secs;
use crate::hash::sha256_hex;
use crate::metrics::Metrics;
use crate::names::normalize_pkg_name;
use crate::ssrf::{guarded_get, guarded_get_with, Guard, SsrfGuardResolver};
use crate::storage::{self, Storage};

/// The verbatim snapshot, written by whichever source delivered it (leader OSV
/// pull, local-path ferry, or an admin `PUT`). A derived/carried view under the
/// reserved `_advisories/` prefix — deletable and regenerable, never truth.
pub const FEED_KEY: &str = "_advisories/osv-pypi.zip";

/// Worker-derived set of PEP 792 `quarantined` projects, persisted for the
/// byte-gate probe (rung 5). Defined now; unused until then.
pub(crate) const QUARANTINED_KEY: &str = "_advisories/quarantined.json";

/// Materialized org audit report: the walked inventory joined with the advisory
/// index and 30-day download counters, rebuilt at the end of each leader audit
/// sweep and served (admin-gated) at `/audit` and `/audit.json`.
pub(crate) const REPORT_KEY: &str = "_advisories/report.json";

/// The OSV bulk export for the PyPI ecosystem — the same database `uv audit`
/// queries live, so advisory ids shown by pypiron and by a laptop always agree.
/// No auth, ETag'd, regenerated continuously.
pub const DEFAULT_FEED_URL: &str =
    "https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip";

/// Hard ceiling on a fetched/read feed. The real export is ~32 MB; 256 MB is
/// generous headroom that still refuses a hostile or runaway body. Public so the
/// `sync` relay caps a source-server pull with the same bound.
pub const MAX_FEED_BYTES: u64 = 256 * 1024 * 1024;

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
    /// The newest `modified` timestamp seen across every entry in the snapshot
    /// (withdrawn/non-PyPI included), the baseline the per-node malware probe
    /// backfills from. `None` when no entry carried a parseable `modified`.
    watermark: Option<OffsetDateTime>,
}

impl AdvisoryDb {
    /// Distinct package names carrying at least one malware-block rule.
    pub(crate) fn block_names(&self) -> usize {
        self.block.len()
    }

    /// The snapshot's malware-probe watermark: the newest advisory `modified` it
    /// contains. The probe applies only advisories published after this.
    pub fn watermark(&self) -> Option<OffsetDateTime> {
        self.watermark
    }

    /// Total advisory records in the audit index.
    pub(crate) fn audit_records(&self) -> usize {
        self.audit.values().map(Vec::len).sum()
    }

    /// Whether any advisory in the audit index names `name_norm`. The cheap
    /// coarse filter that keeps the audit report's inventory collection
    /// proportional to matched packages (the corpus ∩ OSV), not corpus size.
    pub fn audit_has_name(&self, name_norm: &str) -> bool {
        self.audit.contains_key(name_norm)
    }
}

/// The `MAL-*` ids that condemn `name_norm` at `version` (empty = not blocked).
/// `name_norm` must already be [`normalize_pkg_name`]-normalized. `version` is
/// `None` when the filename yielded no parseable version (a legacy binary
/// format): fail-closed, only an all-versions rule (the "this package is
/// malware" case) still condemns it — an exact/range rule is a claim about a
/// specific version we can't read, so it can't be proven and doesn't fire.
pub fn blocking_advisories<'a>(
    db: &'a AdvisoryDb,
    name_norm: &str,
    version: Option<&str>,
) -> Vec<&'a str> {
    block_hits(&db.block, name_norm, version)
}

/// The `MAL-*` ids in a `name → rules` block map that condemn `name_norm` at
/// `version` — the shared core of both the baseline block set and the probe
/// overlay. `version = None` (unreadable filename) matches only all-versions rules.
fn block_hits<'a>(
    block: &'a HashMap<String, Vec<MalRule>>,
    name_norm: &str,
    version: Option<&str>,
) -> Vec<&'a str> {
    block
        .get(name_norm)
        .map(|rules| {
            rules
                .iter()
                .filter(|rule| match version {
                    Some(v) => rule.scope.matches(v),
                    None => matches!(rule.scope, VersionScope::AllVersions),
                })
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

// ---------------------------------------------------------------------------
// Org audit report (rung 7): the inventory × audit-index join, materialized.
// ---------------------------------------------------------------------------

/// One (package, version) the audit walked, with its resolved origin and 30-day
/// download count — the worker-supplied input to the pure [`build_report`] join.
/// `origin` is the claim label ([`crate::origin::PRIVATE`]/`MIRROR`/`UNCLAIMED`).
pub struct AuditInventory {
    pub package: String,
    pub version: String,
    pub origin: String,
    pub downloads_30d: u64,
}

/// One materialized audit row: a hosted (package, version) that a known advisory
/// affects. Serialized verbatim into [`REPORT_KEY`] and served by `/audit.json`,
/// so field names are the JSON contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReportRow {
    pub package: String,
    pub version: String,
    pub origin: String,
    /// Advisory ids affecting this version (sorted, deduped; never rewritten).
    pub advisories: Vec<String>,
    pub severity: String,
    pub fixed_in: Vec<String>,
    pub downloads_30d: u64,
    /// Whether the byte gate would 403 this file: a `MAL-*` match (on the
    /// included non-private origin) or a quarantined project. Vulnerabilities are
    /// informational (`false`); malware is blocked (`true`).
    pub blocked: bool,
}

/// The materialized org audit: the join of hosted inventory with the advisory
/// index, ranked by downloads. `generated_unix` stamps the build time;
/// `feed_sha256` is the snapshot the rows were derived from.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Report {
    pub generated_unix: u64,
    pub feed_sha256: String,
    pub rows: Vec<ReportRow>,
}

/// Join walked inventory × the audit index into the materialized report. Pure and
/// unit-tested — no I/O, deterministic output. Private-origin entries are excluded
/// (OSV names live in PyPI's namespace; origin exclusivity proves a same-named
/// private package is not the one OSV named — `Unclaimed` counts as non-private,
/// included). An entry no advisory affects yields no row (the report lists only
/// vulnerable/malicious inventory). `blocked` mirrors the byte gate: a `MAL-*`
/// match or a quarantined project. Rows sort by downloads descending, then
/// package/version, so an unchanged corpus serializes byte-identically.
pub fn build_report(
    inventory: &[AuditInventory],
    db: &AdvisoryDb,
    quarantined: &HashSet<String>,
    generated_unix: u64,
    feed_sha256: &str,
) -> Report {
    let mut rows = Vec::new();
    for item in inventory {
        if item.origin == crate::origin::PRIVATE {
            continue;
        }
        let records = advisories_for(db, &item.package, &item.version);
        if records.is_empty() {
            continue;
        }
        let mut advisories: Vec<String> = records.iter().map(|r| r.id.clone()).collect();
        advisories.sort();
        advisories.dedup();
        // Severity: the first non-empty among the matched records (one package's
        // advisories, usually one). Verbatim from the feed, never rewritten.
        let severity = records
            .iter()
            .map(|r| r.severity.as_str())
            .find(|s| !s.is_empty())
            .unwrap_or_default()
            .to_string();
        let mut fixed_in: Vec<String> = records
            .iter()
            .flat_map(|r| r.fixed_in.iter().cloned())
            .collect();
        fixed_in.sort();
        fixed_in.dedup();
        let blocked = !blocking_advisories(db, &item.package, Some(&item.version)).is_empty()
            || quarantined.contains(&item.package);
        rows.push(ReportRow {
            package: item.package.clone(),
            version: item.version.clone(),
            origin: item.origin.clone(),
            advisories,
            severity,
            fixed_in,
            downloads_30d: item.downloads_30d,
            blocked,
        });
    }
    rows.sort_by(|a, b| {
        b.downloads_30d
            .cmp(&a.downloads_30d)
            .then_with(|| a.package.cmp(&b.package))
            .then_with(|| a.version.cmp(&b.version))
    });
    Report {
        generated_unix,
        feed_sha256: feed_sha256.to_string(),
        rows,
    }
}

// ---- OSV wire structs (tolerant: every field defaulted, unknowns ignored) ---

#[derive(Deserialize)]
struct OsvAdvisory {
    #[serde(default)]
    id: String,
    #[serde(default)]
    summary: String,
    /// RFC 3339 last-modified stamp — the probe's watermark axis. OSV bumps it on
    /// every content change (including a withdrawal), so it is monotone per id.
    #[serde(default)]
    modified: Option<String>,
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
            Ok(adv) => {
                // The watermark spans every entry (withdrawn/non-PyPI included): a
                // withdrawn advisory reappears at the top of the probe's CSV with a
                // bumped `modified`, and the baseline must already cover it so the
                // probe doesn't re-walk it.
                advance_watermark(&mut db.watermark, adv.modified.as_deref());
                if !ingest_advisory(&mut db, &adv) {
                    skipped += 1;
                }
            }
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

/// Iterate an advisory's PyPI-ecosystem affected clauses paired with their
/// normalized package name, skipping non-PyPI ecosystems and unservable names.
fn pypi_clauses(adv: &OsvAdvisory) -> impl Iterator<Item = (String, &OsvAffected)> {
    adv.affected.iter().filter_map(|affected| {
        if !affected
            .package
            .ecosystem
            .eq_ignore_ascii_case(PYPI_ECOSYSTEM)
        {
            return None;
        }
        checked_osv_name(&affected.package.name).map(|name| (name, affected))
    })
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
    for (name, affected) in pypi_clauses(adv) {
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

/// Parse an OSV `modified`/CSV RFC 3339 timestamp. `None` (unparseable) sorts as
/// "no information" — the probe treats it as not-newer, never advancing the
/// watermark past a stamp it can't read.
fn parse_modified(raw: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(raw, &Rfc3339).ok()
}

/// Fold a candidate `modified` into a running max (the snapshot watermark).
fn advance_watermark(current: &mut Option<OffsetDateTime>, candidate: Option<&str>) {
    if let Some(ts) = candidate.and_then(parse_modified) {
        if current.is_none_or(|c| ts > c) {
            *current = Some(ts);
        }
    }
}

/// The `MAL-*` block rules one parsed advisory contributes: `(normalized_name,
/// rule)` per affected PyPI clause. Empty when the advisory is not `MAL-*`,
/// withdrawn, or names no PyPI package. The same clause logic
/// [`ingest_advisory`] uses, projected to the block set alone — what the probe
/// overlays ahead of the daily baseline.
fn mal_rules(adv: &OsvAdvisory) -> Vec<(String, MalRule)> {
    if adv.withdrawn.is_some() || adv.id.is_empty() || !adv.id.starts_with("MAL-") {
        return Vec::new();
    }
    pypi_clauses(adv)
        .map(|(name, affected)| {
            (
                name,
                MalRule {
                    id: adv.id.clone(),
                    scope: scope_for(affected),
                },
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Live snapshot state + refresh plumbing (blackbox-tested)
// ---------------------------------------------------------------------------

/// The live snapshot every request-path probe reads. Held behind
/// `Arc<RwLock<Arc<AdvisoryState>>>`: a reader takes the lock, clones the inner
/// `Arc`, and drops the guard immediately, so probes never contend with a
/// refresh swap. A poisoned lock is recovered, never a panic path.
#[derive(Clone, Default)]
pub struct AdvisoryState {
    /// The parsed views, or `None` until the first snapshot loads (armed but
    /// unfed — nothing to block yet, degraded not fatal). Behind an `Arc` so a
    /// quarantined-set swap (a separate, more frequent event) carries the whole
    /// db forward with a refcount bump — never a deep clone under the write lock.
    pub db: Option<Arc<AdvisoryDb>>,
    /// Content identity of the loaded snapshot (hex sha256 of the zip).
    pub zip_sha256: Option<String>,
    /// The verbatim snapshot bytes, retained in memory (~32 MB, behind an `Arc`
    /// so state swaps stay refcount bumps). `_advisories/` is NOT replicated, so
    /// after a failover to a never-seeded bucket the zip would exist nowhere
    /// reachable; the leader re-seeds these bytes when it finds `FEED_KEY` absent
    /// on its selected bucket, keeping a fresh node from booting unfed. Also lets
    /// `GET /advisories/feed` serve without a 32 MB storage read + re-hash.
    pub zip: Option<Arc<Vec<u8>>>,
    /// The storage `ObjectMeta.etag` the loaded snapshot was read at, so the
    /// follower reload skips the 32 MB GET when the key hasn't moved.
    pub storage_etag: Option<String>,
    /// Per-node malware-probe overlay: `MAL-*` block rules applied ahead of the
    /// daily baseline, keyed like [`AdvisoryDb::block`] (normalized name → rules).
    /// Consulted by the byte gate ∪ the baseline. Cleared to empty on every
    /// snapshot (re)load — the fresh baseline absorbs it and the probe backfills
    /// anything still newer, so there are no coherence states to reason about.
    pub overlay: Arc<HashMap<String, Vec<MalRule>>>,
    /// Worker-derived `quarantined` projects (rung 5), consulted by the byte
    /// gate. Carried across feed reloads; swapped by its own etag-gated reload.
    pub quarantined: HashSet<String>,
    /// The storage `ObjectMeta.etag` [`QUARANTINED_KEY`] was read at, so the
    /// every-node reload skips the GET when the set hasn't moved. Absent key and
    /// never-loaded both read as `None` → an empty set that never un-blocks.
    pub quarantined_etag: Option<String>,
    /// Unix seconds of the load (0 = never loaded).
    pub loaded_unix: u64,
}

impl AdvisoryState {
    /// Read the current snapshot: lock, clone the inner `Arc`, drop the guard.
    /// Recovers a poisoned lock instead of panicking (never a request-path panic).
    pub fn read(slot: &RwLock<Arc<AdvisoryState>>) -> Arc<AdvisoryState> {
        slot.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// The `MAL-*` ids blocking `(name_norm, version)` across the baseline block
    /// set ∪ the probe overlay (deduped by id, union of matches — fail-closed).
    /// Owned strings because the two sources are separate maps. `name_norm` must
    /// already be [`normalize_pkg_name`]-normalized.
    pub fn blocking(&self, name_norm: &str, version: Option<&str>) -> Vec<String> {
        let mut ids: Vec<String> = self
            .db
            .as_ref()
            .map(|db| block_hits(&db.block, name_norm, version))
            .unwrap_or_default()
            .into_iter()
            .map(str::to_string)
            .collect();
        for id in block_hits(&self.overlay, name_norm, version) {
            if !ids.iter().any(|seen| seen == id) {
                ids.push(id.to_string());
            }
        }
        ids
    }

    /// Whether anything can block yet: a loaded baseline, or a non-empty probe
    /// overlay. `false` is the armed-but-unfed node with nothing to enforce.
    pub fn has_block_data(&self) -> bool {
        self.db.is_some() || !self.overlay.is_empty()
    }
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

/// An SSRF-guarded HTTP client for a feed URL: the resolver closes DNS-rebind and
/// redirects are followed manually (through `guarded_get*`) so every hop is
/// re-validated. Honors `HTTP(S)_PROXY` (corporate egress and the hermetic tests
/// both reach OSV through a forward proxy). Shared by the daily feed poll and the
/// per-node probe.
fn build_feed_client(url: &str) -> Result<(reqwest::Client, Arc<Guard>)> {
    let guard = Arc::new(Guard::new(url, &[], &[])?);
    let client = reqwest::Client::builder()
        .user_agent("pypiron-advisory/0.1 (+https://github.com/blackthorn-interstellar/pypiron)")
        // The resolver catches name targets (and DNS-rebind); IP-literal targets
        // are caught by guarded_get's pre-flight, which owns redirect-following so
        // every hop is re-validated. Auto-follow would let a redirect Location
        // reach a forbidden literal the resolver never sees — so disable it and go
        // through guarded_get.
        .dns_resolver(Arc::new(SsrfGuardResolver::new(guard.clone())))
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .build()
        .context("building advisory feed HTTP client")?;
    Ok((client, guard))
}

/// A resolved feed source. Built once per process (the URL client and its SSRF
/// guard are reused across polls); a local path holds no client.
pub(crate) struct FeedSource {
    feed: String,
    url_client: Option<(reqwest::Client, Arc<Guard>)>,
}

impl FeedSource {
    /// Build the source for `feed`. A URL gets an SSRF-guarded client that —
    /// unlike sync/proxy — honors `HTTP(S)_PROXY`, because corporate egress and
    /// the hermetic tests both reach OSV through a forward proxy.
    pub fn new(feed: &str) -> Result<Self> {
        let url_client = if is_url(feed) {
            Some(build_feed_client(feed)?)
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
            Some((client, guard)) => fetch_url(client, guard, &self.feed, http_etag).await,
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

/// Fetch a feed source (a `http(s)` URL or a local path) to bytes, once, for the
/// `sync` relay's explicit `--advisory-feed`. Reuses the server's own
/// [`FeedSource`] — a URL rides the SSRF-guarded, proxy-honoring, 60s-read,
/// size-capped client; a path is read with the same ceiling. Unconditional (no
/// `If-None-Match`), so a source that answers `304` to a first request is
/// misbehaving and errors rather than silently yielding no bytes.
pub async fn fetch_feed_bytes(feed: &str) -> Result<Vec<u8>> {
    match FeedSource::new(feed)?.poll(None).await? {
        RawFetch::Bytes { bytes, .. } => Ok(bytes),
        RawFetch::NotModified => {
            bail!("advisory feed source answered 304 to an unconditional fetch")
        }
    }
}

/// Conditional GET of a URL feed, size-capped. A `304` short-circuits with no
/// body; otherwise the ETag rides back for the next poll.
async fn fetch_url(
    client: &reqwest::Client,
    guard: &Guard,
    url: &str,
    if_none_match: Option<&str>,
) -> Result<RawFetch> {
    // Route through the SSRF pre-flight (like proxy/sync): check_target runs on
    // the initial URL and re-validates every redirect Location, so an IP-literal
    // hop the DNS resolver can't see is refused rather than followed.
    let parsed =
        reqwest::Url::parse(url).with_context(|| format!("parsing advisory feed URL {url}"))?;
    let resp = guarded_get_with(client, guard, parsed, None, |req| match if_none_match {
        Some(tag) => req.header(reqwest::header::IF_NONE_MATCH, tag),
        None => req,
    })
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
    let http_etag = response_etag(&resp);
    let bytes = read_body_capped(resp, MAX_FEED_BYTES, url).await?;
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

/// Parse feed bytes off the async runtime (a full export is CPU-bound). Takes an
/// `Arc` so the caller retains the same bytes (as [`AdvisoryState::zip`]) without
/// a second copy — the parse borrows them, the caller keeps the `Arc`.
async fn parse_off_thread(bytes: Arc<Vec<u8>>) -> Result<(AdvisoryDb, String)> {
    tokio::task::spawn_blocking(move || {
        let slice: &[u8] = &bytes;
        let db = parse_feed(slice)?;
        Ok((db, sha256_hex(slice)))
    })
    .await
    .context("advisory parse task panicked")?
}

/// Read `key`'s current storage etag (via a 1-key LIST, no body), or `None` when
/// the key is absent.
async fn key_storage_etag(storage: &dyn Storage, key: &str) -> Result<Option<String>> {
    let page = storage.list_page(key, None, 1).await?;
    Ok(page.into_iter().find(|m| m.key == key).map(|m| m.etag))
}

/// Read `FEED_KEY`'s current storage etag (via a 1-key LIST, no body), or `None`
/// when no snapshot exists yet.
pub async fn feed_storage_etag(storage: &dyn Storage) -> Result<Option<String>> {
    key_storage_etag(storage, FEED_KEY).await
}

/// Load the persisted snapshot from storage, or `None` when the key is absent.
async fn load_from_storage(storage: &dyn Storage) -> Result<Option<AdvisoryState>> {
    let Some(etag) = feed_storage_etag(storage).await? else {
        return Ok(None);
    };
    let zip = Arc::new(storage.get_bytes(FEED_KEY).await?);
    let (db, sha) = parse_off_thread(zip.clone()).await?;
    Ok(Some(AdvisoryState {
        db: Some(Arc::new(db)),
        zip_sha256: Some(sha),
        zip: Some(zip),
        storage_etag: Some(etag),
        overlay: Arc::default(),
        quarantined: HashSet::new(),
        quarantined_etag: None,
        loaded_unix: unix_now_secs(),
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
    let zip = Arc::new(bytes);
    let (db, sha) = match parse_off_thread(zip.clone()).await {
        Ok(pair) => pair,
        Err(e) => {
            warn!(error = %e, "advisory feed: source did not parse; trying stored snapshot");
            return None;
        }
    };
    if let Err(e) = storage
        .put_bytes(FEED_KEY, (*zip).clone(), Some("application/zip"))
        .await
    {
        warn!(error = %e, "advisory feed: persisting snapshot failed; loading in-memory");
    }
    Some(AdvisoryState {
        db: Some(Arc::new(db)),
        zip_sha256: Some(sha),
        zip: Some(zip),
        storage_etag: feed_storage_etag(storage).await.unwrap_or(None),
        overlay: Arc::default(),
        quarantined: HashSet::new(),
        quarantined_etag: None,
        loaded_unix: unix_now_secs(),
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
    /// Whether we have already warned that blocking is armed but unfed, so the
    /// "armed but unfed" line is logged once per unfed period, not per tick.
    unfed_warned: bool,
}

/// Latch source reachability and log only on a state change: a first failure
/// warns, a first recovery informs, and steady state stays silent. Returns the
/// success payload so the caller can do cycle-specific work on `Ok`.
fn note_source_result<T>(
    source_failing: &mut bool,
    result: Result<T>,
    recovered: &str,
    failed: &str,
) -> Option<T> {
    match result {
        Ok(v) => {
            if *source_failing {
                info!("{recovered}");
                *source_failing = false;
            }
            Some(v)
        }
        Err(e) => {
            if !*source_failing {
                warn!(error = %e, "{failed}");
                *source_failing = true;
            }
            None
        }
    }
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
            let result = poll_and_persist(&ctx, feed, memo).await;
            if note_source_result(
                &mut memo.source_failing,
                result,
                "advisory feed: source reachable again",
                "advisory feed: source unreachable; serving last snapshot",
            )
            .is_some()
            {
                refreshed_ok = true;
            }
        }
    }

    // The leader re-seeds a snapshot it holds onto a selected bucket that lacks
    // it — the failover-to-a-starved-bucket heal (`_advisories/` isn't fanned out
    // as truth, so nothing else would). Runs for any leader (a storage-delivered
    // feed has no source poll above but must still re-seed).
    if is_leader {
        if let Err(e) = reseed_if_absent(&ctx).await {
            debug!(error = %e, "advisory feed: re-seed check failed; will retry next tick");
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

    // Every-node quarantined-set reload, on the same tick as the zip. The leader
    // derives and publishes it from its audit sweep; every node (leader included)
    // adopts it here when the key's etag moves. Independent of the feed snapshot's
    // staleness gauge — the quarantined set has its own storage object.
    if let Err(e) = reload_quarantined(&ctx).await {
        debug!(error = %e, "advisory feed: quarantined reload failed; serving last set");
    }

    // Arm the gauge only once a snapshot is actually loaded — an armed-but-unfed
    // node has nothing to age.
    let has_snapshot = AdvisoryState::read(ctx.slot).db.is_some();
    if refreshed_ok && has_snapshot {
        ctx.metrics.advisory_refresh_ok();
    }

    // Armed but unfed: the feature is on but no snapshot is available yet. Warn
    // once (this is where the implicit default lands after its first background
    // obtain fails), and re-arm the warning if a later loss ever recurs.
    if !has_snapshot && !memo.unfed_warned {
        warn!(
            "malware blocking armed but unfed: no advisory snapshot is available yet, so nothing \
             is blocked. It self-arms the moment a snapshot arrives (a local-path ferry or a sync push)."
        );
        memo.unfed_warned = true;
    } else if has_snapshot {
        memo.unfed_warned = false;
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
    // Validate before persisting — the storage copy is what every node loads. The
    // every-node reload then reads it back and retains the bytes as `zip`.
    let bytes = Arc::new(bytes);
    parse_off_thread(bytes.clone()).await?;
    ctx.storage
        .put_bytes(FEED_KEY, (*bytes).clone(), Some("application/zip"))
        .await
        .context("persisting advisory snapshot")?;
    Ok(())
}

/// Leader-only: if the selected bucket is missing `FEED_KEY` but we hold a
/// snapshot in memory, re-persist the retained bytes. Because `_advisories/` is
/// never fanned out as truth, a failover to a never-seeded bucket would leave the
/// zip nowhere reachable — running nodes stay armed on their in-memory copy, but a
/// fresh node booting onto that bucket (feed maybe unreachable) would come up
/// armed-but-unfed and serve malware. Re-seeding heals that as long as one armed
/// node survives; the write is once per absence (the next tick sees the key
/// present and the normal etag flow resumes). A full-fleet cold start onto a
/// starved bucket with the feed unreachable is still operator-ferry territory.
async fn reseed_if_absent(ctx: &RefreshCtx<'_>) -> Result<()> {
    let Some(zip) = AdvisoryState::read(ctx.slot).zip.clone() else {
        return Ok(()); // nothing retained to re-seed
    };
    if feed_storage_etag(ctx.storage).await?.is_some() {
        return Ok(()); // the key is present on the selected bucket
    }
    ctx.storage
        .put_bytes(FEED_KEY, (*zip).clone(), Some("application/zip"))
        .await
        .context("re-seeding advisory snapshot onto a bucket missing it")?;
    info!("advisory feed: re-seeded the snapshot onto a selected bucket that was missing it");
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
    let zip = Arc::new(ctx.storage.get_bytes(FEED_KEY).await?);
    let (db, sha) = parse_off_thread(zip.clone()).await?;
    let db = Arc::new(db);
    // Read the quarantined set under the same lock we store under (no await
    // between), so a concurrent `publish_quarantined`/`reload_quarantined` swap
    // is carried forward instead of clobbered — the two swaps each preserve the
    // other's field.
    let mut guard = ctx.slot.write().unwrap_or_else(|p| p.into_inner());
    *guard = Arc::new(AdvisoryState {
        db: Some(db),
        zip_sha256: Some(sha),
        zip: Some(zip),
        storage_etag: Some(etag),
        // A fresh baseline clears the probe overlay: it now covers everything up
        // to its watermark, and the next probe backfills anything still newer.
        overlay: Arc::default(),
        loaded_unix: unix_now_secs(),
        // Carry the quarantined set + its etag forward (read under this lock).
        ..(**guard).clone()
    });
    Ok(true)
}

/// `QUARANTINED_KEY`'s current storage etag (a 1-key LIST, no body), or `None`
/// when the leader has never written the set — which reads as an empty set.
async fn quarantined_storage_etag(storage: &dyn Storage) -> Result<Option<String>> {
    key_storage_etag(storage, QUARANTINED_KEY).await
}

/// Parse the persisted quarantined-set JSON (a sorted string array) into a set.
fn parse_quarantined(bytes: &[u8]) -> Result<HashSet<String>> {
    let names: Vec<String> = serde_json::from_slice(bytes).context("parsing quarantined set")?;
    Ok(names.into_iter().collect())
}

/// Every-node half for the quarantined set: reload it when `QUARANTINED_KEY`'s
/// storage etag moves, exactly like the zip. An absent key is an empty set (the
/// leader has published nothing, or a dequarantine emptied it — the leader writes
/// `[]`, never deletes, so followers still see the etag move). A garbage body
/// warns and keeps the previous set: a corrupt read must never un-quarantine.
/// The db is carried forward under the lock (see [`reload`]).
async fn reload_quarantined(ctx: &RefreshCtx<'_>) -> Result<()> {
    let etag = quarantined_storage_etag(ctx.storage).await?;
    if etag == AdvisoryState::read(ctx.slot).quarantined_etag {
        return Ok(()); // unchanged (same etag, or still-absent None == None)
    }
    let quarantined = match &etag {
        None => HashSet::new(),
        Some(_) => match parse_quarantined(&ctx.storage.get_bytes(QUARANTINED_KEY).await?) {
            Ok(set) => set,
            Err(e) => {
                warn!(error = %e, "advisory feed: quarantined set did not parse; keeping previous");
                return Ok(());
            }
        },
    };
    swap_quarantined(ctx.slot, quarantined, etag);
    Ok(())
}

/// Swap the quarantined set and its etag into the live snapshot, carrying the db
/// and feed identity forward under the write lock (a refcount bump each, no deep
/// clone, no await). Shared by the leader's immediate swap and the every-node
/// reload.
fn swap_quarantined(
    slot: &RwLock<Arc<AdvisoryState>>,
    quarantined: HashSet<String>,
    quarantined_etag: Option<String>,
) {
    let mut guard = slot.write().unwrap_or_else(|p| p.into_inner());
    *guard = Arc::new(AdvisoryState {
        quarantined,
        quarantined_etag,
        ..(**guard).clone()
    });
}

/// Leader: publish the worker-derived quarantined set. Writes `QUARANTINED_KEY`
/// only when the set actually changed from what's loaded (including an empty
/// array on a transition to empty, so a dequarantine propagates to followers),
/// then swaps the leader's own in-memory set immediately so its byte gate reflects
/// the change without waiting for the next etag poll. `set` must be the result of
/// a *complete* sweep — a partial set would flap dequarantines.
pub async fn publish_quarantined(
    storage: &dyn Storage,
    slot: &RwLock<Arc<AdvisoryState>>,
    set: std::collections::BTreeSet<String>,
) -> Result<()> {
    let current = AdvisoryState::read(slot);
    let unchanged = current.quarantined.len() == set.len()
        && set.iter().all(|name| current.quarantined.contains(name));
    if unchanged {
        return Ok(());
    }
    // BTreeSet iterates sorted, so the persisted array is stable — an unchanged
    // set never rewrites the key and stampedes followers into a reload.
    let names: Vec<&String> = set.iter().collect();
    let bytes = serde_json::to_vec(&names).context("serializing quarantined set")?;
    storage
        .put_bytes(QUARANTINED_KEY, bytes, Some("application/json"))
        .await
        .context("persisting quarantined set")?;
    // Adopt the just-written etag so this node's own reload poll no-ops.
    let etag = quarantined_storage_etag(storage).await.unwrap_or(None);
    swap_quarantined(slot, set.into_iter().collect(), etag);
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-node malware probe: block a new MAL-* advisory within minutes of OSV
// publishing it, ahead of the daily baseline, at near-zero bandwidth.
// ---------------------------------------------------------------------------

/// Head bytes of the modified-id CSV pulled first, before any range-doubling. The
/// freshest advisories sit at the very top; a MAL burst is rarely more than a few
/// KB, so most polls read this little (and most read nothing — a 304).
const PROBE_CSV_START_BYTES: u64 = 4096;

/// Ceiling on one fetched advisory JSON (OSV records are a few KB); reuses the
/// zip per-entry bound.
const MAX_ADVISORY_JSON_BYTES: u64 = MAX_ENTRY_BYTES;

/// The sibling base URL of a probe-shaped feed: a `.../all.zip` URL yields
/// `.../PyPI`, from which `modified_id.csv` and `<ID>.json` hang. `None` for a
/// non-URL feed or a URL that isn't the OSV `all.zip` export — the probe is inert
/// for those (no sibling endpoints to poll).
pub fn probe_base(feed: &str) -> Option<String> {
    if !is_url(feed) {
        return None;
    }
    feed.strip_suffix("/all.zip").map(str::to_string)
}

/// Per-node probe bookkeeping, owned by the worker loop (never shared, so a
/// failover simply re-baselines from the snapshot). Mirrors [`RefreshMemo`].
#[derive(Default)]
pub struct ProbeMemo {
    /// The snapshot identity (`zip_sha256`) the probe state is aligned to. When it
    /// moves, a reload cleared the overlay: re-baseline the watermark and drop the
    /// CSV etag so the next poll re-walks from the new floor.
    aligned_sha: Option<String>,
    /// The floor: advisories with `modified` at or below this are already covered
    /// by the baseline or a prior application, so the walk stops there.
    watermark: Option<OffsetDateTime>,
    /// The CSV's last-seen ETag, for the next conditional GET.
    csv_etag: Option<String>,
    /// Whether the source was failing last cycle (warn on transition, not per poll).
    source_failing: bool,
}

/// One scan of the modified-id CSV head (a pure decision over already-fetched
/// text — unit-tested).
#[derive(Debug, Default, PartialEq)]
struct CsvScan {
    /// `(modified, id)` for `MAL-*` lines strictly newer than the watermark, in
    /// file order (newest first).
    fresh_mal: Vec<(OffsetDateTime, String)>,
    /// The newest `modified` across ALL fresh lines (MAL or not) — the watermark
    /// advance target, so a non-MAL bump isn't re-walked next cycle.
    max_modified: Option<OffsetDateTime>,
    /// True once a line at/below the watermark was seen (the fresh window is fully
    /// contained in the fetched head) or the whole file was fetched. False means
    /// the head may be truncating fresh lines — the caller doubles the range.
    complete: bool,
}

/// A completed CSV fetch: either a 304 (unchanged) or the fetched head plus its
/// ETag and whether it is the whole file.
enum CsvWalk {
    NotModified,
    Scanned { scan: CsvScan, etag: Option<String> },
}

/// Whether an id is a safe `MAL-*` advisory id to interpolate into a URL path —
/// defends the per-advisory fetch against a hostile CSV line (path traversal).
fn is_probe_advisory_id(id: &str) -> bool {
    id.starts_with("MAL-")
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._".contains(&b))
}

/// Parse the CSV head top-down, stopping at the first line at/below `watermark`
/// (the file is sorted newest-first). `is_full` is whether the fetched bytes are
/// the whole file — if not, the final (possibly truncated) line is dropped and an
/// unterminated window signals the caller to widen the range.
fn parse_csv_head(text: &str, watermark: Option<OffsetDateTime>, is_full: bool) -> CsvScan {
    let mut lines: Vec<&str> = text.split('\n').collect();
    // A ranged fetch can truncate the final line; drop it. On the full file the
    // trailing segment is either "" (file ended in \n) or a real last line — the
    // empty-line filter and `is_full` handle both.
    if !is_full {
        lines.pop();
    }
    let mut scan = CsvScan {
        complete: is_full,
        ..CsvScan::default()
    };
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((ts_raw, id)) = line.split_once(',') else {
            continue;
        };
        let Some(ts) = parse_modified(ts_raw.trim()) else {
            continue;
        };
        if watermark.is_some_and(|w| ts <= w) {
            scan.complete = true;
            break;
        }
        advance_watermark(&mut scan.max_modified, Some(ts_raw.trim()));
        let id = id.trim();
        if is_probe_advisory_id(id) {
            scan.fresh_mal.push((ts, id.to_string()));
        }
    }
    scan
}

/// Upsert probe results into an overlay map: drop every rule whose id is being
/// re-applied or withdrawn, then add the new rules. Idempotent (a re-applied id
/// replaces, never duplicates) and order-independent; empty vectors are pruned.
fn merge_overlay(
    overlay: &mut HashMap<String, Vec<MalRule>>,
    added: Vec<(String, MalRule)>,
    withdrawn: &HashSet<String>,
) {
    let mut stale: HashSet<String> = withdrawn.clone();
    for (_, rule) in &added {
        stale.insert(rule.id.clone());
    }
    if !stale.is_empty() {
        for rules in overlay.values_mut() {
            rules.retain(|r| !stale.contains(&r.id));
        }
        overlay.retain(|_, rules| !rules.is_empty());
    }
    for (name, rule) in added {
        overlay.entry(name).or_default().push(rule);
    }
}

/// Swap the probe overlay in the shared slot: upsert `added`, remove `withdrawn`,
/// carrying the baseline/quarantine forward under the write lock (refcount bumps,
/// no await, no deep clone of the db). Shared byte-gate reads stay lock-cheap.
fn apply_probe_overlay(
    slot: &RwLock<Arc<AdvisoryState>>,
    added: Vec<(String, MalRule)>,
    withdrawn: &HashSet<String>,
) {
    let mut guard = slot.write().unwrap_or_else(|p| p.into_inner());
    let mut overlay = (*guard.overlay).clone();
    merge_overlay(&mut overlay, added, withdrawn);
    *guard = Arc::new(AdvisoryState {
        overlay: Arc::new(overlay),
        ..(**guard).clone()
    });
}

/// The probe's SSRF-guarded client bound to a feed's sibling base URL.
struct ProbeSource {
    base: String,
    client: reqwest::Client,
    guard: Arc<Guard>,
}

impl ProbeSource {
    fn new(base: &str) -> Result<Self> {
        let (client, guard) = build_feed_client(base)?;
        Ok(Self {
            base: base.to_string(),
            client,
            guard,
        })
    }

    /// Conditional, ranged GET of the modified-id CSV. `Ok(None)` on a 304 (the
    /// common poll). `is_full` is inferred from a short read: asking for `end`
    /// bytes and getting fewer means EOF, so the whole file is in hand.
    async fn fetch_csv(&self, if_none_match: Option<&str>, end: u64) -> Result<Option<CsvFetch>> {
        let url = format!("{}/modified_id.csv", self.base);
        let parsed = reqwest::Url::parse(&url).with_context(|| format!("parsing {url}"))?;
        let range = format!("bytes=0-{}", end.saturating_sub(1));
        let resp = guarded_get_with(&self.client, &self.guard, parsed, None, |req| {
            let req = req.header(reqwest::header::RANGE, &range);
            match if_none_match {
                Some(tag) => req.header(reqwest::header::IF_NONE_MATCH, tag),
                None => req,
            }
        })
        .await
        .with_context(|| format!("fetching {url}"))?;
        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(None);
        }
        let resp = resp
            .error_for_status()
            .with_context(|| format!("fetching {url}"))?;
        let etag = response_etag(&resp);
        let bytes = read_body_capped(resp, MAX_FEED_BYTES, &url).await?;
        let is_full = (bytes.len() as u64) < end;
        Ok(Some(CsvFetch {
            text: String::from_utf8_lossy(&bytes).into_owned(),
            etag,
            is_full,
        }))
    }

    /// GET and parse one advisory's JSON (`<base>/<ID>.json`).
    async fn fetch_advisory(&self, id: &str) -> Result<OsvAdvisory> {
        let url = format!("{}/{id}.json", self.base);
        let parsed = reqwest::Url::parse(&url).with_context(|| format!("parsing {url}"))?;
        let resp = guarded_get(
            &self.client,
            &self.guard,
            parsed,
            Some(Duration::from_secs(30)),
        )
        .await
        .with_context(|| format!("fetching {url}"))?
        .error_for_status()
        .with_context(|| format!("fetching {url}"))?;
        let bytes = read_body_capped(resp, MAX_ADVISORY_JSON_BYTES, &url).await?;
        serde_json::from_slice(&bytes).with_context(|| format!("parsing advisory {id}"))
    }
}

/// One CSV fetch's text plus its ETag and whether it is the whole file.
struct CsvFetch {
    text: String,
    etag: Option<String>,
    is_full: bool,
}

/// The response's `ETag` header as an owned string, if present and UTF-8.
fn response_etag(resp: &reqwest::Response) -> Option<String> {
    resp.headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Stream a response body into memory under a hard cap.
async fn read_body_capped(resp: reqwest::Response, cap: u64, url: &str) -> Result<Vec<u8>> {
    use futures::StreamExt;
    let mut bytes = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("reading {url}"))?;
        if bytes.len() as u64 + chunk.len() as u64 > cap {
            bail!("{url} exceeds {cap} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Walk the CSV from its head, doubling the fetched range until the fresh window
/// is fully contained (a line at/below the watermark) or the whole file is read.
async fn walk_csv(
    source: &ProbeSource,
    etag: Option<&str>,
    watermark: Option<OffsetDateTime>,
) -> Result<CsvWalk> {
    let mut end = PROBE_CSV_START_BYTES;
    // Only the first request is conditional; a doubling already knows it changed.
    let mut conditional = etag;
    loop {
        let Some(fetch) = source.fetch_csv(conditional, end).await? else {
            return Ok(CsvWalk::NotModified);
        };
        conditional = None;
        let scan = parse_csv_head(&fetch.text, watermark, fetch.is_full);
        if scan.complete || end >= MAX_FEED_BYTES {
            return Ok(CsvWalk::Scanned {
                scan,
                etag: fetch.etag,
            });
        }
        end = end.saturating_mul(2).min(MAX_FEED_BYTES);
    }
}

/// One per-node malware-probe cycle: conditional-GET the modified-id CSV, apply
/// any `MAL-*` advisory newer than the baseline ahead of the daily snapshot, and
/// un-block withdrawn ones. Purely additive — a failure degrades to daily-baseline
/// behavior, never fail-open. `feed` is the `.../all.zip` URL; the CSV and
/// per-advisory URLs are its siblings. Runs on every node (not leader-gated).
pub async fn probe(ctx: RefreshCtx<'_>, feed: &str, memo: &mut ProbeMemo) {
    let result = probe_once(&ctx, feed, memo).await;
    if let Some(armed) = note_source_result(
        &mut memo.source_failing,
        result,
        "malware probe: advisory source reachable again",
        "malware probe: advisory source unreachable; serving the daily baseline",
    ) {
        // Arm the age gauge only when a cycle actually ran (a snapshot exists);
        // an idle armed-but-unfed node has nothing to age.
        if armed {
            ctx.metrics.malware_probe_ok();
        }
    }
}

/// The probe cycle proper. Returns whether a cycle ran (a snapshot was loaded);
/// `false` idles (armed but unfed). Advances `memo` only on a fully successful
/// cycle — a mid-cycle fetch error leaves the floor and etag untouched, so the
/// next cycle retries the same window (application is all-or-nothing).
async fn probe_once(ctx: &RefreshCtx<'_>, feed: &str, memo: &mut ProbeMemo) -> Result<bool> {
    let snap = AdvisoryState::read(ctx.slot);
    let Some(db) = snap.db.as_ref() else {
        return Ok(false); // armed but unfed — no baseline to backfill from yet
    };
    // Re-baseline on a snapshot swap: the reload already cleared the overlay, so
    // reset the floor to the new snapshot and drop the etag to force a re-walk.
    if memo.aligned_sha.as_deref() != snap.zip_sha256.as_deref() {
        memo.aligned_sha = snap.zip_sha256.clone();
        memo.watermark = db.watermark();
        memo.csv_etag = None;
    }
    let Some(watermark) = memo.watermark else {
        return Ok(false); // the snapshot carried no timestamps — no floor to walk
    };
    let Some(base) = probe_base(feed) else {
        return Ok(false); // not a probe-shaped feed (the caller gates on this too)
    };
    let source = ProbeSource::new(&base)?;
    let (scan, etag) = match walk_csv(&source, memo.csv_etag.as_deref(), Some(watermark)).await? {
        CsvWalk::NotModified => return Ok(true), // fresh, nothing new
        CsvWalk::Scanned { scan, etag } => (scan, etag),
    };

    // Fetch and classify each fresh MAL advisory. All-or-nothing this cycle: a
    // failed fetch bails before the floor advances, so the window is retried.
    let mut added: Vec<(String, MalRule)> = Vec::new();
    let mut withdrawn: HashSet<String> = HashSet::new();
    let mut applied_ids: Vec<String> = Vec::new();
    let mut withdrawn_ids: Vec<String> = Vec::new();
    for (_, csv_id) in &scan.fresh_mal {
        let adv = source.fetch_advisory(csv_id).await?;
        let id = if adv.id.is_empty() {
            csv_id.clone()
        } else {
            adv.id.clone()
        };
        if adv.withdrawn.is_some() {
            withdrawn.insert(id.clone());
            withdrawn_ids.push(id);
        } else {
            let rules = mal_rules(&adv);
            if !rules.is_empty() {
                applied_ids.push(id);
                added.extend(rules);
            }
        }
    }

    if !added.is_empty() || !withdrawn.is_empty() {
        apply_probe_overlay(ctx.slot, added, &withdrawn);
        if !applied_ids.is_empty() {
            info!(advisories = ?applied_ids, "malware advisory applied ahead of the daily baseline");
        }
        if !withdrawn_ids.is_empty() {
            info!(advisories = ?withdrawn_ids, "malware advisory withdrawn; removed from the probe overlay");
        }
    }

    // Advance the floor past every fresh line (non-MAL bumps included) and adopt
    // the new CSV etag for the next conditional poll.
    if let Some(max) = scan.max_modified {
        memo.watermark = Some(max);
    }
    memo.csv_etag = etag;
    Ok(true)
}

/// Read a storage key's bytes, mapping a "not found" error to `None`.
async fn get_optional_bytes(storage: &dyn Storage, key: &str) -> Result<Option<Vec<u8>>> {
    match storage.get_bytes(key).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if storage::is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Read the persisted snapshot bytes for `GET /advisories/feed`, or `None` when
/// no snapshot exists.
pub async fn stored_feed_bytes(storage: &dyn Storage) -> Result<Option<Vec<u8>>> {
    get_optional_bytes(storage, FEED_KEY).await
}

/// Persist the audit report to [`REPORT_KEY`], but only when its content changed
/// (rows + feed identity) from the stored copy — `generated_unix` alone never
/// rewrites it, so an unchanged corpus and feed don't churn the key every sweep.
/// A stored report that won't read is treated as changed and overwritten.
pub async fn write_report_if_changed(storage: &dyn Storage, report: &Report) -> Result<()> {
    if let Ok(existing) = storage.get_bytes(REPORT_KEY).await {
        if let Ok(prev) = serde_json::from_slice::<Report>(&existing) {
            if prev.feed_sha256 == report.feed_sha256 && prev.rows == report.rows {
                return Ok(());
            }
        }
    }
    let bytes = serde_json::to_vec(report).context("serializing advisory report")?;
    storage
        .put_bytes(REPORT_KEY, bytes, Some("application/json"))
        .await
        .context("persisting advisory report")
}

/// Read the stored audit report bytes for `/audit.json`, or `None` when no report
/// has been materialized yet.
pub async fn stored_report_bytes(storage: &dyn Storage) -> Result<Option<Vec<u8>>> {
    get_optional_bytes(storage, REPORT_KEY).await
}

/// Read and parse the stored audit report for the `/audit` HTML page, or `None`
/// when none has been materialized yet.
pub async fn stored_report(storage: &dyn Storage) -> Result<Option<Report>> {
    match stored_report_bytes(storage).await? {
        Some(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).context("parsing advisory report")?,
        )),
        None => Ok(None),
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
            blocking_advisories(&db, "evil-pkg", Some("1.0.0")),
            ["MAL-2024-0001"]
        );
        assert!(blocking_advisories(&db, "evil-pkg", Some("1.0.1")).is_empty());
        // Name normalization applies on the query side too.
        assert!(blocking_advisories(&db, "notevil", Some("1.0.0")).is_empty());
        // An exact-version rule can't be proven against an unknown version.
        assert!(blocking_advisories(&db, "evil-pkg", None).is_empty());
    }

    #[test]
    fn all_versions_blocks_everything_including_unparseable() {
        let zip = osv_zip(&[("MAL-2.json", mal_all("MAL-2024-0002", "badpkg"))]);
        let db = parse_feed(&zip).unwrap();
        assert_eq!(
            blocking_advisories(&db, "badpkg", Some("0.1")),
            ["MAL-2024-0002"]
        );
        assert_eq!(
            blocking_advisories(&db, "badpkg", Some("99.99.99")),
            ["MAL-2024-0002"]
        );
        // An unparseable version must still be blocked (fail-closed sentinel).
        assert_eq!(
            blocking_advisories(&db, "badpkg", Some("not-a-version")),
            ["MAL-2024-0002"]
        );
        // A filename that yields no version at all is likewise blocked by an
        // all-versions rule (the byte-gate's legacy-format path).
        assert_eq!(blocking_advisories(&db, "badpkg", None), ["MAL-2024-0002"]);
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
        assert!(blocking_advisories(&db, "widget", Some("2.19.0")).is_empty());
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
        assert!(blocking_advisories(&db, "widget", Some("1.0")).is_empty());
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
            blocking_advisories(&db, "goodpkg", Some("1.0.0")),
            ["MAL-2024-0005"]
        );
    }

    #[test]
    fn exact_match_is_version_normalized() {
        // The feed says 1.0; a request for 1.0.0 is the same PEP 440 version.
        let zip = osv_zip(&[("M.json", mal_exact("MAL-2024-0006", "pkg", "1.0"))]);
        let db = parse_feed(&zip).unwrap();
        assert_eq!(
            blocking_advisories(&db, "pkg", Some("1.0.0")),
            ["MAL-2024-0006"]
        );
        assert_eq!(
            blocking_advisories(&db, "pkg", Some("1.0")),
            ["MAL-2024-0006"]
        );
    }

    #[test]
    fn mal_advisory_is_both_blocked_and_audited() {
        let zip = osv_zip(&[("M.json", mal_exact("MAL-2024-0007", "pkg", "1.0.0"))]);
        let db = parse_feed(&zip).unwrap();
        assert_eq!(
            blocking_advisories(&db, "pkg", Some("1.0.0")),
            ["MAL-2024-0007"]
        );
        // Malware also shows in the audit index (with a blocked flag downstream).
        assert_eq!(advisories_for(&db, "pkg", "1.0.0")[0].id, "MAL-2024-0007");
        assert_eq!(db.block_names(), 1);
        assert_eq!(db.audit_records(), 1);
    }

    fn inv(package: &str, version: &str, origin: &str, downloads: u64) -> AuditInventory {
        AuditInventory {
            package: package.into(),
            version: version.into(),
            origin: origin.into(),
            downloads_30d: downloads,
        }
    }

    #[test]
    fn report_join_filters_private_ranks_and_flags_blocked() {
        let db = parse_feed(&osv_zip(&[
            ("m.json", mal_exact("MAL-2024-1", "evil", "1.0.0")),
            ("p.json", pysec_fixed("PYSEC-2024-2", "widget", "2.0.0")),
        ]))
        .unwrap();
        let quarantined = HashSet::new();
        let inventory = vec![
            // Vulnerable but not blocked (PYSEC is audit-only) — top of the rank.
            inv("widget", "1.5.0", "mirror", 10),
            // Malware → blocked.
            inv("evil", "1.0.0", "mirror", 5),
            // Private-origin same name as the MAL package → excluded entirely.
            inv("evil", "1.0.0", "private", 999),
            // Fixed version of the vulnerable package → no row.
            inv("widget", "2.0.0", "mirror", 3),
            // No advisory → no row (even at the top of downloads).
            inv("clean", "9.9.9", "mirror", 100),
        ];
        let report = build_report(&inventory, &db, &quarantined, 123, "deadbeef");
        assert_eq!(report.generated_unix, 123);
        assert_eq!(report.feed_sha256, "deadbeef");
        assert_eq!(
            report.rows.len(),
            2,
            "private/fixed/clean rows must be dropped"
        );
        // Sorted by downloads descending: widget (10) then evil (5).
        let widget = &report.rows[0];
        assert_eq!(
            (widget.package.as_str(), widget.version.as_str()),
            ("widget", "1.5.0")
        );
        assert_eq!(widget.advisories, ["PYSEC-2024-2"]);
        assert_eq!(widget.severity, "7.5");
        assert_eq!(widget.fixed_in, ["2.0.0"]);
        assert_eq!(widget.downloads_30d, 10);
        assert!(
            !widget.blocked,
            "a vulnerability is informational, not blocked"
        );
        let evil = &report.rows[1];
        assert_eq!(evil.package, "evil");
        assert_eq!(evil.advisories, ["MAL-2024-1"]);
        assert_eq!(evil.origin, "mirror");
        assert!(evil.blocked, "a MAL match must be flagged blocked");
    }

    #[test]
    fn report_quarantine_flags_blocked_without_mal() {
        let db = parse_feed(&osv_zip(&[(
            "p.json",
            pysec_fixed("PYSEC-2024-2", "widget", "2.0.0"),
        )]))
        .unwrap();
        let quarantined: HashSet<String> = ["widget".to_string()].into_iter().collect();
        let report = build_report(
            &[inv("widget", "1.0.0", "mirror", 1)],
            &db,
            &quarantined,
            0,
            "",
        );
        assert_eq!(report.rows.len(), 1);
        // A vulnerable row whose project is ALSO quarantined is blocked (the byte
        // gate would 403 it), even though PYSEC never blocks on its own.
        assert!(report.rows[0].blocked);
    }

    #[test]
    fn audit_has_name_filters_to_indexed_packages() {
        let db = parse_feed(&osv_zip(&[(
            "p.json",
            pysec_fixed("PYSEC-2024-2", "widget", "2.0.0"),
        )]))
        .unwrap();
        assert!(db.audit_has_name("widget"));
        assert!(!db.audit_has_name("unheard-of"));
    }

    // ---------------------------- probe: pure parts --------------------------

    /// An OSV entry carrying an explicit `modified` (for watermark tests).
    fn dated(id: &str, name: &str, modified: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "modified": modified,
            "affected": [{"package": {"ecosystem": "PyPI", "name": name}, "versions": ["1.0"]}],
        })
    }

    fn all_versions_rule(id: &str) -> MalRule {
        MalRule {
            id: id.to_string(),
            scope: VersionScope::AllVersions,
        }
    }

    fn ids_of(scan: &CsvScan) -> Vec<&str> {
        scan.fresh_mal.iter().map(|(_, id)| id.as_str()).collect()
    }

    #[test]
    fn watermark_is_the_max_modified_across_all_entries() {
        // The newest `modified` wins even when it belongs to a withdrawn entry
        // (skipped from the block set but still counted toward the watermark).
        let withdrawn = serde_json::json!({
            "id": "MAL-2024-3",
            "modified": "2024-06-01T00:00:00Z",
            "withdrawn": "2024-06-01T00:00:00Z",
            "affected": [{"package": {"ecosystem": "PyPI", "name": "c"}, "versions": ["1.0"]}],
        });
        let db = parse_feed(&osv_zip(&[
            ("a.json", dated("MAL-2024-1", "a", "2024-01-01T00:00:00Z")),
            ("b.json", dated("PYSEC-2024-2", "b", "2024-03-15T12:00:00Z")),
            ("w.json", withdrawn),
        ]))
        .unwrap();
        assert_eq!(db.watermark(), parse_modified("2024-06-01T00:00:00Z"));
    }

    #[test]
    fn watermark_is_none_when_no_entry_carries_a_timestamp() {
        let db = parse_feed(&osv_zip(&[("m.json", mal_exact("MAL-2024-1", "a", "1.0"))])).unwrap();
        assert_eq!(db.watermark(), None);
    }

    #[test]
    fn csv_scan_collects_mal_above_watermark_and_stops() {
        let wm = parse_modified("2024-03-01T00:00:00Z");
        // Newest first: a MAL above, a non-MAL above, then a MAL at/below → stop.
        let csv = "2024-05-01T00:00:00Z,MAL-2024-9\n\
                   2024-04-01T00:00:00Z,PYSEC-2024-8\n\
                   2024-02-01T00:00:00Z,MAL-2024-7\n";
        let scan = parse_csv_head(csv, wm, true);
        assert_eq!(ids_of(&scan), ["MAL-2024-9"]);
        // The watermark advances past the newest fresh line (the non-MAL counts).
        assert_eq!(scan.max_modified, parse_modified("2024-05-01T00:00:00Z"));
        assert!(
            scan.complete,
            "a line at/below the watermark terminates the window"
        );
    }

    #[test]
    fn csv_scan_incomplete_head_requests_more_range() {
        let wm = parse_modified("2024-01-01T00:00:00Z");
        // A truncated head (not the full file) whose last line is still above the
        // watermark: the trailing partial line is dropped and the window is not
        // terminated, so the caller must widen the range.
        let csv = "2024-05-01T00:00:00Z,MAL-2024-9\n\
                   2024-04-01T00:00:00Z,MAL-2024-8\n\
                   2024-03-01T00:00:00Z,MAL-2024-7";
        let scan = parse_csv_head(csv, wm, false);
        assert!(
            !scan.complete,
            "an unterminated head must ask for more bytes"
        );
        assert_eq!(ids_of(&scan), ["MAL-2024-9", "MAL-2024-8"]);
    }

    #[test]
    fn csv_scan_full_file_is_complete_without_a_watermark_line() {
        let wm = parse_modified("2020-01-01T00:00:00Z");
        let scan = parse_csv_head("2024-05-01T00:00:00Z,MAL-2024-9\n", wm, true);
        assert!(scan.complete);
        assert_eq!(ids_of(&scan), ["MAL-2024-9"]);
    }

    #[test]
    fn csv_scan_rejects_unsafe_advisory_ids() {
        let wm = parse_modified("2020-01-01T00:00:00Z");
        // A path-traversal id never becomes a fetch URL.
        let csv = "2024-05-01T00:00:00Z,MAL-../../etc/passwd\n\
                   2024-04-01T00:00:00Z,MAL-2024-0001\n";
        let scan = parse_csv_head(csv, wm, true);
        assert_eq!(ids_of(&scan), ["MAL-2024-0001"]);
    }

    #[test]
    fn overlay_upserts_and_withdraws_by_id() {
        let mut overlay: HashMap<String, Vec<MalRule>> = HashMap::new();
        merge_overlay(
            &mut overlay,
            vec![
                ("evil".into(), all_versions_rule("MAL-1")),
                ("bad".into(), all_versions_rule("MAL-2")),
            ],
            &HashSet::new(),
        );
        assert_eq!(block_hits(&overlay, "evil", Some("1.0")), ["MAL-1"]);
        assert_eq!(block_hits(&overlay, "bad", Some("9.9")), ["MAL-2"]);

        // Re-applying MAL-1 replaces (no duplicate); withdrawing MAL-2 removes it.
        merge_overlay(
            &mut overlay,
            vec![("evil".into(), all_versions_rule("MAL-1"))],
            &HashSet::from(["MAL-2".to_string()]),
        );
        assert_eq!(block_hits(&overlay, "evil", Some("1.0")), ["MAL-1"]);
        assert!(block_hits(&overlay, "bad", Some("9.9")).is_empty());
        assert!(!overlay.contains_key("bad"), "an emptied name is pruned");
    }

    #[test]
    fn blocking_unions_baseline_and_overlay() {
        let db = parse_feed(&osv_zip(&[(
            "m.json",
            mal_exact("MAL-2024-B", "widget", "1.0.0"),
        )]))
        .unwrap();
        let mut overlay: HashMap<String, Vec<MalRule>> = HashMap::new();
        merge_overlay(
            &mut overlay,
            vec![("widget".into(), all_versions_rule("MAL-2024-O"))],
            &HashSet::new(),
        );
        let state = AdvisoryState {
            db: Some(Arc::new(db)),
            overlay: Arc::new(overlay),
            ..AdvisoryState::default()
        };
        // Baseline blocks 1.0.0; the all-versions overlay blocks everything.
        let mut ids = state.blocking("widget", Some("1.0.0"));
        ids.sort();
        assert_eq!(ids, ["MAL-2024-B", "MAL-2024-O"]);
        // A version only the overlay covers is still blocked (no baseline match).
        assert_eq!(state.blocking("widget", Some("2.0.0")), ["MAL-2024-O"]);
        assert!(state.has_block_data());
    }

    #[test]
    fn probe_base_only_for_the_osv_all_zip_url() {
        assert_eq!(
            probe_base("https://osv.example/PyPI/all.zip").as_deref(),
            Some("https://osv.example/PyPI")
        );
        assert_eq!(probe_base("/local/path/all.zip"), None);
        assert_eq!(probe_base("https://osv.example/PyPI/other.zip"), None);
    }
}
