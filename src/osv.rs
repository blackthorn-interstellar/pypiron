//! The pure OSV parse/match core: the PyPI export parsed into two in-memory
//! views — a malware **block set** (`MAL-*` ids only) and an **audit index**
//! (everything) — plus the request-time version matchers over them.
//!
//! A leaf module by design: it depends only on std, `zip`, `serde`/`serde_json`,
//! `pep440_rs`, `time`, `anyhow`, [`crate::names`] and the wall-clock read in
//! [`crate::clock`] — no async, no I/O, no logging — so it is unit-tested here
//! and fuzzed directly (`fuzz_advisories`, which mirrors that module list).
//! The fetch/persist/reload plumbing and the org audit report live in
//! [`crate::advisories`], which re-exports the types callers reference.

use std::collections::HashMap;
use std::io::{Cursor, Read};

use anyhow::{Context, Result};
use pep440_rs::Version;
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use zip::ZipArchive;

use crate::clock::now_utc;
use crate::names::normalize_pkg_name;

/// Per-entry ceiling inside the zip. OSV records are KBs; this only refuses a
/// decompression bomb hiding in one member.
pub(crate) const MAX_ENTRY_BYTES: u64 = 8 * 1024 * 1024;

/// OSV's ecosystem string for PyPI (matched case-insensitively for tolerance).
const PYPI_ECOSYSTEM: &str = "PyPI";

/// How far past our own clock an advisory's `modified` stamp may sit and still
/// move the watermark. The probe walks the CSV newest-first and stops at the
/// watermark, so one nonsense far-future stamp would push it past every real
/// advisory and stall the malware fast path until the next daily baseline. An
/// hour absorbs ordinary skew between us and the feed publisher.
const MAX_WATERMARK_SKEW: time::Duration = time::Duration::hours(1);

/// Ceiling on a stored advisory `summary`, which is display-only (the audit
/// report and project page render it). The advisory-level fields are cloned
/// once per affected clause, so an unbounded summary in a record with many
/// clauses multiplies; a couple hundred bytes is more than any real OSV
/// one-liner and far more than the UI shows.
const MAX_SUMMARY_BYTES: usize = 512;

/// Ceiling on a stored `severity`. Real values are a word (`HIGH`) or a CVSS
/// vector (~44 bytes for v3.1, ~250 for a long v4). Anything larger is junk, so
/// it is dropped rather than truncated — [`AdvisoryRecord::severity`] promises
/// the feed's bytes verbatim, and an empty severity is already the "absent"
/// case.
const MAX_SEVERITY_BYTES: usize = 256;

/// Ceiling on a stored advisory `id`. Real OSV ids are short and shaped
/// (`MAL-2024-0001`, `GHSA-xxxx-xxxx-xxxx`, `PYSEC-2024-42`); a longer one is
/// not an OSV id, so the whole record is skipped. Skipping keeps the id
/// byte-equal to the feed (AC9) where truncation would not.
const MAX_ID_BYTES: usize = 128;

/// Ceiling on the affected clauses one advisory contributes. Real records name
/// a handful of packages; the biggest malware campaigns, hundreds. The cap
/// bounds the per-clause clone amplification of the advisory-level fields.
///
/// Dropping the tail is fail-open for a hand-crafted record that buries a
/// `MAL-*` clause past the cap — but that takes control of the feed, and feed
/// control already means simply not publishing the advisory. The bounded
/// failure beats the unbounded one: an OOM takes the node down and stops
/// enforcing anything at all.
const MAX_CLAUSES_PER_ADVISORY: usize = 1_000;

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
    /// falling back to raw string equality when the feed's side won't parse.
    Exact(Vec<String>),
    /// Half-open `[introduced, fixed)` ranges; `fixed = None` is open-ended.
    Ranges(Vec<(Version, Option<Version>)>),
}

impl VersionScope {
    /// Does `version` (a raw requested version string) fall in this scope?
    ///
    /// Fail-closed on a version pep440 can't read: nothing about such a string
    /// proves it sits *outside* a scope, so it falls inside every one — the
    /// same treatment [`block_hits`] gives a filename that yielded no version
    /// at all. A wheel named `pkg-!!!-py3-none-any.whl` must not walk past an
    /// exact or ranged `MAL-*` rule the way a numeric comparison would let it.
    pub fn matches(&self, version: &str) -> bool {
        match self {
            VersionScope::AllVersions => true,
            VersionScope::Exact(versions) => match version.parse::<Version>() {
                Ok(_) => versions.iter().any(|v| version_eq(v, version)),
                Err(_) => true, // fail closed
            },
            VersionScope::Ranges(ranges) => match version.parse::<Version>() {
                Ok(v) => ranges
                    .iter()
                    .any(|(intro, fixed)| v >= *intro && fixed.as_ref().is_none_or(|f| v < *f)),
                Err(_) => true, // fail closed
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
/// format): fail closed by treating every blocking rule for the package as a
/// match. An unreadable filename must not bypass an exact or ranged MAL rule.
pub fn blocking_advisories<'a>(
    db: &'a AdvisoryDb,
    name_norm: &str,
    version: Option<&str>,
) -> Vec<&'a str> {
    block_hits(&db.block, name_norm, version)
}

/// The `MAL-*` ids in a `name → rules` block map that condemn `name_norm` at
/// `version` — the shared core of both the baseline block set and the probe
/// overlay. `version = None` (unreadable filename) matches every rule for the
/// package so enforcement fails closed.
pub(crate) fn block_hits<'a>(
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
                    None => true,
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

// ---- OSV wire structs (tolerant: every field defaulted, unknowns ignored) ---

#[derive(Deserialize)]
pub(crate) struct OsvAdvisory {
    #[serde(default)]
    pub(crate) id: String,
    #[serde(default)]
    summary: String,
    /// RFC 3339 last-modified stamp — the probe's watermark axis. OSV bumps it on
    /// every content change (including a withdrawal), so it is monotone per id.
    #[serde(default)]
    modified: Option<String>,
    /// Present (non-null) iff the advisory was withdrawn — skip those.
    #[serde(default)]
    pub(crate) withdrawn: Option<String>,
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
/// non-PyPI-ecosystem entry is skipped, never fatal — one truncated record must
/// not blind the whole fleet.
pub fn parse_feed(zip_bytes: &[u8]) -> Result<AdvisoryDb> {
    let mut zip = ZipArchive::new(Cursor::new(zip_bytes)).context("advisory feed is not a zip")?;
    let names: Vec<String> = zip
        .file_names()
        .filter(|n| n.ends_with(".json"))
        .map(str::to_string)
        .collect();

    let mut db = AdvisoryDb::default();
    let horizon = watermark_horizon();
    for name in names {
        let Ok(entry) = zip.by_name(&name) else {
            continue;
        };
        let mut buf = Vec::new();
        if entry
            .take(MAX_ENTRY_BYTES + 1)
            .read_to_end(&mut buf)
            .is_err()
            || buf.len() as u64 > MAX_ENTRY_BYTES
        {
            continue;
        }
        // The watermark spans every entry (withdrawn/non-PyPI included): a
        // withdrawn advisory reappears at the top of the probe's CSV with a
        // bumped `modified`, and the baseline must already cover it so the
        // probe doesn't re-walk it.
        if let Ok(adv) = serde_json::from_slice::<OsvAdvisory>(&buf) {
            advance_watermark_before(&mut db.watermark, adv.modified.as_deref(), horizon);
            ingest_advisory(&mut db, &adv);
        }
    }
    Ok(db)
}

/// Iterate an advisory's PyPI-ecosystem affected clauses paired with their
/// normalized package name, skipping non-PyPI ecosystems and unservable names,
/// and stopping at [`MAX_CLAUSES_PER_ADVISORY`] (see the const for why the tail
/// is safe to drop).
fn pypi_clauses(adv: &OsvAdvisory) -> impl Iterator<Item = (String, &OsvAffected)> {
    adv.affected
        .iter()
        .filter_map(|affected| {
            if !affected
                .package
                .ecosystem
                .eq_ignore_ascii_case(PYPI_ECOSYSTEM)
            {
                return None;
            }
            checked_osv_name(&affected.package.name).map(|name| (name, affected))
        })
        .take(MAX_CLAUSES_PER_ADVISORY)
}

/// Fold one advisory into the db. Returns false (→ skip count) when the entry is
/// withdrawn or names no PyPI package. Never rewrites the id (AC9: byte-equal).
fn ingest_advisory(db: &mut AdvisoryDb, adv: &OsvAdvisory) -> bool {
    if adv.withdrawn.is_some() || !usable_id(&adv.id) {
        return false;
    }
    let is_malware = adv.id.starts_with("MAL-");
    let severity = adv
        .database_specific
        .severity
        .as_deref()
        .filter(|s| usable_severity(s))
        .or_else(|| {
            adv.severity
                .iter()
                .map(|s| s.score.as_str())
                .find(|s| usable_severity(s))
        })
        .map(str::to_string)
        .unwrap_or_default();
    let summary = truncated(&adv.summary, MAX_SUMMARY_BYTES);

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
            summary: summary.clone(),
            severity: severity.clone(),
            fixed_in: fixed_versions(affected),
            matcher: scope,
        });
    }
    matched_pypi
}

/// Whether an id is a plausible OSV id we can store: non-empty and no longer
/// than [`MAX_ID_BYTES`]. The id is cloned once per affected clause, so an
/// oversized one is the cheapest half of a memory-amplification record.
fn usable_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_BYTES
}

/// Whether a severity string is worth storing verbatim: non-empty and no
/// longer than [`MAX_SEVERITY_BYTES`].
fn usable_severity(severity: &str) -> bool {
    !severity.is_empty() && severity.len() <= MAX_SEVERITY_BYTES
}

/// `s` clipped to at most `max` bytes, backing up to the nearest char boundary
/// so the result is still valid UTF-8. Display fields only.
fn truncated(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
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
pub(crate) fn parse_modified(raw: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(raw, &Rfc3339).ok()
}

/// Fold a candidate `modified` into a running max (the snapshot watermark),
/// against a horizon of now + [`MAX_WATERMARK_SKEW`].
pub(crate) fn advance_watermark(current: &mut Option<OffsetDateTime>, candidate: Option<&str>) {
    advance_watermark_before(current, candidate, watermark_horizon());
}

/// The newest `modified` a snapshot will believe: now plus the allowed skew.
/// Read once per feed rather than once per entry.
fn watermark_horizon() -> OffsetDateTime {
    now_utc().saturating_add(MAX_WATERMARK_SKEW)
}

/// [`advance_watermark`] against an explicit `horizon`: a candidate past it is
/// ignored outright, so one advisory stamped year 3000 can't carry the
/// watermark past every real advisory and blind the probe's CSV walk.
pub(crate) fn advance_watermark_before(
    current: &mut Option<OffsetDateTime>,
    candidate: Option<&str>,
    horizon: OffsetDateTime,
) {
    if let Some(ts) = candidate.and_then(parse_modified) {
        if ts <= horizon && current.is_none_or(|c| ts > c) {
            *current = Some(ts);
        }
    }
}

/// The `MAL-*` block rules one parsed advisory contributes: `(normalized_name,
/// rule)` per affected PyPI clause. Empty when the advisory is not `MAL-*`,
/// withdrawn, or names no PyPI package. The same clause logic [`ingest_advisory`]
/// uses, projected to the block set alone — what the probe overlays ahead of the
/// daily baseline.
pub(crate) fn mal_rules(adv: &OsvAdvisory) -> Vec<(String, MalRule)> {
    if adv.withdrawn.is_some() || !usable_id(&adv.id) || !adv.id.starts_with("MAL-") {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};
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

    /// An OSV entry carrying an explicit `modified` (for watermark tests).
    fn dated(id: &str, name: &str, modified: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "modified": modified,
            "affected": [{"package": {"ecosystem": "PyPI", "name": name}, "versions": ["1.0"]}],
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
        // An unknown filename version cannot prove this is a different, safe
        // release, so the byte gate fails closed.
        assert_eq!(
            blocking_advisories(&db, "evil-pkg", None),
            ["MAL-2024-0001"]
        );
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
    fn far_future_modified_never_moves_the_watermark() {
        // One nonsense year-3000 stamp would otherwise carry the watermark past
        // every real advisory, and the probe's newest-first CSV walk stops at
        // the watermark — so the malware fast path would skip real records.
        let horizon = parse_modified("2024-06-01T01:00:00Z").unwrap();
        let mut wm = None;
        advance_watermark_before(&mut wm, Some("3000-01-01T00:00:00Z"), horizon);
        assert_eq!(wm, None);

        // A real advisory still lands, and the bogus one can't displace it.
        advance_watermark_before(&mut wm, Some("2024-05-30T00:00:00Z"), horizon);
        advance_watermark_before(&mut wm, Some("3000-01-01T00:00:00Z"), horizon);
        assert_eq!(wm, parse_modified("2024-05-30T00:00:00Z"));

        // The skew window itself is inclusive: a stamp at the horizon is fine.
        advance_watermark_before(&mut wm, Some("2024-06-01T01:00:00Z"), horizon);
        assert_eq!(wm, Some(horizon));
    }

    #[test]
    fn feed_watermark_ignores_a_far_future_entry() {
        // Same property through `parse_feed`, whose horizon is the wall clock:
        // the real 2024 stamp wins over an entry dated in the year 3000.
        let db = parse_feed(&osv_zip(&[
            ("a.json", dated("MAL-2024-1", "a", "2024-01-01T00:00:00Z")),
            ("b.json", dated("MAL-2024-2", "b", "3000-01-01T00:00:00Z")),
        ]))
        .unwrap();
        assert_eq!(db.watermark(), parse_modified("2024-01-01T00:00:00Z"));
    }

    #[test]
    fn unparseable_version_matches_ranged_and_exact_rules() {
        // `pkg-!!!-py3-none-any.whl` yields Some("!!!"), which pep440 can't
        // read. It must fail closed against BOTH rule shapes, exactly as a
        // filename yielding no version at all does.
        let zip = osv_zip(&[
            ("M1.json", mal_exact("MAL-2024-0100", "exactpkg", "1.0.0")),
            (
                "M2.json",
                serde_json::json!({
                    "id": "MAL-2024-0101",
                    "affected": [{
                        "package": {"ecosystem": "PyPI", "name": "rangepkg"},
                        "ranges": [{"type": "ECOSYSTEM", "events": [
                            {"introduced": "1.0"}, {"fixed": "2.0"},
                        ]}],
                    }],
                }),
            ),
        ]);
        let db = parse_feed(&zip).unwrap();
        assert_eq!(
            blocking_advisories(&db, "exactpkg", Some("!!!")),
            ["MAL-2024-0100"]
        );
        assert_eq!(
            blocking_advisories(&db, "rangepkg", Some("!!!")),
            ["MAL-2024-0101"]
        );
        // A readable version outside the rule is still not blocked — the
        // fail-closed arm must not swallow the ordinary negative case.
        assert!(blocking_advisories(&db, "rangepkg", Some("2.5")).is_empty());
        assert!(blocking_advisories(&db, "exactpkg", Some("1.0.1")).is_empty());
    }

    #[test]
    fn pep427_escaped_filename_versions_still_read_numerically() {
        // The byte gate matches on the version pulled out of the *filename*,
        // which PEP 427 escapes (`-` and `_` separators both become `_`). Those
        // forms must keep parsing, or fail-closed would blanket-block ordinary
        // releases of any package that carries an advisory. pep440 accepts `_`
        // wherever a separator is legal, so every escaped form survives.
        for v in [
            "1.0_alpha1",
            "1.0_a1",
            "1.0_post1",
            "1.0_dev1",
            "1.0_rc1",
            "1.0+cuda_11",
        ] {
            assert!(v.parse::<Version>().is_ok(), "{v} stopped parsing");
        }
        // The one exception: PEP 440's *implicit* post release needs a literal
        // dash (`1.0-1`), so its escaped filename spelling is unreadable and
        // does fail closed. Narrow and deliberate — an unreadable version is
        // exactly what must not walk past a MAL rule.
        assert!("1.0-1".parse::<Version>().is_ok());
        assert!("1.0_1".parse::<Version>().is_err());
    }

    #[test]
    fn oversized_advisory_fields_and_clause_lists_are_capped() {
        let long_summary = "s".repeat(MAX_SUMMARY_BYTES * 4);
        let long_severity = "v".repeat(MAX_SEVERITY_BYTES + 1);
        let clauses: Vec<_> = (0..MAX_CLAUSES_PER_ADVISORY + 50)
            .map(|i| {
                serde_json::json!({
                    "package": {"ecosystem": "PyPI", "name": format!("pkg{i}")},
                    "versions": ["1.0"],
                })
            })
            .collect();
        let db = parse_feed(&osv_zip(&[(
            "big.json",
            serde_json::json!({
                "id": "MAL-2024-0200",
                "summary": long_summary,
                "database_specific": {"severity": long_severity},
                "affected": clauses,
            }),
        )]))
        .unwrap();

        assert_eq!(db.audit_records(), MAX_CLAUSES_PER_ADVISORY);
        assert_eq!(db.block_names(), MAX_CLAUSES_PER_ADVISORY);
        let rec = advisories_for(&db, "pkg0", "1.0");
        assert_eq!(rec[0].summary.len(), MAX_SUMMARY_BYTES);
        // An unusable severity is dropped, not truncated: the field promises
        // the feed's bytes verbatim, and empty is already the "absent" case.
        assert_eq!(rec[0].severity, "");
        // The clause past the cap contributed nothing at all.
        let over = format!("pkg{}", MAX_CLAUSES_PER_ADVISORY);
        assert!(blocking_advisories(&db, &over, Some("1.0")).is_empty());
    }

    #[test]
    fn summary_truncation_lands_on_a_char_boundary() {
        // A multibyte char straddling the cap must back up, not split.
        let s = "é".repeat(MAX_SUMMARY_BYTES); // 2 bytes each
        let out = truncated(&s, MAX_SUMMARY_BYTES);
        assert_eq!(out.len(), MAX_SUMMARY_BYTES);
        assert_eq!(
            truncated(&s, MAX_SUMMARY_BYTES - 1).len(),
            MAX_SUMMARY_BYTES - 2
        );
        assert_eq!(truncated("short", MAX_SUMMARY_BYTES), "short");
    }

    #[test]
    fn oversized_id_skips_the_record_rather_than_rewriting_it() {
        // Truncating would break the byte-equal id contract (AC9), so an id no
        // OSV record could have drops the whole entry.
        let long_id = format!("MAL-{}", "9".repeat(MAX_ID_BYTES));
        let db = parse_feed(&osv_zip(&[(
            "x.json",
            mal_exact(&long_id, "victim", "1.0"),
        )]))
        .unwrap();
        assert!(blocking_advisories(&db, "victim", Some("1.0")).is_empty());
        assert_eq!(db.audit_records(), 0);
    }
}
