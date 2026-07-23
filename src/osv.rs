//! The pure OSV parse/match core: the PyPI export parsed into two in-memory
//! views — a malware **block set** (`MAL-*` ids only) and an **audit index**
//! (everything) — plus the request-time version matchers over them.
//!
//! A leaf module by design: it depends only on std, `zip`, `serde`/`serde_json`,
//! `pep440_rs`, `time`, `anyhow`, and [`crate::names`] — no async, no I/O, no
//! logging — so it is unit-tested here and fuzzed directly (`fuzz_advisories`).
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

use crate::names::normalize_pkg_name;

/// Per-entry ceiling inside the zip. OSV records are KBs; this only refuses a
/// decompression bomb hiding in one member.
pub(crate) const MAX_ENTRY_BYTES: u64 = 8 * 1024 * 1024;

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
            advance_watermark(&mut db.watermark, adv.modified.as_deref());
            ingest_advisory(&mut db, &adv);
        }
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
pub(crate) fn parse_modified(raw: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(raw, &Rfc3339).ok()
}

/// Fold a candidate `modified` into a running max (the snapshot watermark).
pub(crate) fn advance_watermark(current: &mut Option<OffsetDateTime>, candidate: Option<&str>) {
    if let Some(ts) = candidate.and_then(parse_modified) {
        if current.is_none_or(|c| ts > c) {
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
}
