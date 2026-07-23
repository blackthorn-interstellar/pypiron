//! Fuzz the OSV advisory parse/match core in `src/osv.rs`.
//!
//! `parse_feed` eats a fully upstream-controlled zip (the OSV PyPI export, or an
//! attacker-substituted body), and the version matchers it feeds are then on the
//! request path: the download byte gate calls `blocking_advisories` and the
//! project page calls `advisories_for` on every hit. So the properties that
//! matter — checked here over arbitrary bytes and arbitrary (name, version):
//!
//!   * never panic: `parse_feed` is total over any bytes, and the matchers are
//!     total over any version string (parseable or not);
//!   * fail-closed: an `AllVersions` scope condemns EVERY version, including ones
//!     pep440 can't read — malware must never slip through on a junk version;
//!   * the per-entry byte cap holds: no parsed record's fields exceed
//!     `MAX_ENTRY_BYTES` (a dropped cap would let a decompression bomb through);
//!   * every blocked name is also audited (`block_names <= audit_records`).
#![no_main]
#![allow(dead_code)]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

// Load the real modules from the parent crate. `osv` depends only on `names`
// (plus std/zip/serde/pep440_rs/time/anyhow), so this pair is the whole surface.
#[path = "../../src/names.rs"]
mod names;
#[path = "../../src/osv.rs"]
mod osv;

fuzz_target!(|data: &[u8]| {
    // Two arbitrary strings drive the matcher axis. The WHOLE input doubles as
    // the candidate feed zip below, so a raw-OSV-zip seed drives `parse_feed`
    // directly while these still let the fuzzer explore the (name, version)
    // match space (and mutate toward names the feed actually carries).
    let mut u = Unstructured::new(data);
    let name = String::arbitrary(&mut u).unwrap_or_default();
    let version = String::arbitrary(&mut u).unwrap_or_default();
    let norm = names::normalize_pkg_name(&name);

    // Fail-closed: an all-versions rule (the "this package is malware" case) must
    // match any version string, including one pep440 can't parse. Also proves the
    // matcher is total on `version` even before a feed is in hand.
    assert!(
        osv::VersionScope::AllVersions.matches(&version),
        "AllVersions failed to match version {version:?}"
    );
    // Exact reflexivity: `version_eq` is reflexive, so an exact list containing v
    // matches v — an exact malware rule for v always fires on v.
    assert!(
        osv::VersionScope::Exact(vec![version.clone()]).matches(&version),
        "Exact([v]) failed to match its own v={version:?}"
    );

    // Parse arbitrary bytes as an OSV export. Total: Ok or Err, never a panic.
    let Ok(db) = osv::parse_feed(data) else {
        return;
    };

    // Every name in the malware block set also carries an audit record, so the
    // distinct blocked-name count can't exceed the total audit-record count.
    assert!(
        db.block_names() <= db.audit_records(),
        "block_names {} exceeded audit_records {}",
        db.block_names(),
        db.audit_records(),
    );

    // The matcher lookups the byte gate and audit run — total for any (name,
    // version), whether or not `version` parses and whether or not the name hits.
    let _ = osv::blocking_advisories(&db, &norm, Some(&version));
    let _ = osv::blocking_advisories(&db, &norm, None);
    let records = osv::advisories_for(&db, &norm, &version);

    // A parsed record's fields came from a JSON member the parser capped at
    // MAX_ENTRY_BYTES, so no stored field can exceed it. A regression that
    // removed the cap and parsed an oversized member would trip this.
    for record in records {
        assert!(
            record.id.len() as u64 <= osv::MAX_ENTRY_BYTES,
            "advisory id exceeds MAX_ENTRY_BYTES"
        );
        assert!(
            record.summary.len() as u64 <= osv::MAX_ENTRY_BYTES,
            "advisory summary exceeds MAX_ENTRY_BYTES"
        );
    }
});
