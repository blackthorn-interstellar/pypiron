//! Fuzz the filename / package-name / version parsers in `src/names.rs`.
//!
//! These eat fully attacker-controlled strings: the multipart `filename`/`name`
//! fields on upload and the filenames of mirrored PyPI artifacts. They slice on
//! byte offsets and feed substrings to `pep440_rs`, so the property we care about
//! is: never panic, on any input.
#![no_main]
#![allow(dead_code)]

use libfuzzer_sys::fuzz_target;

// Load the real module from the parent crate (its only third-party dep is
// pep440_rs); a `#[path]` module file may keep its `//!` header.
#[path = "../../src/names.rs"]
mod names;
use names::*;

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };

    // None of these may panic on hostile input (multibyte UTF-8 boundaries,
    // empty stems, dash-only names, versions that aren't versions, ...).
    let norm = normalize_pkg_name(s);
    let _ = is_normalized(s);
    let _ = matches_prefix(s, "acme");
    let _ = matches_prefix("acme-foo", s);
    let _ = infer_package_from_filename(s);
    let _ = infer_version_from_filename(s);

    // PEP 503 normalization must be idempotent — the canonical-URL 301 in
    // `simple_pkg` would otherwise redirect in a loop.
    assert_eq!(normalize_pkg_name(&norm), norm, "normalize_pkg_name not idempotent");
});
