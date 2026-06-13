//! Fuzz the index renderers in `src/render.rs`.
//!
//! Indexes are built from sidecar fields (filename, sha256, version, yanked
//! reason, requires-python). Several of those are attacker-influenced and land
//! inside double-quoted HTML attributes (`href="/files/.../<filename>#sha256=<h>"`).
//! Two properties, checked over arbitrary field values:
//!
//!   1. The PEP 691 JSON is always valid JSON.
//!   2. The per-package HTML's `href` attribute is quote-safe: decoding the
//!      attribute value recovers exactly the intended URL. If any interpolated
//!      field smuggles a raw `"` into the attribute, the value gets truncated
//!      and this fails — i.e. this catches HTML-attribute injection.
#![no_main]
#![allow(dead_code)]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

// render.rs reaches for `crate::names` and `crate::sidecar`; provide both at the
// binary root so those absolute paths resolve, then pull render in beside them.
#[path = "../../src/names.rs"]
mod names;
#[path = "../../src/sidecar.rs"]
mod sidecar;
#[path = "../../src/render.rs"]
mod render;

use render::FileMetadata;
use sidecar::Yanked;

fn carve(u: &mut Unstructured) -> (String, FileMetadata) {
    let pkg = String::arbitrary(u).unwrap_or_default();
    let yanked = match u8::arbitrary(u).unwrap_or(0) % 3 {
        0 => Yanked::Flag(false),
        1 => Yanked::Flag(true),
        _ => Yanked::Reason(String::arbitrary(u).unwrap_or_default()),
    };
    let fm = FileMetadata {
        filename: String::arbitrary(u).unwrap_or_default(),
        sha256: String::arbitrary(u).unwrap_or_default(),
        size: u64::arbitrary(u).unwrap_or(0),
        upload_time: Option::<String>::arbitrary(u).unwrap_or(None),
        version: Option::<String>::arbitrary(u).unwrap_or(None),
        yanked,
        requires_python: Option::<String>::arbitrary(u).unwrap_or(None),
        core_metadata: bool::arbitrary(u).unwrap_or(false),
    };
    (pkg, fm)
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let (pkg, fm) = carve(&mut u);
    let files = std::slice::from_ref(&fm);

    // 1. PEP 691 JSON must always parse.
    let json = render::pep691_package_json(&pkg, files);
    serde_json::from_str::<serde_json::Value>(&json).expect("PEP 691 JSON is not valid JSON");

    // 2. The HTML `href` must be quote-safe and lossless.
    let html = render::pep503_package_html(&pkg, files);
    if let Some(start) = html.find("href=\"/files/") {
        let after = &html[start + "href=\"".len()..];
        if let Some(end) = after.find('"') {
            let raw_attr = &after[..end];
            let decoded = html_escape::decode_html_entities(raw_attr);
            let intended = format!("/files/{}/{}#sha256={}", pkg, fm.filename, fm.sha256);
            assert_eq!(
                decoded, intended,
                "href attribute truncated/altered — HTML-attribute injection via an interpolated field"
            );
        }
    }
});
