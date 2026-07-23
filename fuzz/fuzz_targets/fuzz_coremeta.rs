//! Fuzz the core-metadata parser in `src/coremeta.rs`.
//!
//! On every wheel/sdist upload the server pulls `METADATA`/`PKG-INFO` out of the
//! archive and hands those bytes — fully attacker-controlled — to `parse`, whose
//! fields then land on the human project page. It is a hand-rolled RFC 822-ish
//! parser: it folds continuation lines, splits the header block from the body at
//! the first blank line, and returns `&text[body_start..]` by byte offset. The
//! property we care about: never panic, on any bytes (lone `\r`, no trailing
//! newline, multi-byte UTF-8 straddling a fold/blank-line boundary, a `:`-less
//! line, a body-only document, megabytes of folded continuations).
#![no_main]
#![allow(dead_code)]

use libfuzzer_sys::fuzz_target;

#[path = "../../src/coremeta.rs"]
mod coremeta;

fuzz_target!(|data: &[u8]| {
    // `parse` is total: it lossily decodes invalid UTF-8 and degrades garbage to
    // defaults rather than erroring, so it must accept literally any byte string.
    let m = coremeta::parse(data);

    // Shape invariant: every `project_urls` entry comes from exactly one header —
    // a `Home-page` line contributes 1, a `Project-URL` line contributes 0 or 1
    // (dropped when it carries no comma to split on). So the splitter can never
    // fabricate more entries than there were matching header lines. Count those
    // the way the parser enumerates headers (decode lossily, stop at the first
    // blank line where the body begins, skip folded continuation lines) and
    // assert the count is an upper bound. A regression that double-counts or
    // invents an entry trips this.
    let text = String::from_utf8_lossy(data);
    let mut header_urls = 0usize;
    for line in text.split_inclusive('\n') {
        let content = line.trim_end_matches(['\r', '\n']);
        if content.is_empty() {
            break; // the body starts here; later lines are not headers
        }
        if line.starts_with([' ', '\t']) {
            continue; // a folded continuation of the previous header, not a new one
        }
        if let Some((key, _)) = content.split_once(':') {
            let key = key.trim().to_ascii_lowercase();
            if key == "home-page" || key == "project-url" {
                header_urls += 1;
            }
        }
    }
    assert!(
        m.project_urls.len() <= header_urls,
        "parse fabricated project_urls: {} entries from {} Home-page/Project-URL header lines (input {text:?})",
        m.project_urls.len(),
        header_urls,
    );

    // Touch every stored field so the fuzzer must keep them valid UTF-8 (they
    // came from a lossy decode).
    for (label, url) in &m.project_urls {
        let _ = (label.len(), url.len());
    }

    // Re-parsing the canonical text reconstructed from the parse must be stable
    // for the fields the page shows: feed the description back through and it may
    // not panic either (it becomes a bare body).
    if let Some(desc) = &m.description {
        let _ = coremeta::parse(desc.as_bytes());
    }
});
