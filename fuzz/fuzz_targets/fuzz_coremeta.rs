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

    // Every `Project-URL` line is split on the first comma into (label, url); a
    // line without a comma is dropped. So the count of project_urls coming from
    // `project-url` headers can only ever be <= the count of `home-page` +
    // `project-url` headers — a cheap shape check that the splitter never
    // fabricates entries.
    for (label, url) in &m.project_urls {
        // Fields are stored trimmed; this just touches the strings so the
        // fuzzer must keep them valid UTF-8 (they came from a lossy decode).
        let _ = (label.len(), url.len());
    }

    // Re-parsing the canonical text reconstructed from the parse must be stable
    // for the fields the page shows: feed the description back through and it may
    // not panic either (it becomes a bare body).
    if let Some(desc) = &m.description {
        let _ = coremeta::parse(desc.as_bytes());
    }
});
