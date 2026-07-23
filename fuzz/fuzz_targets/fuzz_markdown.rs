//! Fuzz the locked-down Markdown renderer in `src/markdown.rs`.
//!
//! `render_limited` turns an uploader-controlled README into an HTML fragment
//! that lands verbatim on the human project page. It IS the sanitizer — there is
//! no downstream escaping — so every security property has to hold here: it walks
//! pulldown-cmark's event stream and emits ONLY a fixed tag whitelist, escapes
//! every text node, drops raw HTML, and passes every link/image URL through
//! `safe_href` (http/https only, no `javascript:`/`data:`). The properties,
//! checked over arbitrary README text:
//!
//!   1. Never panic. `safe_href` slices a `&str` by scheme length, so a
//!      multi-byte char straddling byte 7/8 used to panic it (a request-path
//!      panic = persistent DoS of `/project/<pkg>/`); this target exists to keep
//!      that class dead on any input.
//!   2. Every emitted tag name is in the whitelist. Because `encode_text` escapes
//!      `<`/`>`/`&` in text and `encode_double_quoted_attribute` escapes them in
//!      attribute values, EVERY literal `<` in the output begins a tag the
//!      renderer itself emitted — so scanning `<` positions enumerates exactly
//!      the emitted tags (no naive `on\w+=` substring search, which legitimate
//!      escaped text would trip).
//!   3. Every `href`/`src` value would pass `safe_href` (called directly) and
//!      carries no `javascript:`/`data:` scheme.
#![no_main]
#![allow(dead_code)]

use libfuzzer_sys::fuzz_target;

#[path = "../../src/markdown.rs"]
mod markdown;

/// Every tag `render_limited` is allowed to emit (open or close). Anything else
/// in the output is an escape from the whitelist.
const WHITELIST: &[&str] = &[
    "a",
    "blockquote",
    "br",
    "code",
    "del",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "img",
    "li",
    "ol",
    "p",
    "pre",
    "strong",
    "table",
    "td",
    "th",
    "thead",
    "tr",
    "ul",
];

/// The double-quoted attribute value after `needle` in `html`, for each match:
/// from just past the opening quote up to the next `"`. Any `"` inside the value
/// is escaped to `&quot;`, so this always captures the whole value.
fn attr_values<'a>(html: &'a str, needle: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut search = 0;
    while let Some(rel) = html[search..].find(needle) {
        let start = search + rel + needle.len();
        let rest = &html[start..];
        let end = rest
            .find('"')
            .expect("emitted attribute value is not terminated by a closing quote");
        out.push(&rest[..end]);
        search = start + end;
    }
    out
}

fuzz_target!(|data: &[u8]| {
    let Ok(md) = std::str::from_utf8(data) else {
        return;
    };
    let html = markdown::render_limited(md);

    // 1. Every `<`-delimited tag must be in the whitelist. A textual `<` can't
    //    reach the output unescaped, so each one begins an emitted tag.
    let mut i = 0;
    while let Some(rel) = html[i..].find('<') {
        let lt = i + rel;
        let after = &html[lt + 1..];
        let after = after.strip_prefix('/').unwrap_or(after); // close tag
        let name: String = after
            .chars()
            .take_while(char::is_ascii_alphanumeric)
            .collect();
        assert!(
            WHITELIST.contains(&name.as_str()),
            "render_limited emitted non-whitelisted tag <{name}> for input {md:?}"
        );
        i = lt + 1;
    }

    // 2. Every emitted `href`/`src` value must survive `safe_href` unchanged and
    //    carry no dangerous scheme. The value in the attribute is the safe_href
    //    output, attribute-escaped; decoding it must recover a URL safe_href
    //    still approves, or an interpolated field smuggled an unsafe URL through.
    for (needle, attr) in [("<a href=\"", "href"), ("<img src=\"", "src")] {
        for raw in attr_values(&html, needle) {
            let decoded = html_escape::decode_html_entities(raw);
            assert!(
                markdown::safe_href(&decoded).is_some(),
                "emitted {attr} value {decoded:?} is not safe_href-approved (input {md:?})"
            );
            let scheme = decoded.trim_start().to_ascii_lowercase();
            assert!(
                !scheme.starts_with("javascript:") && !scheme.starts_with("data:"),
                "emitted {attr} carries an unsafe scheme: {decoded:?} (input {md:?})"
            );
        }
    }
});
