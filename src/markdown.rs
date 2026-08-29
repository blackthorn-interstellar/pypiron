//! Render a *limited, locked-down* HTML subset from a Markdown README for the
//! human project page. Security model: we walk pulldown-cmark's event stream
//! and emit ONLY a fixed whitelist of tags, each with a fixed set of attributes.
//! Every text node is HTML-escaped, raw/inline HTML is dropped, and links and
//! images are restricted to `http`/`https`. The renderer therefore *cannot*
//! emit a `<script>`, an event handler, a `style`, or a `javascript:`/`data:`
//! URL no matter how hostile the input — so no separate HTML sanitizer is
//! needed. Pure and infallible: display must never fail the page.

use html_escape::{encode_double_quoted_attribute, encode_text};
use pulldown_cmark::{Event, HeadingLevel, Options, Parser, Tag, TagEnd};

/// Ceiling on the rendered HTML fragment. A README is upload-controlled and
/// bounded only by the 16 MiB metadata cap, and markup amplifies: a file of
/// nothing but `>` blockquotes or `*` emphasis renders several times its own
/// size. The result lands in the project-page cache, whose overflow policy
/// clears the *whole* map — so one hostile description must not be able to
/// evict every other cached page, nor to have N concurrent cold requests each
/// hold a multi-megabyte render. 2 MiB is far past any real README.
const MAX_RENDERED_BYTES: usize = 2 * 1024 * 1024;

/// Shown in place of the rest of the document when the render hits
/// [`MAX_RENDERED_BYTES`] — silent truncation would read as a broken README.
const TRUNCATION_NOTICE: &str = "<p><em>… description truncated …</em></p>";

/// Render Markdown to a constrained, safe HTML fragment of at most
/// [`MAX_RENDERED_BYTES`] plus the truncation notice (and the few bytes of one
/// fixed-size literal that may straddle the cut).
pub fn render_limited(md: &str) -> String {
    let opts = Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let mut out = String::with_capacity((md.len() + md.len() / 2).min(MAX_RENDERED_BYTES));
    // Close tags for currently-open whitelisted elements. A dropped element
    // pushes an empty string, so Start/End stay balanced and its text children
    // still flow through unwrapped.
    let mut closes: Vec<&'static str> = Vec::new();
    // Bytes those pending close tags will cost. Charged against the ceiling as
    // they are opened, so there is always room to balance the fragment at a cut.
    let mut closes_len = 0usize;
    // Set when the ceiling stopped the render, not the end of the document.
    let mut truncated = false;
    // An image's alt text arrives as events *between* Start(Image)/End(Image);
    // we buffer it into the `alt` attribute rather than the body. `.0` is the
    // safe src (None if the URL was rejected → the whole image is dropped).
    let mut image: Option<(Option<String>, String)> = None;
    // Depth of images nested inside the current image's alt span — CommonMark
    // allows `![outer ![inner](b)](a)`. We only finalize the outer image at its
    // own End, so a nested End never falls through to the close stack.
    let mut image_depth: u32 = 0;
    // Header cells (`<th>`) vs body cells (`<td>`): pulldown emits both as
    // TableCell; the difference is whether we're inside the TableHead.
    let mut in_head = false;

    for ev in Parser::new_ext(md, opts) {
        // Room left for this event, after reserving the pending close tags. At
        // zero the render stops: every large append below is bounded by `room`,
        // so the only overshoot possible is one fixed-size literal.
        let room = MAX_RENDERED_BYTES.saturating_sub(out.len() + closes_len);
        if room == 0 {
            truncated = true;
            break;
        }
        // While collecting an image's alt span, take only text; ignore markup.
        if image.is_some() {
            match ev {
                Event::Start(Tag::Image { .. }) => image_depth += 1,
                Event::End(TagEnd::Image) if image_depth > 0 => image_depth -= 1,
                Event::End(TagEnd::Image) => {
                    // `take()` clears the state regardless; emit only when the
                    // src survived the safe-URL check (else the image is dropped).
                    if let Some((Some(src), alt)) = image.take() {
                        let tag = format!(
                            "<img src=\"{}\" alt=\"{}\" loading=\"lazy\" referrerpolicy=\"no-referrer\">",
                            encode_double_quoted_attribute(&src),
                            encode_double_quoted_attribute(&alt),
                        );
                        // All-or-nothing: a tag cut in half would leave an
                        // unterminated attribute. An image too big for the room
                        // left is simply dropped, like an unsafe-scheme one.
                        if tag.len() <= room {
                            out.push_str(&tag);
                        }
                    }
                }
                Event::Text(t) | Event::Code(t) => {
                    if let Some((_, alt)) = image.as_mut() {
                        // The alt buffer never reaches `out`, so the loop's
                        // ceiling check can't see it growing — bound it here.
                        let alt_room = MAX_RENDERED_BYTES.saturating_sub(alt.len());
                        alt.push_str(trim_to(&t, alt_room));
                    }
                }
                _ => {}
            }
            continue;
        }

        match ev {
            Event::Start(Tag::Image { dest_url, .. }) => {
                image = Some((
                    safe_href(&dest_url)
                        .filter(fits_in_render)
                        .map(str::to_string),
                    String::new(),
                ));
            }
            Event::Start(Tag::TableHead) => {
                in_head = true;
                out.push_str("<thead><tr>");
                closes.push("</tr></thead>");
                closes_len += "</tr></thead>".len();
            }
            Event::Start(Tag::TableCell) => {
                out.push_str(if in_head { "<th>" } else { "<td>" });
                let close = if in_head { "</th>" } else { "</td>" };
                closes.push(close);
                closes_len += close.len();
            }
            Event::Start(tag) => {
                let (open, close) = open_close(&tag);
                // A tag whose open string no longer fits is dropped whole (a cut
                // one would leave an unterminated attribute); pushing the empty
                // close keeps the stack balanced, as for a non-whitelisted tag.
                if open.len() + close.len() <= room {
                    out.push_str(&open);
                    closes.push(close);
                    closes_len += close.len();
                } else {
                    closes.push("");
                }
            }
            Event::End(TagEnd::TableHead) => {
                in_head = false;
                if let Some(c) = closes.pop() {
                    closes_len -= c.len();
                    out.push_str(c);
                }
            }
            Event::End(_) => {
                if let Some(c) = closes.pop() {
                    closes_len -= c.len();
                    out.push_str(c);
                }
            }
            Event::Text(t) => {
                if !push_escaped(&mut out, &t, room) {
                    truncated = true;
                    break;
                }
            }
            Event::Code(t) => {
                out.push_str("<code>");
                let whole = push_escaped(&mut out, &t, room.saturating_sub("<code></code>".len()));
                out.push_str("</code>");
                if !whole {
                    truncated = true;
                    break;
                }
            }
            Event::SoftBreak => out.push('\n'),
            Event::HardBreak => out.push_str("<br>"),
            Event::Rule => out.push_str("<hr>"),
            Event::TaskListMarker(done) => out.push_str(if done { "[x] " } else { "[ ] " }),
            // Raw/inline HTML, math, footnote refs: dropped entirely.
            _ => {}
        }
    }
    if truncated {
        // Close what is still open, innermost first, so the cut fragment is
        // still balanced HTML, then say why it stops. The room reserved above
        // means this drain was already paid for.
        while let Some(c) = closes.pop() {
            out.push_str(c);
        }
        out.push_str(TRUNCATION_NOTICE);
    }
    out
}

/// HTML-escape `text` into `out`, appending at most `room` bytes. The source is
/// trimmed *before* escaping so a single huge text node can't transiently
/// allocate several times the ceiling, and the escaped result is trimmed again
/// because escaping expands. A cut always lands inside already-escaped text,
/// which holds no `<`, so it cannot re-open markup. Returns false when the text
/// did not fit whole — the caller stops there rather than rendering a document
/// with a silent hole in the middle of it.
fn push_escaped(out: &mut String, text: &str, room: usize) -> bool {
    let source = trim_to(text, room);
    let escaped = encode_text(source);
    let written = trim_to(&escaped, room);
    out.push_str(written);
    source.len() == text.len() && written.len() == escaped.len()
}

/// Whether a URL is short enough to be worth escaping into a tag at all.
fn fits_in_render(url: &&str) -> bool {
    url.len() <= MAX_RENDERED_BYTES
}

/// The longest prefix of `s` that is at most `max` bytes and ends on a char
/// boundary — slicing mid-character would panic, and this runs on a request
/// path over arbitrary UTF-8 from package metadata.
fn trim_to(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Open/close strings for a whitelisted tag. A non-whitelisted tag returns
/// empty strings so its text children still render, just unwrapped.
fn open_close(tag: &Tag) -> (String, &'static str) {
    match tag {
        Tag::Paragraph => ("<p>".into(), "</p>"),
        Tag::Heading { level, .. } => {
            let (open, close) = heading_tags(*level);
            (format!("<{open}>"), close)
        }
        Tag::BlockQuote(_) => ("<blockquote>".into(), "</blockquote>"),
        Tag::CodeBlock(_) => ("<pre><code>".into(), "</code></pre>"),
        Tag::List(Some(_)) => ("<ol>".into(), "</ol>"),
        Tag::List(None) => ("<ul>".into(), "</ul>"),
        Tag::Item => ("<li>".into(), "</li>"),
        Tag::Emphasis => ("<em>".into(), "</em>"),
        Tag::Strong => ("<strong>".into(), "</strong>"),
        Tag::Strikethrough => ("<del>".into(), "</del>"),
        Tag::Table(_) => ("<table>".into(), "</table>"),
        Tag::TableRow => ("<tr>".into(), "</tr>"),
        // A URL past the whole render ceiling is not a URL; treat it like an
        // unsafe scheme and drop the anchor, so escaping it never allocates a
        // multiple of the ceiling just to be thrown away.
        Tag::Link { dest_url, .. } => match safe_href(dest_url).filter(fits_in_render) {
            Some(href) => (
                format!(
                    "<a href=\"{}\" rel=\"nofollow noopener noreferrer\">",
                    encode_double_quoted_attribute(href)
                ),
                "</a>",
            ),
            None => (String::new(), ""),
        },
        // HtmlBlock, definition lists, super/subscript, math, metadata,
        // footnotes — and Image/TableHead/TableCell handled above — are dropped.
        _ => (String::new(), ""),
    }
}

/// The open-tag name and matching close tag for a heading level.
fn heading_tags(l: HeadingLevel) -> (&'static str, &'static str) {
    match l {
        HeadingLevel::H1 => ("h1", "</h1>"),
        HeadingLevel::H2 => ("h2", "</h2>"),
        HeadingLevel::H3 => ("h3", "</h3>"),
        HeadingLevel::H4 => ("h4", "</h4>"),
        HeadingLevel::H5 => ("h5", "</h5>"),
        HeadingLevel::H6 => ("h6", "</h6>"),
    }
}

/// Allow only `http`/`https` URLs into an `href` — author-controlled metadata
/// must never smuggle in `javascript:` or `data:` schemes. Applied here to
/// README links/images, and shared with [`crate::html`], which applies the same
/// policy to package project links.
pub(crate) fn safe_href(url: &str) -> Option<&str> {
    // Compare on bytes: a metadata URL is arbitrary UTF-8, and slicing a `&str`
    // at a fixed index panics when it splits a multi-byte char (a request-path
    // panic, since the value rides in from package METADATA). `[u8]` slices are
    // bounded only by length, so a length guard makes them panic-free.
    let b = url.trim_start().as_bytes();
    let scheme_ok = (b.len() >= 7 && b[..7].eq_ignore_ascii_case(b"http://"))
        || (b.len() >= 8 && b[..8].eq_ignore_ascii_case(b"https://"));
    scheme_ok.then_some(url.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_formatting_renders() {
        let h = render_limited("# Title\n\nA **bold** and *em* and `code`.");
        assert!(h.contains("<h1>Title</h1>"));
        assert!(h.contains("<strong>bold</strong>"));
        assert!(h.contains("<em>em</em>"));
        assert!(h.contains("<code>code</code>"));
    }

    #[test]
    fn lists_blockquote_codeblock_render() {
        let h = render_limited("- a\n- b\n\n> quote\n\n```\nfn x(){}\n```\n");
        assert!(h.contains("<ul><li>a</li><li>b</li></ul>"));
        assert!(h.contains("<blockquote>"));
        assert!(h.contains("<pre><code>fn x(){}\n</code></pre>"));
    }

    #[test]
    fn gfm_table_renders_with_th_and_td() {
        let h = render_limited("| A | B |\n|---|---|\n| 1 | 2 |\n");
        assert!(h.contains("<table>"));
        assert!(h.contains("<thead><tr><th>A</th><th>B</th></tr></thead>"));
        assert!(h.contains("<tr><td>1</td><td>2</td></tr>"));
    }

    #[test]
    fn raw_and_inline_html_is_dropped() {
        // A standalone HTML block is dropped wholesale — content and all.
        let block = render_limited("before\n\n<script>alert(1)</script>\n\nafter");
        assert!(!block.contains("<script>"));
        assert!(!block.contains("alert(1)"));
        assert!(block.contains("before") && block.contains("after"));
        // Inline raw HTML tags are dropped, but their inner *text* still flows.
        let inline = render_limited("text <b onclick=\"x()\">y</b> z");
        assert!(!inline.contains("<b"));
        assert!(!inline.contains("onclick"));
        assert!(inline.contains("y"));
    }

    #[test]
    fn unsafe_link_schemes_are_dropped_but_text_survives() {
        let h = render_limited("[click](javascript:alert(1)) and [data](data:text/html,x)");
        assert!(!h.contains("javascript:"));
        assert!(!h.contains("data:text/html"));
        assert!(!h.contains("<a ")); // neither link emitted an anchor
        assert!(h.contains("click"));
        assert!(h.contains("data"));
    }

    #[test]
    fn safe_link_renders_with_nofollow() {
        let h = render_limited("[home](https://example.com/p)");
        assert!(h.contains(
            "<a href=\"https://example.com/p\" rel=\"nofollow noopener noreferrer\">home</a>"
        ));
    }

    #[test]
    fn link_url_cannot_break_out_of_the_href_attribute() {
        let h = render_limited("[x](https://e.com/\"><script>alert(1)</script>)");
        // The quote is attribute-escaped, so the href never terminates early and
        // no script tag is injected.
        assert!(!h.contains("<script>"));
        assert!(!h.contains("\"><"));
        assert!(h.contains("&quot;"));
    }

    #[test]
    fn https_image_renders_data_and_javascript_dropped() {
        let ok = render_limited("![alt text](https://example.com/i.png)");
        assert!(ok.contains(
            "<img src=\"https://example.com/i.png\" alt=\"alt text\" loading=\"lazy\" referrerpolicy=\"no-referrer\">"
        ));
        let bad = render_limited("![x](javascript:alert(1)) ![y](data:image/png;base64,AAAA)");
        assert!(!bad.contains("<img"));
        assert!(!bad.contains("javascript:"));
    }

    #[test]
    fn image_alt_text_cannot_inject_markup() {
        let h = render_limited("![\"><script>x](https://e.com/i.png)");
        assert!(h.contains("<img src=\"https://e.com/i.png\""));
        // The alt is fully attribute-escaped (both the quote and angle bracket),
        // so nothing can terminate the attribute or inject a tag.
        assert!(h.contains("alt=\"&quot;&gt;x\""));
        assert!(!h.contains("<script>"));
    }

    #[test]
    fn nested_images_stay_well_formed() {
        // CommonMark allows an image inside another image's alt; the close stack
        // must not be corrupted (the inner End must not pop an outer tag).
        let h = render_limited(
            "para ![a ![b](http://x/b.png)](http://x/a.png) and ![c](http://x/c.png) end",
        );
        // The outer paragraph stays balanced and wraps everything.
        assert!(h.starts_with("<p>para "));
        assert!(h.ends_with(" end</p>"));
        // The standalone image still renders; nothing leaks outside the <p>.
        assert!(h.contains("<img src=\"http://x/c.png\""));
        assert_eq!(h.matches("</p>").count(), 1);
    }

    #[test]
    fn a_huge_document_is_truncated_with_a_visible_marker() {
        // Markup amplification: each `> ` line costs 2 source bytes and renders
        // a whole blockquote + paragraph. Uncapped this grows without bound.
        let md = "> x\n\n".repeat(400_000);
        let h = render_limited(&md);
        assert!(h.len() <= MAX_RENDERED_BYTES + TRUNCATION_NOTICE.len() + 16);
        assert!(h.ends_with(TRUNCATION_NOTICE), "no truncation marker");
        // Balanced: every element opened before the cut is closed at it.
        assert_eq!(
            h.matches("<blockquote>").count(),
            h.matches("</blockquote>").count()
        );
        assert_eq!(h.matches("<p>").count(), h.matches("</p>").count());
    }

    #[test]
    fn one_huge_text_node_is_capped_too() {
        // A single event, so the per-event bound does the work, not the loop's.
        // `<` escapes to 4 bytes, so the naive path would emit 4x the source.
        let h = render_limited(&"<".repeat(4 * 1024 * 1024));
        assert!(h.len() <= MAX_RENDERED_BYTES + TRUNCATION_NOTICE.len() + 16);
        assert!(h.ends_with(TRUNCATION_NOTICE));
        // The source `<` is escaped, so the cut can only land inside an entity.
        assert!(h.starts_with("<p>&lt;"));
    }

    #[test]
    fn a_document_under_the_cap_is_untouched() {
        let h = render_limited("# Title\n\nbody\n");
        assert!(!h.contains("truncated"));
        assert_eq!(h, "<h1>Title</h1><p>body</p>");
    }

    #[test]
    fn multibyte_text_is_cut_on_a_char_boundary() {
        // A cut landing mid-character would panic the slice; € is 3 bytes.
        let h = render_limited(&"€".repeat(MAX_RENDERED_BYTES));
        assert!(h.len() <= MAX_RENDERED_BYTES + TRUNCATION_NOTICE.len() + 16);
        assert!(h.ends_with(TRUNCATION_NOTICE));
    }

    #[test]
    fn empty_input_is_empty() {
        assert_eq!(render_limited(""), "");
        assert_eq!(render_limited("   \n\n"), "");
    }

    #[test]
    fn safe_href_handles_non_ascii_without_panicking() {
        // A project URL is arbitrary UTF-8 from package METADATA; a multi-byte
        // char straddling byte 7/8 used to panic the str slice (request-path
        // panic = persistent DoS of /project/<pkg>/).
        assert_eq!(safe_href("€€€"), None);
        assert_eq!(safe_href("abcdef€://x"), None);
        assert_eq!(
            safe_href("https://exämple.com/€"),
            Some("https://exämple.com/€")
        );
        assert_eq!(safe_href("http://ok"), Some("http://ok"));
        assert_eq!(safe_href("javascript:alert(1)"), None);
    }
}
