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

use crate::web::safe_href;

/// Render Markdown to a constrained, safe HTML fragment.
pub fn render_limited(md: &str) -> String {
    let opts = Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let mut out = String::with_capacity(md.len() + md.len() / 2);
    // Close tags for currently-open whitelisted elements. A dropped element
    // pushes an empty string, so Start/End stay balanced and its text children
    // still flow through unwrapped.
    let mut closes: Vec<&'static str> = Vec::new();
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
        // While collecting an image's alt span, take only text; ignore markup.
        if image.is_some() {
            match ev {
                Event::Start(Tag::Image { .. }) => image_depth += 1,
                Event::End(TagEnd::Image) if image_depth > 0 => image_depth -= 1,
                Event::End(TagEnd::Image) => {
                    // `take()` clears the state regardless; emit only when the
                    // src survived the safe-URL check (else the image is dropped).
                    if let Some((Some(src), alt)) = image.take() {
                        out.push_str(&format!(
                            "<img src=\"{}\" alt=\"{}\" loading=\"lazy\" referrerpolicy=\"no-referrer\">",
                            encode_double_quoted_attribute(&src),
                            encode_double_quoted_attribute(&alt),
                        ));
                    }
                }
                Event::Text(t) | Event::Code(t) => {
                    if let Some((_, alt)) = image.as_mut() {
                        alt.push_str(&t);
                    }
                }
                _ => {}
            }
            continue;
        }

        match ev {
            Event::Start(Tag::Image { dest_url, .. }) => {
                image = Some((safe_href(&dest_url).map(str::to_string), String::new()));
            }
            Event::Start(Tag::TableHead) => {
                in_head = true;
                out.push_str("<thead><tr>");
                closes.push("</tr></thead>");
            }
            Event::Start(Tag::TableCell) => {
                out.push_str(if in_head { "<th>" } else { "<td>" });
                closes.push(if in_head { "</th>" } else { "</td>" });
            }
            Event::Start(tag) => {
                let (open, close) = open_close(&tag);
                out.push_str(&open);
                closes.push(close);
            }
            Event::End(TagEnd::TableHead) => {
                in_head = false;
                if let Some(c) = closes.pop() {
                    out.push_str(c);
                }
            }
            Event::End(_) => {
                if let Some(c) = closes.pop() {
                    out.push_str(c);
                }
            }
            Event::Text(t) => out.push_str(&encode_text(&t)),
            Event::Code(t) => {
                out.push_str("<code>");
                out.push_str(&encode_text(&t));
                out.push_str("</code>");
            }
            Event::SoftBreak => out.push('\n'),
            Event::HardBreak => out.push_str("<br>"),
            Event::Rule => out.push_str("<hr>"),
            Event::TaskListMarker(done) => out.push_str(if done { "[x] " } else { "[ ] " }),
            // Raw/inline HTML, math, footnote refs: dropped entirely.
            _ => {}
        }
    }
    out
}

/// Open/close strings for a whitelisted tag. A non-whitelisted tag returns
/// empty strings so its text children still render, just unwrapped.
fn open_close(tag: &Tag) -> (String, &'static str) {
    match tag {
        Tag::Paragraph => ("<p>".into(), "</p>"),
        Tag::Heading { level, .. } => (format!("<{}>", heading(*level)), heading_end(*level)),
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
        Tag::Link { dest_url, .. } => match safe_href(dest_url) {
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

fn heading(l: HeadingLevel) -> &'static str {
    match l {
        HeadingLevel::H1 => "h1",
        HeadingLevel::H2 => "h2",
        HeadingLevel::H3 => "h3",
        HeadingLevel::H4 => "h4",
        HeadingLevel::H5 => "h5",
        HeadingLevel::H6 => "h6",
    }
}

fn heading_end(l: HeadingLevel) -> &'static str {
    match l {
        HeadingLevel::H1 => "</h1>",
        HeadingLevel::H2 => "</h2>",
        HeadingLevel::H3 => "</h3>",
        HeadingLevel::H4 => "</h4>",
        HeadingLevel::H5 => "</h5>",
        HeadingLevel::H6 => "</h6>",
    }
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
    fn empty_input_is_empty() {
        assert_eq!(render_limited(""), "");
        assert_eq!(render_limited("   \n\n"), "");
    }
}
