//! Parse Python core metadata (PEP 566/643) — RFC 822-style headers followed
//! by a free-text description body — into the handful of fields the human
//! project page shows. Pure parsing, no I/O, unit-tested.
//!
//! This deliberately does NOT render the description: the page shows it
//! verbatim in a `<pre>` (see [`crate::web::project_html`]). Rendering Markdown
//! or reStructuredText safely is a separate, opt-in concern.

/// Display fields lifted from a wheel's `METADATA` (or an sdist's `PKG-INFO`).
/// Every field is optional — legacy and minimal artifacts omit most of them.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct CoreMetadata {
    pub version: Option<String>,
    pub summary: Option<String>,
    pub author: Option<String>,
    pub author_email: Option<String>,
    /// PEP 639 `License-Expression` if present, else the legacy `License`.
    pub license: Option<String>,
    pub requires_python: Option<String>,
    pub keywords: Option<String>,
    /// `Home-page` plus every `Project-URL` (label, url), in file order.
    pub project_urls: Vec<(String, String)>,
    pub classifiers: Vec<String>,
    pub requires_dist: Vec<String>,
    /// The long description / README body, exactly as stored.
    pub description: Option<String>,
}

/// Parse core-metadata bytes. Invalid UTF-8 is lossily decoded rather than
/// rejected — display is best-effort and must never fail the page.
pub fn parse(bytes: &[u8]) -> CoreMetadata {
    let text = String::from_utf8_lossy(bytes);
    let (headers, body) = split_headers_body(&text);

    let mut m = CoreMetadata::default();
    let mut license_expression: Option<String> = None;
    let mut description_header: Option<String> = None;

    for (key, value) in headers {
        let v = value.trim();
        if v.is_empty() {
            continue;
        }
        match key.to_ascii_lowercase().as_str() {
            "version" => m.version = Some(v.to_string()),
            "summary" => m.summary = Some(v.to_string()),
            "author" => m.author = Some(v.to_string()),
            "author-email" => m.author_email = Some(v.to_string()),
            "license" => m.license = Some(v.to_string()),
            "license-expression" => license_expression = Some(v.to_string()),
            "requires-python" => m.requires_python = Some(v.to_string()),
            "keywords" => m.keywords = Some(v.to_string()),
            "home-page" => m.project_urls.push(("Homepage".to_string(), v.to_string())),
            "project-url" => {
                if let Some((label, url)) = v.split_once(',') {
                    m.project_urls
                        .push((label.trim().to_string(), url.trim().to_string()));
                }
            }
            "classifier" => m.classifiers.push(v.to_string()),
            "requires-dist" => m.requires_dist.push(v.to_string()),
            "description" => description_header = Some(v.to_string()),
            _ => {}
        }
    }

    // PEP 639: a SPDX `License-Expression` supersedes the free-text `License`.
    if let Some(expr) = license_expression {
        m.license = Some(expr);
    }

    // Modern metadata carries the description in the body; the legacy form put
    // it in a folded `Description` header. Prefer the body when present.
    let body = body.trim();
    m.description = if !body.is_empty() {
        Some(body.to_string())
    } else {
        description_header
    };

    m
}

/// Split a metadata document at the first blank line: header block before,
/// description body after. Handles RFC 822 continuation lines (a value folded
/// onto following whitespace-indented lines). Both `\n` and `\r\n` are accepted.
fn split_headers_body(text: &str) -> (Vec<(String, String)>, &str) {
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut last: Option<usize> = None;
    let mut offset = 0;
    let mut body_start = text.len();

    for line in text.split_inclusive('\n') {
        let content = line.trim_end_matches(['\r', '\n']);
        if content.is_empty() {
            body_start = offset + line.len();
            break;
        }
        if line.starts_with([' ', '\t']) {
            // Folded continuation of the previous header value.
            if let Some(i) = last {
                headers[i].1.push('\n');
                headers[i].1.push_str(content.trim_start());
            }
        } else if let Some((k, v)) = content.split_once(':') {
            headers.push((k.trim().to_string(), v.trim_start().to_string()));
            last = Some(headers.len() - 1);
        }
        offset += line.len();
    }

    (headers, &text[body_start..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_modern_wheel_metadata_with_body_description() {
        let md = b"Metadata-Version: 2.1\n\
Name: demo\n\
Version: 1.2.3\n\
Summary: A demo package\n\
Author: Ada Lovelace\n\
Author-email: ada@example.com\n\
License-Expression: MIT\n\
Requires-Python: >=3.9\n\
Keywords: demo,example\n\
Home-page: https://example.com\n\
Project-URL: Source, https://github.com/ada/demo\n\
Classifier: Programming Language :: Python :: 3\n\
Classifier: License :: OSI Approved :: MIT License\n\
Requires-Dist: requests>=2\n\
Requires-Dist: click; extra == \"cli\"\n\
\n\
# Demo\n\nThis is the README body.\n";
        let m = parse(md);
        assert_eq!(m.version.as_deref(), Some("1.2.3"));
        assert_eq!(m.summary.as_deref(), Some("A demo package"));
        assert_eq!(m.author.as_deref(), Some("Ada Lovelace"));
        assert_eq!(m.author_email.as_deref(), Some("ada@example.com"));
        assert_eq!(m.license.as_deref(), Some("MIT"));
        assert_eq!(m.requires_python.as_deref(), Some(">=3.9"));
        assert_eq!(m.keywords.as_deref(), Some("demo,example"));
        assert_eq!(
            m.project_urls,
            vec![
                ("Homepage".to_string(), "https://example.com".to_string()),
                (
                    "Source".to_string(),
                    "https://github.com/ada/demo".to_string()
                ),
            ]
        );
        assert_eq!(m.classifiers.len(), 2);
        assert_eq!(
            m.requires_dist,
            vec!["requests>=2", "click; extra == \"cli\""]
        );
        assert_eq!(
            m.description.as_deref(),
            Some("# Demo\n\nThis is the README body.")
        );
    }

    #[test]
    fn license_expression_supersedes_legacy_license() {
        let md = b"Name: demo\nLicense: BSD-ish free text\nLicense-Expression: Apache-2.0\n";
        assert_eq!(parse(md).license.as_deref(), Some("Apache-2.0"));
    }

    #[test]
    fn legacy_description_header_with_folding() {
        // Old-style: no body, description folded into a continued header.
        let md = b"Name: demo\nDescription: line one\n        line two\n";
        let m = parse(md);
        assert_eq!(m.description.as_deref(), Some("line one\nline two"));
    }

    #[test]
    fn empty_and_garbage_degrade_to_defaults() {
        assert_eq!(parse(b""), CoreMetadata::default());
        // A bare body with no headers is still a (empty-headers) parse.
        let m = parse(b"\njust a body\n");
        assert!(m.version.is_none());
        assert_eq!(m.description.as_deref(), Some("just a body"));
    }

    #[test]
    fn crlf_line_endings_parse() {
        let md = b"Name: demo\r\nSummary: windows\r\n\r\nbody here\r\n";
        let m = parse(md);
        assert_eq!(m.summary.as_deref(), Some("windows"));
        assert_eq!(m.description.as_deref(), Some("body here"));
    }
}
