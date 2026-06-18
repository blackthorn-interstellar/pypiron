use html_escape::{encode_double_quoted_attribute, encode_text};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

use crate::names::infer_version_from_filename;
use crate::sidecar::{Sidecar, Yanked};
use crate::status::ProjectStatusDoc;

/// Simple API version lineage: 1.0 PEP 629 (initial) · 1.1 PEP 700 (`versions`,
/// file `size`/`upload-time`) · 1.2 PEP 708 "tracks"/alternate-locations (out of
/// scope, see STANDARDS.md) · 1.3 PEP 740 `provenance` · 1.4 PEP 792 project
/// status markers.
const API_VERSION: &str = "1.4";

/// PEP 691 JSON simple-API content type (response, outgoing/incoming Accept).
pub const SIMPLE_JSON_CONTENT_TYPE: &str = "application/vnd.pypi.simple.v1+json";
/// PEP 503 HTML simple-API content type.
pub const SIMPLE_HTML_CONTENT_TYPE: &str = "text/html; charset=utf-8";

/// File metadata for rendering indexes, sourced from sidecars.
#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub filename: String,
    pub sha256: String,
    pub size: u64,
    /// RFC 3339 upload timestamp (PEP 700); sidecar first, storage last-modified fallback.
    pub upload_time: Option<String>,
    /// Version from the upload form, captured in the sidecar.
    pub version: Option<String>,
    /// PEP 592 yank state from the sidecar.
    pub yanked: Yanked,
    /// `Requires-Python` from the upload form, captured in the sidecar.
    pub requires_python: Option<String>,
    /// Whether a PEP 658 `<filename>.metadata` companion exists.
    pub core_metadata: bool,
    /// Whether a PEP 740 `<filename>.provenance` companion exists.
    pub provenance: bool,
}

impl FileMetadata {
    /// Build an index entry from an artifact's sidecar. `core_metadata` and
    /// `provenance` are whether the `<filename>.metadata` / `.provenance`
    /// companions exist. The worker and `verify` both derive index entries this
    /// exact way — keep them in lockstep by routing through here.
    pub fn from_sidecar(
        filename: &str,
        sc: Sidecar,
        core_metadata: bool,
        provenance: bool,
    ) -> Self {
        Self {
            filename: filename.to_string(),
            sha256: sc.sha256,
            size: sc.size,
            upload_time: Some(sc.upload_time),
            version: Some(sc.version).filter(|v| !v.is_empty()),
            yanked: sc.yanked,
            requires_python: sc.requires_python,
            core_metadata,
            provenance,
        }
    }
}

/// Render minimal PEP 503 per‑package HTML (with PEP 629 version meta and, for
/// a non-active project, the PEP 792 `pypi:project-status` meta). Quarantine
/// link-omission is the caller's job — it passes an empty `files` slice.
pub fn pep503_package_html(
    package: &str,
    files: &[FileMetadata],
    status: &ProjectStatusDoc,
) -> String {
    // Links are relative to the API under /files/<pkg>/<filename>
    let mut body = String::new();
    body.push_str(&format!(
        r#"<html><head><meta name="pypi:repository-version" content="{API_VERSION}">"#
    ));
    // PEP 792: emit the status marker only when non-active (active MAY be, and
    // here is, omitted). `reason` is arbitrary text, so it must be escaped.
    if !status.status.is_active() {
        body.push_str(&format!(
            r#"<meta name="pypi:project-status" content="{}">"#,
            status.status.as_str()
        ));
        if let Some(reason) = &status.reason {
            body.push_str(&format!(
                r#"<meta name="pypi:project-status-reason" content="{}">"#,
                encode_double_quoted_attribute(reason)
            ));
        }
    }
    body.push_str(&format!(
        r#"<title>Links for {}</title></head><body>"#,
        encode_text(package)
    ));
    // The package name is the same in every link's href; encode it once.
    let pkg_attr = encode_double_quoted_attribute(package);
    for f in files {
        // Two contexts, two escapers: link text wants text escaping, but the
        // href attribute (and #sha256 fragment) must escape `"` too, or an
        // uploaded filename like `a" onmouseover=…` breaks out of the attribute.
        let fname_text = encode_text(&f.filename);
        let fname_attr = encode_double_quoted_attribute(&f.filename);
        let sha_attr = encode_double_quoted_attribute(&f.sha256);
        let mut attrs = String::new();
        if let Some(rp) = &f.requires_python {
            attrs.push_str(&format!(
                r#" data-requires-python="{}""#,
                encode_double_quoted_attribute(rp)
            ));
        }
        if f.core_metadata {
            // PEP 714 name plus the original PEP 658 name for older clients.
            attrs.push_str(r#" data-core-metadata="true" data-dist-info-metadata="true""#);
        }
        if f.provenance {
            // PEP 740: point at the provenance companion served next to the
            // artifact. Root-relative like every URL we emit — we don't know
            // our public base, and clients resolve it against the index URL.
            attrs.push_str(&format!(
                r#" data-provenance="/files/{pkg_attr}/{fname_attr}.provenance""#
            ));
        }
        match &f.yanked {
            Yanked::Flag(false) => {}
            Yanked::Flag(true) => attrs.push_str(r#" data-yanked="""#),
            Yanked::Reason(reason) => attrs.push_str(&format!(
                r#" data-yanked="{}""#,
                encode_double_quoted_attribute(reason)
            )),
        }
        body.push_str(&format!(
            r##"<a href="/files/{pkg_attr}/{fname_attr}#sha256={sha_attr}"{attrs}>{fname_text}</a><br/>"##
        ));
    }
    body.push_str("</body></html>");
    body
}

/// Render minimal PEP 503 global HTML index (with PEP 629 version meta).
pub fn pep503_global_html(packages: &[String]) -> String {
    let mut body = String::new();
    body.push_str(&format!(
        r#"<html><head><meta name="pypi:repository-version" content="{API_VERSION}"><title>Simple index</title></head><body>"#
    ));
    for p in packages {
        let p_attr = encode_double_quoted_attribute(p);
        let p_text = encode_text(p);
        body.push_str(&format!(r#"<a href="/simple/{p_attr}/">{p_text}</a><br/>"#));
    }
    body.push_str("</body></html>");
    body
}

#[derive(Serialize)]
struct Pep691File {
    filename: String,
    url: String,
    hashes: HashMap<String, String>,
    size: u64,
    #[serde(rename = "upload-time", skip_serializing_if = "Option::is_none")]
    upload_time: Option<String>,
    yanked: Yanked,
    #[serde(rename = "requires-python", skip_serializing_if = "Option::is_none")]
    requires_python: Option<String>,
    #[serde(rename = "core-metadata", skip_serializing_if = "Option::is_none")]
    core_metadata: Option<bool>,
    #[serde(rename = "dist-info-metadata", skip_serializing_if = "Option::is_none")]
    dist_info_metadata: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance: Option<String>,
}

#[derive(Serialize)]
struct Pep691PkgIndex<'a> {
    #[serde(rename = "meta")]
    meta: Pep691Meta<'a>,
    name: &'a str,
    /// PEP 792 (top-level, NOT nested in `meta` — the spec's nested example is
    /// a known doc bug). Omitted for an active project.
    #[serde(rename = "project-status", skip_serializing_if = "Option::is_none")]
    project_status: Option<&'a ProjectStatusDoc>,
    versions: Vec<String>,
    files: Vec<Pep691File>,
}

#[derive(Serialize)]
struct Pep691Meta<'a> {
    #[serde(rename = "api-version")]
    api_version: &'a str,
}

/// PEP 691 + PEP 700 package JSON (with the PEP 792 `project-status` object for
/// a non-active project). Quarantine link-omission is the caller's job — it
/// passes an empty `files` slice, which empties `versions` too.
pub fn pep691_package_json(
    package: &str,
    files: &[FileMetadata],
    status: &ProjectStatusDoc,
) -> String {
    let versions: Vec<String> = files
        .iter()
        .filter_map(|f| {
            f.version
                .clone()
                .or_else(|| infer_version_from_filename(&f.filename))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let files: Vec<Pep691File> = files
        .iter()
        .map(|f| {
            let mut hashes = HashMap::new();
            hashes.insert("sha256".to_string(), f.sha256.clone());
            Pep691File {
                filename: f.filename.clone(),
                url: format!("/files/{package}/{}", f.filename),
                hashes,
                size: f.size,
                upload_time: f.upload_time.clone(),
                yanked: f.yanked.clone(),
                requires_python: f.requires_python.clone(),
                core_metadata: f.core_metadata.then_some(true),
                dist_info_metadata: f.core_metadata.then_some(true),
                provenance: f
                    .provenance
                    .then(|| format!("/files/{package}/{}.provenance", f.filename)),
            }
        })
        .collect();

    let doc = Pep691PkgIndex {
        meta: Pep691Meta {
            api_version: API_VERSION,
        },
        name: package,
        project_status: (!status.status.is_active()).then_some(status),
        versions,
        files,
    };
    serde_json::to_string(&doc)
        .unwrap_or_else(|_| format!("{{\"meta\":{{\"api-version\":\"{API_VERSION}\"}}}}"))
}

#[derive(Serialize)]
struct Pep691ProjectRef<'a> {
    name: &'a str,
    url: String,
}

#[derive(Serialize)]
struct Pep691Global<'a> {
    #[serde(rename = "meta")]
    meta: Pep691Meta<'a>,
    projects: Vec<Pep691ProjectRef<'a>>,
}

/// Minimal PEP 691 global index JSON.
pub fn pep691_global_json(packages: &[String]) -> String {
    let projects: Vec<Pep691ProjectRef> = packages
        .iter()
        .map(|p| Pep691ProjectRef {
            name: p,
            url: format!("/simple/{p}/"),
        })
        .collect();

    let doc = Pep691Global {
        meta: Pep691Meta {
            api_version: API_VERSION,
        },
        projects,
    };
    serde_json::to_string(&doc)
        .unwrap_or_else(|_| format!("{{\"meta\":{{\"api-version\":\"{API_VERSION}\"}}}}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::ProjectStatus;

    fn meta(version: Option<&str>) -> FileMetadata {
        FileMetadata {
            filename: "six-1.16.0-py2.py3-none-any.whl".into(),
            sha256: "abc123".into(),
            size: 11236,
            upload_time: Some("2026-06-11T00:00:00Z".into()),
            version: version.map(str::to_string),
            yanked: Yanked::Flag(false),
            requires_python: None,
            core_metadata: false,
            provenance: false,
        }
    }

    fn active() -> ProjectStatusDoc {
        ProjectStatusDoc::default()
    }

    #[test]
    fn package_json_has_pep700_fields() {
        let json = pep691_package_json("six", &[meta(Some("1.16.0"))], &active());
        let doc: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(doc["meta"]["api-version"], "1.4");
        assert_eq!(doc["versions"][0], "1.16.0");
        assert_eq!(doc["files"][0]["size"], 11236);
        assert_eq!(doc["files"][0]["upload-time"], "2026-06-11T00:00:00Z");
    }

    #[test]
    fn package_json_versions_fall_back_to_filename_inference() {
        let json = pep691_package_json("six", &[meta(None)], &active());
        let doc: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(doc["versions"][0], "1.16.0");
    }

    #[test]
    fn package_html_has_hash_fragment_and_version_meta() {
        let html = pep503_package_html("six", &[meta(None)], &active());
        assert!(html.contains("#sha256=abc123"));
        assert!(!html.contains("data-yanked"));
        assert!(html.contains(r#"<meta name="pypi:repository-version" content="1.4">"#));
    }

    #[test]
    fn active_status_is_omitted_entirely() {
        // PEP 792 lets us omit the active marker; we do, in both formats.
        let html = pep503_package_html("six", &[meta(Some("1.16.0"))], &active());
        assert!(!html.contains("pypi:project-status"));

        let doc: serde_json::Value = serde_json::from_str(&pep691_package_json(
            "six",
            &[meta(Some("1.16.0"))],
            &active(),
        ))
        .unwrap();
        assert!(doc.get("project-status").is_none());
        // It is top-level when present, never nested in meta.
        assert!(doc["meta"].get("project-status").is_none());
    }

    #[test]
    fn non_active_status_renders_top_level_with_escaped_reason() {
        let status = ProjectStatusDoc {
            status: ProjectStatus::Archived,
            reason: Some(r#"moved to "foo""#.into()),
        };
        let html = pep503_package_html("six", &[meta(Some("1.16.0"))], &status);
        assert!(html.contains(r#"<meta name="pypi:project-status" content="archived">"#));
        assert!(html.contains(
            r#"<meta name="pypi:project-status-reason" content="moved to &quot;foo&quot;">"#
        ));

        let doc: serde_json::Value = serde_json::from_str(&pep691_package_json(
            "six",
            &[meta(Some("1.16.0"))],
            &status,
        ))
        .unwrap();
        assert_eq!(doc["project-status"]["status"], "archived");
        assert_eq!(doc["project-status"]["reason"], r#"moved to "foo""#);
    }

    #[test]
    fn quarantine_omits_links_but_keeps_marker() {
        // The caller drops the files for a quarantined project; render still
        // emits the marker and now has nothing to link.
        let status = ProjectStatusDoc {
            status: ProjectStatus::Quarantined,
            reason: None,
        };
        assert!(status.status.blocks_downloads());

        let html = pep503_package_html("six", &[], &status);
        assert!(html.contains(r#"<meta name="pypi:project-status" content="quarantined">"#));
        assert!(!html.contains("<a href"));

        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[], &status)).unwrap();
        assert_eq!(doc["project-status"]["status"], "quarantined");
        assert_eq!(doc["files"].as_array().unwrap().len(), 0);
        assert_eq!(doc["versions"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn requires_python_and_core_metadata_render() {
        let mut m = meta(Some("1.16.0"));
        m.requires_python = Some(">=3.9".into());
        m.core_metadata = true;

        let html = pep503_package_html("six", &[m.clone()], &active());
        assert!(html.contains(r#"data-requires-python="&gt;=3.9""#));
        assert!(html.contains(r#"data-core-metadata="true""#));
        assert!(html.contains(r#"data-dist-info-metadata="true""#));

        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[m], &active())).unwrap();
        assert_eq!(doc["files"][0]["requires-python"], ">=3.9");
        assert_eq!(doc["files"][0]["core-metadata"], true);
        assert_eq!(doc["files"][0]["dist-info-metadata"], true);

        let plain = meta(None);
        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[plain], &active())).unwrap();
        assert!(doc["files"][0].get("requires-python").is_none());
        assert!(doc["files"][0].get("core-metadata").is_none());
    }

    #[test]
    fn provenance_renders_in_html_and_json() {
        let mut m = meta(Some("1.16.0"));
        m.provenance = true;

        let html = pep503_package_html("six", &[m.clone()], &active());
        assert!(html.contains(
            r#"data-provenance="/files/six/six-1.16.0-py2.py3-none-any.whl.provenance""#
        ));

        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[m], &active())).unwrap();
        assert_eq!(
            doc["files"][0]["provenance"],
            "/files/six/six-1.16.0-py2.py3-none-any.whl.provenance"
        );

        // Absent companion → no field / attribute at all.
        let plain = meta(None);
        let html = pep503_package_html("six", std::slice::from_ref(&plain), &active());
        assert!(!html.contains("data-provenance"));
        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[plain], &active())).unwrap();
        assert!(doc["files"][0].get("provenance").is_none());
    }

    #[test]
    fn filename_and_sha_cannot_break_out_of_href_attribute() {
        // A filename passes upload validation as long as it has no '/'/'\\' and
        // isn't a dotfile/sidecar — a double-quote is allowed, so it must not be
        // able to terminate the href attribute and inject markup.
        let mut m = meta(Some("1.0"));
        m.filename = r#"a" onmouseover=alert(1) x.whl"#.into();
        m.sha256 = r#"b"><script>"#.into();
        let html = pep503_package_html("six", &[m], &active());
        // In the href the quote is escaped, so the attribute never terminates
        // early (the raw `href="/files/six/a"` breakout must not appear).
        assert!(
            html.contains(r#"href="/files/six/a&quot; onmouseover=alert(1) x.whl#sha256=b&quot;"#)
        );
        assert!(!html.contains(r#"href="/files/six/a""#));
        // The link text keeps the literal filename — harmless in text context.
        assert!(html.contains(r#">a" onmouseover=alert(1) x.whl</a>"#));
    }

    #[test]
    fn yanked_renders_in_html_and_json() {
        let mut yanked = meta(Some("1.16.0"));
        yanked.yanked = Yanked::Reason("broken \"wheel\"".into());

        let html = pep503_package_html("six", &[yanked.clone()], &active());
        assert!(html.contains(r#"data-yanked="broken &quot;wheel&quot;""#));

        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[yanked], &active())).unwrap();
        assert_eq!(doc["files"][0]["yanked"], "broken \"wheel\"");

        let mut flagged = meta(Some("1.16.0"));
        flagged.yanked = Yanked::Flag(true);
        let html = pep503_package_html("six", &[flagged.clone()], &active());
        assert!(html.contains(r#"data-yanked="""#));
        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[flagged], &active())).unwrap();
        assert_eq!(doc["files"][0]["yanked"], true);
    }
}
