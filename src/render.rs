use html_escape::{encode_double_quoted_attribute, encode_text};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

use crate::names::infer_version_from_filename;
use crate::sidecar::Yanked;

/// Simple API version: PEP 691 (1.0) + PEP 700 fields (1.1).
const API_VERSION: &str = "1.1";

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
}

/// Render minimal PEP 503 per‑package HTML (with PEP 629 version meta).
pub fn pep503_package_html(package: &str, files: &[FileMetadata]) -> String {
    // Links are relative to the API under /files/<pkg>/<filename>
    let mut body = String::new();
    body.push_str(&format!(
        r#"<html><head><meta name="pypi:repository-version" content="{API_VERSION}"><title>Links for {}</title></head><body>"#,
        encode_text(package)
    ));
    for f in files {
        let fname = encode_text(&f.filename);
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
        match &f.yanked {
            Yanked::Flag(false) => {}
            Yanked::Flag(true) => attrs.push_str(r#" data-yanked="""#),
            Yanked::Reason(reason) => attrs.push_str(&format!(
                r#" data-yanked="{}""#,
                encode_double_quoted_attribute(reason)
            )),
        }
        body.push_str(&format!(
            r##"<a href="/files/{}/{fname}#sha256={}"{attrs}>{fname}</a><br/>"##,
            encode_text(package),
            f.sha256
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
        let p = encode_text(p);
        body.push_str(&format!(r#"<a href="/simple/{p}/">{p}</a><br/>"#));
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
}

#[derive(Serialize)]
struct Pep691PkgIndex<'a> {
    #[serde(rename = "meta")]
    meta: Pep691Meta<'a>,
    name: &'a str,
    versions: Vec<String>,
    files: Vec<Pep691File>,
}

#[derive(Serialize)]
struct Pep691Meta<'a> {
    #[serde(rename = "api-version")]
    api_version: &'a str,
}

/// PEP 691 + PEP 700 package JSON.
pub fn pep691_package_json(package: &str, files: &[FileMetadata]) -> String {
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
            }
        })
        .collect();

    let doc = Pep691PkgIndex {
        meta: Pep691Meta {
            api_version: API_VERSION,
        },
        name: package,
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
        }
    }

    #[test]
    fn package_json_has_pep700_fields() {
        let json = pep691_package_json("six", &[meta(Some("1.16.0"))]);
        let doc: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(doc["meta"]["api-version"], "1.1");
        assert_eq!(doc["versions"][0], "1.16.0");
        assert_eq!(doc["files"][0]["size"], 11236);
        assert_eq!(doc["files"][0]["upload-time"], "2026-06-11T00:00:00Z");
    }

    #[test]
    fn package_json_versions_fall_back_to_filename_inference() {
        let json = pep691_package_json("six", &[meta(None)]);
        let doc: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(doc["versions"][0], "1.16.0");
    }

    #[test]
    fn package_html_has_hash_fragment_and_version_meta() {
        let html = pep503_package_html("six", &[meta(None)]);
        assert!(html.contains("#sha256=abc123"));
        assert!(!html.contains("data-yanked"));
        assert!(html.contains(r#"<meta name="pypi:repository-version" content="1.1">"#));
    }

    #[test]
    fn requires_python_and_core_metadata_render() {
        let mut m = meta(Some("1.16.0"));
        m.requires_python = Some(">=3.9".into());
        m.core_metadata = true;

        let html = pep503_package_html("six", &[m.clone()]);
        assert!(html.contains(r#"data-requires-python="&gt;=3.9""#));
        assert!(html.contains(r#"data-core-metadata="true""#));
        assert!(html.contains(r#"data-dist-info-metadata="true""#));

        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[m])).unwrap();
        assert_eq!(doc["files"][0]["requires-python"], ">=3.9");
        assert_eq!(doc["files"][0]["core-metadata"], true);
        assert_eq!(doc["files"][0]["dist-info-metadata"], true);

        let plain = meta(None);
        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[plain])).unwrap();
        assert!(doc["files"][0].get("requires-python").is_none());
        assert!(doc["files"][0].get("core-metadata").is_none());
    }

    #[test]
    fn yanked_renders_in_html_and_json() {
        let mut yanked = meta(Some("1.16.0"));
        yanked.yanked = Yanked::Reason("broken \"wheel\"".into());

        let html = pep503_package_html("six", &[yanked.clone()]);
        assert!(html.contains(r#"data-yanked="broken &quot;wheel&quot;""#));

        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[yanked])).unwrap();
        assert_eq!(doc["files"][0]["yanked"], "broken \"wheel\"");

        let mut flagged = meta(Some("1.16.0"));
        flagged.yanked = Yanked::Flag(true);
        let html = pep503_package_html("six", &[flagged.clone()]);
        assert!(html.contains(r#"data-yanked="""#));
        let doc: serde_json::Value =
            serde_json::from_str(&pep691_package_json("six", &[flagged])).unwrap();
        assert_eq!(doc["files"][0]["yanked"], true);
    }
}
