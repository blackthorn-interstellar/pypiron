use html_escape::encode_text;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

use crate::names::infer_version_from_filename;

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
        body.push_str(&format!(
            r##"<a href="/files/{}/{fname}#sha256={}">{fname}</a><br/>"##,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    yanked: Option<bool>,
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
                yanked: None,
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
        assert!(html.contains(r#"<meta name="pypi:repository-version" content="1.1">"#));
    }
}
