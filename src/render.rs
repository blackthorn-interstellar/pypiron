use html_escape::encode_text;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};

/// Simple API version: PEP 691 (1.0) + PEP 700 fields (1.1).
const API_VERSION: &str = "1.1";

/// File metadata for rendering indexes
#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub filename: String,
    pub sha256: String,
    pub size: u64,
    /// RFC 3339 upload timestamp (PEP 700), from storage last-modified.
    pub upload_time: Option<String>,
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

/// Best-effort version extraction from an artifact filename (PEP 700 `versions`).
fn infer_version_from_filename(filename: &str) -> Option<String> {
    if let Some(stem) = filename.strip_suffix(".whl") {
        // PEP 427: distribution-version(-build)?-python-abi-platform.whl
        return stem.split('-').nth(1).map(str::to_string);
    }
    let stem = filename
        .strip_suffix(".tar.gz")
        .or_else(|| filename.strip_suffix(".tar.bz2"))
        .or_else(|| filename.strip_suffix(".tar.xz"))
        .or_else(|| filename.strip_suffix(".zip"))?;
    stem.rsplit_once('-').map(|(_, v)| v.to_string())
}

/// PEP 691 + PEP 700 package JSON.
pub fn pep691_package_json(package: &str, files: &[FileMetadata]) -> String {
    let versions: Vec<String> = files
        .iter()
        .filter_map(|f| infer_version_from_filename(&f.filename))
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

    #[test]
    fn infers_wheel_version() {
        assert_eq!(
            infer_version_from_filename("six-1.16.0-py2.py3-none-any.whl"),
            Some("1.16.0".to_string())
        );
    }

    #[test]
    fn infers_sdist_version() {
        assert_eq!(
            infer_version_from_filename("six-1.16.0.tar.gz"),
            Some("1.16.0".to_string())
        );
    }

    #[test]
    fn package_json_has_pep700_fields() {
        let files = vec![FileMetadata {
            filename: "six-1.16.0-py2.py3-none-any.whl".into(),
            sha256: "abc123".into(),
            size: 11236,
            upload_time: Some("2026-06-11T00:00:00Z".into()),
        }];
        let json = pep691_package_json("six", &files);
        let doc: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(doc["meta"]["api-version"], "1.1");
        assert_eq!(doc["versions"][0], "1.16.0");
        assert_eq!(doc["files"][0]["size"], 11236);
        assert_eq!(doc["files"][0]["upload-time"], "2026-06-11T00:00:00Z");
    }

    #[test]
    fn package_html_has_hash_fragment_and_version_meta() {
        let files = vec![FileMetadata {
            filename: "six-1.16.0-py2.py3-none-any.whl".into(),
            sha256: "abc123".into(),
            size: 11236,
            upload_time: None,
        }];
        let html = pep503_package_html("six", &files);
        assert!(html.contains("#sha256=abc123"));
        assert!(html.contains(r#"<meta name="pypi:repository-version" content="1.1">"#));
    }
}
