//! PEP 691 Simple API: the shared client and types `sync` and the proxy both
//! consume. One project listing, one request — file URLs, hashes, PEP 700
//! sizes/timestamps, PEP 658/714 metadata signals, and PEP 740 provenance all
//! ride in the same response. It is the standard API, so a source can be PyPI,
//! another pypiron, or any PEP 691 index.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;

use crate::render::{FileMetadata, SIMPLE_JSON_CONTENT_TYPE};
use crate::sidecar::Yanked;
use crate::status::ProjectStatusDoc;

/// One file from a PEP 691 listing (PEP 700 + PEP 658/714 + PEP 740 fields).
#[derive(Debug, Clone, Deserialize)]
pub struct SimpleFile {
    pub filename: String,
    pub url: String,
    #[serde(default)]
    pub hashes: HashMap<String, String>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(rename = "upload-time", default)]
    pub upload_time: Option<String>,
    #[serde(rename = "requires-python", default)]
    pub requires_python: Option<String>,
    #[serde(default)]
    pub yanked: Yanked,
    /// PEP 714 / PEP 658: bool or a hash object; anything but false/null means
    /// the metadata companion exists upstream.
    #[serde(rename = "core-metadata", default)]
    pub core_metadata: Option<serde_json::Value>,
    #[serde(rename = "dist-info-metadata", default)]
    pub dist_info_metadata: Option<serde_json::Value>,
    /// PEP 740: URL of the file's provenance object (absolute on PyPI).
    #[serde(default)]
    pub provenance: Option<String>,
}

impl SimpleFile {
    pub fn sha256(&self) -> Option<&str> {
        self.hashes.get("sha256").map(String::as_str)
    }

    pub fn has_core_metadata(&self) -> bool {
        let truthy = |v: &serde_json::Value| !matches!(v, serde_json::Value::Bool(false));
        self.core_metadata.as_ref().map(truthy).unwrap_or(false)
            || self
                .dist_info_metadata
                .as_ref()
                .map(truthy)
                .unwrap_or(false)
    }

    /// Index entry rendered from this listing. `version` is left to filename
    /// inference downstream — the Simple API doesn't bind files to versions.
    pub fn as_file_metadata(&self) -> FileMetadata {
        FileMetadata {
            filename: self.filename.clone(),
            sha256: self.sha256().unwrap_or_default().to_string(),
            size: self.size.unwrap_or(0),
            upload_time: self.upload_time.clone(),
            version: None,
            yanked: self.yanked.clone(),
            requires_python: self.requires_python.clone(),
            core_metadata: self.has_core_metadata(),
            provenance: self.provenance.is_some(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SimpleIndex {
    #[serde(default)]
    pub files: Vec<SimpleFile>,
    /// PEP 792 project status, relayed verbatim. We are CONSUMING someone
    /// else's index here, so an unknown/foreign marker degrades to `None`
    /// (== active) rather than failing the whole listing — the opposite of our
    /// own fail-closed [`crate::status::read_status`].
    #[serde(
        rename = "project-status",
        default,
        deserialize_with = "lenient_status"
    )]
    pub project_status: Option<ProjectStatusDoc>,
}

/// Parse the upstream `project-status` object, swallowing anything we don't
/// recognize (a future fifth marker must not break mirroring the whole index).
fn lenient_status<'de, D>(de: D) -> Result<Option<ProjectStatusDoc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<serde_json::Value>::deserialize(de)?;
    Ok(raw.and_then(|v| serde_json::from_value(v).ok()))
}

/// Fetch a package's PEP 691 JSON listing from `base`. `Ok(None)` on a 404 —
/// the package isn't on this index. `timeout` bounds the whole request for
/// latency-sensitive callers (the proxy); `None` relies on the client's own
/// timeouts (sync).
pub async fn fetch_index(
    client: &Client,
    base: &str,
    pkg: &str,
    timeout: Option<Duration>,
) -> Result<Option<SimpleIndex>> {
    let url = format!("{}/simple/{pkg}/", base.trim_end_matches('/'));
    let mut req = client
        .get(&url)
        .header(reqwest::header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE);
    if let Some(t) = timeout {
        req = req.timeout(t);
    }
    let resp = req.send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(resp.error_for_status()?.json().await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_file(json: serde_json::Value) -> SimpleFile {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn parses_pep700_metadata_and_provenance_fields() {
        let f = simple_file(serde_json::json!({
            "filename": "six-1.16.0-py2.py3-none-any.whl",
            "url": "/files/six/six-1.16.0-py2.py3-none-any.whl",
            "hashes": {"sha256": "abc"},
            "size": 11236,
            "upload-time": "2021-05-05T14:18:17Z",
            "requires-python": ">=2.7",
            "yanked": false,
            "core-metadata": {"sha256": "def"},
            "provenance": "https://pypi.org/integrity/six/1.16.0/six-1.16.0-py2.py3-none-any.whl/provenance"
        }));
        assert_eq!(f.sha256(), Some("abc"));
        assert!(f.has_core_metadata());
        let meta = f.as_file_metadata();
        assert_eq!(meta.size, 11236);
        assert_eq!(meta.upload_time.as_deref(), Some("2021-05-05T14:18:17Z"));
        assert!(meta.core_metadata);
        assert!(meta.provenance);

        // A bare file (no hashes / metadata / provenance) degrades cleanly.
        let bare = simple_file(serde_json::json!({
            "filename": "six-1.16.0.tar.gz",
            "url": "https://files.example.com/six-1.16.0.tar.gz"
        }));
        assert_eq!(bare.sha256(), None);
        assert!(!bare.has_core_metadata());
        assert!(!bare.as_file_metadata().provenance);
    }

    #[test]
    fn project_status_relays_from_upstream_and_degrades_safely() {
        use crate::status::ProjectStatus;

        let archived: SimpleIndex = serde_json::from_value(serde_json::json!({
            "files": [],
            "project-status": {"status": "archived", "reason": "moved"}
        }))
        .unwrap();
        let doc = archived.project_status.unwrap();
        assert_eq!(doc.status, ProjectStatus::Archived);
        assert_eq!(doc.reason.as_deref(), Some("moved"));

        // Absent → None (== active).
        let plain: SimpleIndex =
            serde_json::from_value(serde_json::json!({ "files": [] })).unwrap();
        assert!(plain.project_status.is_none());

        // An unknown/foreign marker must NOT fail the whole listing.
        let future: SimpleIndex = serde_json::from_value(serde_json::json!({
            "files": [],
            "project-status": {"status": "hexed"}
        }))
        .unwrap();
        assert!(future.project_status.is_none());
    }

    #[test]
    fn yanked_reason_parses_from_simple_api() {
        let f = simple_file(serde_json::json!({
            "filename": "six-1.16.0-py2.py3-none-any.whl",
            "url": "x",
            "hashes": {"sha256": "abc"},
            "yanked": "broken release"
        }));
        assert_eq!(f.yanked, Yanked::Reason("broken release".into()));
    }
}
