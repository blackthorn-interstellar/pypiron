//! PEP 691 Simple API: the shared client and types `sync` and the proxy both
//! consume. One project listing, one request — file URLs, hashes, PEP 700
//! sizes/timestamps, PEP 658/714 metadata signals, and PEP 740 provenance all
//! ride in the same response. It is the standard API, so a source can be PyPI,
//! another pypiron, or any PEP 691 index.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Result};
use futures::StreamExt;
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

    /// PEP 714/658 core-metadata digest, if the listing carried one as a hash
    /// object (`{"sha256": "…"}`). A bare `true` advertises the companion without
    /// a digest, so returns `None` — nothing to verify against.
    pub fn core_metadata_sha256(&self) -> Option<&str> {
        [&self.core_metadata, &self.dist_info_metadata]
            .into_iter()
            .flatten()
            .find_map(|v| v.get("sha256").and_then(serde_json::Value::as_str))
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
#[serde(from = "RawIndex")]
pub struct SimpleIndex {
    pub files: Vec<SimpleFile>,
    /// PEP 792 project status, relayed verbatim. We are CONSUMING someone
    /// else's index here, so an unknown/foreign marker degrades rather than
    /// failing the whole listing — the opposite of our own fail-closed
    /// [`crate::status::read_status`]. It degrades to `None`, which therefore
    /// covers BOTH "no marker" and "a marker we couldn't parse": any caller that
    /// would *write* the difference must ask [`SimpleIndex::upstream_status`].
    pub project_status: Option<ProjectStatusDoc>,
    /// Upstream sent a `project-status` we couldn't parse. See [`UpstreamStatus`].
    project_status_unparseable: bool,
}

/// The listing exactly as it arrives, before the status is interpreted. Keeping
/// the raw value is what lets [`SimpleIndex`] tell an unparseable marker from an
/// absent one; parsing straight into `Option<ProjectStatusDoc>` cannot.
#[derive(Deserialize)]
struct RawIndex {
    #[serde(default)]
    files: Vec<SimpleFile>,
    #[serde(rename = "project-status", default)]
    project_status: Option<serde_json::Value>,
}

impl From<RawIndex> for SimpleIndex {
    fn from(raw: RawIndex) -> Self {
        // Anything we don't recognize is swallowed (a future fifth marker must
        // not break mirroring the whole index) — but it is remembered, because
        // "unparseable" read as "active" is fail-open: it would let a garbled or
        // hostile source clear a freeze the destination is holding.
        let parsed = raw
            .project_status
            .map(|v| serde_json::from_value::<ProjectStatusDoc>(v).ok());
        SimpleIndex {
            files: raw.files,
            project_status_unparseable: parsed.as_ref().is_some_and(Option::is_none),
            project_status: parsed.flatten(),
        }
    }
}

/// What an upstream listing says about a project's PEP 792 status. Three states,
/// not two: a `project-status` we could not parse is NOT the same as no marker
/// at all, and a mirror that collapses the two clears a destination freeze on the
/// strength of upstream garbage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamStatus {
    /// No marker at all. PEP 792 omits it for active, so this means active.
    Absent,
    /// A marker we understood.
    Known(ProjectStatusDoc),
    /// A marker we could not parse — a future status, or garbage. No verdict:
    /// we can't tell a freeze from an active project, so nothing may be relayed.
    Unparseable,
}

impl SimpleIndex {
    /// Build an active-project listing from an adapter whose source protocol
    /// carries files but has no PEP 792 project-status field.
    pub(crate) fn active(files: Vec<SimpleFile>) -> Self {
        Self {
            files,
            project_status: None,
            project_status_unparseable: false,
        }
    }

    /// The upstream verdict with "unparseable" kept distinct from "absent", for
    /// callers that would otherwise act on the difference.
    pub fn upstream_status(&self) -> UpstreamStatus {
        match (&self.project_status, self.project_status_unparseable) {
            (Some(doc), _) => UpstreamStatus::Known(doc.clone()),
            (None, true) => UpstreamStatus::Unparseable,
            (None, false) => UpstreamStatus::Absent,
        }
    }
}

/// Outcome of a conditional listing fetch (see [`fetch_index_conditional`]).
pub enum IndexFetch {
    /// `304`: the source confirms the listing is byte-identical to the ETag we
    /// sent. Nothing to re-parse — and nothing to reconcile.
    NotModified,
    /// `404`: the package isn't on this index.
    NotFound,
    /// `200`: a fresh listing, plus the change tokens to remember for next time.
    Found {
        index: SimpleIndex,
        /// The response ETag, opaque — stored and compared for equality only,
        /// never parsed (PyPI's is a Fastly token).
        etag: Option<String>,
        /// PyPI's `X-PyPI-Last-Serial` (human-readable "moved N→M"); for logs.
        last_serial: Option<u64>,
    },
}

/// The PEP 503 simple-index root for a source `base`. A `--from` that names only
/// the index host (`https://pypi.org`) gets the standard `/simple` appended; a
/// base that already ends in its simple segment is honored verbatim, so a source
/// whose listing lives off the standard path works by naming it in full. That is
/// what lets a migration drain devpi — its per-index listing is
/// `<base>/USER/INDEX/+simple/`, not `/simple/` — as well as Artifactory
/// (`.../api/pypi/<repo>/simple`) and Nexus (`.../repository/<repo>/simple`).
pub(crate) fn simple_root(base: &str) -> String {
    let base = base.trim_end_matches('/');
    match base.rsplit('/').next() {
        Some("simple") | Some("+simple") => base.to_string(),
        _ => format!("{base}/simple"),
    }
}

/// Fetch a package's PEP 691 JSON listing from `base`, optionally conditional
/// on a previously-seen ETag. `if_none_match` rides as `If-None-Match`; a `304`
/// short-circuits with [`IndexFetch::NotModified`] (no body, no parse). The
/// ETag is opaque — only ever compared for equality. `timeout` bounds the whole
/// request for latency-sensitive callers; `None` relies on the client's own
/// timeouts. `auth` attaches basic-auth for an authenticated source (the listing
/// URL is always the source host, and the client follows no redirects, so the
/// credential never travels off-host here).
pub async fn fetch_index_conditional(
    client: &Client,
    base: &str,
    pkg: &str,
    timeout: Option<Duration>,
    if_none_match: Option<&str>,
    auth: Option<(&str, &str)>,
) -> Result<IndexFetch> {
    let url = format!("{}/{pkg}/", simple_root(base));
    let mut req = client
        .get(&url)
        .header(reqwest::header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE);
    if let Some((user, pass)) = auth {
        req = req.basic_auth(user, Some(pass));
    }
    if let Some(t) = timeout {
        req = req.timeout(t);
    }
    if let Some(tag) = if_none_match {
        req = req.header(reqwest::header::IF_NONE_MATCH, tag);
    }
    let resp = req.send().await?;
    match resp.status() {
        reqwest::StatusCode::NOT_MODIFIED => Ok(IndexFetch::NotModified),
        reqwest::StatusCode::NOT_FOUND => Ok(IndexFetch::NotFound),
        _ => {
            let resp = resp.error_for_status()?;
            // Read headers before `.json()` consumes the response.
            let etag = resp
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let last_serial = resp
                .headers()
                .get("x-pypi-last-serial")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok());
            let index = read_index_capped(resp).await?;
            Ok(IndexFetch::Found {
                index,
                etag,
                last_serial,
            })
        }
    }
}

/// A single package's PEP 691 JSON is KBs to a few MB even for the largest
/// projects; cap the body well above that so a hostile or runaway upstream index
/// can't be buffered unbounded into RAM during a sync.
const MAX_INDEX_BYTES: u64 = 64 * 1024 * 1024;

/// Read a PEP 691 JSON listing with a hard size ceiling. `resp.json()` would
/// buffer the whole body with no bound (the timeout caps time, not size).
async fn read_index_capped(resp: reqwest::Response) -> Result<SimpleIndex> {
    if resp
        .content_length()
        .is_some_and(|len| len > MAX_INDEX_BYTES)
    {
        bail!("upstream index exceeds {MAX_INDEX_BYTES} bytes (Content-Length)");
    }
    // Capture the declared type before the body is consumed. Artifactory and
    // Nexus default corporate deployments to the HTML PEP 503 simple API, and a
    // fat-fingered credential yields a 200 HTML login page — neither is JSON, so
    // a bare `serde_json::from_slice` would surface an opaque "expected value at
    // line 1 column 1". We fail closed with the cause and the fix instead.
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let mut buf: Vec<u8> =
        Vec::with_capacity(resp.content_length().map_or(0, |l| l.min(MAX_INDEX_BYTES)) as usize);
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if buf.len() as u64 + chunk.len() as u64 > MAX_INDEX_BYTES {
            bail!("upstream index exceeds {MAX_INDEX_BYTES} bytes");
        }
        buf.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&buf).map_err(|_| non_json_index_error(&content_type, &buf))
}

/// The migration promise is "one command"; the failure mode that breaks it is a
/// source that answers 200 with something other than PEP 691 JSON. Turn that
/// into one actionable line naming the cause (HTML simple index vs. some other
/// non-JSON body) and the fix, never a raw serde error. We do NOT scrape HTML —
/// the deliverable is a clear refusal, so the operator points `--from` at a
/// JSON-capable endpoint (or fixes the credential behind a login page).
fn non_json_index_error(content_type: &str, body: &[u8]) -> anyhow::Error {
    let ct = content_type.split(';').next().unwrap_or("").trim();
    let declared = if ct.is_empty() { "none" } else { ct };
    let looks_html = ct.eq_ignore_ascii_case("text/html")
        || body
            .iter()
            .find(|b| !b.is_ascii_whitespace())
            .is_some_and(|b| *b == b'<');
    if looks_html {
        anyhow::anyhow!(
            "source returned an HTML page (Content-Type: {declared}), not the \
             PEP 691 JSON pypiron migration requires — point --from at a \
             JSON-capable endpoint, or check credentials if this is a login page. \
             Artifactory/Nexus serve HTML PEP 503 by default; pypiron does not \
             scrape HTML."
        )
    } else {
        anyhow::anyhow!(
            "source returned a non-JSON response (Content-Type: {declared}) where \
             pypiron migration requires a PEP 691 JSON simple index — point --from \
             at a JSON-capable endpoint or check credentials."
        )
    }
}

/// Fetch a package's PEP 691 JSON listing from `base`. `Ok(None)` on a 404 —
/// the package isn't on this index. `timeout` bounds the whole request for
/// latency-sensitive callers (the proxy); `None` relies on the client's own
/// timeouts (sync). Unconditional: this never sends `If-None-Match`.
pub async fn fetch_index(
    client: &Client,
    base: &str,
    pkg: &str,
    timeout: Option<Duration>,
) -> Result<Option<SimpleIndex>> {
    match fetch_index_conditional(client, base, pkg, timeout, None, None).await? {
        IndexFetch::Found { index, .. } => Ok(Some(index)),
        IndexFetch::NotFound => Ok(None),
        // We sent no `If-None-Match`, so a 304 is the source misbehaving.
        IndexFetch::NotModified => Err(anyhow::anyhow!(
            "source returned 304 without a conditional request"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_file(json: serde_json::Value) -> SimpleFile {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn simple_root_appends_or_honors_explicit_base() {
        // Host-only source: the standard /simple path is appended.
        assert_eq!(simple_root("https://pypi.org"), "https://pypi.org/simple");
        assert_eq!(simple_root("https://pypi.org/"), "https://pypi.org/simple");
        // An explicit simple base is honored as-is (no double /simple).
        assert_eq!(
            simple_root("https://pypi.org/simple"),
            "https://pypi.org/simple"
        );
        // devpi's per-index +simple listing is honored verbatim, so a migration
        // can name it directly as --from.
        assert_eq!(
            simple_root("http://127.0.0.1:3141/user/dev/+simple"),
            "http://127.0.0.1:3141/user/dev/+simple"
        );
        // Artifactory / Nexus explicit simple bases, and their host-form.
        assert_eq!(
            simple_root("https://art/artifactory/api/pypi/repo/simple/"),
            "https://art/artifactory/api/pypi/repo/simple"
        );
        assert_eq!(
            simple_root("https://nexus/repository/repo"),
            "https://nexus/repository/repo/simple"
        );
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
        let doc = archived.project_status.clone().unwrap();
        assert_eq!(doc.status, ProjectStatus::Archived);
        assert_eq!(doc.reason.as_deref(), Some("moved"));

        assert_eq!(archived.upstream_status(), UpstreamStatus::Known(doc));

        // Absent → None (== active).
        let plain: SimpleIndex =
            serde_json::from_value(serde_json::json!({ "files": [] })).unwrap();
        assert!(plain.project_status.is_none());
        assert_eq!(plain.upstream_status(), UpstreamStatus::Absent);

        // An unknown/foreign marker must NOT fail the whole listing — but it is
        // NOT "absent" either, or relaying it would clear a destination freeze.
        for garbage in [
            serde_json::json!({"status": "hexed"}),
            serde_json::json!("archived"),
            serde_json::json!({}),
        ] {
            let future: SimpleIndex = serde_json::from_value(serde_json::json!({
                "files": [],
                "project-status": garbage
            }))
            .unwrap();
            assert!(future.project_status.is_none());
            assert_eq!(future.upstream_status(), UpstreamStatus::Unparseable);
        }
    }

    #[test]
    fn non_json_index_error_names_html_and_the_fix() {
        // An HTML content-type (Artifactory/Nexus default, or a login page).
        let e = non_json_index_error("text/html; charset=utf-8", b"<!DOCTYPE html>");
        let msg = e.to_string();
        assert!(msg.contains("HTML page"), "{msg}");
        assert!(msg.contains("text/html"), "{msg}");
        assert!(msg.contains("PEP 691 JSON"), "{msg}");
        assert!(msg.contains("--from"), "{msg}");

        // No/opaque content-type but an HTML-looking body still reads as HTML.
        let sniffed = non_json_index_error("application/octet-stream", b"  \n<html><body>");
        assert!(sniffed.to_string().contains("HTML page"), "{sniffed}");

        // A genuinely non-HTML, non-JSON body reports the declared type and the fix.
        let other = non_json_index_error("text/plain", b"not json, not html");
        let msg = other.to_string();
        assert!(msg.contains("non-JSON response"), "{msg}");
        assert!(msg.contains("text/plain"), "{msg}");
        assert!(msg.contains("--from"), "{msg}");

        // A missing content-type is reported as "none", never a raw serde error.
        assert!(non_json_index_error("", b"garbage")
            .to_string()
            .contains("none"));
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
