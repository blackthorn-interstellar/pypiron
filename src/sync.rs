//! Mirror packages from PyPI into this registry.
//!
//! Default mode writes straight to storage with the same code the server
//! uses: artifact, then a sidecar carrying PyPI's true `upload-time` and
//! sha256 (no re-hashing), then a dirty marker. Historical timestamps are
//! gated on storage credentials by construction — the HTTP upload API never
//! accepts one. `--to <url>` keeps the legacy HTTP mode (POST to a remote
//! `/legacy/`), with the accepted limitation that timestamps become mirror
//! time.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use futures::stream::{self, StreamExt};
use reqwest::{multipart, Client};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tracing::{error, info, warn};

use crate::names::{matches_prefix, normalize_pkg_name};
use crate::origin;
use crate::sidecar::{metadata_key, sidecar_key, Sidecar, Yanked};
use crate::storage::{Storage, StorageArgs};
use crate::worker::mark_dirty;
use crate::PACKAGES_PREFIX;

#[derive(Debug, Clone, Args)]
pub struct SyncArgs {
    /// Path to a text file containing packages to mirror (one per line). Only names are allowed (no versions).
    #[arg(long, env = "PYPIRON_PACKAGES_LIST")]
    pub packages_list: PathBuf,

    /// Source PyPI base (default: https://pypi.org). We call /pypi/<name>/json here.
    #[arg(
        long = "from",
        env = "PYPIRON_SYNC_FROM",
        default_value = "https://pypi.org"
    )]
    pub src_base: String,

    /// Destination PypIron base URL for HTTP mode (POSTs to <to>/legacy/).
    /// Omit to write directly to storage (the default, preserves timestamps).
    #[arg(long = "to", env = "PYPIRON_SYNC_TO")]
    pub dst_base: Option<String>,

    /// Basic auth username for the HTTP-mode destination (optional).
    #[arg(long, env = "PYPIRON_SYNC_USERNAME")]
    pub username: Option<String>,

    /// Basic auth password for the HTTP-mode destination (optional).
    #[arg(long, env = "PYPIRON_SYNC_PASSWORD")]
    pub password: Option<String>,

    /// Refuse to mirror names inside this private namespace (PEP 503-normalized)
    #[arg(long, env = "PYPIRON_PRIVATE_PREFIX")]
    pub private_prefix: Option<String>,

    /// Parallel downloads/uploads.
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,

    /// Print actions without downloading/uploading.
    #[arg(long)]
    pub dry_run: bool,

    /// Filtering flags (wheel/python/abi/platform).
    #[command(flatten)]
    pub filter: FilterArgs,

    /// Storage configuration for direct mode (same flags as `serve`).
    #[command(flatten)]
    pub storage: StorageArgs,
}

#[derive(Debug, Clone, Args)]
pub struct FilterArgs {
    /// Only mirror wheel files (.whl)
    #[arg(long)]
    pub only_wheels: bool,

    /// Only mirror source distributions (sdist)
    #[arg(long)]
    pub only_sdists: bool,

    /// Include wheels whose python tag matches any of these (e.g. py3, cp311). Comma-separated or repeatable.
    #[arg(long, value_delimiter = ',', value_name = "TAG")]
    pub python_tag: Vec<String>,

    /// Include wheels whose ABI tag matches any of these (e.g. none, cp311). Comma-separated or repeatable.
    #[arg(long, value_delimiter = ',', value_name = "TAG")]
    pub abi_tag: Vec<String>,

    /// Include wheels whose platform tag matches any of these (e.g. any, manylinux2014_x86_64, macosx_*_arm64, win_amd64). Supports '*' wildcard.
    #[arg(long, value_delimiter = ',', value_name = "TAG")]
    pub platform_tag: Vec<String>,

    /// Exclude wheels whose platform tag matches any of these (supports '*' wildcard).
    #[arg(long, value_delimiter = ',', value_name = "TAG")]
    pub exclude_platform_tag: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PyPiJson {
    releases: HashMap<String, Vec<PyPiFile>>,
}

#[derive(Debug, Clone, Deserialize)]
struct PyPiFile {
    filename: String,
    url: String,
    digests: PyPiDigests,
    #[serde(default)]
    packagetype: Option<String>,
    #[serde(default)]
    upload_time_iso_8601: Option<String>,
    #[serde(default)]
    requires_python: Option<String>,
    #[serde(default)]
    yanked: bool,
    #[serde(default)]
    yanked_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct PyPiDigests {
    sha256: String,
}

/// A file selected for mirroring, with its release version.
struct Selected {
    version: String,
    file: PyPiFile,
}

enum Sink {
    Storage(std::sync::Arc<dyn Storage>),
    Http(String),
}

pub async fn run_sync(args: SyncArgs) -> Result<()> {
    let client = Client::builder()
        .user_agent("pypiron-sync/0.1 (+https://github.com/brycedrennan/pypiron)")
        .build()?;

    let packages = read_package_names(&args.packages_list).await?;
    if packages.is_empty() {
        return Err(anyhow!(
            "No packages found in {}",
            args.packages_list.display()
        ));
    }

    let sink = match &args.dst_base {
        Some(dst) => {
            let endpoint = normalize_legacy_endpoint(dst);
            info!("HTTP mode: uploading to {endpoint}");
            Sink::Http(endpoint)
        }
        None => {
            info!("direct-storage mode");
            Sink::Storage(args.storage.build().await?)
        }
    };

    let mut failures = 0usize;
    for pkg in packages {
        if let Err(e) = sync_one_package(&client, &args, &sink, &pkg).await {
            error!(package=%pkg, error=?e, "package sync failed");
            failures += 1;
        }
    }
    if failures > 0 {
        bail!("{failures} package(s) failed to sync");
    }
    Ok(())
}

async fn sync_one_package(client: &Client, args: &SyncArgs, sink: &Sink, pkg: &str) -> Result<()> {
    // Policy gates come before any network traffic.
    if let Some(prefix) = &args.private_prefix {
        let prefix = normalize_pkg_name(prefix);
        if matches_prefix(pkg, &prefix) {
            bail!("'{pkg}' is inside the private namespace '{prefix}'; refusing to mirror");
        }
    }
    if let Sink::Storage(storage) = sink {
        match origin::read_origin(storage.as_ref(), pkg).await.as_deref() {
            Some(origin::MIRROR) | None => {}
            Some(other) => {
                bail!("'{pkg}' is {other}-owned; refusing to mirror over it")
            }
        }
    }

    let selected = fetch_selected_files(client, args, pkg).await?;
    info!("Syncing {pkg} ({} matching files selected)", selected.len());

    if args.dry_run {
        for s in &selected {
            println!("[dry-run] would copy {} ({})", s.file.filename, s.file.url);
        }
        return Ok(());
    }

    if let Sink::Storage(storage) = sink {
        // Claim only when actually writing.
        if origin::read_origin(storage.as_ref(), pkg).await.is_none() {
            origin::claim_origin(storage.as_ref(), pkg, origin::MIRROR).await?;
        }
    }

    let results: Vec<Result<bool>> = stream::iter(selected)
        .map(|s| async move {
            match sink {
                Sink::Storage(storage) => {
                    mirror_to_storage(client, storage.as_ref(), pkg, &s).await
                }
                Sink::Http(endpoint) => upload_via_http(client, args, endpoint, &s).await,
            }
        })
        .buffer_unordered(args.concurrency.max(1))
        .collect()
        .await;

    let mut wrote = false;
    let mut errors = 0usize;
    for r in &results {
        match r {
            Ok(true) => wrote = true,
            Ok(false) => {}
            Err(e) => {
                error!(package=%pkg, error=?e, "file failed");
                errors += 1;
            }
        }
    }

    if wrote {
        if let Sink::Storage(storage) = sink {
            mark_dirty(storage.as_ref(), pkg).await?;
        }
    }
    if errors > 0 {
        bail!("{errors} file(s) failed for '{pkg}'");
    }
    Ok(())
}

async fn fetch_selected_files(
    client: &Client,
    args: &SyncArgs,
    pkg: &str,
) -> Result<Vec<Selected>> {
    let url = format!("{}/pypi/{}/json", args.src_base.trim_end_matches('/'), pkg);
    let resp = client.get(url).send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(anyhow!("Package not found on source: {pkg}"));
    }
    let json: PyPiJson = resp.error_for_status()?.json().await?;

    let mut selected = Vec::new();
    for (version, files) in json.releases {
        for file in files {
            if matches_filters(&file, &args.filter) {
                selected.push(Selected {
                    version: version.clone(),
                    file,
                });
            }
        }
    }
    if selected.is_empty() {
        warn!("No matching files for package '{pkg}'");
    }
    Ok(selected)
}

/// Mirror one file into storage. Returns false if it was already present.
/// Writes follow the ordering invariant: artifact, metadata, sidecar; the
/// caller drops the package's dirty marker after the batch.
async fn mirror_to_storage(
    client: &Client,
    storage: &dyn Storage,
    pkg: &str,
    s: &Selected,
) -> Result<bool> {
    let key = format!("{PACKAGES_PREFIX}{pkg}/{}", s.file.filename);
    if storage.head_exists(&key).await.unwrap_or(false) {
        return Ok(false);
    }

    info!("  - mirroring {}", s.file.filename);
    let bytes = download_verified(client, &s.file).await?;

    storage
        .put_bytes(&key, bytes.clone(), Some("application/octet-stream"))
        .await?;

    // Best-effort PEP 658: PyPI serves metadata at <file-url>.metadata.
    if s.file.filename.ends_with(".whl") {
        if let Ok(resp) = client.get(format!("{}.metadata", s.file.url)).send().await {
            if resp.status().is_success() {
                if let Ok(md) = resp.bytes().await {
                    let _ = storage
                        .put_bytes(
                            &metadata_key(&key),
                            md.to_vec(),
                            Some("text/plain; charset=utf-8"),
                        )
                        .await;
                }
            }
        }
    }

    let yanked = match (&s.file.yanked_reason, s.file.yanked) {
        (Some(reason), _) if !reason.trim().is_empty() => Yanked::Reason(reason.trim().into()),
        (_, flag) => Yanked::Flag(flag),
    };
    let sc = Sidecar {
        // PyPI's digest, verified against the downloaded bytes — not re-derived.
        sha256: s.file.digests.sha256.clone(),
        size: bytes.len() as u64,
        version: s.version.clone(),
        // PyPI's true upload time: the whole point of direct-storage sync.
        upload_time: s.file.upload_time_iso_8601.clone().unwrap_or_default(),
        requires_python: s.file.requires_python.clone(),
        yanked,
    };
    storage
        .put_bytes(
            &sidecar_key(&key),
            serde_json::to_vec(&sc)?,
            Some("application/json"),
        )
        .await?;
    Ok(true)
}

async fn download_verified(client: &Client, file: &PyPiFile) -> Result<Vec<u8>> {
    let resp = client.get(&file.url).send().await?.error_for_status()?;
    let bytes = resp.bytes().await?.to_vec();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let got = format!("{:x}", hasher.finalize());
    if !got.eq_ignore_ascii_case(&file.digests.sha256) {
        bail!(
            "sha256 mismatch for {} (expected {}, got {got})",
            file.filename,
            file.digests.sha256
        );
    }
    Ok(bytes)
}

/// HTTP mode: push through a remote `/legacy/` with full metadata fields.
/// Returns true on success (the remote may 409 on existing files — treated
/// as already-present, false).
async fn upload_via_http(
    client: &Client,
    args: &SyncArgs,
    endpoint: &str,
    s: &Selected,
) -> Result<bool> {
    let bytes = download_verified(client, &s.file).await?;

    info!("  - uploading {}", s.file.filename);
    let part = multipart::Part::bytes(bytes)
        .file_name(s.file.filename.clone())
        .mime_str("application/octet-stream")?;

    let mut form = multipart::Form::new()
        .text(":action", "file_upload")
        .text("protocol_version", "1")
        .text(
            "name",
            s.file.filename.split('-').next().unwrap_or("").to_string(),
        )
        .text("version", s.version.clone())
        .text("sha256_digest", s.file.digests.sha256.clone())
        .part("content", part);
    if let Some(rp) = &s.file.requires_python {
        form = form.text("requires_python", rp.clone());
    }

    let mut req = client.post(endpoint).multipart(form);
    if let (Some(u), Some(p)) = (args.username.as_ref(), args.password.as_ref()) {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await?;
    if resp.status() == reqwest::StatusCode::CONFLICT {
        return Ok(false);
    }
    if !resp.status().is_success() {
        let code = resp.status();
        let body = resp.text().await.unwrap_or_else(|_| "<no body>".into());
        bail!("upload failed [{code}]: {body}");
    }
    Ok(true)
}

fn matches_filters(file: &PyPiFile, f: &FilterArgs) -> bool {
    let fname = file.filename.to_ascii_lowercase();
    let is_wheel =
        fname.ends_with(".whl") || matches!(file.packagetype.as_deref(), Some("bdist_wheel"));

    if f.only_wheels && !is_wheel {
        return false;
    }
    if f.only_sdists && is_wheel {
        return false;
    }

    let has_tag_filters = !(f.python_tag.is_empty()
        && f.abi_tag.is_empty()
        && f.platform_tag.is_empty()
        && f.exclude_platform_tag.is_empty());

    if !is_wheel {
        // Non-wheel (e.g., sdist). If tag filters provided, sdists won't match.
        return !has_tag_filters;
    }

    let tags = match parse_wheel_tags(&file.filename) {
        Some(t) => t,
        None => {
            warn!(filename=%file.filename, "Could not parse wheel tags; skipping if filters present");
            return !has_tag_filters;
        }
    };

    // Exclusions first
    if !f.exclude_platform_tag.is_empty()
        && tokens_match_any(&tags.platform, &f.exclude_platform_tag)
    {
        return false;
    }

    if !f.python_tag.is_empty() && !tokens_match_any(&tags.python, &f.python_tag) {
        return false;
    }
    if !f.abi_tag.is_empty() && !tokens_match_any(&tags.abi, &f.abi_tag) {
        return false;
    }
    if !f.platform_tag.is_empty() && !tokens_match_any(&tags.platform, &f.platform_tag) {
        return false;
    }

    true
}

#[derive(Debug, Clone)]
struct WheelTags {
    python: Vec<String>,
    abi: Vec<String>,
    platform: Vec<String>,
}

fn tokens_match_any(tokens: &[String], filters: &[String]) -> bool {
    let tokens_lc: Vec<String> = tokens.iter().map(|t| t.to_ascii_lowercase()).collect();
    for f in filters {
        let pat = f.to_ascii_lowercase();
        for t in &tokens_lc {
            if tag_matches(t, &pat) {
                return true;
            }
        }
    }
    false
}

/// Supports exact match or glob-like '*' anywhere in the filter (matches ordered substrings).
fn tag_matches(tag: &str, filter: &str) -> bool {
    if filter == "*" {
        return true;
    }
    if !filter.contains('*') {
        return tag == filter;
    }
    glob_like_contains(tag, filter)
}

/// Simple glob-like matching: '*' matches any substring, parts must appear in order.
fn glob_like_contains(haystack: &str, pattern: &str) -> bool {
    let mut rest = haystack;
    for part in pattern.split('*') {
        if part.is_empty() {
            continue;
        }
        if let Some(idx) = rest.find(part) {
            rest = &rest[idx + part.len()..];
        } else {
            return false;
        }
    }
    true
}

/// Parse wheel filename into (python, abi, platform) tags.
/// Uses the last 3 dash-separated fields before ".whl" per PEP 427.
fn parse_wheel_tags(filename: &str) -> Option<WheelTags> {
    if !filename.ends_with(".whl") {
        return None;
    }
    let stem = filename.strip_suffix(".whl")?;
    let parts: Vec<&str> = stem.split('-').collect();
    if parts.len() < 5 {
        // name, version, [build?], py, abi, platform  -> min 5 fields (without build)
        return None;
    }
    let py = parts[parts.len() - 3]
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let abi = parts[parts.len() - 2]
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let plat = parts[parts.len() - 1]
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    Some(WheelTags {
        python: py,
        abi,
        platform: plat,
    })
}

async fn read_package_names(path: &PathBuf) -> Result<Vec<String>> {
    let text = fs::read_to_string(path)
        .await
        .with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.contains("==") {
            bail!(
                "line {}: versions are not allowed in the packages list (found '{line}'). \
                 Put only the package name (one per line) to mirror ALL releases.",
                lineno + 1
            );
        }
        out.push(normalize_pkg_name(line));
    }
    Ok(out)
}

fn normalize_legacy_endpoint(dst_base: &str) -> String {
    let trimmed = dst_base.trim_end_matches('/');
    if trimmed.ends_with("/legacy") {
        format!("{trimmed}/")
    } else {
        format!("{trimmed}/legacy/")
    }
}
