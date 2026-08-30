//! Pypicloud's non-standard package API, adapted into sync's existing file
//! pipeline. Patterns choose names; this module only reads stored records.

use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use reqwest::{Client, Response, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tracing::warn;

use super::{PrivatePattern, SourceAuth};
use crate::names::checked_pkg_name;
use crate::sidecar::Yanked;
use crate::simple::{IndexFetch, SimpleFile, SimpleIndex};

const MAX_API_BODY_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Deserialize)]
struct PackageNames {
    packages: Vec<String>,
}

#[derive(Deserialize)]
struct PackageRecords {
    packages: Vec<PackageRecord>,
}

#[derive(Deserialize)]
struct PackageRecord {
    name: String,
    filename: String,
    #[serde(default)]
    metadata: Option<PackageMetadata>,
}

#[derive(Default, Deserialize)]
struct PackageMetadata {
    #[serde(default, alias = "hash-sha256")]
    hash_sha256: Option<String>,
    #[serde(default, alias = "requires-python")]
    requires_python: Option<String>,
    #[serde(default)]
    uploader: Option<String>,
}

fn api_url(base: &str, tail: &[&str]) -> Result<Url> {
    let mut url = Url::parse(base).with_context(|| format!("invalid pypicloud URL '{base}'"))?;
    url.set_query(None);
    url.set_fragment(None);
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| anyhow!("pypicloud URL cannot be a base: '{base}'"))?;
        segments.pop_if_empty();
        segments.push("api").push("package");
        for part in tail {
            segments.push(part);
        }
    }
    Ok(url)
}

fn listing_url(base: &str, tail: &[&str]) -> Result<Url> {
    let mut url = api_url(base, tail)?;
    let path = format!("{}/", url.path().trim_end_matches('/'));
    url.set_path(&path);
    Ok(url)
}

fn with_auth(req: reqwest::RequestBuilder, auth: Option<&SourceAuth>) -> reqwest::RequestBuilder {
    match auth {
        Some(auth) => req.basic_auth(&auth.user, Some(&auth.pass)),
        None => req,
    }
}

async fn parse_json<T: DeserializeOwned>(resp: Response, action: &str) -> Result<T> {
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_else(|_| "<no body>".into());
        bail!("{action} [{status}]: {body}");
    }
    if resp
        .content_length()
        .is_some_and(|length| length > MAX_API_BODY_BYTES)
    {
        bail!("{action}: response exceeds {MAX_API_BODY_BYTES} bytes");
    }
    let mut bytes = Vec::with_capacity(
        resp.content_length()
            .map_or(0, |length| length.min(MAX_API_BODY_BYTES)) as usize,
    );
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len() as u64 + chunk.len() as u64 > MAX_API_BODY_BYTES {
            bail!("{action}: response exceeds {MAX_API_BODY_BYTES} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "{action}: source did not return pypicloud package JSON (check --source-kind and credentials)"
        )
    })
}

pub(super) async fn discover_packages(
    client: &Client,
    base: &str,
    auth: Option<&SourceAuth>,
    patterns: &[PrivatePattern],
) -> Result<Vec<String>> {
    let url = listing_url(base, &[])?;
    let response = with_auth(client.get(url), auth).send().await?;
    let listing: PackageNames = parse_json(response, "pypicloud project discovery").await?;
    let mut matched = Vec::new();
    for raw in listing.packages {
        let Some(name) = checked_pkg_name(&raw) else {
            warn!(project = %raw, "pypicloud returned an invalid project name; skipping");
            continue;
        };
        if patterns.iter().any(|pattern| pattern.matches(&name)) {
            matched.push(name);
        }
    }
    matched.sort();
    matched.dedup();
    Ok(matched)
}

pub(super) async fn fetch_index(
    client: &Client,
    base: &str,
    package: &str,
    if_none_match: Option<&str>,
    auth: Option<&SourceAuth>,
) -> Result<IndexFetch> {
    let url = listing_url(base, &[package])?;
    let mut request = with_auth(client.get(url), auth);
    if let Some(etag) = if_none_match {
        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
    }
    let response = request.send().await?;
    match response.status() {
        StatusCode::NOT_MODIFIED => return Ok(IndexFetch::NotModified),
        StatusCode::NOT_FOUND => return Ok(IndexFetch::NotFound),
        _ => {}
    }
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let records: PackageRecords = parse_json(response, "pypicloud package listing").await?;
    let mut marked = 0usize;
    let mut unmarked = 0usize;
    let mut files = Vec::with_capacity(records.packages.len());
    for record in records.packages {
        let record_name = checked_pkg_name(&record.name)
            .ok_or_else(|| anyhow!("pypicloud returned invalid package name '{}'", record.name))?;
        if record_name != package {
            bail!(
                "pypicloud returned package '{record_name}' while listing '{package}'; refusing to cross package ownership"
            );
        }
        let metadata = record.metadata.unwrap_or_default();
        if metadata
            .uploader
            .as_deref()
            .is_some_and(|uploader| !uploader.trim().is_empty())
        {
            marked += 1;
        } else {
            unmarked += 1;
        }
        let mut hashes = HashMap::new();
        if let Some(digest) = metadata
            .hash_sha256
            .filter(|digest| !digest.trim().is_empty())
        {
            hashes.insert("sha256".to_string(), digest);
        }
        files.push(SimpleFile {
            url: api_url(base, &[package, &record.filename])?.to_string(),
            filename: record.filename,
            hashes,
            size: None,
            upload_time: None,
            requires_python: metadata.requires_python,
            yanked: Yanked::default(),
            core_metadata: None,
            dist_info_metadata: None,
            provenance: None,
        });
    }
    if unmarked > 0 {
        warn!(
            package,
            uploader_tagged = marked,
            unmarked,
            "pypicloud package has artifacts without uploader metadata; migrating them because the private package list is authoritative"
        );
    }
    Ok(IndexFetch::Found {
        index: SimpleIndex::active(files),
        etag,
        last_serial: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_urls_preserve_a_base_path_and_escape_filenames() {
        let url = api_url(
            "https://packages.example/root/",
            &["acme-tool", "acme_tool-1.0+local.whl"],
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://packages.example/root/api/package/acme-tool/acme_tool-1.0+local.whl"
        );
    }
}
