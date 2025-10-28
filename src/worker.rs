use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use anyhow::Result;
use aws_sdk_s3::{primitives::ByteStream, Client as S3Client};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::render::{
    pep503_global_html, pep503_package_html, pep691_global_json, pep691_package_json,
};
use crate::{
    AppState, PACKAGES_PREFIX, QUEUE_PENDING_PREFIX, QUEUE_PROCESSING_PREFIX, SIMPLE_PREFIX,
};

pub async fn run_worker(state: Arc<AppState>) {
    loop {
        if let Err(e) = tick(&state).await {
            error!(error=?e, "worker tick failed");
        }
        sleep(state.worker_interval).await;
    }
}

async fn tick(state: &AppState) -> Result<()> {
    // 1) list up to batch_size from pending/
    let jobs = list_jobs(
        &state.s3,
        &state.bucket,
        QUEUE_PENDING_PREFIX,
        state.job_batch_size,
    )
    .await?;
    if jobs.is_empty() {
        info!("worker: no jobs");
        return Ok(());
    }

    info!(count = jobs.len(), "worker: claiming jobs");

    // 2) move to processing/
    let mut claimed = Vec::new();
    for key in &jobs {
        let dst = key.replace(QUEUE_PENDING_PREFIX, QUEUE_PROCESSING_PREFIX);
        copy_then_delete(&state.s3, &state.bucket, key, &dst).await?;
        claimed.push(dst);
    }

    // 3) aggregate packages from job content or filename
    let mut touched: HashSet<String> = HashSet::new();
    for key in &claimed {
        match read_job_package(&state.s3, &state.bucket, key).await {
            Ok(pkg) => {
                touched.insert(pkg);
            }
            Err(e) => {
                warn!(error=?e, key=%key, "could not read job; attempting to infer from key");
                if let Some(pkg) = infer_pkg_from_job_key(key) {
                    touched.insert(pkg);
                }
            }
        }
    }

    if touched.is_empty() {
        warn!("worker: no packages inferred; cleaning up claimed jobs");
        delete_keys(&state.s3, &state.bucket, &claimed).await?;
        return Ok(());
    }

    // 4) per-package index regeneration
    for pkg in touched.iter() {
        let files = list_artifacts(&state.s3, &state.bucket, pkg).await?;
        write_pkg_indexes(&state.s3, &state.bucket, pkg, &files).await?;
    }

    // 5) global index regeneration (once per batch)
    let packages = list_all_packages(&state.s3, &state.bucket).await?;
    write_global_indexes(&state.s3, &state.bucket, &packages).await?;

    // 6) cleanup processed jobs
    delete_keys(&state.s3, &state.bucket, &claimed).await?;
    info!(count=?claimed.len(), "worker: jobs processed");
    Ok(())
}

async fn list_jobs(s3: &S3Client, bucket: &str, prefix: &str, limit: usize) -> Result<Vec<String>> {
    let out = s3
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(limit as i32)
        .send()
        .await?;
    let mut keys = Vec::new();
    for o in out.contents() {
        if let Some(k) = o.key() {
            if k.ends_with(".json") {
                keys.push(k.to_string());
            }
        }
    }
    Ok(keys)
}

async fn copy_then_delete(s3: &S3Client, bucket: &str, src: &str, dst: &str) -> Result<()> {
    // copy
    let src_uri = format!("{bucket}/{src}");
    s3.copy_object()
        .bucket(bucket)
        .key(dst)
        .copy_source(src_uri)
        .send()
        .await?;
    // delete src
    s3.delete_object().bucket(bucket).key(src).send().await?;
    Ok(())
}

async fn read_job_package(s3: &S3Client, bucket: &str, key: &str) -> Result<String> {
    let out = s3.get_object().bucket(bucket).key(key).send().await?;
    let data = out.body.collect().await?.into_bytes();
    // Minimal JSON: {"package":"<name>", ...}
    #[derive(serde::Deserialize)]
    struct JobPkg {
        package: String,
    }
    let job: JobPkg = serde_json::from_slice(&data)?;
    Ok(job.package)
}

fn infer_pkg_from_job_key(key: &str) -> Option<String> {
    // .../processing/<epoch>-<package>-<filename>.json
    let fname = key.split('/').next_back()?;
    let mut parts = fname.splitn(3, '-'); // epoch, package, rest
    let _ = parts.next()?;
    let package = parts.next()?.to_string();
    Some(package)
}

async fn list_artifacts(s3: &S3Client, bucket: &str, pkg: &str) -> Result<Vec<String>> {
    let prefix = format!("{PACKAGES_PREFIX}{pkg}/");
    let mut token = None;
    let mut files = BTreeSet::new(); // stable order
    loop {
        let mut req = s3.list_objects_v2().bucket(bucket).prefix(&prefix);
        if let Some(t) = token.take() {
            req = req.continuation_token(t);
        }
        let out = req.send().await?;
        for o in out.contents() {
            if let Some(k) = o.key() {
                if let Some(fname) = k.strip_prefix(&prefix) {
                    if !fname.is_empty() {
                        files.insert(fname.to_string());
                    }
                }
            }
        }
        if out.is_truncated().unwrap_or(false) {
            token = out.next_continuation_token.map(|s| s.to_string());
        } else {
            break;
        }
    }
    Ok(files.into_iter().collect())
}

async fn write_pkg_indexes(s3: &S3Client, bucket: &str, pkg: &str, files: &[String]) -> Result<()> {
    let html = pep503_package_html(pkg, files);
    let json = pep691_package_json(pkg, files);

    // /simple/<pkg>/index.html, index.json
    let base = format!("{SIMPLE_PREFIX}{pkg}/");
    s3.put_object()
        .bucket(bucket)
        .key(format!("{base}index.html"))
        .content_type("text/html; charset=utf-8")
        .body(ByteStream::from(html.into_bytes()))
        .send()
        .await?;
    s3.put_object()
        .bucket(bucket)
        .key(format!("{base}index.json"))
        .content_type("application/json")
        .body(ByteStream::from(json.into_bytes()))
        .send()
        .await?;
    Ok(())
}

async fn list_all_packages(s3: &S3Client, bucket: &str) -> Result<Vec<String>> {
    // Enumerate under /packages/ to get canonical package set
    let prefix = super::PACKAGES_PREFIX.to_string();
    let mut token = None;
    let mut pkgs = BTreeSet::new();
    loop {
        let mut req = s3
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&prefix)
            .delimiter("/");
        if let Some(t) = token.take() {
            req = req.continuation_token(t);
        }
        let out = req.send().await?;
        for cp in out.common_prefixes() {
            if let Some(p) = cp.prefix() {
                if let Some(name) = p.strip_prefix(&prefix).and_then(|s| s.strip_suffix('/')) {
                    pkgs.insert(name.to_string());
                }
            }
        }
        if out.is_truncated().unwrap_or(false) {
            token = out.next_continuation_token.map(|s| s.to_string());
        } else {
            break;
        }
    }
    Ok(pkgs.into_iter().collect())
}

async fn write_global_indexes(s3: &S3Client, bucket: &str, packages: &[String]) -> Result<()> {
    let html = pep503_global_html(packages);
    let json = pep691_global_json(packages);

    s3.put_object()
        .bucket(bucket)
        .key(format!("{SIMPLE_PREFIX}index.html"))
        .content_type("text/html; charset=utf-8")
        .body(ByteStream::from(html.into_bytes()))
        .send()
        .await?;
    s3.put_object()
        .bucket(bucket)
        .key(format!("{SIMPLE_PREFIX}index.json"))
        .content_type("application/json")
        .body(ByteStream::from(json.into_bytes()))
        .send()
        .await?;
    Ok(())
}

async fn delete_keys(s3: &S3Client, bucket: &str, keys: &[String]) -> Result<()> {
    // S3 DeleteObjects could batch; MVP: delete one-by-one for simplicity
    for k in keys {
        let _ = s3.delete_object().bucket(bucket).key(k).send().await;
    }
    Ok(())
}
