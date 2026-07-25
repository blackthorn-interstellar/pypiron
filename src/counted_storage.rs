//! Storage-op accounting for `/metrics`.
//!
//! Wraps a [`Storage`] and bumps `pypiron_storage_ops_total{op=...}` once per
//! trait call, then delegates unchanged. Counting sits at the trait boundary —
//! it answers "how many backend requests does serving this route cost" (the
//! S3 bill, the per-endpoint budget the micro-benchmarks pin), not how many
//! wire round-trips a backend needed internally (a paged `list_all` is one
//! op). `presign_get` is deliberately uncounted: signing is blind local math,
//! the same reasoning as [`crate::observed_storage`].
//!
//! Every trait method is overridden, including the defaulted ones — inheriting
//! a default here would re-enter the wrapper (e.g. `stored_size` counting a
//! list instead of delegating to the backend's single HEAD).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::body::Body;
use axum::response::Response;

use crate::metrics::{Metrics, StorageOp};
use crate::storage::{FileEntry, ObjectMeta, Storage};

pub struct CountedStorage {
    inner: Arc<dyn Storage>,
    metrics: Arc<Metrics>,
}

impl CountedStorage {
    pub fn new(inner: Arc<dyn Storage>, metrics: Arc<Metrics>) -> Self {
        Self { inner, metrics }
    }

    fn count(&self, op: StorageOp) {
        self.metrics.record_storage_op(op);
    }
}

#[async_trait::async_trait]
impl Storage for CountedStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        self.count(StorageOp::Read);
        self.inner.head_exists(key).await
    }

    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        self.count(StorageOp::Read);
        self.inner.stored_size(key).await
    }

    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        self.count(StorageOp::Read);
        self.inner.serve_artifact(key, range).await
    }

    async fn presign_get(&self, key: &str, expires: Duration) -> Result<Option<String>> {
        self.inner.presign_get(key, expires).await
    }

    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        self.count(StorageOp::Write);
        self.inner.put_bytes(key, bytes, content_type).await
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool> {
        self.count(StorageOp::Write);
        self.inner.put_if_absent(key, bytes, content_type).await
    }

    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<bool> {
        self.count(StorageOp::Write);
        self.inner.put_file_if_absent(key, path, content_type).await
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.count(StorageOp::Read);
        self.inner.get_bytes(key).await
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        self.count(StorageOp::List);
        self.inner.list_dir_entries(dir_prefix).await
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.count(StorageOp::List);
        self.inner.list_all(prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        self.count(StorageOp::List);
        self.inner.list_page(prefix, after, limit).await
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        self.count(StorageOp::Delete);
        self.inner.delete_keys(keys).await
    }

    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }

    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.count(StorageOp::Read);
        self.inner.get_with_etag(key).await
    }

    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        self.count(StorageOp::Write);
        self.inner.put_if_none_match(key, bytes).await
    }

    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        self.count(StorageOp::Write);
        self.inner.put_if_match(key, etag, bytes).await
    }

    // A server-side copy is a raw signed HTTP call, not an object_store op, so it
    // is deliberately *not* counted here — that absence is exactly what lets the
    // microbench op-count fall when the copy transport replaces a GET+PUT.
    fn copy_origin(&self) -> Option<crate::storage::CopyOrigin> {
        self.inner.copy_origin()
    }

    async fn copy_credential_identity(&self) -> Result<Option<String>> {
        self.inner.copy_credential_identity().await
    }

    async fn server_side_copy(
        &self,
        src: &crate::storage::CopyOrigin,
        src_key: &str,
        dst_key: &str,
        expected_size: u64,
    ) -> Result<crate::storage::CopyOutcome> {
        self.inner
            .server_side_copy(src, src_key, dst_key, expected_size)
            .await
    }
}
