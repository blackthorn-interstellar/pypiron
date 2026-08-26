//! Transparent storage observation for per-node bucket selection.
//!
//! Every remote data-plane call delegates to the real [`Storage`] unchanged and
//! reports its result to [`HealthController`]. Successful missing-object and CAS
//! outcomes are healthy: the store answered. Authentication, authorization,
//! precondition, KMS, quota, configuration, and other 4xx failures alarm but do
//! not influence selection. Only timeouts, connection failures, HTTP 408, and
//! HTTP 5xx count as availability failures.
//!
//! `presign_get` is deliberately unobserved. Signing is blind local math, so it
//! carries no evidence that the bucket is reachable.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::response::Response;
use object_store::client::{HttpError, HttpErrorKind};
use object_store::Error as ObjectStoreError;

use crate::bucket_health::{classify, BucketSignal, HealthController, InvalidBucket, SignalClass};
use crate::storage::{is_not_found, FileEntry, ObjectMeta, Storage};

/// A storage handle tagged with its configured bucket index.
pub struct ObservedStorage {
    inner: Arc<dyn Storage>,
    bucket_index: usize,
    health: Arc<HealthController>,
}

impl ObservedStorage {
    pub fn new(
        inner: Arc<dyn Storage>,
        bucket_index: usize,
        health: Arc<HealthController>,
    ) -> std::result::Result<Self, InvalidBucket> {
        health.validate_bucket(bucket_index)?;
        Ok(Self {
            inner,
            bucket_index,
            health,
        })
    }

    fn record<T>(&self, result: &Result<T>) {
        record_result(&self.health, self.bucket_index, result);
    }
}

fn record_result<T>(health: &HealthController, bucket_index: usize, result: &Result<T>) {
    let signal = match result {
        Ok(_) => BucketSignal::Success,
        Err(error) => signal_for_error(error),
    };
    // Construction validates the immutable bucket index against the
    // controller. Observation failure is therefore impossible without a
    // programming error, and must never replace the storage result.
    let _ = health.observe(bucket_index, signal);
}

/// Reduce an anyhow/object-store error to a health signal without guessing.
pub(crate) fn signal_for_error(error: &Error) -> BucketSignal {
    if crate::storage::is_bucket_unavailable(error) {
        return BucketSignal::HttpStatus(503);
    }
    if is_not_found(error) {
        return BucketSignal::Success;
    }

    // A typed missing object is proof that the store answered. Find this before
    // inspecting text: object names are arbitrary and may contain alarm words.
    for cause in error.chain() {
        if let Some(store) = cause.downcast_ref::<ObjectStoreError>() {
            if crate::storage::object_store_is_missing_bucket(store) {
                return BucketSignal::HttpStatus(503);
            }
            if let ObjectStoreError::NotFound { source, .. } = store {
                return if crate::storage::message_is_missing_bucket(&source.to_string()) {
                    BucketSignal::HttpStatus(503)
                } else {
                    BucketSignal::Success
                };
            }
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::NotFound {
                return BucketSignal::Success;
            }
        }
    }

    // Typed availability detection runs before the text scan below. Object keys
    // appear in object_store error Displays, so an outage whose text embeds a
    // name like "quota-tool" or "slowdown-1.0" would otherwise be misread as an
    // (ignored) quota/throttle alarm instead of the availability failure it is.
    // The alarm scan applies only when the type does not prove an outage.
    if let Some(signal) = typed_availability_signal(error) {
        return signal;
    }

    let text = error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if let Some(signal) = semantic_alarm(&text) {
        return signal;
    }

    for cause in error.chain() {
        if let Some(store) = cause.downcast_ref::<ObjectStoreError>() {
            if let Some(signal) = object_store_signal(store) {
                return signal;
            }
        }
        if let Some(request) = cause.downcast_ref::<reqwest::Error>() {
            if request.is_timeout() {
                return BucketSignal::Timeout;
            }
            if request.is_connect() {
                return BucketSignal::ConnectionFailure;
            }
            if let Some(status) = request.status() {
                return signal_for_status(status.as_u16());
            }
        }
        if let Some(http) = cause.downcast_ref::<HttpError>() {
            match http.kind() {
                HttpErrorKind::Timeout => return BucketSignal::Timeout,
                HttpErrorKind::Connect => return BucketSignal::ConnectionFailure,
                _ => {}
            }
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if let Some(signal) = io_signal(io.kind()) {
                return signal;
            }
        }
    }

    status_from_text(&text)
        .map(signal_for_status)
        .unwrap_or(BucketSignal::OtherError)
}

/// Availability failures the error *type* proves — timeouts, connection loss,
/// and 5xx/408 statuses — independent of any object name embedded in the text.
/// Returns `None` for everything else (auth, KMS, quota, config, CAS) so those
/// still fail closed and never move selection.
fn typed_availability_signal(error: &Error) -> Option<BucketSignal> {
    for cause in error.chain() {
        if let Some(request) = cause.downcast_ref::<reqwest::Error>() {
            if request.is_timeout() {
                return Some(BucketSignal::Timeout);
            }
            if request.is_connect() {
                return Some(BucketSignal::ConnectionFailure);
            }
            if let Some(status) = request.status() {
                let signal = signal_for_status(status.as_u16());
                if is_availability_signal(signal) {
                    return Some(signal);
                }
            }
        }
        if let Some(http) = cause.downcast_ref::<HttpError>() {
            match http.kind() {
                HttpErrorKind::Timeout => return Some(BucketSignal::Timeout),
                HttpErrorKind::Connect => return Some(BucketSignal::ConnectionFailure),
                _ => {}
            }
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if let Some(signal) = io_signal(io.kind()) {
                if is_availability_signal(signal) {
                    return Some(signal);
                }
            }
        }
    }
    None
}

fn is_availability_signal(signal: BucketSignal) -> bool {
    matches!(classify(signal), SignalClass::AvailabilityFailure)
}

fn object_store_signal(error: &ObjectStoreError) -> Option<BucketSignal> {
    match error {
        ObjectStoreError::NotFound { source, .. } => Some(
            if crate::storage::message_is_missing_bucket(&source.to_string()) {
                BucketSignal::HttpStatus(503)
            } else {
                BucketSignal::Success
            },
        ),
        ObjectStoreError::PermissionDenied { .. } => Some(BucketSignal::HttpStatus(403)),
        ObjectStoreError::Unauthenticated { .. } => Some(BucketSignal::HttpStatus(401)),
        ObjectStoreError::Precondition { .. } => Some(BucketSignal::HttpStatus(412)),
        ObjectStoreError::AlreadyExists { .. } => Some(BucketSignal::HttpStatus(409)),
        ObjectStoreError::NotModified { .. } => Some(BucketSignal::HttpStatus(304)),
        ObjectStoreError::InvalidPath { .. }
        | ObjectStoreError::NotSupported { .. }
        | ObjectStoreError::NotImplemented { .. }
        | ObjectStoreError::UnknownConfigurationKey { .. } => {
            Some(BucketSignal::ConfigurationError)
        }
        // Generic preserves the network error as its source. Inspect the anyhow
        // chain for HttpError/reqwest/io, then its stable status text below.
        ObjectStoreError::Generic { .. } => None,
        _ => Some(BucketSignal::OtherError),
    }
}

fn io_signal(kind: std::io::ErrorKind) -> Option<BucketSignal> {
    use std::io::ErrorKind;
    match kind {
        ErrorKind::NotFound => Some(BucketSignal::Success),
        ErrorKind::TimedOut => Some(BucketSignal::Timeout),
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe => Some(BucketSignal::ConnectionFailure),
        ErrorKind::PermissionDenied => Some(BucketSignal::HttpStatus(403)),
        ErrorKind::InvalidInput | ErrorKind::InvalidData | ErrorKind::Unsupported => {
            Some(BucketSignal::ConfigurationError)
        }
        _ => None,
    }
}

fn signal_for_status(status: u16) -> BucketSignal {
    if status == 404 {
        BucketSignal::Success
    } else {
        BucketSignal::HttpStatus(status)
    }
}

fn semantic_alarm(text: &str) -> Option<BucketSignal> {
    if contains_any(
        text,
        &[
            "aws kms",
            "kms.",
            "kms:",
            "kms key",
            "kms-key",
            "kms_key",
            "kmsinvalidstate",
            "key management service",
            "keymanagementservice",
            "invalid key id",
            "disabledexception",
        ],
    ) {
        return Some(BucketSignal::KmsError);
    }
    if contains_any(
        text,
        &[
            "quota",
            "throttl",
            "slowdown",
            "too many requests",
            "rate exceeded",
            "limit exceeded",
            // GCS rate limiting (rateLimitExceeded / userRateLimitExceeded) and
            // Azure throttling (ServerBusy) are the S3 SlowDown analog: alarm,
            // but never evidence another bucket is safer, so don't fail over.
            "ratelimitexceeded",
            "serverbusy",
            "server is busy",
        ],
    ) {
        return Some(BucketSignal::QuotaError);
    }
    if contains_any(
        text,
        &[
            "incorrectly configured",
            "invalid endpoint",
            "invalid region",
            "unknown configuration",
            "configuration key",
        ],
    ) {
        return Some(BucketSignal::ConfigurationError);
    }
    if contains_any(
        text,
        &["permission denied", "access denied", "unauthenticated"],
    ) {
        return Some(BucketSignal::HttpStatus(403));
    }
    if text.contains("precondition") {
        return Some(BucketSignal::HttpStatus(412));
    }
    None
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

/// Extract the HTTP code from object_store's stable retry error wording.
fn status_from_text(text: &str) -> Option<u16> {
    for marker in ["status code:", "http status:", "response status:"] {
        let Some((_, rest)) = text.split_once(marker) else {
            continue;
        };
        let digits: String = rest
            .chars()
            .skip_while(|ch| !ch.is_ascii_digit())
            .take_while(char::is_ascii_digit)
            .take(3)
            .collect();
        let status = digits.parse::<u16>().ok()?;
        if (100..=599).contains(&status) {
            return Some(status);
        }
    }
    None
}

#[async_trait]
impl Storage for ObservedStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        let result = self.inner.head_exists(key).await;
        self.record(&result);
        result
    }

    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        let result = self.inner.stored_size(key).await;
        self.record(&result);
        result
    }

    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        let result = self.inner.serve_artifact(key, range).await;
        self.record(&result);
        result
    }

    async fn presign_get(&self, key: &str, expires: Duration) -> Result<Option<String>> {
        // Local HMAC math: no health evidence in either direction.
        self.inner.presign_get(key, expires).await
    }

    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        let result = self.inner.put_bytes(key, bytes, content_type).await;
        self.record(&result);
        result
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let result = self.inner.put_if_absent(key, bytes, content_type).await;
        self.record(&result);
        result
    }

    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &Path,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let result = self.inner.put_file_if_absent(key, path, content_type).await;
        self.record(&result);
        result
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let result = self.inner.get_bytes(key).await;
        self.record(&result);
        result
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        let result = self.inner.list_dir_entries(dir_prefix).await;
        self.record(&result);
        result
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let result = self.inner.list_all(prefix).await;
        self.record(&result);
        result
    }

    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        // Forward to the inner backend so its native paging (S3 start-after) is
        // used; the trait default would route back through our own `list_all`.
        let result = self.inner.list_page(prefix, after, limit).await;
        self.record(&result);
        result
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        let result = self.inner.delete_keys(keys).await;
        self.record(&result);
        result
    }

    fn supports_leases(&self) -> bool {
        // A capability query performs no remote I/O.
        self.inner.supports_leases()
    }

    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let result = self.inner.get_with_etag(key).await;
        self.record(&result);
        result
    }

    async fn head_etag(&self, key: &str) -> Result<Option<String>> {
        let result = self.inner.head_etag(key).await;
        self.record(&result);
        result
    }

    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let result = self.inner.put_if_none_match(key, bytes).await;
        self.record(&result);
        result
    }

    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let result = self.inner.put_if_match(key, etag, bytes).await;
        self.record(&result);
        result
    }

    // The server-side-copy transport is a raw signed HTTP call outside
    // object_store, so it drives neither this availability observer nor the op
    // counter; forward the trait surface unobserved (a copy failure falls back
    // to streaming, which is observed).
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

    async fn store_content_checksum(
        &self,
        key: &str,
        md5_hex: &str,
    ) -> Result<Option<crate::sidecar::StoreChecksum>> {
        self.inner.store_content_checksum(key, md5_hex).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket_health::{HealthPolicy, HealthState};
    use crate::storage::NotFound;

    #[test]
    fn missing_bucket_is_an_outage_not_a_healthy_missing_key() {
        let error = object_error(ObjectStoreError::NotFound {
            path: "probe".to_string(),
            source: source("404 <Code>NoSuchBucket</Code>"),
        });
        assert_eq!(signal_for_error(&error), BucketSignal::HttpStatus(503));
        let list_error = object_error(ObjectStoreError::Generic {
            store: "S3",
            source: source("404 <Code>NoSuchBucket</Code>"),
        });
        assert_eq!(signal_for_error(&list_error), BucketSignal::HttpStatus(503));
        assert_eq!(
            signal_for_error(&Error::new(NotFound("NoSuchBucket".into()))),
            BucketSignal::Success
        );
    }

    fn source(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(std::io::Error::other(message.to_string()))
    }

    fn object_error(error: ObjectStoreError) -> Error {
        Error::new(error)
    }

    struct PresignOnlyStorage;

    #[async_trait]
    impl Storage for PresignOnlyStorage {
        async fn head_exists(&self, _key: &str) -> Result<bool> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn serve_artifact(&self, _key: &str, _range: Option<&str>) -> Result<Response<Body>> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn presign_get(&self, _key: &str, _expires: Duration) -> Result<Option<String>> {
            Err(Error::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "local signer failed",
            )))
        }

        async fn put_bytes(
            &self,
            _key: &str,
            _bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<()> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn put_if_absent(
            &self,
            _key: &str,
            _bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bool> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn put_file_if_absent(
            &self,
            _key: &str,
            _path: &Path,
            _content_type: Option<&str>,
        ) -> Result<bool> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn get_bytes(&self, _key: &str) -> Result<Vec<u8>> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn list_dir_entries(&self, _dir_prefix: &str) -> Result<Vec<FileEntry>> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn list_all(&self, _prefix: &str) -> Result<Vec<ObjectMeta>> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn delete_keys(&self, _keys: &[String]) -> Result<()> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn get_with_etag(&self, _key: &str) -> Result<Option<(Vec<u8>, String)>> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn put_if_none_match(&self, _key: &str, _bytes: Vec<u8>) -> Result<Option<String>> {
            Err(anyhow::anyhow!("unused"))
        }

        async fn put_if_match(
            &self,
            _key: &str,
            _etag: &str,
            _bytes: Vec<u8>,
        ) -> Result<Option<String>> {
            Err(anyhow::anyhow!("unused"))
        }
    }

    #[test]
    fn missing_objects_are_healthy() {
        let error = Error::new(NotFound("packages/example/missing.whl".to_string()));
        assert_eq!(signal_for_error(&error), BucketSignal::Success);

        let error = object_error(ObjectStoreError::NotFound {
            path: "missing".to_string(),
            source: source("404"),
        });
        assert_eq!(signal_for_error(&error), BucketSignal::Success);
    }

    #[test]
    fn auth_and_precondition_errors_alarm_without_affecting_availability() {
        let cases = [
            (
                ObjectStoreError::PermissionDenied {
                    path: "key".to_string(),
                    source: source("denied"),
                },
                BucketSignal::HttpStatus(403),
            ),
            (
                ObjectStoreError::Unauthenticated {
                    path: "key".to_string(),
                    source: source("bad credentials"),
                },
                BucketSignal::HttpStatus(401),
            ),
            (
                ObjectStoreError::Precondition {
                    path: "key".to_string(),
                    source: source("etag changed"),
                },
                BucketSignal::HttpStatus(412),
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(signal_for_error(&object_error(error)), expected);
        }
    }

    #[test]
    fn only_network_availability_errors_drive_failover() {
        for (message, expected) in [
            (
                "Server returned non-2xx status code: 408 Request Timeout",
                BucketSignal::HttpStatus(408),
            ),
            (
                "Server returned non-2xx status code: 503 Service Unavailable",
                BucketSignal::HttpStatus(503),
            ),
            (
                "Server returned non-2xx status code: 429 Too Many Requests",
                BucketSignal::QuotaError,
            ),
        ] {
            let error = object_error(ObjectStoreError::Generic {
                store: "S3",
                source: source(message),
            });
            assert_eq!(signal_for_error(&error), expected);
        }

        let timeout = object_error(ObjectStoreError::Generic {
            store: "S3",
            source: Box::new(HttpError::new(
                HttpErrorKind::Timeout,
                std::io::Error::new(std::io::ErrorKind::TimedOut, "late"),
            )),
        });
        assert_eq!(signal_for_error(&timeout), BucketSignal::Timeout);

        let connect = object_error(ObjectStoreError::Generic {
            store: "S3",
            source: Box::new(HttpError::new(
                HttpErrorKind::Connect,
                std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "down"),
            )),
        });
        assert_eq!(signal_for_error(&connect), BucketSignal::ConnectionFailure);
    }

    #[test]
    fn a_typed_timeout_stays_an_outage_even_when_its_text_names_a_quota() {
        // Object keys ride along in error Displays. A genuine timeout whose text
        // embeds "quota" (e.g. a package named quota-tool) must classify by type
        // as an availability failure, not as an ignored quota/throttle alarm.
        let timeout = object_error(ObjectStoreError::Generic {
            store: "S3",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "GET quota-tool/quota-tool-1.0.whl timed out",
            )),
        });
        let signal = signal_for_error(&timeout);
        assert_eq!(signal, BucketSignal::Timeout);
        assert_eq!(classify(signal), SignalClass::AvailabilityFailure);
    }

    #[test]
    fn semantic_service_failures_override_an_http_5xx() {
        for (message, expected) in [
            ("503: KMS key is disabled", BucketSignal::KmsError),
            (
                "503 SlowDown: request rate exceeded",
                BucketSignal::QuotaError,
            ),
            (
                "503: incorrectly configured region",
                BucketSignal::ConfigurationError,
            ),
        ] {
            let error = object_error(ObjectStoreError::Generic {
                store: "S3",
                source: source(message),
            });
            assert_eq!(signal_for_error(&error), expected);
        }
    }

    #[test]
    fn http_404_is_healthy_but_other_4xx_alarm() {
        for (status, expected) in [
            (404, BucketSignal::Success),
            (400, BucketSignal::HttpStatus(400)),
            (409, BucketSignal::HttpStatus(409)),
            (412, BucketSignal::HttpStatus(412)),
        ] {
            let error = object_error(ObjectStoreError::Generic {
                store: "S3",
                source: source(&format!("Server returned non-2xx status code: {status}")),
            });
            assert_eq!(signal_for_error(&error), expected);
        }
    }

    #[test]
    fn successful_false_and_none_cas_results_are_healthy() {
        let controller =
            HealthController::new(2, HealthPolicy::new(1, Duration::from_secs(60)).unwrap())
                .unwrap();

        let cas_false: Result<bool> = Ok(false);
        record_result(&controller, 1, &cas_false);
        let first = controller.worker_tick();
        assert_eq!(first.states[1], HealthState::Healthy);

        let unavailable = object_error(ObjectStoreError::Generic {
            store: "S3",
            source: source("Server returned non-2xx status code: 503"),
        });
        let failed: Result<()> = Err(unavailable);
        record_result(&controller, 1, &failed);
        assert_eq!(controller.worker_tick().states[1], HealthState::Unhealthy);

        let cas_none: Result<Option<String>> = Ok(None);
        record_result(&controller, 1, &cas_none);
        assert_eq!(controller.worker_tick().states[1], HealthState::Healthy);
    }

    #[tokio::test]
    async fn local_presigning_never_produces_a_health_observation() {
        let controller = Arc::new(
            HealthController::new(2, HealthPolicy::new(1, Duration::from_secs(60)).unwrap())
                .unwrap(),
        );
        let observed =
            ObservedStorage::new(Arc::new(PresignOnlyStorage), 1, controller.clone()).unwrap();

        assert!(observed
            .presign_get("packages/example/example.whl", Duration::from_secs(60))
            .await
            .is_err());
        let snapshot = controller.worker_tick();
        assert_eq!(snapshot.states[1], HealthState::Unknown);
        assert_eq!(snapshot.alarms, vec![0, 0]);
        assert!(snapshot.topology_revalidation.is_empty());
    }
}
