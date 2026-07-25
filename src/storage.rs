use anyhow::{anyhow, bail, Context as _, Result};
use async_trait::async_trait;
use axum::body::Body;
use clap::Args as ClapArgs;
use http::{header, Response, StatusCode};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

// Cloud object-store deps: S3, GCS, and Azure Blob behind one API. Disk is a
// separate, dependency-free backend; everything remote shares one impl.
use futures::StreamExt as _;
use object_store::aws::{
    AmazonS3Builder, AmazonS3ConfigKey, AwsCredential, AwsCredentialProvider, S3CopyIfNotExists,
};
use object_store::azure::{
    AzureConfigKey, AzureCredential, AzureCredentialProvider, MicrosoftAzureBuilder,
};
use object_store::client::ClientConfigKey;
use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::path::Path as OsPath;
use object_store::signer::Signer;
use object_store::{
    Attribute, Attributes, Error as OsError, GetOptions, GetRange, ObjectStore, ObjectStoreExt,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, RetryConfig, StaticCredentialProvider,
    UpdateVersion, WriteMultipart,
};

use crate::config::BucketOverride;
use crate::hash::sha256_hex;
use crate::range::{parse_range, read_capacity, RangeSpec};

/// In a failover topology one blackholed request must return in bounded time so
/// its availability observation can move selection; object_store's defaults (10
/// retries over three minutes) make that impossible. Health and background
/// maintenance apply their own one-second eligibility cancellation; data
/// transfer keeps the route's one-hour bound so large uploads are not mistaken
/// for dead buckets. The bound is backend-neutral — every cloud in a mixed list
/// gets it, because any one of them can be the hung bucket.
const FAILOVER_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const FAILOVER_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60 * 60);

fn failover_retry_config() -> RetryConfig {
    RetryConfig {
        max_retries: 0,
        retry_timeout: FAILOVER_REQUEST_TIMEOUT,
        ..Default::default()
    }
}

fn connect_timeout_str() -> String {
    format!("{}s", FAILOVER_CONNECT_TIMEOUT.as_secs())
}

fn request_timeout_str() -> String {
    format!("{}s", FAILOVER_REQUEST_TIMEOUT.as_secs())
}

// Apply the failover transport bounds — bounded connect/request timeouts and no
// retries (see `failover_retry_config`) — to a cloud builder. Each backend has
// its own config-key enum and object_store exposes no shared builder trait, so a
// macro over the `Client(..)` variant path removes the duplication where a
// generic fn cannot. `$client_key` is that variant (e.g. `AmazonS3ConfigKey::Client`).
macro_rules! bound_transport {
    ($builder:expr, $client_key:path) => {
        $builder
            .with_config(
                $client_key(ClientConfigKey::ConnectTimeout),
                connect_timeout_str(),
            )
            .with_config($client_key(ClientConfigKey::Timeout), request_timeout_str())
            .with_retry(failover_retry_config())
    };
}

fn bound_s3_transport(builder: AmazonS3Builder) -> AmazonS3Builder {
    bound_transport!(builder, AmazonS3ConfigKey::Client)
}

fn bound_gcs_transport(builder: GoogleCloudStorageBuilder) -> GoogleCloudStorageBuilder {
    bound_transport!(builder, GoogleConfigKey::Client)
}

fn bound_azure_transport(builder: MicrosoftAzureBuilder) -> MicrosoftAzureBuilder {
    bound_transport!(builder, AzureConfigKey::Client)
}

// Point a custom-endpoint builder (S3 or Azure — same inherent methods, no
// shared trait) at `$url`, allowing plaintext only when the URL is `http://`.
// A macro, not a fn, so it spans both builder types.
macro_rules! with_http_endpoint {
    ($b:expr, $url:expr) => {{
        let b = $b.with_endpoint($url.clone());
        if $url.starts_with("http://") {
            b.with_allow_http(true)
        } else {
            b
        }
    }};
}

/// Storage configuration for `serve` and the maintenance commands — one binary,
/// one storage layer, no second implementation. (`sync` never embeds this.)
#[derive(ClapArgs, Debug, Clone)]
pub struct StorageArgs {
    /// Root data directory for disk storage (defaults to $HOME/.pypiron/packages)
    #[arg(long, env = "PYPIRON_DATA_DIR")]
    pub data_dir: Option<String>,

    /// Store everything under this key prefix, so pypiron can share a bucket
    /// (e.g. "pypi" → "pypi/packages/..."). On disk, a subdirectory of --data-dir.
    #[arg(long, env = "PYPIRON_STORAGE_PREFIX")]
    pub storage_prefix: Option<String>,

    /// Object storage: one or more bucket URIs, any mix of backends. Scheme
    /// required: `s3://name[@region]`, `gs://name[@region]`, or
    /// `az://container[@region]`, comma-separated. The optional `@region` labels
    /// the bucket's region (and selects the S3 signing region). Order is
    /// preference — the first bucket is preferred. A single entry is ordinary
    /// single-bucket mode; several enable replication and failover. Unset means
    /// disk at `--data-dir`.
    #[arg(
        long = "buckets",
        env = "PYPIRON_BUCKETS",
        value_delimiter = ',',
        value_name = "URI"
    )]
    pub buckets: Vec<String>,

    /// S3 endpoint URL (for S3-compatible services); applies to every s3:// bucket
    #[arg(long, env = "PYPIRON_S3_ENDPOINT_URL")]
    pub s3_endpoint_url: Option<String>,

    /// Force S3 path-style addressing
    #[arg(long, env = "PYPIRON_S3_FORCE_PATH_STYLE")]
    pub s3_force_path_style: bool,

    // --- Google Cloud Storage (gs:// buckets) ---
    /// Path to a GCS service-account JSON key. Without it, Application Default
    /// Credentials are used — but presigned URLs are then unavailable.
    #[arg(long, env = "PYPIRON_GCS_SERVICE_ACCOUNT_PATH")]
    pub gcs_service_account_path: Option<String>,

    /// GCS endpoint URL (for a local emulator such as fake-gcs-server)
    #[arg(long, env = "PYPIRON_GCS_ENDPOINT_URL")]
    pub gcs_endpoint_url: Option<String>,

    // --- Azure Blob Storage (az:// buckets) ---
    /// Azure storage account name
    #[arg(long, env = "PYPIRON_AZURE_ACCOUNT")]
    pub azure_account: Option<String>,

    /// Azure storage account access key. Enables presigned (SAS) URLs.
    #[arg(long, env = "PYPIRON_AZURE_ACCESS_KEY")]
    pub azure_access_key: Option<String>,

    /// Azure endpoint URL (for a local emulator such as Azurite)
    #[arg(long, env = "PYPIRON_AZURE_ENDPOINT_URL")]
    pub azure_endpoint_url: Option<String>,

    /// Use the Azurite storage emulator (well-known dev account and key)
    #[arg(long, env = "PYPIRON_AZURE_USE_EMULATOR")]
    pub azure_use_emulator: bool,

    /// Per-bucket overrides from `[serve.bucket."scheme://name"]` in
    /// pypiron.toml, keyed by the raw TOML URI. TOML-only by design — not a CLI
    /// arg — so `clap(skip)`; `merge_storage_file` populates it from the same
    /// `[serve]` table serve and the maintenance commands already read.
    #[clap(skip)]
    pub overrides: HashMap<String, BucketOverride>,
}

/// One parsed `--buckets` entry: a backend, a bucket/container name, and an
/// optional region annotation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BucketSpec {
    pub(crate) scheme: BucketScheme,
    pub(crate) name: String,
    /// The optional per-bucket `@region` annotation. On S3 it also selects the
    /// signing region; on every scheme it labels the bucket's region for node
    /// read affinity. Never part of the bucket's topology identity.
    pub(crate) region: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BucketScheme {
    S3,
    Gcs,
    Azure,
}

impl BucketScheme {
    /// The URI scheme prefix, used to make a bucket's topology identity
    /// backend-qualified so `s3://x` and `gs://x` are never the same bucket.
    fn as_prefix(self) -> &'static str {
        match self {
            BucketScheme::S3 => "s3",
            BucketScheme::Gcs => "gs",
            BucketScheme::Azure => "az",
        }
    }
}

impl BucketSpec {
    /// Scheme-qualified bucket identity for topology stamps and duplicate
    /// detection: `s3://name`, `gs://name`, `az://name`. The `@region` is
    /// deliberately excluded — the same bucket reached from two regions is one
    /// bucket — but the backend is not: two different clouds may host the same
    /// name and must hash and compare distinctly.
    fn identity(&self) -> String {
        format!("{}://{}", self.scheme.as_prefix(), self.name)
    }
}

/// Parse one `--buckets` URI. The scheme is required so a bare name can never be
/// silently misfiled to the wrong backend; an optional `@region` annotation is
/// accepted on any scheme.
fn parse_bucket_uri(entry: &str) -> Result<BucketSpec> {
    let entry = entry.trim();
    let Some((scheme, rest)) = entry.split_once("://") else {
        bail!(
            "bucket entry '{entry}' is missing a scheme; use s3://name[@region], gs://name, or az://container"
        );
    };
    let scheme = match scheme {
        "s3" => BucketScheme::S3,
        "gs" => BucketScheme::Gcs,
        "az" => BucketScheme::Azure,
        other => bail!(
            "bucket entry '{entry}' has unknown scheme '{other}://'; use s3://, gs://, or az://"
        ),
    };
    let (name, region) = match rest.split_once('@') {
        Some((_, "")) => bail!("bucket entry '{entry}' has an empty region after '@'"),
        Some((name, region)) => (name, Some(region.to_string())),
        None => (rest, None),
    };
    if name.is_empty() {
        bail!("bucket entry '{entry}' has an empty bucket name");
    }
    Ok(BucketSpec {
        scheme,
        name: name.to_string(),
        region,
    })
}

impl StorageArgs {
    /// The parsed `--buckets` list, empty when unset. Every entry is validated;
    /// a bad URI is a startup error naming the offending entry. In the same order
    /// and count as [`build_all`](Self::build_all)/[`bucket_names`](Self::bucket_names),
    /// so a spec's index matches its storage handle — the read-affinity startup
    /// matches the node's region against these to pick its read bucket.
    pub(crate) fn bucket_specs(&self) -> Result<Vec<BucketSpec>> {
        self.buckets.iter().map(|e| parse_bucket_uri(e)).collect()
    }

    /// Fail startup when a `[serve.bucket."..."]` key names a bucket outside the
    /// configured `--buckets` list — typo protection, listing the valid
    /// identities. Serve-only by design: `buckets migrate` deliberately reaches
    /// a bucket being *removed* from the list (via
    /// [`build_one_by_identity`](Self::build_one_by_identity)), so its override
    /// must not be rejected there.
    pub(crate) fn validate_override_keys(&self) -> Result<()> {
        if self.overrides.is_empty() {
            return Ok(());
        }
        let valid: Vec<String> = self
            .bucket_specs()?
            .iter()
            .map(BucketSpec::identity)
            .collect();
        // Identity strips `@region`, so two distinct TOML keys (e.g.
        // "s3://cache" and "s3://cache@us-west-2") can collapse to one bucket.
        // `override_for` returns the first HashMap match, so a collision would
        // apply one of two contradictory tables non-deterministically across
        // restarts — silently picking an endpoint or credential set by hash
        // order. Reject it at startup, naming both keys, so the config is
        // fail-closed rather than order-dependent.
        let mut seen: HashMap<String, &str> = HashMap::new();
        for key in self.overrides.keys() {
            let id = parse_bucket_uri(key)
                .with_context(|| format!("per-bucket override key '{key}'"))?
                .identity();
            if !valid.contains(&id) {
                bail!(
                    "per-bucket override key '{key}' names bucket '{id}', which is not in the \
                     configured bucket list. Valid buckets: {}",
                    valid.join(", ")
                );
            }
            if let Some(prev) = seen.insert(id.clone(), key) {
                bail!(
                    "per-bucket override keys '{prev}' and '{key}' both resolve to bucket '{id}' \
                     (the '@region' suffix is not part of a bucket's identity); declare each \
                     bucket's override exactly once"
                );
            }
        }
        Ok(())
    }

    /// A `--data-dir` alongside `--buckets` is a benign default (the Dockerfile
    /// ships `PYPIRON_DATA_DIR=/data`), not a second source of truth: the buckets
    /// win and disk is unused. Warn rather than fail (fail-closed rule 5d).
    pub(crate) fn warn_if_data_dir_ignored(&self) {
        if !self.buckets.is_empty() && self.data_dir.is_some() {
            tracing::warn!(
                "--data-dir is set but ignored because --buckets selects object storage; \
                 disk is used only when no buckets are configured"
            );
        }
    }

    /// The per-bucket override for `spec`, matched by identity (`scheme://name`,
    /// `@region` excluded) so a key written with or without a region resolves to
    /// the same bucket. Validates the matched override's fields against the
    /// bucket's scheme and its `env-prefix` credentials before returning it, so a
    /// misconfigured override fails before any bucket I/O.
    fn override_for(&self, spec: &BucketSpec) -> Result<Option<&BucketOverride>> {
        let want = spec.identity();
        for (key, ov) in &self.overrides {
            let key_id = parse_bucket_uri(key)
                .with_context(|| format!("per-bucket override key '{key}'"))?
                .identity();
            if key_id == want {
                self.validate_override(spec, ov)?;
                return Ok(Some(ov));
            }
        }
        Ok(None)
    }

    /// Fail-closed field validation for one matched override: reject a field that
    /// does not apply to the bucket's scheme (rule 5c) and require `env-prefix`
    /// credentials to be whole (rule 5b), naming the offending bucket.
    fn validate_override(&self, spec: &BucketSpec, ov: &BucketOverride) -> Result<()> {
        let id = spec.identity();
        match spec.scheme {
            BucketScheme::S3 => {
                if ov.service_account_path.is_some() {
                    bail!("per-bucket override for '{id}' sets 'service-account-path', which applies only to gs:// buckets");
                }
                if ov.account.is_some() {
                    bail!("per-bucket override for '{id}' sets 'account', which applies only to az:// buckets");
                }
                validate_env_prefix_s3(&id, ov.env_prefix.as_deref())?;
            }
            BucketScheme::Gcs => {
                if ov.force_path_style.is_some() {
                    bail!("per-bucket override for '{id}' sets 'force-path-style', which applies only to s3:// buckets");
                }
                if ov.env_prefix.is_some() {
                    bail!("per-bucket override for '{id}' sets 'env-prefix', which applies only to s3:// and az:// buckets; a gs:// bucket's credentials are a key file — use 'service-account-path'");
                }
                if ov.account.is_some() {
                    bail!("per-bucket override for '{id}' sets 'account', which applies only to az:// buckets");
                }
            }
            BucketScheme::Azure => {
                if ov.force_path_style.is_some() {
                    bail!("per-bucket override for '{id}' sets 'force-path-style', which applies only to s3:// buckets");
                }
                if ov.service_account_path.is_some() {
                    bail!("per-bucket override for '{id}' sets 'service-account-path', which applies only to gs:// buckets");
                }
                validate_env_prefix_azure(&id, ov.env_prefix.as_deref())?;
            }
        }
        Ok(())
    }

    /// The node's default (preferred) storage handle — bucket 0. Preserves the
    /// exact single-bucket construction, crash-injection wrapper included.
    pub async fn build(&self) -> Result<Arc<dyn Storage>> {
        self.build_all()
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("no storage backend configured"))
    }

    /// [`build`](Self::build) plus the storage-format read gate — the write-open
    /// entry point for a headless op (rebuild-index) that mutates the default
    /// bucket. The gate runs strictly: a bucket whose format cannot be verified —
    /// a real GET error or a hang past the one-second control bound — refuses
    /// rather than being written blind, and the operator retries once it is
    /// reachable. New writers should build through this, not `build`, so the gate
    /// can never be forgotten.
    pub async fn build_for_write(&self) -> Result<Arc<dyn Storage>> {
        let storage = self.build().await?;
        let name = self.bucket_names().into_iter().next().unwrap_or_default();
        let handles = [crate::buckets::BucketHandle {
            storage: storage.clone(),
            name,
        }];
        crate::format::verify_format(&handles, |_, _| false).await?;
        Ok(storage)
    }

    /// [`build_all`](Self::build_all) plus the storage-format read gate over every
    /// configured bucket — the write-open entry point for a headless op that
    /// mutates the whole fleet (`buckets migrate`, `origin release`).
    ///
    /// Unlike [`build_for_write`](Self::build_for_write), this skips an
    /// availability failure (serve's classifier, one-second control bound folded
    /// in) rather than refusing: these ops are designed to defer an unreachable
    /// member, and they defer its WRITES under the same bound, so a hung member is
    /// never verified-skipped here yet written blind there. `buckets migrate`
    /// partial-migrates and repairs the missed member on a later startup;
    /// `origin release` re-checks every bucket in its own preflight and aborts if
    /// one is unreachable. A reachable member stamped with a newer format still
    /// refuses; the serve gate is the backstop when a skipped member recovers.
    pub async fn build_all_for_write(&self) -> Result<Vec<Arc<dyn Storage>>> {
        let storages = self.build_all().await?;
        let handles: Vec<crate::buckets::BucketHandle> = storages
            .iter()
            .cloned()
            .zip(self.bucket_names())
            .map(|(storage, name)| crate::buckets::BucketHandle { storage, name })
            .collect();
        let availability = |_: usize, error: &anyhow::Error| {
            crate::bucket_health::classify(crate::observed_storage::signal_for_error(error))
                == crate::bucket_health::SignalClass::AvailabilityFailure
        };
        crate::format::verify_format(&handles, |index, error| {
            crate::buckets::topology_error_is_availability(index, error, &availability)
        })
        .await?;
        Ok(storages)
    }

    /// One storage handle per configured bucket, in preference order: a single
    /// handle for disk/GCS/Azure or a lone S3 bucket, the full list for
    /// multi-bucket S3. Only bucket 0 gets the crash-injection wrapper.
    pub async fn build_all(&self) -> Result<Vec<Arc<dyn Storage>>> {
        let mut handles = self.build_backends().await?;
        // Crash-consistency hook for the chaos tests: abort the process just
        // before the Nth mutating storage operation, on the node's default
        // bucket only. Inert without the env var; see
        // tests/test_crash_consistency.py.
        if let Some(n) = fault_abort_after_writes() {
            if let Some(first) = handles.first_mut() {
                *first = Arc::new(FaultInjectStorage::new(first.clone(), n));
            }
        }
        Ok(handles)
    }

    /// Identity of each configured bucket, in the same order and count as
    /// [`build_all`](Self::build_all), for topology stamping and logs. A
    /// `--buckets` list contributes each bucket's scheme-qualified identity
    /// (`s3://name`, `gs://name`, `az://name`) so a backend mismatch can never
    /// hash the same; with no list, the single disk data directory. The identity
    /// never includes the `@region`.
    pub fn bucket_names(&self) -> Vec<String> {
        if self.buckets.is_empty() {
            return vec![self.resolved_data_dir()];
        }
        // `build_all` parses first and fails on a bad URI, so by the time this is
        // zipped against the handles the list is already valid.
        self.bucket_specs()
            .map(|specs| specs.iter().map(BucketSpec::identity).collect())
            .unwrap_or_default()
    }

    /// The disk data directory actually used, applying the default.
    fn resolved_data_dir(&self) -> String {
        self.data_dir.clone().unwrap_or_else(|| {
            std::env::var("HOME")
                .map(|home| format!("{home}/.pypiron/packages"))
                .unwrap_or_else(|_| "./.pypiron/packages".to_string())
        })
    }

    /// The storage prefix in the normalized form the backends want, if set.
    fn resolved_prefix(&self) -> Result<Option<String>> {
        self.storage_prefix
            .as_deref()
            .map(normalize_prefix)
            .transpose()
    }

    /// Short, human-friendly description for the startup banner.
    pub fn describe(&self) -> String {
        let where_ = if self.buckets.is_empty() {
            format!("disk · {}", self.resolved_data_dir())
        } else {
            format!("buckets · {}", self.buckets.join(", "))
        };
        match &self.storage_prefix {
            Some(p) => format!("{where_} · prefix {p}"),
            None => where_,
        }
    }

    async fn build_backends(&self) -> Result<Vec<Arc<dyn Storage>>> {
        let prefix = self.resolved_prefix()?;
        // No `--buckets` list is the disk default. Disk has no key namespace to
        // share, so the prefix is simply a subdirectory of the data dir — same
        // tree, one level down.
        if self.buckets.is_empty() {
            let mut root = PathBuf::from(self.resolved_data_dir());
            if let Some(ref p) = prefix {
                root.push(p);
            }
            return Ok(vec![Arc::new(DiskStorage::new(root))]);
        }
        // A `--buckets` list is object storage: parse every URI up front so one
        // bad entry fails startup before any bucket is contacted, then build each
        // with its backend's native builder. A single-entry list is ordinary
        // single-bucket mode (failover machinery stays dormant with one handle).
        let specs = self.bucket_specs()?;
        let failover = specs.len() > 1;
        let mut handles = Vec::with_capacity(specs.len());
        for spec in &specs {
            let ov = self.override_for(spec)?;
            handles.push(match spec.scheme {
                BucketScheme::S3 => {
                    self.build_one_s3(&spec.name, spec.region.as_deref(), &prefix, failover, ov)
                        .await?
                }
                BucketScheme::Gcs => {
                    self.build_one_gcs(&spec.name, &prefix, failover, ov)
                        .await?
                }
                BucketScheme::Azure => {
                    self.build_one_azure(&spec.name, &prefix, failover, ov)
                        .await?
                }
            });
        }
        Ok(handles)
    }

    /// Build one S3-backed [`Storage`]. Credentials come from the standard AWS
    /// chain (env vars, web identity, instance metadata). The default
    /// S3ConditionalPut is ETag-match, so the single-PUT create-if-absent path
    /// works on S3 and S3-compatible stores out of the box; large artifacts are
    /// published with a multipart copy-if-not-exists. `region` is the per-bucket
    /// `@region`, if any.
    /// Build a single storage handle for a bucket named by its scheme-qualified
    /// identity (`s3://name`, `gs://name`, `az://name`) as recorded in a topology
    /// stamp. `migrate` uses this to reach a bucket being *removed* from the list
    /// so its `_repl/` notes can be checked before it is dropped. Failover
    /// (bounded transport, no SDK retries) is on so an unreachable removed bucket
    /// fails fast rather than hanging the maintenance command.
    pub async fn build_one_by_identity(&self, identity: &str) -> Result<Arc<dyn Storage>> {
        let prefix = self.resolved_prefix()?;
        let spec = parse_bucket_uri(identity)?;
        let ov = self.override_for(&spec)?;
        match spec.scheme {
            BucketScheme::S3 => {
                self.build_one_s3(&spec.name, spec.region.as_deref(), &prefix, true, ov)
                    .await
            }
            BucketScheme::Gcs => self.build_one_gcs(&spec.name, &prefix, true, ov).await,
            BucketScheme::Azure => self.build_one_azure(&spec.name, &prefix, true, ov).await,
        }
    }

    async fn build_one_s3(
        &self,
        bucket: &str,
        region: Option<&str>,
        prefix: &Option<String>,
        failover: bool,
        ov: Option<&BucketOverride>,
    ) -> Result<Arc<dyn Storage>> {
        if bucket.is_empty() {
            bail!("empty S3 bucket name");
        }
        let base = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
        let mut b = if failover {
            bound_s3_transport(base)
        } else {
            base
        };
        // Region precedence: the per-bucket `@region` suffix, else the builder's
        // own default (which `from_env()` seeds from ambient AWS_REGION /
        // AWS_DEFAULT_REGION).
        if let Some(r) = region {
            b = b.with_region(r.to_string());
        }
        // Endpoint and addressing: from_env → backend-wide flag → per-bucket
        // override (the override wins).
        let endpoint = ov
            .and_then(|o| o.endpoint_url.clone())
            .or_else(|| self.s3_endpoint_url.clone());
        let force_path_style = ov
            .and_then(|o| o.force_path_style)
            .unwrap_or(self.s3_force_path_style);
        if let Some(ref url) = endpoint {
            b = with_http_endpoint!(b, url);
        }
        if force_path_style {
            b = b.with_virtual_hosted_style_request(false);
        } else if endpoint.is_none() {
            // Real AWS prefers virtual-hosted-style addressing; custom endpoints
            // (MinIO et al.) keep the path-style default.
            b = b.with_virtual_hosted_style_request(true);
        }
        // Per-bucket scoped credentials via env-prefix. Explicit `with_*` beats
        // `from_env()`, so this bucket authenticates with its own keys while the
        // rest fall through to the ambient AWS chain. Presence was validated at
        // startup (rule 5b); the pattern-match is a fail-closed backstop.
        if let Some(env_prefix) = ov
            .and_then(|o| o.env_prefix.as_deref())
            .filter(|p| !p.is_empty())
        {
            if let (Some(id), Some(secret)) = (
                env_nonempty(&format!("{env_prefix}AWS_ACCESS_KEY_ID")),
                env_nonempty(&format!("{env_prefix}AWS_SECRET_ACCESS_KEY")),
            ) {
                // Install the scoped credential as an explicit provider rather
                // than via `with_access_key_id`/`with_secret_access_key`: those
                // leave the builder's `token` seeded by `from_env()` from the
                // ambient AWS_SESSION_TOKEN, so a bucket authenticated with a
                // second account's permanent IAM keys would be signed with the
                // host role's session token and rejected (InvalidToken). A
                // StaticCredentialProvider carries exactly the scoped token —
                // the prefixed AWS_SESSION_TOKEN if set, otherwise none — and
                // nothing ambient leaks in.
                let token = env_nonempty(&format!("{env_prefix}AWS_SESSION_TOKEN"));
                b = b.with_credentials(Arc::new(StaticCredentialProvider::new(AwsCredential {
                    key_id: id,
                    secret_key: secret,
                    token,
                })));
            }
        }
        let s3 = Arc::new(
            b.build()
                .with_context(|| format!("configure S3 backend for bucket '{bucket}'"))?,
        );
        let creds = s3.credentials().clone();
        let store: Arc<dyn ObjectStore> = s3.clone();
        let signer: Arc<dyn Signer> = s3;
        let mut storage = ObjectStorage::new(store, Some(signer), prefix.clone(), "s3");
        // Server-side copy is a multi-bucket-only replication transport; single
        // buckets never replicate, so they carry no copy material.
        if failover {
            let resolved_region = region
                .map(str::to_string)
                .or_else(|| env_nonempty("AWS_REGION"))
                .or_else(|| env_nonempty("AWS_DEFAULT_REGION"))
                .unwrap_or_else(|| "us-east-1".to_string());
            storage = storage.with_copy(CopyBackend::S3 {
                client: copy_http_client()?,
                creds,
                region: resolved_region,
                bucket: bucket.to_string(),
                endpoint: endpoint.clone(),
                virtual_hosted: !force_path_style && endpoint.is_none(),
            });
        }
        Ok(Arc::new(storage))
    }

    /// Build one GCS-backed [`Storage`]. Credentials come from Application
    /// Default Credentials unless a service-account key is given; presigning
    /// needs that key, so under ADC (or an emulator) presign is disabled.
    /// object_store maps create-if-absent and CAS to GCS generation
    /// preconditions natively — no builder flag required.
    async fn build_one_gcs(
        &self,
        bucket: &str,
        prefix: &Option<String>,
        failover: bool,
        ov: Option<&BucketOverride>,
    ) -> Result<Arc<dyn Storage>> {
        if bucket.is_empty() {
            bail!("empty GCS bucket name");
        }
        let base = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
        let mut b = if failover {
            bound_gcs_transport(base)
        } else {
            base
        };
        let mut can_sign = false;
        // Service-account key: per-bucket override wins over the backend-wide
        // flag. A per-bucket key enables presigning for this bucket alone.
        let sa_path = ov
            .and_then(|o| o.service_account_path.clone())
            .or_else(|| self.gcs_service_account_path.clone());
        if let Some(p) = sa_path {
            b = b.with_service_account_path(p);
            can_sign = true;
        }
        let endpoint = ov
            .and_then(|o| o.endpoint_url.clone())
            .or_else(|| self.gcs_endpoint_url.clone());
        if let Some(url) = &endpoint {
            // Emulator (fake-gcs-server): point at it and skip signing.
            b = b
                .with_config(GoogleConfigKey::BaseUrl, url.clone())
                .with_config(GoogleConfigKey::SkipSignature, "true");
            can_sign = false;
        }
        let gcs = Arc::new(
            b.build()
                .with_context(|| format!("configure GCS backend for bucket '{bucket}'"))?,
        );
        let creds = gcs.credentials().clone();
        let store: Arc<dyn ObjectStore> = gcs.clone();
        let signer = can_sign.then_some(gcs as Arc<dyn Signer>);
        let mut storage = ObjectStorage::new(store, signer, prefix.clone(), "gcs");
        if failover {
            storage = storage.with_copy(CopyBackend::Gcs {
                client: copy_http_client()?,
                creds,
                bucket: bucket.to_string(),
                endpoint: endpoint.clone(),
            });
        }
        Ok(Arc::new(storage))
    }

    /// Build one Azure Blob-backed [`Storage`]. Credentials come from the Azure
    /// environment chain; SAS presigning needs the account access key (or the
    /// emulator). object_store maps create-if-absent to `If-None-Match: *` and
    /// CAS to `If-Match` natively — no builder flag required.
    async fn build_one_azure(
        &self,
        container: &str,
        prefix: &Option<String>,
        failover: bool,
        ov: Option<&BucketOverride>,
    ) -> Result<Arc<dyn Storage>> {
        if container.is_empty() {
            bail!("empty Azure container name");
        }
        let base = MicrosoftAzureBuilder::from_env().with_container_name(container);
        let mut b = if failover {
            bound_azure_transport(base)
        } else {
            base
        };
        let mut can_sign = false;
        // Account: per-bucket override wins over the backend-wide flag.
        let mut copy_account = ov
            .and_then(|o| o.account.clone())
            .or_else(|| self.azure_account.clone());
        if let Some(a) = &copy_account {
            b = b.with_account(a.clone());
        }
        // The account key doubles as Shared Key signing material for Copy Blob.
        let mut shared_key: Option<String> = None;
        if let Some(ref k) = self.azure_access_key {
            b = b.with_access_key(k.clone());
            can_sign = true;
            shared_key = Some(k.clone());
        }
        // Per-bucket scoped key via env-prefix (explicit beats from_env and the
        // backend-wide key). Presence validated at startup (rule 5b).
        if let Some(env_prefix) = ov
            .and_then(|o| o.env_prefix.as_deref())
            .filter(|p| !p.is_empty())
        {
            if let Some(key) = env_nonempty(&format!("{env_prefix}AZURE_ACCESS_KEY")) {
                b = b.with_access_key(key.clone());
                can_sign = true;
                shared_key = Some(key);
            }
        }
        if self.azure_use_emulator {
            b = b.with_use_emulator(true);
            can_sign = true;
            // Azurite's well-known account + key, so an emulator copy can sign.
            copy_account.get_or_insert_with(|| "devstoreaccount1".to_string());
            shared_key.get_or_insert_with(|| {
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_string()
            });
        }
        let endpoint = ov
            .and_then(|o| o.endpoint_url.clone())
            .or_else(|| self.azure_endpoint_url.clone());
        if let Some(url) = &endpoint {
            b = with_http_endpoint!(b, url);
        }
        let az = Arc::new(
            b.build()
                .with_context(|| format!("configure Azure backend for container '{container}'"))?,
        );
        let creds = az.credentials().clone();
        let store: Arc<dyn ObjectStore> = az.clone();
        let signer = can_sign.then_some(az as Arc<dyn Signer>);
        let mut storage = ObjectStorage::new(store, signer, prefix.clone(), "azure");
        // Copy Blob is same-account only; without a resolvable account we cannot
        // build the source/destination URLs, so copy stays off (the ladder streams).
        if failover {
            if let Some(account) = copy_account {
                storage = storage.with_copy(CopyBackend::Azure {
                    client: copy_http_client()?,
                    creds,
                    account,
                    container: container.to_string(),
                    endpoint: endpoint.clone(),
                    shared_key,
                });
            }
        }
        Ok(Arc::new(storage))
    }
}

/// An environment variable's value, treating empty as unset — an empty
/// `PYPIRON`/`AWS`/`AZURE` var is a common container footgun (`value: ""`), and
/// a scoped credential must fail closed on it, not silently read blank.
fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Require both halves of an S3 `env-prefix` credential to be present (rule 5b):
/// one half is unusable and none-at-all means scoped creds were promised but not
/// delivered. Names the bucket and the exact env vars so the fix is obvious.
fn validate_env_prefix_s3(id: &str, prefix: Option<&str>) -> Result<()> {
    let Some(prefix) = prefix.filter(|p| !p.is_empty()) else {
        return Ok(());
    };
    let key_id = env_nonempty(&format!("{prefix}AWS_ACCESS_KEY_ID"));
    let secret = env_nonempty(&format!("{prefix}AWS_SECRET_ACCESS_KEY"));
    match (key_id.is_some(), secret.is_some()) {
        (true, true) => Ok(()),
        (false, false) => bail!(
            "per-bucket override for '{id}' sets env-prefix '{prefix}' but neither \
             {prefix}AWS_ACCESS_KEY_ID nor {prefix}AWS_SECRET_ACCESS_KEY is set; scoped \
             credentials were promised but none were delivered"
        ),
        (true, false) => bail!(
            "per-bucket override for '{id}' sets env-prefix '{prefix}' but \
             {prefix}AWS_SECRET_ACCESS_KEY is empty/unset"
        ),
        (false, true) => bail!(
            "per-bucket override for '{id}' sets env-prefix '{prefix}' but \
             {prefix}AWS_ACCESS_KEY_ID is empty/unset"
        ),
    }
}

/// Require an Azure `env-prefix` credential to be present (rule 5b): a set
/// prefix with no `<P>AZURE_ACCESS_KEY` promised scoped creds and delivered none.
fn validate_env_prefix_azure(id: &str, prefix: Option<&str>) -> Result<()> {
    let Some(prefix) = prefix.filter(|p| !p.is_empty()) else {
        return Ok(());
    };
    if env_nonempty(&format!("{prefix}AZURE_ACCESS_KEY")).is_none() {
        bail!(
            "per-bucket override for '{id}' sets env-prefix '{prefix}' but \
             {prefix}AZURE_ACCESS_KEY is empty/unset; scoped credentials were promised \
             but none were delivered"
        );
    }
    Ok(())
}

/// The crash-injection write threshold from the environment, if set (chaos tests).
fn fault_abort_after_writes() -> Option<i64> {
    std::env::var("PYPIRON_FAULT_ABORT_AFTER_WRITES")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
}

/// Sentinel error for "object does not exist" — callers translate this to
/// 404; every other storage error is an outage and must surface as one.
#[derive(Debug, thiserror::Error)]
#[error("not found: {0}")]
pub struct NotFound(pub String);

/// A remote bucket/container is missing, rather than one object inside it.
/// Keep this distinct from [`NotFound`]: the health controller must fail over
/// on the former and treat the latter as a successful store response.
#[derive(Debug, thiserror::Error)]
#[error("{backend} bucket unavailable: {detail}")]
pub(crate) struct BucketUnavailable {
    backend: &'static str,
    detail: String,
}

/// True if `err` is (or wraps) a missing-object error.
pub fn is_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<NotFound>().is_some()
}

pub(crate) fn is_bucket_unavailable(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.downcast_ref::<BucketUnavailable>().is_some())
}

pub(crate) fn message_is_missing_bucket(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    // S3 (and GCS, which shares the "specified bucket does not exist" wording)
    // report a missing bucket; Azure reports a missing container. Both are the
    // container-level outage the health controller must fail over on — distinct
    // from a healthy missing object inside a live bucket.
    message.contains("<code>nosuchbucket</code>")
        || message.contains("\"code\":\"nosuchbucket\"")
        || message.contains("specified bucket does not exist")
        || message.contains("<code>containernotfound</code>")
        || message.contains("\"code\":\"containernotfound\"")
        || message.contains("specified container does not exist")
}

pub(crate) fn object_store_is_missing_bucket(error: &OsError) -> bool {
    match error {
        OsError::NotFound { source, .. } | OsError::Generic { source, .. } => {
            message_is_missing_bucket(&source.to_string())
        }
        _ => false,
    }
}

fn bucket_unavailable(backend: &str, error: &OsError) -> anyhow::Error {
    // Do not preserve the typed object-level NotFound in the error chain: that
    // variant also represents a missing bucket, which health must classify as
    // an outage rather than a healthy missing key.
    BucketUnavailable {
        backend: match backend {
            "s3" => "s3",
            "gcs" => "gcs",
            "azure" => "azure",
            _ => "object storage",
        },
        detail: error.to_string(),
    }
    .into()
}

/// A file from a directory listing, with the metadata index rendering needs.
pub struct FileEntry {
    pub key: String,
    pub size: u64,
    /// RFC 3339 last-modified timestamp (serves as PEP 700 upload-time).
    pub last_modified: Option<String>,
}

/// One object from a flat (recursive) listing — see [`Storage::list_all`].
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    /// Opaque change detector, compared for equality only: the S3 ETag, or
    /// mtime+size on disk. Two listings agree on (key, size, etag) iff the
    /// object hasn't been rewritten between them.
    pub etag: String,
}

/// First characters a key can have under the prefixes the audit enumerates:
/// normalized package names start with [a-z0-9] (names.rs), and the global
/// index files are `index.html`/`index.json`. Fanning a flat listing out over
/// these sub-prefixes makes enumeration parallel — S3 pagination within one
/// prefix is inherently serial.
pub const SHARD_CHARS: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

#[async_trait]
pub trait Storage: Send + Sync {
    /// Check if an object exists.
    async fn head_exists(&self, key: &str) -> Result<bool>;

    /// The stored size of `key` in bytes, or `None` if it is absent. One
    /// metadata round-trip — a HEAD on the object stores, a stat on disk — used
    /// by [`store_artifact_verified`] to confirm a just-written artifact
    /// actually landed its bytes rather than a silent zero-byte 200.
    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        // Correct for any backend via one bounded listing; the native backends
        // override it with a single HEAD/stat. The exact key sorts ahead of any
        // sidecar or companion sharing its prefix, so a tiny page suffices.
        for obj in self.list_page(key, None, 4).await? {
            if obj.key == key {
                return Ok(Some(obj.size));
            }
        }
        Ok(None)
    }

    /// Serve an artifact as an HTTP response, honoring a `Range` header.
    /// Each backend uses its native range machinery (seek for disk, S3's own
    /// validation for S3). Errors mean "not found" to the caller.
    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>>;

    /// A presigned GET URL, where the backend supports one (S3). `None` means
    /// "serve it yourself" (disk).
    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>>;

    /// Write bytes to `key`. `content_type` is best-effort (ignored on Disk).
    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()>;

    /// Atomically create `key` only if it does not exist. Returns false when
    /// the object was already there (or we lost the race). This is what
    /// enforces filename immutability and origin exclusivity — a HEAD check
    /// alone is a TOCTOU hole.
    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool>;

    /// `put_if_absent`, but the body comes from a local file — artifacts of
    /// any size are stored without ever being held in memory.
    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<bool>;

    /// Read full object bytes (indexes, sidecars — small files only).
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>>;

    /// List immediate file entries under the directory `dir_prefix` (non-recursive),
    /// returning full keys (dir_prefix + filename) with size and last-modified.
    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>>;

    /// Flat, recursive listing of every object whose key starts with
    /// `prefix`, sorted by key. This is the cheap way to see an entire
    /// corpus: one paged LIST per 1,000 keys on S3 (vs. one LIST per
    /// directory), one filesystem walk on disk. `prefix` is a *key* prefix,
    /// not a directory — `packages/a` matches every package starting with
    /// 'a', which is how callers parallelize (see [`SHARD_CHARS`]).
    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// One bounded page of a flat listing: up to `limit` objects whose key
    /// sorts strictly after `after` (or from the start when `after` is `None`),
    /// in ascending key order. Lets a sweep or diff walk an unbounded prefix in
    /// bounded batches — never holding the whole backlog or package tree
    /// resident. The default derives
    /// paging from [`list_all`](Storage::list_all); backends with native
    /// pagination (S3/GCS/Azure) override it so the cap is honored at the wire
    /// via start-after, not after a full listing.
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        let all = self.list_all(prefix).await?;
        Ok(all
            .into_iter()
            .filter(|obj| after.is_none_or(|a| obj.key.as_str() > a))
            .take(limit)
            .collect())
    }

    /// Delete multiple keys (best-effort).
    async fn delete_keys(&self, keys: &[String]) -> Result<()>;

    /// Whether this backend supports conditional writes for leader leases.
    /// Disk is explicitly single-node: no lease, always leader.
    fn supports_leases(&self) -> bool {
        false
    }

    /// Read object bytes plus ETag; `None` if the object is missing.
    async fn get_with_etag(&self, _key: &str) -> Result<Option<(Vec<u8>, String)>> {
        Err(anyhow!("leases are not supported by this backend"))
    }

    /// Create-if-absent (`If-None-Match: *`). `Some(etag)` on success,
    /// `None` if the object already exists or we lost the race.
    async fn put_if_none_match(&self, _key: &str, _bytes: Vec<u8>) -> Result<Option<String>> {
        Err(anyhow!("leases are not supported by this backend"))
    }

    /// Replace-if-unchanged (`If-Match`). `Some(new_etag)` on success,
    /// `None` if the ETag no longer matches.
    async fn put_if_match(
        &self,
        _key: &str,
        _etag: &str,
        _bytes: Vec<u8>,
    ) -> Result<Option<String>> {
        Err(anyhow!("leases are not supported by this backend"))
    }

    /// This backend's identity as a server-side-copy *source*, or `None` when it
    /// can never be one (Disk, or a cloud backend whose signing material is
    /// unreachable). See [`CopyOrigin`]; multi-bucket replication only.
    fn copy_origin(&self) -> Option<CopyOrigin> {
        None
    }

    /// The credential identity this backend authenticates as, for the boot
    /// copy-eligibility matrix's static pre-filter (`None` = unknown, so the
    /// per-cell boot verification decides alone). One metadata round-trip at
    /// boot; never on a request path.
    async fn copy_credential_identity(&self) -> Result<Option<String>> {
        Ok(None)
    }

    /// Copy `src_key` from the sibling backend `src` describes into `dst_key` on
    /// self, server-side — zero bytes cross this node. `expected_size` lets a
    /// backend refuse an object too large for its single-request copy verb.
    /// `Copied` = the object now exists on self (provider success parsed);
    /// `NotCopyable` = this backend cannot serve the copy, so the caller streams
    /// (not an error); `Err` = a copy was attempted and failed, so the caller
    /// streams and then leaves its repair note. The default cannot copy.
    async fn server_side_copy(
        &self,
        src: &CopyOrigin,
        src_key: &str,
        dst_key: &str,
        expected_size: u64,
    ) -> Result<CopyOutcome> {
        let _ = (src, src_key, dst_key, expected_size);
        Ok(CopyOutcome::NotCopyable)
    }
}

/// Which cloud a [`CopyOrigin`] belongs to. A server-side copy is always
/// same-provider, so this is the first thing eligibility compares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyProvider {
    S3,
    Gcs,
    Azure,
}

/// A backend's identity as a server-side-copy *source*: enough for the boot
/// eligibility matrix to decide whether a destination can pull from it, and
/// enough for the destination's copy verb to name it. Produced by
/// [`Storage::copy_origin`].
#[derive(Clone, Debug)]
pub struct CopyOrigin {
    pub provider: CopyProvider,
    /// The source bucket/container name, named by the destination's copy verb
    /// (S3 `x-amz-copy-source`, GCS source bucket, Azure source blob URL).
    pub location: String,
    /// A custom endpoint (MinIO, Azurite, a GCS emulator) or `None` for the real
    /// cloud. Two custom-endpoint backends are copy-compatible only when equal;
    /// two real-AWS buckets in different regions are (CopyObject is cross-region).
    pub endpoint: Option<String>,
    /// Azure storage account (Copy Blob is same-account only); `None` elsewhere.
    pub account: Option<String>,
}

impl CopyOrigin {
    /// Stable per-handle key for the boot matrix's verified set. Bucket
    /// identities are unique within a topology, so this never collides.
    pub fn handle_key(&self) -> String {
        format!(
            "{:?}|{}|{}|{}",
            self.provider,
            self.location,
            self.endpoint.as_deref().unwrap_or(""),
            self.account.as_deref().unwrap_or(""),
        )
    }
}

/// Outcome of a [`Storage::server_side_copy`] attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyOutcome {
    /// Copied server-side; the object now exists on the destination.
    Copied,
    /// This backend cannot serve the copy (cross-provider, oversize, or no
    /// signing material). The caller streams; not an error.
    NotCopyable,
}

/// Whether `dst` may server-side-copy from `src`: same provider, compatible
/// endpoints, and — for S3/Azure — the same credential identity. Real AWS
/// cross-region is eligible (endpoints both `None`); two distinct custom
/// endpoints (separate MinIO clusters) are not. The boot verification then
/// confirms each eligible cell against the live buckets.
pub fn copy_pair_eligible(
    dst: &CopyOrigin,
    dst_identity: Option<&str>,
    src: &CopyOrigin,
    src_identity: Option<&str>,
) -> bool {
    if dst.provider != src.provider || dst.endpoint != src.endpoint {
        return false;
    }
    match dst.provider {
        // Same access key = same account = readable-src/writable-dst by default.
        // A custom endpoint whose identity we could not resolve still qualifies
        // on equal-endpoint alone; the boot copy verifies it for real.
        CopyProvider::S3 => match (dst_identity, src_identity) {
            (Some(a), Some(b)) => a == b,
            _ => dst.endpoint.is_some(),
        },
        // One process carries one GCS credential chain; the boot copy gates the
        // rest (a project the credential cannot write simply fails the cell).
        CopyProvider::Gcs => true,
        // Copy Blob is same-account only.
        CopyProvider::Azure => dst.account.is_some() && dst.account == src.account,
    }
}

/// A wedged artifact write must fail in bounded time instead of parking on the
/// one-hour transport ceiling ([`FAILOVER_REQUEST_TIMEOUT`]), yet a healthy
/// multi-MiB upload dripping over a slow link must still finish. Budget a flat
/// base plus a per-MiB allowance: generous enough that steady progress never
/// trips it (the blackbox suite drips 16 MiB over 11s), tight enough that a
/// stalled connection is abandoned long before the request deadline.
const ARTIFACT_WRITE_TIMEOUT_BASE: std::time::Duration = std::time::Duration::from_secs(60);
const ARTIFACT_WRITE_TIMEOUT_PER_MIB: std::time::Duration = std::time::Duration::from_secs(1);

fn artifact_write_timeout(payload_size: u64) -> std::time::Duration {
    let mib = (payload_size / (1024 * 1024)).min(u32::MAX as u64) as u32;
    ARTIFACT_WRITE_TIMEOUT_BASE + ARTIFACT_WRITE_TIMEOUT_PER_MIB * mib
}

/// Bound one artifact write (D3): a connection that stops making progress is
/// abandoned with a clear error instead of hanging on the transport ceiling.
pub async fn bounded_artifact_write<T>(
    key: &str,
    payload_size: u64,
    op: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let budget = artifact_write_timeout(payload_size);
    match tokio::time::timeout(budget, op).await {
        Ok(result) => result,
        Err(_elapsed) => Err(anyhow!(
            "artifact write to {key} exceeded {budget:?}; treating the connection as wedged"
        )),
    }
}

/// D1: HEAD `key` and require the stored object is exactly `expected_size`
/// bytes. A conditional create that 200-acked but landed truncated or zero
/// bytes (observed only behind CI's fault proxy) is caught here instead of
/// being trusted and propagated as truth.
pub async fn verify_stored_size(
    storage: &dyn Storage,
    key: &str,
    expected_size: u64,
) -> Result<()> {
    let stored = storage
        .stored_size(key)
        .await
        .with_context(|| format!("HEAD {key} to verify the stored artifact"))?
        .ok_or_else(|| anyhow!("artifact {key} vanished immediately after a create success"))?;
    if stored != expected_size {
        bail!(
            "artifact {key} stored {stored} bytes, expected {expected_size} (write landed corrupt)"
        );
    }
    Ok(())
}

/// How an already-present body under an immutable artifact key is resolved by
/// [`store_artifact_verified`].
#[derive(Clone, Copy)]
pub enum Existing<'a> {
    /// Filenames are immutable (the upload path): a body already at the key is
    /// the caller's conflict — returned as `Ok(false)`, never overwritten. A
    /// create that lands corrupt is deleted so the caller's retry starts from a
    /// clean key instead of being locked out by its own zero-byte debris.
    Reject,
    /// The sha256 is authoritative (the replication copy): a matching body
    /// dedups (`Ok(false)`), a wrong one is stale crash debris repaired in
    /// place, and either way the key ends holding exactly these bytes.
    Repair(&'a str),
}

/// The body of an artifact write: already resident (replication) or spooled to
/// disk and streamed so large uploads never sit in memory (upload).
pub enum ArtifactBody<'a> {
    Bytes(Vec<u8>),
    Spool(&'a std::path::Path),
}

impl ArtifactBody<'_> {
    async fn read_all(&self) -> Result<Vec<u8>> {
        match self {
            ArtifactBody::Bytes(bytes) => Ok(bytes.clone()),
            ArtifactBody::Spool(path) => fs::read(path)
                .await
                .with_context(|| format!("read spool {} for repair", path.display())),
        }
    }
}

/// Store an immutable artifact at `key`, then prove the bytes that landed are
/// the bytes we meant to write — the shared write primitive for the upload path
/// and the replication copy.
///
/// D1: a conditional create that 200-acks but silently lands zero or truncated
/// bytes used to be trusted and propagated. Every create success is now
/// confirmed with one HEAD (`stored size == expected_size`); an already-present
/// body is resolved by `existing`. D3: the write is bounded by a payload-scaled
/// timeout so a wedged connection can never park the caller on the transport
/// ceiling.
///
/// Returns `true` when this call is what put the correct bytes at `key` (a
/// create or a repair) and `false` when an already-correct body was found.
pub async fn store_artifact_verified(
    storage: &dyn Storage,
    key: &str,
    body: ArtifactBody<'_>,
    expected_size: u64,
    content_type: Option<&str>,
    existing: Existing<'_>,
) -> Result<bool> {
    let created = match &body {
        ArtifactBody::Bytes(bytes) => {
            bounded_artifact_write(
                key,
                expected_size,
                storage.put_if_absent(key, bytes.clone(), content_type),
            )
            .await?
        }
        ArtifactBody::Spool(path) => {
            bounded_artifact_write(
                key,
                expected_size,
                storage.put_file_if_absent(key, path, content_type),
            )
            .await?
        }
    };
    if created {
        if let Err(error) = verify_stored_size(storage, key, expected_size).await {
            if matches!(existing, Existing::Reject) {
                // Our own just-created object is corrupt; drop it so an
                // immutable retry is not permanently blocked by a 409 against
                // debris only this writer could have left.
                let _ = storage.delete_keys(&[key.to_string()]).await;
            }
            return Err(error);
        }
        return Ok(true);
    }
    let expected_sha = match existing {
        Existing::Reject => return Ok(false),
        Existing::Repair(sha) => sha,
    };
    let current = storage
        .get_bytes(key)
        .await
        .with_context(|| format!("read back already-present artifact {key}"))?;
    if sha256_hex(&current) == expected_sha {
        return Ok(false);
    }
    // A wrong-sha body under an immutable key is stale crash debris (e.g. a
    // zero-byte object a 200-acked-but-failed write left behind). Overwrite it
    // with the correct bytes and re-verify, so heal converges instead of
    // bailing forever on an object nothing could repair.
    tracing::warn!(
        key = %key,
        expected_sha = %expected_sha,
        "repairing artifact whose stored bytes do not match its sidecar"
    );
    let correct = body.read_all().await?;
    bounded_artifact_write(
        key,
        expected_size,
        storage.put_bytes(key, correct, content_type),
    )
    .await?;
    let after = storage
        .get_bytes(key)
        .await
        .with_context(|| format!("re-verify repaired artifact {key}"))?;
    let after_sha = sha256_hex(&after);
    if after_sha != expected_sha {
        bail!(
            "artifact {key} still wrong after repair: stored {after_sha}, expected {expected_sha}"
        );
    }
    Ok(true)
}

/// Create `key` if absent — bounded (D3) and, on a create success, verified
/// (D1). `true` means this call created the object; `false` means it already
/// existed and the caller must reconcile the winner. Unlike
/// [`store_artifact_verified`] this never overwrites a byte-divergent winner:
/// the replication copy freezes that conflict rather than clobbering it.
pub async fn create_artifact_verified(
    storage: &dyn Storage,
    key: &str,
    bytes: Vec<u8>,
    expected_size: u64,
    content_type: Option<&str>,
) -> Result<bool> {
    let created = bounded_artifact_write(
        key,
        expected_size,
        storage.put_if_absent(key, bytes, content_type),
    )
    .await?;
    if created {
        verify_stored_size(storage, key, expected_size).await?;
    }
    Ok(created)
}

/// EXDEV ("invalid cross-device link"), hardcoded so we don't pull in libc.
const EXDEV: i32 = 18;

/// ------------------------------ DiskStorage -------------------------------
pub struct DiskStorage {
    root: PathBuf,
    /// Serializes `put_if_match` so its read-compare-write CAS is atomic against
    /// other tasks in this single-node process. Disk has no object-store
    /// conditional put; leader leasing stays off (`supports_leases` = false),
    /// but the origin-claim lifecycle needs CAS, and a
    /// process-local lock is a correct CAS for the single node disk supports.
    cas_lock: tokio::sync::Mutex<()>,
}

/// Content-addressed etag for the disk conditional-write path: identical bytes
/// hash identically (the same ABA caveat object stores carry without
/// versioning), which is all the origin CAS needs — its transitions all change
/// the content string. Distinct from list_all's mtime+size etag; the two etag
/// spaces never cross (one drives CAS, the other drives audit fingerprints).
fn disk_content_etag(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

impl DiskStorage {
    pub fn new<P: Into<PathBuf>>(root: P) -> Self {
        Self {
            root: root.into(),
            cas_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn resolve(&self, key: &str) -> Result<PathBuf> {
        if key.is_empty() {
            return Err(anyhow!("empty key"));
        }
        let rel = Path::new(key);
        let mut clean = PathBuf::new();
        for c in rel.components() {
            match c {
                Component::Normal(seg) => clean.push(seg),
                Component::CurDir => continue,
                _ => return Err(anyhow!("invalid key component in {}", key)),
            }
        }
        Ok(self.root.join(clean))
    }

    async fn ensure_parent(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    /// A unique temp path next to `path` (same filesystem, so rename/link is atomic).
    fn tmp_sibling(&self, path: &Path) -> Result<PathBuf> {
        // A per-process atomic counter — not the clock alone — guarantees a
        // distinct staging path per call. On a coarse-clock host two concurrent
        // writes to the same key can read identical nanos, share one tmp inode,
        // and clobber each other's bytes (corrupting e.g. the .origin marker).
        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("bad path"))?;
        let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        Ok(path.with_file_name(format!(".tmp-{nanos}-{}-{seq}-{name}", std::process::id())))
    }

    /// hard_link `tmp`→`dest` as an atomic create-if-absent (EEXIST → already
    /// there), then remove `tmp` regardless. `Ok(false)` means the destination
    /// already existed.
    async fn link_atomic(&self, tmp: &Path, dest: &Path) -> Result<bool> {
        let created = match fs::hard_link(tmp, dest).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(anyhow::Error::from(e)),
        };
        let _ = fs::remove_file(tmp).await;
        created
    }

    /// Crash-safe overwrite: write `bytes` to a temp sibling of `dest`, then
    /// atomically rename it into place, removing the temp on a failed rename. A
    /// crash leaves either the prior file or nothing at `dest`, never a torn
    /// write (atomicity, not durability — see `put_bytes`). `ctx`, when set,
    /// labels the rename failure.
    async fn write_rename(&self, dest: &Path, bytes: &[u8], ctx: Option<&str>) -> Result<()> {
        let tmp = self.tmp_sibling(dest)?;
        fs::write(&tmp, bytes).await?;
        if let Err(e) = fs::rename(&tmp, dest).await {
            let _ = fs::remove_file(&tmp).await;
            let e = anyhow::Error::from(e);
            return Err(match ctx {
                Some(c) => e.context(c.to_string()),
                None => e,
            });
        }
        Ok(())
    }

    /// Crash-safe create-if-absent: write `bytes` to a temp sibling of `dest`,
    /// then hard-link it into place. `Ok(false)` means the destination already
    /// existed; the temp is always removed.
    async fn write_link(&self, dest: &Path, bytes: &[u8]) -> Result<bool> {
        let tmp = self.tmp_sibling(dest)?;
        fs::write(&tmp, bytes).await?;
        self.link_atomic(&tmp, dest).await
    }
}

#[async_trait]
impl Storage for DiskStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        let p = self.resolve(key)?;
        Ok(fs::metadata(p).await.is_ok())
    }

    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        match fs::metadata(self.resolve(key)?).await {
            Ok(meta) => Ok(Some(meta.len())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context(format!("stat {key}"))),
        }
    }

    async fn presign_get(
        &self,
        _key: &str,
        _expires: std::time::Duration,
    ) -> Result<Option<String>> {
        Ok(None)
    }

    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        let path = self.resolve(key)?;
        let md = match fs::metadata(&path).await {
            Ok(md) => md,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(NotFound(key.to_string()).into());
            }
            Err(e) => return Err(anyhow::Error::from(e).context(format!("stat {key}"))),
        };
        if !md.is_file() {
            return Err(NotFound(key.to_string()).into());
        }
        let size = md.len();

        let resp = match parse_range(range, size) {
            RangeSpec::Full => {
                let file = fs::File::open(&path).await?;
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_LENGTH, size)
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(Body::from_stream(ReaderStream::with_capacity(
                        file,
                        read_capacity(size),
                    )))?
            }
            RangeSpec::Partial(start, end) => {
                let mut file = fs::File::open(&path).await?;
                file.seek(SeekFrom::Start(start)).await?;
                let len = end - start + 1;
                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_LENGTH, len)
                    .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"))
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(Body::from_stream(ReaderStream::with_capacity(
                        file.take(len),
                        read_capacity(len),
                    )))?
            }
            RangeSpec::Unsatisfiable => Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{size}"))
                .body(Body::empty())?,
        };
        Ok(resp)
    }

    async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<()> {
        // Write-to-tmp + rename: a process crash or full disk never leaves a
        // torn file at the final key. This is atomicity, not power-loss
        // durability — we deliberately skip fsync (it would serialize every
        // small index write); the single-node disk backend leans on backups or
        // a journaling FS for that, while cloud backends are durable. S3 PUTs
        // are already atomic.
        let p = self.resolve(key)?;
        self.ensure_parent(&p).await?;
        self.write_rename(&p, &bytes, None).await
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<bool> {
        // hard_link fails with EEXIST if the destination exists — an atomic
        // create-if-absent with full content, unlike create_new + write.
        let p = self.resolve(key)?;
        self.ensure_parent(&p).await?;
        self.write_link(&p, &bytes).await
    }

    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        _content_type: Option<&str>,
    ) -> Result<bool> {
        let p = self.resolve(key)?;
        self.ensure_parent(&p).await?;
        // Same atomic create-if-absent as put_if_absent. Try linking the
        // source directly (free when the spool shares a filesystem with the
        // data dir); EXDEV falls back to a copy into a tmp sibling first.
        match fs::hard_link(path, &p).await {
            Ok(()) => return Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(e) if e.raw_os_error() == Some(EXDEV) => {}
            Err(e) => return Err(anyhow::Error::from(e)),
        }
        let tmp = self.tmp_sibling(&p)?;
        fs::copy(path, &tmp).await?;
        self.link_atomic(&tmp, &p).await
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let p = self.resolve(key)?;
        match fs::read(&p).await {
            Ok(bytes) => Ok(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(NotFound(key.to_string()).into())
            }
            Err(e) => Err(anyhow::Error::from(e).context(format!("read {key}"))),
        }
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        // A missing directory is an empty listing; any other error must
        // propagate — a silent empty here would make the reconciler delete
        // live indexes off a phantom "no packages" observation.
        let dir = self.resolve(dir_prefix)?;
        let mut rd = match fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(anyhow::Error::from(e).context(format!("list {dir_prefix}"))),
        };
        let mut files = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            // The entry can vanish between readdir and this stat: a concurrent
            // upload's temp sibling (tmp_sibling) is renamed/removed out from
            // under us. It's gone for good and was never an artifact, so skip
            // it — propagating ENOENT here spuriously fails the whole rebuild.
            let md = match entry.metadata().await {
                Ok(md) => md,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(anyhow::Error::from(e).context(format!("stat {dir_prefix}")));
                }
            };
            if md.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    let last_modified = md
                        .modified()
                        .ok()
                        .map(OffsetDateTime::from)
                        .and_then(|t| t.format(&Rfc3339).ok());
                    files.push(FileEntry {
                        key: format!("{}{}", dir_prefix, name),
                        size: md.len(),
                        last_modified,
                    });
                }
            }
        }
        files.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(files)
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        for k in keys {
            if let Ok(p) = self.resolve(k) {
                let _ = fs::remove_file(p).await;
            }
        }
        Ok(())
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        // Key prefix, not directory: walk the deepest enclosing directory and
        // filter first-level names against the remainder, so a sharded call
        // ("packages/a") never walks the other shards' trees. The walk is
        // std::fs on a blocking thread — a million-file tree is syscall
        // bound, and tokio::fs would add a channel hop per dirent.
        let (dir_part, name_filter) = match prefix.rfind('/') {
            Some(i) => (&prefix[..=i], &prefix[i + 1..]),
            None => ("", prefix),
        };
        let root = self.resolve(if dir_part.is_empty() { "." } else { dir_part })?;
        let dir_prefix = dir_part.to_string();
        let name_filter = name_filter.to_string();
        tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            let top = match std::fs::read_dir(&root) {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
                Err(e) => return Err(anyhow::Error::from(e).context("list_all root")),
            };
            for entry in top {
                let entry = entry?;
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                if !name.starts_with(&name_filter) {
                    continue;
                }
                walk_disk(&entry.path(), &format!("{dir_prefix}{name}"), &mut out)?;
            }
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        })
        .await?
    }

    // Conditional writes for the origin-claim lifecycle. Disk stays single-node
    // for leasing (`supports_leases` is left false), but these are correct for
    // one process, so the P2 single-bucket race fixes apply on disk too.
    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let p = self.resolve(key)?;
        match fs::read(&p).await {
            Ok(bytes) => {
                let etag = disk_content_etag(&bytes);
                Ok(Some((bytes, etag)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context(format!("get_with_etag {key}"))),
        }
    }

    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let p = self.resolve(key)?;
        self.ensure_parent(&p).await?;
        let etag = disk_content_etag(&bytes);
        Ok(self.write_link(&p, &bytes).await?.then_some(etag))
    }

    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let p = self.resolve(key)?;
        // Hold the CAS lock across the read-compare-write so two tasks cannot
        // both observe the same current etag and both write.
        let _guard = self.cas_lock.lock().await;
        let current = match fs::read(&p).await {
            Ok(b) => Some(disk_content_etag(&b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(anyhow::Error::from(e).context(format!("put_if_match read {key}")))
            }
        };
        if current.as_deref() != Some(etag) {
            return Ok(None);
        }
        let new_etag = disk_content_etag(&bytes);
        self.ensure_parent(&p).await?;
        self.write_rename(&p, &bytes, Some(&format!("put_if_match write {key}")))
            .await?;
        Ok(Some(new_etag))
    }
}

/// Recurse one filesystem subtree, appending every regular file as an
/// ObjectMeta keyed by `key_base` plus its relative path.
fn walk_disk(path: &Path, key_base: &str, out: &mut Vec<ObjectMeta>) -> Result<()> {
    // The entry can vanish between the parent's readdir and this stat — a
    // concurrent upload's temp sibling (tmp_sibling) is renamed/removed out
    // from under us. It was never an artifact, so skip it; propagating ENOENT
    // here would spuriously fail the whole audit/rebuild walk (mirrors the
    // guard in list_dir_entries).
    let md = match std::fs::symlink_metadata(path) {
        Ok(md) => md,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    if md.is_file() {
        out.push(ObjectMeta {
            key: key_base.to_string(),
            size: md.len(),
            etag: disk_etag(&md),
        });
        return Ok(());
    }
    if !md.is_dir() {
        return Ok(());
    }
    // Same race for a directory removed between the stat above and this read.
    let rd = match std::fs::read_dir(path) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in rd {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        walk_disk(&entry.path(), &format!("{key_base}/{name}"), out)?;
    }
    Ok(())
}

/// mtime (nanos) + size: changes whenever the file is rewritten. Disk writes
/// go through tmp+rename, so a content change always produces a new inode
/// with a new mtime.
fn disk_etag(md: &std::fs::Metadata) -> String {
    let mtime = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{mtime}-{}", md.len())
}

/// ------------------------------ ObjectStorage -----------------------------
/// One backend for every cloud object store — S3, GCS, and Azure Blob — over
/// the `object_store` crate. Disk stays a separate, dependency-free backend;
/// everything remote shares this single implementation, so there is no
/// per-cloud code to drift.
pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    /// Present only when the backend can mint presigned GET URLs (S3 always;
    /// GCS with a service-account key; Azure with an account key or emulator).
    /// `None` means "serve it yourself" — never a hard failure.
    signer: Option<Arc<dyn Signer>>,
    /// Root every key under this bare key prefix (no slashes at either end),
    /// letting pypiron share a bucket. `None` means keys sit at the bucket root.
    prefix: Option<String>,
    /// Backend name, for error context.
    backend: &'static str,
    /// Server-side-copy material for the replication transport, when this
    /// backend can originate one ([`Storage::server_side_copy`]). `None` in
    /// single-bucket mode and on any backend without reachable signing material.
    copy: Option<CopyBackend>,
}

/// Everything the destination side needs to sign and issue a server-side copy
/// verb (S3 CopyObject, GCS rewrite, Azure Copy Blob), captured from our own
/// config where object_store hides it. Held on the *destination* backend: the
/// copy is originated and signed by the destination, referencing the source
/// bucket by name (same credential identity by the boot matrix's precondition).
enum CopyBackend {
    S3 {
        client: reqwest::Client,
        creds: AwsCredentialProvider,
        region: String,
        bucket: String,
        endpoint: Option<String>,
        virtual_hosted: bool,
    },
    Gcs {
        client: reqwest::Client,
        creds: GcpCredentialProvider,
        bucket: String,
        endpoint: Option<String>,
    },
    Azure {
        client: reqwest::Client,
        creds: AzureCredentialProvider,
        account: String,
        container: String,
        endpoint: Option<String>,
        /// The base64 account key, when configured — enables Shared Key signing.
        /// Absent under Azure AD / managed identity, where a bearer token is used.
        shared_key: Option<String>,
    },
}

/// S3's single-request CopyObject cap. A larger object needs multipart copy;
/// wheels never approach it, so we decline instead and let the caller stream.
const S3_MAX_SINGLE_COPY: u64 = 5 * 1024 * 1024 * 1024;
/// The real GCS JSON API host; the rewrite endpoint lives under `/storage/v1`.
const DEFAULT_GCS_BASE: &str = "https://storage.googleapis.com";
const AZURE_BLOB_SUFFIX: &str = "blob.core.windows.net";
const AZURE_COPY_VERSION: &str = "2021-08-06";

fn copy_now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

/// Build the bounded HTTP client the copy verbs use. Server-side copies move no
/// bytes through this node, but a slow control plane or a multi-hop GCS rewrite
/// still needs headroom; a dead endpoint fails fast on the connect bound.
fn copy_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(FAILOVER_CONNECT_TIMEOUT)
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .context("build the server-side-copy HTTP client")
}

/// A cloud endpoint URL's authority (`host[:port]`), the value both the request
/// `Host` header and the SigV4 signed `host` header carry.
fn endpoint_authority(endpoint: &str) -> Result<String> {
    let url = url::Url::parse(endpoint).with_context(|| format!("parse endpoint {endpoint}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("endpoint {endpoint} has no host"))?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

/// First `n` chars of an error body, for bounded log/error context.
fn clip(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// The S3 CopyObject destination request URL, its `Host` authority, and the
/// canonical URI to sign. Virtual-hosted for real AWS (`bucket.s3.region.…`),
/// path-style for a custom endpoint (`endpoint/bucket/key`). `enc_dst_key` is
/// the URI-encoded destination object key.
fn s3_copy_target(
    virtual_hosted: bool,
    endpoint: Option<&str>,
    bucket: &str,
    region: &str,
    enc_dst_key: &str,
) -> Result<(String, String, String)> {
    if virtual_hosted {
        let host = format!("{bucket}.s3.{region}.amazonaws.com");
        Ok((
            format!("https://{host}/{enc_dst_key}"),
            host,
            format!("/{enc_dst_key}"),
        ))
    } else {
        let base = endpoint
            .ok_or_else(|| anyhow!("path-style S3 copy requires a configured endpoint"))?
            .trim_end_matches('/');
        Ok((
            format!("{base}/{bucket}/{enc_dst_key}"),
            endpoint_authority(base)?,
            format!("/{bucket}/{enc_dst_key}"),
        ))
    }
}

/// The GCS JSON-API `rewriteTo` URL for one source→destination object pair. Both
/// object names are fully URL-encoded (slashes become `%2F`).
fn gcs_rewrite_url(
    base: &str,
    src_bucket: &str,
    src_key: &str,
    dst_bucket: &str,
    dst_key: &str,
) -> String {
    let base = base.trim_end_matches('/');
    let enc_src = crate::reqsign::uri_encode(src_key, false);
    let enc_dst = crate::reqsign::uri_encode(dst_key, false);
    format!("{base}/storage/v1/b/{src_bucket}/o/{enc_src}/rewriteTo/b/{dst_bucket}/o/{enc_dst}")
}

/// The Azure Blob service base URL for an account, honoring a custom endpoint
/// (Azurite) when configured.
fn azure_blob_base(account: &str, endpoint: Option<&str>) -> String {
    match endpoint {
        Some(e) => e.trim_end_matches('/').to_string(),
        None => format!("https://{account}.{AZURE_BLOB_SUFFIX}"),
    }
}

#[derive(serde::Deserialize)]
struct GcsRewriteResponse {
    #[serde(default)]
    done: bool,
    #[serde(rename = "rewriteToken", default)]
    rewrite_token: Option<String>,
}

/// At or below this size an upload is a single conditional PUT; above it the
/// body streams to a unique staging key as parallel multipart parts (bounded
/// RSS) and is then published atomically with `copy_if_not_exists`. The 16 MB
/// part size keeps a ~900 MB wheel to a handful of in-flight parts.
const MULTIPART_THRESHOLD: u64 = 64 * 1024 * 1024;
const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 6;
const READ_CHUNK: usize = 8 * 1024 * 1024;

/// Staging keys live here; large uploads land under this prefix and are then
/// published (copy-if-not-exists) to their final key. Always cleaned up.
pub(crate) const STAGING_PREFIX: &str = "_staging/";

/// Packs object_store's (e_tag, version) pair into one opaque token. Stores use
/// differing combinations to express a conditional update (S3/Azure: ETag; GCS:
/// generation), so we round-trip both. Compared only for equality.
const VERSION_SEP: char = '\u{1f}';

impl ObjectStorage {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        signer: Option<Arc<dyn Signer>>,
        prefix: Option<String>,
        backend: &'static str,
    ) -> Self {
        Self {
            store,
            signer,
            prefix,
            backend,
            copy: None,
        }
    }

    /// Attach the server-side-copy material for a multi-bucket fleet. Chained by
    /// the cloud builders; single-bucket handles never call it (copy stays off).
    fn with_copy(mut self, copy: CopyBackend) -> Self {
        self.copy = Some(copy);
        self
    }

    /// The storage-prefix-rooted object key as a plain string — the form the
    /// hand-rolled copy verbs sign (they cannot take an [`OsPath`]). The source
    /// and destination share one process-wide `--storage-prefix`.
    fn prefixed(&self, key: &str) -> String {
        match &self.prefix {
            Some(p) => format!("{p}/{key}"),
            None => key.to_string(),
        }
    }

    /// S3 CopyObject: a signed `PUT` whose `x-amz-copy-source` names the source
    /// bucket + key. Cross-bucket and cross-region; overwrites the destination
    /// key (the caller's sidecar-first ordering is the divergence gate, so this
    /// only ever lands sha-adjudicated truth). Handles S3's 200-with-error-body.
    async fn s3_copy(
        &self,
        copy: &CopyBackend,
        src_bucket: &str,
        src_key: &str,
        dst_key: &str,
        expected_size: u64,
    ) -> Result<CopyOutcome> {
        let CopyBackend::S3 {
            client,
            creds,
            region,
            bucket,
            endpoint,
            virtual_hosted,
        } = copy
        else {
            return Ok(CopyOutcome::NotCopyable);
        };
        if expected_size > S3_MAX_SINGLE_COPY {
            return Ok(CopyOutcome::NotCopyable);
        }
        let cred = creds
            .get_credential()
            .await
            .map_err(|e| anyhow!("resolve S3 credential for copy: {e}"))?;
        let enc_dst = crate::reqsign::uri_encode(dst_key, true);
        let (url, host, canonical_uri) = s3_copy_target(
            *virtual_hosted,
            endpoint.as_deref(),
            bucket,
            region,
            &enc_dst,
        )?;
        let copy_source = format!(
            "/{src_bucket}/{}",
            crate::reqsign::uri_encode(src_key, true)
        );
        let cred_ref = crate::reqsign::AwsCredential {
            key_id: &cred.key_id,
            secret: &cred.secret_key,
            token: cred.token.as_deref(),
        };
        let extra_headers = [("x-amz-copy-source".to_string(), copy_source)];
        let signed = crate::reqsign::sign_s3_request(
            &crate::reqsign::S3Request {
                method: "PUT",
                canonical_uri: &canonical_uri,
                canonical_query: "",
                host: &host,
                extra_headers: &extra_headers,
                payload_sha256_hex: crate::reqsign::EMPTY_PAYLOAD_SHA256,
            },
            &cred_ref,
            region,
            "s3",
            copy_now(),
        );
        let mut req = client.put(&url);
        for (name, value) in &signed {
            req = req.header(name.as_str(), value.as_str());
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("S3 CopyObject PUT {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "S3 CopyObject to {dst_key} failed: HTTP {status}: {}",
                clip(&body, 400)
            );
        }
        // S3 answers CopyObject with 200 even on some failures, carrying an
        // <Error> element instead of <CopyObjectResult>. Treat that as a failed
        // copy so the ladder streams.
        if body.contains("<Error>") || !body.contains("<CopyObjectResult") {
            bail!(
                "S3 CopyObject to {dst_key} returned a 200 error body: {}",
                clip(&body, 400)
            );
        }
        Ok(CopyOutcome::Copied)
    }

    /// GCS rewrite: a bearer-authorized `POST` that copies within GCS. A large
    /// object comes back `done:false` with a continuation token; drive the loop
    /// to completion.
    async fn gcs_copy(
        &self,
        copy: &CopyBackend,
        src_bucket: &str,
        src_key: &str,
        dst_key: &str,
    ) -> Result<CopyOutcome> {
        let CopyBackend::Gcs {
            client,
            creds,
            bucket,
            endpoint,
        } = copy
        else {
            return Ok(CopyOutcome::NotCopyable);
        };
        let base = endpoint.as_deref().unwrap_or(DEFAULT_GCS_BASE);
        let cred = creds
            .get_credential()
            .await
            .map_err(|e| anyhow!("resolve GCS credential for copy: {e}"))?;
        let rewrite_url = gcs_rewrite_url(base, src_bucket, src_key, bucket, dst_key);
        let mut token: Option<String> = None;
        // Bounded so a control plane that never reports done cannot spin forever;
        // each hop rewrites a fixed slab, which covers multi-GB objects.
        for _ in 0..10_000 {
            let url = match &token {
                Some(t) => format!(
                    "{rewrite_url}?rewriteToken={}",
                    crate::reqsign::uri_encode(t, false)
                ),
                None => rewrite_url.clone(),
            };
            let resp = client
                .post(&url)
                .header("authorization", format!("Bearer {}", cred.bearer))
                .header("content-length", "0")
                .send()
                .await
                .with_context(|| format!("GCS rewrite POST {rewrite_url}"))?;
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                bail!(
                    "GCS rewrite to {dst_key} failed: HTTP {status}: {}",
                    clip(&body, 400)
                );
            }
            let parsed: GcsRewriteResponse = serde_json::from_str(&body)
                .with_context(|| format!("parse GCS rewrite response: {}", clip(&body, 400)))?;
            if parsed.done {
                return Ok(CopyOutcome::Copied);
            }
            match parsed.rewrite_token {
                Some(t) => token = Some(t),
                None => {
                    bail!("GCS rewrite to {dst_key} is not done but returned no continuation token")
                }
            }
        }
        bail!("GCS rewrite to {dst_key} did not complete within the continuation bound")
    }

    /// Azure Copy Blob: a `PUT` whose `x-ms-copy-source` names the source blob
    /// URL, same storage account. Small blobs complete synchronously; a pending
    /// (async) copy is reported as a failure so the ladder streams (wheels never
    /// go async).
    async fn azure_copy(
        &self,
        copy: &CopyBackend,
        src: &CopyOrigin,
        src_key: &str,
        dst_key: &str,
    ) -> Result<CopyOutcome> {
        let CopyBackend::Azure {
            client,
            creds,
            account,
            container,
            endpoint,
            shared_key,
        } = copy
        else {
            return Ok(CopyOutcome::NotCopyable);
        };
        if src.account.as_deref() != Some(account.as_str()) {
            return Ok(CopyOutcome::NotCopyable);
        }
        let base = azure_blob_base(account, endpoint.as_deref());
        let enc_src = crate::reqsign::uri_encode(src_key, true);
        let enc_dst = crate::reqsign::uri_encode(dst_key, true);
        let src_url = format!("{base}/{}/{enc_src}", src.location);
        let dst_url = format!("{base}/{container}/{enc_dst}");
        let date = crate::reqsign::rfc1123(copy_now());
        let ms_headers: Vec<(String, String)> = vec![
            ("x-ms-copy-source".to_string(), src_url),
            ("x-ms-date".to_string(), date),
            ("x-ms-version".to_string(), AZURE_COPY_VERSION.to_string()),
        ];
        let authorization = match shared_key {
            Some(key) => {
                let resource = format!("/{container}/{enc_dst}");
                crate::reqsign::azure_shared_key_authorization(
                    account,
                    key,
                    "PUT",
                    &resource,
                    &ms_headers,
                )?
            }
            None => {
                let cred = creds
                    .get_credential()
                    .await
                    .map_err(|e| anyhow!("resolve Azure credential for copy: {e}"))?;
                match &*cred {
                    AzureCredential::BearerToken(token) => format!("Bearer {token}"),
                    // A SAS token cannot originate a Copy Blob signature here.
                    _ => return Ok(CopyOutcome::NotCopyable),
                }
            }
        };
        let mut req = client
            .put(&dst_url)
            .header("content-length", "0")
            .header("authorization", authorization);
        for (name, value) in &ms_headers {
            req = req.header(name.as_str(), value.as_str());
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("Azure Copy Blob PUT {dst_url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "Azure Copy Blob to {dst_key} failed: HTTP {status}: {}",
                clip(&body, 400)
            );
        }
        match resp
            .headers()
            .get("x-ms-copy-status")
            .and_then(|v| v.to_str().ok())
        {
            Some("success") => Ok(CopyOutcome::Copied),
            other => bail!(
                "Azure Copy Blob to {dst_key} did not complete synchronously (x-ms-copy-status: {})",
                other.unwrap_or("<none>")
            ),
        }
    }

    /// A logical key as an object_store path, rooted under the storage prefix.
    /// Our keys carry no leading/trailing or doubled slashes, so this
    /// round-trips exactly. The empty key addresses the prefix root itself.
    fn oskey(&self, key: &str) -> OsPath {
        match &self.prefix {
            Some(p) => OsPath::from(format!("{p}/{key}")),
            None => OsPath::from(key),
        }
    }

    /// Inverse of [`Self::oskey`]: a store location back to a logical key.
    /// `None` when the location lies outside the prefix — listings are always
    /// prefix-scoped, so that means a store returned something we never wrote.
    fn unkey<'a>(&self, loc: &'a str) -> Option<&'a str> {
        match &self.prefix {
            Some(p) => loc.strip_prefix(p.as_str())?.strip_prefix('/'),
            None => Some(loc),
        }
    }

    /// Classify a non-typed object_store error into an `anyhow` error: a missing
    /// bucket/container becomes the [`BucketUnavailable`] fail-over signal (so the
    /// health controller can move selection); every other error keeps its
    /// operation context. Used where the store call has no "missing object" case.
    fn store_err(&self, error: OsError, op: &str, key: impl std::fmt::Display) -> anyhow::Error {
        if object_store_is_missing_bucket(&error) {
            bucket_unavailable(self.backend, &error)
        } else {
            anyhow::Error::from(error).context(format!("{}: {op} {key}", self.backend))
        }
    }

    /// Classify a GET-family error where a typed `NotFound` means "missing
    /// object". `None` says the object is simply absent — the caller decides what
    /// that means (404, empty listing, skip); `Some` is a fatal error already
    /// wrapped with the bucket-outage signal or operation context. Note the
    /// asymmetry with [`Self::store_err`]: here a missing bucket is only surfaced
    /// through the typed `NotFound` path, so a non-`NotFound` error is never
    /// reclassified as a bucket outage.
    fn classify_get(
        &self,
        error: OsError,
        op: &str,
        key: impl std::fmt::Display,
    ) -> Option<anyhow::Error> {
        match error {
            e @ OsError::NotFound { .. } if object_store_is_missing_bucket(&e) => {
                Some(bucket_unavailable(self.backend, &e))
            }
            OsError::NotFound { .. } => None,
            e => Some(anyhow::Error::from(e).context(format!("{}: {op} {key}", self.backend))),
        }
    }

    /// Drain a listing `stream`, keeping objects whose logical key starts with
    /// the exact byte `prefix` (object_store lists by directory; the byte-prefix
    /// filter is ours). Stops after `limit` matches — pass `usize::MAX` for an
    /// unbounded listing. Objects arrive in ascending key order; callers wanting
    /// a total order sort the result themselves.
    async fn list_matching(
        &self,
        mut stream: impl futures::Stream<Item = object_store::Result<object_store::ObjectMeta>> + Unpin,
        prefix: &str,
        op: &str,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let m = match item {
                Ok(meta) => meta,
                Err(error) => return Err(self.store_err(error, op, prefix)),
            };
            let Some(key) = self.unkey(m.location.as_ref()) else {
                continue;
            };
            if key.starts_with(prefix) {
                out.push(ObjectMeta {
                    key: key.to_string(),
                    size: m.size,
                    etag: pack_version(&m.e_tag, &m.version),
                });
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// The enclosing directory object_store should list, plus whether to scope
    /// the listing at all, for our raw byte-prefix contract. A storage prefix
    /// always scopes the listing (even at the tree root — listing the whole
    /// bucket would return objects that aren't ours).
    fn list_dir_prefix(&self, prefix: &str) -> Option<OsPath> {
        // object_store's list() treats the prefix as a directory (it appends a
        // '/'), but our contract is a raw byte prefix: SHARD_CHARS passes
        // "packages/a" to match "packages/alpha". So list the enclosing
        // directory and filter by the exact byte prefix. A trailing-slash
        // prefix ("packages/foo/") lists only that directory; a sharded prefix
        // ("packages/a") lists "packages/" — the audit fans those out across
        // shards in parallel.
        let dir = match prefix.rfind('/') {
            Some(i) => &prefix[..=i],
            None => "",
        };
        (!dir.is_empty() || self.prefix.is_some()).then(|| self.oskey(dir))
    }

    /// GET the whole object as a 200 response.
    async fn full_response(&self, path: &OsPath, key: &str) -> Result<Response<Body>> {
        let res = match self.store.get(path).await {
            Ok(r) => r,
            Err(e) => match self.classify_get(e, "get", key) {
                None => return Err(NotFound(key.to_string()).into()),
                Some(err) => return Err(err),
            },
        };
        let size = res.meta.size;
        let ct = content_type_of(&res.attributes);
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, size)
            .header(header::CONTENT_TYPE, ct)
            .header(header::ACCEPT_RANGES, "bytes")
            .body(Body::from_stream(res.into_stream()))?)
    }

    /// Stream a spooled file into a multipart upload at `staging`, bounding
    /// resident memory to a few parts in flight. Aborts on any error so no
    /// orphaned parts linger billable.
    async fn stream_multipart(
        &self,
        staging: &OsPath,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<()> {
        // Open the local spool BEFORE initiating the multipart upload: a failure
        // here (spool vanished, EMFILE) must not leave an initiated multipart with
        // orphan parts billing forever, since we'd drop the writer without abort().
        let mut file = fs::File::open(path)
            .await
            .with_context(|| format!("open upload spool {}", path.display()))?;
        let opts = PutMultipartOptions::from(ct_attrs(content_type));
        let upload = self
            .store
            .put_multipart_opts(staging, opts)
            .await
            .map_err(|error| self.store_err(error, "begin multipart", staging))?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, MULTIPART_PART_SIZE);
        let mut buf = vec![0u8; READ_CHUNK];
        loop {
            let n = match file.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    let _ = writer.abort().await;
                    return Err(anyhow::Error::from(e).context("read upload spool"));
                }
            };
            if let Err(e) = writer.wait_for_capacity(MULTIPART_CONCURRENCY).await {
                let _ = writer.abort().await;
                return Err(anyhow::Error::from(e).context("multipart part upload"));
            }
            writer.write(&buf[..n]);
        }
        writer
            .finish()
            .await
            .with_context(|| format!("{}: finish multipart {staging}", self.backend))?;
        Ok(())
    }
}

#[async_trait]
impl Storage for ObjectStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        match self.store.head(&self.oskey(key)).await {
            Ok(_) => Ok(true),
            Err(e) => match self.classify_get(e, "head", key) {
                None => Ok(false),
                Some(err) => Err(err),
            },
        }
    }

    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        match self.store.head(&self.oskey(key)).await {
            Ok(meta) => Ok(Some(meta.size)),
            Err(e) => match self.classify_get(e, "head", key) {
                None => Ok(None),
                Some(err) => Err(err),
            },
        }
    }

    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>> {
        let Some(signer) = &self.signer else {
            return Ok(None);
        };
        let url = signer
            .signed_url(reqwest::Method::GET, &self.oskey(key), expires)
            .await
            .with_context(|| format!("{}: presign {key}", self.backend))?;
        Ok(Some(url.to_string()))
    }

    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        let path = self.oskey(key);
        let Some(raw_range) = range else {
            return self.full_response(&path, key).await;
        };
        // A range needs the size to build Content-Range and to reject an
        // unsatisfiable range with 416 — one HEAD, only on ranged requests.
        let size = match self.store.head(&path).await {
            Ok(m) => m.size,
            Err(e) => match self.classify_get(e, "head", key) {
                None => return Err(NotFound(key.to_string()).into()),
                Some(err) => return Err(err),
            },
        };
        match parse_range(Some(raw_range), size) {
            RangeSpec::Full => self.full_response(&path, key).await,
            RangeSpec::Unsatisfiable => Ok(Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{size}"))
                .body(Body::empty())?),
            RangeSpec::Partial(start, end) => {
                let opts = GetOptions {
                    range: Some(GetRange::Bounded(start..end + 1)),
                    ..Default::default()
                };
                let res = match self.store.get_opts(&path, opts).await {
                    Ok(r) => r,
                    Err(e) => match self.classify_get(e, "get", key) {
                        None => return Err(NotFound(key.to_string()).into()),
                        Some(err) => return Err(err),
                    },
                };
                let ct = content_type_of(&res.attributes);
                let len = end - start + 1;
                Ok(Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_LENGTH, len)
                    .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"))
                    .header(header::CONTENT_TYPE, ct)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(Body::from_stream(res.into_stream()))?)
            }
        }
    }

    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        let opts = PutOptions {
            mode: PutMode::Overwrite,
            attributes: ct_attrs(content_type),
            ..Default::default()
        };
        self.store
            .put_opts(&self.oskey(key), PutPayload::from(bytes), opts)
            .await
            .map_err(|error| self.store_err(error, "put", key))?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let opts = PutOptions {
            mode: PutMode::Create,
            attributes: ct_attrs(content_type),
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.oskey(key), PutPayload::from(bytes), opts)
            .await
        {
            Ok(_) => Ok(true),
            Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => Ok(false),
            Err(e) => Err(self.store_err(e, "put_if_absent", key)),
        }
    }

    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<bool> {
        let size = fs::metadata(path)
            .await
            .with_context(|| format!("stat upload spool {}", path.display()))?
            .len();
        if size <= MULTIPART_THRESHOLD {
            // Small enough to create with one conditional PUT.
            let bytes = fs::read(path)
                .await
                .with_context(|| format!("read upload spool {}", path.display()))?;
            return self.put_if_absent(key, bytes, content_type).await;
        }
        // Too big for a single PUT: stream to a unique staging key (bounded
        // RSS), then publish atomically. copy_if_not_exists is the race-free
        // create-if-absent for large objects — native on GCS/Azure, a
        // multipart copy on S3.
        let staging = self.oskey(&staging_key(key));
        self.stream_multipart(&staging, path, content_type).await?;
        let outcome = match self
            .store
            .copy_if_not_exists(&staging, &self.oskey(key))
            .await
        {
            Ok(()) => Ok(true),
            Err(OsError::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(self.store_err(e, "publish", key)),
        };
        let _ = self.store.delete(&staging).await;
        outcome
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        match self.store.get(&self.oskey(key)).await {
            Ok(res) => Ok(res
                .bytes()
                .await
                .with_context(|| format!("{}: read {key}", self.backend))?
                .to_vec()),
            Err(e) => match self.classify_get(e, "get", key) {
                None => Err(NotFound(key.to_string()).into()),
                Some(err) => Err(err),
            },
        }
    }

    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        // list_with_delimiter is the directory listing: immediate files in
        // `objects`, sub-directories in `common_prefixes` (which we drop). A
        // missing prefix is an empty listing, not an error.
        let res = match self
            .store
            .list_with_delimiter(Some(&self.oskey(dir_prefix)))
            .await
        {
            Ok(result) => result,
            Err(error) => return Err(self.store_err(error, "list", dir_prefix)),
        };
        let mut entries: Vec<FileEntry> = res
            .objects
            .into_iter()
            .filter_map(|m| {
                Some(FileEntry {
                    key: self.unkey(m.location.as_ref())?.to_string(),
                    size: m.size,
                    last_modified: OffsetDateTime::from_unix_timestamp(m.last_modified.timestamp())
                        .ok()
                        .and_then(|t| t.format(&Rfc3339).ok()),
                })
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(entries)
    }

    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let list_prefix = self.list_dir_prefix(prefix);
        let stream = self.store.list(list_prefix.as_ref());
        let mut out = self
            .list_matching(stream, prefix, "list_all", usize::MAX)
            .await?;
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        let list_prefix = self.list_dir_prefix(prefix);
        // Native start-after: the client passes `offset` to ListObjectsV2, so a
        // later page never re-lists earlier keys. Objects arrive in ascending
        // key order, so the first `limit` matches are exactly the next page.
        let offset = after.map(|a| self.oskey(a));
        let stream = match &offset {
            Some(offset) => self.store.list_with_offset(list_prefix.as_ref(), offset),
            None => self.store.list(list_prefix.as_ref()),
        };
        self.list_matching(stream, prefix, "list_page", limit).await
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        for k in keys {
            match self.store.delete(&self.oskey(k)).await {
                Ok(()) => {}
                Err(error) => match self.classify_get(error, "delete", k) {
                    None => {}
                    Some(err) => return Err(err),
                },
            }
        }
        Ok(())
    }

    fn supports_leases(&self) -> bool {
        true
    }

    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        match self.store.get(&self.oskey(key)).await {
            Ok(res) => {
                let etag = pack_version(&res.meta.e_tag, &res.meta.version);
                let bytes = res
                    .bytes()
                    .await
                    .with_context(|| format!("{}: read {key}", self.backend))?
                    .to_vec();
                Ok(Some((bytes, etag)))
            }
            Err(e) => match self.classify_get(e, "get", key) {
                None => Ok(None),
                Some(err) => Err(err),
            },
        }
    }

    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        match self
            .store
            .put_opts(
                &self.oskey(key),
                PutPayload::from(bytes),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(res) => Ok(Some(pack_version(&res.e_tag, &res.version))),
            Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => Ok(None),
            Err(e) => Err(self.store_err(e, "put_if_none_match", key)),
        }
    }

    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        let opts = PutOptions::from(PutMode::Update(unpack_version(etag)));
        match self
            .store
            .put_opts(&self.oskey(key), PutPayload::from(bytes), opts)
            .await
        {
            Ok(res) => Ok(Some(pack_version(&res.e_tag, &res.version))),
            // A failed precondition, a concurrent conditional write, or a
            // since-deleted object: we lost, cleanly.
            Err(OsError::Precondition { .. } | OsError::AlreadyExists { .. }) => Ok(None),
            Err(e) => match self.classify_get(e, "put_if_match", key) {
                None => Ok(None),
                Some(err) => Err(err),
            },
        }
    }

    fn copy_origin(&self) -> Option<CopyOrigin> {
        Some(match self.copy.as_ref()? {
            CopyBackend::S3 {
                bucket, endpoint, ..
            } => CopyOrigin {
                provider: CopyProvider::S3,
                location: bucket.clone(),
                endpoint: endpoint.clone(),
                account: None,
            },
            CopyBackend::Gcs {
                bucket, endpoint, ..
            } => CopyOrigin {
                provider: CopyProvider::Gcs,
                location: bucket.clone(),
                endpoint: endpoint.clone(),
                account: None,
            },
            CopyBackend::Azure {
                container,
                account,
                endpoint,
                ..
            } => CopyOrigin {
                provider: CopyProvider::Azure,
                location: container.clone(),
                endpoint: endpoint.clone(),
                account: Some(account.clone()),
            },
        })
    }

    async fn copy_credential_identity(&self) -> Result<Option<String>> {
        match self.copy.as_ref() {
            Some(CopyBackend::S3 { creds, .. }) => {
                let cred = creds
                    .get_credential()
                    .await
                    .map_err(|e| anyhow!("resolve S3 credential identity: {e}"))?;
                Ok(Some(cred.key_id.clone()))
            }
            Some(CopyBackend::Azure { account, .. }) => Ok(Some(account.clone())),
            Some(CopyBackend::Gcs { .. }) | None => Ok(None),
        }
    }

    async fn server_side_copy(
        &self,
        src: &CopyOrigin,
        src_key: &str,
        dst_key: &str,
        expected_size: u64,
    ) -> Result<CopyOutcome> {
        let Some(copy) = self.copy.as_ref() else {
            return Ok(CopyOutcome::NotCopyable);
        };
        let src_key = self.prefixed(src_key);
        let dst_key = self.prefixed(dst_key);
        match (copy, src.provider) {
            (CopyBackend::S3 { .. }, CopyProvider::S3) => {
                self.s3_copy(copy, &src.location, &src_key, &dst_key, expected_size)
                    .await
            }
            (CopyBackend::Gcs { .. }, CopyProvider::Gcs) => {
                self.gcs_copy(copy, &src.location, &src_key, &dst_key).await
            }
            (CopyBackend::Azure { .. }, CopyProvider::Azure) => {
                self.azure_copy(copy, src, &src_key, &dst_key).await
            }
            _ => Ok(CopyOutcome::NotCopyable),
        }
    }
}

/// Normalize a user-supplied storage prefix into a bare key prefix carrying no
/// leading, trailing, or doubled slashes — the form `ObjectStorage` prepends.
/// Rejects traversal and empty segments so a prefix cannot escape itself.
pub(crate) fn normalize_prefix(raw: &str) -> Result<String> {
    let p = raw.trim().trim_matches('/');
    if p.is_empty() {
        return Err(anyhow!("--storage-prefix must not be empty"));
    }
    if p.split('/')
        .any(|seg| seg.is_empty() || seg == "." || seg == "..")
    {
        return Err(anyhow!(
            "--storage-prefix must not contain empty, '.', or '..' segments: {raw:?}"
        ));
    }
    Ok(p.to_string())
}

/// Best-effort content type as object_store attributes (ignored by stores that
/// don't support it).
fn ct_attrs(content_type: Option<&str>) -> Attributes {
    let mut a = Attributes::new();
    if let Some(ct) = content_type {
        a.insert(Attribute::ContentType, ct.to_string().into());
    }
    a
}

fn content_type_of(attrs: &Attributes) -> String {
    attrs
        .get(&Attribute::ContentType)
        .map(|v| v.as_ref().to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

/// A unique staging key for a large upload, namespaced by its final filename.
fn staging_key(key: &str) -> String {
    let fname = key.rsplit('/').next().unwrap_or(key);
    let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
    format!("{STAGING_PREFIX}{nanos}-{}-{fname}", std::process::id())
}

fn pack_version(e_tag: &Option<String>, version: &Option<String>) -> String {
    format!(
        "{}{VERSION_SEP}{}",
        e_tag.as_deref().unwrap_or(""),
        version.as_deref().unwrap_or("")
    )
}

fn unpack_version(packed: &str) -> UpdateVersion {
    let (e, v) = packed.split_once(VERSION_SEP).unwrap_or((packed, ""));
    UpdateVersion {
        e_tag: (!e.is_empty()).then(|| e.to_string()),
        version: (!v.is_empty()).then(|| v.to_string()),
    }
}

/// ---------------------------- FaultInjectStorage ---------------------------
/// Crash-point injection for the chaos tests: delegates everything, but
/// aborts the whole process immediately *before* the Nth mutating operation.
/// Sweeping N over a scenario's write count exercises a crash in every gap of
/// the write protocol; recovery + `pypiron verify-index` then prove convergence.
pub struct FaultInjectStorage {
    inner: Arc<dyn Storage>,
    remaining: std::sync::atomic::AtomicI64,
}

impl FaultInjectStorage {
    pub fn new(inner: Arc<dyn Storage>, abort_after: i64) -> Self {
        Self {
            inner,
            remaining: std::sync::atomic::AtomicI64::new(abort_after),
        }
    }

    fn count_mutation(&self, op: &str, key: &str) {
        let left = self
            .remaining
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if left <= 0 {
            eprintln!("fault injection: aborting before {op} {key}");
            std::process::abort();
        }
    }
}

#[async_trait]
impl Storage for FaultInjectStorage {
    async fn head_exists(&self, key: &str) -> Result<bool> {
        self.inner.head_exists(key).await
    }
    async fn stored_size(&self, key: &str) -> Result<Option<u64>> {
        self.inner.stored_size(key).await
    }
    async fn serve_artifact(&self, key: &str, range: Option<&str>) -> Result<Response<Body>> {
        self.inner.serve_artifact(key, range).await
    }
    async fn presign_get(&self, key: &str, expires: std::time::Duration) -> Result<Option<String>> {
        self.inner.presign_get(key, expires).await
    }
    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get_bytes(key).await
    }
    async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
        self.inner.list_dir_entries(dir_prefix).await
    }
    async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.inner.list_all(prefix).await
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        self.inner.list_page(prefix, after, limit).await
    }
    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }
    async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.inner.get_with_etag(key).await
    }

    async fn put_bytes(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        self.count_mutation("put_bytes", key);
        self.inner.put_bytes(key, bytes, content_type).await
    }
    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<bool> {
        self.count_mutation("put_if_absent", key);
        self.inner.put_if_absent(key, bytes, content_type).await
    }
    async fn put_file_if_absent(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: Option<&str>,
    ) -> Result<bool> {
        self.count_mutation("put_file_if_absent", key);
        self.inner.put_file_if_absent(key, path, content_type).await
    }
    async fn delete_keys(&self, keys: &[String]) -> Result<()> {
        self.count_mutation("delete_keys", keys.first().map_or("", String::as_str));
        self.inner.delete_keys(keys).await
    }
    async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        self.count_mutation("put_if_none_match", key);
        self.inner.put_if_none_match(key, bytes).await
    }
    async fn put_if_match(&self, key: &str, etag: &str, bytes: Vec<u8>) -> Result<Option<String>> {
        self.count_mutation("put_if_match", key);
        self.inner.put_if_match(key, etag, bytes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3_origin(bucket: &str, endpoint: Option<&str>) -> CopyOrigin {
        CopyOrigin {
            provider: CopyProvider::S3,
            location: bucket.to_string(),
            endpoint: endpoint.map(String::from),
            account: None,
        }
    }

    #[test]
    fn copy_eligibility_real_aws_is_cross_region_within_an_account() {
        // Two real-AWS buckets (no custom endpoint), same access key — copyable
        // even across regions (CopyObject resolves the source region itself).
        let a = s3_origin("bucket-a", None);
        let b = s3_origin("bucket-b", None);
        assert!(copy_pair_eligible(&a, Some("AKIA"), &b, Some("AKIA")));
        // Different access keys (separate accounts): not eligible.
        assert!(!copy_pair_eligible(&a, Some("AKIA"), &b, Some("AKIB")));
    }

    #[test]
    fn copy_eligibility_distinct_custom_endpoints_are_not_copyable() {
        // Two separate MinIO clusters: same provider, different endpoints — no.
        let a = s3_origin("b", Some("http://minio-a:9000"));
        let b = s3_origin("b2", Some("http://minio-b:9000"));
        assert!(!copy_pair_eligible(&a, Some("k"), &b, Some("k")));
        // One MinIO, two buckets (same endpoint): copyable.
        let c = s3_origin("b1", Some("http://minio:9000"));
        let d = s3_origin("b2", Some("http://minio:9000"));
        assert!(copy_pair_eligible(&c, Some("k"), &d, Some("k")));
    }

    #[test]
    fn copy_eligibility_rejects_cross_provider_and_cross_account_azure() {
        let s3 = s3_origin("b", None);
        let gcs = CopyOrigin {
            provider: CopyProvider::Gcs,
            location: "b".to_string(),
            endpoint: None,
            account: None,
        };
        assert!(!copy_pair_eligible(&s3, Some("k"), &gcs, Some("k")));
        let az1 = CopyOrigin {
            provider: CopyProvider::Azure,
            location: "c1".to_string(),
            endpoint: None,
            account: Some("acct1".to_string()),
        };
        let az2 = CopyOrigin {
            provider: CopyProvider::Azure,
            location: "c2".to_string(),
            endpoint: None,
            account: Some("acct2".to_string()),
        };
        // Copy Blob is same-account only.
        assert!(!copy_pair_eligible(&az1, None, &az2, None));
        let az2_same = CopyOrigin {
            account: Some("acct1".to_string()),
            ..az2
        };
        assert!(copy_pair_eligible(&az1, None, &az2_same, None));
    }

    #[test]
    fn s3_copy_target_virtual_hosted_vs_path_style() {
        // Real AWS: virtual-hosted, regional host, key at the root.
        let (url, host, uri) =
            s3_copy_target(true, None, "iron-east", "us-west-2", "packages/p/p-1.whl").unwrap();
        assert_eq!(
            url,
            "https://iron-east.s3.us-west-2.amazonaws.com/packages/p/p-1.whl"
        );
        assert_eq!(host, "iron-east.s3.us-west-2.amazonaws.com");
        assert_eq!(uri, "/packages/p/p-1.whl");
        // MinIO: path-style, host is the endpoint authority, bucket in the path.
        let (url, host, uri) = s3_copy_target(
            false,
            Some("http://127.0.0.1:9000"),
            "b",
            "us-east-1",
            "packages/p/p-1.whl",
        )
        .unwrap();
        assert_eq!(url, "http://127.0.0.1:9000/b/packages/p/p-1.whl");
        assert_eq!(host, "127.0.0.1:9000");
        assert_eq!(uri, "/b/packages/p/p-1.whl");
        // Path-style with no endpoint is a misconfiguration.
        assert!(s3_copy_target(false, None, "b", "us-east-1", "k").is_err());
    }

    #[test]
    fn gcs_rewrite_url_encodes_object_slashes() {
        let url = gcs_rewrite_url(
            "https://storage.googleapis.com/",
            "src-bucket",
            "packages/p/p-1.whl",
            "dst-bucket",
            "packages/p/p-1.whl",
        );
        assert_eq!(
            url,
            "https://storage.googleapis.com/storage/v1/b/src-bucket/o/packages%2Fp%2Fp-1.whl/rewriteTo/b/dst-bucket/o/packages%2Fp%2Fp-1.whl"
        );
    }

    #[test]
    fn azure_blob_base_uses_account_or_endpoint() {
        assert_eq!(
            azure_blob_base("iron", None),
            "https://iron.blob.core.windows.net"
        );
        assert_eq!(
            azure_blob_base(
                "devstoreaccount1",
                Some("http://127.0.0.1:10000/devstoreaccount1/")
            ),
            "http://127.0.0.1:10000/devstoreaccount1"
        );
    }

    #[test]
    fn copy_origin_handle_key_is_distinct_per_bucket() {
        assert_ne!(
            s3_origin("a", None).handle_key(),
            s3_origin("b", None).handle_key()
        );
        assert_eq!(
            s3_origin("a", None).handle_key(),
            s3_origin("a", None).handle_key()
        );
    }

    fn storage_args() -> StorageArgs {
        StorageArgs {
            data_dir: None,
            storage_prefix: None,
            buckets: Vec::new(),
            s3_endpoint_url: None,
            s3_force_path_style: false,
            gcs_service_account_path: None,
            gcs_endpoint_url: None,
            azure_account: None,
            azure_access_key: None,
            azure_endpoint_url: None,
            azure_use_emulator: false,
            overrides: HashMap::new(),
        }
    }

    fn args_with(buckets: &[&str], overrides: &[(&str, BucketOverride)]) -> StorageArgs {
        let mut a = storage_args();
        a.buckets = buckets.iter().map(|s| s.to_string()).collect();
        a.overrides = overrides
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        a
    }

    #[test]
    fn override_key_outside_the_bucket_list_is_rejected() {
        let args = args_with(
            &["s3://real"],
            &[(
                "s3://typo",
                BucketOverride {
                    endpoint_url: Some("http://x".into()),
                    ..Default::default()
                },
            )],
        );
        let err = args.validate_override_keys().unwrap_err().to_string();
        assert!(err.contains("s3://typo"), "{err}");
        assert!(err.contains("s3://real"), "{err}");
    }

    #[test]
    fn two_override_keys_for_one_bucket_are_rejected() {
        // "s3://cache" and "s3://cache@us-west-2" collapse to one identity;
        // accepting both would let `override_for` pick a table by hash order.
        let args = args_with(
            &["s3://cache"],
            &[
                (
                    "s3://cache",
                    BucketOverride {
                        endpoint_url: Some("http://a".into()),
                        ..Default::default()
                    },
                ),
                (
                    "s3://cache@us-west-2",
                    BucketOverride {
                        endpoint_url: Some("http://b".into()),
                        ..Default::default()
                    },
                ),
            ],
        );
        let err = args.validate_override_keys().unwrap_err().to_string();
        assert!(err.contains("s3://cache"), "{err}");
        assert!(err.contains("both resolve to bucket"), "{err}");
    }

    #[test]
    fn override_key_matches_bucket_ignoring_region() {
        // A key carrying @region resolves to the same identity as the plain URI.
        let args = args_with(
            &["s3://cache@us-west-2"],
            &[(
                "s3://cache",
                BucketOverride {
                    endpoint_url: Some("http://minio:9000".into()),
                    ..Default::default()
                },
            )],
        );
        args.validate_override_keys()
            .expect("region is not identity");
        let spec = parse_bucket_uri("s3://cache@us-west-2").unwrap();
        let ov = args
            .override_for(&spec)
            .unwrap()
            .expect("resolved by identity");
        assert_eq!(ov.endpoint_url.as_deref(), Some("http://minio:9000"));
    }

    #[test]
    fn wrong_scheme_override_field_is_rejected() {
        // `account` is Azure-only; on an s3:// bucket it must fail closed.
        let args = args_with(
            &["s3://b"],
            &[(
                "s3://b",
                BucketOverride {
                    account: Some("acct".into()),
                    ..Default::default()
                },
            )],
        );
        let spec = parse_bucket_uri("s3://b").unwrap();
        let err = args.override_for(&spec).unwrap_err().to_string();
        assert!(err.contains("account"), "{err}");
        assert!(err.contains("s3://b"), "{err}");
    }

    #[test]
    fn env_prefix_on_gcs_is_rejected() {
        let args = args_with(
            &["gs://g"],
            &[(
                "gs://g",
                BucketOverride {
                    env_prefix: Some("P_".into()),
                    ..Default::default()
                },
            )],
        );
        let spec = parse_bucket_uri("gs://g").unwrap();
        let err = args.override_for(&spec).unwrap_err().to_string();
        assert!(err.contains("env-prefix"), "{err}");
        assert!(err.contains("service-account-path"), "{err}");
    }

    #[test]
    fn multi_s3_transport_disables_retries_without_a_short_transfer_deadline() {
        let retry = failover_retry_config();
        assert_eq!(retry.max_retries, 0);
        assert_eq!(retry.retry_timeout, FAILOVER_REQUEST_TIMEOUT);

        let builder = bound_s3_transport(AmazonS3Builder::new().with_bucket_name("packages"));
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Client(ClientConfigKey::ConnectTimeout)),
            Some("2s".to_string())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Client(ClientConfigKey::Timeout)),
            Some("3600s".to_string())
        );
        // No explicit provider means the builder still constructs its normal
        // WebIdentity/ECS/EKS/IMDS chain lazily. Static credentials also remain
        // accepted. The bounded retry policy changes requests, not discovery.
        builder.build().unwrap();
        bound_s3_transport(
            AmazonS3Builder::new()
                .with_bucket_name("packages")
                .with_access_key_id("test")
                .with_secret_access_key("test"),
        )
        .build()
        .unwrap();
    }

    #[test]
    fn no_buckets_is_disk_at_the_data_dir() {
        let mut args = storage_args();
        args.data_dir = Some("/data".to_string());
        assert_eq!(args.bucket_names(), ["/data"]);
        assert_eq!(args.describe(), "disk · /data");
    }

    #[test]
    fn bucket_uris_parse_backend_name_and_region() {
        assert_eq!(
            parse_bucket_uri("s3://iron-east@us-east-1").unwrap(),
            BucketSpec {
                scheme: BucketScheme::S3,
                name: "iron-east".to_string(),
                region: Some("us-east-1".to_string()),
            }
        );
        assert_eq!(
            parse_bucket_uri(" gs://iron-backup ").unwrap(),
            BucketSpec {
                scheme: BucketScheme::Gcs,
                name: "iron-backup".to_string(),
                region: None,
            }
        );
        // `@region` is a plain annotation on every scheme, not just S3.
        assert_eq!(
            parse_bucket_uri("gs://iron-backup@us-central1").unwrap(),
            BucketSpec {
                scheme: BucketScheme::Gcs,
                name: "iron-backup".to_string(),
                region: Some("us-central1".to_string()),
            }
        );
        assert_eq!(
            parse_bucket_uri("az://ironblob@eastus").unwrap(),
            BucketSpec {
                scheme: BucketScheme::Azure,
                name: "ironblob".to_string(),
                region: Some("eastus".to_string()),
            }
        );
    }

    #[test]
    fn bucket_uris_reject_bad_entries_by_name() {
        for (entry, needle) in [
            ("iron-east", "missing a scheme"),
            ("ftp://iron-east", "unknown scheme"),
            ("s3://iron@", "empty region"),
            ("gs://iron@", "empty region"),
            ("s3://", "empty bucket name"),
        ] {
            let error = parse_bucket_uri(entry).unwrap_err().to_string();
            assert!(
                error.contains(needle) && error.contains(entry.trim()),
                "entry {entry:?} error {error:?} should mention {needle:?}"
            );
        }
    }

    #[test]
    fn bucket_names_come_from_the_parsed_uri_list() {
        let mut args = storage_args();
        args.buckets = vec![
            "s3://iron-east@us-east-1".to_string(),
            "gs://iron-backup".to_string(),
            "az://ironblob".to_string(),
        ];
        assert_eq!(
            args.bucket_names(),
            ["s3://iron-east", "gs://iron-backup", "az://ironblob"]
        );
    }

    #[test]
    fn same_name_across_backends_is_a_distinct_identity() {
        let mut args = storage_args();
        args.buckets = vec!["s3://shared".to_string(), "gs://shared".to_string()];
        // A legal mixed list: same name, different backend. The scheme-qualified
        // identities differ, so nothing downstream mistakes them for a duplicate.
        assert_eq!(args.bucket_names(), ["s3://shared", "gs://shared"]);
    }

    #[test]
    fn annotated_region_does_not_change_bucket_identity() {
        // The topology stamp hashes bucket identity, which must be byte-identical
        // whether or not a `@region` annotation is present, on every scheme.
        for (annotated, bare) in [
            ("s3://iron@us-east-1", "s3://iron"),
            ("gs://iron@us-central1", "gs://iron"),
            ("az://iron@eastus", "az://iron"),
        ] {
            assert_eq!(
                parse_bucket_uri(annotated).unwrap().identity(),
                parse_bucket_uri(bare).unwrap().identity(),
                "region annotation on {annotated:?} must not change identity"
            );
        }
    }

    #[test]
    fn distinguishes_missing_bucket_from_missing_object_text() {
        assert!(message_is_missing_bucket(
            "404 <Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message>"
        ));
        // Azure reports a missing container as the same container-level outage.
        assert!(message_is_missing_bucket(
            "404 <Code>ContainerNotFound</Code><Message>The specified container does not exist.</Message>"
        ));
        assert!(!message_is_missing_bucket(
            "Object at location packages/no-such-bucket.whl not found"
        ));
        assert!(!message_is_missing_bucket(
            "Object at location packages/NoSuchBucket.whl not found"
        ));
    }

    #[test]
    fn normalize_prefix_strips_slashes_and_rejects_traversal() {
        assert_eq!(normalize_prefix("pypi").unwrap(), "pypi");
        assert_eq!(normalize_prefix("/pypi/").unwrap(), "pypi");
        assert_eq!(normalize_prefix(" a/b ").unwrap(), "a/b");
        for bad in ["", "/", "   ", "a//b", "../etc", "a/../b", "a/./b"] {
            assert!(normalize_prefix(bad).is_err(), "expected reject: {bad:?}");
        }
    }

    #[test]
    fn oskey_and_unkey_round_trip() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let keyed =
            |p: Option<&str>| ObjectStorage::new(store.clone(), None, p.map(String::from), "mem");

        let plain = keyed(None);
        assert_eq!(plain.oskey("packages/a/x.whl").as_ref(), "packages/a/x.whl");
        assert_eq!(plain.unkey("packages/a/x.whl"), Some("packages/a/x.whl"));

        let pfx = keyed(Some("pypi"));
        assert_eq!(
            pfx.oskey("packages/a/x.whl").as_ref(),
            "pypi/packages/a/x.whl"
        );
        assert_eq!(pfx.unkey("pypi/packages/a/x.whl"), Some("packages/a/x.whl"));
        // The empty key addresses the prefix root, not the bucket root.
        assert_eq!(pfx.oskey("").as_ref(), "pypi");
        // A sibling that merely shares a name stem is not ours.
        assert_eq!(pfx.unkey("pypi-other/packages/x"), None);
        assert_eq!(pfx.unkey("other/x"), None);
    }

    #[tokio::test]
    async fn disk_list_all_walks_filters_and_detects_change() {
        let dir = std::env::temp_dir().join(format!("pypiron-listall-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let s = DiskStorage::new(&dir);
        s.put_bytes("packages/alpha/a-1.0.tar.gz", b"x".to_vec(), None)
            .await
            .unwrap();
        s.put_bytes(
            "packages/alpha/a-1.0.tar.gz.meta.json",
            b"{}".to_vec(),
            None,
        )
        .await
        .unwrap();
        s.put_bytes("packages/beta/b-1.0.tar.gz", b"y".to_vec(), None)
            .await
            .unwrap();

        let all = s.list_all("packages/").await.unwrap();
        assert_eq!(
            all.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
            [
                "packages/alpha/a-1.0.tar.gz",
                "packages/alpha/a-1.0.tar.gz.meta.json",
                "packages/beta/b-1.0.tar.gz",
            ]
        );

        // Sharded key prefix: only the matching first-level subtree.
        let shard = s.list_all("packages/a").await.unwrap();
        assert_eq!(shard.len(), 2);
        assert!(shard.iter().all(|o| o.key.starts_with("packages/alpha/")));
        assert!(s.list_all("packages/z").await.unwrap().is_empty());
        assert!(s.list_all("nope/").await.unwrap().is_empty());

        // Rewriting an object must change its etag.
        let before = all[0].etag.clone();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        s.put_bytes("packages/alpha/a-1.0.tar.gz", b"xx".to_vec(), None)
            .await
            .unwrap();
        let after = &s.list_all("packages/alpha/a-1.0.tar.gz").await.unwrap()[0];
        assert_ne!(before, after.etag);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn version_token_round_trips_etag_and_generation() {
        // Stores express a conditional update with different fields: S3/Azure
        // use the ETag, GCS the generation. The opaque token must carry both
        // back into an UpdateVersion unchanged.
        for (etag, version) in [
            (Some("\"abc123\"".to_string()), None), // S3 / Azure: ETag only
            (Some("\"xyz\"".to_string()), Some("17".to_string())), // GCS: ETag + generation
            (None, Some("42".to_string())),         // generation only
            (None, None),                           // neither
        ] {
            let token = pack_version(&etag, &version);
            let back = unpack_version(&token);
            assert_eq!(back.e_tag, etag);
            assert_eq!(back.version, version);
        }
        // Distinct inputs produce distinct tokens (fingerprint equality).
        assert_ne!(
            pack_version(&Some("a".into()), &None),
            pack_version(&None, &Some("a".into())),
        );
    }

    #[test]
    fn artifact_write_timeout_scales_by_whole_mib() {
        use std::time::Duration;
        // Base floor for anything under a MiB (rounds down).
        assert_eq!(artifact_write_timeout(0), Duration::from_secs(60));
        assert_eq!(
            artifact_write_timeout(1024 * 1024 - 1),
            Duration::from_secs(60)
        );
        // One second added per whole MiB.
        assert_eq!(artifact_write_timeout(1024 * 1024), Duration::from_secs(61));
        // The 16 MiB blackbox drip (11s) sits well inside its budget.
        assert_eq!(
            artifact_write_timeout(16 * 1024 * 1024),
            Duration::from_secs(76)
        );
        // A pathological size saturates instead of overflowing the u32 multiply.
        assert!(artifact_write_timeout(u64::MAX) >= Duration::from_secs(60));
    }
}

/// Minimal in-memory Storage for unit tests across modules.
#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct InMemStorage {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        gets: AtomicUsize,
        fail_next_get: AtomicBool,
        /// Artificial `get_bytes` latency in milliseconds (0 = none). Lets a
        /// test hold one loader in flight long enough for concurrent readers to
        /// observe the single-flight refill claim.
        get_delay_ms: AtomicU64,
    }

    impl InMemStorage {
        pub fn insert(&self, key: &str, bytes: Vec<u8>) {
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
        }
        pub fn get_count(&self) -> usize {
            self.gets.load(Ordering::SeqCst)
        }
        pub fn fail_next_get(&self) {
            self.fail_next_get.store(true, Ordering::SeqCst);
        }
        pub fn set_get_delay(&self, delay: std::time::Duration) {
            self.get_delay_ms
                .store(delay.as_millis() as u64, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl Storage for InMemStorage {
        async fn head_exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        async fn serve_artifact(
            &self,
            _key: &str,
            _range: Option<&str>,
        ) -> Result<axum::response::Response<axum::body::Body>> {
            anyhow::bail!("serve_artifact not supported by InMemStorage")
        }
        async fn presign_get(
            &self,
            _key: &str,
            _expires: std::time::Duration,
        ) -> Result<Option<String>> {
            Ok(None)
        }
        async fn put_bytes(
            &self,
            key: &str,
            bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<()> {
            self.insert(key, bytes);
            Ok(())
        }
        async fn put_if_absent(
            &self,
            key: &str,
            bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bool> {
            let mut map = self.objects.lock().unwrap();
            if map.contains_key(key) {
                return Ok(false);
            }
            map.insert(key.to_string(), bytes);
            Ok(true)
        }
        async fn put_file_if_absent(
            &self,
            key: &str,
            path: &std::path::Path,
            content_type: Option<&str>,
        ) -> Result<bool> {
            let bytes = std::fs::read(path)?;
            self.put_if_absent(key, bytes, content_type).await
        }
        async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            if self.fail_next_get.swap(false, Ordering::SeqCst) {
                anyhow::bail!("injected storage failure");
            }
            let delay = self.get_delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| NotFound(key.to_string()).into())
        }
        async fn list_dir_entries(&self, dir_prefix: &str) -> Result<Vec<FileEntry>> {
            let map = self.objects.lock().unwrap();
            let mut out: Vec<FileEntry> = map
                .iter()
                .filter(|(k, _)| k.starts_with(dir_prefix) && !k[dir_prefix.len()..].contains('/'))
                .map(|(k, v)| FileEntry {
                    key: k.clone(),
                    size: v.len() as u64,
                    last_modified: Some("2026-01-01T00:00:00Z".to_string()),
                })
                .collect();
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        }
        async fn delete_keys(&self, keys: &[String]) -> Result<()> {
            let mut map = self.objects.lock().unwrap();
            for k in keys {
                map.remove(k);
            }
            Ok(())
        }
        async fn list_all(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
            let map = self.objects.lock().unwrap();
            let mut out: Vec<ObjectMeta> = map
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| ObjectMeta {
                    key: k.clone(),
                    size: v.len() as u64,
                    etag: test_etag(v),
                })
                .collect();
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        }
        fn supports_leases(&self) -> bool {
            true
        }
        async fn get_with_etag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
            if self.fail_next_get.swap(false, Ordering::SeqCst) {
                anyhow::bail!("injected storage failure");
            }
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .map(|b| (b.clone(), test_etag(b))))
        }
        async fn put_if_none_match(&self, key: &str, bytes: Vec<u8>) -> Result<Option<String>> {
            let mut map = self.objects.lock().unwrap();
            if map.contains_key(key) {
                return Ok(None);
            }
            let etag = test_etag(&bytes);
            map.insert(key.to_string(), bytes);
            Ok(Some(etag))
        }
        async fn put_if_match(
            &self,
            key: &str,
            etag: &str,
            bytes: Vec<u8>,
        ) -> Result<Option<String>> {
            let mut map = self.objects.lock().unwrap();
            match map.get(key) {
                Some(current) if test_etag(current) == etag => {
                    let new_etag = test_etag(&bytes);
                    map.insert(key.to_string(), bytes);
                    Ok(Some(new_etag))
                }
                _ => Ok(None),
            }
        }
    }

    fn test_etag(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }
}
