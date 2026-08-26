//! `pypiron.toml`: file-based configuration, layered under CLI/env.
//!
//! Four pieces, one per concern:
//!   - top-level `private-prefix` — the reserved private namespace, shared by
//!     `sync` and the `serve` proxy (one knob, one place).
//!   - `[mirror]` — the slice of PyPI you want, names included. Shared by
//!     `sync` (push mirror) and `serve --proxy-upstream` (on-demand pull
//!     mirror): set it once, it governs whichever you run.
//!   - `[serve]` — the server process (non-secret knobs only; credentials and
//!     cloud keys stay in CLI/env — see docs/concepts/authentication.md).
//!   - `[sync]` — the push-mirror job (source/dest + concurrency).
//!
//! Precedence is CLI/env (clap merges those) > file > built-in default.
//! Unknown keys are hard errors — a typo'd mirror rule that silently no-ops is
//! how you mirror the wrong thing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

pub(crate) const DEFAULT_CONFIG_PATH: &str = "pypiron.toml";

/// The annotated starter config printed by `pypiron config init`. Every knob is
/// present and commented out with its default, so the file doubles as the
/// reference. When you add a field to any struct below, add its line here — the
/// `annotated_template_uncomments_and_parses` test fails on a renamed or removed
/// key, but a *newly added* one has to be documented by hand.
pub const TEMPLATE: &str = include_str!("config_template.toml");

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ConfigFile {
    /// Reserved private namespace (PEP 503-normalized). Shared by `sync` and the
    /// `serve` proxy — the dependency-confusion control belongs in one place.
    pub private_prefix: Option<String>,
    #[serde(default)]
    pub mirror: UpstreamConfig,
    #[serde(default)]
    pub serve: ServeConfig,
    #[serde(default)]
    pub sync: SyncConfig,
}

/// `[mirror]`: the slice of PyPI to mirror/proxy. Same fields as the shared
/// mirror CLI flags; consumed by both `sync` and `serve --proxy-upstream`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct UpstreamConfig {
    /// Inline package scope; each entry is a name with optional PEP 440
    /// specifiers (e.g. "requests>=2.20,<3"), same syntax as a packages.txt
    /// line. The slice's name axis: `sync` mirrors exactly these, and the
    /// `serve` proxy serves only these from upstream (fail-closed when set).
    pub include_packages: Option<Vec<String>>,
    /// File of package specs, one per line; same syntax as `include-packages`.
    pub include_packages_from: Option<PathBuf>,
    /// Package specs to subtract from the include set. Bare names deny the
    /// whole project; version specifiers deny only matching files.
    pub exclude_packages: Option<Vec<String>>,
    /// File of package deny specs, one per line.
    pub exclude_packages_from: Option<PathBuf>,
    /// Artifact formats to keep: wheel, sdist, other. Unset means all formats.
    pub include_format: Option<Vec<String>>,
    pub include_python_tag: Option<Vec<String>>,
    pub include_abi_tag: Option<Vec<String>>,
    pub include_platform_tag: Option<Vec<String>>,
    pub exclude_python_tag: Option<Vec<String>>,
    pub exclude_abi_tag: Option<Vec<String>>,
    pub exclude_platform_tag: Option<Vec<String>>,
    pub exclude_newer: Option<String>,
    pub exclude_older: Option<String>,
    pub exclude_python_below: Option<String>,
    pub exclude_dev: Option<bool>,
    pub exclude_windows: Option<bool>,
    pub exclude_prereleases: Option<bool>,
    pub exclude_larger: Option<String>,
    /// Yanked files (PEP 592) are excluded by default; set `true` to mirror them.
    pub include_yanked: Option<bool>,
}

/// `[serve]`: the server process. Non-secret knobs only — admin/uploader/read
/// passwords and the Azure access key stay in CLI/env. Mirrors the `serve` CLI
/// flags one-to-one (including storage selection).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ServeConfig {
    pub bind_addr: Option<String>,
    pub artifact_delivery: Option<String>,
    pub access_log: Option<bool>,
    pub access_log_format: Option<String>,
    pub trusted_proxy: Option<bool>,
    pub login_cooldown_secs: Option<u64>,
    pub proxy_upstream: Option<String>,
    pub allow_insecure_upstream: Option<bool>,
    pub proxy_stream_threshold: Option<String>,
    pub advisory_feed: Option<String>,
    pub malware_block: Option<bool>,
    pub malware_probe_secs: Option<u64>,
    pub metrics_project_labels: Option<bool>,
    pub spool_dir: Option<PathBuf>,
    pub wait_on_upload: Option<bool>,
    pub wait_on_upload_secs: Option<u64>,
    /// Accept uploads whose version isn't valid PEP 440. Off by default (reject).
    pub allow_legacy_versions: Option<bool>,
    pub worker_interval_secs: Option<u64>,
    pub bucket_leave_failures: Option<u32>,
    pub bucket_return_healthy_secs: Option<u64>,
    pub fanout_grace_secs: Option<u64>,
    pub intent_grace_secs: Option<u64>,
    pub audit_on_boot: Option<bool>,
    pub transparency: Option<bool>,
    pub reconcile_interval_secs: Option<u64>,
    pub repl_sweep_interval_secs: Option<u64>,
    pub lease_ttl_secs: Option<u64>,
    pub download_stats: Option<bool>,
    pub counters_resolution: Option<String>,
    pub counters_flush_interval_secs: Option<u64>,
    pub counters_rollup_interval_secs: Option<u64>,
    pub counters_retention_days: Option<i64>,
    pub index_cache_ttl_secs: Option<u64>,
    // Storage selection: disk (default) or object storage via `buckets`.
    pub data_dir: Option<String>,
    pub storage_prefix: Option<String>,
    /// Object-storage bucket list: a TOML array of `s3://`/`gs://`/`az://` URIs.
    /// Empty/unset means disk; one entry is single-bucket; several enable
    /// replication and failover.
    pub buckets: Option<Vec<String>>,
    pub s3_endpoint_url: Option<String>,
    pub s3_force_path_style: Option<bool>,
    pub gcs_service_account_path: Option<String>,
    pub gcs_endpoint_url: Option<String>,
    pub azure_account: Option<String>,
    pub azure_endpoint_url: Option<String>,
    pub azure_use_emulator: Option<bool>,
    /// Per-bucket overrides, keyed by bucket URI (`[serve.bucket."s3://name"]`).
    /// The config file's one nested table: the rare fleet where buckets need
    /// different endpoints or credentials names them here. Matched to a bucket
    /// by identity (`scheme://name`, `@region` excluded), so a key written with
    /// or without a region resolves to the same bucket. Secrets never live in
    /// the file — `env-prefix` names where they do.
    pub bucket: Option<HashMap<String, BucketOverride>>,
}

/// `[serve.bucket."scheme://name"]`: overrides for one bucket. Every field is
/// optional and valid only for the matching scheme (a wrong-scheme field is a
/// startup error). `endpoint-url` applies to any scheme; `force-path-style` is
/// S3-only; `env-prefix` names an env var prefix holding the bucket's secret
/// (S3 or Azure); `service-account-path` is a GCS key file; `account` is the
/// Azure storage account. TOML-only by design — there is no CLI/env form.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct BucketOverride {
    /// S3/GCS/Azure endpoint URL for this bucket, overriding the backend-wide
    /// endpoint flag. `http://` implies allow-http.
    pub endpoint_url: Option<String>,
    /// Force S3 path-style addressing for this bucket (S3 only).
    pub force_path_style: Option<bool>,
    /// Env var prefix for this bucket's scoped credentials (S3/Azure). S3 reads
    /// `<P>AWS_ACCESS_KEY_ID` + `<P>AWS_SECRET_ACCESS_KEY` (+ optional
    /// `<P>AWS_SESSION_TOKEN`); Azure reads `<P>AZURE_ACCESS_KEY`.
    pub env_prefix: Option<String>,
    /// Path to a GCS service-account JSON key for this bucket (GCS only); also
    /// enables presigned URLs for it.
    pub service_account_path: Option<String>,
    /// Azure storage account name for this bucket (Azure only).
    pub account: Option<String>,
}

/// `[sync]`: the push-mirror job. The package scope and mirror rules live in
/// `[mirror]`; `private-prefix` lives at the top level.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SyncConfig {
    pub from: Option<String>,
    pub to: Option<String>,
    pub admin_user: Option<String>,
    pub admin_pass: Option<String>,
    /// Credentials for an authenticated source index (a private devpi,
    /// Artifactory, or Nexus). Both are required together; prefer supplying the
    /// password via `PYPIRON_SYNC_SOURCE_PASS`.
    pub source_user: Option<String>,
    pub source_pass: Option<String>,
    pub concurrency: Option<usize>,
    pub package_concurrency: Option<usize>,
    /// Mirror files whose version isn't valid PEP 440. Off by default (skip them).
    pub allow_legacy_versions: Option<bool>,
    /// Advisory snapshot to ferry to the destination. Unset relays the source
    /// server's snapshot (on by default); a URL/path fetches that feed; `""`
    /// disables the relay. Shares the `PYPIRON_ADVISORY_FEED` concept with serve.
    pub advisory_feed: Option<String>,
}

/// Load configuration. An explicit `--config` path must exist; without one,
/// `./pypiron.toml` is used when present and silently skipped when not.
/// Relative `include-packages-from` and `exclude-packages-from` paths inside the
/// file resolve against the config file's own directory, not the process cwd.
pub fn load(explicit: Option<&Path>) -> Result<ConfigFile> {
    let path = match explicit {
        Some(p) => p.to_path_buf(),
        None => {
            let default = Path::new(DEFAULT_CONFIG_PATH);
            if !default.exists() {
                return Ok(ConfigFile::default());
            }
            default.to_path_buf()
        }
    };
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading config {}", path.display()))?;

    let mut cfg: ConfigFile =
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;

    rebase_relative(&mut cfg.mirror.include_packages_from, &path);
    rebase_relative(&mut cfg.mirror.exclude_packages_from, &path);
    // Announce only after a clean parse — silent auto-discovery of
    // ./pypiron.toml is how an unrelated CLI invocation gets quietly rewired,
    // but a malformed file shouldn't claim it "loaded". The read/parse errors
    // above carry the path via `with_context`, so failures still name the file.
    info!("loaded configuration from {}", path.display());
    Ok(cfg)
}

fn rebase_relative(path: &mut Option<PathBuf>, config_path: &Path) {
    let Some(rel) = path.as_ref().filter(|p| p.is_relative()) else {
        return;
    };
    if let Some(dir) = config_path.parent().filter(|d| !d.as_os_str().is_empty()) {
        *path = Some(dir.join(rel));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sync_and_mirror_sections() {
        let cfg: ConfigFile = toml::from_str(
            r#"
            private-prefix = "acme"

            [sync]
            to = "http://localhost:8080"
            concurrency = 8
            package-concurrency = 16

            [mirror]
            include-packages = ["requests>=2.20,<3", "six"]
            exclude-packages = ["six==1.15.0"]
            include-format = ["wheel"]
            include-python-tag = ["py3"]
            exclude-newer = "2026-01-01T00:00:00Z"
            exclude-prereleases = true
            exclude-larger = "250MB"
            include-yanked = true
            "#,
        )
        .unwrap();
        assert_eq!(cfg.private_prefix.as_deref(), Some("acme"));
        assert_eq!(cfg.mirror.include_packages.unwrap().len(), 2);
        assert_eq!(cfg.mirror.exclude_packages.unwrap().len(), 1);
        assert_eq!(cfg.sync.concurrency, Some(8));
        assert_eq!(cfg.sync.package_concurrency, Some(16));
        assert_eq!(cfg.mirror.include_format.unwrap(), ["wheel"]);
        assert_eq!(
            cfg.mirror.exclude_newer.as_deref(),
            Some("2026-01-01T00:00:00Z")
        );
        assert_eq!(cfg.mirror.exclude_prereleases, Some(true));
        assert_eq!(cfg.mirror.exclude_larger.as_deref(), Some("250MB"));
        assert_eq!(cfg.mirror.include_yanked, Some(true));
    }

    #[test]
    fn parses_serve_section() {
        let cfg: ConfigFile = toml::from_str(
            r#"
            [serve]
            bind-addr = "127.0.0.1:9000"
            proxy-upstream = "https://pypi.org"
            buckets = ["s3://acme-mirror"]
            reconcile-interval-secs = 3600
            repl-sweep-interval-secs = 120
            "#,
        )
        .unwrap();
        assert_eq!(cfg.serve.bind_addr.as_deref(), Some("127.0.0.1:9000"));
        assert_eq!(
            cfg.serve.proxy_upstream.as_deref(),
            Some("https://pypi.org")
        );
        assert_eq!(cfg.serve.buckets.unwrap(), ["s3://acme-mirror"]);
        assert_eq!(cfg.serve.reconcile_interval_secs, Some(3600));
        assert_eq!(cfg.serve.repl_sweep_interval_secs, Some(120));
    }

    #[test]
    fn parses_per_bucket_overrides() {
        let cfg: ConfigFile = toml::from_str(
            r#"
            [serve]
            buckets = ["s3://iron-east@us-east-1", "s3://minio-cache"]

            [serve.bucket."s3://minio-cache"]
            endpoint-url = "http://minio.internal:9000"
            force-path-style = true
            env-prefix = "MINIO_CACHE_"
            "#,
        )
        .unwrap();
        let bucket = cfg.serve.bucket.expect("bucket table present");
        let ov = bucket.get("s3://minio-cache").expect("keyed override");
        assert_eq!(
            ov.endpoint_url.as_deref(),
            Some("http://minio.internal:9000")
        );
        assert_eq!(ov.force_path_style, Some(true));
        assert_eq!(ov.env_prefix.as_deref(), Some("MINIO_CACHE_"));
        assert!(ov.service_account_path.is_none());
        assert!(ov.account.is_none());
    }

    #[test]
    fn unknown_bucket_override_field_is_rejected() {
        let err =
            toml::from_str::<ConfigFile>("[serve.bucket.\"s3://x\"]\nendpont-url = \"http://x\"\n")
                .unwrap_err();
        assert!(err.to_string().contains("endpont-url"));
    }

    #[test]
    fn unknown_mirror_key_is_rejected() {
        let err =
            toml::from_str::<ConfigFile>("[mirror]\ninclude-formatt = [\"wheel\"]\n").unwrap_err();
        assert!(err.to_string().contains("include-formatt"));
    }

    #[test]
    fn shipped_mirror_examples_parse() {
        // The recipe files under examples/mirror/ are user-facing config kept in
        // step with the docs. deny_unknown_fields makes a renamed or typo'd knob
        // fail here at parse time instead of silently rotting an example.
        let lean: ConfigFile =
            toml::from_str(include_str!("../examples/mirror/lean-linux-ci.toml")).unwrap();
        assert_eq!(lean.mirror.include_format.unwrap(), ["wheel"]);
        assert_eq!(
            lean.mirror.exclude_platform_tag.unwrap(),
            ["win*", "macosx_*"]
        );

        let no_pypy: ConfigFile =
            toml::from_str(include_str!("../examples/mirror/no-pypy.toml")).unwrap();
        assert_eq!(no_pypy.mirror.exclude_python_tag.unwrap(), ["pp*"]);

        let stable: ConfigFile =
            toml::from_str(include_str!("../examples/mirror/stable-only.toml")).unwrap();
        assert_eq!(stable.mirror.exclude_prereleases, Some(true));

        let air: ConfigFile =
            toml::from_str(include_str!("../examples/mirror/air-gapped.toml")).unwrap();
        assert_eq!(air.mirror.exclude_newer.as_deref(), Some(""));
        assert_eq!(air.mirror.include_yanked, Some(true));
    }

    #[test]
    fn annotated_template_uncomments_and_parses() {
        // Uncommenting every `# <key> = <value>` line must yield a config that
        // still parses under deny_unknown_fields. A renamed or removed knob
        // leaves a stale template line that this catches at build time.
        let uncommented = TEMPLATE
            .lines()
            .map(|line| match line.strip_prefix("# ") {
                Some(rest)
                    if rest.split_once(" = ").is_some_and(|(k, _)| {
                        !k.is_empty()
                            && k.chars()
                                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                    }) =>
                {
                    rest.to_string()
                }
                _ => line.to_string(),
            })
            .collect::<Vec<_>>()
            .join("\n");

        let cfg: ConfigFile =
            toml::from_str(&uncommented).expect("uncommented template must parse");

        // Spot-check that documented defaults match the real ones and land in
        // the right section.
        assert_eq!(cfg.private_prefix.as_deref(), Some("acme"));
        assert_eq!(cfg.serve.bind_addr.as_deref(), Some("0.0.0.0:8080"));
        assert_eq!(cfg.serve.artifact_delivery.as_deref(), Some("auto"));
        assert_eq!(cfg.serve.counters_retention_days, Some(90));
        assert_eq!(cfg.serve.s3_force_path_style, Some(false));
        // Advisory knobs are Option-in-clap (defaults resolved in code), so the
        // template documents the resolved defaults; assert the comment matches.
        assert_eq!(
            cfg.serve.advisory_feed.as_deref(),
            Some("https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip")
        );
        assert_eq!(cfg.serve.malware_block, Some(true));
        assert_eq!(cfg.serve.malware_probe_secs, Some(120));
        assert_eq!(cfg.mirror.exclude_newer.as_deref(), Some("7"));
        assert_eq!(
            cfg.mirror.include_format,
            Some(vec!["wheel".to_string(), "sdist".to_string()])
        );
        assert_eq!(cfg.sync.to.as_deref(), Some("http://localhost:8080"));
        assert_eq!(cfg.sync.concurrency, Some(4));
    }

    #[test]
    fn empty_config_is_fine() {
        let cfg: ConfigFile = toml::from_str("").unwrap();
        assert!(cfg.mirror.include_packages.is_none());
        assert!(cfg.mirror.include_format.is_none());
        assert!(cfg.serve.bind_addr.is_none());
        assert!(cfg.private_prefix.is_none());
    }
}
