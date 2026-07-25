//! The command-line surface: the clap argument tree and the self-contained
//! subcommand handlers (token minting, healthcheck, origin release, bucket
//! migration) plus the pypiron.toml merge helpers that only rearrange parsed
//! args. Anything that builds an [`crate::app::AppState`] — `run_serve`,
//! `run_rebuild_index`, the `cli_main` dispatcher — stays in `app.rs`; the
//! dispatcher there calls into these handlers.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};

use crate::app::{AccessLogFormat, ArtifactDelivery, VERSION};
use crate::{
    bucket_health, buckets, config, names, observed_storage, origin, replicate, storage, sync,
    transparency, verify,
};
use buckets::{BucketHandle, BucketSet};
use names::checked_pkg_name;
use storage::{Storage, StorageArgs};

// Bare `pypiron` (no args) prints help (arg_required_else_help). Every verb is a
// subcommand — serving is `pypiron serve`. Only genuinely cross-cutting flags
// (`--log-format`) live at the top level; everything serve-specific is under
// `serve`, so the top-level help stays a short front door instead of dumping
// every server flag.

/// PypIron — a fast single-binary PyPI server: index, upload, mirror, on-demand proxy.
#[derive(Parser, Debug)]
#[command(author, version = VERSION, about, long_about = None, arg_required_else_help = true)]
pub struct Cli {
    /// Subcommands: `serve`, `sync`, `verify-index`, `rebuild-index`, `healthcheck`.
    #[command(subcommand)]
    pub(crate) command: Option<Commands>,

    /// Path to a pypiron.toml (defaults to ./pypiron.toml when present). Read by
    /// every subcommand: `serve`/`sync` use the full file, and the maintenance
    /// commands (`verify-index`/`rebuild-index`) read the `[serve]` storage
    /// selection so they target the same backend the server does. CLI/env
    /// values take precedence over the file. `global` so it may sit before or
    /// after the subcommand.
    #[arg(long, env = "PYPIRON_CONFIG", global = true)]
    pub(crate) config: Option<std::path::PathBuf>,

    /// Log output format: `text` (human-readable) or `json` (one object per
    /// line, for log pipelines). Applies to every subcommand; `global` so it
    /// may sit before or after the subcommand.
    #[arg(
        long,
        env = "PYPIRON_LOG_FORMAT",
        value_enum,
        default_value_t = LogFormat::Text,
        global = true
    )]
    pub(crate) log_format: LogFormat,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the PypIron server (the primary, day-to-day command)
    Serve(Box<ServeArgs>),
    /// Mirror packages from PyPI (or another source) into a PypIron instance
    Sync(Box<sync::SyncArgs>),
    /// Server maintenance: recompute every index from truth and diff against
    /// what storage serves (read-only); exits nonzero on any divergence
    ///
    /// Run on a server node against the same storage backend `serve` uses (it
    /// reads the `[serve]` storage config from --config / pypiron.toml).
    /// Whole-corpus scan: cost scales with corpus, not churn. S3 rule of thumb:
    /// ~$0.5 and ~20 min per million files (single node, default concurrency;
    /// read-only). The daily `serve` audit stays seconds/pennies via fingerprints.
    VerifyIndex(Box<verify::VerifyArgs>),
    /// Server maintenance: rebuild every materialized view from truth,
    /// unconditionally. Run after restoring a backup or editing storage
    /// out-of-band.
    ///
    /// Run on a server node against the same storage backend `serve` uses (it
    /// reads the `[serve]` storage config from --config / pypiron.toml).
    /// Whole-corpus scan and rewrite: cost scales with corpus, not churn. S3
    /// rule of thumb: ~$1-1.5 and ~20-30 min per million files (single node,
    /// default concurrency). To only check for drift, use read-only `verify-index`.
    RebuildIndex(Box<RebuildIndexArgs>),
    /// Server maintenance: replay the tamper-evident checkpoint chain against
    /// storage and report any artifact that was rewritten or vanished
    /// out-of-band (read-only); exits nonzero on any violation
    ///
    /// Run on a server node against the same storage backend `serve` uses (it
    /// reads the `[serve]` storage config from --config / pypiron.toml). Catches
    /// the one attack every other check misses: an attacker with your storage
    /// credentials rewriting an artifact and its recorded sha256 together. Files
    /// present in storage but not yet committed to the chain are fine — the
    /// chain lags truth by at most one audit.
    VerifyChain(Box<transparency::VerifyChainArgs>),
    /// Probe a running server's `/health` and exit 0 (healthy) or 1
    /// (unreachable / unhealthy).
    ///
    /// Self-contained — no `curl`/`wget` — so the slim container image can use it
    /// as its `HEALTHCHECK` and orchestrators can reuse it as a liveness probe.
    Healthcheck(HealthcheckArgs),
    /// Mint a short-lived (5-minute) install token from a running server,
    /// stamped with auto-detected repo/commit/user for attribution.
    ///
    /// Prints the token to stdout (everything else to stderr), so it pipes:
    /// `export UV_INDEX_PYPIRON_PASSWORD=$(pypiron create-token --url …)` with
    /// username `__token__`. Default role is reader.
    CreateToken(CreateTokenArgs),
    /// Work with pypiron.toml — currently just `config init`.
    Config(ConfigArgs),
    /// Deliberate package-origin maintenance.
    Origin(Box<OriginArgs>),
    /// Deliberate multi-bucket topology maintenance.
    Buckets(Box<BucketsArgs>),
}

#[derive(ClapArgs, Debug)]
pub struct BucketsArgs {
    #[command(subcommand)]
    pub(crate) command: BucketsCommand,
}

#[derive(Subcommand, Debug)]
pub enum BucketsCommand {
    /// Bump the topology generation and conditionally re-stamp every reachable
    /// configured bucket after adding, removing, or reordering buckets.
    Migrate(BucketsMigrateArgs),
}

#[derive(ClapArgs, Debug)]
pub struct BucketsMigrateArgs {
    #[command(flatten)]
    pub(crate) storage: StorageArgs,

    /// Drop a bucket even when it holds artifacts no surviving bucket has —
    /// **permanent data loss**. Without it, migrate refuses to remove a bucket
    /// that is the fleet's only copy of some content (back the corpus up onto a
    /// surviving bucket first, e.g. by adding the survivor and letting
    /// replication converge, then remove the old one).
    #[arg(long, env = "PYPIRON_MIGRATE_FORCE")]
    pub(crate) force: bool,
}

#[derive(ClapArgs, Debug)]
pub struct OriginArgs {
    #[command(subcommand)]
    pub(crate) command: OriginCommand,
}

#[derive(Subcommand, Debug)]
pub enum OriginCommand {
    /// Release an empty package claim for deliberate private/mirror repurposing.
    /// Every configured bucket must be reachable and empty.
    Release(OriginReleaseArgs),
}

#[derive(ClapArgs, Debug)]
pub struct OriginReleaseArgs {
    /// Package name to release (PEP 503 normalization is applied).
    package: String,
    #[command(flatten)]
    pub(crate) storage: StorageArgs,
}

#[derive(ClapArgs, Debug)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub(crate) command: ConfigCommand,
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Print an annotated pypiron.toml to stdout. Every knob is present,
    /// commented out with its default, so you `> pypiron.toml` and uncomment
    /// the lines you want.
    Init,
}

#[derive(ClapArgs, Debug)]
pub struct CreateTokenArgs {
    /// Server base URL, e.g. `http://localhost:8080`.
    #[arg(long, env = "PYPIRON_URL")]
    url: String,

    /// Role to request: `reader` (default), `uploader`, or `admin`. Cannot
    /// exceed what `--auth` grants.
    #[arg(long, default_value = "reader")]
    role: String,

    /// Credential to authenticate the mint request, as `user:pass`. Omit on an
    /// open (public-read) server when requesting a reader token.
    #[arg(long, env = "PYPIRON_AUTH")]
    auth: Option<String>,

    /// Override the auto-detected repository URL (`git remote get-url origin`).
    #[arg(long)]
    repo: Option<String>,

    /// Override the auto-detected commit (`git rev-parse --short HEAD`).
    #[arg(long)]
    commit: Option<String>,

    /// Override the auto-detected user (`$USER`, else `id -un`).
    #[arg(long)]
    user: Option<String>,
}

#[derive(ClapArgs, Debug)]
pub struct HealthcheckArgs {
    /// URL to probe. Defaults to `http://127.0.0.1:<port>/health`, where the port
    /// is taken from `PYPIRON_BIND_ADDR` (so a port override is honored without
    /// repeating it here), falling back to 8080.
    #[arg(long, env = "PYPIRON_HEALTHCHECK_URL")]
    url: Option<String>,
}

#[derive(ClapArgs, Debug)]
pub struct RebuildIndexArgs {
    #[command(flatten)]
    pub(crate) storage: StorageArgs,
}

/// Loopback `/health` URL for the port `serve` would bind. `bind` is the raw
/// `PYPIRON_BIND_ADDR` value (e.g. `0.0.0.0:8080`); an unset or unparseable
/// value falls back to the default 8080. Always loopback — the probe runs inside
/// the container, regardless of which interface the server binds to.
fn loopback_health_url(bind: Option<&str>) -> String {
    let port = bind
        .and_then(|a| a.parse::<std::net::SocketAddr>().ok())
        .map_or(8080, |a| a.port());
    format!("http://127.0.0.1:{port}/health")
}

/// One-shot probe for container/orchestrator health checks: GET `/health` and
/// map the result onto the process exit code (2xx → 0, anything else → nonzero;
/// the returned `Err` becomes exit 1). Self-contained over the binary's existing
/// HTTP client, so the slim runtime image needs no `curl`/`wget`.
pub async fn run_healthcheck(args: HealthcheckArgs) -> Result<()> {
    let url = args
        .url
        .unwrap_or_else(|| loopback_health_url(std::env::var("PYPIRON_BIND_ADDR").ok().as_deref()));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("building healthcheck client")?;
    let status = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("health probe could not reach {url}"))?
        .status();
    if status.is_success() {
        Ok(())
    } else {
        anyhow::bail!("health probe {url} returned HTTP {}", status.as_u16());
    }
}

/// Run a command and return its trimmed stdout, or None on any failure (missing
/// binary, nonzero exit, empty output) — auto-detection is best-effort.
fn run_trimmed(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// Best-effort current user: `$USER`/`$LOGNAME`, falling back to `id -un`.
fn detect_user() -> Option<String> {
    std::env::var("USER")
        .ok()
        .or_else(|| std::env::var("LOGNAME").ok())
        .filter(|s| !s.is_empty())
        .or_else(|| run_trimmed("id", &["-un"]))
}

/// Mint a token from a running server and print it to stdout. Repo/commit/user
/// are auto-detected from the working tree unless overridden; the actual
/// gather-vs-route decision lives server-side, so we just submit what we find.
pub async fn run_create_token(args: CreateTokenArgs) -> Result<()> {
    let repo = args
        .repo
        .or_else(|| run_trimmed("git", &["remote", "get-url", "origin"]));
    let commit = args
        .commit
        .or_else(|| run_trimmed("git", &["rev-parse", "--short", "HEAD"]));
    let user = args.user.or_else(detect_user);

    let url = format!("{}/tokens", args.url.trim_end_matches('/'));
    let body = serde_json::json!({
        "role": args.role,
        "repo": repo,
        "commit": commit,
        "user": user,
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("building token client")?;
    let mut request = client.post(&url).json(&body);
    if let Some(auth) = args.auth.as_deref() {
        let (u, p) = auth.split_once(':').unwrap_or((auth, ""));
        request = request.basic_auth(u, Some(p));
    }
    let resp = request
        .send()
        .await
        .with_context(|| format!("requesting a token from {url}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "token request failed (HTTP {}): {}",
            status.as_u16(),
            text.trim()
        );
    }
    let parsed: serde_json::Value =
        serde_json::from_str(&text).context("parsing token response")?;
    let token = parsed
        .get("token")
        .and_then(|t| t.as_str())
        .ok_or_else(|| anyhow!("token response missing `token` field"))?;
    if let Some(exp) = parsed.get("expires_at").and_then(|t| t.as_str()) {
        eprintln!(
            "minted {} token (expires {exp}); install with username __token__",
            args.role
        );
    }
    println!("{token}");
    Ok(())
}

/// Point a maintenance command (verify-index, rebuild-index) at the same
/// storage the server uses, by folding the `[serve]` storage config from
/// pypiron.toml under its CLI/env args. Without this, `--config` would be a
/// silent no-op on these commands and a file-only operator would audit the
/// default `./data` disk store instead of their real (e.g. S3) backend — a
/// false "converged" for the exact backup-recovery case they exist for. Echoes
/// the resolved backend so an operator can see what they're pointed at.
pub fn apply_maintenance_config(
    storage: &mut StorageArgs,
    config_path: Option<&std::path::Path>,
    matches: &clap::ArgMatches,
    subcommand: &str,
) -> Result<()> {
    let file = config::load(config_path)?;
    let m = matches
        .subcommand_matches(subcommand)
        .expect("dispatched subcommand always matches");
    merge_storage_file(storage, &file.serve, m)?;
    eprintln!("{subcommand}: storage backend {}", storage.describe());
    Ok(())
}

pub fn apply_nested_maintenance_config(
    storage: &mut StorageArgs,
    config_path: Option<&std::path::Path>,
    matches: &clap::ArgMatches,
    command: &str,
    subcommand: &str,
) -> Result<()> {
    let file = config::load(config_path)?;
    let command_matches = matches
        .subcommand_matches(command)
        .and_then(|parent| parent.subcommand_matches(subcommand))
        .expect("dispatched nested subcommand always matches");
    merge_storage_file(storage, &file.serve, command_matches)?;
    eprintln!(
        "{command} {subcommand}: storage backend {}",
        storage.describe()
    );
    Ok(())
}

pub async fn run_origin_release(args: OriginReleaseArgs) -> Result<()> {
    let pkg = checked_pkg_name(&args.package)
        .ok_or_else(|| anyhow!("invalid package name '{}'", args.package))?;
    let storages = args.storage.build_all_for_write().await?;
    // Preflight every bucket before the first CAS. Without this, discovering an
    // artifact or outage on bucket B after releasing A would create exactly the
    // partial-origin window this maintenance command exists to avoid.
    let mut targets = Vec::new();
    for storage in &storages {
        if let Some(observed) = origin::releasable_for_repurpose(storage.as_ref(), &pkg).await? {
            targets.push((storage.clone(), observed));
        }
    }
    if targets.is_empty() {
        anyhow::bail!("package '{pkg}' has no live origin claim to release");
    }

    let mut completed = Vec::new();
    for (storage, observed) in targets {
        let outcome =
            origin::release_observed_for_repurpose(storage.as_ref(), &pkg, &observed).await;
        match outcome {
            Ok(Some(unclaimed)) => completed.push((storage, observed.state, unclaimed)),
            Ok(None) => {
                rollback_origin_releases(&pkg, &completed).await?;
                anyhow::bail!("package '{pkg}' changed during origin release; no claims released");
            }
            Err(error) => {
                rollback_origin_releases(&pkg, &completed).await?;
                return Err(error).context(format!(
                    "release origin for '{pkg}'; earlier bucket releases were rolled back"
                ));
            }
        }
    }
    eprintln!(
        "released origin claim for '{pkg}' on {} bucket(s)",
        completed.len()
    );
    Ok(())
}

async fn rollback_origin_releases(
    pkg: &str,
    completed: &[(
        Arc<dyn Storage>,
        origin::OriginState,
        origin::OriginObservation,
    )],
) -> Result<()> {
    let mut failures = Vec::new();
    for (storage, original, unclaimed) in completed.iter().rev() {
        if let Err(error) =
            origin::restore_released_for_repurpose(storage.as_ref(), pkg, *original, unclaimed)
                .await
        {
            failures.push(format!("{error:#}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "origin release failed and rollback was incomplete: {}",
            failures.join("; ")
        )
    }
}

/// How many sole-copy artifacts the removal gate names before it stops scanning.
/// Enough to make the refusal concrete without walking a fully-diverged tree.
const MIGRATE_UNIQUE_SAMPLE: usize = 5;

pub async fn run_buckets_migrate(args: BucketsMigrateArgs) -> Result<()> {
    let storages = args.storage.build_all_for_write().await?;
    let names = args.storage.bucket_names();
    let handles = storages
        .into_iter()
        .zip(names)
        .map(|(storage, name)| BucketHandle { storage, name })
        .collect();
    let buckets = BucketSet::new(handles);
    let is_availability = |error: &anyhow::Error| {
        bucket_health::classify(observed_storage::signal_for_error(error))
            == bucket_health::SignalClass::AvailabilityFailure
    };
    // Buckets this migration adds to an existing fleet — a freshly-added bucket
    // holds none of the corpus yet, so it must serve no region reads until a
    // clean reconcile proves it caught up. Identified by diffing the previous
    // stamped member list, or (single-bucket → multi-bucket, where no prior stamp
    // exists to diff) by which buckets are still empty of corpus. Each is fenced
    // with a backfill sentinel seeded *before* the re-stamp commits (see below).
    let mut added_indices: Vec<usize> = Vec::new();
    // Refuse to change the topology while any reachable bucket still holds
    // undrained `_repl/` repair notes: a stranded note may be the sole copy of a
    // record not yet replicated, and shrinking/reordering could remove the
    // bucket it points at. Let the sweep drain first. Surviving buckets that are
    // unreachable are skipped — migrate itself tolerates them, and we cannot
    // inspect them.
    if buckets.is_multi() {
        for handle in buckets.handles() {
            match replicate::has_undrained_repl_notes(handle.storage.as_ref()).await {
                Ok(false) => {}
                Ok(true) => bail!(
                    "bucket '{}' has undrained _repl/ repair notes; a stranded note may be a \
                     record's only copy. Let the running server's sweep drain them (or wait for \
                     the periodic sweep), then retry `pypiron buckets migrate`.",
                    handle.name
                ),
                Err(error) if is_availability(&error) => {
                    eprintln!(
                        "migrate: bucket '{}' unreachable while checking for repair notes; skipping",
                        handle.name
                    );
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("check bucket '{}' for undrained repair notes", handle.name)
                    })
                }
            }
        }

        // A bucket being *removed* from the list is the dangerous case the
        // surviving-bucket loop above cannot see: it holds `_repl/` notes as a
        // fan-out source, and a stranded note there can be a record's sole copy.
        // Read the previous topology from the reachable new-list buckets, then
        // check every member the new list drops. Unlike a surviving bucket, a
        // removed bucket we cannot reach is a refusal: we must not drop a bucket
        // we could not prove note-free.
        let previous = buckets
            .stamped_member_names_with(|_, error| is_availability(error))
            .await
            .context("read the previous topology to find buckets being removed")?;
        match previous {
            // Buckets in the new list but absent from the previous stamped
            // topology are the ones this migration adds.
            Some(previous) => {
                for (index, handle) in buckets.handles().iter().enumerate() {
                    if !previous.iter().any(|name| name == &handle.name) {
                        added_indices.push(index);
                    }
                }
                let new_names: std::collections::HashSet<&str> = buckets
                    .handles()
                    .iter()
                    .map(|handle| handle.name.as_str())
                    .collect();
                let survivors: Vec<Arc<dyn Storage>> = buckets
                    .handles()
                    .iter()
                    .map(|handle| handle.storage.clone())
                    .collect();
                for removed in previous
                    .iter()
                    .filter(|name| !new_names.contains(name.as_str()))
                {
                    let storage = args
                        .storage
                        .build_one_by_identity(removed)
                        .await
                        .with_context(|| format!("connect to removed bucket '{removed}'"))?;
                    match replicate::has_undrained_repl_notes(storage.as_ref()).await {
                        Ok(false) => {}
                        Ok(true) => bail!(
                            "bucket '{removed}' is being removed but still holds undrained _repl/ \
                         repair notes; a stranded note there may be a record's only copy. Let the \
                         running server's sweep drain them (or wait for the periodic sweep), then \
                         retry `pypiron buckets migrate`."
                        ),
                        Err(error) if is_availability(&error) => bail!(
                        "bucket '{removed}' is being removed but is unreachable, so its _repl/ \
                         repair notes cannot be verified drained; refusing to drop a bucket that \
                         may hold a record's only copy. Bring it back, let the sweep drain, then \
                         retry `pypiron buckets migrate`. ({error:#})"
                    ),
                        Err(error) => {
                            return Err(error).with_context(|| {
                                format!(
                                    "check removed bucket '{removed}' for undrained repair notes"
                                )
                            })
                        }
                    }

                    // An empty `_repl/` tree proves nothing about content the fleet
                    // never fanned out — a snapshot seeded out-of-band, or a bucket
                    // whose backfill has not converged. Diff its `packages/` against
                    // every surviving bucket and refuse the drop if it is the sole
                    // copy of any artifact. `--force` accepts the loss.
                    if !args.force {
                        let samples = replicate::artifacts_unique_to_removed(
                            storage.as_ref(),
                            &survivors,
                            MIGRATE_UNIQUE_SAMPLE,
                        )
                        .await
                        .with_context(|| {
                            format!(
                            "diff removed bucket '{removed}' against surviving buckets; a survivor \
                             that could not be inspected is treated as a refusal, never a drop"
                        )
                        })?;
                        if !samples.is_empty() {
                            let examples = samples.join(", ");
                            bail!(
                                "bucket '{removed}' is being removed but is the only copy of \
                             {}{} artifact(s), e.g. {examples}. Dropping it loses that content. \
                             Back the corpus up onto a surviving bucket first (add the survivor, \
                             let replication converge, then remove this one), or pass --force to \
                             accept the data loss.",
                                samples.len(),
                                if samples.len() >= MIGRATE_UNIQUE_SAMPLE {
                                    "+"
                                } else {
                                    ""
                                }
                            );
                        }
                    }
                }
            }
            // No reachable bucket carries a topology stamp: the source is
            // single-bucket mode (which never stamps) or a first-ever multi-bucket
            // migration. There is no previous member list to diff, so identify the
            // freshly-added buckets by content — a bucket already holding
            // `packages/` is the established source and keeps serving region
            // reads, while every bucket still empty of corpus is fenced until a
            // clean reconcile proves it caught up. This is the primary
            // single-bucket → multi-bucket expansion path.
            None => {
                for (index, handle) in buckets.handles().iter().enumerate() {
                    match replicate::bucket_is_corpus_empty(handle.storage.as_ref()).await {
                        Ok(true) => added_indices.push(index),
                        Ok(false) => {}
                        Err(error) if is_availability(&error) => {
                            // Cannot prove it already holds corpus, so fence it
                            // fail-closed; the sentinel lands on its reachable
                            // peers, so an unreachable dest is still gated.
                            eprintln!(
                                "migrate: bucket '{}' unreachable while checking for existing \
                                 corpus; fencing it until a clean reconcile",
                                handle.name
                            );
                            added_indices.push(index);
                        }
                        Err(error) => {
                            return Err(error).with_context(|| {
                                format!("check bucket '{}' for existing corpus", handle.name)
                            })
                        }
                    }
                }
            }
        }

        // Seed each freshly-added bucket's backfill sentinel *before* committing
        // the topology stamp (write-ahead gate). The stamp is what a retry reads
        // back as the "previous" topology and what makes the new buckets live; if
        // seeding ran only after it, a crash — or every peer seed failing once —
        // between the two would leave an empty bucket serving region reads
        // forever, because the retry would see previous == the new list and seed
        // nothing. Seeding first, and refusing to stamp when a new bucket cannot
        // be gated on any reachable peer, keeps it fenced until reconcile drains
        // it. Sentinels orphaned by a later bail are empty and harmless — the next
        // clean reconcile drains them.
        for &dest in &added_indices {
            let mut seeded_any = false;
            for (peer, handle) in buckets.handles().iter().enumerate() {
                if peer == dest {
                    continue;
                }
                match replicate::seed_backfill_sentinel(handle.storage.as_ref(), dest).await {
                    Ok(()) => seeded_any = true,
                    Err(error) if is_availability(&error) => {
                        eprintln!(
                            "migrate: peer '{}' unreachable while gating new bucket '{}'; \
                             skipping this peer",
                            handle.name,
                            buckets.handles()[dest].name
                        );
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "seed the backfill sentinel for new bucket '{}' on peer '{}'; \
                                 refusing to commit a topology that would expose an ungated, \
                                 empty bucket to region reads",
                                buckets.handles()[dest].name,
                                handle.name
                            )
                        });
                    }
                }
            }
            if !seeded_any {
                bail!(
                    "could not seed the backfill sentinel for new bucket '{}' on any reachable \
                     peer; refusing to commit a topology that would let an unconverged bucket \
                     serve region reads. Bring a peer back, then retry `pypiron buckets migrate`.",
                    buckets.handles()[dest].name
                );
            }
        }
    }
    let report = buckets
        .migrate_topology_with(|_, error| is_availability(error))
        .await?;
    let generation = report
        .generation
        .ok_or_else(|| anyhow!("no reachable bucket was available to migrate"))?;
    eprintln!(
        "topology generation {generation}: re-stamped {} bucket(s), {} unreachable",
        report.stamped_indices.len(),
        report.unreachable_indices.len()
    );

    // The backfill sentinels were already seeded above, before the stamp
    // committed, so a freshly-added bucket is fenced the moment its topology is
    // live — no crash window between the stamp and the gate.
    if !added_indices.is_empty() {
        eprintln!(
            "seeded backfill sentinels for {} newly-added bucket(s); they serve no region reads \
             until a reconcile pass confirms the corpus converged",
            added_indices.len()
        );
    }
    Ok(())
}

/// Log output format.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable lines (default).
    Text,
    /// One JSON object per line, for log pipelines.
    Json,
}

/// `PypIron` - A fast, reliable, and scalable `PyPI` server
#[derive(ClapArgs, Debug, Clone)]
pub struct ServeArgs {
    #[command(flatten)]
    pub(crate) storage: StorageArgs,

    /// Uploader credential username — may publish (ordinary uploads). With no
    /// credential of any kind configured, the server is read-only.
    #[arg(long, env = "PYPIRON_UPLOADER_USER")]
    pub(crate) uploader_user: Option<String>,

    /// Uploader credential password (see --uploader-user).
    #[arg(long, env = "PYPIRON_UPLOADER_PASS")]
    pub(crate) uploader_pass: Option<String>,

    /// Admin credential username — may do everything an uploader can, plus the
    /// privileged operations: mirror uploads (backdating + `mirror` origin),
    /// deletion, and yank. Configuring a password is what enables those
    /// operations; the username defaults to `admin` when only the password is
    /// set.
    #[arg(long, env = "PYPIRON_ADMIN_USER")]
    pub(crate) admin_user: Option<String>,

    /// Admin credential password (see --admin-user).
    #[arg(long, env = "PYPIRON_ADMIN_PASS")]
    pub(crate) admin_pass: Option<String>,

    /// Reserve this namespace for private uploads: new private packages must
    /// match `<prefix>` or `<prefix>-*` (PEP 503-normalized)
    #[arg(long, env = "PYPIRON_PRIVATE_PREFIX")]
    pub(crate) private_prefix: Option<String>,

    /// How artifact bytes reach clients. `stream`: proxy through this node
    /// (URL-keyed HTTP caches like pip's stay effective). `redirect`: 302 to
    /// presigned S3 URLs so this node never touches wheel bytes. `auto`
    /// (default): per-client — redirect clients whose caches are immune to
    /// presigned-URL churn (uv), stream everyone else. Disk backend always
    /// streams. See dev/DESIGN.md for the tradeoffs.
    #[arg(
        long,
        env = "PYPIRON_ARTIFACT_DELIVERY",
        value_enum,
        default_value_t = ArtifactDelivery::Auto
    )]
    pub(crate) artifact_delivery: ArtifactDelivery,

    /// Also log reads (index listings, downloads) — the full access log. By
    /// default only mutations (uploads, deletes, yanks, status changes) are
    /// logged, since reads can become the workload at high rps. Either way
    /// `/health` and `/metrics` log only at debug. Each line carries method,
    /// path, status, latency, size, client IP, project tag, and User-Agent on
    /// the `pypiron::access` target (tunable via `RUST_LOG`).
    #[arg(long, env = "PYPIRON_ACCESS_LOG")]
    pub(crate) access_log: bool,

    /// Access-log rendering when `--access-log` is on: `structured` (default;
    /// key=value in text mode, JSON under `--log-format json`) or `clf` (Combined
    /// Log Format on stdout, for GoAccess/lnav/awstats).
    #[arg(
        long,
        env = "PYPIRON_ACCESS_LOG_FORMAT",
        value_enum,
        default_value_t = AccessLogFormat::Structured
    )]
    pub(crate) access_log_format: AccessLogFormat,

    /// Honor `X-Forwarded-For`/`X-Real-IP` for the logged client IP. Off by
    /// default: those headers are client-settable, so trusting them ungated lets
    /// a direct caller forge its audit-logged address. Enable only when pypiron
    /// sits behind a reverse proxy that sets them.
    #[arg(long, env = "PYPIRON_TRUSTED_PROXY")]
    pub(crate) trusted_proxy: bool,

    /// Failed-login cooldown in seconds. An address that fails login five
    /// times, each within this window of the last, is refused further
    /// credential-bearing requests (429 with Retry-After) until the window
    /// passes. Successful logins are never counted and anonymous requests are
    /// never throttled. Enforced per instance. 0 disables (rely on an edge
    /// rate limit instead).
    #[arg(
        long,
        env = "PYPIRON_LOGIN_COOLDOWN_SECS",
        default_value_t = crate::auth::DEFAULT_LOGIN_COOLDOWN_SECS
    )]
    pub(crate) login_cooldown_secs: u64,

    /// Worker interval in seconds. The nudge path makes same-process writes
    /// visible at rebuild speed regardless; this is the marker-poll cadence
    /// for peer nodes' writes. 1s costs ~$0.45/month in S3 LISTs.
    #[arg(long, env = "PYPIRON_WORKER_INTERVAL_SECS", default_value = "1")]
    pub(crate) worker_interval_secs: u64,

    /// Consecutive availability failures before leaving the selected bucket.
    /// Only timeouts, connection failures, and 5xx count; auth/CAS/KMS/quota/
    /// config errors alarm without changing selection.
    #[arg(long, env = "PYPIRON_BUCKET_LEAVE_FAILURES", default_value = "3")]
    pub(crate) bucket_leave_failures: u32,

    /// Continuous healthy seconds required before returning to a recovered,
    /// more-preferred bucket. The long return window prevents flap oscillation.
    #[arg(
        long,
        env = "PYPIRON_BUCKET_RETURN_HEALTHY_SECS",
        default_value = "300"
    )]
    pub(crate) bucket_return_healthy_secs: u64,

    /// This node's region label. Cloud nodes auto-detect it from the platform
    /// (environment then instance metadata); set this to override detection,
    /// e.g. for on-prem or MinIO fleets. Region only steers reads to a near
    /// bucket — it never changes where writes go.
    #[arg(long, env = "PYPIRON_NODE_REGION")]
    pub(crate) node_region: Option<String>,

    /// Grace period a synchronous multi-bucket fan-out gives each secondary
    /// bucket, measured from the moment the selected bucket's write completed.
    /// A secondary that cannot converge within it gets a durable `_repl/` repair
    /// note (drained by the sweep) instead of blocking the ack further, so one
    /// slow or hung bucket adds at most this much upload latency.
    #[arg(long, env = "PYPIRON_FANOUT_GRACE_SECS", default_value = "30")]
    pub(crate) fanout_grace_secs: u64,

    /// Seconds an in-flight write may hold off its package's rebuild before
    /// the worker assumes the writer crashed and rebuilds anyway. Must exceed
    /// the slowest expected upload.
    #[arg(long, env = "PYPIRON_INTENT_GRACE_SECS", default_value = "900")]
    pub(crate) intent_grace_secs: u64,

    /// Run an audit sweep as soon as this node becomes leader (heals a
    /// restored backup or a crashed predecessor without waiting an interval).
    /// On by default; disable with `--audit-on-boot false` (or
    /// `PYPIRON_AUDIT_ON_BOOT=false`).
    #[arg(long, env = "PYPIRON_AUDIT_ON_BOOT", default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) audit_on_boot: bool,

    /// Write tamper-evident checkpoints: each audit appends a hash-chained link
    /// under `_transparency/chain/` committing the sha256 of every changed
    /// package's files, so `pypiron verify-chain` can later catch an out-of-band
    /// artifact rewrite. On by default; disable with `--transparency false` (or
    /// `PYPIRON_TRANSPARENCY=false`). Off only stops new links — verify-chain
    /// still checks whatever chain exists.
    #[arg(long, env = "PYPIRON_TRANSPARENCY", default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) transparency: bool,

    /// Seconds between audit sweeps. Day-to-day freshness rides the event
    /// markers; the audit only catches out-of-band storage changes, so daily
    /// is plenty. Fingerprint shards make an unchanged corpus cost a flat
    /// listing and nothing else.
    #[arg(long, env = "PYPIRON_RECONCILE_INTERVAL_SECS", default_value = "86400")]
    pub(crate) reconcile_interval_secs: u64,

    /// Seconds between periodic `_repl/` repair-note sweeps (multi-bucket only).
    /// Decoupled from `--worker-interval-secs`: notes exist only after a fan-out
    /// failure, and the sweep also fires immediately when an unhealthy bucket
    /// heals, so the 1 s tick never has to drive `_repl/` LISTs.
    #[arg(long, env = "PYPIRON_REPL_SWEEP_INTERVAL_SECS", default_value = "300")]
    pub(crate) repl_sweep_interval_secs: u64,

    /// Leader lease TTL in seconds (multi-node S3 only; sloppy by design)
    #[arg(long, env = "PYPIRON_LEASE_TTL_SECS", default_value = "30")]
    pub(crate) lease_ttl_secs: u64,

    /// Count per-package/version downloads per day into the S3-backed counter
    /// store (`_counters/`). A best-effort derived analytic — lossy by design,
    /// never truth. Adds a periodic small PUT per node (see
    /// docs/reference/configuration.md). On by default; disable with
    /// `--download-stats false` (or `PYPIRON_DOWNLOAD_STATS=false`).
    #[arg(long, env = "PYPIRON_DOWNLOAD_STATS", default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) download_stats: bool,

    /// Counter resolution (intra-day bucket width): a whole number of minutes
    /// dividing a day, e.g. `1d`, `1h`, `30m`, `2h`. Coarser is cheaper; changing
    /// it is non-destructive (old days keep their granularity).
    #[arg(long, env = "PYPIRON_COUNTERS_RESOLUTION", default_value = "1d")]
    pub(crate) counters_resolution: String,

    /// Seconds between counter flushes (every node). Lower = fresher and less
    /// loss on crash, at more S3 PUTs. The dominant cost knob.
    #[arg(
        long,
        env = "PYPIRON_COUNTERS_FLUSH_INTERVAL_SECS",
        default_value = "300"
    )]
    pub(crate) counters_flush_interval_secs: u64,

    /// Seconds between leader compaction passes (freeze finished days, prune).
    #[arg(
        long,
        env = "PYPIRON_COUNTERS_ROLLUP_INTERVAL_SECS",
        default_value = "3600"
    )]
    pub(crate) counters_rollup_interval_secs: u64,

    /// Staleness bound on the in-memory index/page caches, in seconds. Only
    /// matters multi-node: a node's own writes invalidate its caches exactly, so
    /// the TTL bounds how long ANOTHER node's index write can go unseen here.
    /// Single-node deployments can raise it freely to drop the once-per-TTL
    /// revalidation read.
    #[arg(long, env = "PYPIRON_INDEX_CACHE_TTL_SECS", default_value = "1")]
    pub(crate) index_cache_ttl_secs: u64,

    /// Days of per-day counter history to keep before deletion.
    #[arg(long, env = "PYPIRON_COUNTERS_RETENTION_DAYS", default_value = "90")]
    pub(crate) counters_retention_days: i64,

    /// Wait for the uploaded file to appear in the index before returning
    /// 200 (publish-then-install CI pipelines)
    #[arg(long, env = "PYPIRON_WAIT_ON_UPLOAD")]
    pub(crate) wait_on_upload: bool,

    /// Bound on the wait-on-upload poll, in seconds
    #[arg(long, env = "PYPIRON_WAIT_ON_UPLOAD_SECS", default_value = "10")]
    pub(crate) wait_on_upload_secs: u64,

    /// Address to bind the server to
    #[arg(long, env = "PYPIRON_BIND_ADDR", default_value = "0.0.0.0:8080")]
    pub(crate) bind_addr: String,

    /// Directory for upload spool files (defaults to the system temp dir).
    /// Point this at real disk on distros where /tmp is a RAM-backed tmpfs —
    /// otherwise large uploads spool into memory and defeat streaming.
    #[arg(long, env = "PYPIRON_SPOOL_DIR")]
    pub(crate) spool_dir: Option<std::path::PathBuf>,

    /// Read credential username — when set, the simple indexes and artifact
    /// downloads require basic auth (this credential, the uploader, or the
    /// admin all work). When unset, reads are public. Usernames support
    /// `+tag` subaddressing (e.g. `reader+billing-api`) for per-project
    /// traffic attribution in /metrics and the request logs.
    #[arg(long, env = "PYPIRON_READ_USER")]
    pub(crate) read_user: Option<String>,

    /// Read credential password (see --read-user).
    #[arg(long, env = "PYPIRON_READ_PASS")]
    pub(crate) read_pass: Option<String>,

    /// Secret key for signing short-lived install tokens. When set, clients may
    /// `POST /tokens` to mint a 5-minute bearer token (presented as basic-auth
    /// username `__token__`); when unset, token minting and verification are
    /// disabled. Tokens are stateless — the key must be identical across nodes,
    /// like the other credentials. Generate with e.g. `openssl rand -hex 32`.
    #[arg(long, env = "PYPIRON_TOKEN_SIGNING_KEY")]
    pub(crate) token_signing_key: Option<String>,

    /// Serve unknown (non-private) packages on demand from this upstream
    /// simple index (e.g. https://pypi.org): package pages are answered from
    /// upstream metadata and artifacts are downloaded, verified, and cached
    /// in storage as `mirror`-origin packages on first request. Names claimed
    /// `private` (or inside --private-prefix) never fall through. When a package
    /// scope is set (`--include-package`/`[mirror].include-packages`), only those names
    /// fall through and the rest are 404'd (fail-closed). Off by default.
    #[arg(long, env = "PYPIRON_PROXY_UPSTREAM")]
    pub(crate) proxy_upstream: Option<String>,

    /// Allow a plaintext `http://` proxy upstream. Off by default: over http a
    /// network MITM controls both the artifact bytes and the sha256 we verify
    /// them against, so the hash check stops being a security control.
    #[arg(long, env = "PYPIRON_ALLOW_INSECURE_UPSTREAM")]
    pub(crate) allow_insecure_upstream: bool,

    /// PEM bundle of extra CA certificates to trust for **upstream** TLS — the
    /// private root a corporate forwarding TLS proxy (a MITM appliance) presents.
    /// Augments the built-in roots (a direct fetch of public PyPI keeps working);
    /// it does not replace them. Applied to the proxy upstream fetch and the
    /// advisory feed/probe. Loaded fail-closed at startup: a missing or
    /// unparseable bundle refuses to start.
    #[arg(
        long = "upstream-ca-cert",
        env = "PYPIRON_UPSTREAM_CA_CERT",
        value_name = "PEM"
    )]
    pub(crate) upstream_ca_cert: Option<std::path::PathBuf>,

    /// Emit per-client `project` attribution labels on `/metrics`
    /// (`pypiron_project_requests_total`). Off by default: `/metrics` is
    /// unauthenticated, and the label is derived from the basic-auth username
    /// subaddress, so exposing it lets any scraper enumerate internal project
    /// names.
    #[arg(long, env = "PYPIRON_METRICS_PROJECT_LABELS")]
    pub(crate) metrics_project_labels: bool,

    /// Permit the proxy to fetch listing-derived URLs (artifact, `.metadata`,
    /// `.provenance`, redirect targets) whose host matches this exact value, even
    /// if it resolves to a private/internal address. Repeatable; comma-separated
    /// in the env var. Empty by default: only the configured upstream host is
    /// exempt. For a fully-internal deployment whose files live on a different
    /// private host than the index.
    #[arg(long, env = "PYPIRON_PROXY_ALLOW_HOST", value_delimiter = ',')]
    pub(crate) proxy_allow_host: Vec<String>,

    /// Permit the proxy to fetch listing-derived URLs whose IP falls in this
    /// CIDR (e.g. `10.0.0.0/8`), even though it is otherwise private. Repeatable;
    /// comma-separated in the env var. Empty by default. Same escape hatch as
    /// `--proxy-allow-host`, keyed by address range instead of host name.
    #[arg(long, env = "PYPIRON_PROXY_ALLOW_CIDR", value_delimiter = ',')]
    pub(crate) proxy_allow_cidr: Vec<String>,

    /// Advisory feed (OSV PyPI export): a URL or a local path. Powers malware
    /// blocking and the org audit. Defaults to the OSV PyPI export URL (the
    /// startup log names the URL it will poll); `""` disables the feature
    /// entirely. `Option` with no clap default so startup can tell an explicit
    /// setting (fail-closed if it can't produce a snapshot) from the default.
    #[arg(long, env = "PYPIRON_ADVISORY_FEED")]
    pub(crate) advisory_feed: Option<String>,

    /// Refuse to serve or cache artifacts whose (name, version) is in the feed's
    /// malware block set. Default true. Requires a snapshot source — the feed, or
    /// a previously delivered `_advisories/` copy. `Option` with no clap default
    /// so an explicit `--malware-block true` is distinguishable from the default.
    #[arg(long, env = "PYPIRON_MALWARE_BLOCK", action = clap::ArgAction::Set)]
    pub(crate) malware_block: Option<bool>,

    /// How often (seconds) each node probes OSV's per-advisory feed to block a
    /// newly-published malware advisory within minutes, ahead of the daily feed
    /// refresh. Near-zero bandwidth (a conditional CSV GET that 304s most polls)
    /// and no persisted state. `0` disables; inert unless the feed is the OSV
    /// `all.zip` URL. Default 120.
    #[arg(long, env = "PYPIRON_MALWARE_PROBE_SECS", default_value_t = 120)]
    pub(crate) malware_probe_secs: u64,

    /// The slice of PyPI the proxy serves and caches — names included. The same
    /// mirror-selection surface as `sync`, set once and shared: a `[mirror]` table
    /// in pypiron.toml governs both. With a package scope, the proxy serves only
    /// those names (and matching versions) from upstream.
    #[command(flatten)]
    pub(crate) mirror: sync::MirrorArgs,
}

/// Did this arg come from the command line or an env var (as opposed to sitting
/// at its clap default)? This is how `[serve]` layers *under* CLI/env without
/// dropping clap's `[default: …]` hints: the file fills a knob only when the
/// user left it untouched. Panics only on a typo'd id — every id below is a real
/// `ServeArgs`/`StorageArgs` field.
pub fn arg_from_cli_or_env(m: &clap::ArgMatches, id: &str) -> bool {
    matches!(
        m.value_source(id),
        Some(clap::parser::ValueSource::CommandLine) | Some(clap::parser::ValueSource::EnvVariable)
    )
}

/// Parse a `[serve]` string into a clap value-enum, naming the table key on
/// error so a bad `storage = "s4"` reads clearly.
fn serve_value_enum<T: clap::ValueEnum>(key: &str, v: &str) -> Result<T> {
    <T as clap::ValueEnum>::from_str(v, true).map_err(|e| anyhow::anyhow!("[serve].{key}: {e}"))
}

/// Fold the `[serve]` table into the parsed CLI args. Defaulted/bool/enum knobs
/// take the file value only when the CLI/env didn't set them; `Option` knobs use
/// CLI/env-or-file. Secrets (credentials, the Azure access key) are never here —
/// they stay CLI/env only.
pub fn merge_serve_file(
    cli: &mut ServeArgs,
    f: &config::ServeConfig,
    m: &clap::ArgMatches,
) -> Result<()> {
    macro_rules! fill {
        ($field:expr, $id:literal, $val:expr) => {
            if !arg_from_cli_or_env(m, $id) {
                if let Some(v) = $val {
                    $field = v;
                }
            }
        };
    }
    // Like `fill!`, but the file value is a string parsed into a value-enum
    // (`$key` names the table key in a parse error).
    macro_rules! fill_enum {
        ($field:expr, $id:literal, $key:literal, $val:expr) => {
            if !arg_from_cli_or_env(m, $id) {
                if let Some(v) = $val {
                    $field = serve_value_enum($key, v)?;
                }
            }
        };
    }

    // Server knobs (defaulted scalars / bools / enums).
    fill!(cli.bind_addr, "bind_addr", f.bind_addr.clone());
    fill_enum!(
        cli.artifact_delivery,
        "artifact_delivery",
        "artifact-delivery",
        &f.artifact_delivery
    );
    fill!(cli.access_log, "access_log", f.access_log);
    fill!(cli.trusted_proxy, "trusted_proxy", f.trusted_proxy);
    fill!(
        cli.login_cooldown_secs,
        "login_cooldown_secs",
        f.login_cooldown_secs
    );
    fill_enum!(
        cli.access_log_format,
        "access_log_format",
        "access-log-format",
        &f.access_log_format
    );
    fill!(cli.wait_on_upload, "wait_on_upload", f.wait_on_upload);
    fill!(
        cli.wait_on_upload_secs,
        "wait_on_upload_secs",
        f.wait_on_upload_secs
    );
    fill!(
        cli.worker_interval_secs,
        "worker_interval_secs",
        f.worker_interval_secs
    );
    fill!(
        cli.bucket_leave_failures,
        "bucket_leave_failures",
        f.bucket_leave_failures
    );
    fill!(
        cli.bucket_return_healthy_secs,
        "bucket_return_healthy_secs",
        f.bucket_return_healthy_secs
    );
    fill!(
        cli.fanout_grace_secs,
        "fanout_grace_secs",
        f.fanout_grace_secs
    );
    fill!(
        cli.intent_grace_secs,
        "intent_grace_secs",
        f.intent_grace_secs
    );
    fill!(cli.audit_on_boot, "audit_on_boot", f.audit_on_boot);
    fill!(cli.transparency, "transparency", f.transparency);
    fill!(
        cli.allow_insecure_upstream,
        "allow_insecure_upstream",
        f.allow_insecure_upstream
    );
    fill!(
        cli.metrics_project_labels,
        "metrics_project_labels",
        f.metrics_project_labels
    );
    fill!(
        cli.reconcile_interval_secs,
        "reconcile_interval_secs",
        f.reconcile_interval_secs
    );
    fill!(
        cli.malware_probe_secs,
        "malware_probe_secs",
        f.malware_probe_secs
    );
    fill!(
        cli.repl_sweep_interval_secs,
        "repl_sweep_interval_secs",
        f.repl_sweep_interval_secs
    );
    fill!(cli.lease_ttl_secs, "lease_ttl_secs", f.lease_ttl_secs);
    fill!(cli.download_stats, "download_stats", f.download_stats);
    fill!(
        cli.counters_resolution,
        "counters_resolution",
        f.counters_resolution.clone()
    );
    fill!(
        cli.counters_flush_interval_secs,
        "counters_flush_interval_secs",
        f.counters_flush_interval_secs
    );
    fill!(
        cli.counters_rollup_interval_secs,
        "counters_rollup_interval_secs",
        f.counters_rollup_interval_secs
    );
    fill!(
        cli.index_cache_ttl_secs,
        "index_cache_ttl_secs",
        f.index_cache_ttl_secs
    );
    fill!(
        cli.counters_retention_days,
        "counters_retention_days",
        f.counters_retention_days
    );

    // Server-only Option knobs: CLI/env wins when present, else the file.
    cli.proxy_upstream = cli.proxy_upstream.take().or(f.proxy_upstream.clone());
    cli.spool_dir = cli.spool_dir.take().or(f.spool_dir.clone());
    cli.advisory_feed = cli.advisory_feed.take().or(f.advisory_feed.clone());
    cli.malware_block = cli.malware_block.or(f.malware_block);

    // Storage selection is shared with the maintenance commands, so it lives in
    // its own helper.
    merge_storage_file(&mut cli.storage, f, m)
}

/// Fold the storage-selection knobs from `[serve]` into a [`StorageArgs`]. The
/// `[serve]` table is the one place storage is configured; the server *and* the
/// maintenance commands (verify-index, rebuild-index) read it from here, so a
/// `pypiron.toml`-only operator points all of them at the same backend. CLI/env
/// still wins over the file. Credentials/cloud keys never live in the file.
fn merge_storage_file(
    storage: &mut StorageArgs,
    f: &config::ServeConfig,
    m: &clap::ArgMatches,
) -> Result<()> {
    if !arg_from_cli_or_env(m, "s3_force_path_style") {
        if let Some(v) = f.s3_force_path_style {
            storage.s3_force_path_style = v;
        }
    }
    if !arg_from_cli_or_env(m, "azure_use_emulator") {
        if let Some(v) = f.azure_use_emulator {
            storage.azure_use_emulator = v;
        }
    }
    storage.data_dir = storage.data_dir.take().or(f.data_dir.clone());
    storage.storage_prefix = storage.storage_prefix.take().or(f.storage_prefix.clone());
    // The `buckets` list comes from the file only when CLI/env supplied none —
    // CLI/env always wins.
    if storage.buckets.is_empty() {
        if let Some(list) = &f.buckets {
            storage.buckets = list.clone();
        }
    }
    storage.s3_endpoint_url = storage.s3_endpoint_url.take().or(f.s3_endpoint_url.clone());
    storage.gcs_service_account_path = storage
        .gcs_service_account_path
        .take()
        .or(f.gcs_service_account_path.clone());
    storage.gcs_endpoint_url = storage
        .gcs_endpoint_url
        .take()
        .or(f.gcs_endpoint_url.clone());
    storage.azure_account = storage.azure_account.take().or(f.azure_account.clone());
    storage.azure_endpoint_url = storage
        .azure_endpoint_url
        .take()
        .or(f.azure_endpoint_url.clone());
    // Per-bucket overrides are TOML-only (no CLI/env form), so they come from the
    // file whenever the file has them. All six `StorageArgs` embeds fold the same
    // `[serve]` table here, so serve and the maintenance commands resolve the
    // identical per-bucket config.
    if storage.overrides.is_empty() {
        if let Some(map) = &f.bucket {
            storage.overrides = map.clone();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_health_url_defaults_and_follows_bind_port() {
        // Unset → default port.
        assert_eq!(loopback_health_url(None), "http://127.0.0.1:8080/health");
        // The bind port is honored; the bind host is ignored (always loopback).
        assert_eq!(
            loopback_health_url(Some("0.0.0.0:9000")),
            "http://127.0.0.1:9000/health"
        );
        assert_eq!(
            loopback_health_url(Some("[::]:7000")),
            "http://127.0.0.1:7000/health"
        );
        // Garbage never panics — it falls back to the default port.
        assert_eq!(
            loopback_health_url(Some("not-an-addr")),
            "http://127.0.0.1:8080/health"
        );
    }
}
