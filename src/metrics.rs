//! Process counters served at `/metrics` in Prometheus text format.
//!
//! Hand-rolled atomics, no metrics crate: the counter set is small and fixed,
//! and the exposition format is a dozen lines of text. Requests are bucketed
//! by route group and status class — low cardinality on purpose (per-package
//! labels would make the scrape payload scale with the registry).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::bucket_health::{HealthState, WorkerHealthSnapshot};
use crate::clock::unix_now_secs;

/// Route groups, by path prefix. Order matches the counter matrix.
pub(crate) const ROUTES: [&str; 6] = ["simple", "files", "legacy", "health", "metrics", "other"];
/// Status classes. Order matches the counter matrix.
const STATUS_CLASSES: [&str; 4] = ["2xx", "3xx", "4xx", "5xx"];

/// Index of the `files` route and the `2xx` status class in the matrix above.
/// The dashboard's "files served" tile is files-route successes; naming the
/// indices (and asserting them in a test) keeps that out of magic numbers.
const ROUTE_FILES: usize = 1;
const CLASS_2XX: usize = 0;

/// Cap on distinct project attribution tags. Tags are client-supplied
/// (basic-auth username subaddresses), so without a cap a hostile or
/// misconfigured client could grow the scrape payload without bound.
/// Past the cap, new tags land in [`OVERFLOW_TAG`].
const MAX_PROJECT_TAGS: usize = 256;
const OVERFLOW_TAG: &str = "_overflow";

#[derive(Default)]
struct BucketMetricState {
    names: Vec<String>,
    states: Vec<HealthState>,
    selected_index: usize,
    read_selected_index: usize,
    selection_generation: u64,
    alarm_totals: HashMap<String, u64>,
    topology_write_fenced: bool,
}

/// Index into [`ROUTES`] for a request path.
pub fn route_group(path: &str) -> usize {
    if path == "/simple" || path.starts_with("/simple/") {
        0
    } else if path.starts_with("/files/") {
        1
    } else if path == "/legacy" || path.starts_with("/legacy/") {
        2
    } else if path == "/health" {
        3
    } else if path == "/metrics" {
        4
    } else {
        5
    }
}

#[derive(Default)]
pub struct Metrics {
    /// requests[route][status_class]
    requests: [[AtomicU64; STATUS_CLASSES.len()]; ROUTES.len()],
    /// Artifact downloads served, this node, since boot. Counts real artifacts
    /// only (sidecar companions and range partials excluded) on BOTH delivery
    /// paths — streamed 200s and presigned 302s — so unlike `files_served` it
    /// stays accurate on an S3/redirect node. A single aggregate on purpose: the
    /// per-package/version breakdown lives in the counter store (`_counters/`),
    /// never here, to keep the scrape payload off registry-sized cardinality.
    downloads: AtomicU64,
    /// Artifact downloads refused by the malware byte gate (an advisory-blocked
    /// version, or a quarantined project). Unlabeled on purpose — the package,
    /// version, and advisory ids go to the warn log, keeping `/metrics`
    /// low-cardinality. A nonzero rate is the active-remediation signal: some
    /// machine still *asks* for malware — an incident, not a statistic.
    blocked_downloads: AtomicU64,
    /// Package index rebuilds (worker + reconcile + deletes).
    pub index_rebuilds: AtomicU64,
    /// Full reconcile sweeps completed.
    pub reconcile_sweeps: AtomicU64,
    /// Audit outcomes, summed across passes: packages the audit rebuilt
    /// (fingerprint differed or force-deep) vs skipped (fingerprint hit, zero
    /// reads). A high skip ratio is the daily-audit default earning its keep.
    pub audit_packages_rebuilt: AtomicU64,
    pub audit_packages_skipped: AtomicU64,
    /// Last completed audit's wall duration, seconds, as f64 bits (gauge).
    audit_last_duration_bits: AtomicU64,
    /// Registry inventory, recomputed each full sweep from the shard listings
    /// (zero extra reads): distinct projects with artifacts, distinct
    /// (project, version) releases, and artifact files (sidecars excluded).
    /// `inventory_ready` flips true after the first clean sweep — until then
    /// the homepage shows nothing rather than a misleading zero.
    inventory_ready: std::sync::atomic::AtomicBool,
    inventory_projects: AtomicU64,
    inventory_releases: AtomicU64,
    inventory_files: AtomicU64,
    /// Total bytes of artifact files (sidecars excluded), summed off the same
    /// shard listings as the file count — the `size` already in each listing.
    inventory_bytes: AtomicU64,
    /// Global-index CAS write-backs lost to a peer (reload-and-retry fired).
    /// A nonzero value is two nodes legitimately racing the name set — the
    /// proof that dual leadership is converging, not corrupting.
    pub global_cas_conflicts: AtomicU64,
    /// Unpaired intents consumed after the grace period: a writer dropped an
    /// intent and died before committing. A rising rate means writers crash.
    pub stale_intents_healed: AtomicU64,
    /// Upstream package-listing fetches (proxy mode), by outcome.
    pub proxy_listing_fetches: AtomicU64,
    pub proxy_listing_errors: AtomicU64,
    /// Upstream artifacts downloaded and committed to storage (proxy mode).
    pub proxy_artifacts_cached: AtomicU64,
    pub proxy_artifact_errors: AtomicU64,
    /// requests by project attribution tag and route group. A mutex, not
    /// atomics: only requests that carry credentials touch it, and the
    /// critical section is one map bump.
    project_requests: Mutex<HashMap<String, [u64; ROUTES.len()]>>,

    /// True once more than one bucket is configured. The replication metrics
    /// below are dormant machinery on a single-bucket node, so they are omitted
    /// from the exposition entirely until multi-bucket is live (design §4/G).
    multi_bucket: std::sync::atomic::AtomicBool,
    /// Records (sidecar+artifact pairs) copied into another bucket, and their
    /// artifact bytes — the quantifiable cost of keeping the warm copies warm.
    pub replication_objects: AtomicU64,
    pub replication_bytes: AtomicU64,
    /// Byte-conflict freezes (§6.3): same filename, different bytes on two
    /// buckets. Every nonzero value is a human-actionable split-brain.
    pub replication_freezes: AtomicU64,
    /// Different-byte private/private conflicts resolved by keeping the older
    /// upload and quarantining the loser. Every event is an operator alarm.
    pub replication_conflict_quarantines: AtomicU64,
    /// Last pairwise reconcile-diff wall duration, seconds, as f64 bits (gauge).
    reconcile_diff_duration_bits: AtomicU64,
    /// Undelivered `_repl/` markers, by destination bucket, as measured by the
    /// last sweep (gauge). Low cardinality — one series per configured bucket.
    marker_backlog: Mutex<HashMap<String, u64>>,
    /// Latest P4 health/selection view. `None` until a multi-bucket worker
    /// publishes its first snapshot; never populated by a single-bucket node.
    bucket_health: Mutex<Option<BucketMetricState>>,
    /// Unix seconds of this node's last successful advisory-snapshot refresh
    /// (leader source poll, or a follower's storage check). 0 = never — the
    /// `pypiron_advisory_snapshot_age_seconds` gauge is omitted until then.
    advisory_last_refresh_unix: AtomicU64,
    /// Unix seconds of this node's last successful malware-probe cycle (a CSV poll,
    /// including a 304). 0 = never — the `pypiron_malware_probe_age_seconds` gauge
    /// is omitted until the first probe lands.
    malware_probe_last_unix: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark this node's advisory snapshot as freshly refreshed (resets the age
    /// gauge). Called once per successful refresh cycle by the worker tick.
    pub fn advisory_refresh_ok(&self) {
        self.advisory_last_refresh_unix
            .store(unix_now_secs(), Ordering::Relaxed);
    }

    /// Mark this node's malware probe as freshly polled (resets the probe-age
    /// gauge). Called once per successful probe cycle, a 304 included.
    pub fn malware_probe_ok(&self) {
        self.malware_probe_last_unix
            .store(unix_now_secs(), Ordering::Relaxed);
    }

    /// Count a request against a project attribution tag. The tag must
    /// already be sanitized (label-safe charset) by the caller.
    pub fn record_project(&self, tag: &str, route: usize) {
        let mut map = self
            .project_requests
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(counts) = map.get_mut(tag) {
            counts[route] += 1;
            return;
        }
        let key = if map.len() < MAX_PROJECT_TAGS {
            tag
        } else {
            OVERFLOW_TAG
        };
        map.entry(key.to_string()).or_insert([0; ROUTES.len()])[route] += 1;
    }

    /// Record the wall duration of the audit pass that just completed.
    pub fn set_audit_duration(&self, secs: f64) {
        self.audit_last_duration_bits
            .store(secs.to_bits(), Ordering::Relaxed);
    }

    /// Arm the replication metric family (called once at startup when more than
    /// one bucket is configured). Until then the family is omitted entirely.
    pub fn set_multi_bucket(&self) {
        self.multi_bucket.store(true, Ordering::Relaxed);
    }

    /// Count one replicated record: a sidecar+artifact pair copied into another
    /// bucket, and its artifact bytes.
    pub fn record_replicated(&self, bytes: u64) {
        self.replication_objects.fetch_add(1, Ordering::Relaxed);
        self.replication_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record the wall duration of the reconcile diff that just completed.
    pub fn set_reconcile_diff_duration(&self, secs: f64) {
        self.reconcile_diff_duration_bits
            .store(secs.to_bits(), Ordering::Relaxed);
    }

    /// Publish the per-destination `_repl/` marker backlog measured by a sweep.
    pub fn set_marker_backlog(&self, dest: &str, count: u64) {
        self.marker_backlog
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(dest.to_string(), count);
    }

    /// Publish one worker health snapshot and accumulate its per-bucket alarm
    /// deltas. This is the only P4 metrics update surface: names establish the
    /// bounded label set, `generation` is the applied BucketSet generation, and
    /// `topology_write_fenced` is the fail-closed runtime mismatch state.
    ///
    /// A single bucket is a hard no-op, even if called accidentally. Multi-bucket
    /// mode is armed here as well as at startup so call ordering cannot suppress
    /// the series.
    pub fn update_bucket_health(
        &self,
        snapshot: &WorkerHealthSnapshot,
        bucket_names: &[String],
        generation: u64,
        topology_write_fenced: bool,
    ) {
        if bucket_names.len() < 2 {
            return;
        }
        self.set_multi_bucket();

        let mut guard = self.bucket_health.lock().unwrap_or_else(|e| e.into_inner());
        let state = guard.get_or_insert_with(BucketMetricState::default);
        state.names = bucket_names.to_vec();
        state.states = bucket_names
            .iter()
            .enumerate()
            .map(|(index, _)| {
                snapshot
                    .states
                    .get(index)
                    .copied()
                    .unwrap_or(HealthState::Unknown)
            })
            .collect();
        state.selected_index = snapshot.selected_index;
        state.read_selected_index = snapshot.read_selected_index;
        state.selection_generation = generation;
        state.topology_write_fenced = topology_write_fenced;
        for (index, name) in bucket_names.iter().enumerate() {
            let delta = snapshot.alarms.get(index).copied().unwrap_or(0);
            let total = state.alarm_totals.entry(name.clone()).or_default();
            *total = total.saturating_add(delta);
        }
    }

    /// Publish the registry inventory measured by a clean sweep.
    pub fn set_inventory(&self, projects: u64, releases: u64, files: u64, bytes: u64) {
        self.inventory_projects.store(projects, Ordering::Relaxed);
        self.inventory_releases.store(releases, Ordering::Relaxed);
        self.inventory_files.store(files, Ordering::Relaxed);
        self.inventory_bytes.store(bytes, Ordering::Relaxed);
        self.inventory_ready.store(true, Ordering::Relaxed);
    }

    /// The last measured inventory, or `None` before the first sweep completes.
    pub fn inventory(&self) -> Option<Inventory> {
        self.inventory_ready
            .load(Ordering::Relaxed)
            .then(|| Inventory {
                projects: self.inventory_projects.load(Ordering::Relaxed),
                releases: self.inventory_releases.load(Ordering::Relaxed),
                files: self.inventory_files.load(Ordering::Relaxed),
                bytes: self.inventory_bytes.load(Ordering::Relaxed),
            })
    }

    /// Count one delivered artifact download (this node). Called from the file
    /// handler alongside the durable per-package counter.
    pub fn record_download(&self) {
        self.downloads.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one download refused by the malware byte gate (advisory-blocked or
    /// quarantined). Called at the single block site in the file handler.
    pub fn record_blocked_download(&self) {
        self.blocked_downloads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_request(&self, route: usize, status: u16) {
        let class = match status {
            200..=299 => 0,
            300..=399 => 1,
            400..=499 => 2,
            _ => 3,
        };
        self.requests[route][class].fetch_add(1, Ordering::Relaxed);
    }

    /// A consistent-enough point-in-time copy of the request counters for the
    /// human dashboard (`/dashboard`). Atomics are read individually, not under
    /// a global lock, so totals can be off by a handful under concurrent
    /// traffic — fine for a glanceable page, never used for correctness.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut requests = [[0u64; STATUS_CLASSES.len()]; ROUTES.len()];
        for (r, row) in requests.iter_mut().enumerate() {
            for (c, cell) in row.iter_mut().enumerate() {
                *cell = self.requests[r][c].load(Ordering::Relaxed);
            }
        }
        MetricsSnapshot {
            requests,
            downloads: self.downloads.load(Ordering::Relaxed),
        }
    }

    /// Prometheus text exposition (format version 0.0.4).
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(2048);
        out.push_str(
            "# HELP pypiron_http_requests_total HTTP requests by route group and status class.\n",
        );
        out.push_str("# TYPE pypiron_http_requests_total counter\n");
        for (r, route) in ROUTES.iter().enumerate() {
            for (c, class) in STATUS_CLASSES.iter().enumerate() {
                let v = self.requests[r][c].load(Ordering::Relaxed);
                out.push_str(&format!(
                    "pypiron_http_requests_total{{route=\"{route}\",status=\"{class}\"}} {v}\n"
                ));
            }
        }
        for (name, help, value) in [
            (
                "pypiron_downloads_total",
                "Artifact downloads served (real artifacts; streamed 200s and presigned 302s).",
                &self.downloads,
            ),
            (
                "pypiron_blocked_downloads_total",
                "Artifact downloads refused by the malware byte gate (advisory-blocked or quarantined).",
                &self.blocked_downloads,
            ),
            (
                "pypiron_index_rebuilds_total",
                "Package index rebuilds.",
                &self.index_rebuilds,
            ),
            (
                "pypiron_reconcile_sweeps_total",
                "Full reconcile sweeps completed.",
                &self.reconcile_sweeps,
            ),
            (
                "pypiron_audit_packages_rebuilt_total",
                "Packages the audit rebuilt (fingerprint differed or force-deep).",
                &self.audit_packages_rebuilt,
            ),
            (
                "pypiron_audit_packages_skipped_total",
                "Packages the audit skipped on a fingerprint hit (zero reads).",
                &self.audit_packages_skipped,
            ),
            (
                "pypiron_global_cas_conflicts_total",
                "Global-index CAS write-backs lost to a peer (reload-and-retry).",
                &self.global_cas_conflicts,
            ),
            (
                "pypiron_stale_intents_healed_total",
                "Unpaired intents consumed after the grace period (crashed writer).",
                &self.stale_intents_healed,
            ),
            (
                "pypiron_proxy_listing_fetches_total",
                "Upstream package-listing fetches.",
                &self.proxy_listing_fetches,
            ),
            (
                "pypiron_proxy_listing_errors_total",
                "Upstream package-listing fetch failures.",
                &self.proxy_listing_errors,
            ),
            (
                "pypiron_proxy_artifacts_cached_total",
                "Upstream artifacts downloaded and committed to storage.",
                &self.proxy_artifacts_cached,
            ),
            (
                "pypiron_proxy_artifact_errors_total",
                "Upstream artifact fetch or verification failures.",
                &self.proxy_artifact_errors,
            ),
        ] {
            emit_counter(&mut out, name, help, value.load(Ordering::Relaxed));
        }
        let audit_secs = f64::from_bits(self.audit_last_duration_bits.load(Ordering::Relaxed));
        emit_gauge(
            &mut out,
            "pypiron_audit_last_duration_seconds",
            "Wall duration of the last completed audit pass.",
            audit_secs,
        );
        // Advisory snapshot staleness (per node). Emitted only once a refresh has
        // succeeded — a permanent zero on a node that never armed the feature
        // would be a misleading "0 seconds old". Age is computed at render time.
        let advisory_refresh = self.advisory_last_refresh_unix.load(Ordering::Relaxed);
        if advisory_refresh != 0 {
            let age = unix_now_secs().saturating_sub(advisory_refresh);
            emit_gauge(
                &mut out,
                "pypiron_advisory_snapshot_age_seconds",
                "Seconds since this node last refreshed the advisory snapshot.",
                age,
            );
        }
        // Malware-probe staleness (per node). Emitted only once a probe has
        // succeeded — a node that never armed the probe would otherwise show a
        // misleading "0 seconds". Age is computed at render time.
        let probe_refresh = self.malware_probe_last_unix.load(Ordering::Relaxed);
        if probe_refresh != 0 {
            let age = unix_now_secs().saturating_sub(probe_refresh);
            emit_gauge(
                &mut out,
                "pypiron_malware_probe_age_seconds",
                "Seconds since this node's last successful malware probe (CSV poll).",
                age,
            );
        }
        for (name, help, value) in [
            (
                "pypiron_registry_projects",
                "Distinct projects with at least one artifact (last sweep).",
                self.inventory_projects.load(Ordering::Relaxed),
            ),
            (
                "pypiron_registry_releases",
                "Distinct (project, version) releases (last sweep).",
                self.inventory_releases.load(Ordering::Relaxed),
            ),
            (
                "pypiron_registry_files",
                "Artifact files, excluding sidecars (last sweep).",
                self.inventory_files.load(Ordering::Relaxed),
            ),
            (
                "pypiron_registry_bytes",
                "Total bytes of artifact files, excluding sidecars (last sweep).",
                self.inventory_bytes.load(Ordering::Relaxed),
            ),
        ] {
            emit_gauge(&mut out, name, help, value);
        }
        // Replication family — emitted only on a multi-bucket node (design §G);
        // a single-bucket node never runs replication, so the series would be a
        // permanent row of zeros.
        if self.multi_bucket.load(Ordering::Relaxed) {
            for (name, help, value) in [
                (
                    "pypiron_replication_objects_total",
                    "Records (sidecar+artifact) copied into another bucket.",
                    &self.replication_objects,
                ),
                (
                    "pypiron_replication_bytes_total",
                    "Artifact bytes copied into other buckets.",
                    &self.replication_bytes,
                ),
                (
                    "pypiron_replication_freezes_total",
                    "Byte-conflict freezes (same filename, different bytes on two buckets).",
                    &self.replication_freezes,
                ),
                (
                    "pypiron_replication_conflict_quarantines_total",
                    "Byte conflicts resolved by keeping the first upload and quarantining the loser.",
                    &self.replication_conflict_quarantines,
                ),
            ] {
                emit_counter(&mut out, name, help, value.load(Ordering::Relaxed));
            }
            let diff_secs =
                f64::from_bits(self.reconcile_diff_duration_bits.load(Ordering::Relaxed));
            emit_gauge(
                &mut out,
                "pypiron_reconcile_diff_duration_seconds",
                "Wall duration of the last pairwise reconcile diff.",
                diff_secs,
            );
            let backlog = self
                .marker_backlog
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            out.push_str(
                "# HELP pypiron_replication_marker_backlog Undelivered _repl/ markers found on reachable source buckets, by destination.\n",
            );
            out.push_str("# TYPE pypiron_replication_marker_backlog gauge\n");
            let mut dests: Vec<&String> = backlog.keys().collect();
            dests.sort();
            for dest in dests {
                out.push_str(&format!(
                    "pypiron_replication_marker_backlog{{dest=\"{dest}\"}} {}\n",
                    backlog[dest]
                ));
            }
            drop(backlog);
            self.render_bucket_health(&mut out);
        }
        let map = self
            .project_requests
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !map.is_empty() {
            out.push_str("# HELP pypiron_project_requests_total Requests by client project tag (basic-auth username subaddress) and route group.\n");
            out.push_str("# TYPE pypiron_project_requests_total counter\n");
            let mut tags: Vec<&String> = map.keys().collect();
            tags.sort();
            for tag in tags {
                for (r, route) in ROUTES.iter().enumerate() {
                    let v = map[tag][r];
                    if v > 0 {
                        out.push_str(&format!(
                            "pypiron_project_requests_total{{project=\"{tag}\",route=\"{route}\"}} {v}\n"
                        ));
                    }
                }
            }
        }
        out
    }

    fn render_bucket_health(&self, out: &mut String) {
        let guard = self.bucket_health.lock().unwrap_or_else(|e| e.into_inner());
        let Some(state) = guard.as_ref() else {
            return;
        };

        out.push_str(
            "# HELP pypiron_bucket_health_state Per-node bucket health: healthy=1, unknown=0, unhealthy=-1.\n",
        );
        out.push_str("# TYPE pypiron_bucket_health_state gauge\n");
        out.push_str("# HELP pypiron_bucket_selected Selected (write) bucket (one-hot).\n");
        out.push_str("# TYPE pypiron_bucket_selected gauge\n");
        out.push_str(
            "# HELP pypiron_bucket_read_selected Read-serving bucket for this node (one-hot).\n",
        );
        out.push_str("# TYPE pypiron_bucket_read_selected gauge\n");
        out.push_str(
            "# HELP pypiron_bucket_health_alarms_total Credential, CAS, KMS, quota, configuration, and other non-availability storage errors.\n",
        );
        out.push_str("# TYPE pypiron_bucket_health_alarms_total counter\n");
        for (index, name) in state.names.iter().enumerate() {
            let name = prometheus_label(name);
            let labels = format!("bucket=\"{name}\",index=\"{index}\"");
            let health = match state
                .states
                .get(index)
                .copied()
                .unwrap_or(HealthState::Unknown)
            {
                HealthState::Healthy => 1,
                HealthState::Unknown => 0,
                HealthState::Unhealthy => -1,
            };
            let selected = usize::from(index == state.selected_index);
            let read_selected = usize::from(index == state.read_selected_index);
            let alarms = state
                .alarm_totals
                .get(&state.names[index])
                .copied()
                .unwrap_or(0);
            out.push_str(&format!(
                "pypiron_bucket_health_state{{{labels}}} {health}\n"
            ));
            out.push_str(&format!("pypiron_bucket_selected{{{labels}}} {selected}\n"));
            out.push_str(&format!(
                "pypiron_bucket_read_selected{{{labels}}} {read_selected}\n"
            ));
            out.push_str(&format!(
                "pypiron_bucket_health_alarms_total{{{labels}}} {alarms}\n"
            ));
        }
        emit_gauge(
            out,
            "pypiron_bucket_selection_generation",
            "Storage selection generation.",
            state.selection_generation,
        );
        emit_gauge(
            out,
            "pypiron_bucket_topology_write_fenced",
            "Writes blocked by a runtime topology mismatch (1=fenced).",
            u8::from(state.topology_write_fenced),
        );
    }
}

/// Emit a single-value counter metric: `# HELP`, `# TYPE`, and the value line.
fn emit_counter(out: &mut String, name: &str, help: &str, value: impl std::fmt::Display) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
    ));
}

/// Emit a single-value gauge metric: `# HELP`, `# TYPE`, and the value line.
fn emit_gauge(out: &mut String, name: &str, help: &str, value: impl std::fmt::Display) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n"
    ));
}

fn prometheus_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Registry size: distinct projects with at least one artifact, distinct
/// `(project, version)` releases, and artifact files (sidecar/metadata/
/// provenance excluded) with their total bytes. Shown under the homepage
/// header, and the serialized form of the storage-backed view
/// `_state/inventory.json` (see worker.rs). `#[serde(default)]` keeps an older
/// or truncated object readable as zeros rather than a parse error.
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Inventory {
    #[serde(default)]
    pub projects: u64,
    #[serde(default)]
    pub releases: u64,
    #[serde(default)]
    pub files: u64,
    /// Total bytes of those artifact files (sidecars excluded).
    #[serde(default)]
    pub bytes: u64,
}

/// A copy of the request counters at one instant, with the derived numbers the
/// dashboard shows. Plain data so [`crate::html::dashboard_html`] stays a pure
/// function that can be unit-tested without spinning up [`Metrics`].
pub struct MetricsSnapshot {
    /// `requests[route][status_class]`, indexed by [`ROUTES`]/`STATUS_CLASSES`.
    requests: [[u64; STATUS_CLASSES.len()]; ROUTES.len()],
    /// Artifact downloads served this node since boot (both delivery paths).
    downloads: u64,
}

impl MetricsSnapshot {
    /// Every request the process has served since boot.
    pub fn total_requests(&self) -> u64 {
        self.requests.iter().flatten().sum()
    }

    /// Successful (2xx) responses on the `/files/` route — wheels streamed by
    /// this node plus their `.metadata`/`.provenance` companion fetches. NOT a
    /// faithful "downloads" count: under `redirect`/`auto` delivery a wheel GET
    /// is a 302 (lands in files-route 3xx, excluded here), so on an S3 node this
    /// reads ~0 because the bytes come from S3, not us. Labeled "Files served".
    pub fn files_served(&self) -> u64 {
        self.requests[ROUTE_FILES][CLASS_2XX]
    }

    /// Artifact downloads served by this node since boot — real artifacts on
    /// both delivery paths (streamed 200s and presigned 302s), so it stays
    /// accurate on an S3/redirect node where [`Self::files_served`] reads ~0.
    pub fn downloads_total(&self) -> u64 {
        self.downloads
    }

    /// `(route_group, total requests)` across all status classes, in matrix
    /// order; callers sort/filter for the "top route groups" chart.
    pub fn route_totals(&self) -> Vec<(&'static str, u64)> {
        ROUTES
            .iter()
            .enumerate()
            .map(|(r, name)| (*name, self.requests[r].iter().sum()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_snapshot(
        selected_index: usize,
        states: Vec<HealthState>,
        alarms: Vec<u64>,
    ) -> WorkerHealthSnapshot {
        WorkerHealthSnapshot {
            selected_index,
            selection_change: None,
            read_selected_index: selected_index,
            read_selection_change: None,
            states,
            topology_revalidation: Vec::new(),
            alarms,
        }
    }

    #[test]
    fn matrix_indices_match_route_names() {
        assert_eq!(ROUTES[ROUTE_FILES], "files");
        assert_eq!(STATUS_CLASSES[CLASS_2XX], "2xx");
    }

    #[test]
    fn snapshot_reports_totals_files_served_and_breakdowns() {
        let m = Metrics::new();
        m.record_request(route_group("/simple/"), 200);
        m.record_request(route_group("/simple/"), 404);
        m.record_request(route_group("/files/six/six.whl"), 200);
        m.record_request(route_group("/files/six/six.whl"), 200);
        m.record_download();
        m.record_download();
        let snap = m.snapshot();
        assert_eq!(snap.total_requests(), 4);
        assert_eq!(snap.files_served(), 2);
        assert_eq!(snap.downloads_total(), 2);
        let routes: std::collections::HashMap<_, _> = snap.route_totals().into_iter().collect();
        assert_eq!(routes["simple"], 2);
        assert_eq!(routes["files"], 2);
    }

    #[test]
    fn route_groups_classify_paths() {
        assert_eq!(ROUTES[route_group("/simple/")], "simple");
        assert_eq!(ROUTES[route_group("/simple/six/index.json")], "simple");
        assert_eq!(ROUTES[route_group("/files/six/six.whl")], "files");
        assert_eq!(ROUTES[route_group("/legacy/")], "legacy");
        assert_eq!(ROUTES[route_group("/health")], "health");
        assert_eq!(ROUTES[route_group("/metrics")], "metrics");
        assert_eq!(ROUTES[route_group("/nope")], "other");
    }

    #[test]
    fn renders_prometheus_text() {
        let m = Metrics::new();
        m.record_request(route_group("/simple/"), 200);
        m.record_request(route_group("/simple/"), 404);
        m.proxy_artifacts_cached.fetch_add(3, Ordering::Relaxed);
        m.record_download();
        let text = m.render();
        assert!(text.contains("pypiron_http_requests_total{route=\"simple\",status=\"2xx\"} 1"));
        assert!(text.contains("# TYPE pypiron_downloads_total counter"));
        assert!(text.contains("pypiron_downloads_total 1"));
        // The malware byte-gate tripwire is always present (at zero until a block).
        assert!(text.contains("# TYPE pypiron_blocked_downloads_total counter"));
        assert!(text.contains("pypiron_blocked_downloads_total 0"));
        assert!(text.contains("pypiron_http_requests_total{route=\"simple\",status=\"4xx\"} 1"));
        assert!(text.contains("pypiron_proxy_artifacts_cached_total 3"));
        assert!(text.contains("# TYPE pypiron_http_requests_total counter"));
        // New worker/audit counters and the duration gauge are always present.
        assert!(text.contains("pypiron_audit_packages_rebuilt_total 0"));
        assert!(text.contains("pypiron_audit_packages_skipped_total 0"));
        assert!(text.contains("pypiron_global_cas_conflicts_total 0"));
        assert!(text.contains("pypiron_stale_intents_healed_total 0"));
        assert!(text.contains("# TYPE pypiron_audit_last_duration_seconds gauge"));
        assert!(text.contains("pypiron_audit_last_duration_seconds 0"));
        // No project traffic recorded: the family is omitted entirely.
        assert!(!text.contains("pypiron_project_requests_total"));
    }

    #[test]
    fn audit_duration_gauge_reflects_last_pass() {
        let m = Metrics::new();
        m.set_audit_duration(12.5);
        m.audit_packages_rebuilt.fetch_add(2, Ordering::Relaxed);
        m.audit_packages_skipped.fetch_add(40, Ordering::Relaxed);
        let text = m.render();
        assert!(
            text.contains("pypiron_audit_last_duration_seconds 12.5"),
            "{text}"
        );
        assert!(text.contains("pypiron_audit_packages_rebuilt_total 2"));
        assert!(text.contains("pypiron_audit_packages_skipped_total 40"));
    }

    #[test]
    fn renders_multi_bucket_health_selection_alarms_generation_and_fence() {
        let m = Metrics::new();
        m.replication_conflict_quarantines
            .fetch_add(2, Ordering::Relaxed);
        let names = vec![
            "iron-east".to_string(),
            "iron-west".to_string(),
            "iron-eu".to_string(),
        ];
        m.update_bucket_health(
            &worker_snapshot(
                1,
                vec![
                    HealthState::Unhealthy,
                    HealthState::Healthy,
                    HealthState::Unknown,
                ],
                vec![2, 0, 3],
            ),
            &names,
            7,
            true,
        );

        let text = m.render();
        assert!(text.contains("pypiron_bucket_health_state{bucket=\"iron-east\",index=\"0\"} -1"));
        assert!(text.contains("pypiron_bucket_health_state{bucket=\"iron-west\",index=\"1\"} 1"));
        assert!(text.contains("pypiron_bucket_health_state{bucket=\"iron-eu\",index=\"2\"} 0"));
        assert!(text.contains("pypiron_bucket_selected{bucket=\"iron-east\",index=\"0\"} 0"));
        assert!(text.contains("pypiron_bucket_selected{bucket=\"iron-west\",index=\"1\"} 1"));
        assert!(
            text.contains("pypiron_bucket_health_alarms_total{bucket=\"iron-east\",index=\"0\"} 2")
        );
        assert!(
            text.contains("pypiron_bucket_health_alarms_total{bucket=\"iron-eu\",index=\"2\"} 3")
        );
        assert!(text.contains("pypiron_bucket_selection_generation 7"));
        assert!(text.contains("pypiron_bucket_topology_write_fenced 1"));
        assert!(text.contains("pypiron_replication_conflict_quarantines_total 2"));
    }

    #[test]
    fn bucket_alarm_deltas_accumulate_and_gauges_replace() {
        let m = Metrics::new();
        let names = vec!["east".to_string(), "west".to_string()];
        m.update_bucket_health(
            &worker_snapshot(
                0,
                vec![HealthState::Healthy, HealthState::Unknown],
                vec![2, 1],
            ),
            &names,
            3,
            true,
        );
        m.update_bucket_health(
            &worker_snapshot(
                1,
                vec![HealthState::Healthy, HealthState::Healthy],
                vec![4, 8],
            ),
            &names,
            4,
            false,
        );

        let text = m.render();
        assert!(text.contains("pypiron_bucket_health_alarms_total{bucket=\"east\",index=\"0\"} 6"));
        assert!(text.contains("pypiron_bucket_health_alarms_total{bucket=\"west\",index=\"1\"} 9"));
        assert!(text.contains("pypiron_bucket_selected{bucket=\"west\",index=\"1\"} 1"));
        assert!(text.contains("pypiron_bucket_selection_generation 4"));
        assert!(text.contains("pypiron_bucket_topology_write_fenced 0"));
    }

    #[test]
    fn single_bucket_health_update_emits_no_multi_bucket_families() {
        let m = Metrics::new();
        m.update_bucket_health(
            &worker_snapshot(0, vec![HealthState::Healthy], vec![9]),
            &["only".to_string()],
            12,
            true,
        );
        let text = m.render();
        assert!(!text.contains("pypiron_bucket_health_state"));
        assert!(!text.contains("pypiron_bucket_selected"));
        assert!(!text.contains("pypiron_bucket_selection_generation"));
        assert!(!text.contains("pypiron_bucket_health_alarms_total"));
        assert!(!text.contains("pypiron_bucket_topology_write_fenced"));
        assert!(!text.contains("pypiron_replication_objects_total"));
    }

    #[test]
    fn prometheus_bucket_labels_are_escaped() {
        assert_eq!(prometheus_label("east\\\"\nwest"), "east\\\\\\\"\\nwest");
    }

    #[test]
    fn inventory_is_none_until_set_then_reports_and_exposes_gauges() {
        let m = Metrics::new();
        assert!(m.inventory().is_none());
        // Gauges are present (at zero) before the first sweep.
        assert!(m.render().contains("pypiron_registry_projects 0"));

        m.set_inventory(12, 345, 6789, 1_048_576);
        let inv = m.inventory().expect("inventory set");
        assert_eq!(
            (inv.projects, inv.releases, inv.files, inv.bytes),
            (12, 345, 6789, 1_048_576)
        );
        let text = m.render();
        assert!(text.contains("# TYPE pypiron_registry_releases gauge"));
        assert!(text.contains("pypiron_registry_projects 12"));
        assert!(text.contains("pypiron_registry_releases 345"));
        assert!(text.contains("pypiron_registry_files 6789"));
        assert!(text.contains("pypiron_registry_bytes 1048576"));
    }

    #[test]
    fn records_project_attribution() {
        let m = Metrics::new();
        m.record_project("billing-api", route_group("/simple/"));
        m.record_project("billing-api", route_group("/simple/"));
        m.record_project("billing-api", route_group("/files/six/six.whl"));
        m.record_project("etl", route_group("/simple/"));
        let text = m.render();
        assert!(text.contains(
            "pypiron_project_requests_total{project=\"billing-api\",route=\"simple\"} 2"
        ));
        assert!(text
            .contains("pypiron_project_requests_total{project=\"billing-api\",route=\"files\"} 1"));
        assert!(text.contains("pypiron_project_requests_total{project=\"etl\",route=\"simple\"} 1"));
        // Zero cells are omitted.
        assert!(!text.contains("project=\"etl\",route=\"files\""));
    }

    #[test]
    fn project_tags_cap_into_overflow() {
        let m = Metrics::new();
        for i in 0..MAX_PROJECT_TAGS {
            m.record_project(&format!("tag{i}"), 0);
        }
        m.record_project("one-too-many", 0);
        m.record_project("and-another", 0);
        // Known tags still count past the cap.
        m.record_project("tag0", 0);
        let text = m.render();
        assert!(!text.contains("one-too-many"));
        assert!(!text.contains("and-another"));
        assert!(text
            .contains("pypiron_project_requests_total{project=\"_overflow\",route=\"simple\"} 2"));
        assert!(
            text.contains("pypiron_project_requests_total{project=\"tag0\",route=\"simple\"} 2")
        );
    }
}
