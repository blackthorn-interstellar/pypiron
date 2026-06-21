//! Process counters served at `/metrics` in Prometheus text format.
//!
//! Hand-rolled atomics, no metrics crate: the counter set is small and fixed,
//! and the exposition format is a dozen lines of text. Requests are bucketed
//! by route group and status class — low cardinality on purpose (per-package
//! labels would make the scrape payload scale with the registry).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Route groups, by path prefix. Order matches the counter matrix.
pub const ROUTES: [&str; 6] = ["simple", "files", "legacy", "health", "metrics", "other"];
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
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
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
        let map = self
            .project_requests
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let project_requests = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
        MetricsSnapshot {
            requests,
            project_requests,
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
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} counter\n"));
            out.push_str(&format!("{name} {}\n", value.load(Ordering::Relaxed)));
        }
        let audit_secs = f64::from_bits(self.audit_last_duration_bits.load(Ordering::Relaxed));
        out.push_str(
            "# HELP pypiron_audit_last_duration_seconds Wall duration of the last completed audit pass.\n",
        );
        out.push_str("# TYPE pypiron_audit_last_duration_seconds gauge\n");
        out.push_str(&format!(
            "pypiron_audit_last_duration_seconds {audit_secs}\n"
        ));
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
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n"));
            out.push_str(&format!("{name} {value}\n"));
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
/// dashboard shows. Plain data so [`crate::web::dashboard_html`] stays a pure
/// function that can be unit-tested without spinning up [`Metrics`].
pub struct MetricsSnapshot {
    /// `requests[route][status_class]`, indexed by [`ROUTES`]/`STATUS_CLASSES`.
    requests: [[u64; STATUS_CLASSES.len()]; ROUTES.len()],
    /// `(project_tag, per-route counts)` for every attribution tag seen.
    project_requests: Vec<(String, [u64; ROUTES.len()])>,
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

    /// `(project_tag, total requests)` across all routes; callers sort/filter
    /// for the "top projects" chart.
    pub fn project_totals(&self) -> Vec<(String, u64)> {
        self.project_requests
            .iter()
            .map(|(tag, counts)| (tag.clone(), counts.iter().sum()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        m.record_project("billing-api", route_group("/files/six/six.whl"));
        m.record_project("etl", route_group("/simple/"));
        m.record_download();
        m.record_download();
        let snap = m.snapshot();
        assert_eq!(snap.total_requests(), 4);
        assert_eq!(snap.files_served(), 2);
        assert_eq!(snap.downloads_total(), 2);
        let routes: std::collections::HashMap<_, _> = snap.route_totals().into_iter().collect();
        assert_eq!(routes["simple"], 2);
        assert_eq!(routes["files"], 2);
        let projects: std::collections::HashMap<_, _> = snap.project_totals().into_iter().collect();
        assert_eq!(projects["billing-api"], 1);
        assert_eq!(projects["etl"], 1);
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
