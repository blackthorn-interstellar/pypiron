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
const ROUTES: [&str; 6] = ["simple", "files", "legacy", "health", "metrics", "other"];
/// Status classes. Order matches the counter matrix.
const STATUS_CLASSES: [&str; 4] = ["2xx", "3xx", "4xx", "5xx"];

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

    pub fn record_request(&self, route: usize, status: u16) {
        let class = match status {
            200..=299 => 0,
            300..=399 => 1,
            400..=499 => 2,
            _ => 3,
        };
        self.requests[route][class].fetch_add(1, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let text = m.render();
        assert!(text.contains("pypiron_http_requests_total{route=\"simple\",status=\"2xx\"} 1"));
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
