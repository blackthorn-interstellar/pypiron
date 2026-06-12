//! Process counters served at `/metrics` in Prometheus text format.
//!
//! Hand-rolled atomics, no metrics crate: the counter set is small and fixed,
//! and the exposition format is a dozen lines of text. Requests are bucketed
//! by route group and status class — low cardinality on purpose (per-package
//! labels would make the scrape payload scale with the registry).

use std::sync::atomic::{AtomicU64, Ordering};

/// Route groups, by path prefix. Order matches the counter matrix.
const ROUTES: [&str; 6] = ["simple", "files", "legacy", "health", "metrics", "other"];
/// Status classes. Order matches the counter matrix.
const STATUS_CLASSES: [&str; 4] = ["2xx", "3xx", "4xx", "5xx"];

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

pub struct Metrics {
    /// requests[route][status_class]
    requests: [[AtomicU64; STATUS_CLASSES.len()]; ROUTES.len()],
    /// Package index rebuilds (worker + reconcile + deletes).
    pub index_rebuilds: AtomicU64,
    /// Full reconcile sweeps completed.
    pub reconcile_sweeps: AtomicU64,
    /// Upstream package-listing fetches (proxy mode), by outcome.
    pub proxy_listing_fetches: AtomicU64,
    pub proxy_listing_errors: AtomicU64,
    /// Upstream artifacts downloaded and committed to storage (proxy mode).
    pub proxy_artifacts_cached: AtomicU64,
    pub proxy_artifact_errors: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            index_rebuilds: AtomicU64::new(0),
            reconcile_sweeps: AtomicU64::new(0),
            proxy_listing_fetches: AtomicU64::new(0),
            proxy_listing_errors: AtomicU64::new(0),
            proxy_artifacts_cached: AtomicU64::new(0),
            proxy_artifact_errors: AtomicU64::new(0),
        }
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
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
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
    }
}
