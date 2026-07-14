//! What region is this node in? A node learns its region the way it knows its
//! hostname: it asks its platform. Detection runs once at startup, never on a
//! request path, and any failure yields `None` — a node that learns nothing
//! behaves exactly as one with no region. The result is only a label used to
//! pick a near read bucket; it never moves a write.
//!
//! All decision logic is pure and takes injected I/O (an env lookup, a DMI file
//! reader, an async fetch), so every branch is unit-testable without a network
//! or real environment. The thin real wrappers below are exercised blackbox.

use std::time::Duration;

use tracing::info;

use crate::storage::{BucketScheme, BucketSpec};

/// The cloud a node runs on. A detected region only matches a bucket of the
/// agreeing scheme; an operator-declared region carries no provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Provider {
    Aws,
    Gcp,
    Azure,
}

/// How a node's region was learned. This changes matching: an operator's word
/// is trusted across schemes (on-prem/MinIO fleets opt in this way), a detected
/// region only matches its own cloud.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegionSource {
    /// Operator override (`--node-region`): a trusted region string, no provider.
    Explicit,
    /// Learned from platform environment or instance metadata.
    Detected,
}

/// A node's region: the normalized region string, the provider it was detected
/// on (absent for an operator override), and how it was learned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeRegion {
    pub(crate) provider: Option<Provider>,
    pub(crate) region: String,
    pub(crate) source: RegionSource,
}

const DMI_VENDOR_FILES: [&str; 3] = [
    "/sys/class/dmi/id/board_vendor",
    "/sys/class/dmi/id/sys_vendor",
    "/sys/class/dmi/id/product_name",
];
const HYPERVISOR_UUID_FILE: &str = "/sys/hypervisor/uuid";

const AWS_TOKEN_URL: &str = "http://169.254.169.254/latest/api/token";
const AWS_REGION_URL: &str = "http://169.254.169.254/latest/meta-data/placement/region";
const GCP_ZONE_URL: &str = "http://metadata.google.internal/computeMetadata/v1/instance/zone";
const AZURE_LOCATION_URL: &str =
    "http://169.254.169.254/metadata/instance/compute/location?api-version=2021-02-01&format=text";

/// One second, total, for every metadata probe — no retries. Detection must
/// never delay startup beyond this, and a hung link-local address is common on
/// non-cloud hosts that happen to pass the DMI gate.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// An HTTP request to a metadata endpoint, described so the decision logic can
/// be driven with a canned fetch in tests.
#[derive(Clone, Debug)]
struct HttpProbe {
    method: ProbeMethod,
    url: String,
    headers: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug)]
enum ProbeMethod {
    Get,
    Put,
}

impl HttpProbe {
    fn get(url: &str, headers: &[(&str, &str)]) -> Self {
        Self::new(ProbeMethod::Get, url, headers)
    }

    fn put(url: &str, headers: &[(&str, &str)]) -> Self {
        Self::new(ProbeMethod::Put, url, headers)
    }

    fn new(method: ProbeMethod, url: &str, headers: &[(&str, &str)]) -> Self {
        Self {
            method,
            url: url.to_string(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

/// Detect this node's region, in order: operator override, AWS environment,
/// then the DMI-gated metadata probe for exactly the indicated cloud. First hit
/// wins; anything unlearned is `None`.
async fn detect_region<EnvFn, DmiFn, FetchFn, Fut>(
    explicit: Option<&str>,
    env: EnvFn,
    dmi: DmiFn,
    fetch: FetchFn,
) -> Option<NodeRegion>
where
    EnvFn: Fn(&str) -> Option<String>,
    DmiFn: Fn(&str) -> Option<String>,
    FetchFn: Fn(HttpProbe) -> Fut,
    Fut: std::future::Future<Output = Option<String>>,
{
    // (a) Operator word: trusted, no provider.
    if let Some(raw) = explicit {
        let region = normalize(raw);
        if !region.is_empty() {
            return Some(NodeRegion {
                provider: None,
                region,
                source: RegionSource::Explicit,
            });
        }
    }
    // (b) AWS environment. AWS_REGION then AWS_DEFAULT_REGION.
    for key in ["AWS_REGION", "AWS_DEFAULT_REGION"] {
        if let Some(raw) = env(key) {
            let region = normalize(&raw);
            if !region.is_empty() {
                return Some(NodeRegion {
                    provider: Some(Provider::Aws),
                    region,
                    source: RegionSource::Detected,
                });
            }
        }
    }
    // (c) DMI vendor gate: a missing/unreadable DMI (macOS, bare metal) is not a
    // cloud, so no probe is attempted. (d) Probe only the indicated provider.
    let provider = provider_from_dmi(&dmi)?;
    let region = normalize(&probe_region(provider, &fetch).await?);
    if region.is_empty() {
        return None;
    }
    Some(NodeRegion {
        provider: Some(provider),
        region,
        source: RegionSource::Detected,
    })
}

/// The provider a node's DMI board/system identity indicates, if any. Bare
/// metal and non-Linux hosts have no DMI to read here and return `None`.
fn provider_from_dmi(dmi: impl Fn(&str) -> Option<String>) -> Option<Provider> {
    let vendor_says = |needle: &str| {
        DMI_VENDOR_FILES
            .iter()
            .filter_map(|path| dmi(path))
            .any(|value| value.to_ascii_lowercase().contains(needle))
    };
    if vendor_says("amazon ec2")
        || dmi(HYPERVISOR_UUID_FILE)
            .is_some_and(|uuid| uuid.trim_start().to_ascii_lowercase().starts_with("ec2"))
    {
        return Some(Provider::Aws);
    }
    if vendor_says("google") {
        return Some(Provider::Gcp);
    }
    if vendor_says("microsoft corporation") {
        return Some(Provider::Azure);
    }
    None
}

/// Read the region string from the indicated provider's metadata service. Any
/// transport failure (or a hung link-local address) surfaces as `None`.
async fn probe_region<FetchFn, Fut>(provider: Provider, fetch: &FetchFn) -> Option<String>
where
    FetchFn: Fn(HttpProbe) -> Fut,
    Fut: std::future::Future<Output = Option<String>>,
{
    match provider {
        Provider::Aws => {
            // IMDSv2: mint a token, then read the region with it. On token
            // failure, one plain IMDSv1 GET (older/relaxed instances).
            let token = fetch(HttpProbe::put(
                AWS_TOKEN_URL,
                &[("X-aws-ec2-metadata-token-ttl-seconds", "21600")],
            ))
            .await;
            let region_probe = match token.as_deref().map(str::trim) {
                Some(token) if !token.is_empty() => {
                    HttpProbe::get(AWS_REGION_URL, &[("X-aws-ec2-metadata-token", token)])
                }
                _ => HttpProbe::get(AWS_REGION_URL, &[]),
            };
            fetch(region_probe).await
        }
        Provider::Gcp => {
            let zone = fetch(HttpProbe::get(
                GCP_ZONE_URL,
                &[("Metadata-Flavor", "Google")],
            ))
            .await?;
            Some(gcp_region_from_zone(&zone))
        }
        Provider::Azure => fetch(HttpProbe::get(AZURE_LOCATION_URL, &[("Metadata", "true")])).await,
    }
}

/// GCP metadata returns a zone as `projects/<num>/zones/us-central1-a`; the
/// region is that last segment with the trailing `-<letter>` zone suffix
/// stripped.
fn gcp_region_from_zone(zone: &str) -> String {
    let last = zone.trim().rsplit('/').next().unwrap_or(zone).trim();
    match last.rsplit_once('-') {
        Some((region, suffix))
            if suffix.len() == 1 && suffix.chars().all(|c| c.is_ascii_alphabetic()) =>
        {
            region.to_string()
        }
        _ => last.to_string(),
    }
}

/// Trim and lowercase a region string before it is stored or compared. Regions
/// are ASCII, so an ASCII fold is exact.
fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

/// Does a node's region match this bucket's `@region` label? A detected region
/// requires provider↔scheme agreement and exact region equality; an operator
/// override needs only region equality, so on-prem fleets can label any
/// backend. Regions never prefix-match (`eastus` and `eastus2` are distinct).
pub(crate) fn matches(node: &NodeRegion, spec: &BucketSpec) -> bool {
    let Some(label) = spec.region.as_deref() else {
        return false;
    };
    if normalize(label) != node.region {
        return false;
    }
    match node.source {
        RegionSource::Explicit => true,
        RegionSource::Detected => matches!(
            (node.provider, spec.scheme),
            (Some(Provider::Aws), BucketScheme::S3)
                | (Some(Provider::Gcp), BucketScheme::Gcs)
                | (Some(Provider::Azure), BucketScheme::Azure)
        ),
    }
}

/// Detect this node's region at startup and log the outcome. The `explicit`
/// operator override arrives already merged from `--node-region`/its env var.
pub(crate) async fn detect(explicit: Option<&str>) -> Option<NodeRegion> {
    // A dedicated bounded client that does NOT go through the SSRF guard: the
    // guard blocks 169.254.169.254 by design, and it must keep doing so.
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(PROBE_TIMEOUT)
        .build()
        .ok();
    let fetch = |probe: HttpProbe| {
        let client = client.clone();
        async move {
            match client {
                Some(client) => http_fetch(&client, probe).await,
                None => None,
            }
        }
    };
    let result = detect_region(explicit, env_var, read_dmi, fetch).await;
    match &result {
        Some(node) => info!(
            provider = ?node.provider,
            region = %node.region,
            source = ?node.source,
            "node region detected"
        ),
        None => info!("node region not detected; serving region-agnostic"),
    }
    result
}

fn env_var(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn read_dmi(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

async fn http_fetch(client: &reqwest::Client, probe: HttpProbe) -> Option<String> {
    let mut req = match probe.method {
        ProbeMethod::Get => client.get(&probe.url),
        ProbeMethod::Put => client.put(&probe.url),
    };
    for (name, value) in &probe.headers {
        req = req.header(name.as_str(), value.as_str());
    }
    let resp = req.send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.text().await.ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::future::{ready, Ready};

    fn map_lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    /// A canned fetch keyed by URL (headers ignored), immediate result.
    fn fetch_urls(pairs: &[(&str, &str)]) -> impl Fn(HttpProbe) -> Ready<Option<String>> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |probe: HttpProbe| ready(map.get(&probe.url).cloned())
    }

    fn spec(scheme: BucketScheme, region: Option<&str>) -> BucketSpec {
        BucketSpec {
            scheme,
            name: "bucket".to_string(),
            region: region.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn explicit_override_wins_and_carries_no_provider() {
        // Set even though AWS env and EC2 DMI are present — the override wins,
        // is trimmed/lowercased, and records no provider.
        let node = detect_region(
            Some("  US-WEST-2 "),
            map_lookup(&[("AWS_REGION", "us-east-1")]),
            map_lookup(&[("/sys/class/dmi/id/sys_vendor", "Amazon EC2")]),
            fetch_urls(&[]),
        )
        .await
        .unwrap();
        assert_eq!(
            node,
            NodeRegion {
                provider: None,
                region: "us-west-2".to_string(),
                source: RegionSource::Explicit,
            }
        );
    }

    #[tokio::test]
    async fn aws_region_env_detects_aws_provider() {
        let node = detect_region(
            None,
            map_lookup(&[("AWS_REGION", "US-East-1")]),
            map_lookup(&[]),
            fetch_urls(&[]),
        )
        .await
        .unwrap();
        assert_eq!(node.provider, Some(Provider::Aws));
        assert_eq!(node.region, "us-east-1");
        assert_eq!(node.source, RegionSource::Detected);
    }

    #[tokio::test]
    async fn aws_default_region_env_is_the_fallback() {
        let node = detect_region(
            None,
            map_lookup(&[("AWS_DEFAULT_REGION", "eu-west-1")]),
            map_lookup(&[]),
            fetch_urls(&[]),
        )
        .await
        .unwrap();
        assert_eq!(node.provider, Some(Provider::Aws));
        assert_eq!(node.region, "eu-west-1");
    }

    #[tokio::test]
    async fn no_signals_or_blank_override_means_no_region() {
        assert_eq!(
            detect_region(None, map_lookup(&[]), map_lookup(&[]), fetch_urls(&[])).await,
            None
        );
        assert_eq!(
            detect_region(
                Some("   "),
                map_lookup(&[]),
                map_lookup(&[]),
                fetch_urls(&[])
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn aws_dmi_probes_imdsv2_then_reads_region() {
        let node = detect_region(
            None,
            map_lookup(&[]),
            map_lookup(&[("/sys/class/dmi/id/sys_vendor", "Amazon EC2")]),
            fetch_urls(&[
                (AWS_TOKEN_URL, "a-token"),
                (AWS_REGION_URL, "ap-southeast-2"),
            ]),
        )
        .await
        .unwrap();
        assert_eq!(node.provider, Some(Provider::Aws));
        assert_eq!(node.region, "ap-southeast-2");
        assert_eq!(node.source, RegionSource::Detected);
    }

    #[tokio::test]
    async fn aws_forwards_the_imdsv2_token_on_the_region_read() {
        let fetch = |probe: HttpProbe| {
            let body = match probe.url.as_str() {
                AWS_TOKEN_URL => Some("tok-123".to_string()),
                AWS_REGION_URL => probe
                    .headers
                    .iter()
                    .any(|(k, v)| k == "X-aws-ec2-metadata-token" && v == "tok-123")
                    .then(|| "sa-east-1".to_string()),
                _ => None,
            };
            ready(body)
        };
        let node = detect_region(
            None,
            map_lookup(&[]),
            map_lookup(&[("/sys/class/dmi/id/board_vendor", "Amazon EC2")]),
            fetch,
        )
        .await
        .unwrap();
        assert_eq!(node.region, "sa-east-1");
    }

    #[tokio::test]
    async fn aws_falls_back_to_imdsv1_when_token_fails() {
        // No token URL in the map (probe returns None) → plain IMDSv1 GET still
        // reads the region. Also exercises the hypervisor-uuid EC2 gate.
        let node = detect_region(
            None,
            map_lookup(&[]),
            map_lookup(&[("/sys/hypervisor/uuid", "ec2e1c9a-1234")]),
            fetch_urls(&[(AWS_REGION_URL, "us-east-2")]),
        )
        .await
        .unwrap();
        assert_eq!(node.provider, Some(Provider::Aws));
        assert_eq!(node.region, "us-east-2");
    }

    #[tokio::test]
    async fn gcp_dmi_reads_zone_and_strips_to_region() {
        let node = detect_region(
            None,
            map_lookup(&[]),
            map_lookup(&[("/sys/class/dmi/id/sys_vendor", "Google")]),
            fetch_urls(&[(GCP_ZONE_URL, "projects/123456/zones/us-central1-a")]),
        )
        .await
        .unwrap();
        assert_eq!(node.provider, Some(Provider::Gcp));
        assert_eq!(node.region, "us-central1");
    }

    #[tokio::test]
    async fn azure_dmi_reads_location() {
        let node = detect_region(
            None,
            map_lookup(&[]),
            map_lookup(&[("/sys/class/dmi/id/sys_vendor", "Microsoft Corporation")]),
            fetch_urls(&[(AZURE_LOCATION_URL, "eastus")]),
        )
        .await
        .unwrap();
        assert_eq!(node.provider, Some(Provider::Azure));
        assert_eq!(node.region, "eastus");
    }

    #[tokio::test]
    async fn cloud_without_reachable_metadata_yields_no_region() {
        assert_eq!(
            detect_region(
                None,
                map_lookup(&[]),
                map_lookup(&[("/sys/class/dmi/id/sys_vendor", "Amazon EC2")]),
                fetch_urls(&[]),
            )
            .await,
            None
        );
    }

    #[test]
    fn dmi_vendor_gate_maps_each_cloud() {
        assert_eq!(
            provider_from_dmi(map_lookup(&[(
                "/sys/class/dmi/id/sys_vendor",
                "Amazon EC2"
            )])),
            Some(Provider::Aws)
        );
        assert_eq!(
            provider_from_dmi(map_lookup(&[(
                "/sys/class/dmi/id/product_name",
                "Google Compute Engine"
            )])),
            Some(Provider::Gcp)
        );
        assert_eq!(
            provider_from_dmi(map_lookup(&[(
                "/sys/class/dmi/id/sys_vendor",
                "Microsoft Corporation"
            )])),
            Some(Provider::Azure)
        );
        assert_eq!(
            provider_from_dmi(map_lookup(&[("/sys/hypervisor/uuid", "ec2e1c9a-1234")])),
            Some(Provider::Aws)
        );
        // Missing DMI (macOS/bare metal) and an unrelated hypervisor: not cloud.
        assert_eq!(provider_from_dmi(map_lookup(&[])), None);
        assert_eq!(
            provider_from_dmi(map_lookup(&[("/sys/class/dmi/id/sys_vendor", "QEMU")])),
            None
        );
    }

    #[test]
    fn gcp_zone_becomes_region() {
        assert_eq!(
            gcp_region_from_zone("projects/123/zones/us-central1-a"),
            "us-central1"
        );
        assert_eq!(gcp_region_from_zone("europe-west4-b"), "europe-west4");
        // Already a region (no single-letter zone suffix): passed through.
        assert_eq!(gcp_region_from_zone("us-central1"), "us-central1");
    }

    #[test]
    fn normalize_trims_and_lowercases() {
        assert_eq!(normalize("  US-East-1 "), "us-east-1");
    }

    #[test]
    fn detected_region_requires_provider_scheme_agreement() {
        let aws_east = NodeRegion {
            provider: Some(Provider::Aws),
            region: "us-east-1".to_string(),
            source: RegionSource::Detected,
        };
        assert!(matches(
            &aws_east,
            &spec(BucketScheme::S3, Some("us-east-1"))
        ));
        // Same region, wrong cloud.
        assert!(!matches(
            &aws_east,
            &spec(BucketScheme::Gcs, Some("us-east-1"))
        ));
        // Different region never matches (no prefix match), but case folds.
        let azure_eastus = NodeRegion {
            provider: Some(Provider::Azure),
            region: "eastus".to_string(),
            source: RegionSource::Detected,
        };
        assert!(!matches(
            &azure_eastus,
            &spec(BucketScheme::Azure, Some("eastus2"))
        ));
        assert!(matches(
            &azure_eastus,
            &spec(BucketScheme::Azure, Some("EastUS"))
        ));
    }

    #[test]
    fn explicit_override_matches_any_scheme_and_never_an_unlabeled_bucket() {
        let node = NodeRegion {
            provider: None,
            region: "rack-7".to_string(),
            source: RegionSource::Explicit,
        };
        assert!(matches(&node, &spec(BucketScheme::S3, Some("rack-7"))));
        assert!(matches(&node, &spec(BucketScheme::Gcs, Some("rack-7"))));
        // A bucket with no `@region` label is never a region match.
        assert!(!matches(&node, &spec(BucketScheme::S3, None)));
    }
}
