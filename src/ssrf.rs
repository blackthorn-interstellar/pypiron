//! SSRF guard for the outbound fetches the proxy and `sync` make against
//! upstream-supplied URLs.
//!
//! The trust boundary: the operator-configured upstream host
//! (`--proxy-upstream` / sync `--src-base`) is trusted and may legitimately be a
//! private/internal address. Everything derived from *listing content* — a
//! file's `url`, its `.metadata` companion, the `provenance` URL, and any
//! redirect `Location` — is attacker-influenceable and must resolve to a public
//! address unless the operator explicitly allow-lists it.
//!
//! Two layers close the gap, because neither alone is sufficient:
//!   * A [`SsrfGuardResolver`] wired into the reqwest client filters DNS results,
//!     so a *hostname* target can never connect to a forbidden address (and a
//!     DNS-rebind across redirect hops is caught at each connect).
//!   * A pre-flight [`Guard::check_target`] validates *IP-literal* hosts before
//!     the request is issued. hyper-util skips DNS resolution for literal hosts,
//!     so the resolver never sees `http://169.254.169.254/…`; the pre-flight is
//!     the only thing that catches it. It also re-validates every redirect
//!     `Location` in the manual redirect loop below.

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use ipnet::IpNet;
use reqwest::{Client, Response, Url};
use url::Host;

/// A deterministic guard refusal: the target is forbidden and no retry will ever
/// change that. Typed (not a bare `anyhow!`) so the download retry loops can
/// downcast and fail fast instead of burning the 2s+4s backoff on a dead target.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct Blocked(String);

/// Private, loopback, link-local, or otherwise non-routable — never a valid
/// target for an upstream fetch derived from listing content.
pub(crate) fn is_forbidden_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.octets()[0] == 0
                // Reserved for future use (Class E, 240.0.0.0/4) — never a real host.
                || v4.octets()[0] >= 240
                // Carrier-grade NAT (100.64.0.0/10).
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 64)
        }
        IpAddr::V6(v6) => {
            // Fold any embedded IPv4 back to v4 and re-run the v4 rules: both the
            // `::ffff:a.b.c.d` mapped form and the deprecated `::a.b.c.d`
            // compatible form are reachable on a dual-stack host, so
            // `::169.254.169.254` must be judged as its v4. `to_ipv4()` covers
            // both (it also folds `::1`/`::`, already forbidden as v4).
            if let Some(v4) = v6.to_ipv4() {
                return is_forbidden_ip(&IpAddr::V4(v4));
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // Unique-local fc00::/7.
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // Link-local fe80::/10.
                || (v6.segments()[0] & 0xffc0) == 0xfe80
            // 6to4 (2002::/16), Teredo (2001::/32), and NAT64 (64:ff9b::/96)
            // embed IPv4 but aren't routable to an internal target; accepted as
            // low-risk rather than decoded here.
        }
    }
}

/// The allow-list an outbound fetch is validated against. Shared (via `Arc`) by
/// the [`SsrfGuardResolver`] on the client and the pre-flight [`Guard::check_target`],
/// so the name path and the literal path enforce identical rules.
#[derive(Debug, Default)]
pub(crate) struct Guard {
    /// The operator-configured upstream host, exempt by name — an internal or
    /// self-hosted mirror on a private range must still work.
    upstream_host: Option<String>,
    /// Additional operator-allow-listed hosts (exact host-string match), for a
    /// fully-internal deployment whose files live on a different private host.
    allow_hosts: HashSet<String>,
    /// Operator-allow-listed CIDRs: a target IP inside one is permitted even if
    /// it is otherwise forbidden.
    allow_cidrs: Vec<IpNet>,
}

impl Guard {
    /// Build a guard from the upstream URL (its host becomes exempt) plus the
    /// operator's allow-host / allow-cidr opt-ins. Both allow-lists default
    /// empty (fail-closed); an unparseable CIDR is a hard configuration error.
    pub(crate) fn new(
        upstream: &str,
        allow_hosts: &[String],
        allow_cidrs: &[String],
    ) -> Result<Self> {
        let upstream_host = Url::parse(upstream)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string));
        let allow_cidrs = allow_cidrs
            .iter()
            .map(|raw| {
                raw.parse::<IpNet>()
                    .with_context(|| format!("--proxy-allow-cidr '{raw}' is not a valid CIDR"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            upstream_host,
            allow_hosts: allow_hosts.iter().cloned().collect(),
            allow_cidrs,
        })
    }

    /// Is this host exempt by name (the upstream, or an operator allow-host)?
    fn host_exempt(&self, host: &str) -> bool {
        self.upstream_host.as_deref() == Some(host) || self.allow_hosts.contains(host)
    }

    /// Is this resolved address permitted — routable, or inside an allow-cidr?
    fn ip_allowed(&self, ip: &IpAddr) -> bool {
        !is_forbidden_ip(ip) || self.allow_cidrs.iter().any(|net| net.contains(ip))
    }

    /// Pre-flight check before issuing a request (or following a redirect) to
    /// `url`. IP-literal hosts are validated here — the reqwest resolver never
    /// sees them. Name hosts pass through untouched: they are validated at
    /// connect time by [`SsrfGuardResolver`], which also closes DNS-rebind.
    pub(crate) fn check_target(&self, url: &Url) -> Result<()> {
        let Some(host) = url.host() else {
            return Err(Blocked(format!("refusing to fetch '{url}': URL has no host")).into());
        };
        let ip = match host {
            // A registrable name; the DNS resolver enforces the address rules
            // (and DNS-rebind) at connect time, so the pre-flight passes it. The
            // upstream host is re-exempted here on every redirect hop, which
            // trusts operator-controlled upstream DNS — accepted in the trust
            // model (the upstream is operator-configured, hence trusted).
            Host::Domain(_) => return Ok(()),
            // An IP literal (the URL parser already folded decimal/octal/hex
            // IPv4 forms into this). Never the exempt upstream *name*, but may be
            // allow-listed by host string or fall inside an allow-cidr.
            Host::Ipv4(v4) => IpAddr::V4(v4),
            Host::Ipv6(v6) => IpAddr::V6(v6),
        };
        // An exempt IPv6 literal may be written bracketed (`[::1]`) or bare
        // depending on where it came from; accept either against the allow-list.
        let exempt = self.host_exempt(&ip.to_string()) || self.host_exempt(&format!("[{ip}]"));
        if exempt || self.ip_allowed(&ip) {
            return Ok(());
        }
        Err(Blocked(format!(
            "refusing to fetch '{url}': {ip} is a private/loopback/link-local address"
        ))
        .into())
    }
}

/// DNS resolver that refuses to hand back a forbidden address for a non-exempt
/// host. Filtering at resolve time (rather than validating a name up front)
/// closes the DNS-rebind gap: reqwest connects to exactly the addresses returned
/// here, on the initial request and on every redirect hop.
pub(crate) struct SsrfGuardResolver {
    guard: Arc<Guard>,
}

impl SsrfGuardResolver {
    pub(crate) fn new(guard: Arc<Guard>) -> Self {
        Self { guard }
    }
}

impl reqwest::dns::Resolve for SsrfGuardResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let guard = self.guard.clone();
        let host = name.as_str().to_string();
        Box::pin(async move {
            let exempt = guard.host_exempt(&host);
            let addrs = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let filtered: Vec<std::net::SocketAddr> = addrs
                .filter(|addr| exempt || guard.ip_allowed(&addr.ip()))
                .collect();
            if filtered.is_empty() {
                return Err(format!(
                    "refusing to connect to '{host}': resolves only to private/loopback addresses"
                )
                .into());
            }
            Ok(Box::new(filtered.into_iter())
                as Box<dyn Iterator<Item = std::net::SocketAddr> + Send>)
        })
    }
}

/// Redirect cap for the manual loop. Matches reqwest's own default of 10 — deep
/// enough for legitimate CDN chains, shallow enough to bound a redirect bomb.
const MAX_REDIRECTS: u32 = 10;

/// GET `url`, following up to [`MAX_REDIRECTS`] redirects, validating the host of
/// every hop (initial request and each `Location`) against `guard`. The client
/// this is called on MUST be built with [`SsrfGuardResolver`] and
/// `redirect(Policy::none())` — this function owns redirect-following so it can
/// re-validate each target; auto-follow would let a hop reach a forbidden
/// literal the resolver never sees. `timeout` bounds each hop.
pub(crate) async fn guarded_get(
    client: &Client,
    guard: &Guard,
    url: Url,
    timeout: Option<Duration>,
) -> Result<Response> {
    guarded_get_with(client, guard, url, timeout, |req| req).await
}

/// Like [`guarded_get`], but the caller decorates each per-hop request builder —
/// e.g. to attach a conditional `If-None-Match`. The decorator runs on every hop
/// so the header survives redirects, and the SSRF re-validation is identical.
pub(crate) async fn guarded_get_with<F>(
    client: &Client,
    guard: &Guard,
    url: Url,
    timeout: Option<Duration>,
    decorate: F,
) -> Result<Response>
where
    F: Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
{
    let mut current = url;
    for _ in 0..=MAX_REDIRECTS {
        // Re-validate every hop, including the upstream-host re-exemption (see
        // check_target): a redirect Location is as untrusted as the listing URL.
        guard.check_target(&current)?;
        let mut req = decorate(client.get(current.clone()));
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        let resp = req.send().await?;
        if !resp.status().is_redirection() {
            return Ok(resp);
        }
        let Some(location) = resp.headers().get(reqwest::header::LOCATION) else {
            // A 3xx with no Location isn't a redirect we can follow; hand it back
            // and let the caller's error_for_status turn it into a clean error.
            return Ok(resp);
        };
        let location = location
            .to_str()
            .map_err(|_| anyhow!("redirect Location is not valid text fetching '{current}'"))?;
        current = current
            .join(location)
            .with_context(|| format!("resolving redirect Location '{location}'"))?;
    }
    // A redirect chain this deep is a misbehaving/hostile upstream, not a blip:
    // deterministic, so mark it non-retryable.
    Err(Blocked(format!(
        "too many redirects (> {MAX_REDIRECTS}) fetching upstream URL"
    ))
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn is_forbidden_ip_blocks_private_and_allows_public() {
        for ip in [
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            // Multicast and Class-E (reserved) v4 are never real hosts.
            IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)),
            // IPv4-mapped IPv6 form of the metadata endpoint.
            "::ffff:169.254.169.254".parse().unwrap(),
            // IPv4-compatible (deprecated) IPv6 form — reachable on dual-stack.
            "::169.254.169.254".parse().unwrap(),
            "::127.0.0.1".parse().unwrap(),
        ] {
            assert!(is_forbidden_ip(&ip), "{ip} should be blocked");
        }
        for ip in [
            IpAddr::V4(Ipv4Addr::new(151, 101, 0, 223)), // files.pythonhosted.org
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            IpAddr::V6(Ipv6Addr::new(0x2606, 0x4700, 0, 0, 0, 0, 0, 1)),
        ] {
            assert!(!is_forbidden_ip(&ip), "{ip} should be allowed");
        }
    }

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn check_target_refuses_forbidden_ip_literals() {
        let guard = Guard::new("https://pypi.org", &[], &[]).unwrap();
        for target in [
            "http://127.0.0.1/x",
            "http://169.254.169.254/latest/meta-data/",
            "http://10.0.0.1/x",
            "http://[::1]/x",
            "http://[::ffff:169.254.169.254]/x",
            // IPv4-compatible IPv6 form of a loopback — must fold to v4.
            "http://[::127.0.0.1]/x",
            // Decimal-encoded 127.0.0.1 — the URL parser folds it to a literal.
            "http://2130706433/x",
        ] {
            let err = guard
                .check_target(&url(target))
                .expect_err(&format!("{target} must be refused"));
            assert!(err.to_string().contains("refusing to fetch"));
            // Deterministic refusals are typed so the retry loops fail fast.
            assert!(
                err.downcast_ref::<Blocked>().is_some(),
                "{target} refusal must be a Blocked error"
            );
        }
    }

    #[test]
    fn check_target_allows_public_and_names() {
        let guard = Guard::new("https://pypi.org", &[], &[]).unwrap();
        // Public literal.
        guard.check_target(&url("http://8.8.8.8/x")).unwrap();
        // A name is deferred to the resolver, so the pre-flight passes it —
        // even a name that will resolve privately (rebind is caught at connect).
        guard
            .check_target(&url("http://internal.example/x"))
            .unwrap();
        guard
            .check_target(&url("https://files.pythonhosted.org/p/x.whl"))
            .unwrap();
    }

    #[test]
    fn check_target_exempts_upstream_host() {
        // A private upstream host (self-hosted mirror) is exempt by name.
        let guard = Guard::new("http://mirror.internal", &[], &[]).unwrap();
        guard
            .check_target(&url("http://mirror.internal/simple/x/"))
            .unwrap();
        // A literal upstream is exempt by its host string too.
        let guard = Guard::new("http://10.1.2.3:8080", &[], &[]).unwrap();
        guard.check_target(&url("http://10.1.2.3:8080/x")).unwrap();
        // An IPv6-literal upstream is exempt despite bracket-form differences
        // between the URL host string and the bare address.
        let guard = Guard::new("http://[::1]:8080", &[], &[]).unwrap();
        guard.check_target(&url("http://[::1]:8080/x")).unwrap();
    }

    #[test]
    fn check_target_honors_allow_host_and_cidr() {
        let guard = Guard::new(
            "https://pypi.org",
            &["files.internal".to_string()],
            &["10.42.0.0/16".to_string()],
        )
        .unwrap();
        // Allow-listed private host by name.
        guard.check_target(&url("http://files.internal/x")).unwrap();
        // Allow-listed private literal inside the CIDR.
        guard.check_target(&url("http://10.42.7.7/x")).unwrap();
        // A private literal outside the CIDR is still refused.
        guard
            .check_target(&url("http://10.43.0.1/x"))
            .expect_err("outside the allow-cidr must be refused");
    }

    #[test]
    fn bad_cidr_is_a_hard_error() {
        let err = Guard::new("https://pypi.org", &[], &["not-a-cidr".to_string()]).unwrap_err();
        assert!(err.to_string().contains("valid CIDR"));
    }
}
