//! Authentication and authorization: the credential-pairing and Basic-auth
//! decoders behind [`crate::app::AppState`]'s role checks, the admin request
//! guard, the project-attribution tag, and the failed-login throttle. Split
//! out of `app.rs`; the role predicates themselves stay methods on `AppState`.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::http::{header, HeaderMap, StatusCode};
use base64::engine::general_purpose::STANDARD as b64;
use base64::Engine;

use crate::app::AppState;
use crate::clock;
use crate::token;

/// Treat an empty string as an unset value. An empty environment variable
/// (e.g. `PYPIRON_ADMIN_PASS=`) parses as `Some("")`, not `None` — a common
/// container/helm footgun (an unset secret, `value: ""`, `$UNSET`).
pub(crate) fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

/// Pair a credential's two halves, treating an empty half as unconfigured so
/// the role disables (fail closed) instead of enabling a bypassable credential.
/// Because `ct_eq("", "")` is true, an empty password half would otherwise
/// authenticate any client that sends an empty password.
pub(crate) fn cred_pair<'a>(
    user: Option<&'a str>,
    pass: Option<&'a str>,
) -> Option<(&'a str, &'a str)> {
    nonempty(user).zip(nonempty(pass))
}

/// The conventional admin username supplied when only `--admin-pass` is given.
const DEFAULT_ADMIN_USER: &str = "admin";

/// Default the admin username to `admin` when a password was given without one —
/// the password is the secret; the username need not be repeated. The default
/// applies *only* alongside a password, so the no-admin (read-only)
/// configuration keeps both halves unset and never trips the half-configured
/// startup error. A password-less username is returned unchanged, so a stray
/// `--admin-user` still fails closed.
pub(crate) fn resolve_admin_user(user: Option<&str>, pass: Option<&str>) -> Option<String> {
    if nonempty(pass).is_some() && nonempty(user).is_none() {
        Some(DEFAULT_ADMIN_USER.to_string())
    } else {
        user.map(str::to_string)
    }
}

/// A half-configured credential pair — exactly one of username/password set
/// (an empty value counts as unset) — can never authenticate anyone, and a
/// half-configured *read* credential silently serves every index and artifact
/// publicly. Returns the error message to fail startup with, or None if the
/// pair is whole (both set) or absent (neither set).
pub(crate) fn credential_pair_error(
    label: &str,
    user: Option<&str>,
    pass: Option<&str>,
) -> Option<String> {
    match (nonempty(user).is_some(), nonempty(pass).is_some()) {
        (true, false) => Some(format!(
            "{label} username is set but its password is empty/unset"
        )),
        (false, true) => Some(format!(
            "{label} password is set but its username is empty/unset"
        )),
        _ => None,
    }
}

/// Gate the privileged routes (delete, yank, status, feed push, audit) behind the
/// admin credential, with RFC 7235/7231-correct status codes:
/// - no admin credential configured at all → 403 (the operation is disabled for
///   everyone, not an authentication challenge);
/// - credentials that validly authenticate as a lower role (reader or uploader)
///   but not admin → 403 (understood, insufficient — never re-challenge a
///   credential that already worked);
/// - no credentials, or credentials that authenticate as nobody → 401 (with the
///   `WWW-Authenticate` challenge added by middleware).
pub(crate) fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, String)> {
    if state.is_admin(headers) {
        return Ok(());
    }
    if state.admin_credential().is_none() {
        return Err((
            StatusCode::FORBIDDEN,
            "This operation is disabled (no admin credential configured)".into(),
        ));
    }
    if state.authenticates_below_admin(headers) {
        return Err((StatusCode::FORBIDDEN, "Admin credential required".into()));
    }
    Err((StatusCode::UNAUTHORIZED, "Admin credential required".into()))
}

pub(crate) fn check_basic_auth(headers: &HeaderMap, user: &str, pass: &str) -> Result<()> {
    let (u, p) = basic_credentials(headers).ok_or_else(|| anyhow!("missing basic auth"))?;
    // Gmail-style subaddressing: `ci+billing-api` authenticates as `ci`; the
    // suffix is a project attribution tag, not part of the identity.
    let base = u.split_once('+').map_or(u.as_str(), |(b, _)| b);
    // Username is not a secret; the password is — compare it in constant time.
    if (u == user || base == user) && token::ct_eq(&p, pass) {
        Ok(())
    } else {
        Err(anyhow!("bad credentials"))
    }
}

/// Decode the `Authorization: Basic` header into (username, password).
/// None when absent or malformed — callers decide whether that matters.
pub(crate) fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let auth = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let encoded = auth.strip_prefix("Basic ")?;
    let decoded = b64.decode(encoded).ok()?;
    let s = String::from_utf8(decoded).ok()?;
    let (u, p) = s.split_once(':').unwrap_or((s.as_str(), ""));
    Some((u.to_string(), p.to_string()))
}

/// Project attribution tag from the Basic-auth username: the part after `+`
/// (`ci+billing-api` → `billing-api`), or the whole username when untagged.
/// Deliberately works without any credential check — open servers still get
/// attribution from whatever username the client volunteers. The value is
/// client-supplied, so it is held to a label-safe charset and length; anything
/// else is dropped rather than escaped.
pub(crate) fn project_tag(headers: &HeaderMap) -> Option<String> {
    let (user, _) = basic_credentials(headers)?;
    let tag = user.split_once('+').map_or(user.as_str(), |(_, t)| t);
    let ok = !tag.is_empty()
        && tag.len() <= 64
        && tag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    ok.then(|| tag.to_string())
}

/// Default `--login-cooldown-secs`: five minutes, the fail2ban/Grafana-order
/// posture. Shared by the CLI default and `LoginThrottle::default`.
pub(crate) const DEFAULT_LOGIN_COOLDOWN_SECS: u64 = 300;

/// Failed logins before the cooldown engages. Not a knob: the tunable is the
/// cooldown itself; five is the sshd/fail2ban convention and far more retries
/// than any real client needs before its operator fixes the credential.
const LOGIN_FAIL_LIMIT: u32 = 5;

/// Addresses the throttle will track before shedding state. Only a guesser
/// spread across tens of thousands of addresses in one cooldown window can
/// reach it — a scale where per-address throttling is moot and the edge/WAF
/// is the real defense — so past the cap the map fails open (sheds) rather
/// than growing without bound or refusing addresses it has never seen.
const LOGIN_THROTTLE_CAP: usize = 32 * 1024;

/// One address's recent failed-login history. Millisecond epoch timestamps
/// come from [`clock::now_epoch_millis`] so the deterministic simulator can
/// drive expiry; the clock is wall time, not monotonic, so an NTP step skews a
/// cooldown by at most the step, in a known direction, and self-heals.
#[derive(Clone, Copy, Default)]
struct FailWindow {
    count: u32,
    last_ms: u64,
    blocked_until_ms: u64,
}

/// Per-address brute-force throttle on failed logins. [`LOGIN_FAIL_LIMIT`]
/// failures from one address, each within the cooldown of the last, and that
/// address's credential-bearing requests are refused (429) until the cooldown
/// passes — without evaluating the credential, so even a correct guess during
/// the cooldown confirms nothing. Successes are never counted and anonymous
/// requests never participate, so the throttle cannot lock out an address that
/// isn't already failing logins. State is per process: each replica enforces
/// its own budget, bounding a fleet's aggregate at replicas × the limit.
#[derive(Clone)]
pub struct LoginThrottle {
    cooldown_ms: u64,
    map: Arc<Mutex<HashMap<IpAddr, FailWindow>>>,
}

impl Default for LoginThrottle {
    fn default() -> Self {
        Self::new(Duration::from_secs(DEFAULT_LOGIN_COOLDOWN_SECS))
    }
}

impl LoginThrottle {
    /// A throttle enforcing `cooldown`; zero disables it entirely.
    pub(crate) fn new(cooldown: Duration) -> Self {
        Self {
            cooldown_ms: cooldown.as_millis().min(u64::MAX as u128) as u64,
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Seconds (rounded up, so never a `Retry-After: 0`) until `ip` may
    /// attempt to log in again, or None when it isn't blocked.
    pub(crate) fn blocked_secs(&self, ip: IpAddr) -> Option<u64> {
        self.blocked_secs_at(throttle_key(ip), clock::now_epoch_millis())
    }

    /// Count one failed login from `ip`.
    pub(crate) fn record_failure(&self, ip: IpAddr) {
        self.record_failure_at(throttle_key(ip), clock::now_epoch_millis());
    }

    fn blocked_secs_at(&self, key: IpAddr, now_ms: u64) -> Option<u64> {
        if self.cooldown_ms == 0 {
            return None;
        }
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let w = map.get(&key)?;
        if w.blocked_until_ms > now_ms {
            return Some((w.blocked_until_ms - now_ms).div_ceil(1000));
        }
        if now_ms.saturating_sub(w.last_ms) >= self.cooldown_ms {
            // Fully decayed and unblocked: drop it, so a quiet server's map
            // returns to empty instead of holding every address that ever
            // fumbled a password.
            map.remove(&key);
        }
        None
    }

    fn record_failure_at(&self, key: IpAddr, now_ms: u64) {
        if self.cooldown_ms == 0 {
            return;
        }
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= LOGIN_THROTTLE_CAP && !map.contains_key(&key) {
            let cooldown = self.cooldown_ms;
            map.retain(|_, w| {
                w.blocked_until_ms > now_ms || now_ms.saturating_sub(w.last_ms) < cooldown
            });
            if map.len() >= LOGIN_THROTTLE_CAP {
                map.clear();
            }
        }
        let w = map.entry(key).or_default();
        if now_ms.saturating_sub(w.last_ms) >= self.cooldown_ms {
            // Failures further apart than the cooldown never accumulate.
            w.count = 0;
        }
        w.count += 1;
        w.last_ms = now_ms;
        if w.count >= LOGIN_FAIL_LIMIT {
            w.blocked_until_ms = now_ms.saturating_add(self.cooldown_ms);
            w.count = 0;
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// The address failures are counted against. IPv4 stands alone; IPv6 collapses
/// to its /64, because one host commonly owns a whole /64 (SLAAC) and counting
/// exact v6 addresses would hand a guesser 2^64 fresh identities. A v4-mapped
/// v6 address keys as the v4 address it carries — collapsing those to their
/// shared /64 would put the entire v4 internet behind one dual-stack listener
/// into a single bucket.
fn throttle_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return IpAddr::V4(v4);
            }
            let mut octets = v6.octets();
            octets[8..].fill(0);
            IpAddr::V6(octets.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn basic_headers(user: &str, pass: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        let v = format!("Basic {}", b64.encode(format!("{user}:{pass}")));
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(&v).unwrap());
        h
    }

    #[test]
    fn empty_credential_half_is_unconfigured() {
        // An empty password env var (`PYPIRON_ADMIN_PASS=`) must not enable a
        // credential: ct_eq("", "") is true, so it would accept any client.
        assert_eq!(
            cred_pair(Some("admin"), Some("secret")),
            Some(("admin", "secret"))
        );
        assert_eq!(cred_pair(Some("admin"), Some("")), None);
        assert_eq!(cred_pair(Some("admin"), None), None);
        assert_eq!(cred_pair(Some(""), Some("secret")), None);
        assert_eq!(cred_pair(None, Some("secret")), None);
        assert_eq!(cred_pair(None, None), None);
    }

    #[test]
    fn half_configured_credential_is_rejected() {
        // Exactly one half set (empty counts as unset) is a fatal misconfig.
        assert!(credential_pair_error("read", Some("reader"), None).is_some());
        assert!(credential_pair_error("read", None, Some("secret")).is_some());
        assert!(credential_pair_error("read", Some("reader"), Some("")).is_some());
        assert!(credential_pair_error("read", Some(""), Some("secret")).is_some());
        // Both halves set, or neither: accepted.
        assert!(credential_pair_error("read", Some("reader"), Some("secret")).is_none());
        assert!(credential_pair_error("read", None, None).is_none());
        assert!(credential_pair_error("read", Some(""), Some("")).is_none());
    }

    #[test]
    fn admin_username_defaults_only_with_a_password() {
        // Password given, username omitted (or empty) -> conventional default.
        assert_eq!(
            resolve_admin_user(None, Some("secret")).as_deref(),
            Some("admin")
        );
        assert_eq!(
            resolve_admin_user(Some(""), Some("secret")).as_deref(),
            Some("admin")
        );
        // An explicit username is preserved.
        assert_eq!(
            resolve_admin_user(Some("root"), Some("secret")).as_deref(),
            Some("root")
        );
        // No password -> no admin: the username is NOT defaulted, so the
        // read-only configuration keeps both halves unset and the half-configured
        // check stays quiet.
        assert_eq!(resolve_admin_user(None, None), None);
        assert_eq!(resolve_admin_user(None, Some("")), None);
        // A password-less username is left untouched so it still fails closed via
        // the half-configured check.
        assert_eq!(
            resolve_admin_user(Some("root"), None).as_deref(),
            Some("root")
        );
    }

    #[test]
    fn basic_auth_exact_match() {
        assert!(check_basic_auth(&basic_headers("ci", "tok"), "ci", "tok").is_ok());
        assert!(check_basic_auth(&basic_headers("ci", "nope"), "ci", "tok").is_err());
        assert!(check_basic_auth(&basic_headers("other", "tok"), "ci", "tok").is_err());
        assert!(check_basic_auth(&HeaderMap::new(), "ci", "tok").is_err());
    }

    #[test]
    fn basic_auth_accepts_subaddressed_username() {
        assert!(check_basic_auth(&basic_headers("ci+billing-api", "tok"), "ci", "tok").is_ok());
        // The password still has to be right.
        assert!(check_basic_auth(&basic_headers("ci+billing-api", "nope"), "ci", "tok").is_err());
        // The base has to match exactly — no prefix matching.
        assert!(check_basic_auth(&basic_headers("cif+billing-api", "tok"), "ci", "tok").is_err());
        // A configured username containing '+' still matches itself exactly.
        assert!(check_basic_auth(&basic_headers("ci+team", "tok"), "ci+team", "tok").is_ok());
    }

    /// The pure `_at` transitions, driven by hand-rolled clocks — the global
    /// sim-clock override is process-wide and off limits to parallel tests.
    #[test]
    fn login_throttle_blocks_after_limit_and_expires() {
        let t = LoginThrottle::new(Duration::from_secs(300));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let start = 1_000_000;
        for i in 0..4 {
            t.record_failure_at(ip, start + i * 1_000);
            assert_eq!(t.blocked_secs_at(ip, start + i * 1_000), None);
        }
        // Fifth failure within the window: blocked for the full cooldown,
        // reported rounded up (never Retry-After: 0).
        t.record_failure_at(ip, start + 4_000);
        assert_eq!(t.blocked_secs_at(ip, start + 4_000), Some(300));
        assert_eq!(t.blocked_secs_at(ip, start + 4_000 + 299_500), Some(1));
        // Cooldown over: unblocked, and the stale entry is swept on lookup.
        assert_eq!(t.blocked_secs_at(ip, start + 4_000 + 300_000), None);
        assert_eq!(t.len(), 0);
        // Other addresses were never affected.
        assert_eq!(
            t.blocked_secs_at("203.0.113.10".parse().unwrap(), start),
            None
        );
    }

    #[test]
    fn login_failures_slower_than_the_cooldown_never_block() {
        let t = LoginThrottle::new(Duration::from_secs(300));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        for i in 0..20u64 {
            let now = 1_000_000 + i * 300_000;
            t.record_failure_at(ip, now);
            assert_eq!(t.blocked_secs_at(ip, now), None, "attempt {i}");
        }
    }

    #[test]
    fn login_throttle_zero_cooldown_disables() {
        let t = LoginThrottle::new(Duration::ZERO);
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        for _ in 0..20 {
            t.record_failure_at(ip, 1_000_000);
        }
        assert_eq!(t.blocked_secs_at(ip, 1_000_000), None);
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn login_throttle_key_masks_ipv6_to_slash_64() {
        let a: IpAddr = "2001:db8:1:2:aaaa::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:bbbb::2".parse().unwrap();
        let c: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_eq!(throttle_key(a), throttle_key(b));
        assert_ne!(throttle_key(a), throttle_key(c));
        // IPv4 stands alone; a v4-mapped v6 keys as its v4, NOT as the shared
        // ::ffff:0:0/64 (which would bucket the whole v4 internet together).
        let v4: IpAddr = "192.0.2.7".parse().unwrap();
        assert_eq!(throttle_key(v4), v4);
        let mapped: IpAddr = "::ffff:192.0.2.7".parse().unwrap();
        assert_eq!(throttle_key(mapped), v4);
        let other_mapped: IpAddr = "::ffff:192.0.2.8".parse().unwrap();
        assert_ne!(throttle_key(mapped), throttle_key(other_mapped));
    }

    #[test]
    fn login_throttle_sheds_state_at_the_cap() {
        let t = LoginThrottle::new(Duration::from_secs(300));
        let now = 1_000_000;
        // Distinct live addresses up to the cap, then one more: the sweep finds
        // nothing expired, so the map sheds entirely rather than growing.
        for i in 0..LOGIN_THROTTLE_CAP as u32 {
            t.record_failure_at(IpAddr::V4(std::net::Ipv4Addr::from(i)), now);
        }
        assert_eq!(t.len(), LOGIN_THROTTLE_CAP);
        t.record_failure_at("203.0.113.9".parse().unwrap(), now);
        assert_eq!(t.len(), 1);
        // With the old entries expired instead, the sweep alone makes room:
        // the cap-filling addresses decay and the newcomer is the survivor.
        let t = LoginThrottle::new(Duration::from_secs(300));
        for i in 0..LOGIN_THROTTLE_CAP as u32 {
            t.record_failure_at(IpAddr::V4(std::net::Ipv4Addr::from(i)), now);
        }
        let later = now + 400_000;
        t.record_failure_at("198.51.100.1".parse().unwrap(), later);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn project_tag_extraction() {
        assert_eq!(
            project_tag(&basic_headers("ci+billing-api", "tok")).as_deref(),
            Some("billing-api")
        );
        // Untagged username: the username itself is the attribution.
        assert_eq!(
            project_tag(&basic_headers("etl", "tok")).as_deref(),
            Some("etl")
        );
        // No credentials, empty tags, oversized or label-unsafe tags: dropped.
        assert_eq!(project_tag(&HeaderMap::new()), None);
        assert_eq!(project_tag(&basic_headers("ci+", "tok")), None);
        assert_eq!(project_tag(&basic_headers("ci+bad\"label", "tok")), None);
        assert_eq!(
            project_tag(&basic_headers(&format!("ci+{}", "x".repeat(65)), "tok")),
            None
        );
    }
}
