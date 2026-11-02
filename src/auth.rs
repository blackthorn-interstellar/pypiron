//! Authentication and authorization: the credential-pairing and Basic-auth
//! decoders behind [`crate::app::AppState`]'s role checks, the admin request
//! guard, the project-attribution tag, and the failed-login throttle. Split
//! out of `app.rs`; the role predicates themselves stay methods on `AppState`.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
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
    if state.authenticates(headers) {
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

/// Networks the throttle will track. Past half of it, an address it is not
/// already tracking is counted against its whole /24 (IPv6: /48), and past
/// three quarters against its /16 (/32): one allocation's worth of addresses
/// then shares a single budget instead of claiming a slot each, so filling the
/// map takes thousands of distinct networks, not one cheap IPv6 /48. A
/// legitimate client only shares a budget with a guesser on its own network.
/// Only at the cap itself, which takes a guesser spread across that many
/// networks (a scale where the edge/WAF is the real defense), are
/// credential-bearing requests from untracked networks refused (fail closed)
/// rather than the map growing without bound or forgetting who is locked out.
const LOGIN_THROTTLE_CAP: usize = 32 * 1024;

/// Minimum spacing of the map sweeps a crowded throttle runs on admission, so
/// a flood of untracked addresses can't make every request scan the whole map
/// under the lock.
const LOGIN_SWEEP_INTERVAL_MS: u64 = 1_000;

/// An address as the throttle counts it: a network and its prefix length.
type ThrottleKey = (IpAddr, u8);

/// The throttle's state, behind one lock.
#[derive(Default)]
struct ThrottleTable {
    map: HashMap<ThrottleKey, FailWindow>,
    swept_ms: u64,
}

/// One wrong credential in flight from an address. Requests repeating the same
/// credential share the slot (`refs`): sending one guess twice at once is still
/// one guess, so a stale credential reused across parallel requests costs one
/// try. `refs == 0` marks the slot free.
#[derive(Clone, Copy, Default)]
struct Pending {
    fp: u64,
    refs: u32,
    failed: bool,
}

/// One address's recent failed-login history plus the wrong credentials it has
/// in flight. Millisecond epoch timestamps come from [`clock::now_epoch_millis`]
/// so the deterministic simulator can drive expiry; the clock is wall time, not
/// monotonic, so an NTP step skews a cooldown by at most the step, in a known
/// direction, and self-heals.
#[derive(Clone, Copy, Default)]
struct FailWindow {
    count: u32,
    last_ms: u64,
    blocked_until_ms: u64,
    pending: [Pending; LOGIN_FAIL_LIMIT as usize],
}

impl FailWindow {
    fn in_flight(&self) -> u32 {
        self.pending.iter().filter(|p| p.refs > 0).count() as u32
    }

    /// Holds a lockout or an unfinished attempt: state the throttle must keep.
    fn live(&self, now_ms: u64) -> bool {
        self.blocked_until_ms > now_ms || self.in_flight() > 0
    }
}

/// Per-address brute-force throttle on failed logins. [`LOGIN_FAIL_LIMIT`]
/// failures from one address, each within the cooldown of the last, and that
/// address's credential-bearing requests are refused (429) until the cooldown
/// passes — without evaluating the credential, so even a correct guess during
/// the cooldown confirms nothing.
///
/// The budget is reserved before a request runs, not tallied after its 401:
/// every wrong credential takes a slot on admission, and the failures counted
/// plus the slots in flight never exceed the limit, so guesses sent at the same
/// time share the five. Whether a request is admitted never depends on whether
/// its credential is right — only a wrong one takes a slot, but a full budget
/// refuses right and wrong alike, or a 429-versus-200 would itself answer the
/// guess. Successes are never counted and anonymous requests never participate.
/// State is per process: each replica enforces its own budget, bounding a
/// fleet's aggregate at replicas × the limit.
#[derive(Clone)]
pub struct LoginThrottle {
    cooldown_ms: u64,
    table: Arc<Mutex<ThrottleTable>>,
    /// Per-process key for credential fingerprints, so a guesser can't pick
    /// two credentials that share a slot.
    fp_key: RandomState,
}

impl Default for LoginThrottle {
    fn default() -> Self {
        Self::new(Duration::from_secs(DEFAULT_LOGIN_COOLDOWN_SECS))
    }
}

/// A credential-bearing request admitted by [`LoginThrottle::admit`]. A wrong
/// credential holds its slot until [`LoginAttempt::finish`] reports the outcome
/// or the request is dropped mid-flight (client gone: no answer, no failure).
pub(crate) struct LoginAttempt {
    throttle: LoginThrottle,
    key: ThrottleKey,
    /// The wrong credential's fingerprint; None for one that authenticates.
    guess: Option<u64>,
    failed: bool,
}

impl LoginAttempt {
    /// Settle the attempt: `failed` when the response was a 401.
    pub(crate) fn finish(mut self, failed: bool) {
        self.failed = failed;
    }
}

impl Drop for LoginAttempt {
    fn drop(&mut self) {
        if let Some(fp) = self.guess {
            self.throttle
                .release_at(self.key, fp, self.failed, clock::now_epoch_millis());
        }
    }
}

impl LoginThrottle {
    /// A throttle enforcing `cooldown`; zero disables it entirely.
    pub(crate) fn new(cooldown: Duration) -> Self {
        Self {
            cooldown_ms: cooldown.as_millis().min(u64::MAX as u128) as u64,
            table: Arc::default(),
            fp_key: RandomState::new(),
        }
    }

    /// The identity a credential is checked as: the Basic username without its
    /// `+tag` attribution suffix, plus the password — so `ci+a:pw` and
    /// `ci+b:pw` are one guess. Any other scheme hashes the raw header.
    pub(crate) fn fingerprint(&self, headers: &HeaderMap) -> u64 {
        match basic_credentials(headers) {
            Some((user, pass)) => {
                let base = user.split_once('+').map_or(user.as_str(), |(b, _)| b);
                self.fp_key.hash_one((base, pass.as_str()))
            }
            None => self
                .fp_key
                .hash_one(headers.get(header::AUTHORIZATION).map(|v| v.as_bytes())),
        }
    }

    /// Admit a credential-bearing request from `ip`, or refuse it with the
    /// `Retry-After` seconds (rounded up, never 0). `guess` is the fingerprint
    /// of a credential that authenticates as nobody; None for a valid one.
    pub(crate) fn admit(&self, ip: IpAddr, guess: Option<u64>) -> Result<LoginAttempt, u64> {
        let key = self.admit_at(ip, guess, clock::now_epoch_millis())?;
        Ok(LoginAttempt {
            throttle: self.clone(),
            key,
            guess,
            failed: false,
        })
    }

    /// One atomic step: refuse a blocked or fully-reserved address, else
    /// reserve a slot for a wrong credential. Refusal depends only on the
    /// address's state and the credential bytes, never on validity. Returns
    /// the key the address is counted under.
    fn admit_at(&self, ip: IpAddr, guess: Option<u64>, now_ms: u64) -> Result<ThrottleKey, u64> {
        let keys = throttle_keys(ip);
        if self.cooldown_ms == 0 {
            return Ok(keys[0]);
        }
        let mut guard = self.table.lock().unwrap_or_else(|e| e.into_inner());
        let table = &mut *guard;
        let map = &mut table.map;
        // The most specific network already tracked counts the address.
        let Some(key) = keys.into_iter().find(|k| map.contains_key(k)) else {
            if map.len() >= LOGIN_THROTTLE_CAP / 2
                && now_ms.saturating_sub(table.swept_ms) >= LOGIN_SWEEP_INTERVAL_MS
            {
                // Expired history is dropped for free; partial counts only
                // once the map is full, and lockouts and attempts in flight
                // never.
                table.swept_ms = now_ms;
                let full = map.len() >= LOGIN_THROTTLE_CAP;
                let cooldown = self.cooldown_ms;
                map.retain(|_, w| {
                    w.live(now_ms) || (!full && now_ms.saturating_sub(w.last_ms) < cooldown)
                });
            }
            let key = match map.len() * 4 / LOGIN_THROTTLE_CAP {
                0 | 1 => keys[0],
                2 => keys[1],
                3 => keys[2],
                _ => return Err(self.cooldown_ms.div_ceil(1000)),
            };
            if let Some(fp) = guess {
                let mut w = FailWindow::default();
                w.pending[0] = Pending {
                    fp,
                    refs: 1,
                    failed: false,
                };
                map.insert(key, w);
            }
            return Ok(key);
        };
        let Some(w) = map.get_mut(&key) else {
            return Ok(key);
        };
        if w.blocked_until_ms > now_ms {
            return Err((w.blocked_until_ms - now_ms).div_ceil(1000));
        }
        let decayed = now_ms.saturating_sub(w.last_ms) >= self.cooldown_ms;
        if decayed {
            // Failures further apart than the cooldown never accumulate.
            w.count = 0;
        }
        if let Some(p) =
            guess.and_then(|fp| w.pending.iter_mut().find(|p| p.refs > 0 && p.fp == fp))
        {
            p.refs += 1;
            return Ok(key);
        }
        if w.count + w.in_flight() >= LOGIN_FAIL_LIMIT {
            return Err(1);
        }
        match guess {
            Some(fp) => {
                if let Some(p) = w.pending.iter_mut().find(|p| p.refs == 0) {
                    *p = Pending {
                        fp,
                        refs: 1,
                        failed: false,
                    };
                }
            }
            // Fully decayed and idle: drop it, so a quiet server's map returns
            // to empty instead of holding every address that ever fumbled.
            None if decayed && !w.live(now_ms) => {
                map.remove(&key);
            }
            None => {}
        }
        Ok(key)
    }

    /// Release one reference to `fp`'s slot. When the last one lets go and any
    /// of them failed, that is one failed login.
    fn release_at(&self, key: ThrottleKey, fp: u64, failed: bool, now_ms: u64) {
        let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        let Some(w) = table.map.get_mut(&key) else {
            return;
        };
        let Some(p) = w.pending.iter_mut().find(|p| p.refs > 0 && p.fp == fp) else {
            return;
        };
        p.refs -= 1;
        p.failed |= failed;
        if p.refs > 0 || !p.failed {
            return;
        }
        if now_ms.saturating_sub(w.last_ms) >= self.cooldown_ms {
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
        self.table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map
            .len()
    }
}

/// The networks failures are counted against, most specific first; a crowded
/// map counts a new address by a coarser one (see [`LOGIN_THROTTLE_CAP`]).
/// IPv4 stands alone, then /24, then /16. IPv6 starts at its /64, because one
/// host commonly owns a whole /64 (SLAAC) and counting exact v6 addresses would
/// hand a guesser 2^64 fresh identities; then /48 (a site), then /32 (an
/// ISP). A v4-mapped v6 address keys as the v4 address it carries — collapsing
/// those to their shared /64 would put the entire v4 internet behind one
/// dual-stack listener into a single bucket.
fn throttle_keys(ip: IpAddr) -> [ThrottleKey; 3] {
    match ip {
        IpAddr::V4(v4) => [32u8, 24, 16].map(|bits| {
            (
                IpAddr::V4((u32::from(v4) & (u32::MAX << (32 - bits))).into()),
                bits,
            )
        }),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => throttle_keys(IpAddr::V4(v4)),
            None => [64u8, 48, 32].map(|bits| {
                let net = u128::from(v6) & (u128::MAX << (128 - bits));
                (IpAddr::V6(net.into()), bits)
            }),
        },
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
    /// One failed login: a wrong credential admitted, then answered 401.
    fn fail(t: &LoginThrottle, ip: IpAddr, fp: u64, now: u64) {
        let key = t.admit_at(ip, Some(fp), now).unwrap();
        t.release_at(key, fp, true, now);
    }

    /// What a valid credential from `ip` gets: Ok, or the Retry-After secs.
    fn login(t: &LoginThrottle, ip: IpAddr, now: u64) -> Result<(), u64> {
        guess(t, ip, None, now)
    }

    /// Admission alone, without the key it was counted under.
    fn guess(t: &LoginThrottle, ip: IpAddr, fp: Option<u64>, now: u64) -> Result<(), u64> {
        t.admit_at(ip, fp, now).map(|_| ())
    }

    /// The address's own key: a lone v4 address, or a v6 /64.
    fn key(ip: IpAddr) -> ThrottleKey {
        throttle_keys(ip)[0]
    }

    fn window(t: &LoginThrottle, ip: IpAddr) -> FailWindow {
        t.table.lock().unwrap().map[&key(ip)]
    }

    #[test]
    fn login_throttle_blocks_after_limit_and_expires() {
        let t = LoginThrottle::new(Duration::from_secs(300));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let start = 1_000_000;
        for i in 0..4 {
            fail(&t, ip, i, start + i * 1_000);
            assert_eq!(login(&t, ip, start + i * 1_000), Ok(()));
        }
        // Fifth failure within the window: blocked for the full cooldown,
        // reported rounded up (never Retry-After: 0) — the right credential
        // and a wrong one alike.
        fail(&t, ip, 4, start + 4_000);
        assert_eq!(login(&t, ip, start + 4_000), Err(300));
        assert_eq!(guess(&t, ip, Some(9), start + 4_000), Err(300));
        assert_eq!(login(&t, ip, start + 4_000 + 299_500), Err(1));
        // Cooldown over: unblocked, and the stale entry is swept on lookup.
        assert_eq!(login(&t, ip, start + 4_000 + 300_000), Ok(()));
        assert_eq!(t.len(), 0);
        // Other addresses were never affected.
        assert_eq!(login(&t, "203.0.113.10".parse().unwrap(), start), Ok(()));
    }

    #[test]
    fn login_failures_slower_than_the_cooldown_never_block() {
        let t = LoginThrottle::new(Duration::from_secs(300));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        for i in 0..20u64 {
            let now = 1_000_000 + i * 300_000;
            fail(&t, ip, i, now);
            assert_eq!(login(&t, ip, now), Ok(()), "attempt {i}");
        }
    }

    #[test]
    fn login_throttle_zero_cooldown_disables() {
        let t = LoginThrottle::new(Duration::ZERO);
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        for i in 0..20 {
            fail(&t, ip, i, 1_000_000);
        }
        assert_eq!(login(&t, ip, 1_000_000), Ok(()));
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn login_throttle_key_masks_ipv6_to_slash_64() {
        let a: IpAddr = "2001:db8:1:2:aaaa::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:bbbb::2".parse().unwrap();
        let c: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_eq!(key(a), key(b));
        assert_ne!(key(a), key(c));
        // IPv4 stands alone; a v4-mapped v6 keys as its v4, NOT as the shared
        // ::ffff:0:0/64 (which would bucket the whole v4 internet together).
        let v4: IpAddr = "192.0.2.7".parse().unwrap();
        assert_eq!(key(v4), (v4, 32));
        let mapped: IpAddr = "::ffff:192.0.2.7".parse().unwrap();
        assert_eq!(key(mapped), (v4, 32));
        let other_mapped: IpAddr = "::ffff:192.0.2.8".parse().unwrap();
        assert_ne!(key(mapped), key(other_mapped));
        // The coarser networks a crowded map falls back to.
        let net = |s: &str, bits| (s.parse::<IpAddr>().unwrap(), bits);
        assert_eq!(
            throttle_keys(a),
            [
                net("2001:db8:1:2::", 64),
                net("2001:db8:1::", 48),
                net("2001:db8::", 32)
            ]
        );
        assert_eq!(
            throttle_keys(mapped),
            [
                net("192.0.2.7", 32),
                net("192.0.2.0", 24),
                net("192.0.0.0", 16)
            ]
        );
        // A network is keyed with its length: 2001:db8:1::/64 is not the /48.
        assert_ne!(throttle_keys(c)[0].0, throttle_keys(c)[1].0);
        let zero: IpAddr = "2001:db8:1::1".parse().unwrap();
        assert_ne!(throttle_keys(zero)[0], throttle_keys(zero)[1]);
    }

    /// One address per /16, so every tier keys each one apart.
    fn spread(i: u32) -> IpAddr {
        IpAddr::V4(std::net::Ipv4Addr::from(i << 16))
    }

    #[test]
    fn one_network_cannot_fill_the_throttle() {
        // Every /64 of one IPv6 /48 holding an attempt in flight — 65,536 of
        // them, twice the cap — used to take a slot each, fill the map with
        // live entries and refuse every credentialed client server-wide. Past
        // half the cap the rest are counted as their /48, which shares one
        // budget and fills nothing.
        let t = LoginThrottle::new(Duration::from_secs(300));
        let now = 1_000_000;
        for i in 0..=u16::MAX {
            let ip = IpAddr::V6(std::net::Ipv6Addr::new(0x2001, 0xdb8, 1, i, 0, 0, 0, 1));
            let _ = guess(&t, ip, Some(u64::from(i)), now);
        }
        assert_eq!(t.len(), LOGIN_THROTTLE_CAP / 2 + 1);
        // Everyone else still logs in, and still guesses on their own budget.
        let elsewhere: IpAddr = "198.51.100.1".parse().unwrap();
        assert_eq!(login(&t, elsewhere, now), Ok(()));
        assert_eq!(guess(&t, elsewhere, Some(1), now), Ok(()));
        // The flood's /48 shares one spent budget.
        let late: IpAddr = "2001:db8:1:ffff::9".parse().unwrap();
        assert_eq!(guess(&t, late, Some(1 << 20), now), Err(1));
    }

    #[test]
    fn login_throttle_cap_overflow_keeps_blocks() {
        // A lockout must survive a flood of fresh networks: filling the map
        // with live partial counts once wiped every block (`map.clear()`).
        let t = LoginThrottle::new(Duration::from_secs(300));
        let now = 1_000_000;
        let blocked: IpAddr = "203.0.113.9".parse().unwrap();
        for i in 0..5 {
            fail(&t, blocked, i, now);
        }
        for i in 1..LOGIN_THROTTLE_CAP as u32 {
            fail(&t, spread(i), 0, now);
        }
        assert_eq!(t.len(), LOGIN_THROTTLE_CAP);
        // Within the sweep interval a full map refuses rather than rescanning.
        let newcomer: IpAddr = "198.51.100.1".parse().unwrap();
        assert_eq!(guess(&t, newcomer, Some(7), now), Err(300));
        // The next sweep admits it by shedding partial counts, not the block.
        let later = now + LOGIN_SWEEP_INTERVAL_MS;
        assert_eq!(guess(&t, newcomer, Some(7), later), Ok(()));
        assert_eq!(login(&t, blocked, later), Err(299));
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn login_throttle_full_of_live_entries_refuses_newcomers_blindly() {
        // Every slot holds an attempt in flight: nothing may be shed, so a
        // network the map can't track is refused — with the same answer for a
        // right credential as a wrong one, or the refusal would be an oracle.
        let t = LoginThrottle::new(Duration::from_secs(300));
        let now = 1_000_000;
        for i in 0..LOGIN_THROTTLE_CAP as u32 {
            guess(&t, spread(i), Some(0), now).unwrap();
        }
        let newcomer: IpAddr = "198.51.100.1".parse().unwrap();
        let later = now + LOGIN_SWEEP_INTERVAL_MS;
        assert_eq!(guess(&t, newcomer, Some(7), later), Err(300));
        assert_eq!(login(&t, newcomer, later), Err(300));
        assert_eq!(t.len(), LOGIN_THROTTLE_CAP);
        // Expired partial counts alone are always shed.
        let t = LoginThrottle::new(Duration::from_secs(300));
        for i in 0..LOGIN_THROTTLE_CAP as u32 {
            fail(&t, spread(i), 0, now);
        }
        assert_eq!(guess(&t, newcomer, Some(7), now + 400_000), Ok(()));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn a_crowded_throttle_drops_only_expired_history() {
        // Past half the cap, a newcomer's sweep drops history the cooldown
        // already forgave, but keeps a partial count still inside it: shedding
        // that early would hand its owner fresh guesses.
        let t = LoginThrottle::new(Duration::from_secs(300));
        let now = 1_000_000;
        let recent: IpAddr = "203.0.113.9".parse().unwrap();
        fail(&t, recent, 1, now + 200_000);
        for i in 1..(LOGIN_THROTTLE_CAP / 2) as u32 {
            fail(&t, spread(i), 0, now);
        }
        assert_eq!(t.len(), LOGIN_THROTTLE_CAP / 2);
        let newcomer: IpAddr = "198.51.100.1".parse().unwrap();
        assert_eq!(guess(&t, newcomer, Some(7), now + 300_000), Ok(()));
        assert_eq!(t.len(), 2);
        assert_eq!(window(&t, recent).count, 1);
    }

    #[test]
    fn login_throttle_bounds_in_flight_guesses() {
        let t = LoginThrottle::new(Duration::from_secs(300));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let now = 1_000_000;
        // Five distinct wrong credentials in flight at once fill the budget.
        for fp in 1..=5 {
            assert_eq!(guess(&t, ip, Some(fp), now), Ok(()), "guess {fp}");
        }
        // A sixth is refused, and so is the right credential: admitting it
        // would turn the 429-versus-200 into an answer.
        assert_eq!(guess(&t, ip, Some(6), now), Err(1));
        assert_eq!(login(&t, ip, now), Err(1));
        // Repeating an in-flight credential is the same guess: admitted.
        assert_eq!(guess(&t, ip, Some(1), now), Ok(()));
        // A slot freed without a 401 (e.g. a public read) makes room.
        t.release_at(key(ip), 2, false, now);
        assert_eq!(guess(&t, ip, Some(6), now), Ok(()));
        // Credential 1 has two holders: one failing frees nothing yet...
        t.release_at(key(ip), 1, true, now);
        assert_eq!(guess(&t, ip, Some(7), now), Err(1));
        // ...and the last release counts the pair as one failure.
        t.release_at(key(ip), 1, false, now);
        assert_eq!(window(&t, ip).count, 1);
        assert_eq!(guess(&t, ip, Some(7), now), Err(1));
        for fp in [3, 4, 5, 6] {
            t.release_at(key(ip), fp, true, now);
        }
        assert_eq!(login(&t, ip, now), Err(300));
        assert_eq!(guess(&t, ip, Some(8), now), Err(300));
    }

    #[test]
    fn login_throttle_pending_survives_decay() {
        // A held request outliving the cooldown keeps its entry: sweeping it
        // would free the slot and lose the failure it is about to report.
        let t = LoginThrottle::new(Duration::from_secs(300));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let now = 1_000_000;
        fail(&t, ip, 1, now);
        guess(&t, ip, Some(2), now).unwrap();
        let later = now + 400_000;
        assert_eq!(login(&t, ip, later), Ok(()));
        assert_eq!(t.len(), 1);
        t.release_at(key(ip), 2, true, later);
        assert_eq!(window(&t, ip).count, 1);
    }

    #[test]
    fn login_attempt_drop_releases_without_a_failure() {
        let t = LoginThrottle::new(Duration::from_secs(300));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        // A request cancelled mid-flight (client gone) frees its slot and
        // counts nothing: the client never got an answer.
        let attempt = t.admit(ip, Some(1)).unwrap();
        assert_eq!(window(&t, ip).in_flight(), 1);
        drop(attempt);
        let w = window(&t, ip);
        assert_eq!((w.count, w.in_flight()), (0, 0));
        // A finished 401 counts one failure.
        t.admit(ip, Some(1)).unwrap().finish(true);
        let w = window(&t, ip);
        assert_eq!((w.count, w.in_flight()), (1, 0));
        // A valid credential reserves nothing.
        t.admit(ip, None).unwrap().finish(true);
        assert_eq!(window(&t, ip).count, 1);
    }

    #[test]
    fn login_fingerprint_ignores_the_attribution_tag() {
        let t = LoginThrottle::default();
        let fp = |u, p| t.fingerprint(&basic_headers(u, p));
        assert_eq!(fp("ci+billing", "pw"), fp("ci+web", "pw"));
        assert_eq!(fp("ci", "pw"), fp("ci+web", "pw"));
        assert_ne!(fp("ci", "pw"), fp("ci", "pw2"));
        assert_ne!(fp("ci", "pw"), fp("cd", "pw"));
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
