//! Install-token expiry reads the protocol clock (`pypiron::clock`), so the
//! simulator's clock override can drive a token past its `exp`.
//!
//! Its own test binary on purpose: the override is process-global, and this is
//! the only test in the process (see `src/clock.rs`).

use axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
use base64::{engine::general_purpose::STANDARD, Engine};
use pypiron::sim::{SimClock, SimStorage};
use pypiron::token::{self, Claims, Role};
use pypiron::AppState;

#[test]
fn token_expiry_follows_the_sim_clock() {
    // 2026-01-01T00:00:00Z — long before the real clock, so a real-clock read
    // would call this token expired from the first assert.
    let start = time::OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap();
    let clock = SimClock::new(start);
    let _guard = clock.install_global();

    let mut state = AppState::headless(SimStorage::new(clock.clone()));
    state.token_signing_key = Some("sim-signing-key".into());
    let now = start.unix_timestamp();
    let claims = Claims {
        role: Role::Uploader,
        repo: None,
        commit: None,
        user: None,
        iat: now,
        exp: now + 60,
        anon: false,
    };
    let minted = token::mint("sim-signing-key", &claims).unwrap();
    let mut headers = HeaderMap::new();
    let basic = STANDARD.encode(format!("{}:{minted}", token::TOKEN_USERNAME));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Basic {basic}")).unwrap(),
    );

    assert_eq!(state.token_role(&headers), Some(Role::Uploader));
    clock.advance(std::time::Duration::from_secs(59));
    assert_eq!(state.token_role(&headers), Some(Role::Uploader));
    clock.advance(std::time::Duration::from_secs(1));
    assert_eq!(state.token_role(&headers), None, "expired at exp");
}
