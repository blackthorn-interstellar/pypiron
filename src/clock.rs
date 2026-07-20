//! Wall-clock and entropy reads on a protocol path, funneled through one seam.
//!
//! Every time-of-day and marker-identity read the correctness protocol depends
//! on goes through this module so the deterministic simulator (dev/MOONSHOT.md
//! rung 1) can virtualize time and marker identity: freeze the clock, advance
//! it by hand, and replace random nonces with a counted sequence, making a whole
//! fleet's execution reproducible. Production takes the real-clock branch,
//! paying one relaxed atomic load of overhead per call.
//!
//! The override is process-global. It is intended for the single-threaded
//! simulator (and this module's own determinism self-check), NEVER for parallel
//! `#[test]`s: enabling it flips the clock for every thread at once, so two
//! tests touching it concurrently would see each other's time. The simulator
//! runs on one thread and holds a [`SimClockGuard`] for the whole run.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use time::OffsetDateTime;

/// Fast-path flag: `false` in production, so the hot check is a single relaxed
/// load followed by the real-clock branch.
static SIM_ENABLED: AtomicBool = AtomicBool::new(false);
/// Simulated absolute time as Unix nanoseconds. Unix nanos for dates in this
/// century (~1.77e18 for 2026) sit comfortably inside `u64` (~1.8e19 ceiling),
/// so a `u64` holds the whole representable simulator range; conversions
/// saturate rather than wrap.
static SIM_UNIX_NANOS: AtomicU64 = AtomicU64::new(0);
/// Monotonic counter behind [`sim_nonce`], reset to 0 each time the override is
/// enabled so every run starts from `sim0`.
static SIM_NONCE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Clamp an `OffsetDateTime` to the representable Unix-nanos range as `u64`.
/// Pre-epoch instants clamp to 0; anything past the `u64` ceiling saturates.
fn to_unix_nanos(t: OffsetDateTime) -> u64 {
    t.unix_timestamp_nanos().clamp(0, u64::MAX as i128) as u64
}

/// Current UTC time: the real clock in production, the simulated absolute time
/// when the override is enabled.
pub fn now_utc() -> OffsetDateTime {
    if SIM_ENABLED.load(Ordering::Relaxed) {
        let nanos = SIM_UNIX_NANOS.load(Ordering::Relaxed);
        return OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    }
    OffsetDateTime::now_utc()
}

/// Current Unix epoch time in milliseconds. The real branch preserves the exact
/// degrade behavior of the private-upload conflict tiebreak: a pre-epoch or
/// unrepresentable system clock maps to a value reconciliation quarantines
/// rather than trusting. The sim branch derives from the simulated nanos.
pub fn now_epoch_millis() -> u64 {
    if SIM_ENABLED.load(Ordering::Relaxed) {
        return SIM_UNIX_NANOS.load(Ordering::Relaxed) / 1_000_000;
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

/// Seconds since the Unix epoch, read straight from the system clock — NOT
/// sim-virtualized, unlike [`now_utc`]/[`now_epoch_millis`]. For operational
/// timestamps (metrics, probe cadence, feed-poll freshness) that never gate the
/// correctness protocol, so the simulator has no reason to control them and
/// every caller read the raw clock directly already. Pre-epoch clocks map to 0.
pub fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `None` in production. When the override is enabled, a unique deterministic
/// nonce (`sim0`, `sim1`, …) that replaces the random marker/claim entropy so
/// the simulator's histories are reproducible.
pub fn sim_nonce() -> Option<String> {
    if SIM_ENABLED.load(Ordering::Relaxed) {
        let n = SIM_NONCE_SEQ.fetch_add(1, Ordering::Relaxed);
        // 32 hex chars: origin claims validate their nonce as 128-bit hex
        // (fail-closed), and the sequence must satisfy the same shape real
        // entropy does. Still trivially readable in traces: ...0000002a.
        return Some(format!("{n:032x}"));
    }
    None
}

/// Enable the process-global override starting at `start`, returning a guard
/// that disables it on drop — so a panicking simulator run cannot poison the
/// clock for later runs in the same process. Resets the nonce sequence to 0.
/// The time and nonce are staged before the fast-path flag flips, so no reader
/// ever sees `enabled` with stale simulated state.
pub fn enable_sim(start: OffsetDateTime) -> SimClockGuard {
    SIM_UNIX_NANOS.store(to_unix_nanos(start), Ordering::Relaxed);
    SIM_NONCE_SEQ.store(0, Ordering::Relaxed);
    SIM_ENABLED.store(true, Ordering::Relaxed);
    SimClockGuard { _private: () }
}

/// Advance the global override by `d`. A no-op (never a panic) when the override
/// is not enabled, so callers need not branch on sim state.
#[cfg(test)]
pub(crate) fn sim_advance(d: std::time::Duration) {
    if !SIM_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let add = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
    let new = SIM_UNIX_NANOS.load(Ordering::Relaxed).saturating_add(add);
    SIM_UNIX_NANOS.store(new, Ordering::Relaxed);
}

/// Set the global override to an absolute Unix-nanos instant. Used by
/// [`crate::sim::SimClock`] to mirror its own advances into the global seam
/// while it holds the install; has no effect on readers unless the override is
/// enabled.
pub fn set_sim_unix_nanos(nanos: u64) {
    SIM_UNIX_NANOS.store(nanos, Ordering::Relaxed);
}

/// Disables the global clock override when dropped. Held for the lifetime of a
/// simulator run.
#[must_use = "dropping the guard immediately disables the sim clock"]
pub struct SimClockGuard {
    _private: (),
}

impl Drop for SimClockGuard {
    fn drop(&mut self) {
        SIM_ENABLED.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test only: the override is process-global, so a second concurrent
    // test touching it would race this one (see the module-level warning).
    #[test]
    fn sim_override_drives_time_and_nonce_then_restores() {
        // Real clock before we enable the override.
        assert!(now_utc() > OffsetDateTime::UNIX_EPOCH);
        assert!(sim_nonce().is_none());

        let start = OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap();
        {
            let _guard = enable_sim(start);
            assert_eq!(now_utc(), start);
            assert_eq!(now_epoch_millis(), 1_767_225_600_000);
            // Nonces count from zero, deterministically, shaped as the
            // 128-bit hex the origin-claim validator requires.
            assert_eq!(
                sim_nonce().as_deref(),
                Some("00000000000000000000000000000000")
            );
            assert_eq!(
                sim_nonce().as_deref(),
                Some("00000000000000000000000000000001")
            );
            // Time advances by hand.
            sim_advance(std::time::Duration::from_secs(5));
            assert_eq!(now_utc(), start + time::Duration::seconds(5));
            assert_eq!(now_epoch_millis(), 1_767_225_605_000);
        }
        // Guard dropped: the real clock is restored and nonces vanish.
        assert!(sim_nonce().is_none());
        assert!(now_utc() > start);
    }
}
