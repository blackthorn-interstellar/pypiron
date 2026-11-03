//! Per-node bucket health and selection.
//!
//! The async worker owns probes and real-traffic error mapping. This module is
//! deliberately synchronous: feed it classified signals plus a caller-supplied
//! [`Instant`], then apply the returned transition. That keeps timing,
//! hysteresis, and preference policy deterministic and unit-testable without a
//! mock runtime.

use std::error::Error;
use std::fmt;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// One storage observation, before it affects bucket health.
///
/// Semantic service errors get their own variants because an HTTP status alone
/// cannot reliably distinguish a dead bucket from credentials, KMS, quota, or
/// configuration trouble. Callers should map those errors before falling back
/// to [`BucketSignal::HttpStatus`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketSignal {
    Success,
    Timeout,
    ConnectionFailure,
    HttpStatus(u16),
    KmsError,
    QuotaError,
    ConfigurationError,
    OtherError,
}

/// The only three ways an observation can affect selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalClass {
    /// The bucket answered successfully. Clears its failure streak.
    Healthy,
    /// The bucket may be unavailable. Counts toward the leave threshold.
    AvailabilityFailure,
    /// Alarm-worthy, but never evidence that another bucket is safer.
    Ignored,
}

/// Classify a storage observation without guessing.
///
/// Only timeouts, connection failures, HTTP 408, and HTTP 5xx can drive a
/// selection change. Authentication, CAS, KMS, quota, configuration, unknown,
/// and other HTTP failures are fail-closed: the caller may alarm on them, but
/// this state machine ignores them.
pub const fn classify(signal: BucketSignal) -> SignalClass {
    match signal {
        BucketSignal::Success => SignalClass::Healthy,
        BucketSignal::Timeout | BucketSignal::ConnectionFailure => SignalClass::AvailabilityFailure,
        BucketSignal::HttpStatus(408 | 500..=599) => SignalClass::AvailabilityFailure,
        BucketSignal::HttpStatus(200..=399) => SignalClass::Healthy,
        BucketSignal::HttpStatus(_)
        | BucketSignal::KmsError
        | BucketSignal::QuotaError
        | BucketSignal::ConfigurationError
        | BucketSignal::OtherError => SignalClass::Ignored,
    }
}

/// Current health known by this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthState {
    /// No successful probe or full failure streak has been observed yet.
    Unknown,
    Healthy,
    Unhealthy,
}

/// Hysteresis policy. Leaving is failure-count based; returning is time based.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HealthPolicy {
    pub leave_after_failures: u32,
    pub return_after_healthy: Duration,
}

impl HealthPolicy {
    pub fn new(
        leave_after_failures: u32,
        return_after_healthy: Duration,
    ) -> Result<Self, HealthConfigError> {
        if leave_after_failures == 0 {
            return Err(HealthConfigError::ZeroLeaveThreshold);
        }
        Ok(Self {
            leave_after_failures,
            return_after_healthy,
        })
    }
}

/// Invalid selector construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthConfigError {
    NoBuckets,
    ZeroLeaveThreshold,
}

impl fmt::Display for HealthConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoBuckets => f.write_str("bucket health requires at least one bucket"),
            Self::ZeroLeaveThreshold => {
                f.write_str("bucket leave threshold must be greater than zero")
            }
        }
    }
}

impl Error for HealthConfigError {}

/// A caller supplied an index outside the configured bucket list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidBucket {
    pub index: usize,
    pub bucket_count: usize,
}

impl fmt::Display for InvalidBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bucket index {} is outside configured bucket count {}",
            self.index, self.bucket_count
        )
    }
}

impl Error for InvalidBucket {}

/// A selection change. The configured list order is the preference order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectionChange {
    pub from: usize,
    pub to: usize,
}

/// Where a node with a region read preference serves reads from.
///
/// Two states, not three. Reads that sit on the region bucket because the write
/// pin failed over onto it are not a mode of their own: that is `FollowWrite`
/// while the write pin happens to be there, and it ends when the write pin
/// leaves, with nothing to remember or expire. Recording that "loan" as a flag
/// is what let one outlive the pin twice (`cc9627d`, and a boot that recorded
/// one as earned).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadPin {
    /// Reads go wherever the write pin is.
    FollowWrite,
    /// Reads are pinned to the region bucket, whatever the write pin does. The
    /// bucket served the full return window and a worker caught-up check passed,
    /// and it has stayed usable ever since.
    Earned,
}

/// The read pin's whole transition function, over the region bucket as it is
/// now. The pin leaves the instant the bucket stops being usable (Unhealthy, or
/// recovered with its topology stamp unverified, which makes it ineligible for
/// replication and so possibly owed repair notes). It enters only through the
/// gate. Nothing else moves it.
fn next_read_pin(pin: ReadPin, usable: bool, served_window: bool, caught_up: bool) -> ReadPin {
    match pin {
        ReadPin::Earned if !usable => ReadPin::FollowWrite,
        ReadPin::FollowWrite if usable && served_window && caught_up => ReadPin::Earned,
        pin => pin,
    }
}

/// Effects of one observation or timer tick, for tests to assert on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HealthUpdate {
    /// The selected *write* bucket after applying this update, changed or not.
    pub selected_index: usize,
    /// Present only when the write selection changed.
    pub selection_change: Option<SelectionChange>,
    /// The selected *read* bucket after applying this update (equals
    /// `selected_index` unless the read pin is earned).
    pub read_selected_index: usize,
    /// Present only when the read selection changed.
    pub read_selection_change: Option<SelectionChange>,
}

/// Whether a bucket's topology stamp is trusted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Topology {
    Verified,
    /// The bucket just became reachable: any first success while not Healthy,
    /// `Unknown` included. Its stamp must be checked before anything relies on
    /// it.
    Pending,
    /// Pending, and handed to the worker in a snapshot. Only the worker's check
    /// of this recovery clears it; a newer recovery while the check is in flight
    /// sets it back to `Pending`.
    Checking,
}

#[derive(Clone, Debug)]
struct BucketStatus {
    state: HealthState,
    consecutive_failures: u32,
    healthy_since: Option<Instant>,
    topology: Topology,
}

impl BucketStatus {
    fn new(state: HealthState) -> Self {
        Self {
            state,
            consecutive_failures: 0,
            healthy_since: None,
            topology: Topology::Verified,
        }
    }

    /// Healthy with a verified topology stamp: the only buckets either pin may
    /// move onto.
    fn usable(&self) -> bool {
        self.state == HealthState::Healthy && self.topology == Topology::Verified
    }

    /// Usable and continuously healthy for the whole return window.
    fn served(&self, window: Duration, now: Instant) -> bool {
        self.usable()
            && self
                .healthy_since
                .is_some_and(|since| now.saturating_duration_since(since) >= window)
    }
}

/// Per-node health view and selected bucket.
///
/// Bucket zero starts selected. Other buckets start unknown and become eligible
/// after a successful probe or operation. A selected bucket is abandoned only
/// after its configured failure streak. A healthy selected bucket returns to a
/// more-preferred one only after that bucket has stayed continuously healthy for
/// the configured duration.
pub(crate) struct BucketHealth {
    policy: HealthPolicy,
    buckets: Vec<BucketStatus>,
    selected: usize,
    /// This node's region bucket, when one was matched at startup. `None` (no
    /// region, unlabelled buckets, or single-bucket) means reads always follow
    /// the write selection, behaviorally identical to a node with no affinity.
    read_preference: Option<usize>,
    read_pin: ReadPin,
}

/// One worker-cycle view of health observed by request and worker threads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerHealthSnapshot {
    pub selected_index: usize,
    /// Coalesces observations into the one switch the worker needs to apply.
    /// Repeated until [`HealthController::apply_selection`] publishes it.
    pub selection_change: Option<SelectionChange>,
    /// The read bucket this node should serve reads from (equals
    /// `selected_index` when the node has no region read preference).
    pub read_selected_index: usize,
    /// The one read-pin switch the worker needs to apply, repeated until
    /// [`HealthController::apply_read_selection`] publishes it.
    pub read_selection_change: Option<SelectionChange>,
    pub states: Vec<HealthState>,
    /// Sorted and repeated until acknowledged by
    /// [`HealthController::topology_check_passed`]. No pin moves onto one of
    /// these buckets until its stamp is checked.
    pub topology_revalidation: Vec<usize>,
    /// Ignored/alarm observations since the previous worker tick, per bucket.
    /// These never affect selection; they exist for logs and metrics.
    pub alarms: Vec<u64>,
}

struct ControllerState {
    health: BucketHealth,
    applied_selected: usize,
    /// The read bucket actually published to the [`BucketSet`]. Like
    /// `applied_selected`, a read switch is repeated in every snapshot until
    /// [`HealthController::apply_read_selection`] publishes it.
    applied_read_selected: usize,
    alarms: Vec<u64>,
}

/// Thread-safe bridge between real storage traffic and the async worker.
///
/// Request paths only take a short standard-library mutex, update the pure state
/// machine, and return. They never switch storage or perform topology I/O. The
/// worker periodically calls [`HealthController::worker_tick`] to drain the
/// coalesced work and apply it outside the lock.
pub struct HealthController {
    state: Mutex<ControllerState>,
}

impl HealthController {
    pub fn new(bucket_count: usize, policy: HealthPolicy) -> Result<Self, HealthConfigError> {
        Ok(Self {
            state: Mutex::new(ControllerState {
                health: BucketHealth::new(bucket_count, policy)?,
                applied_selected: 0,
                applied_read_selected: 0,
                alarms: vec![0; bucket_count],
            }),
        })
    }

    pub fn bucket_count(&self) -> usize {
        self.lock().health.buckets.len()
    }

    pub fn validate_bucket(&self, index: usize) -> Result<(), InvalidBucket> {
        let bucket_count = self.bucket_count();
        if index < bucket_count {
            Ok(())
        } else {
            Err(InvalidBucket {
                index,
                bucket_count,
            })
        }
    }

    /// Record one real data-plane result using the process monotonic clock.
    pub fn observe(&self, index: usize, signal: BucketSignal) -> Result<(), InvalidBucket> {
        self.observe_at(index, signal, Instant::now())
    }

    /// Advance return hysteresis and snapshot pending work. `region_caught_up`
    /// is the worker's fresh caught-up check of [`Self::read_return_pending`]'s
    /// bucket, and is consumed by this tick alone. Storage switching and
    /// topology validation happen after this returns and stay pending until
    /// they are published or acknowledged.
    pub fn worker_tick(&self, region_caught_up: bool) -> WorkerHealthSnapshot {
        self.worker_tick_at(Instant::now(), region_caught_up)
    }

    #[cfg(test)]
    pub(crate) fn health_state(&self, index: usize) -> Result<HealthState, InvalidBucket> {
        self.validate_bucket(index)?;
        Ok(self
            .lock()
            .health
            .state(index)
            .unwrap_or(HealthState::Unknown))
    }

    /// Whether background data-plane maintenance may touch this bucket.
    ///
    /// `Unknown` is eligible: startup verifies the configured topology before
    /// the worker starts, and an idle member may not yet have produced an
    /// ordinary health observation. Any first success while not Healthy
    /// (`Unknown` included) blocks the bucket until its topology stamp is
    /// acknowledged; startup acknowledges the ones its own verification raises.
    pub fn bucket_eligible(&self, index: usize) -> Result<bool, InvalidBucket> {
        let state = self.lock();
        let bucket = state.health.buckets.get(index).ok_or(InvalidBucket {
            index,
            bucket_count: state.health.buckets.len(),
        })?;
        Ok(bucket.state != HealthState::Unhealthy && bucket.topology == Topology::Verified)
    }

    /// Publish the write switch a snapshot proposed. The check and `publish` run
    /// under the lock every observation takes, so an observation that lands
    /// between snapshot and publication (the target failed, or recovered with an
    /// unverified stamp, or the selection moved on) cannot be overwritten by the
    /// stale proposal: nothing is published, and the next snapshot proposes
    /// afresh. Returns `publish`'s result, or `None` when the proposal was stale.
    pub fn apply_selection<T>(&self, to: usize, publish: impl FnOnce() -> T) -> Option<T> {
        let mut state = self.lock();
        let wanted = state.health.selected == to && state.health.usable(to);
        wanted.then(|| {
            state.applied_selected = to;
            publish()
        })
    }

    /// [`Self::apply_selection`] for the read switch.
    pub fn apply_read_selection<T>(&self, to: usize, publish: impl FnOnce() -> T) -> Option<T> {
        let mut state = self.lock();
        let wanted = state.health.read_index() == to && state.health.usable(to);
        wanted.then(|| {
            state.applied_read_selected = to;
            publish()
        })
    }

    /// Seed this node's region read preference. Called once at startup; a node
    /// with no region match never calls this and its reads follow the write
    /// selection forever.
    ///
    /// `earned` is startup's convergence verdict, and it is the only thing that
    /// pins reads to the region bucket: a node that booted with its write home
    /// down may already be reading from the region bucket, but only because the
    /// write pin is there, and those reads leave with it.
    ///
    /// Returns whether the pin ended up earned: an earned verdict on a bucket
    /// that is not usable (unverified, or failed its format check) still leaves
    /// reads following the write pin, and startup seeds the read pin only on
    /// `true`.
    pub fn configure_read_affinity(
        &self,
        preference: usize,
        earned: bool,
    ) -> Result<bool, InvalidBucket> {
        self.validate_bucket(preference)?;
        let mut state = self.lock();
        state.health.set_read_affinity(preference, earned);
        let pinned = state.health.read_pin == ReadPin::Earned;
        // What startup publishes: `seed_read_pin` only for an earned pin;
        // otherwise reads alias the write pin.
        state.applied_read_selected = if pinned {
            preference
        } else {
            state.applied_selected
        };
        Ok(pinned)
    }

    /// The region bucket when checking whether reads may return to it is
    /// worthwhile: a region preference exists, that bucket is usable, and reads
    /// are not already served from it. `None` otherwise, so the worker runs the
    /// caught-up LIST only when it matters.
    pub fn read_return_pending(&self) -> Option<usize> {
        let state = self.lock();
        let region = state.health.read_preference?;
        (state.applied_read_selected != region && state.health.usable(region)).then_some(region)
    }

    /// Trust a bucket's topology stamp because the caller verified it after the
    /// bucket's latest recovery: startup, whose verification I/O is what
    /// observed the recovery. The worker acknowledges through
    /// [`Self::topology_check_passed`] instead.
    pub fn topology_revalidated(&self, index: usize) -> Result<(), InvalidBucket> {
        self.with_bucket(index, |bucket| bucket.topology = Topology::Verified)
    }

    /// Acknowledge a worker check of a bucket a snapshot listed. If the bucket
    /// recovered again while the check was in flight, the check may have read
    /// the stamp before that recovery, so it stays pending for the next
    /// snapshot to re-list.
    pub fn topology_check_passed(&self, index: usize) -> Result<(), InvalidBucket> {
        self.with_bucket(index, |bucket| {
            if bucket.topology == Topology::Checking {
                bucket.topology = Topology::Verified;
            }
        })
    }

    fn with_bucket(
        &self,
        index: usize,
        update: impl FnOnce(&mut BucketStatus),
    ) -> Result<(), InvalidBucket> {
        let mut state = self.lock();
        let bucket_count = state.health.buckets.len();
        let bucket = state.health.buckets.get_mut(index).ok_or(InvalidBucket {
            index,
            bucket_count,
        })?;
        update(bucket);
        Ok(())
    }

    fn observe_at(
        &self,
        index: usize,
        signal: BucketSignal,
        now: Instant,
    ) -> Result<(), InvalidBucket> {
        let mut state = self.lock();
        state.health.observe(index, signal, now)?;
        if classify(signal) == SignalClass::Ignored {
            state.alarms[index] = state.alarms[index].saturating_add(1);
        }
        Ok(())
    }

    fn worker_tick_at(&self, now: Instant, region_caught_up: bool) -> WorkerHealthSnapshot {
        let mut state = self.lock();
        state.health.tick(now, region_caught_up);

        let selected_index = state.health.selected;
        let selection_change =
            (selected_index != state.applied_selected).then_some(SelectionChange {
                from: state.applied_selected,
                to: selected_index,
            });

        // Without a region preference reads alias the write pin natively
        // (BucketSet::read_pin), so never emit a read switch: it would
        // double-bump the shared generation on every write failover.
        let (read_selected_index, read_selection_change) = if state.health.read_preference.is_some()
        {
            let index = state.health.read_index();
            let change = (index != state.applied_read_selected).then_some(SelectionChange {
                from: state.applied_read_selected,
                to: index,
            });
            (index, change)
        } else {
            (selected_index, None)
        };

        let mut topology_revalidation = Vec::new();
        for (index, bucket) in state.health.buckets.iter_mut().enumerate() {
            if bucket.topology != Topology::Verified {
                bucket.topology = Topology::Checking;
                topology_revalidation.push(index);
            }
        }
        let alarms = state.alarms.clone();
        state.alarms.fill(0);
        let states = state
            .health
            .buckets
            .iter()
            .map(|bucket| bucket.state)
            .collect();

        WorkerHealthSnapshot {
            selected_index,
            selection_change,
            read_selected_index,
            read_selection_change,
            states,
            topology_revalidation,
            alarms,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ControllerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl BucketHealth {
    pub fn new(bucket_count: usize, policy: HealthPolicy) -> Result<Self, HealthConfigError> {
        if bucket_count == 0 {
            return Err(HealthConfigError::NoBuckets);
        }
        if policy.leave_after_failures == 0 {
            return Err(HealthConfigError::ZeroLeaveThreshold);
        }

        let mut buckets = vec![BucketStatus::new(HealthState::Unknown); bucket_count];
        buckets[0] = BucketStatus::new(HealthState::Healthy);
        Ok(Self {
            policy,
            buckets,
            selected: 0,
            read_preference: None,
            read_pin: ReadPin::FollowWrite,
        })
    }

    #[cfg(test)]
    fn selected_index(&self) -> usize {
        self.selected
    }

    /// The bucket reads are served from.
    fn read_index(&self) -> usize {
        match (self.read_preference, self.read_pin) {
            (Some(region), ReadPin::Earned) => region,
            _ => self.selected,
        }
    }

    fn usable(&self, index: usize) -> bool {
        self.buckets.get(index).is_some_and(BucketStatus::usable)
    }

    /// Seed the node's region read preference. Only startup's convergence
    /// verdict earns the pin, and only onto a usable bucket.
    fn set_read_affinity(&mut self, preference: usize, earned: bool) {
        self.read_preference = Some(preference);
        let pin = if earned {
            ReadPin::Earned
        } else {
            ReadPin::FollowWrite
        };
        self.read_pin = next_read_pin(pin, self.usable(preference), false, false);
    }

    #[cfg(test)]
    fn state(&self, index: usize) -> Option<HealthState> {
        self.buckets.get(index).map(|bucket| bucket.state)
    }

    /// Apply one observation, then re-evaluate selection. The request path never
    /// has a fresh caught-up check, so it can demote the read pin but never earn
    /// it.
    pub fn observe(
        &mut self,
        index: usize,
        signal: BucketSignal,
        now: Instant,
    ) -> Result<HealthUpdate, InvalidBucket> {
        let bucket_count = self.buckets.len();
        let bucket = self.buckets.get_mut(index).ok_or(InvalidBucket {
            index,
            bucket_count,
        })?;

        match classify(signal) {
            SignalClass::Healthy => {
                if bucket.state != HealthState::Healthy {
                    bucket.topology = Topology::Pending;
                }
                bucket.state = HealthState::Healthy;
                bucket.consecutive_failures = 0;
                if bucket.healthy_since.is_none() {
                    bucket.healthy_since = Some(now);
                }
            }
            SignalClass::AvailabilityFailure => {
                bucket.consecutive_failures = bucket.consecutive_failures.saturating_add(1);
                bucket.healthy_since = None;
                if bucket.consecutive_failures >= self.policy.leave_after_failures {
                    bucket.state = HealthState::Unhealthy;
                }
            }
            SignalClass::Ignored => {}
        }

        Ok(self.evaluate(now, false, false))
    }

    /// Re-evaluate a time-based return without manufacturing a health signal.
    /// `region_caught_up` is the worker's caught-up check for this tick only.
    pub fn tick(&mut self, now: Instant, region_caught_up: bool) -> HealthUpdate {
        self.evaluate(now, region_caught_up, true)
    }

    fn evaluate(&mut self, now: Instant, region_caught_up: bool, from_tick: bool) -> HealthUpdate {
        let (previous, previous_read) = (self.selected, self.read_index());
        let window = self.policy.return_after_healthy;

        let current = &self.buckets[self.selected];
        // Availability wins over return hysteresis when the current bucket is
        // gone, or, at a worker tick, unverified. Only the tick may leave a
        // healthy-but-unverified bucket: the worker checks buckets one at a
        // time, so between two acks a request-path observation would see the
        // current bucket merely awaiting its turn and jump to a more-preferred
        // one that healed seconds ago, skipping the return window.
        if current.state == HealthState::Unhealthy || (from_tick && !current.usable()) {
            // Pick the most-preferred usable bucket, on either side of the
            // current index; with none, stay.
            if let Some(index) = self.buckets.iter().position(BucketStatus::usable) {
                self.selected = index;
            }
        } else if let Some(index) = self.buckets[..self.selected]
            .iter()
            .position(|bucket| bucket.served(window, now))
        {
            // The current bucket still works. A more-preferred bucket must prove
            // continuous health for the much longer return window before we move.
            self.selected = index;
        }

        if let Some(bucket) = self
            .read_preference
            .and_then(|region| self.buckets.get(region))
        {
            self.read_pin = next_read_pin(
                self.read_pin,
                bucket.usable(),
                bucket.served(window, now),
                region_caught_up,
            );
        }

        let read = self.read_index();
        HealthUpdate {
            selected_index: self.selected,
            selection_change: (self.selected != previous).then_some(SelectionChange {
                from: previous,
                to: self.selected,
            }),
            read_selected_index: read,
            read_selection_change: (read != previous_read).then_some(SelectionChange {
                from: previous_read,
                to: read,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    fn policy(failures: u32, healthy_secs: u64) -> HealthPolicy {
        HealthPolicy::new(failures, Duration::from_secs(healthy_secs)).unwrap()
    }

    /// A success whose topology check passes, as startup and the worker
    /// acknowledge it. The update is the observation's, before the ack.
    fn heal(health: &mut BucketHealth, index: usize, now: Instant) -> HealthUpdate {
        let update = health.observe(index, BucketSignal::Success, now).unwrap();
        health.buckets[index].topology = Topology::Verified;
        update
    }

    #[test]
    fn classifier_is_strict_and_fail_closed() {
        for signal in [
            BucketSignal::Timeout,
            BucketSignal::ConnectionFailure,
            BucketSignal::HttpStatus(408),
            BucketSignal::HttpStatus(500),
            BucketSignal::HttpStatus(503),
            BucketSignal::HttpStatus(599),
        ] {
            assert_eq!(classify(signal), SignalClass::AvailabilityFailure);
        }

        for signal in [
            BucketSignal::HttpStatus(401),
            BucketSignal::HttpStatus(403),
            BucketSignal::HttpStatus(412),
            BucketSignal::HttpStatus(429),
            BucketSignal::KmsError,
            BucketSignal::QuotaError,
            BucketSignal::ConfigurationError,
            BucketSignal::OtherError,
        ] {
            assert_eq!(classify(signal), SignalClass::Ignored);
        }

        for signal in [
            BucketSignal::Success,
            BucketSignal::HttpStatus(200),
            BucketSignal::HttpStatus(204),
            BucketSignal::HttpStatus(302),
        ] {
            assert_eq!(classify(signal), SignalClass::Healthy);
        }
    }

    #[test]
    fn rejects_empty_sets_and_zero_leave_thresholds() {
        assert_eq!(
            HealthPolicy::new(0, Duration::from_secs(10)),
            Err(HealthConfigError::ZeroLeaveThreshold)
        );
        assert_eq!(
            BucketHealth::new(0, policy(2, 10)).err(),
            Some(HealthConfigError::NoBuckets)
        );

        let invalid = HealthPolicy {
            leave_after_failures: 0,
            return_after_healthy: Duration::from_secs(10),
        };
        assert_eq!(
            BucketHealth::new(2, invalid).err(),
            Some(HealthConfigError::ZeroLeaveThreshold)
        );
    }

    #[test]
    fn leaves_only_after_consecutive_availability_failures() {
        let now = Instant::now();
        let mut health = BucketHealth::new(2, policy(3, 60)).unwrap();
        heal(&mut health, 1, now);

        for signal in [
            BucketSignal::Timeout,
            BucketSignal::HttpStatus(403),
            BucketSignal::ConnectionFailure,
        ] {
            let update = health.observe(0, signal, now).unwrap();
            assert_eq!(update.selected_index, 0);
            assert_eq!(update.selection_change, None);
        }

        let update = health
            .observe(0, BucketSignal::HttpStatus(503), now)
            .unwrap();
        assert_eq!(update.selected_index, 1);
        assert_eq!(
            update.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        assert_eq!(health.state(0), Some(HealthState::Unhealthy));
    }

    #[test]
    fn success_resets_the_failure_streak() {
        let now = Instant::now();
        let mut health = BucketHealth::new(2, policy(2, 60)).unwrap();
        heal(&mut health, 1, now);
        health.observe(0, BucketSignal::Timeout, now).unwrap();
        heal(&mut health, 0, now);

        let update = health
            .observe(0, BucketSignal::ConnectionFailure, now)
            .unwrap();
        assert_eq!(update.selected_index, 0);
        assert_eq!(health.state(0), Some(HealthState::Healthy));
    }

    #[test]
    fn failover_chooses_the_most_preferred_known_healthy_bucket() {
        let now = Instant::now();
        let mut health = BucketHealth::new(3, policy(1, 60)).unwrap();
        heal(&mut health, 2, now);
        heal(&mut health, 1, now);

        let update = health.observe(0, BucketSignal::Timeout, now).unwrap();
        assert_eq!(update.selected_index, 1);
        assert_eq!(
            update.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
    }

    #[test]
    fn stays_put_when_no_other_bucket_is_known_healthy() {
        let now = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 60)).unwrap();
        let update = health.observe(0, BucketSignal::Timeout, now).unwrap();
        assert_eq!(update.selected_index, 0);
        assert_eq!(update.selection_change, None);
        assert_eq!(health.state(0), Some(HealthState::Unhealthy));
        assert_eq!(health.state(1), Some(HealthState::Unknown));
    }

    #[test]
    fn return_requires_continuous_health_for_the_full_window() {
        let now = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 10)).unwrap();
        heal(&mut health, 1, now);
        health.observe(0, BucketSignal::Timeout, now).unwrap();
        assert_eq!(health.selected_index(), 1);

        let recovered = heal(&mut health, 0, at(now, 1));
        assert_eq!(recovered.selected_index, 1);
        assert_eq!(health.tick(at(now, 10), false).selection_change, None);

        let returned = health.tick(at(now, 11), false);
        assert_eq!(returned.selected_index, 0);
        assert_eq!(
            returned.selection_change,
            Some(SelectionChange { from: 1, to: 0 })
        );
    }

    #[test]
    fn a_flap_restarts_the_return_window_and_does_not_oscillate() {
        let now = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 10)).unwrap();
        heal(&mut health, 1, now);
        health.observe(0, BucketSignal::Timeout, now).unwrap();
        heal(&mut health, 0, at(now, 1));

        health
            .observe(0, BucketSignal::Timeout, at(now, 8))
            .unwrap();
        heal(&mut health, 0, at(now, 9));
        assert_eq!(health.tick(at(now, 18), false).selected_index, 1);

        let returned = health.tick(at(now, 19), false);
        assert_eq!(returned.selected_index, 0);
        assert_eq!(
            returned.selection_change,
            Some(SelectionChange { from: 1, to: 0 })
        );
        assert_eq!(health.tick(at(now, 30), false).selection_change, None);
    }

    #[test]
    fn a_dead_selected_bucket_can_use_a_recovered_preferred_bucket_immediately() {
        let now = Instant::now();
        let mut health = BucketHealth::new(3, policy(1, 60)).unwrap();
        heal(&mut health, 1, now);
        heal(&mut health, 2, now);
        health.observe(0, BucketSignal::Timeout, now).unwrap();
        assert_eq!(health.selected_index(), 1);

        heal(&mut health, 0, at(now, 1));
        assert_eq!(
            health.selected_index(),
            1,
            "return hysteresis still applies"
        );

        let update = health
            .observe(1, BucketSignal::ConnectionFailure, at(now, 2))
            .unwrap();
        assert_eq!(update.selected_index, 0);
        assert_eq!(
            update.selection_change,
            Some(SelectionChange { from: 1, to: 0 })
        );
    }

    #[test]
    fn reachability_transitions_request_topology_revalidation_once() {
        let now = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 60)).unwrap();

        let pending = |health: &BucketHealth| health.buckets[1].topology == Topology::Pending;

        // Unknown -> Healthy counts: any first success while not Healthy.
        health.observe(1, BucketSignal::Success, now).unwrap();
        assert!(pending(&health));
        health.buckets[1].topology = Topology::Verified;

        health
            .observe(1, BucketSignal::Success, at(now, 1))
            .unwrap();
        assert!(!pending(&health), "a steady success raises nothing");

        health
            .observe(1, BucketSignal::HttpStatus(500), at(now, 2))
            .unwrap();
        health
            .observe(1, BucketSignal::Success, at(now, 3))
            .unwrap();
        assert!(pending(&health));
    }

    #[test]
    fn invalid_indices_are_errors_and_do_not_mutate_selection() {
        let now = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 60)).unwrap();
        let error = health.observe(2, BucketSignal::Success, now).unwrap_err();
        assert_eq!(
            error,
            InvalidBucket {
                index: 2,
                bucket_count: 2
            }
        );
        assert_eq!(health.selected_index(), 0);
    }

    #[test]
    fn controller_coalesces_request_observations_for_the_worker() {
        let now = Instant::now();
        let controller = HealthController::new(2, policy(2, 60)).unwrap();

        controller
            .observe_at(1, BucketSignal::Success, now)
            .unwrap();
        controller
            .observe_at(0, BucketSignal::Timeout, now)
            .unwrap();
        controller
            .observe_at(0, BucketSignal::ConnectionFailure, now)
            .unwrap();

        let snapshot = controller.worker_tick_at(now, false);
        assert_eq!(snapshot.selected_index, 0);
        assert_eq!(snapshot.selection_change, None);
        assert_eq!(
            snapshot.states,
            vec![HealthState::Unhealthy, HealthState::Healthy]
        );
        assert_eq!(snapshot.topology_revalidation, vec![1]);
        assert_eq!(snapshot.alarms, vec![0, 0]);

        let pending = controller.worker_tick_at(at(now, 1), false);
        assert_eq!(pending.selection_change, snapshot.selection_change);
        assert_eq!(pending.topology_revalidation, vec![1]);

        controller.topology_revalidated(1).unwrap();
        let ready = controller.worker_tick_at(at(now, 2), false);
        assert_eq!(ready.selected_index, 1);
        assert_eq!(
            ready.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        controller.apply_selection(1, || ()).unwrap();
        let quiet = controller.worker_tick_at(at(now, 2), false);
        assert_eq!(quiet.selection_change, None);
        assert!(quiet.topology_revalidation.is_empty());
    }

    #[test]
    fn controller_deduplicates_revalidation_and_drains_alarm_counts() {
        let now = Instant::now();
        let controller = HealthController::new(2, policy(1, 60)).unwrap();

        controller
            .observe_at(1, BucketSignal::Success, now)
            .unwrap();
        controller
            .observe_at(1, BucketSignal::HttpStatus(403), now)
            .unwrap();
        controller
            .observe_at(1, BucketSignal::ConfigurationError, now)
            .unwrap();
        controller
            .observe_at(1, BucketSignal::Success, now)
            .unwrap();

        let snapshot = controller.worker_tick_at(now, false);
        assert_eq!(snapshot.topology_revalidation, vec![1]);
        assert_eq!(snapshot.alarms, vec![0, 2]);
        controller.topology_revalidated(1).unwrap();
        assert_eq!(controller.worker_tick_at(now, false).alarms, vec![0, 0]);
    }

    #[test]
    fn rejected_topology_candidate_cannot_strand_later_failover() {
        let now = Instant::now();
        let controller = HealthController::new(3, policy(1, 10)).unwrap();

        for index in [1, 2] {
            controller
                .observe_at(index, BucketSignal::Success, now)
                .unwrap();
            controller.topology_revalidated(index).unwrap();
        }
        controller
            .observe_at(0, BucketSignal::Timeout, now)
            .unwrap();
        let to_b = controller.worker_tick_at(now, false);
        assert_eq!(
            to_b.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        controller.apply_selection(1, || ()).unwrap();

        // A recovers long enough to be preferred, but its topology check is
        // deliberately never acknowledged (the worker saw a mismatch).
        controller
            .observe_at(0, BucketSignal::Success, at(now, 1))
            .unwrap();
        let rejected = controller.worker_tick_at(at(now, 11), false);
        assert_eq!(rejected.selected_index, 1);
        assert_eq!(rejected.topology_revalidation, vec![0]);
        assert!(!controller.bucket_eligible(0).unwrap());
        assert!(controller.bucket_eligible(2).unwrap());

        // When the actually selected B dies, the selector must skip invalid A
        // and continue to validated C instead of sticking reads on dead B.
        controller
            .observe_at(1, BucketSignal::Timeout, at(now, 12))
            .unwrap();
        let to_c = controller.worker_tick_at(at(now, 12), false);
        assert_eq!(
            to_c.selection_change,
            Some(SelectionChange { from: 1, to: 2 })
        );
        assert_eq!(to_c.selected_index, 2);
    }

    #[test]
    fn maintenance_waits_for_recovered_topology_validation() {
        let now = Instant::now();
        let controller = HealthController::new(2, policy(1, 60)).unwrap();

        // Startup topology verification happens before ordinary observations.
        // Preserve that validated-but-idle Unknown member as eligible.
        assert_eq!(controller.health_state(1).unwrap(), HealthState::Unknown);
        assert!(controller.bucket_eligible(1).unwrap());

        controller
            .observe_at(1, BucketSignal::Success, now)
            .unwrap();
        assert!(
            !controller.bucket_eligible(1).unwrap(),
            "a newly reachable bucket stays data-plane-ineligible"
        );

        controller.topology_revalidated(1).unwrap();
        assert!(controller.bucket_eligible(1).unwrap());

        controller
            .observe_at(1, BucketSignal::Timeout, at(now, 1))
            .unwrap();
        assert!(!controller.bucket_eligible(1).unwrap());
    }

    #[test]
    fn controller_is_safe_for_concurrent_observers() {
        let controller = std::sync::Arc::new(HealthController::new(2, policy(1_000, 60)).unwrap());
        let mut threads = Vec::new();
        for _ in 0..8 {
            let controller = controller.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    controller
                        .observe(1, BucketSignal::ConfigurationError)
                        .unwrap();
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }

        let snapshot = controller.worker_tick(false);
        assert_eq!(snapshot.alarms, vec![0, 800]);
        assert_eq!(snapshot.selected_index, 0);
    }

    #[test]
    fn read_returns_to_region_only_after_window_and_caught_up() {
        let base = Instant::now();
        // The node's region is the non-preferred bucket (index 1); reads start
        // following the write selection (bucket 0).
        let mut health = BucketHealth::new(2, policy(3, 10)).unwrap();
        health.set_read_affinity(1, false);
        heal(&mut health, 1, base);

        // Window not elapsed: reads stay on the write bucket.
        assert_eq!(health.tick(at(base, 5), false).read_selected_index, 0);
        // Window elapsed but not caught up: still on the write bucket.
        assert_eq!(health.tick(at(base, 11), false).read_selected_index, 0);
        // Caught up and window elapsed: reads return to the region bucket, and
        // the write selection never moved.
        let update = health.tick(at(base, 12), true);
        assert_eq!(update.read_selected_index, 1);
        assert_eq!(
            update.read_selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        assert_eq!(update.selected_index, 0);
        assert_eq!(health.selected_index(), 0);
    }

    #[test]
    fn a_failover_read_grant_is_surrendered_when_the_write_pin_goes_home() {
        let base = Instant::now();
        // Region is bucket 1; reads follow the write selection (bucket 0) and
        // have never passed the drain gate.
        let mut health = BucketHealth::new(2, policy(1, 10)).unwrap();
        health.set_read_affinity(1, false);
        heal(&mut health, 1, base);

        // The write home dies: writes fail over to the region bucket and reads
        // follow, gate or no gate — a dead bucket serves nothing.
        let failover = health
            .observe(0, BucketSignal::Timeout, at(base, 1))
            .unwrap();
        assert_eq!(failover.selected_index, 1);
        assert_eq!(failover.read_selected_index, 1);

        // The write home recovers and the write pin returns. The read grant was
        // only ever the write pin's; bucket 1 may still owe repair notes, so
        // reads go back to bucket 0 rather than staying latched.
        heal(&mut health, 0, at(base, 2));
        let returned = health.tick(at(base, 13), false);
        assert_eq!(returned.selected_index, 0);
        assert_eq!(returned.read_selected_index, 0);
        assert_eq!(
            returned.read_selection_change,
            Some(SelectionChange { from: 1, to: 0 })
        );

        // The ordinary gate governs the return: window plus caught up earns it.
        let earned = health.tick(at(base, 14), true);
        assert_eq!(earned.read_selected_index, 1);
        assert_eq!(earned.selected_index, 0);
    }

    #[test]
    fn a_loan_outlives_the_region_bucket_dying_with_nowhere_to_demote_to() {
        let base = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 10)).unwrap();
        health.set_read_affinity(1, false);
        heal(&mut health, 1, base);

        // The write home dies: reads are lent to the region bucket.
        let failover = health
            .observe(0, BucketSignal::Timeout, at(base, 1))
            .unwrap();
        assert_eq!(failover.selected_index, 1);
        assert_eq!(failover.read_selected_index, 1);

        // Now the region bucket dies too. No bucket is healthy, so the write
        // selection cannot move and reads have nowhere to go: the demotion is a
        // no-op, and must not be mistaken for reads having earned this pin.
        let both_dead = health
            .observe(1, BucketSignal::Timeout, at(base, 2))
            .unwrap();
        assert_eq!(both_dead.selected_index, 1, "nowhere to fail over to");
        assert_eq!(both_dead.read_selected_index, 1);

        // Both heal and the write pin goes home. The loan was never repaid, so
        // reads leave the region bucket and re-enter the drain gate.
        heal(&mut health, 1, at(base, 3));
        heal(&mut health, 0, at(base, 4));
        let returned = health.tick(at(base, 15), false);
        assert_eq!(returned.selected_index, 0);
        assert_eq!(returned.read_selected_index, 0);
        assert_eq!(
            returned.read_selection_change,
            Some(SelectionChange { from: 1, to: 0 })
        );
    }

    #[test]
    fn an_earned_read_pin_survives_a_write_failover_round_trip() {
        let base = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 10)).unwrap();
        health.set_read_affinity(1, false);
        heal(&mut health, 1, base);

        // Reads earn the region pin through the gate before anything fails.
        assert_eq!(health.tick(at(base, 11), true).read_selected_index, 1);

        let failover = health
            .observe(0, BucketSignal::Timeout, at(base, 12))
            .unwrap();
        assert_eq!(failover.selected_index, 1);
        assert_eq!(failover.read_selected_index, 1);

        // The write pin goes home. The region bucket was the write home through
        // the outage, so it missed nothing: the earned pin does not move, even
        // with the gate closed (the worker clears caught-up while reads sit on
        // the region bucket).
        heal(&mut health, 0, at(base, 13));
        let returned = health.tick(at(base, 24), false);
        assert_eq!(returned.selected_index, 0);
        assert_eq!(returned.read_selected_index, 1);
        assert_eq!(returned.read_selection_change, None);
    }

    #[test]
    fn an_unconverged_boot_onto_the_failed_over_write_home_is_a_loan_not_an_earned_pin() {
        let base = Instant::now();
        // The write home (bucket 0) is unreachable at boot, so the write pin
        // fails over onto the region bucket (1) and startup's `read_index`
        // resolves to the write pin — which IS the region bucket. Startup could
        // not confirm convergence (an unreachable peer alone forces that), so
        // this pin never passed the drain gate.
        let mut health = BucketHealth::new(2, policy(1, 10)).unwrap();
        heal(&mut health, 1, base);
        health.observe(0, BucketSignal::Timeout, base).unwrap();
        assert_eq!(health.selected_index(), 1, "write pin failed over");
        health.set_read_affinity(1, false);

        // The write home recovers and the write pin goes home. The read pin was
        // only ever the write pin's, so it must come with it — leaving the
        // region bucket to earn its way back through the gate. Before this was
        // recorded as a loan the pin simply never moved again, which also meant
        // the worker never proposed a read switch and read affinity stayed
        // inactive for the life of the process.
        heal(&mut health, 0, at(base, 1));
        let returned = health.tick(at(base, 12), false);
        assert_eq!(returned.selected_index, 0);
        assert_eq!(returned.read_selected_index, 0);
        assert_eq!(
            returned.read_selection_change,
            Some(SelectionChange { from: 1, to: 0 }),
            "the read switch is what activates read affinity at all"
        );

        // And the ordinary gate governs the real return.
        assert_eq!(health.tick(at(base, 13), true).read_selected_index, 1);
    }

    #[test]
    fn a_simultaneous_recovery_keeps_the_write_pin_until_the_return_window() {
        let base = Instant::now();
        let controller = HealthController::new(2, policy(1, 300)).unwrap();
        controller
            .observe_at(1, BucketSignal::Success, base)
            .unwrap();
        controller.topology_revalidated(1).unwrap();
        controller
            .observe_at(0, BucketSignal::Timeout, at(base, 1))
            .unwrap();
        let failover = controller.worker_tick_at(at(base, 2), false);
        assert_eq!(
            failover.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        controller.apply_selection(1, || ()).unwrap();

        // The node loses both buckets, and both recover in one probe round.
        controller
            .observe_at(1, BucketSignal::Timeout, at(base, 3))
            .unwrap();
        controller
            .observe_at(0, BucketSignal::Success, at(base, 4))
            .unwrap();
        controller
            .observe_at(1, BucketSignal::Success, at(base, 4))
            .unwrap();
        let checking = controller.worker_tick_at(at(base, 5), false);
        assert_eq!(checking.topology_revalidation, [0, 1]);
        assert_eq!(checking.selection_change, None);

        // The worker acks bucket 0 first; a request lands before bucket 1's ack.
        controller.topology_check_passed(0).unwrap();
        controller
            .observe_at(1, BucketSignal::Success, at(base, 6))
            .unwrap();
        controller.topology_check_passed(1).unwrap();
        let after = controller.worker_tick_at(at(base, 7), false);
        assert_eq!(after.selected_index, 1);
        assert_eq!(
            after.selection_change, None,
            "a bucket healed seconds ago must still wait out the return window"
        );
    }

    #[test]
    fn a_topology_blocked_region_bucket_cannot_hold_the_read_pin() {
        let base = Instant::now();
        let controller = HealthController::new(2, policy(1, 10)).unwrap();

        // Boot verifies the region bucket, then it flaps and heals inside the
        // copy-matrix window (src/app.rs), after the boot ack loop: Healthy, but
        // its stamp unverified and so ineligible for replication.
        controller
            .observe_at(1, BucketSignal::Success, base)
            .unwrap();
        controller.topology_revalidated(1).unwrap();
        controller
            .observe_at(1, BucketSignal::Timeout, at(base, 1))
            .unwrap();
        controller
            .observe_at(1, BucketSignal::Success, at(base, 2))
            .unwrap();
        assert!(!controller.bucket_eligible(1).unwrap());

        // Startup's convergence check ran before the flap and says earned. The
        // pin still may not land on a bucket that can be accruing repair notes,
        // so startup is told not to seed it and reads never go there.
        assert!(!controller.configure_read_affinity(1, true).unwrap());
        assert_eq!(controller.lock().health.read_pin, ReadPin::FollowWrite);
        let blocked = controller.worker_tick_at(at(base, 3), false);
        assert_eq!(blocked.read_selected_index, 0);
        assert_eq!(blocked.read_selection_change, None);

        // On acknowledgement it re-earns the pin through the gate.
        controller.topology_check_passed(1).unwrap();
        assert_eq!(
            controller
                .worker_tick_at(at(base, 13), false)
                .read_selected_index,
            0,
            "the gate still governs the return"
        );
        assert_eq!(
            controller
                .worker_tick_at(at(base, 14), true)
                .read_selected_index,
            1
        );
    }

    #[test]
    fn the_request_path_can_demote_the_read_pin_but_never_earn_it() {
        let base = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 10)).unwrap();
        health.set_read_affinity(1, false);
        heal(&mut health, 1, base);

        // Window served, yet an observation has no fresh caught-up check.
        let observed = heal(&mut health, 0, at(base, 11));
        assert_eq!(observed.read_selected_index, 0);
        assert_eq!(health.tick(at(base, 11), true).read_selected_index, 1);

        // The bucket dies and recovers; a caught-up verdict from before the
        // outage is gone with the tick that consumed it.
        health
            .observe(1, BucketSignal::Timeout, at(base, 12))
            .unwrap();
        heal(&mut health, 1, at(base, 13));
        assert_eq!(heal(&mut health, 0, at(base, 24)).read_selected_index, 0);
        assert_eq!(health.tick(at(base, 25), true).read_selected_index, 1);
    }

    #[test]
    fn read_leaves_region_immediately_on_the_leave_threshold() {
        let base = Instant::now();
        let mut health = BucketHealth::new(2, policy(3, 10)).unwrap();
        heal(&mut health, 1, base);
        health.set_read_affinity(1, true);
        assert_eq!(health.read_index(), 1);

        for _ in 0..2 {
            let update = health.observe(1, BucketSignal::Timeout, base).unwrap();
            assert_eq!(update.read_selected_index, 1, "holds below the threshold");
        }
        let update = health.observe(1, BucketSignal::Timeout, base).unwrap();
        assert_eq!(update.read_selected_index, 0);
        assert_eq!(
            update.read_selection_change,
            Some(SelectionChange { from: 1, to: 0 })
        );
    }

    #[test]
    fn no_read_preference_tracks_the_write_selection_in_every_state() {
        let base = Instant::now();
        let mut health = BucketHealth::new(2, policy(1, 10)).unwrap();
        heal(&mut health, 1, base);
        let update = health.observe(0, BucketSignal::Timeout, base).unwrap();
        assert_eq!(update.selected_index, 1);
        assert_eq!(update.read_selected_index, 1);
        assert_eq!(
            update.read_selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
    }

    #[test]
    fn read_preference_never_perturbs_the_write_selection() {
        // Same scenario as `failover_chooses_the_most_preferred_known_healthy_bucket`,
        // but with a region read preference set: the write selection is identical.
        let base = Instant::now();
        let mut health = BucketHealth::new(3, policy(1, 60)).unwrap();
        health.set_read_affinity(2, false);
        heal(&mut health, 2, base);
        heal(&mut health, 1, base);

        let update = health.observe(0, BucketSignal::Timeout, base).unwrap();
        assert_eq!(update.selected_index, 1);
        assert_eq!(
            update.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
    }

    #[test]
    fn controller_reports_read_selection_and_pending_return() {
        let base = Instant::now();
        let controller = HealthController::new(2, policy(1, 10)).unwrap();
        controller.configure_read_affinity(1, false).unwrap();

        // Region bucket 1 becomes reachable; a topology check blocks it until
        // acknowledged, so no return is pending yet.
        controller
            .observe_at(1, BucketSignal::Success, base)
            .unwrap();
        assert_eq!(controller.read_return_pending(), None);
        controller.topology_revalidated(1).unwrap();
        assert_eq!(controller.read_return_pending(), Some(1));

        // With caught_up confirmed and the window elapsed, the worker snapshot
        // proposes the read switch; the write selection is untouched.
        let snap = controller.worker_tick_at(at(base, 11), true);
        assert_eq!(snap.selected_index, 0);
        assert_eq!(snap.read_selected_index, 1);
        assert_eq!(
            snap.read_selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        controller.apply_read_selection(1, || ()).unwrap();
        assert_eq!(controller.read_return_pending(), None);
        assert_eq!(
            controller
                .worker_tick_at(at(base, 12), false)
                .read_selection_change,
            None
        );
    }

    /// Every read-pin transition, over every region-bucket status that feeds
    /// one, against the rules stated rather than the code: the pin leaves the
    /// instant the bucket is unusable, enters only through the gate, and
    /// nothing else moves it.
    #[test]
    fn read_pin_transitions_are_exhaustive() {
        let window = Duration::from_secs(10);
        let now = Instant::now() + window;
        let mut edges = Vec::new();
        for pin in [ReadPin::FollowWrite, ReadPin::Earned] {
            for state in [
                HealthState::Unknown,
                HealthState::Healthy,
                HealthState::Unhealthy,
            ] {
                for topology in [Topology::Verified, Topology::Pending, Topology::Checking] {
                    for healthy_since in [None, Some(now), Some(now - window)] {
                        for caught_up in [false, true] {
                            let bucket = BucketStatus {
                                state,
                                consecutive_failures: 0,
                                healthy_since,
                                topology,
                            };
                            let usable = bucket.usable();
                            let served = bucket.served(window, now);
                            assert_eq!(
                                usable,
                                state == HealthState::Healthy && topology == Topology::Verified
                            );
                            assert!(!served || usable);

                            let next = next_read_pin(pin, usable, served, caught_up);
                            let expected = if !usable {
                                ReadPin::FollowWrite
                            } else if served && caught_up {
                                ReadPin::Earned
                            } else {
                                pin
                            };
                            assert_eq!(
                                next, expected,
                                "{pin:?} {state:?} {topology:?} since={healthy_since:?} caught_up={caught_up}"
                            );
                            if !edges.contains(&(pin, next)) {
                                edges.push((pin, next));
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(
            edges.len(),
            4,
            "every edge of the two-state machine is reachable"
        );

        // Where reads go, for every pin and both write placements.
        let mut health = BucketHealth::new(3, policy(1, 10)).unwrap();
        assert_eq!(
            health.read_index(),
            0,
            "no preference follows the write pin"
        );
        health.read_preference = Some(1);
        for selected in [0, 1] {
            health.selected = selected;
            health.read_pin = ReadPin::FollowWrite;
            assert_eq!(health.read_index(), selected);
            health.read_pin = ReadPin::Earned;
            assert_eq!(health.read_index(), 1);
        }
    }

    #[test]
    fn a_topology_pending_bucket_is_never_selected_by_the_pure_machine() {
        let base = Instant::now();
        let controller = HealthController::new(2, policy(1, 10)).unwrap();
        controller
            .observe_at(1, BucketSignal::Success, base)
            .unwrap();
        controller.topology_revalidated(1).unwrap();
        controller.configure_read_affinity(1, false).unwrap();

        // Writes fail over onto the region bucket; reads follow.
        controller
            .observe_at(0, BucketSignal::Timeout, base)
            .unwrap();
        controller.apply_selection(1, || ()).unwrap();
        controller.apply_read_selection(1, || ()).unwrap();

        // The home heals and serves the full window, but its stamp is never
        // acknowledged. A request-path observation must not send either pin
        // there, though no worker tick has run to override anything.
        controller
            .observe_at(0, BucketSignal::Success, at(base, 1))
            .unwrap();
        controller
            .observe_at(0, BucketSignal::Success, at(base, 12))
            .unwrap();
        let state = controller.lock();
        assert_eq!(state.health.selected, 1);
        assert_eq!(state.health.read_index(), 1);
        drop(state);

        // Nor is a pending bucket a failover target before any tick.
        let controller = HealthController::new(2, policy(1, 10)).unwrap();
        controller
            .observe_at(1, BucketSignal::Success, base)
            .unwrap();
        controller
            .observe_at(0, BucketSignal::Timeout, base)
            .unwrap();
        assert_eq!(controller.lock().health.selected, 0);
    }

    #[test]
    fn a_switch_invalidated_after_its_snapshot_is_not_published() {
        let base = Instant::now();
        let controller = HealthController::new(2, policy(1, 10)).unwrap();
        controller
            .observe_at(1, BucketSignal::Success, base)
            .unwrap();
        controller.topology_revalidated(1).unwrap();
        controller.configure_read_affinity(1, false).unwrap();

        let snapshot = controller.worker_tick_at(at(base, 11), true);
        assert_eq!(
            snapshot.read_selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        // While the worker awaits other checks, the region bucket flaps and
        // comes back with its stamp unverified.
        controller
            .observe_at(1, BucketSignal::Timeout, at(base, 12))
            .unwrap();
        controller
            .observe_at(1, BucketSignal::Success, at(base, 12))
            .unwrap();
        let mut published = false;
        assert_eq!(
            controller.apply_read_selection(1, || published = true),
            None
        );
        assert!(!published, "a stale read switch reached the BucketSet");
        assert_eq!(controller.lock().applied_read_selected, 0);

        // The same for a write switch.
        controller
            .observe_at(0, BucketSignal::Timeout, at(base, 13))
            .unwrap();
        controller.topology_revalidated(1).unwrap();
        let snapshot = controller.worker_tick_at(at(base, 13), false);
        assert_eq!(
            snapshot.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        controller
            .observe_at(1, BucketSignal::Timeout, at(base, 14))
            .unwrap();
        controller
            .observe_at(1, BucketSignal::Success, at(base, 14))
            .unwrap();
        assert_eq!(controller.apply_selection(1, || ()), None);
        assert_eq!(controller.lock().applied_selected, 0);
    }

    #[test]
    fn a_topology_check_cannot_clear_a_newer_recovery() {
        let base = Instant::now();
        let controller = HealthController::new(2, policy(1, 10)).unwrap();
        controller
            .observe_at(1, BucketSignal::Success, base)
            .unwrap();
        assert_eq!(
            controller.worker_tick_at(base, false).topology_revalidation,
            vec![1]
        );

        // The bucket recovers again while that check is in flight. The check may
        // have read the stamp before this recovery, so it clears nothing.
        controller
            .observe_at(1, BucketSignal::Timeout, at(base, 1))
            .unwrap();
        controller
            .observe_at(1, BucketSignal::Success, at(base, 1))
            .unwrap();
        controller.topology_check_passed(1).unwrap();
        assert!(!controller.bucket_eligible(1).unwrap());

        // The next snapshot re-lists it, and that check counts.
        assert_eq!(
            controller
                .worker_tick_at(at(base, 2), false)
                .topology_revalidation,
            vec![1]
        );
        controller.topology_check_passed(1).unwrap();
        assert!(controller.bucket_eligible(1).unwrap());
    }

    #[test]
    fn controller_rejects_an_unconfigured_bucket() {
        let controller = HealthController::new(2, policy(1, 60)).unwrap();
        assert_eq!(
            controller.validate_bucket(2),
            Err(InvalidBucket {
                index: 2,
                bucket_count: 2
            })
        );
        assert_eq!(
            controller.observe(2, BucketSignal::Success),
            Err(InvalidBucket {
                index: 2,
                bucket_count: 2
            })
        );
    }
}

/// A randomized conformance walk over the read pin.
///
/// Neither the stateright models nor the VOPR models the read pin at all, so
/// the two defects fixed in `cc9627d` — a failover grant that latched forever,
/// and a demotion that moved nothing yet reported the pin as earned — had no
/// mechanized adversary. This is that adversary, and it is deliberately *not* a
/// transcribed model: it drives the real [`HealthController`] through random
/// fault / upload / drain schedules and checks serving invariants, so a
/// transcription gap cannot hide a bug the way it did for the models.
///
/// The walk owns the ground truth the controller cannot see — which buckets are
/// actually reachable, and how many `_repl/` repair notes the region bucket is
/// owed — and mirrors two contracts that connect the two: the startup sequence
/// in `src/app.rs` that decides where the read pin is seeded, and the worker
/// cycle in `src/worker.rs` that maintains it. Every mirrored behavior cites the
/// lines it copies, so drift is auditable by reading them side by side.
///
/// Fidelity is what makes a violation mean anything, and two rules do the work:
/// every run starts from a state the real startup can produce, and simulated
/// time inside one worker cycle is bounded by what a real cycle can span. Both
/// are load-bearing, and both were learned the hard way — an earlier revision
/// had neither and reported two "defects" the real code cannot reach: one
/// started every run with a read pin on a never-observed bucket, and one let a
/// region bucket fail, recover and re-earn a full return window inside a single
/// cycle. The boot model below is written against `src/app.rs` line by line for
/// that reason.
///
/// With those fixed it found a real one: a node booting with its write home down
/// recorded reads on the region bucket as an earned pin, and read affinity then
/// died silently for the life of the process. [`ReadPin`] now has no way to say
/// that: an unconverged boot is `FollowWrite`, wherever the write pin is.
#[cfg(test)]
mod conformance_walk {
    use super::*;
    use std::collections::HashSet;
    use std::fmt::Write as _;

    /// Worker loop period: `sleep(state.worker_interval)` at the foot of the
    /// loop (src/worker.rs:222-226), whose flag defaults to 1 s (src/cli.rs:805).
    const TICK: Duration = Duration::from_secs(1);

    /// `BUCKET_HEALTH_IO_TIMEOUT` (src/worker.rs:65). Every piece of I/O a cycle
    /// does is bounded by it, which is what bounds the cycle.
    const IO_TIMEOUT: Duration = Duration::from_secs(1);

    /// Read-return window. The real flag defaults to 300 s (src/cli.rs:816-821)
    /// against a cycle that spans a handful of seconds; what matters to this
    /// walk is only that the window is LONGER than a worst-case cycle, because
    /// that is what makes it impossible for a region bucket to fail, recover and
    /// re-earn the full window inside a single cycle. 10 s clears the widest
    /// topology here (3 buckets, 7 s) by the narrowest margin that still does —
    /// far more adversarial than the shipped 40x ratio, and still sound.
    ///
    /// An operator can configure a window the walk therefore does not model: one
    /// at or below a worst-case cycle, where reads may return to the region
    /// bucket on a caught-up verdict up to a cycle stale. The consequence is
    /// bounded (a missed file is read through from the write home, never a 404),
    /// so startup warns rather than refuses — `read_return_window_under_floor`
    /// (src/worker.rs) computes the floor from the same constants this walk
    /// mirrors, and `src/app.rs` logs it beside the read-affinity decision.
    const RETURN_WINDOW: Duration = Duration::from_secs(10);

    /// Consecutive quiet worker cycles that define quiescence: the return window
    /// in ticks, plus a cycle to acknowledge a topology stamp, a cycle to apply
    /// the switch, and slack. A settled system must be settled, not mid-lag.
    const QUIET_TICKS: usize = 14;

    /// The longest wall clock one worker cycle can span, and therefore the
    /// longest stretch of request traffic that can land inside one. A cycle
    /// serially awaits: one probe per bucket (src/worker.rs:148-190), the
    /// caught-up LIST against each peer (src/worker.rs:398-425), and topology
    /// verification for each recovered bucket (src/worker.rs:289-331) — every
    /// one of them bounded by `IO_TIMEOUT`. Capping simulated intra-cycle time
    /// at that bound is what keeps the walk from inventing schedules the worker
    /// cannot produce.
    fn worst_case_cycle_span(bucket_count: usize) -> Duration {
        IO_TIMEOUT * (2 * bucket_count as u32 + 1)
    }

    /// SplitMix64. Inlined rather than taking a `rand` dev-dependency for ten
    /// lines of arithmetic; the constants are the reference ones, so a seed
    /// reproduces a walk on any platform.
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        /// Uniform in `0..n`. The modulo bias over a 64-bit draw is irrelevant
        /// at the sizes used here (n < 2000).
        fn below(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }
    }

    /// One walk step. Faults, uploads and drains are *injected* events; cycles
    /// and probes are the system running.
    #[derive(Clone, Copy, Debug)]
    enum Step {
        /// Flip a bucket's ground-truth reachability. Invisible to the
        /// controller until something observes it.
        Fault { bucket: usize },
        /// Flip whether a reachable bucket's topology stamp (or storage format)
        /// fails the worker's check: the bucket answers, but no ack comes, so
        /// it stays pending for as long as the walk likes.
        Mismatch { bucket: usize },
        /// One data-plane observation, reporting ground truth — what
        /// `ObservedStorage` feeds `health.observe` from real request and worker
        /// I/O (src/observed_storage.rs:55-61). `after_ms` is the wall clock it
        /// lands at, because `HealthController::observe` stamps `Instant::now()`;
        /// the walk clamps it so a cycle never spans more than
        /// [`worst_case_cycle_span`].
        Probe { bucket: usize, after_ms: u64 },
        /// One full-cadence worker cycle: probe every bucket, then maintain
        /// selection. `racing` is what lands *inside* the cycle — see [`Racing`].
        Tick {
            racing: Option<Racing>,
            after_snapshot: bool,
        },
        /// Idle-cadence cycles: `secs` worker cycles that maintain selection
        /// without probing. Probing is traffic-gated; maintenance is not, so
        /// time never passes in this system without maintenance running
        /// (src/worker.rs:216-222). This is how return windows elapse.
        Advance { secs: u64 },
        /// An upload fans out. A peer that is not eligible is skipped and owed
        /// a durable `_repl/` note before the ack (src/replicate.rs:2136,
        /// :2172-2176).
        Upload,
        /// One sweep pass drains a note. The walk may withhold drains for as
        /// long as it likes: an undrainable note is a reachable state.
        Drain,
    }

    /// What lands between a worker cycle's caught-up determination and the gate
    /// evaluation that consumes it. `region_bucket_caught_up` LISTs each peer in
    /// sequence, every one bounded by `IO_TIMEOUT` (src/worker.rs:398-425), so
    /// ordinary request-path observations and fan-outs interleave there by
    /// construction and the verdict is already up to a cycle old when it is
    /// used. That staleness is inherent to the design, not a defect; the walk
    /// models it so the oracles are checked against it rather than around it.
    ///
    /// `Flap` and `after_snapshot` reach the other window: between the snapshot
    /// and the publication of the switch it proposed, where the worker awaits
    /// topology checks (src/worker.rs). A bucket that fails and recovers there
    /// must neither have the stale switch published onto it nor its new
    /// recovery cleared by the check already in flight.
    #[derive(Clone, Copy, Debug)]
    enum Racing {
        Probe(usize),
        Upload,
        /// The bucket goes down long enough to cross the leave threshold and
        /// comes back up.
        Flap(usize),
    }

    /// Ground truth plus the real controller, driven together.
    struct Walk {
        seed: u64,
        rng: Rng,
        controller: HealthController,
        bucket_count: usize,
        /// The node's region bucket — the one the read pin prefers.
        region: usize,
        leave_after_failures: u32,
        /// How the boot in [`Walk::new`] came out, for the failure report.
        boot: String,
        /// Ground-truth reachability. Only [`Step::Fault`] and
        /// [`Racing::Flap`] change it.
        up: Vec<bool>,
        /// Ground truth for the worker's topology/format check of a reachable
        /// bucket: `true` fails it. Only [`Step::Mismatch`] changes it.
        mismatched: Vec<bool>,
        /// Reachability as the startup topology report captured it, BEFORE the
        /// boot flap below could move it. The pin-seeding decision reads this
        /// snapshot rather than live health, because startup's does too
        /// (`topology.unreachable_indices`, src/app.rs:1194).
        boot_up: Vec<bool>,
        /// Outstanding `_repl/` repair notes the region bucket is owed, by id.
        /// Ground truth for the worker's caught-up determination.
        notes: Vec<u64>,
        /// Next note id. Ids are issued in order, so comparing one against
        /// [`Walk::admitted_at`] says whether a note predates the read pin.
        next_note: u64,
        /// The note counter as it stood when the read pin last landed on the
        /// region bucket. Every outstanding note below this watermark was owed
        /// before reads were admitted, which is the debt the drain gate exists
        /// to refuse. This is the design's own boundary, not a concession: the
        /// gate governs the RETURN of reads to a lagging bucket, never their
        /// retention ([`next_read_pin`]) — and the read
        /// path falls through to the write bucket for an artifact the read
        /// bucket does not hold, so a note taken on with reads already in place
        /// costs a fall-through, not a wrong answer.
        admitted_at: u64,
        /// Whether the node maintains a DISTINCT read pin at all. Until this is
        /// set, `BucketSet::read_pin` aliases the write pin (src/buckets.rs:247-256),
        /// so the controller's `applied_read_selected` is a belief, not what is
        /// served. Startup only activates it when the region bucket is converged
        /// (`seed_read_pin`, src/app.rs:1199-1200); otherwise the first
        /// `switch_read` the worker applies does (src/worker.rs:362-363).
        read_active: bool,
        now: Instant,
        /// Simulated time spent inside the current worker cycle, reset by every
        /// cycle and capped at [`worst_case_cycle_span`].
        cycle_elapsed: Duration,
        trace: Vec<Step>,
        /// How often each read-pin transition in [`Edge`] was taken.
        coverage: [u64; EDGES],
    }

    /// The read-pin transitions the walk must take, so a schedule generator
    /// that drifts away from one cannot pass silently. `DemoteAcrossAFlap` is
    /// a demotion whose step also brought the bucket back, stamp unverified.
    #[derive(Clone, Copy, Debug)]
    enum Edge {
        Earn,
        DemoteUnhealthy,
        DemoteAcrossAFlap,
        EarnedAcrossWriteMove,
        LoanEndsWithTheWritePin,
    }
    const EDGES: usize = 5;

    /// What a transition is classified from, taken before and after each step.
    #[derive(Clone, Copy)]
    struct PinView {
        pin: ReadPin,
        selected: usize,
        unhealthy: bool,
    }

    impl Walk {
        /// Build a controller in a state the real startup can actually produce.
        ///
        /// This mirrors `app::serve`'s boot, in order, because the entry state is
        /// where a walk is easiest to get wrong: a fresh `HealthController` with
        /// a read pin seeded onto a never-observed bucket looks harmless and is
        /// unreachable, and it manufactures "defects" out of the resulting
        /// `Unknown -> Healthy` stamp raise. Real boot has already done that
        /// transition and acknowledged it before any pin is seeded.
        fn new(seed: u64) -> Self {
            let mut rng = Rng(seed);
            // Topology varies per seed: two and three buckets, region at any
            // index (including index 0, where the region bucket is also the
            // write home), and every leave threshold the fixtures ship with.
            let bucket_count = 2 + rng.below(2) as usize;
            let region = rng.below(bucket_count as u64) as usize;
            let leave_after_failures = 1 + rng.below(3) as u32;

            let policy = HealthPolicy::new(leave_after_failures, RETURN_WINDOW).unwrap();
            let controller = HealthController::new(bucket_count, policy).unwrap();
            let now = Instant::now();

            // Which buckets startup could reach. An unreachable one lands in
            // `topology.unreachable_indices` (src/buckets.rs:446-522).
            let up: Vec<bool> = (0..bucket_count).map(|_| rng.below(8) != 0).collect();
            // A region bucket that is behind at boot: `region_owed_no_notes`
            // reads these and refuses the pin (src/app.rs:1195-1196).
            let boot_notes = if rng.below(3) == 0 {
                1 + rng.below(3)
            } else {
                0
            };

            let mut walk = Self {
                seed,
                rng,
                controller,
                bucket_count,
                region,
                leave_after_failures,
                boot: String::new(),
                boot_up: up.clone(),
                mismatched: vec![false; bucket_count],
                up,
                notes: (0..boot_notes).collect(),
                next_note: boot_notes,
                admitted_at: 0,
                read_active: false,
                now,
                cycle_elapsed: Duration::ZERO,
                trace: Vec::new(),
                coverage: [0; EDGES],
            };

            // 1. Storages are wrapped in `ObservedStorage` BEFORE any I/O
            //    (src/app.rs:1083-1093), so `verify_format` and
            //    `verify_topology_with` (src/app.rs:1111-1124) report every call
            //    they make. A reachable bucket is therefore observed Success —
            //    which takes it Unknown -> Healthy and raises its topology stamp.
            for index in 0..bucket_count {
                if walk.up[index] {
                    walk.observe(index);
                }
            }
            // 2. Startup acknowledges every stamp it just raised, for the
            //    verified and stamped buckets alike (src/app.rs:1126-1130). This
            //    is the step whose absence invents a boot-time topology block.
            for index in 0..bucket_count {
                if walk.up[index] {
                    walk.controller.topology_revalidated(index).unwrap();
                }
            }
            // 3. A confirmed-unreachable bucket is collapsed straight through
            //    the leave threshold rather than waiting out hysteresis
            //    (src/app.rs:1132-1137).
            for index in 0..bucket_count {
                if !walk.up[index] {
                    for _ in 0..leave_after_failures {
                        walk.controller
                            .observe_at(index, BucketSignal::ConnectionFailure, walk.now)
                            .unwrap();
                    }
                }
            }
            // 4. One tick applies the resulting write selection (src/app.rs:1139-1148).
            let initial = walk.controller.worker_tick_at(walk.now, false);
            if let Some(change) = initial.selection_change {
                walk.controller.apply_selection(change.to, || ());
            }

            // 5. Between that tick and the pin seeding, startup runs
            //    `build_copy_matrix` (src/app.rs:1167) and `node_region::detect`
            //    (src/app.rs:1178) — both awaits, the first doing per-pair I/O.
            //    Almost none of that I/O is observed: `verify_copy_cell`
            //    (src/buckets.rs:890-910) drives `server_side_copy` and
            //    `stored_size`, which `ObservedStorage` forwards unobserved, and
            //    the ONE observed op per cell is the `delete_keys` cleanup at
            //    src/buckets.rs:901. The region bucket is a copy DESTINATION once
            //    per other reachable bucket, so this window can supply it at most
            //    `reachable - 1` availability failures — not an unbounded budget.
            //    That bound is the whole point: a region bucket only reaches
            //    `Unhealthy` here when its leave threshold is at or below it.
            let reachable_count = walk.boot_up.iter().filter(|up| **up).count();
            let flap_budget = reachable_count.saturating_sub(1) as u32;
            let flap = flap_budget > 0 && walk.rng.below(3) == 0;
            let mut flap_healed = false;
            if flap {
                let failures = 1 + walk.rng.below(flap_budget as u64) as u32;
                walk.up[region] = false;
                for _ in 0..failures {
                    walk.observe(region);
                }
                // The same window can also see it come BACK. That heal is an
                // `Unhealthy -> Healthy` transition, so it raises a fresh
                // topology stamp — and the boot ack loop in step 2 has already
                // run, so nothing re-acks it until the worker's first cycle
                // (src/worker.rs:308). The pin is then seeded onto a bucket that
                // is Healthy but topology-blocked.
                flap_healed = walk.rng.below(2) == 0;
                if flap_healed {
                    walk.up[region] = true;
                    walk.observe(region);
                }
            }

            // 6. Seed the read pin exactly as src/app.rs:1191-1198 does.
            //    `reachable` comes from the topology report; `converged` is
            //    `region_owed_no_notes` (src/replicate.rs:2570-2584), which is
            //    conservative in a way that matters enormously here: it LISTs
            //    every peer, and ANY peer that errors or is unreachable makes it
            //    return false. So a node whose write home is down at boot is
            //    always unconverged, whatever the region bucket itself holds.
            let write_index = walk.controller.lock().applied_selected;
            let reachable = walk.up_at_boot(region);
            let every_peer_answered =
                (0..bucket_count).all(|index| index == region || walk.up_at_boot(index));
            let converged = reachable && every_peer_answered && walk.notes.is_empty();
            let pinned = walk
                .controller
                .configure_read_affinity(region, converged)
                .unwrap();
            let read_index = if pinned { region } else { write_index };
            // Only a pin the controller kept earned gets the distinct read pin
            // turned on (`seed_read_pin`, src/app.rs). Anything else leaves
            // reads aliased to the write pin.
            if pinned {
                walk.read_active = true;
            }
            if read_index == region {
                walk.admitted_at = walk.next_note;
            }

            // 7. `initialize_indexes` (src/app.rs) runs before the worker starts
            //    and HEADs two keys on the write pin, both observed.
            walk.observe(write_index);

            walk.boot = format!(
                "up_at_boot={:?} boot_notes={} flap={} flap_healed={} reachable={} \
                 converged={} write_index={} read_index={} read_active={}",
                walk.boot_up_snapshot(),
                boot_notes,
                flap,
                flap_healed,
                reachable,
                converged,
                write_index,
                read_index,
                walk.read_active
            );
            walk
        }

        /// The reachability the topology report captured, which the pin-seeding
        /// decision reads — deliberately NOT live health, because startup's is
        /// not either (src/app.rs:1194 reads `topology.unreachable_indices`).
        fn up_at_boot(&self, index: usize) -> bool {
            self.boot_up.get(index).copied().unwrap_or(true)
        }

        fn boot_up_snapshot(&self) -> Vec<bool> {
            self.boot_up.clone()
        }

        /// Repair notes the region bucket already owed when reads were admitted
        /// to it and still owes now. Zero is the serving invariant.
        fn stale_debt(&self) -> usize {
            self.notes
                .iter()
                .filter(|id| **id < self.admitted_at)
                .count()
        }

        /// Whether background replication may touch a bucket — the exact
        /// predicate the fan-out consults before it decides to copy or to owe a
        /// note (`replicate::bucket_eligible`, src/replicate.rs:93-98).
        fn eligible(&self, index: usize) -> bool {
            self.controller.bucket_eligible(index).unwrap_or(false)
        }

        fn apply(&mut self, step: Step) {
            self.trace.push(step);
            let before = self.pin_view();
            match step {
                Step::Fault { bucket } => self.up[bucket] = !self.up[bucket],
                Step::Mismatch { bucket } => self.mismatched[bucket] = !self.mismatched[bucket],
                Step::Probe { bucket, after_ms } => {
                    // Clamped, not trusted: a request observation may only push
                    // the clock as far as the current cycle can still run.
                    let budget =
                        worst_case_cycle_span(self.bucket_count).saturating_sub(self.cycle_elapsed);
                    let advance = Duration::from_millis(after_ms).min(budget);
                    self.now += advance;
                    self.cycle_elapsed += advance;
                    self.observe(bucket);
                }
                Step::Tick {
                    racing,
                    after_snapshot,
                } => self.worker_cycle(true, racing, after_snapshot),
                Step::Advance { secs } => {
                    for _ in 0..secs {
                        self.worker_cycle(false, None, false);
                    }
                }
                Step::Upload => self.upload(),
                Step::Drain => {
                    // The sweep only runs against eligible buckets
                    // (src/replicate.rs:2490). Oldest first, like a paged sweep.
                    if !self.notes.is_empty() && self.eligible(self.region) {
                        self.notes.remove(0);
                    }
                }
            }
            self.check_earned_invariant(step);
            self.record_edge(before, self.pin_view());
        }

        fn pin_view(&self) -> PinView {
            let state = self.controller.lock();
            PinView {
                pin: state.health.read_pin,
                selected: state.health.selected,
                unhealthy: state.health.buckets[self.region].state == HealthState::Unhealthy,
            }
        }

        fn record_edge(&mut self, before: PinView, after: PinView) {
            let edge = match (before.pin, after.pin) {
                (ReadPin::FollowWrite, ReadPin::Earned) => Edge::Earn,
                (ReadPin::Earned, ReadPin::FollowWrite) if after.unhealthy => Edge::DemoteUnhealthy,
                (ReadPin::Earned, ReadPin::FollowWrite) => Edge::DemoteAcrossAFlap,
                (ReadPin::Earned, ReadPin::Earned) if before.selected != after.selected => {
                    Edge::EarnedAcrossWriteMove
                }
                (ReadPin::FollowWrite, ReadPin::FollowWrite)
                    if before.selected == self.region && after.selected != self.region =>
                {
                    Edge::LoanEndsWithTheWritePin
                }
                _ => return,
            };
            self.coverage[edge as usize] += 1;
        }

        /// One upload's fan-out. The fan-out skips an ineligible peer outright
        /// and owes it a note before the ack.
        ///
        /// The other producer of notes is deliberately NOT modelled: a copy that
        /// is attempted and fails — the grace deadline or a `Deferred` merge
        /// verdict (src/replicate.rs:2143-2158) — writes a note with no health
        /// observation behind it, so the region bucket can owe one while health
        /// still reads it as fine. Two reasons it stays out. It is bounded: the
        /// next caught-up LIST sees the note, so the exposure is under one worker
        /// cycle. And it can only ever create a note while reads are ALREADY on
        /// the region bucket, which is retention rather than return — a
        /// fall-through to the write bucket, not a wrong answer (see
        /// [`Walk::admitted_at`]). Modelling it would make the serving oracle
        /// assert a rule the design does not claim.
        fn upload(&mut self) {
            if !self.eligible(self.region) {
                self.notes.push(self.next_note);
                self.next_note += 1;
            }
        }

        /// One data-plane observation reporting ground truth.
        fn observe(&mut self, bucket: usize) {
            let signal = if self.up[bucket] {
                BucketSignal::Success
            } else {
                BucketSignal::Timeout
            };
            self.controller
                .observe_at(bucket, signal, self.now)
                .unwrap();
        }

        /// One worker loop iteration: sleep, probe every bucket if the cadence
        /// calls for it, then maintain selection (src/worker.rs:203-227). Only
        /// the probe is traffic-gated; the maintenance below runs every cycle,
        /// which is why wall-clock time cannot pass here without it.
        fn worker_cycle(&mut self, probe: bool, racing: Option<Racing>, after_snapshot: bool) {
            self.now += TICK;
            self.cycle_elapsed = Duration::ZERO;
            // `probe_buckets` (src/worker.rs:148, called at :219).
            if probe {
                for bucket in 0..self.bucket_count {
                    self.observe(bucket);
                }
            }
            self.maintain_bucket_selection(racing, after_snapshot);
        }

        /// `worker::maintain_bucket_selection` (src/worker.rs:261-375), step for
        /// step. The one-cycle lag between what the controller wants and what the
        /// worker has applied is a property of this ordering, not a simulation
        /// of one: `read_return_pending` reads the *applied* pin, and the acks
        /// happen after the snapshot that produced them.
        fn maintain_bucket_selection(&mut self, racing: Option<Racing>, after_snapshot: bool) {
            // The caught-up LIST runs only while a return is pending, and its
            // verdict feeds this one tick.
            let caught_up = match self.controller.read_return_pending() {
                // `region_bucket_caught_up` (src/worker.rs) asks ground truth:
                // does any peer still hold a `_repl/` note owed to the region
                // bucket? The real one additionally answers false when a peer is
                // unreachable; modelling only the note count makes the gate
                // strictly *more* permissive, which is the direction that
                // exposes over-admission.
                Some(_) => self.notes.is_empty(),
                None => false,
            };

            // The caught-up LIST and the topology checks are sequential network
            // I/O. Whatever the fleet does while one is in flight lands here.
            if !after_snapshot {
                self.race(racing);
            }
            let snapshot = self.controller.worker_tick_at(self.now, caught_up);
            if after_snapshot {
                self.race(racing);
            }

            // A recovered bucket's topology stamp is verified before anything
            // may select it; an unreachable one blocks this cycle's switch and
            // feeds back an availability failure, a mismatched one just blocks.
            let mut selection_blocked = HashSet::new();
            for index in &snapshot.topology_revalidation {
                if !self.up[*index] {
                    self.controller
                        .observe_at(*index, BucketSignal::Timeout, self.now)
                        .unwrap();
                    selection_blocked.insert(*index);
                } else if self.mismatched[*index] {
                    selection_blocked.insert(*index);
                } else {
                    self.controller.topology_check_passed(*index).unwrap();
                }
            }

            if let Some(change) = snapshot.selection_change {
                if !selection_blocked.contains(&change.to) {
                    self.controller.apply_selection(change.to, || ());
                }
            }

            if let Some(change) = snapshot.read_selection_change {
                if !selection_blocked.contains(&change.to)
                    && self
                        .controller
                        .apply_read_selection(change.to, || ())
                        .is_some()
                {
                    // `switch_read` activates the distinct read pin the first
                    // time it runs (src/buckets.rs).
                    self.read_active = true;
                    // Reads just landed on the region bucket. Everything it owes
                    // from here on is debt taken on with reads already in place;
                    // everything below the watermark is debt the gate let them
                    // walk past.
                    if change.to == self.region {
                        self.admitted_at = self.next_note;
                    }
                }
            }
        }

        /// What lands inside a worker cycle's I/O, bounded by one `IO_TIMEOUT`.
        fn race(&mut self, racing: Option<Racing>) {
            let Some(racing) = racing else {
                return;
            };
            let advance = IO_TIMEOUT
                .min(worst_case_cycle_span(self.bucket_count).saturating_sub(self.cycle_elapsed));
            self.now += advance;
            self.cycle_elapsed += advance;
            match racing {
                Racing::Probe(bucket) => self.observe(bucket),
                Racing::Upload => self.upload(),
                Racing::Flap(bucket) => {
                    self.up[bucket] = false;
                    for _ in 0..self.leave_after_failures {
                        self.observe(bucket);
                    }
                    self.up[bucket] = true;
                    self.observe(bucket);
                }
            }
        }

        /// Oracle 1, checked after every single step: an earned read pin only
        /// ever sits on a usable region bucket. One that survives the bucket
        /// going Unhealthy or topology-pending is reads held on a bucket that
        /// may be owed repair notes, with nothing left to move them.
        fn check_earned_invariant(&self, step: Step) {
            let state = self.controller.lock();
            if state.health.read_pin == ReadPin::Earned && !state.health.usable(self.region) {
                let status = state.health.buckets[self.region].clone();
                drop(state);
                self.fail(
                    "EARNED",
                    &format!("the read pin is earned on an unusable region bucket {status:?} (after {step:?})"),
                );
            }
        }

        /// Run to quiescence and check the settled-state oracles. Quiescence is
        /// deliberate: both shipped defects were persistent-state latches, and
        /// the design has inherent one-cycle lags that a mid-flight check would
        /// report as violations.
        fn quiesce(&mut self) {
            for _ in 0..QUIET_TICKS {
                self.apply(Step::Tick {
                    racing: None,
                    after_snapshot: false,
                });
            }

            let state = self.controller.lock();
            let applied_write = state.applied_selected;
            // What is actually SERVED, which is the only thing an oracle about
            // serving may look at: the distinct read pin if the node has one,
            // otherwise the write pin it aliases (src/buckets.rs:247-256).
            let applied_read = if self.read_active {
                state.applied_read_selected
            } else {
                applied_write
            };
            let region_status = state.health.buckets[self.region].clone();
            let region_blocked = region_status.topology != Topology::Verified;
            drop(state);

            // Oracle 2 — SERVING. Reads may only be served from the region
            // bucket, while some *other* bucket takes the writes, once the debt
            // it owed when they were admitted has drained. When the region
            // bucket is itself the write selection the question is moot: it is
            // truth by definition, and a dead alternative serves nothing.
            let stale_debt = self.stale_debt();
            if applied_read == self.region && self.region != applied_write && stale_debt != 0 {
                self.fail(
                    "SERVING",
                    &format!(
                        "reads are served from the region bucket {} though {stale_debt} repair \
                         note(s) it already owed when they were admitted are still outstanding, \
                         and writes go to bucket {applied_write}",
                        self.region
                    ),
                );
            }

            // Oracle 3 — LIVENESS. The mirror image, and the reason a fix
            // cannot simply demote harder: a region bucket that is reachable,
            // has been continuously healthy for the full return window, is
            // topology-validated and owes nothing must actually be serving the
            // reads it exists to serve.
            let healthy_for_window = region_status.state == HealthState::Healthy
                && region_status.healthy_since.is_some_and(|since| {
                    self.now.saturating_duration_since(since) >= RETURN_WINDOW
                });
            if self.up[self.region]
                && self.notes.is_empty()
                && healthy_for_window
                && !region_blocked
                && applied_read != self.region
            {
                self.fail(
                    "LIVENESS",
                    &format!(
                        "reads are stranded on bucket {applied_read} though region bucket {} is \
                         healthy for the full window, validated, and owes nothing",
                        self.region
                    ),
                );
            }
        }

        /// Report a violation the way a simulator does: the seed alone
        /// reproduces it, and the trace says how without a re-run.
        fn fail(&self, oracle: &str, detail: &str) -> ! {
            let mut report = String::new();
            let _ = writeln!(report, "\nread-pin conformance walk violated {oracle}");
            let _ = writeln!(report, "  reproduce: PYPIRON_WALK_SEEDS=1 PYPIRON_WALK_START={} cargo test --lib read_pin_conformance_walk_deep -- --ignored --nocapture", self.seed);
            let _ = writeln!(report, "  seed:      {}", self.seed);
            let _ = writeln!(
                report,
                "  topology:  {} buckets, region={}, leave_after_failures={}",
                self.bucket_count, self.region, self.leave_after_failures
            );
            let _ = writeln!(report, "  boot:      {}", self.boot);
            let _ = writeln!(report, "  step:      {}", self.trace.len());
            let _ = writeln!(report, "  detail:    {detail}");
            let state = self.controller.lock();
            let _ = writeln!(
                report,
                "  state:     notes={:?} admitted_at={} stale_debt={} up={:?} states={:?} \
                 applied_write={} applied_read={} health.selected={} read_pin={:?} \
                 region_topology={:?} mismatched={:?} read_active={}",
                self.notes,
                self.admitted_at,
                self.stale_debt(),
                self.up,
                state
                    .health
                    .buckets
                    .iter()
                    .map(|bucket| bucket.state)
                    .collect::<Vec<_>>(),
                state.applied_selected,
                state.applied_read_selected,
                state.health.selected,
                state.health.read_pin,
                state.health.buckets[self.region].topology,
                self.mismatched,
                self.read_active,
            );
            drop(state);
            let _ = writeln!(report, "  trace:");
            for (index, step) in self.trace.iter().enumerate() {
                let _ = writeln!(report, "    {index:>4}: {step:?}");
            }
            panic!("{report}");
        }

        /// Draw one step from the request-path-only alphabet: what happens while
        /// a worker cycle is running. Drawing these in runs rather than one at a
        /// time is what makes the intra-cycle windows reachable at all — but the
        /// run is bounded by [`worst_case_cycle_span`], so it can only ever
        /// describe traffic a real cycle could have overlapped.
        fn draw_inside_a_cycle(&mut self) -> Step {
            // Weighted toward the region bucket: it is the subject of every
            // invariant here, so a uniform bucket choice spends most of the draw
            // on buckets the read pin does not turn on.
            let bucket = if self.rng.below(2) == 0 {
                self.region
            } else {
                self.rng.below(self.bucket_count as u64) as usize
            };
            match self.rng.below(100) {
                0..=39 => Step::Probe {
                    bucket,
                    after_ms: self.rng.below(1_200),
                },
                40..=64 => Step::Fault { bucket },
                65..=79 => Step::Upload,
                _ => Step::Drain,
            }
        }

        /// Draw one weighted random step.
        fn draw(&mut self) -> Step {
            let bucket = self.rng.below(self.bucket_count as u64) as usize;
            match self.rng.below(100) {
                0..=19 => Step::Tick {
                    racing: None,
                    after_snapshot: false,
                },
                20..=29 => Step::Tick {
                    racing: Some(match self.rng.below(4) {
                        0 => Racing::Upload,
                        1 => Racing::Flap(bucket),
                        _ => Racing::Probe(bucket),
                    }),
                    after_snapshot: self.rng.below(2) == 0,
                },
                30..=49 => Step::Probe {
                    bucket,
                    after_ms: self.rng.below(1_200),
                },
                50..=64 => Step::Fault { bucket },
                65..=66 => Step::Mismatch { bucket },
                67..=74 => Step::Advance {
                    secs: 1 + self.rng.below(12),
                },
                75..=84 => Step::Upload,
                // Drains outweigh uploads on purpose. Notes that only ever
                // accumulate keep the caught-up verdict false forever, and a
                // gate that never opens exercises nothing: the interesting
                // states are on both sides of zero outstanding notes.
                _ => Step::Drain,
            }
        }

        fn run(&mut self, steps: usize) {
            // Quiescence is scheduled rather than waited for: fourteen
            // consecutive quiet cycles never come up by chance, which would
            // leave the settled-state oracles checking nothing.
            let mut until_quiesce = 1 + self.rng.below(24);
            let mut inside_cycle = 0u64;
            while self.trace.len() < steps {
                let step = if inside_cycle > 0
                    && self.cycle_elapsed < worst_case_cycle_span(self.bucket_count)
                {
                    inside_cycle -= 1;
                    self.draw_inside_a_cycle()
                } else {
                    inside_cycle = 0;
                    let drawn = self.draw();
                    // One worker cycle in three runs long. Idle-cadence cycles
                    // count: `Advance` is where a return window finishes
                    // elapsing, so it is exactly where a long cycle catches the
                    // read pin mid-move.
                    let is_cycle = matches!(drawn, Step::Tick { .. } | Step::Advance { .. });
                    if is_cycle && self.rng.below(3) == 0 {
                        inside_cycle = 3 + self.rng.below(9);
                    }
                    drawn
                };
                self.apply(step);
                until_quiesce -= 1;
                if until_quiesce == 0 {
                    self.quiesce();
                    until_quiesce = 1 + self.rng.below(24);
                    inside_cycle = 0;
                }
            }
            // Always finish settled, so the last steps of every walk are checked
            // by the settled-state oracles rather than only the earned invariant.
            self.quiesce();
        }
    }

    /// Run one walk and return how often it took each read-pin transition.
    fn walk(seed: u64, steps: usize) -> [u64; EDGES] {
        let mut walk = Walk::new(seed);
        walk.run(steps);
        walk.coverage
    }

    /// Run a seed range and fail unless every read-pin transition was taken:
    /// an oracle over a schedule that never demotes proves nothing.
    fn walk_range(seeds: std::ops::Range<u64>, steps: usize) {
        let mut total = [0u64; EDGES];
        for seed in seeds {
            for (sum, taken) in total.iter_mut().zip(walk(seed, steps)) {
                *sum += taken;
            }
        }
        for edge in [
            Edge::Earn,
            Edge::DemoteUnhealthy,
            Edge::DemoteAcrossAFlap,
            Edge::EarnedAcrossWriteMove,
            Edge::LoanEndsWithTheWritePin,
        ] {
            assert!(
                total[edge as usize] > 0,
                "the walk never took {edge:?}: {total:?}"
            );
        }
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    /// Fast tier: a fixed seed set on every `cargo test`. Sized to stay well
    /// under a second so it is never the reason someone skips the suite. That
    /// budget buys the shallow defect classes; the deep tier below is where the
    /// long interleavings live (dev/TESTING.md).
    #[test]
    fn read_pin_conformance_walk() {
        walk_range(0..256, 400);
    }

    /// Deep tier: the nightly volume, run from
    /// `.github/workflows/simulation.yml`. `PYPIRON_WALK_SEEDS`,
    /// `PYPIRON_WALK_START` and `PYPIRON_WALK_STEPS` size and stride it; the
    /// defaults are a useful local soak on their own.
    #[test]
    #[ignore = "deep tier: minutes, nightly only (PYPIRON_WALK_SEEDS/_START/_STEPS)"]
    fn read_pin_conformance_walk_deep() {
        let start = env_u64("PYPIRON_WALK_START", 0);
        let seeds = env_u64("PYPIRON_WALK_SEEDS", 20_000);
        let steps = env_usize("PYPIRON_WALK_STEPS", 400);
        walk_range(start..start.saturating_add(seeds), steps);
        println!("read-pin conformance walk: {seeds} seeds x {steps} steps from {start}, clean");
    }
}
