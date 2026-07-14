//! Per-node bucket health and selection.
//!
//! The async worker owns probes and real-traffic error mapping. This module is
//! deliberately synchronous: feed it classified signals plus a caller-supplied
//! [`Instant`], then apply the returned transition. That keeps timing,
//! hysteresis, and preference policy deterministic and unit-testable without a
//! mock runtime.

use std::collections::BTreeSet;
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

/// Effects for the async caller to apply after an observation or timer tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthUpdate {
    /// The selected *write* bucket after applying this update, changed or not.
    pub selected_index: usize,
    /// Present only when the write selection changed.
    pub selection_change: Option<SelectionChange>,
    /// The selected *read* bucket after applying this update (equals
    /// `selected_index` when the node has no region read preference).
    pub read_selected_index: usize,
    /// Present only when the read selection changed.
    pub read_selection_change: Option<SelectionChange>,
    /// Buckets that became reachable and must have their topology stamp checked
    /// before writes rely on them. One observation produces at most one entry;
    /// a vector keeps the wiring ready for batched probes without changing API.
    pub topology_revalidation: Vec<usize>,
}

impl HealthUpdate {
    #[cfg(test)]
    pub fn has_transition(&self) -> bool {
        self.selection_change.is_some() || !self.topology_revalidation.is_empty()
    }
}

#[derive(Clone, Debug)]
struct BucketStatus {
    state: HealthState,
    consecutive_failures: u32,
    healthy_since: Option<Instant>,
}

impl BucketStatus {
    fn unknown() -> Self {
        Self {
            state: HealthState::Unknown,
            consecutive_failures: 0,
            healthy_since: None,
        }
    }

    fn selected() -> Self {
        Self {
            state: HealthState::Healthy,
            consecutive_failures: 0,
            healthy_since: None,
        }
    }
}

/// Per-node health view and selected bucket.
///
/// Bucket zero starts selected. Other buckets start unknown and become eligible
/// after a successful probe or operation. A selected bucket is abandoned only
/// after its configured failure streak. A healthy selected bucket returns to a
/// more-preferred one only after that bucket has stayed continuously healthy for
/// the configured duration.
pub struct BucketHealth {
    policy: HealthPolicy,
    buckets: Vec<BucketStatus>,
    selected: usize,
    /// This node's region bucket, when one was matched at startup. `None` (no
    /// region, unlabelled buckets, or single-bucket) means reads always follow
    /// the write selection — behaviorally identical to a node with no affinity.
    read_preference: Option<usize>,
    /// The bucket reads should be served from: the region bucket while it is
    /// usable, otherwise the write selection (dev/READ_AFFINITY_VISION.md).
    read_selected: usize,
    /// Worker-supplied: whether the region bucket currently holds no undrained
    /// replication notes. Gates *return* of reads to the region bucket; a stale
    /// value can only keep reads on the write bucket, never send them to a
    /// lagging one. The request path never sets this.
    region_caught_up: bool,
}

/// One worker-cycle view of health observed by request and worker threads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerHealthSnapshot {
    pub selected_index: usize,
    /// Coalesces observations into the one switch the worker needs to apply.
    /// Repeated until [`HealthController::selection_applied`] acknowledges it.
    pub selection_change: Option<SelectionChange>,
    /// The read bucket this node should serve reads from (equals
    /// `selected_index` when the node has no region read preference).
    pub read_selected_index: usize,
    /// The one read-pin switch the worker needs to apply, repeated until
    /// [`HealthController::read_selection_applied`] acknowledges it.
    pub read_selection_change: Option<SelectionChange>,
    pub states: Vec<HealthState>,
    /// Sorted, deduplicated, and repeated until acknowledged. The worker must
    /// validate these topology stamps before it applies a selection that depends
    /// on a recovered bucket.
    pub topology_revalidation: Vec<usize>,
    /// Ignored/alarm observations since the previous worker tick, per bucket.
    /// These never affect selection; they exist for logs and metrics.
    pub alarms: Vec<u64>,
}

struct ControllerState {
    health: BucketHealth,
    applied_selected: usize,
    /// The read bucket the worker has actually applied to the [`BucketSet`]. Like
    /// `applied_selected`, a read selection is repeated in every snapshot until
    /// [`HealthController::read_selection_applied`] acknowledges it.
    applied_read_selected: usize,
    topology_revalidation: BTreeSet<usize>,
    /// Recovered buckets remain ineligible until their topology stamp passes.
    /// This must live beside the selector: a worker-local skip leaves the pure
    /// state machine pointed at the rejected candidate and strands later
    /// failover when the actually selected bucket dies.
    topology_blocked: BTreeSet<usize>,
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
                topology_revalidation: BTreeSet::new(),
                topology_blocked: BTreeSet::new(),
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

    /// Advance return hysteresis and snapshot pending work. Storage switching
    /// and topology validation happen after this returns and remain pending until
    /// their acknowledgement methods succeed.
    pub fn worker_tick(&self) -> WorkerHealthSnapshot {
        self.worker_tick_at(Instant::now())
    }

    #[cfg(test)]
    pub fn health_state(&self, index: usize) -> Result<HealthState, InvalidBucket> {
        self.validate_bucket(index)?;
        Ok(self
            .lock()
            .health
            .state(index)
            .unwrap_or(HealthState::Unknown))
    }

    /// Whether background data-plane maintenance may touch this bucket.
    ///
    /// `Unknown` is eligible when no topology validation is pending: startup
    /// verifies the configured topology before the worker starts, and an idle
    /// member may not yet have produced an ordinary health observation. A bucket
    /// that crossed through `Unhealthy`, however, is blocked on its first success
    /// until the recovered topology stamp is explicitly acknowledged.
    pub fn bucket_eligible(&self, index: usize) -> Result<bool, InvalidBucket> {
        let state = self.lock();
        let bucket = state.health.buckets.get(index).ok_or(InvalidBucket {
            index,
            bucket_count: state.health.buckets.len(),
        })?;
        Ok(bucket.state != HealthState::Unhealthy && !state.topology_blocked.contains(&index))
    }

    /// Confirm that the worker applied the desired selection. Until this ack,
    /// every snapshot repeats the transition so a failed topology check or
    /// storage switch cannot lose it.
    pub fn selection_applied(&self, index: usize) -> Result<(), InvalidBucket> {
        self.validate_bucket(index)?;
        self.lock().applied_selected = index;
        Ok(())
    }

    /// Seed this node's region read preference and where its read pin starts.
    /// Called once at startup; a node with no region match never calls this and
    /// its read selection stays equal to the write selection forever.
    pub fn configure_read_affinity(
        &self,
        preference: usize,
        read_selected: usize,
    ) -> Result<(), InvalidBucket> {
        self.validate_bucket(preference)?;
        self.validate_bucket(read_selected)?;
        let mut state = self.lock();
        state.health.set_read_affinity(preference, read_selected);
        state.applied_read_selected = read_selected;
        Ok(())
    }

    /// Whether this node has a region read preference at all. False for a node
    /// that matched no bucket, so the worker skips read-affinity maintenance
    /// entirely (its reads follow the write pin natively).
    pub fn has_read_preference(&self) -> bool {
        self.lock().health.read_preference.is_some()
    }

    /// Feed the worker's caught-up determination into the pure state machine.
    /// Only ever gates *return* of reads to the region bucket.
    pub fn set_region_caught_up(&self, caught_up: bool) {
        self.lock().health.region_caught_up = caught_up;
    }

    /// The region bucket when checking whether reads may return to it is
    /// worthwhile: a region preference exists, that bucket is Healthy and
    /// topology-eligible, and reads are not already served from it. `None`
    /// otherwise, so the worker runs the caught-up LIST only when it matters.
    pub fn read_return_pending(&self) -> Option<usize> {
        let state = self.lock();
        let region = state.health.read_preference?;
        if state.applied_read_selected == region {
            return None;
        }
        let healthy =
            state.health.buckets.get(region).map(|b| b.state) == Some(HealthState::Healthy);
        (healthy && !state.topology_blocked.contains(&region)).then_some(region)
    }

    /// Confirm that the worker applied the desired read selection.
    pub fn read_selection_applied(&self, index: usize) -> Result<(), InvalidBucket> {
        self.validate_bucket(index)?;
        self.lock().applied_read_selected = index;
        Ok(())
    }

    /// Confirm one recovered bucket's topology stamp. Pending validation is
    /// repeated in every snapshot until this ack, so transient failures retry.
    pub fn topology_revalidated(&self, index: usize) -> Result<(), InvalidBucket> {
        self.validate_bucket(index)?;
        let mut state = self.lock();
        state.topology_revalidation.remove(&index);
        state.topology_blocked.remove(&index);
        Ok(())
    }

    fn observe_at(
        &self,
        index: usize,
        signal: BucketSignal,
        now: Instant,
    ) -> Result<(), InvalidBucket> {
        let mut state = self.lock();
        let update = state.health.observe(index, signal, now)?;
        state
            .topology_revalidation
            .extend(update.topology_revalidation.iter().copied());
        state.topology_blocked.extend(update.topology_revalidation);
        if classify(signal) == SignalClass::Ignored {
            state.alarms[index] = state.alarms[index].saturating_add(1);
        }
        Ok(())
    }

    fn worker_tick_at(&self, now: Instant) -> WorkerHealthSnapshot {
        let mut state = self.lock();
        state.health.tick(now);

        // `BucketHealth` deliberately knows nothing about topology I/O. If it
        // chose a recovered-but-unvalidated candidate, resolve the desired
        // selection here and feed that choice back into the state machine.
        if state.topology_blocked.contains(&state.health.selected) {
            let fallback = state
                .health
                .buckets
                .iter()
                .enumerate()
                .find(|(index, bucket)| {
                    bucket.state == HealthState::Healthy && !state.topology_blocked.contains(index)
                })
                .map(|(index, _)| index)
                .unwrap_or(state.applied_selected);
            state.health.selected = fallback;
        }

        let selected_index = state.health.selected_index();
        let selection_change =
            (selected_index != state.applied_selected).then_some(SelectionChange {
                from: state.applied_selected,
                to: selected_index,
            });

        // Read selection: only a node with a region preference maintains a
        // distinct read pin. Without one, reads follow the write pin natively
        // (BucketSet::read_pin aliases pin), so never emit a read switch — that
        // would double-bump the shared generation on every write failover. With a
        // preference, take the pure machine's choice, but a topology-blocked
        // region bucket falls back to the write selection just like a write
        // candidate does above (a recovered stamp is not yet trustworthy).
        let (read_selected_index, read_selection_change) = if state.health.read_preference.is_some()
        {
            let mut index = state.health.read_selected;
            if state.topology_blocked.contains(&index) {
                index = selected_index;
            }
            let change = (index != state.applied_read_selected).then_some(SelectionChange {
                from: state.applied_read_selected,
                to: index,
            });
            (index, change)
        } else {
            (selected_index, None)
        };

        let topology_revalidation = state.topology_revalidation.iter().copied().collect();
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

        let mut buckets = vec![BucketStatus::unknown(); bucket_count];
        buckets[0] = BucketStatus::selected();
        Ok(Self {
            policy,
            buckets,
            selected: 0,
            read_preference: None,
            read_selected: 0,
            region_caught_up: false,
        })
    }

    pub fn selected_index(&self) -> usize {
        self.selected
    }

    #[cfg(test)]
    pub fn read_selected_index(&self) -> usize {
        self.read_selected
    }

    /// Seed the node's region read preference and the bucket reads start from.
    /// Called once at startup; `read_selected` is where the read pin was seeded
    /// (the region bucket if reachable, else the write selection).
    fn set_read_affinity(&mut self, preference: usize, read_selected: usize) {
        self.read_preference = Some(preference);
        self.read_selected = read_selected;
    }

    #[cfg(test)]
    fn state(&self, index: usize) -> Option<HealthState> {
        self.buckets.get(index).map(|bucket| bucket.state)
    }

    /// Apply one observation, then re-evaluate selection.
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
        let mut topology_revalidation = Vec::new();

        match classify(signal) {
            SignalClass::Healthy => {
                let became_reachable = bucket.state != HealthState::Healthy;
                bucket.state = HealthState::Healthy;
                bucket.consecutive_failures = 0;
                if bucket.healthy_since.is_none() {
                    bucket.healthy_since = Some(now);
                }
                if became_reachable {
                    topology_revalidation.push(index);
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

        Ok(self.evaluate(now, topology_revalidation))
    }

    /// Re-evaluate a time-based return without manufacturing a health signal.
    pub fn tick(&mut self, now: Instant) -> HealthUpdate {
        self.evaluate(now, Vec::new())
    }

    fn evaluate(&mut self, now: Instant, topology_revalidation: Vec<usize>) -> HealthUpdate {
        let previous = self.selected;
        let previous_read = self.read_selected;

        if self.buckets[self.selected].state == HealthState::Unhealthy {
            // Availability wins over return hysteresis when the current bucket is
            // gone. Pick the most-preferred bucket this node currently knows is
            // healthy, regardless of which side of the current index it sits on.
            if let Some(index) = self
                .buckets
                .iter()
                .position(|bucket| bucket.state == HealthState::Healthy)
            {
                self.selected = index;
            }
        } else {
            // The current bucket still works. A more-preferred bucket must prove
            // continuous health for the much longer return window before we move.
            if let Some(index) = (0..self.selected).find(|&index| {
                let bucket = &self.buckets[index];
                bucket.state == HealthState::Healthy
                    && bucket.healthy_since.is_some_and(|since| {
                        now.saturating_duration_since(since) >= self.policy.return_after_healthy
                    })
            }) {
                self.selected = index;
            }
        }

        self.evaluate_read(now);

        HealthUpdate {
            selected_index: self.selected,
            selection_change: (self.selected != previous).then_some(SelectionChange {
                from: previous,
                to: self.selected,
            }),
            read_selected_index: self.read_selected,
            read_selection_change: (self.read_selected != previous_read).then_some(
                SelectionChange {
                    from: previous_read,
                    to: self.read_selected,
                },
            ),
            topology_revalidation,
        }
    }

    /// Choose the read bucket, on top of the just-computed write selection.
    ///
    /// The region bucket serves reads only while it is Healthy; it is abandoned
    /// the instant it fails its availability streak (the same rule the write pin
    /// uses to leave), and reads fall back to the write selection. Returning to
    /// it is deliberately slow: it must prove continuous health for the full
    /// return window *and* the worker must have confirmed it is caught up. When
    /// the region bucket is also the write selection the caught-up gate is moot —
    /// the write home is truth by definition.
    fn evaluate_read(&mut self, now: Instant) {
        let Some(region) = self.read_preference else {
            self.read_selected = self.selected;
            return;
        };
        if self.read_selected == region {
            if self.buckets[region].state == HealthState::Unhealthy {
                self.read_selected = self.selected;
            }
            return;
        }
        let status = &self.buckets[region];
        let healthy_for_window = status.state == HealthState::Healthy
            && status.healthy_since.is_some_and(|since| {
                now.saturating_duration_since(since) >= self.policy.return_after_healthy
            });
        self.read_selected =
            if region == self.selected || (healthy_for_window && self.region_caught_up) {
                region
            } else {
                self.selected
            };
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
        health.observe(1, BucketSignal::Success, now).unwrap();

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
        health.observe(1, BucketSignal::Success, now).unwrap();
        health.observe(0, BucketSignal::Timeout, now).unwrap();
        health.observe(0, BucketSignal::Success, now).unwrap();

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
        health.observe(2, BucketSignal::Success, now).unwrap();
        health.observe(1, BucketSignal::Success, now).unwrap();

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
        health.observe(1, BucketSignal::Success, now).unwrap();
        health.observe(0, BucketSignal::Timeout, now).unwrap();
        assert_eq!(health.selected_index(), 1);

        let recovered = health
            .observe(0, BucketSignal::Success, at(now, 1))
            .unwrap();
        assert_eq!(recovered.selected_index, 1);
        assert_eq!(recovered.topology_revalidation, vec![0]);
        assert_eq!(health.tick(at(now, 10)).selection_change, None);

        let returned = health.tick(at(now, 11));
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
        health.observe(1, BucketSignal::Success, now).unwrap();
        health.observe(0, BucketSignal::Timeout, now).unwrap();
        health
            .observe(0, BucketSignal::Success, at(now, 1))
            .unwrap();

        health
            .observe(0, BucketSignal::Timeout, at(now, 8))
            .unwrap();
        health
            .observe(0, BucketSignal::Success, at(now, 9))
            .unwrap();
        assert_eq!(health.tick(at(now, 18)).selected_index, 1);

        let returned = health.tick(at(now, 19));
        assert_eq!(returned.selected_index, 0);
        assert_eq!(
            returned.selection_change,
            Some(SelectionChange { from: 1, to: 0 })
        );
        assert_eq!(health.tick(at(now, 30)).selection_change, None);
    }

    #[test]
    fn a_dead_selected_bucket_can_use_a_recovered_preferred_bucket_immediately() {
        let now = Instant::now();
        let mut health = BucketHealth::new(3, policy(1, 60)).unwrap();
        health.observe(1, BucketSignal::Success, now).unwrap();
        health.observe(2, BucketSignal::Success, now).unwrap();
        health.observe(0, BucketSignal::Timeout, now).unwrap();
        assert_eq!(health.selected_index(), 1);

        health
            .observe(0, BucketSignal::Success, at(now, 1))
            .unwrap();
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

        let first = health.observe(1, BucketSignal::Success, now).unwrap();
        assert_eq!(first.topology_revalidation, vec![1]);
        assert!(first.has_transition());

        let steady = health
            .observe(1, BucketSignal::Success, at(now, 1))
            .unwrap();
        assert!(steady.topology_revalidation.is_empty());
        assert!(!steady.has_transition());

        health
            .observe(1, BucketSignal::HttpStatus(500), at(now, 2))
            .unwrap();
        let healed = health
            .observe(1, BucketSignal::Success, at(now, 3))
            .unwrap();
        assert_eq!(healed.topology_revalidation, vec![1]);
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

        let snapshot = controller.worker_tick_at(now);
        assert_eq!(snapshot.selected_index, 0);
        assert_eq!(snapshot.selection_change, None);
        assert_eq!(
            snapshot.states,
            vec![HealthState::Unhealthy, HealthState::Healthy]
        );
        assert_eq!(snapshot.topology_revalidation, vec![1]);
        assert_eq!(snapshot.alarms, vec![0, 0]);

        let pending = controller.worker_tick_at(at(now, 1));
        assert_eq!(pending.selection_change, snapshot.selection_change);
        assert_eq!(pending.topology_revalidation, vec![1]);

        controller.topology_revalidated(1).unwrap();
        let ready = controller.worker_tick_at(at(now, 2));
        assert_eq!(ready.selected_index, 1);
        assert_eq!(
            ready.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        controller.selection_applied(1).unwrap();
        let quiet = controller.worker_tick_at(at(now, 2));
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

        let snapshot = controller.worker_tick_at(now);
        assert_eq!(snapshot.topology_revalidation, vec![1]);
        assert_eq!(snapshot.alarms, vec![0, 2]);
        controller.topology_revalidated(1).unwrap();
        assert_eq!(controller.worker_tick_at(now).alarms, vec![0, 0]);
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
        let to_b = controller.worker_tick_at(now);
        assert_eq!(
            to_b.selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        controller.selection_applied(1).unwrap();

        // A recovers long enough to be preferred, but its topology check is
        // deliberately never acknowledged (the worker saw a mismatch).
        controller
            .observe_at(0, BucketSignal::Success, at(now, 1))
            .unwrap();
        let rejected = controller.worker_tick_at(at(now, 11));
        assert_eq!(rejected.selected_index, 1);
        assert_eq!(rejected.topology_revalidation, vec![0]);
        assert!(!controller.bucket_eligible(0).unwrap());
        assert!(controller.bucket_eligible(2).unwrap());

        // When the actually selected B dies, the selector must skip invalid A
        // and continue to validated C instead of sticking reads on dead B.
        controller
            .observe_at(1, BucketSignal::Timeout, at(now, 12))
            .unwrap();
        let to_c = controller.worker_tick_at(at(now, 12));
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

        let snapshot = controller.worker_tick();
        assert_eq!(snapshot.alarms, vec![0, 800]);
        assert_eq!(snapshot.selected_index, 0);
    }

    #[test]
    fn read_returns_to_region_only_after_window_and_caught_up() {
        let base = Instant::now();
        // The node's region is the non-preferred bucket (index 1); reads start
        // following the write selection (bucket 0).
        let mut health = BucketHealth::new(2, policy(3, 10)).unwrap();
        health.set_read_affinity(1, 0);
        health.observe(1, BucketSignal::Success, base).unwrap();

        // Window not elapsed: reads stay on the write bucket.
        assert_eq!(health.tick(at(base, 5)).read_selected_index, 0);
        // Window elapsed but not caught up: still on the write bucket.
        health.region_caught_up = false;
        assert_eq!(health.tick(at(base, 11)).read_selected_index, 0);
        // Caught up and window elapsed: reads return to the region bucket, and
        // the write selection never moved.
        health.region_caught_up = true;
        let update = health.tick(at(base, 12));
        assert_eq!(update.read_selected_index, 1);
        assert_eq!(
            update.read_selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        assert_eq!(update.selected_index, 0);
        assert_eq!(health.selected_index(), 0);
    }

    #[test]
    fn read_leaves_region_immediately_on_the_leave_threshold() {
        let base = Instant::now();
        let mut health = BucketHealth::new(2, policy(3, 10)).unwrap();
        health.set_read_affinity(1, 1);
        health.observe(1, BucketSignal::Success, base).unwrap();
        assert_eq!(health.read_selected_index(), 1);

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
        health.observe(1, BucketSignal::Success, base).unwrap();
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
        health.set_read_affinity(2, 0);
        health.observe(2, BucketSignal::Success, base).unwrap();
        health.observe(1, BucketSignal::Success, base).unwrap();

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
        controller.configure_read_affinity(1, 0).unwrap();

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
        controller.set_region_caught_up(true);
        let snap = controller.worker_tick_at(at(base, 11));
        assert_eq!(snap.selected_index, 0);
        assert_eq!(snap.read_selected_index, 1);
        assert_eq!(
            snap.read_selection_change,
            Some(SelectionChange { from: 0, to: 1 })
        );
        controller.read_selection_applied(1).unwrap();
        assert_eq!(controller.read_return_pending(), None);
        assert_eq!(
            controller
                .worker_tick_at(at(base, 12))
                .read_selection_change,
            None
        );
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
