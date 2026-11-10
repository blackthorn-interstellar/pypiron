//! Free-space admission for proxy fills, so anonymous cold-cache requests can't
//! fill the spool disk. Concurrency stays unbounded on purpose: a fill is
//! refused only when its declared size, plus what fills already running still
//! have to write, would leave the spool filesystem below a safety floor.
//!
//! A sized fill reserves its declared size up front and hands the reservation
//! back as bytes land (they then show up in the filesystem's own free count,
//! so nothing is counted twice). A fill of unknown size reserves nothing; it,
//! like every fill, re-checks the floor as it writes and aborts before the
//! floor would be breached. Free space comes from `statvfs` on unix; elsewhere,
//! or when the call fails, fills are admitted exactly as before.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bytes in-flight fills have reserved but not yet written.
static RESERVED: AtomicU64 = AtomicU64::new(0);

/// How often (bytes written) a running fill re-checks the floor.
pub(crate) const RECHECK_BYTES: u64 = 16 * 1024 * 1024;

const GIB: u64 = 1024 * 1024 * 1024;

/// A fill refused (or aborted) because the spool disk is nearly full. The
/// request answers 503 with `Retry-After`, which pip and uv retry.
#[derive(Debug, thiserror::Error)]
#[error("spool disk nearly full: {free} bytes free, {reserved} reserved by running fills, {floor} kept free, {need} more needed")]
pub struct SpoolFull {
    free: u64,
    reserved: u64,
    floor: u64,
    need: u64,
}

/// The free space kept untouched: 1 GiB, or 5% of a filesystem smaller than
/// 20 GiB, so a small tmpfs spool can still take a large wheel.
pub(crate) fn floor(total: u64) -> u64 {
    GIB.min(total / 20)
}

/// Whether `need` more bytes fit: free space, minus what running fills still
/// have to write, minus the floor.
pub(crate) fn admits(free: u64, total: u64, reserved: u64, need: u64) -> bool {
    free.saturating_sub(reserved) >= floor(total).saturating_add(need)
}

/// `(free, total)` bytes of the filesystem holding `dir`, or `None` where that
/// can't be read (non-unix, or a failed call), which admits everything.
// The statvfs field types differ per platform (u32 or u64), so `u64::from` is
// a no-op on some and a widening on others.
#[allow(clippy::useless_conversion)]
fn space(dir: &Path) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        let stat = nix::sys::statvfs::statvfs(dir).ok()?;
        let unit = u64::from(stat.fragment_size());
        Some((
            u64::from(stat.blocks_available()).saturating_mul(unit),
            u64::from(stat.blocks()).saturating_mul(unit),
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        None
    }
}

/// A fill's hold on spool space; released (whatever is left of it) on drop.
#[derive(Debug)]
pub struct Reservation {
    remaining: u64,
}

impl Reservation {
    /// Admit a fill of `size` bytes (`None` = unknown) into the spool at `dir`.
    pub fn reserve(dir: &Path, size: Option<u64>) -> Result<Self, SpoolFull> {
        let need = size.unwrap_or(0);
        let Some((free, total)) = space(dir) else {
            return Ok(Self { remaining: 0 });
        };
        RESERVED
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |reserved| {
                admits(free, total, reserved, need).then(|| reserved.saturating_add(need))
            })
            .map(|_| Self { remaining: need })
            .map_err(|reserved| SpoolFull {
                free,
                reserved,
                floor: floor(total),
                need,
            })
    }

    /// `n` more bytes reached the spool: they now count against the
    /// filesystem's free space instead of the reservation.
    pub fn wrote(&mut self, n: u64) {
        let n = n.min(self.remaining);
        self.remaining -= n;
        RESERVED.fetch_sub(n, Ordering::SeqCst);
    }

    /// Re-check, mid-fill, that the floor still holds once every running fill
    /// (this one included) writes what it has reserved.
    pub fn still_fits(&self, dir: &Path) -> Result<(), SpoolFull> {
        let Some((free, total)) = space(dir) else {
            return Ok(());
        };
        let reserved = RESERVED.load(Ordering::SeqCst);
        if admits(free, total, reserved, 0) {
            return Ok(());
        }
        Err(SpoolFull {
            free,
            reserved,
            floor: floor(total),
            need: 0,
        })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        RESERVED.fetch_sub(self.remaining, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_is_a_gib_or_five_percent_of_a_small_filesystem() {
        assert_eq!(floor(500 * GIB), GIB);
        assert_eq!(floor(20 * GIB), GIB);
        assert_eq!(floor(2 * GIB), 2 * GIB / 20);
        assert_eq!(floor(0), 0);
    }

    #[test]
    fn admission_counts_reserved_bytes_and_the_floor() {
        let total = 100 * GIB;
        // Plenty of room: admitted.
        assert!(admits(50 * GIB, total, 0, 10 * GIB));
        // Exactly down to the floor still fits; one byte more does not.
        assert!(admits(11 * GIB, total, 0, 10 * GIB));
        assert!(!admits(11 * GIB, total, 0, 10 * GIB + 1));
        // Running fills' unwritten bytes count against the free space.
        assert!(!admits(20 * GIB, total, 10 * GIB, 10 * GIB));
        assert!(admits(21 * GIB, total, 10 * GIB, 10 * GIB));
        // An unknown size needs only the floor, and nothing overflows.
        assert!(admits(GIB, total, 0, 0));
        assert!(!admits(GIB - 1, total, 0, 0));
        assert!(!admits(u64::MAX - 1, total, 0, u64::MAX));
        assert!(!admits(0, total, u64::MAX, 0));
    }
}
