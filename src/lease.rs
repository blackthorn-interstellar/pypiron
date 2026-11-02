//! Sloppy leader election: an S3 conditional-write lease with TTL and
//! heartbeat. Rebuilds are idempotent, so dual leadership for a few seconds
//! merely duplicates work — this is a cost optimization, never a correctness
//! requirement. No Raft, no fencing.
//!
//! Read "idempotent" precisely: a rebuild is idempotent against *unchanging*
//! truth. Two rebuilds straddling a mutation are not — the one that listed
//! first can land last and leave a stale view standing, since the per-package
//! view PUT is unconditional (only the global index takes an `If-Match`). The
//! periodic audit is the named backstop for exactly that (dev/DESIGN.md,
//! "Split-brain is harmless"), which is what makes sloppiness affordable.
//!
//! This lease therefore does NOT serialize rebuilds, and nothing should be
//! built on the assumption that it does. `is_leader()` is a point-in-time
//! read with no fencing token, so a rebuild outliving the TTL runs beside its
//! successor's; the leader's own audit task rebuilds concurrently with its
//! tick; and `delete_record` rebuilds from any node's request handler,
//! unleased. Serializing is what it is *for*, not what it *guarantees*.
//!
//! Where an expired leader's late write would be *harmful* rather than merely
//! duplicated, the fence lives in the record, not here. The quarantined set is
//! the exhibit: its stored envelope carries a monotone epoch, and the audit
//! sweep's repair publish is conditioned on the epoch it read when the walk
//! began (`advisories::QuarantinedEdit::Sweep`), so a sweep that outlives its
//! lease loses to whatever landed while it ran instead of un-blocking a project
//! its successor froze. Read that as the pattern: give the record a version, do
//! not give the lease a token.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{debug, info, warn};

use crate::storage::Storage;

pub(crate) const LEASE_KEY: &str = "_leader/lease.json";

#[derive(Debug, Serialize, Deserialize)]
struct Lease {
    holder: String,
    term: u64,
    #[serde(rename = "expires-at")]
    expires_at: i64,
}

#[derive(Clone)]
pub struct LeaseManager {
    storage: Arc<dyn Storage>,
    holder: String,
    ttl_secs: i64,
}

impl LeaseManager {
    pub fn new(storage: Arc<dyn Storage>, ttl: Duration) -> Self {
        let holder = format!(
            "{}-{}",
            std::process::id(),
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        );
        Self {
            storage,
            holder,
            ttl_secs: ttl.as_secs().max(1) as i64,
        }
    }

    /// Acquire, renew, or steal the lease; heartbeat by calling every tick.
    /// Any failure path simply reports "not leader" — sloppy by design.
    pub async fn is_leader(&self) -> bool {
        match self.try_hold().await {
            Ok(held) => held,
            Err(e) => {
                warn!(error=?e, "lease check failed; assuming follower");
                false
            }
        }
    }

    async fn try_hold(&self) -> anyhow::Result<bool> {
        let now = crate::clock::now_utc().unix_timestamp();
        let storage = self.storage.as_ref();

        let Some((bytes, etag)) = storage.get_with_etag(LEASE_KEY).await? else {
            // No lease yet: create-if-absent.
            let won = storage
                .put_if_none_match(LEASE_KEY, self.lease_json(1, now))
                .await?
                .is_some();
            if won {
                info!(holder=%self.holder, "lease acquired");
            }
            return Ok(won);
        };

        let current: Lease = match serde_json::from_slice(&bytes) {
            Ok(lease) => lease,
            Err(_) => Lease {
                // Corrupt lease: treat as expired and steal over its ETag.
                holder: String::new(),
                term: 0,
                expires_at: 0,
            },
        };

        if current.holder == self.holder {
            // Renew our own lease.
            let renewed = storage
                .put_if_match(LEASE_KEY, &etag, self.lease_json(current.term, now))
                .await?
                .is_some();
            if !renewed {
                debug!("lost lease renewal race");
            }
            return Ok(renewed);
        }

        // Expired — or impossibly far in the future: a clock-skewed holder
        // that died would otherwise leave an unstealabe lease and a silent
        // leadership vacuum. Anything past now + 3×ttl is bogus.
        if now > current.expires_at || current.expires_at > now + 3 * self.ttl_secs {
            let stolen = storage
                .put_if_match(LEASE_KEY, &etag, self.lease_json(current.term + 1, now))
                .await?
                .is_some();
            if stolen && current.holder.is_empty() {
                // Released at shutdown (or corrupt): a handover, not a theft.
                info!(holder=%self.holder, term = current.term + 1, "lease acquired");
            } else if stolen {
                info!(holder=%self.holder, previous=%current.holder, term = current.term + 1, "lease stolen (expired or bogus expiry)");
            }
            return Ok(stolen);
        }

        Ok(false)
    }

    /// Best-effort release on graceful shutdown: expire the lease if we still
    /// hold it, so a restarted (or replacement) node becomes leader on its
    /// next tick instead of waiting out the TTL — without this, every restart
    /// was a TTL-long window where uploads went unprocessed. The release is a
    /// conditional overwrite (empty holder, expired) against the ETag we read,
    /// never a delete: the store has no conditional delete, and a peer that
    /// took the lease in between must keep it.
    pub async fn release(&self) {
        let (term, etag) = match self.storage.get_with_etag(LEASE_KEY).await {
            Ok(Some((bytes, etag))) => match serde_json::from_slice::<Lease>(&bytes) {
                Ok(lease) if lease.holder == self.holder => (lease.term, etag),
                _ => return,
            },
            _ => return,
        };
        let released = serde_json::to_vec(&Lease {
            holder: String::new(),
            term,
            expires_at: 0,
        })
        .unwrap_or_default();
        match self.storage.put_if_match(LEASE_KEY, &etag, released).await {
            Ok(Some(_)) => info!(holder=%self.holder, "lease released on shutdown"),
            Ok(None) => debug!("lease changed hands before release; left alone"),
            Err(e) => warn!(error=?e, "failed to release lease on shutdown"),
        }
    }

    fn lease_json(&self, term: u64, now: i64) -> Vec<u8> {
        serde_json::to_vec(&Lease {
            holder: self.holder.clone(),
            term,
            expires_at: now + self.ttl_secs,
        })
        .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::InMemStorage;
    use std::time::Duration;

    async fn stored(storage: &InMemStorage) -> Lease {
        serde_json::from_slice(&storage.get_bytes(LEASE_KEY).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn release_expires_own_lease_only() {
        let storage = Arc::new(InMemStorage::default());
        let lm = LeaseManager::new(storage.clone(), Duration::from_secs(30));
        assert!(lm.is_leader().await, "first node acquires the lease");
        lm.release().await;
        let released = stored(&storage).await;
        assert_eq!((released.holder.as_str(), released.expires_at), ("", 0));

        // A foreign lease survives someone else's release.
        let other = LeaseManager::new(storage.clone(), Duration::from_secs(30));
        assert!(other.is_leader().await);
        let late = LeaseManager::new(storage.clone(), Duration::from_secs(30));
        assert!(!late.is_leader().await, "lease is held by `other`");
        late.release().await;
        assert_eq!(stored(&storage).await.holder, other.holder);
    }

    /// A peer that takes the lease between release's read and its write keeps
    /// it. An unconditional delete here removed the peer's fresh lease, and the
    /// leaderless gap that followed let any node start a second leadership.
    #[tokio::test]
    async fn release_never_clobbers_a_peers_fresh_lease() {
        let storage = Arc::new(InMemStorage::default());
        let a = LeaseManager::new(storage.clone(), Duration::from_secs(30));
        assert!(a.is_leader().await);
        let now = crate::clock::now_utc().unix_timestamp();
        storage.write_after_read(
            LEASE_KEY,
            serde_json::to_vec(&Lease {
                holder: "peer".into(),
                term: 2,
                expires_at: now + 30,
            })
            .unwrap(),
        );
        a.release().await;
        assert_eq!(stored(&storage).await.holder, "peer");
    }

    #[tokio::test]
    async fn released_lease_is_acquired_immediately_not_after_ttl() {
        let storage = Arc::new(InMemStorage::default());
        let a = LeaseManager::new(storage.clone(), Duration::from_secs(3600));
        assert!(a.is_leader().await);
        let term = stored(&storage).await.term;
        a.release().await;
        // TTL is an hour; without release the successor would wait it out.
        let b = LeaseManager::new(storage.clone(), Duration::from_secs(3600));
        assert!(
            b.is_leader().await,
            "successor must acquire instantly after a graceful release"
        );
        assert_eq!(stored(&storage).await.term, term + 1, "terms keep rising");
    }
}
