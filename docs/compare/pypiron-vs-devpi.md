---
description: pypiron vs devpi — a fast single-binary private PyPI with cloud storage vs devpi's staging-and-release index workflow. Which fits your team.
---

# pypiron vs devpi

Both host private Python packages and cache PyPI. They are built around
different jobs. devpi is a **publishing workflow**: inheritable indexes you
promote packages between, test-then-release, a web UI, and master-replica
replication. pypiron is a **fast package server**: one binary, cloud storage,
no database, and
installs that scale with CPU instead of your network card.

## Pick devpi when

- **You want a staging-and-release pipeline.** devpi's signature feature is
  inheritable indexes plus `devpi push` to promote a package from a test index
  to a release index once it passes. Its tox integration runs your suite against
  a candidate before you promote it. If test-then-release between indexes is the
  workflow you are buying, use devpi — pypiron does not do it.
- **You lean on per-user and per-index accounts.** devpi models users and
  indexes with per-index access control. pypiron uses two credentials (an
  uploader and an admin); if you need many named accounts with separate indexes,
  devpi fits that shape and pypiron does not.

## Pick pypiron when

- **You want cloud storage without a database.** pypiron stores everything as
  plain files on local disk or an S3, GCS, or Azure bucket — nothing else to
  run or back up. devpi keeps its state in sqlite by default, or PostgreSQL via
  a plugin for a bigger deployment; there is no first-class object-storage
  backend.
- **Install throughput matters.** In the [six-way benchmark](index.md),
  devpi behind nginx peaked at **78 installs/s** on a 2-vCPU box; pypiron hit
  **8,288 installs/s** on the same hardware — 100×+ faster. Any server that streams the wheel itself lands near devpi's number: the limit
is the box's network card, not the code. pypiron hands the download straight to object storage, so its
  ceiling is CPU.
- **You want supply-chain defense on by default.** New releases wait 7 days
  before pypiron serves them, so most attacks surface first
  ([how](../security.md)). A private name is yours or PyPI's, never
  both, which closes dependency confusion. devpi blocks upstream lookups for
  privately uploaded names, but the cooldown and the malware-advisory block are
  pypiron's.
- **You need to survive an outage.** pypiron runs any number of nodes against
  one bucket list spanning regions and clouds; every upload lands on all of them
  and reads fail over with zero data loss ([multi-region](../guides/multi-region.md)).
  devpi offers master-to-replica streaming replication — each replica keeps a
  full copy — which is a different model: one writer, read replicas, no shared
  object store.

## Side by side

| | pypiron | devpi |
| --- | --- | --- |
| Private hosting | ✅ | ✅ |
| Cache / proxy PyPI | ✅ | ✅ |
| Staging → release workflow | — | ✅ (`devpi push`, tox) |
| Cloud storage | S3, GCS, Azure | — (sqlite / PostgreSQL) |
| No database | ✅ | needs sqlite or PostgreSQL |
| Dependency cooldown | ✅ default | — |
| Malware / advisory blocking | ✅ default | — |
| No dependency confusion | ✅ | ✅ |
| Multi-node resilience | one bucket, multi-region failover | master → read replicas |
| Peak installs/s (2 vCPU) | [8,288](index.md) | 78 |

## The honest line

If your team's release process is "push to a test index, run the suite, promote
to release," that pipeline *is* devpi and pypiron will not replace it. If you
want the private index and PyPI cache to be a single fast binary on cloud
storage with supply-chain defense built in — and you do not need the staging
workflow — pypiron is the lighter, faster fit. [Set it up](../guides/standard-cloud.md)
in one command.
