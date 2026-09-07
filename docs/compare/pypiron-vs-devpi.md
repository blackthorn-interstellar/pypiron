---
description: pypiron vs devpi — a fast single-binary private PyPI with cloud storage vs devpi's staging-and-release index workflow. Which fits your team.
---

# pypiron vs devpi

Both host private packages and cache PyPI. devpi centers on staged publishing
between indexes. pypiron centers on a fast, simple server backed by local disk
or cloud storage.

## Choose devpi when

- **You need staged releases.** devpi has inheritable indexes and `devpi push`
  for test-then-promote workflows. pypiron does not.
- **You want per-user and per-index accounts.** devpi models users and
  indexes with per-index access control. pypiron uses admin/uploader/reader
  credentials. Choose devpi for many named accounts and separate indexes.

## Choose pypiron when

- **You want cloud storage without a database.** pypiron stores files on local
  disk or S3/GCS/Azure. devpi uses SQLite by default or PostgreSQL through a
  plugin.
- **Install throughput matters.** On the same 2-vCPU benchmark box, devpi
  behind nginx peaked at **78 installs/s**; pypiron reached
  **[8,288](index.md)**.
- **Security should start on.** pypiron holds new public releases for 7 days,
  blocks known malware, and never sends a private name to public PyPI
  ([details](../security.md)).
- **You want shared-storage scaling.** Point any number of pypiron nodes at one
  bucket, or span buckets across regions and clouds
  ([setup](../guides/multi-region.md)).

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
| Multi-node resilience | shared bucket or multi-bucket failover | master → read replicas |
| Peak installs/s (2 vCPU) | [8,288](index.md) | 78 |

## The deciding question

Need test indexes and promotion? Choose devpi. Need a private index and PyPI
cache in one fast binary? Choose pypiron.

[Start pypiron](../index.md#start-pypiron) ·
[Deploy on cloud storage](../guides/standard-cloud.md) ·
[Migrate from devpi](../guides/migrate.md)
