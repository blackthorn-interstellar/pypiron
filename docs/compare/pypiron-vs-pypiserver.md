---
description: pypiron vs pypiserver — both host private Python packages; pypiron adds a PyPI cache, cloud storage, and 100x the install throughput.
---

# pypiron vs pypiserver

pypiserver serves a directory of private packages and redirects missing ones to
PyPI. It is small, established, and enough for that job. pypiron adds a real
PyPI cache, cloud storage, supply-chain defense, and 100× the measured install
throughput.

## Choose pypiserver when

- **A folder of private packages is enough.** One server, local disk, public
  packages from PyPI.
- **You value age over features.** pypiserver is a small Python application
  maintained for more than a decade.

pypiserver does *not* cache PyPI — a missing package is a redirect to pypi.org,
not a stored copy — and it has no built-in cloud-storage backend. If you need
either, you are already past what it does.

## Choose pypiron when

- **You need a real PyPI cache.** The first install stores a public package
  behind the same URL as your private ones. pypiserver only redirects to PyPI.
- **Cloud storage, no database.** Point pypiron at an S3, GCS, or Azure bucket
  and run any number of nodes against it
  ([setup](../guides/standard-cloud.md)).
- **It is 100× faster in the benchmark.** In the [six-way test](index.md),
  pypiserver on gunicorn peaked at **69 installs/s** on a 2-vCPU box; pypiron hit
  **8,288 installs/s**.
- **Supply-chain defense on by default.** New releases wait 7 days before
  pypiron serves them, private names never fall through to PyPI, and known
  malware is refused ([details](../security.md)).
- **You need more than one region.** A bucket list can span regions or clouds;
  reads fail over automatically ([setup](../guides/multi-region.md)).

## Side by side

| | pypiron | pypiserver |
| --- | --- | --- |
| Private hosting | ✅ | ✅ |
| Auth | admin/uploader/reader credentials, install tokens | htpasswd or custom provider |
| Cache / proxy PyPI | ✅ stores a copy | redirect to PyPI only |
| Cloud storage | S3, GCS, Azure | — (local disk) |
| No database | ✅ | ✅ |
| Dependency cooldown | ✅ default | — |
| Malware / advisory blocking | ✅ default | — |
| No dependency confusion | ✅ | — |
| Multi-node resilience | shared bucket or multi-bucket failover | no built-in failover |
| Peak installs/s (2 vCPU) | [8,288](index.md) | 69 |

## The deciding question

Need a small private index on one box? Choose pypiserver. Need caching, cloud
storage, supply-chain defense, or serious throughput? Choose pypiron.

[Start pypiron](../index.md#start-pypiron) ·
[Deploy on cloud storage](../guides/standard-cloud.md) ·
[Publish and install](../guides/publish-install.md)
