---
description: pypiron vs pypiserver — both host private Python packages; pypiron adds a PyPI cache, cloud storage, and 100x the install throughput.
---

# pypiron vs pypiserver

pypiserver is the smallest thing that serves a private index: a directory of
wheels, htpasswd auth, and a redirect to PyPI for anything it does not have. It
has been maintained for over a decade and is a fine choice when that is all you
need. pypiron does the same job and adds a real PyPI cache, cloud storage,
supply-chain defense, and roughly 100× the install throughput.

## Where pypiserver is enough

- **Single box, local disk, no cache.** If you keep a folder of internal wheels
  on one server and let developers get public packages from pypi.org directly,
  pypiserver covers it with almost no moving parts. pypiron can run just as
  small, but if you will never want caching or cloud storage, pypiserver's
  minimalism is the draw.
- **You want the most boring possible dependency.** pypiserver is a small,
  well-worn Python app. That is a real virtue for a low-stakes internal index.

pypiserver does *not* cache PyPI — a missing package is a redirect to pypi.org,
not a stored copy — and it has no built-in cloud-storage backend. If you need
either, you are already past what it does.

## Where pypiron pulls ahead

- **It actually caches PyPI.** The first install of a public package stores it,
  behind the same URL as your private ones, so the next install and every
  air-gapped or egress-blocked build gets it locally. pypiserver only redirects
  to pypi.org.
- **Cloud storage, no database.** Point pypiron at an S3, GCS, or Azure bucket
  and run as many nodes as you like against it — no database either way
  ([setup](../guides/standard-cloud.md)). pypiserver serves from local directories only.
- **It is roughly 100× faster.** In the [six-way benchmark](index.md),
  pypiserver on gunicorn peaked at **69 installs/s** on a 2-vCPU box; pypiron hit
  **8,288 installs/s** on the same hardware. pypiserver pushes every wheel
  through its own network card, which is where it tops out; pypiron hands the
  download to object storage and scales to CPU.
- **Supply-chain defense on by default.** New releases wait 7 days before
  pypiron serves them ([how](../security.md)), a private name can
  never fall through to PyPI (no dependency confusion), and pypiron refuses any
  file the advisory feed flags as malware. pypiserver has none of these.
- **It survives an outage.** Run nodes across regions or clouds on one bucket
  list; reads fail over with zero data loss ([multi-region](../guides/multi-region.md)).
  pypiserver is single-box.

## Side by side

| | pypiron | pypiserver |
| --- | --- | --- |
| Private hosting | ✅ | ✅ |
| Auth | uploader + admin creds, install tokens | htpasswd |
| Cache / proxy PyPI | ✅ stores a copy | redirect to PyPI only |
| Cloud storage | S3, GCS, Azure | — (local disk) |
| No database | ✅ | ✅ |
| Dependency cooldown | ✅ default | — |
| Malware / advisory blocking | ✅ default | — |
| No dependency confusion | ✅ | — |
| Multi-node resilience | one bucket, multi-region failover | single box |
| Peak installs/s (2 vCPU) | [8,288](index.md) | 69 |

## The honest line

For a single-box private index with no caching, pypiserver is a perfectly good,
long-lived choice and there is no reason to switch for its own sake. The moment
you want a PyPI cache, cloud storage, more than one node, supply-chain defense,
or real throughput, pypiron gives you all of them from one binary.
[Try it](../guides/standard-cloud.md) — the private-index setup is a single command.
