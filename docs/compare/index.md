---
description: Compare self-hosted PyPI servers — pypiserver, devpi, bandersnatch, pypicloud, proxpi, Artifactory, Nexus — and pick the one that fits your setup.
---

# Choosing a self-hosted PyPI server

Six open-source PyPI servers and two commercial platforms cover the field. They
split cleanly by what they were built to do: host your private packages, mirror
public PyPI, proxy PyPI on demand, or all three. Start from the job you have.

| Tool | Private hosting | Proxy/cache PyPI | Full mirror | Cloud storage | No database | Speed |
| --- | :---: | :---: | :---: | :---: | :---: | --- |
| **pypiron** | ✅ | ✅ | ✅ (filtered) | S3, GCS, Azure | ✅ | [8,288 installs/s](../reference/benchmarks.md) |
| pypiserver | ✅ | redirect only | — | — | ✅ | 69/s |
| devpi | ✅ | ✅ | — | — | sqlite/postgres | 78/s |
| bandersnatch | — | — | ✅ | S3-compatible | ✅ | 77/s |
| pypicloud *(archived)* | ✅ | ✅ | — | S3, GCS, Azure | needs DynamoDB/SQL | 42/s |
| proxpi | — | ✅ | — | — | ✅ | 32/s |
| Artifactory / Nexus | ✅ | ✅ | — | ✅ | needs a database | commercial |

The installs/s column is each server's peak in its best cloud-backed config on
one 2-vCPU box; the full six-way rig and methodology are on the
[benchmarks page](../reference/benchmarks.md). The byte-serving servers cluster
at 32–78/s because they push every wheel through their own network card;
pypiron hands the download straight to object storage and scales to CPU instead.

## Which one fits

**You want private packages and a PyPI cache behind one URL, fast, with cloud
storage and no database to run.** That is pypiron. One binary, point it at a
folder or an S3/GCS/Azure bucket, and it hosts your uploads, caches public PyPI
on demand, and mirrors a chosen subset — with dependency-confusion protection
and a 7-day dependency cooldown on by default. It runs multi-node against one
bucket and survives a region or a whole cloud going down. If that is the shape
of your problem, read the [vs devpi](pypiron-vs-devpi.md),
[vs pypiserver](pypiron-vs-pypiserver.md), and
[vs Artifactory](pypiron-vs-artifactory.md) pages.

**You just need a dead-simple private index on a single box, and PyPI packages
come from pypi.org directly.** pypiserver is the smallest thing that works: a
directory of wheels, htpasswd auth, and a fallback redirect to PyPI. It has been
maintained for over a decade and does one job well. pypiron does the same job
and adds caching, cloud storage, and speed — but if you never leave one machine
and never want a cache, pypiserver's simplicity is hard to beat.

**Your team wants a staging-and-release workflow: push a package to a test
index, run your suite against it, then promote it to a release index.** That is
devpi's core strength, and pypiron does not have it. devpi's inheritable
indexes, `devpi push` promotion between them, and tox integration are a genuine
publishing workflow built around test-then-release. If that pipeline is what you
are buying, use devpi. pypiron treats an uploaded filename as immutable and
final — deliberately, for supply-chain safety — so it is the wrong tool for a
promote-between-indexes flow. See [pypiron vs devpi](pypiron-vs-devpi.md).

**You need a complete, offline copy of all of PyPI for an air-gapped network.**
bandersnatch is the reference full-mirror tool and pypiron can do filtered
mirroring, but if you truly want every file PyPI has ever published on local
disk or S3, bandersnatch is purpose-built for it. pypiron's mirror is designed
to carry a *subset* — an approved allowlist, or a filter by name, wheel tags,
size, or Python version — which is what most private networks actually want.
Whole-of-PyPI with no filter is bandersnatch's job.

**You want a caching proxy in front of PyPI and nothing else — no private
uploads.** proxpi is a tiny Flask proxy that does exactly this. pypiron's proxy
does the same and hosts private packages too, but if a lightweight read-through
cache is the entire requirement, proxpi is a smaller dependency.

**You are still running pypicloud.** It was [archived in August 2023](https://github.com/stevearc/pypicloud)
and no longer maintained. It is the closest architectural peer to pypiron —
cloud storage with a redirect-to-S3 read path — but it needs a database
(DynamoDB or SQL) and, in the same benchmark, its Python index layer tops out at
[42 installs/s against pypiron's 8,288](../reference/benchmarks.md). If you liked
pypicloud's model, pypiron is the maintained successor to evaluate.

**You have already standardized on Artifactory or Nexus for every artifact type
in the org** — Docker, Maven, npm, and Python under one commercial platform,
with the access controls and support contract that come with it. Keep it. A
dedicated PyPI server is not going to replace an org-wide binary manager, and it
should not try. pypiron is the pick when Python is the artifact you care about
and you would rather run one small self-hosted binary than license a platform.
See [pypiron vs Artifactory](pypiron-vs-artifactory.md).

## Honest summary

If your problem is "serve my team's private Python packages and a fast cache of
public PyPI, on my own infrastructure, without running a database," pypiron is
built for exactly that and is [100×+ faster than the field](../reference/benchmarks.md)
at it. If your problem is a staging workflow (devpi), a full offline PyPI mirror
(bandersnatch), or an org-wide multi-format binary manager (Artifactory/Nexus),
one of those is the better tool, and this page would rather tell you so than
lose your afternoon.
