---
description: pypiron vs devpi, pypiserver, bandersnatch, pypicloud, proxpi, Artifactory — install benchmarks (8,288/s on 2 vCPU) and which server fits your job.
---

# Comparison and benchmarks

pypiron sustains **8,288 installs/s on 2 vCPU** — 100×+ every other
self-hosted PyPI server on the same box.

![Max sustained install throughput](../assets/install-throughput.svg#only-light)
![Max sustained install throughput](../assets/install-throughput-dark.svg#only-dark)

| Rank | Server | Config | Installs/s |
|---|---|---|---|
| 1 | **pypiron** | S3 + presigned redirect (Rust) | **8,288** |
| 2 | devpi | devpi + nginx | 78 |
| 3 | bandersnatch | full static mirror via nginx | 77 |
| 4 | pypiserver | gunicorn + cached-dir | 69 |
| 5 | pypicloud *(archived)* | S3 + DynamoDB (uwsgi) | 42 |
| 6 | proxpi | flask caching proxy | 32 |

The gap is architecture, not tuning: the other servers stream every wheel
through their own network card; pypiron hands the download to object storage
and scales to CPU. The index holds up against pypi.org itself — replaying PyPI's real request stream, one 8-vCPU box served it at **202,069 requests/s** with a p99 of **2.62 ms**, about 4× the request rate
of all of PyPI.

Each server ran its own documented production topology on the same 2-vCPU AWS
box, serving the same frozen set of real wheels under identical client load —
the [rigs](https://github.com/blackthorn-interstellar/pypiron/tree/master/dev/bench/install/compose)
and [raw results](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/BENCHMARK_RESULTS.md)
are published in the repo. Beyond speed: [how pypiron is tested](../testing.md).

## Which one fits

- **Private packages and a PyPI cache behind one URL, with nothing else to
  run** — pypiron, one binary against a folder or an S3/GCS/Azure bucket.
  [Supply-chain defense](../security.md) is on by default. Features:
  [front page](../index.md). Every flag and env var:
  [configuration](../reference/configuration.md).
- **A staging → release pipeline** — push to a test index, run the suite,
  promote — devpi. That workflow is devpi's core, and pypiron does not do it:
  [pypiron vs devpi](pypiron-vs-devpi.md).
- **A dead-simple private index on one box, no cache** — pypiserver: a
  directory of wheels behind htpasswd auth, maintained for over a decade.
  [pypiron vs pypiserver](pypiron-vs-pypiserver.md).
- **A caching proxy and nothing else** — proxpi, a tiny Flask proxy. pypiron's
  proxy does the same and hosts private packages too.
- **Still on pypicloud** — it was
  [archived in August 2023](https://github.com/stevearc/pypicloud). pypiron is
  the maintained successor with the same redirect-to-S3 shape.
  [Migration guide](../guides/migrate.md).
- **One governed platform for every artifact type in the org** — Artifactory or
  Nexus: [pypiron vs Artifactory](pypiron-vs-artifactory.md).

## When something else is the better tool

- **A byte-complete mirror of all of PyPI** — bandersnatch, purpose-built for
  exactly that. pypiron mirrors a filtered subset: allowlist, name, wheel tags,
  size, or Python version.
- **Docker, Maven, npm, and Python under one roof** — Artifactory or Nexus. A
  dedicated PyPI server should not try to replace an org-wide binary manager.
- **Fully managed, no servers at all** — AWS CodeArtifact, if you are on AWS
  and would rather pay per request than run anything yourself.
