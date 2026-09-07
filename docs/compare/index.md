---
description: pypiron vs devpi, pypiserver, bandersnatch, pypicloud, proxpi, Artifactory — install benchmarks (8,288/s on 2 vCPU) and which server fits your job.
---

# Comparison and benchmarks

pypiron sustains **8,288 installs/s on 2 vCPU** — more than 100× the next
server in the benchmark.

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

The others stream every wheel through the server. pypiron hands wheel downloads
to object storage, leaving the node to serve index and metadata requests.

In a separate index-only benchmark, one 8-vCPU machine served **202,069
requests/s** with a p99 of **2.62 ms**.

Each server ran its own documented production topology on the same 2-vCPU AWS
box, serving the same frozen set of real wheels under identical client load —
the [rigs](https://github.com/blackthorn-interstellar/pypiron/tree/master/dev/bench/install/compose)
and [raw results](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/BENCHMARK_RESULTS.md)
are published in the repo. Beyond speed: [how pypiron is tested](../testing.md).

## Choose by job

- **Private packages and a PyPI cache behind one URL** — pypiron. One binary,
  no database, local disk or S3/GCS/Azure.
- **A staging → release pipeline** — push to a test index, run the suite,
  promote — devpi. That workflow is devpi's core, and pypiron does not do it:
  [pypiron vs devpi](pypiron-vs-devpi.md).
- **A dead-simple private index on one box, no cache** — pypiserver: a
  directory of packages, maintained for over a decade.
  [pypiron vs pypiserver](pypiron-vs-pypiserver.md).
- **A caching proxy and nothing else** — proxpi, a tiny Flask proxy. pypiron's
  proxy does the same and hosts private packages too.
- **A byte-complete mirror of all of PyPI** — bandersnatch, purpose-built for
  that job. pypiron mirrors a filtered subset.
- **One governed platform for every artifact type in the org** — Artifactory or
  Nexus: [pypiron vs Artifactory](pypiron-vs-artifactory.md).
- **Fully managed, no servers at all** — AWS CodeArtifact, if you are on AWS
  and would rather pay per request than run anything yourself.

Still on pypicloud? It was
[archived in August 2023](https://github.com/stevearc/pypicloud). Follow the
[migration guide](../guides/migrate.md).

[Start pypiron](../index.md#start-pypiron) ·
[Deploy on cloud storage](../guides/standard-cloud.md) ·
[Migrate](../guides/migrate.md)
