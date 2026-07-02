# Benchmarks

pypiron is 5–90× faster than other PyPI servers at sustained install throughput.

![Max sustained install throughput](../assets/install-throughput.svg#only-light)
![Max sustained install throughput](../assets/install-throughput-dark.svg#only-dark)

## Headline

pypiron sustains **3,026 real installs/s on 2 vCPU** — 5–90× the other servers,
scaling near-linearly with cores. The full six-way field, every server in its
best cloud-backed config on the same small AWS box:

| Rank | Server | Config | Installs/s |
|---|---|---|---|
| 1 | **pypiron** | S3 + presigned redirect (Rust) | **3,026** |
| 2 | bandersnatch | full static mirror via nginx | 574 |
| 3 | pypiserver | gunicorn + cached-dir | 85 |
| 4 | pypicloud | S3 + DynamoDB (uwsgi) | 47 |
| 5 | devpi | devpi + nginx | 35 |
| 6 | proxpi | flask caching proxy | 32 |

!!! note
    bandersnatch pushes every wheel byte through its own NIC and saturates the
    network with CPU to spare. pypiron offloads wheel bytes to object storage, so
    the node serves only index responses and scales to its CPU.

## Methodology

The comparison is honest by construction. Each competitor runs in its own
documented production topology — right app server, worker count, and the
nginx/DB sidecars it needs. No tool gets a response or edge cache another lacks.
Egress is blocked on every ranking run: a cache miss or upstream fallback fails
loudly instead of being served by a CDN. Every server gets the same frozen,
hash-pinned set of ~100 real projects' wheels, and clients fire byte-identical
`uv` requests at each. The load generator (`oha`) replays the real install mix —
index pages and wheel URLs in install proportions — and follows each redirect to
object storage to pull the wheel bytes. The whole install path is measured.

Each row is that server's own saturation ceiling on the same small box (an
`r7i.large`, 2 vCPU). The Python app servers hit their CPU wall under modest
load. pypiron and bandersnatch serve so leanly that reaching their ceiling
needed a much larger load generator (a fleet of `c7i.8xlarge` boxes, 8× and 4×):
the limit was the 2-vCPU server under test, not the test itself.

## Full results

Methodology, raw numbers, and the AWS rig provenance live in the repo:

- [Benchmark plan](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/BENCHMARK_INSTALL.md)
- [Benchmark results](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/BENCHMARK_RESULTS.md)
