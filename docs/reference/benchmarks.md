# Benchmarks

pypiron is 5–90× faster than other PyPI servers at sustained install throughput.

![Max sustained install throughput](../assets/install-throughput.svg#only-light)
![Max sustained install throughput](../assets/install-throughput-dark.svg#only-dark)

## Headline

A six-way comparison of max sustained real-install throughput, every server in
its best cloud-backed config, on one small AWS box. `oha` replays the install
mix (index + wheel URLs in install proportions) and follows the 302 to download
wheel bytes from S3, so the entire path is exercised.

| Rank | Server | Config | Installs/s |
|---|---|---|---|
| 1 | **pypiron** | S3 + presigned redirect (Rust) | **3,026** |
| 2 | bandersnatch | full static mirror via nginx | 574 |
| 3 | pypiserver | gunicorn + cached-dir | 85 |
| 4 | pypicloud | S3 + DynamoDB (uwsgi) | 47 |
| 5 | devpi | devpi + nginx | 35 |
| 6 | proxpi | flask caching proxy | 32 |

Each row is that server's own saturation ceiling on the same small box (an
`r7i.large`, 2 vCPU). The Python app servers hit their CPU wall under a modest load;
pypiron and bandersnatch serve so leanly that reaching their ceiling took a larger
load fleet (8× and 4× `c7i.8xlarge`). pypiron tops out at 3,026 installs/s on 2 vCPU
— server-bound: the fleet drove it past its knee into collapse, so the box, not the
load fleet, is the limit. It scales roughly linearly with cores.

!!! note
    bandersnatch serves every wheel byte through its own NIC and saturates the
    network with CPU to spare. pypiron's presigned redirect offloads bytes to
    object storage, so its node carries only index responses and 302s and scales
    to its CPU instead. See [Artifact delivery](../concepts/artifact-delivery.md).

## Methodology

The comparison is honest by construction. Each competitor runs in its own
documented production topology (right app server, worker count, and the
nginx/DB sidecars it architecturally needs) — no tool gets a response or edge
cache another lacks. Egress is blocked on every ranking run, so any cache miss
or upstream fallback fails loudly instead of being served by a CDN. Every server
is seeded with the same frozen, hash-pinned set of ~100 real projects' wheels,
and the clients fire byte-identical `uv` requests at each.

## Full results

Methodology, raw numbers, and the AWS rig provenance live in the repo:

- [Benchmark plan](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/BENCHMARK_INSTALL.md)
- [Benchmark results](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/BENCHMARK_RESULTS.md)
