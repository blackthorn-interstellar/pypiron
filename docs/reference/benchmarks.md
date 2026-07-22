---
description: Benchmarks. pypiron sustains 8,288 verified installs/s on 2 vCPU, 100x faster than every other PyPI server. Full six-way field, reproducible.
---

# Benchmarks

pypiron is 100×+ faster than other PyPI servers at sustained install throughput.

![Max sustained install throughput](../assets/install-throughput.svg#only-light)
![Max sustained install throughput](../assets/install-throughput-dark.svg#only-dark)

## Headline

pypiron sustains **8,288 verified installs/s on 2 vCPU** — 100×+ every other
server. The full six-way field, every server in its best cloud-backed config on
the same small AWS box:

| Rank | Server | Config | Installs/s |
|---|---|---|---|
| 1 | **pypiron**† | S3 + presigned redirect (Rust) | **8,288** |
| 2 | devpi | devpi + nginx | 78 |
| 3 | bandersnatch | full static mirror via nginx | 77 |
| 4 | pypiserver | gunicorn + cached-dir | 69 |
| 5 | pypicloud† | S3 + DynamoDB (uwsgi) | 42 |
| 6 | proxpi | flask caching proxy | 32 |

† redirect architecture: the server's job ends at a signed redirect to object
storage, so its installs are counted as **delivery-verified redirects** — a
prober follows a fresh random sample of the actual redirect URLs during every
measurement step and asserts the exact expected bytes come back. Servers that
push wheel bytes through their own NIC are measured delivering those bytes.

!!! note
    The byte-serving servers cluster at 69–78 installs/s because that is the
    box's 12.5 Gbps NIC, not their code — nginx had CPU to spare. That *is* the
    architectural finding: push bytes through the node and the NIC is your
    ceiling; hand bytes to object storage and you scale to CPU. pypicloud is
    the like-for-like peer (it also redirects to S3) — its Python index +
    redirect layer tops out at 42/s, ~200× under pypiron's Rust one.

## Methodology

The comparison is honest by construction. Each competitor runs in its own
documented production topology — right app server, worker count, and the
nginx/DB sidecars it needs. No tool gets a response or edge cache another lacks.
Egress is blocked on every ranking run: a cache miss or upstream fallback fails
loudly instead of being served by a CDN. Every server gets the same frozen,
hash-pinned set of ~100 real projects' wheels, and clients fire byte-identical
requests at each. The load generator (`oha`) replays the real install mix —
index pages and wheel URLs in install proportions, as separate measured
classes — and **installs are counted only from completed wheel fetches**, so a
server cannot look fast by answering cheap index requests while wheel delivery
stalls. For redirect servers, delivery is verified by sampling during load; a
step without verified delivery is discarded.

Each row is that server's own saturation ceiling on the same small box (an
`r7i.large`, 2 vCPU), found by an automatic concurrency search and stamped
server-bound only on step-level evidence (measured CPU-window saturation or a
genuine post-knee decline). The load fleet (4× `c7i.8xlarge`) always had idle
headroom — the limit was the server under test, not the test itself.

## Full results

Methodology, raw numbers, and the AWS rig provenance live in the repo:

- [Benchmark plan](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/BENCHMARK_INSTALL.md)
- [Benchmark results](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/BENCHMARK_RESULTS.md)
