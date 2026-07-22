# Traffic-replay benchmark

Replays PyPI's **real download stream** against a pypiron server and reports what
it can actually serve — throughput, tail latency, and where it saturates —
instead of the "one box could serve all of PyPI" back-of-the-envelope. Measured
answer (one c7i.2xlarge, disk backend): ~**200k** index reads/s and ~**189k**
metadata/s at p99 <3 ms (≈4× PyPI's whole request rate, fleet-confirmed), and
artifact bytes NIC-bound. The eye-catching "~50 ms artifact stall" in the
loopback numbers below is a **loopback benchmarking artifact**, not a server
ceiling — on a real NIC it's a ~1 % keepalive p99 tail (p50 sub-ms); see
[Measured on AWS](#measured-on-aws-c7i2xlarge) and the
[fleet root-cause](../../dev/BENCHMARK_RESULTS.md#traffic-replay-one-box-vs-pypis-real-request-stream).

The pipeline is three stdlib scripts, in the shape of `dev/bench/meter.py` and
`dev/bench/scale.py`:

```sh
# 1. pull a real trace from the public ClickHouse PyPI dataset (~1-2 min)
uv run -- python dev/bench/replay/trace_build.py --requests 1000000 --date 2026-06-28

# 2. fabricate just the artifacts the trace touches into a data dir
uv run -- python dev/bench/replay/seed.py --data-dir /tmp/replay --max-artifact-mb 0

# 3. boot pypiron against it, then replay at recorded pace and faster
target/release/pypiron serve --data-dir /tmp/replay --admin-user admin \
  --admin-pass secret --bind-addr 127.0.0.1:8080 &
uv run -- python dev/bench/replay/replay.py --speed 1,2,5 --connections 64
```

## What's real and what's modeled

This is the honest core of the methodology. The trace is **not** fully
synthetic and **not** a perfect replay — it's real download events with two
modeled dimensions, each a knob you can turn off.

| Dimension | Source | Real? |
|---|---|---|
| Which files are fetched, and how often | `pypi.pypi` download events, per project-prefix bucket | **real** |
| Installer mix (pip / uv / poetry / …) | same, per event | **real** |
| Artifact vs `.metadata` (PEP 658) split | `.whl.metadata` fetches are their own event rows | **real** |
| Artifact byte sizes | `pypi.projects.size` join, median-by-type fallback | **mostly real** |
| Sub-second arrival timing | homogeneous Poisson over `--window-secs` | **modeled** |
| `/simple/<project>/` index reads | `--index-ratio` per artifact fetch | **modeled** |

Why the two modeled dimensions:

- **Arrival timing.** The public dataset records downloads at *day* resolution —
  there is no per-request timestamp. Within the chosen window we place arrivals
  as a homogeneous Poisson process (uniform times, sorted). Relative frequencies
  between files are real; only the microsecond interleaving is invented. Diurnal
  and burst structure within the window are *not* modeled.
- **Index-page reads.** PyPI's download logs (Fastly → the public dataset) cover
  *file* downloads on `files.pythonhosted.org`; they don't include
  `/simple/` index hits served from `pypi.org`. Every resolve fetches an index
  first, so we synthesize `--index-ratio` index reads per artifact fetch, with
  projects drawn by their real download weight. Set `--index-ratio 0` to replay
  only the real file stream.

If you need a pure, no-modeling run: `--index-ratio 0` removes the synthetic
tier, leaving a real (but Poisson-timed) replay of the file-download stream.

## Data source and the read-cap workaround

The trace comes from the same public ClickHouse playground the corpus check uses
(`src/corpus_check.rs`): the `pypi.pypi` table, one row per download event. The
demo user breaks any scan at 10B rows read, and a single day is ~19B rows, so a
whole-day `GROUP BY` returns a **non-deterministic partial** result (two runs
gave completely different top-10s). `project` is the first primary-key column,
so `trace_build.py` slices the day into ~37 project-prefix buckets; each slice
prunes to ~1-3B rows and comes back **complete and deterministic**, and the
buckets are disjoint so nothing is double-counted. This captures the head and
near tail of the popularity curve — not the full cold tail (each bucket is
`--per-bucket`-limited).

`manifest.json` records provenance, the real/fallback size split, and the
caveats above for every trace.

## Reading the replay output

`replay.py` is **open-loop**: a scheduler fires each request at its trace time
(÷ speed) regardless of whether the server is ready, and a fixed pool of
keep-alive connections services them. This is how real traffic behaves — clients
arrive on their own schedule — and it's the opposite of `oha` (used by
`meter.py`), which offers exactly N requests in flight and so can't overload a
server. `oha` is right for hammering one URL; a trace is a stream of thousands of
different URLs, so this uses a small asyncio client instead.

Per tier (`index` / `artifact` / `metadata`) you get rps, p50/p95/p99 service
latency, 2xx/3xx vs error counts, and bytes moved. The headline signal is
**`max_backlog`**: when it stays near zero the server is keeping pace at that
speed; when it climbs, offered load has passed the server's service rate — that
speed is past saturation, and the latency tail is queueing, not service time.
Sweep `--speed` upward until backlog takes off to find the ceiling.

## Local validation run

10k-request trace (1-hour window), seeded with 8 MB-capped artifacts, replayed
against a release binary on an M-series MacBook (disk backend, which always
streams artifact bytes — no S3 redirects):

```
trace:  112,706 real file groups covering 3.9B download events (2026-06-28)
        15,470 requests = 5,470 artifact + 4,530 metadata (real) + 5,470 index (modeled)
seed:   4,082 artifacts / 1,626 MB in 1.7s
```

| speed | wall | total rps | max backlog | artifact p50 / p99 | errors |
|---|---|---|---|---|---|
| x200  | 18.0s | 859   | 8      | 1.2 ms / 34.6 ms   | 0 |
| x1000 | 6.4s  | 2,420 | 6,696  | 16.9 ms / 684 ms   | 0 |
| x3600 | 6.5s  | 2,400 | 12,599 | 17.0 ms / 693 ms   | 0 |

At x200 the box keeps up (backlog ~8, all 200s). Past ~2,400 rps the artifact
tier is disk/loopback-bound (~1.7 GB moved ≈ 2.1 Gbps on loopback) and backlog
climbs — the honest per-box ceiling on this laptop, replacing the old arithmetic.

## Measured on AWS (c7i.2xlarge)

Full write-up and provenance:
[dev/BENCHMARK_RESULTS.md](../../dev/BENCHMARK_RESULTS.md#traffic-replay-one-box-vs-pypis-real-request-stream).
Single c7i.2xlarge (8 vCPU, 15 GB), disk backend, 1M-request trace for
2026-06-28 seeded at **real sizes** (41,507 artifacts, 70.5 GB). Loadgen
co-located over loopback, so these are server CPU/disk ceilings (the NIC is not
exercised); PyPI's real pace that day was ≥46,500 file req/s.

| tier | conns | rps | p99 | vs PyPI pace |
|---|---|---|---|---|
| `/simple/` index | 256 | **202,069** | 2.62 ms | ~4.3× |
| PEP 658 metadata | 128 | 188,546 | 1.39 ms | ~4.1× |
| artifact, 100 KB wheel | 128 | 2,376* | 69.5 ms* | *loopback artifact |
| artifact, 32 MB stream | 8 | 24 | 410 ms | 6.25 Gbps |

Index and metadata clear PyPI's whole request rate ~4× over with p99 under 3 ms
— fleet-confirmed on 12 instances. Artifact bytes are NIC-bound (that's what
`--artifact-delivery redirect` is for).

*The 100 KB artifact p50 (~50 ms, so ~2,376 rps at 128 conns) is a **loopback
benchmarking artifact, not a server ceiling.** Loopback's giant ~65 KB MSS makes
every ~100 KB response fill the client's TCP receive window at a delayed-ACK
boundary, so the server flow-control-stalls one delayed ACK (~50 ms) per request
— on *every* request, which is what tanks the loopback p50. A 12-instance fleet
study (`tcpdump` + toggles + a 2-box real-NIC run) pinned it down: it is a
receive-window × delayed-ACK interaction (**not** Nagle — `TCP_NODELAY` doesn't
fix it; only the client's `TCP_QUICKACK` does), and **over a real NIC it
collapses to a ~1 % keepalive p99 tail with a sub-ms p50**. Full story and
root-cause in
[dev/BENCHMARK_RESULTS.md — fleet root-cause](../../dev/BENCHMARK_RESULTS.md#traffic-replay-one-box-vs-pypis-real-request-stream).
Treat this row as "measure it on your own NIC topology," not as pypiron's
small-artifact throughput.

**Loadgen choice:** to drive a beefy server to its ceiling, use `oha` (Rust,
multi-core) as above — `replay.py` is single-threaded and measures the
mix/popularity realistically but tops out on one core well below a big server's
request ceiling.

## Knobs

`trace_build.py`: `--date`, `--requests`, `--per-bucket`, `--window-secs`,
`--index-ratio`, `--max-groups`, `--seed`.
`seed.py`: `--max-artifact-mb` (0 = real sizes, costs real disk), `--workers`.
`replay.py`: `--speed` (comma list), `--connections`, `--output`.
Every default is set for a quick local run; scale `--requests`, drop
`--max-artifact-mb 0`, and raise `--connections` for a rig measurement.
