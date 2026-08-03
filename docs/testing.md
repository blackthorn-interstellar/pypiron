---
description: Every release runs the gauntlet — killed at every write, tens of millions of fault schedules, six nightly fuzzers, all of PyPI, frontier-model audits.
---

# Gauntlet testing

Every release runs a gauntlet before a single claim ships: the server killed at
every step of every write, tens of millions of seeded fault schedules, six
nightly fuzzers, every file ever uploaded to PyPI, and security audits by every
frontier model. The checks are public — run any of them yourself.

## Real clients, real server

The test suite doesn't check that pypiron's output *looks* correct. It starts
the real binary and drives it over HTTP with the tools your team uses:
uv, pip, poetry, pdm, pipenv, hatch, flit, and twine. A test passes only when a
real client publishes, resolves, and installs a real package against the running
server. No mocks stand in for the clients — mocks would only test our
assumptions about them.

The core three — uv, pip, and twine — run on every pull request, against local
disk, S3, and Azure. All eight run in the weekly compatibility matrix
([the client-by-feature grid, with exact client versions](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#client-compatibility-matrix)).

## Every file on PyPI

pypiron parses filenames, wheel tags, and package metadata. If that parsing is
wrong on even a rare shape, an install breaks. So every parser runs against
[every file ever uploaded to PyPI](https://github.com/blackthorn-interstellar/pypiron/blob/master/src/corpus_check.rs)
— all 17,130,626 of them — and matches ground truth on every one. The check re-runs weekly
in CI, so new packages can't drift out from under it.

## It survives being killed

The one failure that matters for a package server is an index that promises a
file it can't deliver — a broken install. pypiron is built so an interrupted
write can never leave that state, and three suites try hard to prove otherwise:

- **Kill at every step.** The suite sends `kill -9` at each point of every
  write; the server must come back to a consistent, installable tree every time
  ([crash sweep](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_crash_consistency.py)).
- **A node dies mid-upload.** Several nodes share one bucket while one dies in
  the middle of an upload; after restart every node serves byte-identical
  indexes and every acknowledged upload still installs from every node
  ([fleet chaos](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_chaos_fleet.py)).
- **A hostile upstream.** When the proxy fetches from PyPI, the test feeds it
  truncated, corrupt, hash-mismatched, and hanging responses. Each surfaces as
  an error and leaves nothing behind — no half-written file in the cache for a
  later request to serve as good
  ([upstream faults](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_chaos_upstream.py)).

## Simulated disasters

The chaos suites kill one real process at a time. Two more suites go further:

- **Deterministic simulation.** A whole multi-node fleet lives out disasters —
  crashes, partitions, storage faults, clock jumps — faster than real time in
  [a simulator](https://github.com/blackthorn-interstellar/pypiron/blob/master/examples/vopr.rs),
  and any failure
  [reproduces exactly](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#deterministic-simulation-the-vopr)
  from an 8-byte seed. 22,985,591 seeded fault schedules at the time of
  writing; zero outstanding findings.
- **Model checking.** What the simulator samples, a
  [model checker](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#machine-checked-models-stateright)
  settles: it checks every interleaving of uploads, rebuilds, crashes, and
same-filename collisions within its bounds, not a sample. The checker runs the server's own
  decision code, so the proof can't drift from the binary.

## Adversarial inputs

Six coverage-guided fuzzers run
[every night](https://github.com/blackthorn-interstellar/pypiron/blob/master/.github/workflows/fuzz.yml)
against the code that reads attacker- or upstream-controlled bytes — filename
and wheel parsing, metadata, index rendering, range requests. Each one hunts for
a crash or a broken invariant.

Before release, a fuzzer found a real HTML-injection bug in the index
renderer — a crafted name could break out of an HTML attribute. We fixed it,
and the fuzzer that caught it now guards against its return.

## Supply-chain hygiene

pypiron guards your supply chain, so its own has to hold up. A new security
advisory anywhere in the dependency tree
[fails the build](https://github.com/blackthorn-interstellar/pypiron/blob/master/.github/workflows/ci.yml)
on every pull request. And frontier models audit the code for security — the
same models that built pypiron ran pass after pass until the findings came
back clean. Over $7,000 of frontier-model compute (at API list prices) went into
building and hardening pypiron.

## Benchmarks you can re-run

The throughput numbers aren't ours to grade. The
[benchmark rigs](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/bench/install)
are published docker-compose setups for pypiron and all five competitors —
bandersnatch, pypiserver, pypicloud, devpi, and proxpi. Clone the repo and run
them. Results and the head-to-head feature grid are in
[Compare](compare/index.md).

## What runs when

| When | What runs |
| --- | --- |
| Every pull request | Format, lint, unit tests, the core client suite (uv, pip, twine) on local disk, S3, and Azure, plus the dependency-advisory gate |
| Nightly | All six fuzzers, coverage-guided |
| Weekly | The full-PyPI corpus check and the eight-client compatibility matrix — poetry, pdm, pipenv, hatch, and flit join the core three |
| Continuously | [The deterministic simulator](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#deterministic-simulation-the-vopr), on the order of a hundred thousand seeded fault schedules a night |

Every flag and its `PYPIRON_*` env var is in
[Configuration](reference/configuration.md).
