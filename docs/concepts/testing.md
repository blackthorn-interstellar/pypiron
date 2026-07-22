---
description: How pypiron is tested. The real server driven end-to-end by uv, pip, poetry, and twine, adversarially and continuously. Every claim links to a check.
---

# How it's tested

You're about to point every install your team runs at this server. Here's what
stands behind that. Anyone can post a benchmark chart; pypiron is checked
end-to-end, adversarially, and continuously — and every claim below links to a
check you can run yourself.

## Real clients, real server

The test suite doesn't check that pypiron's output *looks* correct. It starts
the real binary and drives it over HTTP with the tools your team actually uses:
uv, pip, poetry, pdm, pipenv, hatch, flit, and twine. A test passes only when a
real client publishes, resolves, and installs a real package against the running
server. No mocks stand in for the clients — mocks would only test our
assumptions about them.

The full client-by-feature matrix, with the exact client versions, is in
[TESTING.md](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#client-compatibility-matrix).

## Every file on PyPI

pypiron parses filenames, wheel tags, and package metadata. If that parsing is
wrong on even a rare shape, an install breaks. So the parsers are run against
[every file ever uploaded to PyPI — all 17 million](https://github.com/blackthorn-interstellar/pypiron/blob/master/src/corpus_check.rs)
and checked against ground truth on each one. It re-runs weekly in CI, so new
packages can't drift out from under it.

## It survives being killed

The one failure that matters for a package server is an index that promises a
file it can't deliver — a broken install. pypiron is built so an interrupted
write can never leave that state, and three suites try hard to prove otherwise:

- **Kill at every step.** The server is `kill -9`'d at each point of every write
  and must come back to a consistent, installable tree every time
  ([crash sweep](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_crash_consistency.py)).
- **A node dies mid-upload.** Several nodes share one bucket while one is killed
  in the middle of an upload; after restart every node serves byte-identical
  indexes and every acknowledged upload still installs from every node
  ([fleet chaos](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_chaos_fleet.py)).
- **A hostile upstream.** When the proxy fetches from PyPI, it's fed truncated,
  corrupt, hash-mismatched, and hanging responses. Each surfaces as an error and
  leaves nothing behind — no half-written file in the cache for a later request
  to serve as good
  ([upstream faults](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_chaos_upstream.py)).

## Adversarial inputs

The code that reads attacker- or upstream-controlled bytes — filename and wheel
parsing, metadata, index rendering, range requests — is exercised by six
coverage-guided fuzzers that run
[every night](https://github.com/blackthorn-interstellar/pypiron/blob/master/.github/workflows/fuzz.yml).
Each one hunts for a crash or a broken invariant.

This isn't theater. Before release, a fuzzer found a real HTML-injection bug in
the index renderer — a crafted name could break out of an HTML attribute. It was
fixed, and the fuzzer that caught it now guards against its return. Honesty beats
polish: the point of fuzzing is to find these before you do.

## Supply-chain hygiene

pypiron guards your supply chain, so its own has to hold up. A new security
advisory anywhere in the dependency tree
[fails the build](https://github.com/blackthorn-interstellar/pypiron/blob/master/.github/workflows/ci.yml)
on every pull request. And Fable 5 — Anthropic's frontier model — ran security
audit pass after pass over the code until the findings came back clean. All told,
over $7,000 of frontier-model compute (at API list prices) went into building and
hardening pypiron.

## Benchmarks you can re-run

The throughput numbers aren't ours to grade. The
[benchmark rigs](https://github.com/blackthorn-interstellar/pypiron/blob/master/bench/install)
are published docker-compose setups for pypiron and all five competitors —
bandersnatch, pypiserver, pypicloud, devpi, and proxpi. Clone the repo and run
them. See [Benchmarks](../reference/benchmarks.md) for the results.

## What runs when

| When | What runs |
| --- | --- |
| Every pull request | Format, lint, unit tests, the full client suite on local disk, S3, and Azure, plus the dependency-advisory gate |
| Nightly | All six fuzzers, coverage-guided |
| Weekly | Full-PyPI corpus check and the client compatibility matrix |
