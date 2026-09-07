---
description: Real package clients, crash tests, exhaustive models, fuzzers, the full PyPI filename corpus, and reproducible benchmarks.
---

# Gauntlet testing

**pypiron is tested as a running package server, under failures that break real
installs.** The test code and benchmark rigs are public.

## Real clients against the real server

Black-box tests start the compiled binary and drive it over HTTP with uv, pip,
poetry, pdm, pipenv, hatch, flit, and twine. Across the suite, they publish,
resolve, download, and install real packages against local disk and object
storage.

uv, pip, and twine run on every pull request. The
[weekly compatibility matrix](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#client-compatibility-matrix)
runs all eight clients against disk and records their exact versions.

## Every PyPI filename

The parser corpus contains all 17,130,626 filenames published to PyPI when the
corpus was built. Weekly CI checks package names, versions, wheel tags, and file
types against that corpus so unusual historical packages do not become parser
bugs.

## Crashes and storage failures

- The [crash sweep](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_crash_consistency.py)
  kills the server at every step of each write, then checks that every index
  entry still points to an installable file.
- The [fleet tests](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_chaos_fleet.py)
  kill nodes during uploads and require every acknowledged upload to survive.
- The [upstream fault tests](https://github.com/blackthorn-interstellar/pypiron/blob/master/tests/test_chaos_upstream.py)
  send truncated, corrupt, hash-mismatched, and stalled responses. Failed
  downloads must leave no usable cache entry.

## Simulation and model checking

The deterministic [VOPR simulator](https://github.com/blackthorn-interstellar/pypiron/blob/master/examples/vopr.rs)
runs multi-node crashes, partitions, storage failures, and clock changes. An
8-byte seed reproduces each run exactly.

[Stateright models](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md#machine-checked-models-stateright)
exhaust every ordering within a bounded state space, including uploads,
rebuilds, crashes, and same-filename collisions. They call the same decision
code used by the server.

## Fuzzing and dependency checks

Eight coverage-guided fuzzers run nightly against parsers, metadata, index
rendering, and range requests. One found an HTML attribute-injection bug before
release; its regression case remains in the suite.

Every pull request also runs `cargo audit` without ignored advisories.

## AI development and review

pypiron was built and reviewed by coding agents from Anthropic, OpenAI, xAI,
and Moonshot. Frontier models have repeatedly audited the code for security,
and the resulting fixes are in the public commit history. These are reviews by
the same class of systems that built the software, not independent human
security audits.

## Reproduce the benchmarks

The [benchmark rigs](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/bench/install)
run pypiron and five other Python package servers under the same workloads.
See [Comparison and benchmarks](compare/index.md) for results.

Detailed commands, schedules, and test architecture live in the
[contributor testing guide](https://github.com/blackthorn-interstellar/pypiron/blob/master/dev/TESTING.md).
