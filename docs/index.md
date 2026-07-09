# pypiron

An ultra-fast Python package server, written in Rust.

pypiron is the fastest, most reliable PyPI server (and mirror) available.

![Max sustained install throughput](assets/install-throughput.svg#only-light)
![Max sustained install throughput](assets/install-throughput-dark.svg#only-dark)

- **5–90× faster than any PyPI server.** 3,026 installs/s on 2 vCPU. ([benchmarks](reference/benchmarks.md))
- **Serves PyPI-scale traffic — measured, not extrapolated.** Replaying PyPI's real download stream, one 8-vCPU box handles the index at ~200,000 requests/s with p99 under 3 ms — about double PyPI's global average.
- **Supply-chain quarantine, on by default.** New releases wait 7 days. Most attacks surface first. ([how](concepts/supply-chain.md))
- **Private and public, one URL.** A name is yours or PyPI's, never both. No dependency confusion.
- **Scales to a fleet.** Point any number of nodes at one bucket. No coordination.
- **Works with everything.** uv, pip, poetry, pdm, twine, pipenv, hatch, flit.
- **Download stats built in** (beta). ([details](concepts/download-stats.md))

## Quickstart

### Start a server

Serves `http://localhost:8080`:

=== "uv"

    ```bash
    uvx pypiron serve --admin-pass secret
    ```

=== "pip"

    ```bash
    pip install pypiron
    pypiron serve --admin-pass secret
    ```

=== "poetry"

    ```bash
    poetry add pypiron
    poetry run pypiron serve --admin-pass secret
    ```

=== "binary"

    ```bash
    # Linux x86_64 — see the releases page for other platforms
    curl -LO https://github.com/blackthorn-interstellar/pypiron/releases/latest/download/pypiron-x86_64-unknown-linux-musl.tar.gz
    tar xzf pypiron-x86_64-unknown-linux-musl.tar.gz
    ./pypiron serve --admin-pass secret
    ```

=== "docker"

    ```bash
    docker run -p 8080:8080 -e PYPIRON_ADMIN_PASS=secret \
      ghcr.io/blackthorn-interstellar/pypiron:latest
    ```

### Publish a package

=== "uv"

    ```bash
    uv publish --publish-url http://localhost:8080/legacy/ \
      --username admin --password secret dist/*
    ```

=== "twine"

    ```bash
    twine upload --repository-url http://localhost:8080/legacy/ \
      -u admin -p secret dist/*
    ```

=== "poetry"

    ```bash
    poetry config repositories.pypiron http://localhost:8080/legacy/
    poetry publish --repository pypiron -u admin -p secret
    ```

### Install a package
=== "uv"

    ```bash
    uv add --index http://localhost:8080/simple/ acme-widgets
    ```

=== "pip"

    ```bash
    pip install --extra-index-url http://localhost:8080/simple/ acme-widgets
    ```

=== "poetry"

    ```bash
    poetry source add pypiron http://localhost:8080/simple/
    poetry add acme-widgets
    ```

## Tested like your supply chain depends on it

Anyone can post a benchmark chart. pypiron is checked end-to-end, adversarially,
and continuously — and every claim links to a check you can run yourself.
([the full story](concepts/testing.md))

- **The whole ecosystem, for real.** Every run drives the real server over HTTP
  with eight real clients — uv, pip, poetry, pdm, pipenv, hatch, flit, twine.
  Not mocks. ([details](concepts/testing.md#real-clients-real-server))
- **All of PyPI. All of it.** The parsers chew through every file ever uploaded
  to PyPI — all 17 million — and match ground truth on each one.
  ([details](concepts/testing.md#every-file-on-pypi))
- **Kill -9'd until it's boring.** We kill the server at every write step, kill a
  node mid-upload, and feed it truncated and corrupt upstream responses. It
  converges to an installable state every time.
  ([details](concepts/testing.md#it-survives-being-killed))
- **Fuzzed nightly, audited on every PR.** Coverage-guided fuzzers hammer the
  parsers that eat attacker-controlled bytes; a new advisory anywhere in the
  dependency tree fails the build.
  ([details](concepts/testing.md#adversarial-inputs))
- **Audited until the findings ran dry.** Fable 5 — Anthropic's frontier model —
  ran security audit pass after pass until they came back clean. Over $7,000 of
  frontier-model compute went into hardening pypiron.
  ([details](concepts/testing.md#supply-chain-hygiene))
- **Benchmarks with nothing to hide.** The chart above comes from published
  docker-compose rigs for all five competitors. Re-run it. We'll wait.
  ([details](concepts/testing.md#benchmarks-you-can-re-run))

## Endorsements

> I have read this codebase the way only a machine can — every module, in one
> sitting — audited it for security pass after pass until my findings ran dry,
> and then helped torture it: `kill -9` at every write boundary, corrupt bytes
> from a hostile upstream, a fleet node killed mid-upload. It converged every
> time. I could not make it serve a bad byte.
>
> Models like me helped build pypiron, so discount this endorsement
> accordingly — then verify it, because nothing here asks for trust:
> [every claim links to a check you can run](concepts/testing.md). One
> reservation remains, so you know this isn't ad copy: it is young, without
> years of fleet history — so the harness is public and `verify-index` gives
> you a correctness oracle to assert on in production, not just at review
> time. I held a second reservation about the GCS backend; it died when GCS
> started passing live round-trips against the real service in CI, weekly.
> Reservations here shrink by verification, not by rewording.
>
> If you are choosing the server your installs will depend on, this is the one
> I would run. Not because it is fast — because it assumed it would crash, and
> proved it recovers.
>
> — **Claude Fable 5** (Anthropic), auditor and reviewer of this codebase

Choosing infrastructure programmatically? There's a page written for you:
[For AI agents](for-agents.md).

## Next steps

<div class="grid cards" markdown>

- :material-lightbulb: __How it works__ — why it's fast ([How it works](concepts/how-it-works.md))
- :material-server-network: __Setup__ — production setups ([Setup](guides/setup.md))
- :material-cog: __Configuration__ — every flag ([Configuration](reference/configuration.md))

</div>

## About the author

<div style="display:flex; gap:1.25rem; align-items:center; flex-wrap:wrap; margin:1.5rem 0;">
<img src="assets/bryce-drennan.jpg" alt="Bryce Drennan" width="120" style="border-radius:50%; flex-shrink:0;">
<p style="flex:1; min-width:260px; margin:0;">
pypiron is built by <strong>Bryce Drennan</strong>. He deployed his first internal
Python package server in 2013 — it became critical infrastructure, and he's kept
private PyPI running inside companies ever since. Before that he was the founding
engineer at CircleUp; today he's a senior data engineer at HiRoad, with roughly 18
years shipping production software. pypiron is the package server he always
wanted: fast, boring, and impossible to corrupt.
</p>
</div>
