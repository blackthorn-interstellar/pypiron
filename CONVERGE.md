# Converge

State for the `/converge` loop. `dev/VISION.md` is the fixed reference; this
file is the loop's memory. Only a human raises the ceiling.

## Ceiling

- Non-test lines: **45330** measured 2026-09-19 at `74b7d62`.
- Ceiling: **47597** (measured + 5%).
- Measure with, from the repo root:

```
for f in $(find src -name '*.rs'); do awk '{ if (pending && /^(pub )?mod /) exit; if (pending) n++; pending = ($0 ~ /^#\[cfg\(test\)\]$/); if (!pending) n++ } END { print n+0 }' "$f"; done | paste -sd+ - | bc
```

(Every `src/**/*.rs` line before the file's `#[cfg(test)] mod …` block.)

- Test lines: **48928** measured 2026-09-22 at `f86345a`.
- Ceiling: **51374** (measured + 5%).
- Measure with, from the repo root (all `src/**/*.rs` lines minus the non-test
  count above, plus the Python blackbox suite):

```
echo "$(cat $(find src -name '*.rs') | wc -l) - <non-test count> + $(find tests -name '*.py' | xargs cat | wc -l)" | bc
```

## Done

- 2026-09-19 Bug: an invalid inline `--private-pattern` / TOML `private-patterns` entry (`#bolt`, empty) was silently dropped by the pattern-file comment filter, so a name the operator believed reserved could fall through to the proxy; now refuses startup naming the entry. Unit test on `PrivateArgs::resolve` and a blackbox startup-refusal test were red first. Lines 45330 -> 45339.

- 2026-09-22 Wrong docs: `docs/for-agents.md` claimed every `--flag` has a `PYPIRON_FLAG` env var; false for `create-token` and not mechanical (`--from` is `PYPIRON_SYNC_FROM`), so an agent's guessed name is silently ignored. Row now points at the configuration reference. Lines unchanged.
- 2026-09-22 Wrong docs: `docs/reference/configuration.md` said a failed storage delete returns `500`; the probe, intent marker, origin reads and the replication-gap record return `503`, the last after the file is already gone. Doc now names both codes and what a `404` retry means. Lines unchanged.
- 2026-09-22 Bug: `pypiron healthcheck` (the Docker HEALTHCHECK) took its port only from `PYPIRON_BIND_ADDR`, so a container whose port was set in `pypiron.toml` was marked unhealthy forever; now falls back to the config's `[serve] bind-addr`, and the Compose recipe passes the file via `PYPIRON_CONFIG` so the probe can see it. Blackbox test red first.
- 2026-09-22 Wrong docs: `docs/guides/multi-region.md` told operators with unprefixed private names to exclude them from the proxy, which neither reserves the name nor blocks a mirror claim, and delists the real private files; it now points at `private-patterns`, which shares the prefix's reservation matcher.
- 2026-09-22 Bug: a refused upload (duplicate-file `409`, lost first claim `403`/`409`) left its intent marker open, so the worker deferred the package's index rebuilds for the full 900 s intent grace — a CI retry's `409` hid the next release. Refusals now close the marker; the not-reserved `403` moved ahead of the marker and an unreachable owner-mismatch arm went. Blackbox test red first.
- 2026-09-22 Wrong docs: `docs/reference/configuration.md` listed the CLI-only sync switches without `--repair-upload-times`, which `src/config.rs` refuses in the file.
- 2026-09-22 Bug: with multiple buckets, a refused yank/unyank/upload-time repair (`404` missing file, `409` repair refusals, CAS exhaustion) left the intent marker `edit_sidecar` opens up front, stalling the package's index for the 900 s intent grace; refusals now close it. The multi-bucket fixture's 3 s reconcile masked it; new test uses the production interval and was red first.

## Rejected

- 2026-09-22 Close the intent on `delete_record`'s "no live origin claim" `500`: only reachable with an artifact that has no `.origin` claim, already a broken store; not a client path.
- 2026-09-22 Drop `cargo check` from `make check` (~3.5 s of ~48 s): `clippy --all-targets` unifies dev-dependency features (tokio `test-util`) into the lib build, so only plain `cargo check` catches production code leaning on a test-only feature.
- 2026-09-22 Delete the seven `#[cfg(test)]` compatibility shims in `origin.rs`/`buckets.rs`: they compile only under test, so the "−85 non-test lines" is a counting artifact; moving test helpers is reorganizing, not simplifying.
- 2026-09-19 `--private-prefix` of 255 or 256 bytes now refuses startup because `{prefix}-*` exceeds the 256-byte pattern cap (a regression in c030441). Real, but no deployment has a 255-byte namespace; not worth a change until someone hits it. One-line fix if ever wanted: build the `-*` pattern in `PrivateNames::new` without re-parsing.

## Empty iterations

0 consecutive.

## Open questions

- **Should failed logins on reads be logged by default?** `docs/security.md`
  promises "the access log records failed logins as `401` and throttled requests
  as `429` at `info` level for fail2ban or SIEM rules." With the default
  (`--access-log` off) the middleware in `src/app.rs` (`log_requests`) only
  considers mutations, so a wrong install credential on `GET /simple/<pkg>/` or
  `GET /files/...` — the read-side brute force a reader credential exists to
  defend — is never logged, and neither is its `429`. Options: (A) also log a
  read when it presented credentials and ended `401`/`429` (the credential
  check `login_ip` is already computed on the hot path, so anonymous reads stay
  free; a small code change plus a blackbox test); (B) narrow the doc to
  "mutations by default; every request with `--access-log`". Recommendation: A —
  the sentence exists for fail2ban rules, and the rules are only useful if the
  guess-heavy path is in the log. Cost of choosing wrong: low either way.
