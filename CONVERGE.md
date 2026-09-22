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
- 2026-09-22 Wrong docs: `docs/security.md` promised the seven-day cooldown for every proxy and `sync` fetch, but `src/sync.rs` keeps any file whose upstream gives no upload time; the page now says so.
- 2026-09-22 Wrong docs (code comments): `PRE_DRAIN_PAUSE` and `AppState::shutting_down` said the drain fails `/health`; it fails `/ready` (`/health` stays 200 by design). Dropped the "malware enforcement lands in a later rung" note; `serve.rs` enforces it.
- 2026-09-22 Wrong docs: `docs/guides/migrate.md`'s devpi/pypicloud `sync --as-private` commands kept the default seven-day cooldown, so a migration silently skipped every private file uploaded in the last week and still exited 0 (reproduced against a fake pypicloud). The commands now pass `--exclude-newer ''` and say why.

## Rejected

- 2026-09-22 Drop the 2.5 s sleep + `total == 0` in `test_head_and_partial_range_are_not_counted`: without it a wrongly counted HEAD flushed before the GET makes the later poll return 1 early and pass — the sleep is the guard.
- 2026-09-22 Delete `provenance::parse_publisher` (4 lines, used only by its own unit tests): same test-helper shuffle as the rejected shims.
- 2026-09-22 Accept a lowercase `basic` auth scheme (RFC 7235 says case-insensitive): no real client (pip, uv, twine, requests) sends it.
- 2026-09-22 Close the intent on `delete_record`'s "no live origin claim" `500`: only reachable with an artifact that has no `.origin` claim, already a broken store; not a client path.
- 2026-09-22 Drop `cargo check` from `make check` (~3.5 s of ~48 s): `clippy --all-targets` unifies dev-dependency features (tokio `test-util`) into the lib build, so only plain `cargo check` catches production code leaning on a test-only feature.
- 2026-09-22 Delete the seven `#[cfg(test)]` compatibility shims in `origin.rs`/`buckets.rs`: they compile only under test, so the "−85 non-test lines" is a counting artifact; moving test helpers is reorganizing, not simplifying.
- 2026-09-19 `--private-prefix` of 255 or 256 bytes now refuses startup because `{prefix}-*` exceeds the 256-byte pattern cap (a regression in c030441). Real, but no deployment has a 255-byte namespace; not worth a change until someone hits it. One-line fix if ever wanted: build the `-*` pattern in `PrivateNames::new` without re-parsing.

## Empty iterations

1 consecutive.

## Open questions

- **Should `sync --as-private` default to no cooldown?** The seven-day
  `exclude-newer` hold exists to keep fresh *public* releases away from
  resolvers, but `sync --as-private` (migrating your own packages off devpi,
  pypicloud, Artifactory or Nexus) inherits it, so anyone not following the
  migration guide verbatim silently leaves out the last week's private uploads
  with a successful exit. The guide now passes `--exclude-newer ''`. Options:
  (A) with `--as-private`, default the cooldown off unless set explicitly —
  ~5-8 lines in `src/sync.rs` (`exclude_newer_input` and the cursor-hash input
  `exclude_newer_raw` must change together) plus a blackbox test; (B) keep the
  default and rely on the guide. Recommendation: A — the hold protects nothing
  for packages you wrote, and the failure is silent. Cost of choosing wrong: low;
  explicit `--exclude-newer` still wins either way.

- **Should a PEP 792 project status be refused on a private package?**
  `POST /project/<pkg>/status` (`write_project_status`, `src/publish.rs`) never
  checks the package's origin, and `pypiron sync` relays upstream statuses
  through it (skipped only with `--as-private`). So a public upstream quarantine
  of a same-named package, synced in, empties the index of *your private*
  package, while `advisory_byte_gate` (`src/serve.rs`) exempts private packages
  and direct file URLs still download. Reproduced: JSON index `files: []`,
  `GET /files/qdemo/...whl` 200. An admin quarantining their own private package
  gets the same half-freeze, contradicting `docs/security.md` ("direct artifact
  requests are refused") and "a private package with the same name is still
  your package". Options: (A) refuse status on a private-origin package with
  `409` (fail closed `503` if the origin read errors); sync then reports an
  error for that package, and admins can no longer hide a private project's
  files via status — ~10 lines plus a blackbox test and a docs note; (B) let
  quarantine also block direct downloads of private packages — but then a synced
  public quarantine fully takes down your private package, which is worse.
  Recommendation: A — PEP 792 status is something pypiron relays for mirrored
  projects, and a public event must never reach a private name. Cost of choosing
  wrong: medium; public endpoint behavior, but a few lines either way.

- **Should `serve` without a proxy apply `[mirror] exclude-packages`?** Today
  `serve` ignores `exclude-packages` unless `--proxy-upstream` is set, but the
  offline `rebuild-index`/`verify-index` read it from the same `pypiron.toml`
  whenever the store has no enforced-excludes stamp (a proxy-less `serve` never
  writes one). So on a proxy-less server that shares its config with `sync`
  (air-gapped setups), `rebuild-index` hides an excluded package, and the next
  upload's rebuild lists it again in full. Reproduced. Options: (A) `serve`
  always applies `exclude-packages` (resolve only the exclude list, so fetch-only
  settings aren't validated offline), matching the maintenance commands and the
  "removes matching names from package listings" sentence — but it would start
  hiding matching packages, private ones included, on proxy-less servers that
  never hid them; (B) `serve` without a proxy writes an empty stamp, so the
  maintenance commands follow `serve` and `[mirror]` stays proxy/sync-only, as
  `docs/reference/configuration.md` scopes it ("`serve --proxy-upstream` and
  `pypiron sync` share `[mirror]`"). Recommendation: B — it keeps today's
  serving behavior and the documented scope, and only stops the offline
  commands from disagreeing. Cost of choosing wrong: low; either is a few lines.

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
