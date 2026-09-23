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
- 2026-09-22 Missing capability (VISION: "waits for index visibility before returning 200"): `--wait-on-upload` polled the stored index, but every node serves installs from its TTL index cache, so a warm follower served the old index after the 200 (2/20 at the 1 s default, 16/20 at 5 s). The wait now outlasts one cache TTL once the file is in storage. Two-node test red first.
- 2026-09-22 Simplify: `token.rs` carried a byte-for-byte copy of `hash::hmac_sha256`, and `reqsign.rs`/`attest.rs` their own `sha256_hex`/`hex`/`hex_lower`; all now use `crate::hash`. Signing KATs and token tests unchanged. Non-test lines −49 (45731).
- 2026-09-22 Wrong docs: `docs/reference/configuration.md` said a username tag (`reader+billing-api`) is recorded "in request metrics"; `/metrics` reads it only with `--metrics-project-labels` (default off). Now names the access log and the flag.
- 2026-09-22 Delete: `make compat` ran serially (`-n 0`) to feed a ~130-line conftest writer for `docs/reference/compatibility.md`, a page removed from the manual in `25d8a82`; CI discarded the output and local runs left an untracked file. Writer, result collection and marker-label validation gone; `make compat` 66 s -> 10 s, same 21 passed / 1 skipped.
- 2026-09-22 Delete: `tenacity` and `requests` sat in the `dev` dependency group since `114000b` and were never imported (`git log -S` empty); dropped, `tenacity` leaves `uv.lock` (`requests` stays via `twine`).
- 2026-09-22 Delete: fixture `s3_server_multi_reconcile_cost` lost its only test in `3ecef9a`; removed (−16 test lines).
- 2026-09-22 Bug: the advisory leader remembered the feed's HTTP ETag before validating and persisting the new snapshot, so one failed storage write turned every later poll into a `304` and the delivered advisory never reached the byte gate until the feed changed. The ETag is now kept only once the bytes are persisted or already loaded. Blackbox test (read-only `_advisories/` for one poll) red first.
- 2026-09-22 Bug: counter compaction summarized a day as soon as any of its shards froze, even when another closeable shard's read or freeze failed that pass; the local summary healed next pass, but summaries replicate copy-if-absent, so peers kept the undercounted day forever. A day with an unfrozen closeable shard is now left unsummarized until a pass freezes them all. Unit test red first.
- 2026-09-22 Bug: an upload's `name` field overrode the wheel filename's project without comparison, so `other-1.0-py3-none-any.whl` sent with `name=demo` was stored and listed under `/simple/demo/` (PyPI refuses this). Non-mirror wheel uploads whose filename project differs from `name` now get `400`; mirror uploads and legacy formats keep the field's word. Blackbox test red first; `test_copy_escaped_keys` moved its escaped bytes into the platform tag.
- 2026-09-22 Bug (security, fail-open): a node with no admin/uploader credential is documented read-only, but `is_admin`/`is_uploader` honored admin and uploader `__token__`s signed with its key — including ones minted on a write-enabled peer sharing the signing key — so a "read-only" replica accepted uploads, yanks and deletes. Token roles now count only for write roles this node has enabled. Blackbox test (two nodes, one key) red first.
- 2026-09-22 Bug: `origin release` — a bucket whose post-CAS verification listing failed after it wrote `unclaimed` returned `Err` without restoring itself, and the CLI rolls back only buckets that returned `Ok`, so it stayed released while the command reported failure. The release now restores its own claim before failing. Unit test (new `InMemStorage::fail_lists_after_cas` hook) red first.
- 2026-09-22 Bug: `buckets migrate` refused to drop a bucket holding the sole copy of an artifact but ignored fences, so a delete (tombstone) or freeze acknowledged while the fleet ran on that bucket alone — the documented evacuation — was lost and the file resurrected from the survivors. Tombstone, frozen and mirror-quarantined keys now count as unique content. Unit test red first.
- 2026-09-22 Bug: a deleted `simple/index.html` beside a populated `simple/index.json` was read as "never published" and left missing, even by `rebuild-index`, so HTML clients got `404` on `/simple/`. `reconcile_global_html` now recreates it when the name set is non-empty (the HTML is always written before the JSON, so that pair can only come from a deletion). Blackbox test red first; 3-minute vopr soak clean. A running disk server still trusts its in-memory "HTML current" memo until restart or `rebuild-index`.
- 2026-09-22 Bug: a package whose `packages/<pkg>/` and `simple/<pkg>/` were both removed out of band stayed in both global indexes forever (`verify-index` red after every `rebuild-index`): the audit walks only listed truth/views, so the name produced no dead observation. A clean full audit now proves each unwalked global name absent and removes it through the existing re-proved path. Blackbox test red first; vopr soak clean.
- 2026-09-22 Bug: `verify-chain --strict` read a committed file's sidecar sha as proof of presence, so an artifact deleted out of band beside its surviving `.meta.json` verified clean (exit 0), against the docs' "changed or missing content exits 1". A matching sidecar now also needs the artifact to exist; otherwise the tombstone/demotion check decides covered vs `vanished`. Unit test red first; reproduced and fixed on a real store.
- 2026-09-22 Bug: `sync`/`[mirror]` opt-in bools (`as-private`, `allow-insecure-source`, `allow-legacy-versions`, `exclude-dev`, `exclude-windows`, `exclude-prereleases`, `include-yanked`) merged as `cli || file`, so an explicit `PYPIRON_X=false` could not override `true` in `pypiron.toml`, against the documented CLI > env > file precedence (e.g. plaintext source credentials stayed allowed). A file value is now dropped when the CLI/env set that bool; `serve` shares the `[mirror]` path. Blackbox test red first.
- 2026-09-22 Bug: download read-through fell back to the write pin whenever the read pin said "not visible", including when the read pin held a tombstone or freeze — so a delete a failed-over node landed on the region bucket, not yet replicated to the write home, was served (200, deleted bytes) by a node that had just seen the tombstone. Read-pin fences now end the request; only absence reads through. Blackbox test (read-affinity pair) red first.
- 2026-09-22 Bug (security, fail-open): the malware/quarantine byte gate skipped any name matching `--private-prefix`/`--private-pattern` before reading the actual owner, so reserving a name that already held cached public (mirror-claimed) bytes made a blocked wheel download again (200). The origin claim alone now exempts a package. Blackbox test (restart with the name reserved) red first.
- 2026-09-22 Bug: with `--malware-block=false` (documented: keep the audit, drop the refusal) the `/audit` report, `/audit.json` and the project page still marked `MAL-*` matches `blocked` while the byte gate served them; `blocked` is documented as "whether the byte gate would 403 this file". Both builders now take the blocking toggle; quarantine still blocks. Unit tests extended.
- 2026-09-22 Bug: `pypiron config init` shows the disk default as `data-dir = "~/.pypiron/packages"`, but a config-file `~` is never shell-expanded, so uncommenting that line made `serve`/`rebuild-index`/`verify-index` use a literal `./~/.pypiron/packages` under the working directory — packages seemed to vanish and `verify-index` passed an empty store. A leading `~`/`~/` in the data dir now expands to `$HOME`. Blackbox test red first; 10-minute vopr soak at `4d06750` clean (~194k seeds).
- 2026-09-23 Bug: `sync --admin-pass` without `--admin-user` silently sent no credential (`with_admin_auth` needs both) and then reported the destination "rejected the admin credentials" (401), while `serve` treats a lone `--admin-pass` as user `admin`; a lone `--admin-user` was likewise dropped instead of refusing (AGENTS.md: half-configured credentials refuse). Sync now uses `serve`'s rule and refuses a password-less username. Found by a hands-on run against real PyPI; blackbox test red first.

## Rejected

- 2026-09-22 Refuse `POST /project/<pkg>/status` for a package with no files (it leaves a stray `.project-status.json` that `verify-index` counts as a package): `sync` relays upstream status for projects whose files were all filtered out, so a 404 would fail those runs; the stray file is not a divergence.
- 2026-09-22 Read `/stats` history from every stored bucket variant, not just configured bucket tags (after removing a bucket, its replicated day rollups stay stored but stop counting): real, but fixing it costs a LIST per stats query or a tag registry maintained by compaction, for best-effort counters after a rare topology change; re-adding the bucket restores the view.
- 2026-09-22 Re-run the advisory gate after a buffered proxy fill (an advisory landing mid-download of a sub-16 MiB wheel is not applied to that one response): the window is a single download of seconds, far below the probe's 120 s cadence, so it is indistinguishable from the request arriving a moment earlier; no deterministic test is possible without timing an advisory into a throttled fetch.
- 2026-09-22 Read-through to every reachable peer when a file is on neither pin (an upload a failed-over node landed on a third bucket during a partition): the window lasts only until the `_repl/` sweep delivers it, and probing every peer on each unfenced miss would make every genuine `404` cost cross-region GETs. The guide's "complete bucket" is the write home by design.
- 2026-09-22 Map an upstream project-index `410 Gone` to not-found (`src/simple.rs` ~232): a cold `410` already answers `404`; only a previously cached listing is revived as stale, and PyPI answers removed projects with `404`, so only an exotic upstream hits it — not worth a 60 s-TTL test.
- 2026-09-22 "An audit rebuild racing a yank records a fingerprint over stale views, freezing the pre-yank index" (`src/worker.rs` audit, single bucket where no rebuild intent fences it): plausible by reading, but unreproduced, and the vopr — which runs the audit concurrently with the tick and counts `concurrent-race` view repairs — reported 0 across ~280k seeds today. Revisit only with a failing seed.
- 2026-09-22 Verify `md5_digest`/`blake2_256_digest` on upload like PyPI: every real client also sends `sha256_digest`, which is verified; a second weaker digest adds nothing. Also `fold_version` treating `1!2` as `1.2`: needs a hand-crafted epoch mismatch no build tool emits.
- 2026-09-22 Make `sync` fail when the destination answers `409` for a filename an admin deleted there: the delete deliberately bars the filename (PyPI semantics); failing would make every later run of that package error forever.
- 2026-09-22 Trim `make check` time: the only sizable test cost is the two model checkers (`tests/model_event_protocol.rs` 8.7 s, `tests/model_replication.rs` 6.9 s of ~16 s `cargo test`); they guard the replication protocol on every change, so the time is the point.
- 2026-09-22 Strip the `integration`/`chaos` marker labels (nothing selects on them) and the `compat(client, feature)` arguments: 77 lines of churn for no gain; they still document coverage.
- 2026-09-22 Drop the 2.5 s sleep + `total == 0` in `test_head_and_partial_range_are_not_counted`: without it a wrongly counted HEAD flushed before the GET makes the later poll return 1 early and pass — the sleep is the guard.
- 2026-09-22 Delete `provenance::parse_publisher` (4 lines, used only by its own unit tests): same test-helper shuffle as the rejected shims.
- 2026-09-22 Accept a lowercase `basic` auth scheme (RFC 7235 says case-insensitive): no real client (pip, uv, twine, requests) sends it.
- 2026-09-22 Close the intent on `delete_record`'s "no live origin claim" `500`: only reachable with an artifact that has no `.origin` claim, already a broken store; not a client path.
- 2026-09-22 Drop `cargo check` from `make check` (~3.5 s of ~48 s): `clippy --all-targets` unifies dev-dependency features (tokio `test-util`) into the lib build, so only plain `cargo check` catches production code leaning on a test-only feature.
- 2026-09-22 Delete the seven `#[cfg(test)]` compatibility shims in `origin.rs`/`buckets.rs`: they compile only under test, so the "−85 non-test lines" is a counting artifact; moving test helpers is reorganizing, not simplifying.
- 2026-09-19 `--private-prefix` of 255 or 256 bytes now refuses startup because `{prefix}-*` exceeds the 256-byte pattern cap (a regression in c030441). Real, but no deployment has a 255-byte namespace; not worth a change until someone hits it. One-line fix if ever wanted: build the `-*` pattern in `PrivateNames::new` without re-parsing.

## Empty iterations

0 consecutive. (2026-09-22: a 10-minute `make vopr-soak` at `b17de0d` ran 235,576 seeds, 0 failed, 0 ack-totality misses.)

## Open questions

- **Should the transparency chain stop treating a later checkpoint as
  permission for a committed file to disappear?** `verify-chain` replays the
  chain last-write-wins (`replay`, `src/transparency.rs`), so a newer link that
  lists a package with fewer files — or `{"pkg": {}}` — silently drops the
  earlier commitments from verification. Someone who holds storage credentials
  can't rewrite locked links, but they can append one, then delete the files,
  and `verify-chain --strict` passes. The design doc defines a violation as "a
  committed filename vanished with no marker authorizing it". Options: (A)
  verify every filename ever committed (keep its last sha across links), with
  a tombstone or demotion fence as the only authorization — but single-bucket
  mirror-cache deletes write no tombstone today, so each would start alarming
  unless they gain a marker (a small eviction marker, ~20 lines + tests); (B)
  keep the chain delta as the authorization and document that append rights
  are enough to retire commitments. Recommendation: A with an eviction marker —
  the chain exists to catch a storage-credential attacker, and append is the
  one write Object Lock still allows. Cost of choosing wrong: medium; it changes
  what `verify-chain` flags on existing stores.

- **Should `sync` fail when the source and destination hold different bytes
  under the same filename?** `sync` skips any file whose *filename* the
  destination already lists (`src/sync.rs` ~2305), and the server's `409` on
  a re-upload is treated as "already present" too, so a source file whose bytes
  changed after migration (devpi volatile indexes allow overwriting a release)
  is silently left at the old bytes while the run exits 0.
  `docs/guides/migrate.md` says "artifact bytes and hashes are preserved".
  Options: (A) when both sides publish a sha256 and they differ, count the file
  as an error (exit nonzero, cursor withheld) naming both hashes — pypiron still
  never overwrites; (B) log a warning and keep exit 0; (C) leave as is.
  Recommendation: A — a migration that verifies nothing about divergent bytes
  should not report success; ~10 lines + a blackbox test. Cost of choosing
  wrong: low.

- **Should an advisory snapshot reload keep the malware probe's newer blocks?**
  The per-node probe blocks a newly published `MAL-*` release within minutes,
  ahead of the daily OSV snapshot. When a new snapshot loads, `src/advisories.rs`
  (~1027) clears the probe overlay on purpose ("the next probe backfills anything
  still newer"). If that snapshot predates an advisory the probe already
  applied, the release downloads again until the next successful probe — about
  2 minutes normally, indefinitely while the probe endpoint is down. That window
  contradicts `docs/security.md` ("a cached file that becomes known malware stops
  downloading"). Options: (A) on reload, keep overlay rules whose advisory id the
  new snapshot does not contain (fail-closed; a withdrawal inside the snapshot
  still removes it) — ~10 lines plus a unit test; (B) keep the designed window
  and say in `docs/security.md` that a snapshot swap can briefly reopen a
  probe-only block. Recommendation: A — a security block should never reopen
  because newer-but-staler data arrived. Cost of choosing wrong: low.

- **Add guards for three unguarded security invariants? (the loop's rules bar
  "coverage for its own sake", so this needs your call)** A 30-mutation break
  test found three one-line regressions no test catches, each a fail-open:
  (1) deleting the length check in `token::ct_eq` (`src/token.rs`) makes an
  empty password match any secret — admin bypass with the default `admin`
  user — and accepts forged tokens with an empty MAC; existing tests' wrong
  secrets all differ inside the shared prefix. (2) Loosening `is_uploader` /
  `is_admin` in `src/app.rs` (`token_role(..).is_some()`, `>= Uploader`) lets a
  reader token publish or an uploader token delete/yank; tests check the role
  cap only at minting. (3) `any` -> `all` in `src/denylist.rs`
  (`version_allowed`, `name_fully_denied`) un-denies a name listed both bare
  and pinned. Options: (A) add a Rust unit test for (1) and (3) and a blackbox
  test in `tests/test_token_auth.py` for (2), ~40 test lines; (B) leave as is.
  Recommendation: A for (1) and (2) — plausible "simplifications" that ship a
  full auth bypass silently; (3) optional. Cost of choosing wrong: low.

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
  Related, same endpoint, decide together: an admin quarantine of a package the
  on-demand proxy serves from upstream is silently undone. Right after the
  `POST`, downloads return `403`, but the proxy index keeps rendering upstream's
  status, and within 60 s the next upstream listing rewrites the local status to
  `active` (`reconcile_observed_status`, `src/proxy.rs`, keeps only
  private-origin statuses) and downloads return `200` again. Reproduced on one
  node. Options for this half: (A) document that proxied packages follow
  upstream status and point at `exclude-packages` for blocking; (B) refuse the
  write with `409` when the proxy serves the package, pointing at
  `exclude-packages` (~6 lines + test + docs line). Recommendation: B — a freeze
  that quietly un-freezes is the worst outcome; a loud refusal is fail-closed.
  Together with A above, the rule becomes "the endpoint only relays status for
  `sync`-mirrored projects".
  Same family, same decision: an admin *yank* of a proxy-cached file
  (`POST /files/<pkg>/<file>/yank` on a package the on-demand proxy serves)
  returns `200` and writes the sidecar, but the proxy index renders upstream's
  yank state (`src/proxy.rs` ~861), so the yank never shows. Code-traced by
  Codex, not reproduced. Refusing it (`409`, like status under A) or overlaying
  local yanks onto the upstream listing are the two ways out.

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

## Leads (not yet adjudicated)

- Flaky: `tests/test_crash_consistency.py::test_dual_leadership_overlap_triggers_cas_conflict` failed once in a full run on 2026-09-22 (loaded machine) and passed 3/3 alone right after.
