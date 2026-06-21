# Cleanup backlog

Working notes from a code/doc audit (2026-06-20). IDs (`C…` code, `D…` doc) are
just labels for tracking — they aren't referenced anywhere else. References use
symbol names rather than line numbers so they survive churn. Prune freely as
items land; delete this file once it's empty.

## Done

- **C22** — `safe_href` multibyte-URL panic on the project page (`web.rs`) — fixed
  (length-guarded slicing + regression test).
- **C23** — duplicated "No metadata available" literal (`web.rs`) — de-duped.
- **D1–D11** — doc-accuracy sweep landed: `CONFIGURATION.md` (`--config`/`PYPIRON_CONFIG`
  row), `DESIGN.md` (added `.project-status.json` + `_state/inventory.json` to the
  layout contract), `STANDARDS.md` (PEP 792 authoring cell), `ROADMAP.md`
  (api-version 1.4, `/dashboard`→`/`+`/projects/`), `VISION.md` (per-client redirect
  caveat), and `SECURITY_AUDIT.md` (all stale `file:line` citations refreshed; L5
  marked resolved by the HTTP-only rewrite; L9 expanded to cover the provenance fetch).

## Done — security DoS caps (M2, L9, L4)

- **M2** — `legacy_upload` now bounds non-file metadata parts (count + aggregate
  bytes), rejecting floods with 400. Test: `tests/test_upload_limits.py`.
- **L9** — proxy `fetch_metadata_url` + `fetch_provenance_url` read through a
  16 MiB-capped `read_capped`.
- **L4** — `sync` streams artifacts to an on-disk spool (bounded RAM, declared-size
  abort) and uploads them streamed; index JSON capped at 64 MiB; new
  `--spool-dir` / `PYPIRON_SYNC_SPOOL_DIR`.

Remaining `SECURITY_AUDIT.md` open items (decision/code, not doc): M1 (SSRF), M3+L10
(proxy fetch coalescing/cap), M4 (zip central-dir parse), L6, L3/L7/L8, L2/L11.

## Open — small behavior fixes (real semantics)

- **C26** — `verify.rs` `check_global` reclassifies *every* storage error as
  "missing-global-index"; sibling `check_package` distinguishes not-found via
  `is_not_found`. Give it `-> Result<()>` and branch. (Not behavior-preserving on
  the failure path — that's the point.)
- **C3** — intent-marker write failure is `warn!`'d in `legacy_upload` but silently
  `.ok()`'d at three sibling sites (`main.rs`) + `proxy.rs`. Warn consistently.
- **C4** — `not_found` (`main.rs`) doubles as the infallible `Response::builder`
  fallback at ~9 sites, logging builder failures as a "read miss" and returning 404
  for intended 200/301/302. Split out a `builder_fallback`.
- **C21** — `publish_inventory`'s persist-failure signal is honored in `tick` but
  silently dropped in `audit` (`worker.rs`). Make the drop explicit (`let _ =` +
  comment). Do **not** re-arm `dirty=true` — that changes behavior.
- **C20** — `.expect("just loaded")` on a request + worker path (`worker.rs`);
  violates the no-expect invariant though currently safe. Fallible insert.
- **C17** — `audit_shard` hand-mirrors `PkgStat::from_raw`'s inventory math
  (`worker.rs`); collapse to one counting impl feeding both tick and audit.
- **C27** — `infer_version_from_filename` (`names.rs`) skips the basename step
  `infer_package_from_filename` does → path-prefixed filenames mis-parse.

## Open — pure dedup / cosmetic (behavior-preserving)

Lowest priority; a single `code-simplifier` pass. Helpers already in-tree
(`eligible_proxy`, `AppState::headless`) can absorb several.

- **C1** — `proxy_package_index` re-codes the eligibility gate inline instead of
  calling `eligible_proxy` (`main.rs`).
- **C2** — `proxy_metadata_passthrough` / `proxy_provenance_passthrough` near-
  identical (`main.rs`); extract `companion_response`.
- **C5** — stray help line on `--intent-grace-secs` (clap shows the wrong knob).
- **C6** — redundant `!uploads_disabled()` guard in the identical-credential warning.
- **C7** — config logs "loaded configuration from X" before it reads/parses.
- **C8** — identical 416 Range-Not-Satisfiable response in both storage backends.
- **C9** — prefix-at-last-slash split duplicated across disk/object `list_all`.
- **C10** — `Yanked::normalized` (`sidecar.rs`) redundant no-alloc middle branch.
- **C11** — test `etag_of` duplicates production `quoted_sha256` (`cache.rs`).
- **C12** — `basic_auth` attach block copy-pasted across ~7 `sync` HTTP fns (one
  `as_ref` drift); extract `with_auth`.
- **C13** — `apply_yank_http` takes a redundant pre-trimmed `base` param (`sync.rs`).
- **C14** — `save_cursors` wraps send/check in an immediately-awaited `async{}` block.
- **C15** — `fetch_metadata_url` / `fetch_provenance_url` near-identical (`proxy.rs`).
- **C16** — `Listing -> Option<Arc<Found>>` match duplicated (`proxy.rs`); add
  `as_found()`. (Keep the `Option<Option<_>>` contract.)
- **C18** — two worker tests duplicate a ~28-line `AppState` literal; `AppState::headless`
  exists but is unused there.
- **C19** — one concept named three ways (audit / sweep / reconcile) in `worker.rs`
  (`reconcile_*` user-facing names + the `reconcile_sweeps_total` metric stay).
- **C24** — serde_json minimal-fallback string duplicated across both JSON renderers
  (`render.rs`).
- **C25** — `check_global` clones the live-package slice for no reason (`verify.rs`).
- **C28** — Prometheus HELP/TYPE header emitted ~5 ways (`metrics.rs`); add `header()`.
- **C29** — `ext_class` table repeats `(suffix, suffix)` for 13/14 rows (`corpus_check.rs`).
- **C30** — bucket-sample dump duplicated across 4 print sites (`corpus_check.rs`).
