# Security audit — findings & dispositions

_Audit date: 2026-06-14._

A multi-agent adversarial review swept the codebase by dimension (auth,
path/key injection, XSS, archive/upload, SSRF / dependency-confusion, sync,
concurrency / CAS / crash-consistency, DoS / panics, config / info-leak). Every
finding was independently re-verified against the source by two reviewers; the
items below are the ones that survived verification.

**Shape of the results.** No memory-safety bugs (there is no `unsafe`), no XSS
(the `render` paths escape in the right contexts, covered by `fuzz_render`), no
clean path-traversal escape (`is_normalized` + `DiskStorage::resolve` hold), and
no remote-unauthenticated code execution. The real risk surface is (1) two
credential fail-open misconfigurations — **now fixed** — and (2) a set of
resource-exhaustion (OOM / amplification) vectors and narrow lock-free
correctness races, almost all gated behind an operator opt-in (sync, proxy) or a
non-default configuration.

## Fixed in this change

| # | Severity | Issue | Commit |
|---|----------|-------|--------|
| 1 | High | Empty-password credential half was accepted (`PYPIRON_ADMIN_PASS=` → any client authenticates as admin). Now treated as unconfigured (fail closed). | `fix: reject empty-password credentials …` |
| 2 | Medium | A half-configured credential pair failed open for reads (only `--read-user` set → all packages served publicly). The server now refuses to start. | `fix: refuse to start on a half-configured credential pair` |

## Open issues

Severity reflects realistic blast radius. "Reachability" is the most important
qualifier: most of these require the operator to enable a feature (`sync`,
`--proxy-upstream`) or run a non-default config, not a remote anonymous request.

### Medium

#### M1 — Sync fetches arbitrary upstream-controlled URLs (SSRF)
- **Update (HTTP-only sync):** Direct-storage mode has been removed — `sync` is an HTTP client only. That eliminates the `.metadata` *readback* variant (the destination server now extracts PEP 658 metadata from the received wheel rather than the sync host GETting `{url}.metadata` from the source). The blind-SSRF core below still stands: sync still GETs each artifact (and best-effort provenance) from upstream-controlled URLs.
- **Location:** `src/sync.rs` `download_once` (artifact GET), `download_provenance` (provenance GET).
- **Risk:** The per-file download `url` comes verbatim from the upstream index JSON with no scheme/host check and no requirement that it relate to `--from`; the request fires (following up to 10 redirects) before any hash check. Points the sync host at `http://169.254.169.254/…` or internal services.
- **Reachability:** Only when the operator runs `pypiron sync --from <index>` against an index they do not fully control. The default (`pypi.org`) is trusted and returns `files.pythonhosted.org` URLs.
- **Fix:** Parse `url` before fetching; require http(s), block link-local/loopback/metadata IPs, and constrain redirects to an allowlist.
- **Tradeoff:** A strict same-host-as-`--from` rule **breaks normal mirroring**, because PyPI's index host (`pypi.org`) legitimately differs from its file host (`files.pythonhosted.org`). The correct fix is an allowlist/SSRF-deny filter (block private ranges, optionally an operator allowlist), which is more code than a one-liner and needs a DNS-rebinding-aware resolver to be airtight. Deferred as a deliberate, scoped piece of work rather than a quick patch. Interim mitigation: only `sync --from` indexes you trust.

#### M2 — Multipart metadata fields are buffered in RAM with no count/aggregate cap (OOM)
- **Location:** `src/main.rs:684-714` (`legacy_upload`).
- **Risk:** Each non-file part is read fully into RAM and stored in a `HashMap`. Each field is capped at 64 KiB, but there is no cap on the number of fields or their aggregate size. Under the 1 GiB body limit an authenticated uploader can pack ~16,000 uniquely-named 64 KiB fields, all resident at once; a few concurrent such requests OOM a small box. The code comment already names this risk; the per-field cap does not close it.
- **Reachability:** Requires the uploader credential (low-privilege in the threat model), bounded by request bandwidth.
- **Fix:** Track aggregate metadata bytes across non-file parts and bound the field count; reject with 400 past a few hundred KiB / N fields.
- **Tradeoff:** Genuinely cheap and low-risk to fix — the only reason it is not in this change is that it is a separate concern from the credential fixes and warrants its own test (a many-field upload). Worth doing next; pairs naturally with M4 (both are upload-path OOMs).

#### M3 — Proxy on-demand artifact caching has no single-flight (bandwidth/disk/CPU amplification)
- **Location:** `src/proxy.rs:306-356` (`ensure_artifact_cached`).
- **Risk:** On a cache miss there is no per-key lock. N concurrent GETs for the same uncached artifact each spool the **full** upstream body to a temp file and SHA-256 it before one wins the `put_if_absent`. Unauthenticated in the default public-mirror config (proxy on, reads public). N× upstream bandwidth, N× transient spool disk, N× CPU.
- **Reachability:** Requires `--proxy-upstream`. Self-heals after the first artifact commits (subsequent requests short-circuit on `head_exists`).
- **Fix:** A per-key in-flight guard (keyed semaphore / `Mutex<HashMap<key, Weak<Notify>>>`) so concurrent fetches coalesce onto one download.
- **Tradeoff:** Adds shared mutable state on the hot artifact path and a little lifecycle complexity (evicting completed keys, not leaking entries on panic). The amplification is bounded by attacker connection count and is transient, so the cost/benefit is real but not urgent. The single-flight also helps M-listing (L11). Reasonable to fold both into one "coalesce upstream fetches" change.

#### M4 — Wheel METADATA extraction eagerly parses the entire zip central directory (memory amplification)
- **Location:** `src/wheel.rs:20-24` (`ZipArchive::new` + `file_names`).
- **Risk:** `ZipArchive::new` materializes every central-directory record (a heavy struct per entry plus a name index) at open time, before any size guard runs; `MAX_METADATA_BYTES` only bounds the *decompressed METADATA content*. A ~1 GiB `.whl` of millions of 46-byte empty entries (~23M) expands to multiple GiB inside `spawn_blocking`, OOM-killing the node.
- **Reachability:** Requires the uploader credential; the content part has no per-file size cap below the 1 GiB body limit.
- **Fix:** Read the End-Of-Central-Directory record and reject an implausible declared entry count before opening; and/or cap wheel size well below the global body limit.
- **Tradeoff:** The clean fix means not relying on `zip::ZipArchive::new`'s all-at-once parse — either a pre-flight EOCD check or a streaming scan for the single `*.dist-info/METADATA` member. Slightly fiddly (zip64, the EOCD locator) but well-bounded. A blunt interim mitigation (a hard wheel-size ceiling, e.g. ≤ a few hundred MiB) is one line and removes the worst case without the parser work.

### Low

#### L1 — Direct-storage mirror writes the upstream filename into the storage key without the server's filename validation — **RESOLVED**
- **Update:** Direct-storage mode has been removed. `sync` now POSTs every file through the server's `/legacy/`, which enforces `valid_artifact_filename` like any other upload, so the unvalidated-key path no longer exists.
- **Location:** *(was `src/sync.rs` `mirror_to_storage`, now deleted)*.
- **Risk:** The object key is `packages/<pkg>/<filename>` straight from the upstream `filename`, skipping `valid_artifact_filename` (which the upload path enforces). `DiskStorage::resolve` still blocks `..`, so this is **not** a namespace escape; impact is in-package-subtree pollution: a filename with `/` creates index-invisible orphan objects, and a `<sibling>.metadata`-shaped name can collide with a legitimate artifact's sidecar/metadata key.
- **Reachability:** Direct-storage `sync` against a malicious/compromised `--from` (which already controls all mirrored bytes).
- **Fix:** Validate `s.file.filename` with the same `valid_artifact_filename`/`sidecar::is_artifact` rules before building the key; skip-with-warning on failure. Ideally share one helper between `main.rs`/`upload.rs` and `sync.rs`.
- **Tradeoff:** Cheap and strictly positive (defense-in-depth + de-duplicates validation). Low priority only because it requires a hostile upstream that can already inject arbitrary verified content; the marginal harm is orphan objects, not data theft.

#### L2 — Upload spool temp files are world-readable with predictable names in a shared temp dir
- **Location:** `src/upload.rs:48-62` (`UploadSpool::new`).
- **Risk:** `File::create` at `pypiron-upload-<pid>-<counter>.spool` in `std::env::temp_dir()` (`/tmp`) — `O_CREAT|O_WRONLY|O_TRUNC`, no `O_EXCL`/`O_NOFOLLOW`, default mode 0644. A co-resident local user can read every uploaded artifact (incl. private packages) during the upload window, or pre-place a symlink at the predictable path to make the server truncate a file it can write.
- **Reachability:** Local only — a multi-tenant / shared host using the default `--spool-dir`. The symlink half is neutralized on modern Linux by `fs.protected_symlinks=1` (default); macOS lacks it. The info-leak half is not mitigated.
- **Fix:** `OpenOptions` with `.create_new(true)` (`O_EXCL`) + `.mode(0o600)` on Unix, a random suffix, and/or a dedicated 0700 spool subdir owned by the service.
- **Tradeoff:** Low-risk to fix and worth doing. Deferred from this change because most deployments run isolated (dedicated container/VM) where there is no untrusted local user, so it is genuinely low-impact there; the fix is a small, OS-specific (`std::os::unix`) change that needs care to stay portable.

#### L3 — `release_empty_claim` TOCTOU: orphaned/empty `.origin` opens a dependency-confusion window
- **Location:** `src/origin.rs:62-75`, triggered from `src/proxy.rs:352` and `src/sync.rs:481`.
- **Risk:** `release_empty_claim` does a non-atomic list-then-delete of `.origin` on a failed first download. Two facets: (a) a concurrent writer that read the live `MIRROR` claim commits its artifact in the gap, so the delete orphans an artifact that now has no origin marker; (b) once `.origin` is gone, `read_origin` returns `None`, so a later opposite-origin (`private`) first-write can claim the name — exactly the dependency-confusion state `.origin` exists to prevent. The worker never recreates `.origin`, so it heals only on a later mirror GET or audit.
- **Reachability:** Only in the no-`--private-prefix` config, which the server **already warns** is dependency-confusion-prone — and in which trivial first-claim races already exist. Needs a precise 3-step concurrent timing race plus a follow-up private upload.
- **Fix:** Make release safe against in-flight peers: compare-and-delete keyed on the marker being unchanged, a per-package lease, or defer orphan cleanup to the worker sweep with a grace period exceeding max download time; and/or have every committing writer re-assert the claim before writing its artifact.
- **Tradeoff:** A correct fix touches the lock-free claim protocol — the riskiest area to change, where a subtle error could break the dependency-confusion guarantee for everyone rather than for one race. Given that `--private-prefix` (the recommended config) closes the practical exploit, the better disposition is **use `--private-prefix`** and treat the protocol hardening as a deliberate, well-tested follow-up rather than a quick edit.

#### L4 — Sync buffers whole artifacts and the index JSON in RAM (OOM)
- **Location:** `src/sync.rs:639` (`resp.bytes().to_vec()`), `:503` (index `json()`), `:664` (HTTP mode copies the buffer again into `multipart::Part::bytes`).
- **Risk:** No streaming and no size cap; the upstream-declared `size` is never used to bound the read. At default concurrency (8 packages × 4 files) up to ~32 full artifacts are resident at once. The upload and proxy paths deliberately stream to a disk spool to avoid exactly this; sync reintroduces it.
- **Reachability:** The offline `sync` CLI/batch job, not the serving path. Most easily hit by mirroring genuinely large wheels (multi-GB CUDA/torch) at default concurrency; aborts the run (recoverable, writes are idempotent).
- **Fix:** Stream the download through an `UploadSpool` (as `proxy.rs` does), hashing on the way; reject when bytes exceed the declared `size` or a ceiling; cap the index JSON length before `.json()`.
- **Tradeoff:** Worth fixing for robustness, but it only kills a recoverable batch job (no serving impact, no corruption), so it ranks below the serving-path OOMs. Interim mitigation: lower `--package-concurrency`/`--concurrency` when mirroring large packages.

#### L5 — Crash-heal sidecar records the upstream-claimed size, not the stored bytes
- **Location:** `src/sync.rs:547`.
- **Risk:** When a prior run left the artifact but not the sidecar, the heal path writes `write_mirror_sidecar(…, s.file.size.unwrap_or(0))` — trusting the upstream `size` (or `0` when absent) instead of measuring the object. Surfaces as a wrong/zero PEP 700 `size` in the served index. Content integrity is unaffected (sha256 still governs).
- **Reachability:** Requires a crash in the artifact→sidecar window plus upstream omitting/misreporting `size`. Self-inflicted/correctness only.
- **Fix:** On the heal path, stat the stored object and record its true size.
- **Tradeoff:** Tiny fix, but tiny impact (an informational field), so it is low priority. The only cost of fixing is one extra `head`/metadata read on the rare heal path — negligible. Batched here for visibility rather than urgency.

#### L6 — `--exclude-platform-tag` fails open for wheels whose tags do not parse
- **Location:** `src/sync.rs:748-754` (`matches_filters`).
- **Risk:** An unparseable `.whl` returns `!has_inclusion_filters` **before** the exclusion check; `has_inclusion_filters` ignores `exclude_platform_tag`, so with an exclusion-only filter an unparseable wheel is included against intent. (Inclusion-only filters fail closed — the asymmetry is the bug.)
- **Reachability:** Sync/proxy with `--exclude-platform-tag` set; only affects the tiny set of malformed wheel filenames (~126 of ~10M historically).
- **Fix:** For unparseable wheels, fail closed whenever **any** filter (inclusion or exclusion) is set; only return `true` when no filters at all are configured.
- **Tradeoff:** One-line logic change, strictly positive. Low priority purely because the affected input set is negligible and an unparseable wheel has no platform tag for the exclusion to match on anyway, so the practical leakage is near-zero.

#### L7 — Audit applies a stale `dead` set, dropping a concurrently-revived package from the global index
- **Location:** `src/worker.rs:263-267` (apply), `:380-384` (classification), `:637-639` (blind removal).
- **Risk:** A package classified `dead` (no artifacts) at scan time is removed from the global index minutes later with no re-validation. If it is re-uploaded during the audit, the tick adds it back, then the audit blindly removes it again. Result: present at `simple/<pkg>/` but missing from the global `simple/` listing until the next audit (default 24h). Truth/artifacts are never lost.
- **Reachability:** Requires a re-upload during a minutes-long audit window. `pip`/`uv` install by hitting `simple/<pkg>/` directly, so installs still work; only root enumeration is wrong.
- **Fix:** Re-validate each `dead` candidate (cheap re-list / HEAD) immediately before removal; never remove a name currently present in `cached.names` without a fresh empty-truth listing.
- **Tradeoff:** Fixing adds a re-list per prune candidate to the audit (more S3 LISTs on large corpora) to close a self-healing, enumeration-only drift. The "dual leadership is only a cost" design comment is slightly overstated here, but the realistic impact is low and time-bounded, so the extra audit cost may not be worth it — document the limitation (this file) and revisit if root-enumeration accuracy becomes load-bearing.

#### L8 — A paused/zombie leader resuming mid-rebuild regresses a per-package view
- **Location:** `src/worker.rs:566-599` (`rebuild_package`), `:881-890` (`put_if_changed`).
- **Risk:** `rebuild_package` lists truth, then writes the view with no re-list and no CAS (`put_if_changed` is an unconditional get/compare/put — unlike the *global* index, which is CAS-fenced). If a leader is paused (SIGSTOP/GC/VM-pause) between list and write while a usurping leader rebuilds the same package after a new upload, the resumed zombie overwrites the fresh view with stale content. No marker remains, so it heals only at the next audit (24h). This refutes the lease comment that dual leadership is "never a correctness requirement" — for per-package views it can be.
- **Reachability:** Needs a leader pause spanning the lease TTL precisely between two awaits, a concurrent same-package upload, and the zombie resuming as the final writer. Truth is never lost; one file's visibility regresses transiently.
- **Fix:** Fence per-package view writes — CAS keyed to a truth-listing fingerprint (commit only if truth has not advanced), a lease-term fence token, or re-list-and-abort-if-changed immediately before the write.
- **Tradeoff:** The robust fix (fingerprint CAS / fencing) is real work in the most delicate concurrency path, and the trigger is a narrow, non-attacker-controlled timing race with a self-healing, single-file outcome. The honest disposition is to record it (the design doc's "only a cost" claim is the thing to correct) and harden it when the per-package write path is next touched, rather than risk a subtle regression for a low-probability event.

#### L9 — Proxy `.metadata` passthrough/cache loads the upstream response into RAM with no size cap
- **Location:** `src/proxy.rs:427-449` (`fetch_metadata_url`).
- **Risk:** `resp.bytes()` buffers the whole upstream `.metadata` body with no Content-Length/byte ceiling (timeout bounds time, not size). Asymmetric with the local extraction path, which enforces `MAX_METADATA_BYTES` (16 MiB).
- **Reachability:** Proxy on + reads public; the attacker does not control the upstream body directly (bounded by the operator-trusted upstream), so it is amplification at most.
- **Fix:** Cap the fetch at the same 16 MiB used in `wheel.rs` (Content-Length check and/or a `take()`-limited read); `None` on overflow.
- **Tradeoff:** Near-trivial to fix and strictly positive (closes an asymmetry). Low priority only because the body is upstream-trusted by default; fold it in whenever the proxy fetch path is next edited (with M3/L10).

#### L10 — Proxy upstream-listing fetch has no in-flight coalescing or outbound concurrency cap
- **Location:** `src/proxy.rs:457-495` (`Proxy::listing`).
- **Risk:** The listings mutex is held only to read/insert the cache, never across `fetch_listing`, which has a 30s timeout. N concurrent requests for the same cold package each issue their own upstream GET, and a flood of *distinct* cold names fans out an unbounded number of simultaneous upstream requests (and evicts the 8192-entry cache). Distinct-name floods can't be coalesced (different keys), so the real gap is the missing global outbound cap.
- **Reachability:** Public caching-mirror config; also self-inflicted by a large cold `uv sync`. Impact is availability/upstream-throttling, not crash/integrity — parked async tasks awaiting I/O are cheap.
- **Fix:** Per-package in-flight coalescing for listings, **plus** a global semaphore bounding concurrent upstream fetches.
- **Tradeoff:** The global cap is the cheap, high-value half (a `Semaphore`); per-key coalescing is more state for less benefit on distinct-name floods. Reasonable to ship just the outbound concurrency cap and skip the coalescing complexity. Deferred with M3 since both are "bound upstream fetches."

#### L11 — Index backfill reads each sidecar-less artifact fully into RAM to hash it, at 64× concurrency
- **Location:** `src/worker.rs:850-851` (`backfill_sidecar`), driven by `SIDECAR_READ_CONCURRENCY = 64`.
- **Risk:** `get_bytes` loads the whole object before hashing (no streaming); up to 64 full artifacts per package, × 8 packages in parallel, can spike to tens of GiB on large legacy wheels and OOM the leader, leaving the index unbuilt.
- **Reachability:** Dormant in steady state — normal uploads always write a sidecar. Only hit during the documented migration of pointing pypiron at a pre-existing bucket of sidecar-less artifacts. Operator-triggered, one-time, not remote.
- **Fix:** Stream the artifact through the hasher (chunked/range GET) instead of `get_bytes`, and/or gate backfill hashing with a small dedicated semaphore independent of the 64-way fan-out.
- **Tradeoff:** Streaming-hash is the right fix but touches the storage read API; the cheaper mitigation is a dedicated low-concurrency semaphore around backfill only. Low priority because it manifests once, during an operator-run migration, where concurrency can also be tuned down. Worth a note in the migration docs.

## Suggested order if/when these are picked up

1. **M2 + M4** — upload-path OOMs, both authenticated-but-low-priv, both cheap (field/aggregate caps; a wheel-size ceiling). Highest value-to-cost.
2. **M3 + L10 + L9** — one "bound and coalesce upstream fetches" change for the proxy.
3. **M1** — the SSRF allowlist/deny-filter; the most code, needs care (DNS rebinding), but the highest-severity open item.
4. **L1, L5, L6, L9** — small, strictly-positive correctness/defense-in-depth edits, batchable.
5. **L3, L7, L8** — lock-free protocol hardening; highest risk to change, lowest realistic impact, all self-healing. Prefer `--private-prefix` (L3) and documentation over edits unless the guarantees become load-bearing.
6. **L2, L11** — local-host / migration-only; fix opportunistically.
