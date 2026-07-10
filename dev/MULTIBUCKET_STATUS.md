# Multi-bucket implementation — status & handoff TODO

Handoff for a fresh session to COMPLETE the multibucket feature. The design
spec is [MULTIBUCKET.md](MULTIBUCKET.md) (committed, authoritative — read its
Vision + §2–§8 first). This file is the implementation state as of
2026-07-10 ~08:45 PT. Delete this file in the final cleanup commit.

## Product constraints (non-negotiable, from Bryce)
- **Single-bucket mode pays nothing new**: no serving-path I/O, no background
  work. Adjudicated exception (Bryce may veto): one tombstone HEAD per
  *upload* and one origin GET + tombstone PUT per *delete* — per-publish-event
  costs buying the filename-reuse ban. The proxy-fill tombstone HEAD was ruled
  redundant and must be removed (P2.5 item 4).
- **Multi-bucket cost must be quantifiable**: metrics (replication
  objects/bytes, marker backlog per dest, freeze conflicts, per-bucket health)
  + a documented cost formula in the user docs.
- **Complexity lives inside pypiron** — customer sees only the bucket list.
- Use **Codex gpt-5.6-sol subagents** for implementation/review where
  practical (pinned via `CODEX_MODEL=gpt-5.6-sol` in
  ~/.claude/foreign-models.env; `codex:codex-rescue` agent type implements,
  `codex-exec` reviews read-only). Heavyweight Sol tasks: run `codex exec`
  directly in a background shell with `timeout 1500` — the 500s shim budget
  times out on deep repo analysis. Pre-stage evidence as flat files for Sol.

## Landed commits (master)
- `3067240` docs: the design spec.
- `7875036` P0 — bucket-list config (`--s3-bucket` repeatable, `name@region`,
  `PYPIRON_S3_BUCKETS`), `src/buckets.rs` (BucketSet/Pinned behind
  RwLock<Arc<Pinned>>), topology stamps (multi-bucket only), fail-closed
  guards.
- `ea2623a` P1 — pin-at-entry: `AppState.storage` deleted; every operation
  captures one `Pinned` at entry; helpers take the handle; generation-tagged
  index/presign caches. Single-bucket cost: one lock-read+Arc-clone per op.
- `60f3415` P2 — origin claims never-deleted (unclaimed sentinel, CAS-only),
  proxy pre-commit claim re-check, private-file tombstones + reuse ban,
  sidecar `origin` + `yank_epoch` fields, disk backend CAS primitives.

## In flight when this was written — CHECK BEFORE ACTING
Two workers from the previous session may still be running or may have
finished. **First: `git log --oneline -5`, `pgrep -fl "cargo|pytest|codex"`,
and `ls -lat src/*.rs` — reconcile reality before dispatching anything.**

1. **P3 (replicator) — implemented, uncommitted, one failing test.**
   The tree contains the complete P3: `src/replicate.rs` (new),
   `tests/test_multibucket.py` (new, two-bucket MinIO tests), wiring in
   worker/main/metrics/origin/sidecar/conftest + DESIGN.md. Last full
   `make test`: 240 passed, ONE error —
   `tests/test_crash_consistency.py::test_dual_leadership_overlap_triggers_cas_conflict`
   (TimeoutError, tests/helpers.py:267). An `impl-p3-finisher` agent was
   root-causing it (suspects: the new `_repl/` marker sweep changing worker
   tick timing; or a non-no-op single-bucket path — verify
   `is_multi()==false` short-circuits everywhere). If it committed, verify
   and move on; if the tree is still dirty, finish this: root-cause (no blind
   timeout bumps), `make check` + full `make test` green, commit
   `feature: multi-bucket P3 — replicator: eager fan-out, _repl/ markers,
   diff backstop, merge rules`. NOTE: `dev/MARKETING.md` belongs to another
   agent — never stage it. Verify `fuzz/fuzz_targets/fuzz_render.rs` change
   is P3-related before staging.
2. **P2.5 (origin hardening) — Codex background task `task-mrenlp3q-yeh3hs`**
   (check `/codex:status task-mrenlp3q-yeh3hs`), launched via codex-rescue in
   a worktree off `60f3415`. Its locked 9-item brief (from a gpt-5.6-sol
   review, adjudicated):
   1. Nonce'd claim bodies: JSON `{"origin":...,"nonce":"<128-bit hex>"}`,
      fresh nonce per write, legacy plain-text reads still parse. Kills etag
      ABA (disk etags are content hashes). Update DESIGN.md layout.
   2. `release_empty_claim`/`demote_mirror_to_private` validate the read
      state was `mirror` before CAS.
   3. Empty-claim reclamation moves OFF the proxy failure path INTO the
      leader audit: reclaim only if zero artifacts AND no live intent
      markers AND claim etag stable across two observations separated by
      intent-grace; after CAS to unclaimed, re-list and revert (fresh nonce)
      if anything appeared.
   4. Proxy re-check moves to immediately before the artifact put; REMOVE
      the tombstone HEAD from the proxy-fill path (redundant).
   5. Legacy mirror-upload path gets the same pre-put claim re-check.
   6. `files_delete` re-reads claim etag after artifact removal; tombstone
      fail-closed if it moved.
   7. `claim_origin` accepts a caller-provided sentinel etag (skip the
      guaranteed-failing create).
   8. Audit quarantines mirror-sidecar'd artifacts under a private claim to
      `_quarantine/<pkg>/<file>@<sha-prefix>` (within-bucket mirror of the
      §6.2 merge rule).
   9. DESIGN.md repurpose guidance → CAS-to-unclaimed via new
      `pypiron origin release <pkg>` admin subcommand; chaos test
      `_assert_storage_clean` must REJECT missing `.origin`.
   If the codex task produced a branch/commit: review, rebase onto P3's
   commit, run full gates, merge to master. If it produced nothing: dispatch
   fresh (codex-rescue or Opus) with the list above.

## Remaining phases (in order, each: make check + make test green, evidence
## pasted, own conventional commit on master)

3. **P4 — per-node bucket selection + health views** (spec §5, §7):
   - Per-bucket health view fed by real-traffic errors + one HEAD probe per
     non-selected bucket on the worker tick (multi-bucket only). Strict
     classification: timeouts/connect/5xx count; 403/401/412/KMS/config NEVER
     count (alarm only).
   - Asymmetric hysteresis: leave selected bucket after N consecutive
     failures over ~seconds (knob); return to a more-preferred bucket only
     after continuous health for ~minutes (knob). Selections settle, never
     oscillate.
   - `BucketSet::switch()` wiring: generation bump, cache clear via existing
     generation tags, audit-on-selection (audit-on-boot code against the
     newly selected bucket), `_dirty` markers dropped by replicator make the
     heal incremental.
   - Leases/leadership stay per-bucket (the `// P4:` comments in worker.rs
     and build_counters mark the construction-time captures to revisit).
   - Runtime topology re-validation on reachability transitions (stamp check;
     mismatch = alarm + stop accepting writes).
   - `pypiron buckets migrate` admin command: bump topology generation,
     re-stamp reachable buckets.
   - Metrics: per-bucket health state gauge, selection index/generation.
   - Blackbox: MinIO bucket-deny (or container stop) → selection switches
     within seconds, uploads continue to next bucket, markers accumulate,
     drift-back after recovery, indexes heal.
4. **P5 — proof matrix + docs**:
   - tests/test_multibucket.py extensions per spec §12 P5: partition/heal,
     switch under upload storm, cross-bucket duplicate freeze (§6.3
     quarantine + index suppression + alarm), demotion race (proxy fill vs
     private upload vs merge), tombstone + yank-epoch convergence, marker
     drain after extended outage, flap settling, straggler delivery,
     cold-start divergence merge. Three-bucket variant for the core paths.
   - Docs: docs/reference/configuration.md (all new knobs; every flag has a
     PYPIRON_ env twin), a user-manual page written outcome-first ("give
     pypiron two buckets in different regions and it survives either one
     dying"), the quantified cost model (per-upload fan-out cost, marker
     backlog, probe cadence, storage multiplier, the per-publish tombstone
     HEAD), and the §11 honest limits.
5. **Final gate**: fresh-context Opus review of the FULL diff
   (3067240..HEAD) + gpt-5.6-sol and Grok adversarial passes (free,
   flat-rate; same three-attacker pattern used throughout). Adjudicate,
   fix confirmed findings, re-run full gates. Then delete this file,
   update dev/ROADMAP.md (multi-bucket → shipped), final cleanup commit.

## Operational playbook (learned the hard way)
- **Workers stall silently.** Idle notifications are noise; a worker with no
  cargo/pytest processes AND no file mtime changes for >20 min is dead.
  Verify evidence yourself (test logs in the session scratchpad), then
  commit-or-redispatch. Order every implementer to commit IMMEDIATELY after
  the green run — never idle between green and commit.
- **Keep a timed heartbeat** (ScheduleWakeup ~1500s) so the loop survives
  workers that die without events. Event-driven-only orchestration starved
  twice overnight.
- Scout briefs (dense path:line maps of storage plumbing, call sites, data
  plane, caches/metrics, worker anatomy, test harness) exist at
  /private/tmp/claude-501/-Users-bryce-projects-pypiron/3bec5de2-e017-4cd0-b457-0a1c3ba3645b/scratchpad/scout-briefs/
  (*.md — tmp-dir, may be gone; re-scout with 6 parallel Explore agents if
  needed). Sol's P0-P2 review: same tmp root, sol-review/review.txt.
- Test harness facts: two buckets on ONE MinIO container suffices for
  replication tests; S3 slice = `pytest -m "s3 and not perf and not stress"`;
  full suite ~11 min; crash tests are the kill-point sweep in
  tests/test_crash_consistency.py.
- Conventional commits, `feature:` spelled out, root cause in bug-fix
  bodies, and end bodies with the session's own Claude-Session trailer.
