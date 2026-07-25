# VOPR fixer routine

The hourly, laptop-independent half of the loop: drain new findings from S3, fix
each through the verified-fix Workflow, commit only what clears every gate. A
committed fix flows master → CI bundle → the fleet's S3 refresh, so the running
soak lands on fixed code within minutes.

Register it as a **cloud routine** (the `schedule` skill / a scheduled cloud
agent) so it survives your laptop being closed. `CronCreate` is session-only —
do **not** use it for this. The prompt below IS the routine (hourly, e.g. cron
`17 * * * *`).

Findings move through S3 prefixes as a simple state machine (default
`soak/findings/` bucket):

```
findings/<hash>.json  ──claim──▶ findings-fixing/  ──▶ findings-resolved/   (committed)
                                                    └─▶ findings-needs-human/ (escalated)
```

A truly-fixed bug stops recurring (the fleet runs the fix), so it never
re-opens; a fix that didn't actually work reappears and re-opens itself. The
routine needs AWS creds (list/move findings) and its own GitHub push creds.

---

## Routine prompt

> You maintain the pypiron VOPR soak. Work only in a fresh checkout of
> `blackthorn-interstellar/pypiron` at a clean `origin/master`
> (`git fetch && git reset --hard origin/master && git clean -fd`). Ensure
> `cargo`/`rustc` are on PATH (rustup if missing). Bucket = `$SOAK_S3_BUCKET`.
>
> 1. `aws s3 ls s3://$SOAK_S3_BUCKET/soak/findings/`. Take at most **3** objects
>    (findings are rare; leave the rest for next hour).
> 2. For each finding object:
>    a. Read it (`aws s3 cp … -`); it has `repro`, `title`, `signature`, and
>       `commit` — the sha of the binary that found it. If `repro` no longer
>       reproduces on master, check out that sha before calling it stale: a
>       schedule-perturbing change retires seeds, it does not fix bugs.
>    b. Claim it: `aws s3 mv` it to `soak/findings-fixing/` (a lightweight lock).
>    c. Run the `vopr-verified-fix` Workflow (dev/ops/soak/verify_fix.workflow.js)
>       with `{ repro, signature, commit: true }`.
>    d. On the returned `outcome`:
>       - `committed` → `aws s3 mv` the object to `soak/findings-resolved/`. (The
>         push triggers CI → new bundle → the fleet refreshes within ~10 min.)
>       - `rejected` / `fix-escalated` / `escalate-checker-suspect` /
>         `not-reproduced` → `aws s3 mv` it to `soak/findings-needs-human/` and
>         write the Workflow's reasons into the object. Do **not** loop-retry.
> 3. Summarize: findings seen, committed, escalated.
>
> Absolute rule: the Workflow's verify gate decides. Never commit a fix it
> rejected, and never edit the checker's invariants to make a seed pass — a
> checker that looks wrong is an escalation, not an edit.

---

## The verify gate (why this is safe to run unattended)

`verify_fix.workflow.js` reports `committed` only when **all** hold:

- **Locus gate** (mechanical): the diff changes `src/` and does **not** delete
  or loosen any invariant, the repair classifier, a class threshold, or a
  violation gate in `examples/vopr.rs`.
- **Adversarial panel** (two independent lenses, unanimous): a *symptom-hunter*
  and an *invariant-guardian* both rule it a genuine cure that weakens nothing.
  Swap in `agentType: 'codex-exec'` / `'grok-exec'` for cross-vendor
  independence where available.
- **Independent empirical re-run**: the diff applies to a clean HEAD worktree,
  `make check` passes, the original seed now passes, and a fresh broad
  `--rotate` sweep is all-green.

Any doubt → escalated to a human. The failure mode is "a human looks at it,"
never "master gets a fix that hid a bug."

## Rollout: earn trust before full-auto

1. **Supervised** (first few real findings): run the Workflow yourself with
   `commit: false`. It produces the fix + the full gate verdict; you read the
   diff and merge by hand.
2. **Autonomous**: register the routine above with `commit: true`. The gate is
   unchanged — you've just removed yourself from the merge click.

## Hosting options

- **Cloud routine (recommended)** — survives your laptop, needs no box. Give it
  AWS creds + a GitHub push token as routine secrets, and let it `rustup` on
  first run. If it can't build Rust or reach AWS, fall back to:
- **On-box timer** — a small on-demand instance with `claude` installed runs
  this prompt via a systemd timer.
