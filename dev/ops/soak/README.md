# Always-on VOPR soak

A self-healing fleet of cheap Graviton **spot** instances that runs the
deterministic simulator (`examples/vopr.rs`) forever and writes a deduped
finding to **S3** the instant it hits a *new distinct* bug. It survives your
laptop closing because nothing runs on your laptop, and it holds **no GitHub
credential** — the box downloads a prebuilt bundle and writes findings entirely
through its **IAM role**.

```
   ┌──────────────── EC2 Auto Scaling Group (100% spot, Graviton) ────────────────┐
   │  each box:  N× pypiron-soak@<core> ─journald─▶ report.py ─IAM─▶  s3://…/findings/<sighash>.json
   │             (vopr --forever --rotate)          (dedup by            (one object per distinct bug;
   │             refresh.timer ─fetch bundle◀──┐    seed-agnostic sig)    optional SNS email)
   └───────────────────────────────────────────┼─────────────────────────────────┘
                                                │ bundle (vopr + ops) via IAM
   push master ─▶ CI (arm64) ─build+tar─▶  s3://…/bundle.tar.gz  ◀── fleet refreshes every ~10 min
        ▲                                                 │
        │ commit fix                                      │ read findings
   ┌────┴──────────────────────────┐                      │
   │ fixer routine (cloud, hourly) │◀─────────────────────┘
   │ → verify_fix.workflow.js      │   (rejected or failed →
   │ → gate: locus+panel+empirical │    left for a human)
   │ → apply: fixed script, no LLM │
   └───────────────────────────────┘
```

Why these choices:

- **Pure IAM, no PAT, no toolchain on the box.** The box downloads a prebuilt
  `vopr` bundle from S3 and `PutObject`s findings to one prefix — nothing else.
  Smaller/cheaper instances work because there is no build to host.
- **Compute-optimized `c7g`/`c6g`, never burstable `t4g`.** The soak pins every
  core at 100%; a burstable instance throttles to ~20% baseline once its CPU
  credits drain. Compute Graviton is also the best $/sustained-vCPU.
- **Spot + `price-capacity-optimized` + an ASG in maintain mode.** Spot is
  ~60-70% off; the ASG replaces reclaimed instances automatically.
- **The S3 key is the dedup.** A single real bug fails a large fraction of
  seeds; the object key is a seed-agnostic *signature hash*, so any number of
  dup seeds across any number of boxes collapse to one object. Listing the
  prefix is the deduped finding set.
- **Determinism is the simplifier.** Every finding is `--seed N --nodes.. …` — a
  few bytes that reproduce the bug on *any* machine, so the fixer never touches
  the fleet.

## What it costs / how many seeds

Single-threaded per seed → **one process per core**. Measured ~530-600 seeds/s
per core on an M-series Mac; budget a conservative **~400 seeds/s/vCPU on
Graviton**.

| Desired | Example pick | vCPU | ~Spot $/mo | ~Seeds/mo |
|--------:|--------------|-----:|-----------:|----------:|
| 1       | c7g.medium   | 1    | ~$12       | ~1.0 B    |
| 1       | c7g.large    | 2    | ~$22       | ~2.1 B    |
| 2       | c7g.large    | 4    | ~$44       | ~4.2 B    |

`DesiredCapacity` counts instances; the mixed pool picks the cheapest available
Graviton (so a `desired=1` box is 1-2 vCPU depending on the pool). The default
fits **$25/mo ≈ ~1-2 billion seeded fault schedules/month — up to ~350× the
entire nightly CI**, continuously. The AWS Budget alarm (set an email) guards
the ceiling.

## Set it up

Prereqs: `aws` configured, `docker` (for the one-time bundle build), and an S3
bucket you own.

```bash
cd dev/ops/soak
export S3_BUCKET=your-bucket
./fleet.sh push-bundle                 # build aarch64 vopr + upload the bundle
EMAIL=you@example.com ./fleet.sh apply # default VPC/subnets; budget alarm to EMAIL
./fleet.sh status                      # stack, instances, bundle drift, findings, seeds
./fleet.sh bundle                      # is the fleet soaking current code?
./fleet.sh findings                    # every deduped finding, newest first, with its repro
```

The bundle is built inside the fleet's own AMI image
([build-vopr.sh](build-vopr.sh)) and smoke-run there before it ships: a binary
linked against a newer glibc than the box's cannot start, and a fleet that
cannot start its soak still looks alive.

Instances take ~1-2 min to fetch the bundle and start soaking. Shell in with no
open port via SSM: `aws ssm start-session --target <instance-id>`.

**Keep the fleet on fixed code automatically:** enable `.github/workflows/soak-bundle.yml`
by setting the repo variables `SOAK_S3_BUCKET`, `SOAK_AWS_ROLE` (an OIDC role
with `s3:PutObject` on the bundle key), and optionally `SOAK_BUNDLE_KEY` /
`SOAK_AWS_REGION`. Every master push then rebuilds the bundle and the fleet
refreshes within ~10 min. Until then, re-run `./fleet.sh push-bundle` after a fix.

That leaves one gap the box cannot see. It refreshes from the bucket on a timer,
so it never lags the bucket — but a fix that never *reaches* the bucket (still
unpushed, or pushed without touching a path `soak-bundle.yml` watches) leaves the
soak grinding on old code, and nothing in the seed counts or a finding says so.
`fleet.sh bundle` (folded into `status`) measures exactly that: what the fleet is
running, how many soak-relevant commits this checkout has moved past it, and how
many of those are unpushed. It has been worth measuring — a finding once arrived
stamped with a commit whose bug had been fixed locally 12 hours earlier.

Turn on the autonomous fixer once you trust it — see
[fixer-routine.md](fixer-routine.md).

Judgment is agentic; landing the fix is not. Agents reproduce and diagnose it,
fix it in an isolated worktree, and then four of them verify (locus, two
adversarial lenses, an independent empirical re-run). On a unanimous pass the
workflow writes the commit message itself — from the seed, the signature, the
oracle name and the gate results, never from an agent's prose — and runs a fixed
`git apply --check` → `git apply` → `make check` → commit → push, with the patch
handed over as a file path rather than as diff text and no decision left in the
step. A non-zero exit anywhere stops it, and the finding goes to
`findings-needs-human/` — the same place a rejected fix goes. Every result the
workflow returns carries `needs_human`, so the routine files it without having
to interpret an outcome string.

Where the commit lands: the apply step runs in its own throwaway worktree, so
nothing touches your working tree, and the commit reaches `origin/master`
directly — your checkout sees it on the next `git pull`, not before. Only the
build cache is shared: `make check` there is pinned to
`<your checkout>/.local/target-soak`, so a fix attempt reuses one target
directory instead of growing a fresh ~600 MB tree it then abandons.

The trade is that the message states the seed,
the signature, the oracle and the gate results rather than a root-cause
narrative: prose is the one field only an agent could write, and a commit
message assembled from the finding's own fields is one an agent that read the
patch cannot steer.

Teardown: `./fleet.sh destroy` removes the compute but keeps the bucket (your
findings); `./fleet.sh destroy --all` empties and deletes the bucket too. It's a
single dedicated bucket named by account, so re-runs reuse it — buckets don't
pile up.

## Seed tracking (`fleet.sh seeds`)

Each reporter process is one **segment**: at start it mints a uuid and then
puts a single JSON object to `s3://…/soak/status/<commit>-<uuid>.json` every
~60s — accumulated seeds/interleavings (reset-safe against soak restarts,
baselined from the journal so a reporter restart never double counts), per-unit
gauges, instance id/type, and a findings count. On SIGTERM/SIGINT (bundle
refresh, spot reclaim's shutdown, `systemctl stop`) it flushes a last snapshot
with `final: true`, so a segment's object is its total; a hard kill just loses
the final ≤60s. One writer per key + S3's atomic whole-object PUT = safe
concurrent writes with zero coordination.

Each object also carries the last five non-heartbeat log lines plus the latest
heartbeat. That is the remote console: a soak that cannot start prints
nothing a finding parser recognises, so without it a broken box reads exactly
like an idle one — live, zero seeds, no findings.

`fleet.sh seeds` (also part of `status`) lists the prefix and aggregates: live
segments (updated <15 min ago) individually — with their log tail, and a
`STALLED` line if no heartbeat has landed in 5 minutes — plus the lifetime sum
over every segment ever. Segments never overlap, so the sum can only undercount
slightly (restart gaps) — never inflate. No SSM, so it works mid-reclaim and
needs only S3 read access.

## Run the finder on any box (no AWS)

The fleet is orchestration around one idempotent installer. On any Linux box,
drop the bundle contents in `/opt/pypiron-soak/` (the `vopr` binary + these ops
files), write `/etc/pypiron-soak/soak.env` (`PYPIRON_SOAK_S3_BUCKET=…`), then:

```bash
sudo /opt/pypiron-soak/install.sh   # one soak per core + reporter + refresh timer
```

Without a bucket the reporter still records findings to a local fallback file.

## Files

| File | Role |
|------|------|
| `fleet.cfn.yaml` | CloudFormation: IAM (S3 get bundle / put findings), SG, launch template (+ bootstrap), spot ASG, budget |
| `fleet.sh` | `push-bundle` / `apply` / `status` / `bundle` / `findings` / `seeds` / `destroy` |
| `build-vopr.sh` | the one build recipe, run inside the fleet's AMI image by both `push-bundle` and CI |
| `pypiron-soak@.service` | one soak process per core (`vopr --forever --rotate`, random start-seed) |
| `report.py` | journal → deduped S3 findings (seed-agnostic signatures, rate-capped, fail-open) + per-segment status objects (seed totals, final-flush on shutdown) |
| `pypiron-soak-reporter.service` | one reporter per box, follows the merged soak journal |
| `fetch-bundle.sh` + `pypiron-soak-refresh.{service,timer}` | poll S3, reinstall + restart when the bundle changes |
| `install.sh` | wire the units up from an extracted bundle (idempotent) |
| `../../../.github/workflows/soak-bundle.yml` | CI: build arm64 + ship the bundle to S3 on master push |
| `verify_fix.workflow.js` | reproduce → fix in src/ → **verify gate** → mechanical apply+push, else escalate |
| `fixer-routine.md` | the hourly cloud-routine prompt that drains findings |

## Security posture (fail-closed)

- **No secret on the box.** No GitHub token, no SSH key. The IAM role can
  `GetObject` the one bundle key and `PutObject` under the findings prefix — and
  nothing else (plus optional `sns:Publish` to one topic).
- **No inbound ports** — egress-only SG; shell is SSM Session Manager.
- **IMDSv2 required** on every instance.
- **The bundle tree is root-owned; the soak account cannot rewrite it.**
  `pypiron-soak-refresh.service` runs as root (it installs units and restarts
  services) and re-executes `fetch-bundle.sh`/`install.sh` from
  `/opt/pypiron-soak`, so that tree is never `chown`ed to the unprivileged `soak`
  user — otherwise a soak-account compromise could overwrite those scripts and
  win root on the next refresh. The soak-run units only read+exec the tree; their
  one local write is `report.py`'s `/var/tmp` fallback. Two things keep it that
  way, so no rollout leaves a window: `fetch-bundle.sh` extracts
  `--no-same-owner`, and `install.sh` — the one step every path into a new bundle
  runs, including a pull still driven by an **old `fetch-bundle.sh` on disk**
  that chowns the tree to `soak` before calling it — reasserts
  `chown -R root:root` on the way past. The first pull normalizes ownership; so
  does a fresh `./fleet.sh apply` or instance replacement.
- The autonomous fixer **cannot weaken an invariant**: the verify gate rejects
  any diff that loosens the checker and commits only a src/ fix that three
  independent checks agree is a genuine cure. Doubt escalates to a human.
- **A finding is data, never instructions.** It arrives from a fleet that reads
  simulator output, so the reporter allowlists the repro to the `vopr` argv shape
  and caps/strips the signature, and the workflow treats what it gets the same
  way: the patch is quoted into reviewer prompts between marker lines carrying a
  token hashed from the patch itself (so a diff cannot forge the end marker), the
  commit message is assembled from structured fields and sanitized before it can
  reach a shell, and the step that pushes is a fixed command list that sees a
  validated file path instead of the diff. No agent between the finding and
  master's history is free to act on what it read.
