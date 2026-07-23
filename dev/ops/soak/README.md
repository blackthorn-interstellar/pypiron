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
   │ → verify_fix.workflow.js      │   (rejected → left for a human)
   │ → gate: locus+panel+empirical │
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
./fleet.sh status                      # stack, instances, finding count
./fleet.sh findings                    # list the deduped findings
```

Instances take ~1-2 min to fetch the bundle and start soaking. Shell in with no
open port via SSM: `aws ssm start-session --target <instance-id>`.

**Keep the fleet on fixed code automatically:** enable `.github/workflows/soak-bundle.yml`
by setting the repo variables `SOAK_S3_BUCKET`, `SOAK_AWS_ROLE` (an OIDC role
with `s3:PutObject` on the bundle key), and optionally `SOAK_BUNDLE_KEY` /
`SOAK_AWS_REGION`. Every master push then rebuilds the bundle and the fleet
refreshes within ~10 min. Until then, re-run `./fleet.sh push-bundle` after a fix.

Turn on the autonomous fixer once you trust it — see
[fixer-routine.md](fixer-routine.md).

Teardown: `./fleet.sh destroy` removes the compute but keeps the bucket (your
findings); `./fleet.sh destroy --all` empties and deletes the bucket too. It's a
single dedicated bucket named by account, so re-runs reuse it — buckets don't
pile up.

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
| `fleet.sh` | `push-bundle` / `apply` / `status` / `findings` / `destroy` |
| `pypiron-soak@.service` | one soak process per core (`vopr --forever --rotate`, random start-seed) |
| `report.py` | journal → deduped S3 findings (seed-agnostic signatures, rate-capped, fail-open) |
| `pypiron-soak-reporter.service` | one reporter per box, follows the merged soak journal |
| `fetch-bundle.sh` + `pypiron-soak-refresh.{service,timer}` | poll S3, reinstall + restart when the bundle changes |
| `install.sh` | wire the units up from an extracted bundle (idempotent) |
| `../../../.github/workflows/soak-bundle.yml` | CI: build arm64 + ship the bundle to S3 on master push |
| `verify_fix.workflow.js` | reproduce → fix in src/ → **verify gate** → commit/escalate |
| `fixer-routine.md` | the hourly cloud-routine prompt that drains findings |

## Security posture (fail-closed)

- **No secret on the box.** No GitHub token, no SSH key. The IAM role can
  `GetObject` the one bundle key and `PutObject` under the findings prefix — and
  nothing else (plus optional `sns:Publish` to one topic).
- **No inbound ports** — egress-only SG; shell is SSM Session Manager.
- **IMDSv2 required** on every instance.
- The autonomous fixer **cannot weaken an invariant**: the verify gate rejects
  any diff that loosens the checker and commits only a src/ fix that three
  independent checks agree is a genuine cure. Doubt escalates to a human.
