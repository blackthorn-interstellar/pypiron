#!/usr/bin/env bash
# Manage the VOPR soak spot fleet (CloudFormation stack in fleet.cfn.yaml).
#
#   ./fleet.sh push-bundle          # build the aarch64 binary + upload the bundle to S3
#   ./fleet.sh apply                # create/update the fleet (default VPC if unset)
#   ./fleet.sh status               # stack + instances + bundle drift + findings + seeds
#   ./fleet.sh bundle               # is the fleet soaking current code?
#   ./fleet.sh findings             # list the deduped findings in S3
#   ./fleet.sh seeds                # seed totals from the fleet's S3 status objects
#   ./fleet.sh destroy [--all]      # tear down the fleet; --all also deletes the bucket
#
# The fleet holds no GitHub credential: the box downloads the bundle and writes
# findings entirely through its IAM role. Idempotent (CloudFormation reconciles).
# Overridable via env:
#   REGION STACK_NAME S3_BUCKET BUNDLE_KEY FINDINGS_PREFIX STATUS_PREFIX SNS_TOPIC
#   DESIRED MAX EMAIL BUDGET VPC_ID SUBNET_IDS
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "$HERE" rev-parse --show-toplevel)
REGION=${REGION:-$(aws configure get region 2>/dev/null || echo us-east-1)}
STACK_NAME=${STACK_NAME:-pypiron-soak}
# One dedicated bucket per account, reused by every run (push-bundle/apply/…).
# Deriving it from the account id means you never accidentally spin up a second.
S3_BUCKET=${S3_BUCKET:-pypiron-soak-$(command aws --no-cli-pager sts get-caller-identity --query Account --output text 2>/dev/null || true)}
BUNDLE_KEY=${BUNDLE_KEY:-soak/bundle.tar.gz}
FINDINGS_PREFIX=${FINDINGS_PREFIX:-soak/findings/}
STATUS_PREFIX=${STATUS_PREFIX:-soak/status/}
SNS_TOPIC=${SNS_TOPIC:-}
DESIRED=${DESIRED:-1}
MAX=${MAX:-2}
EMAIL=${EMAIL:-}
BUDGET=${BUDGET:-25}
# The AMI the fleet boots. Building in it is what keeps the binary runnable
# there — see build-vopr.sh.
BUILD_IMAGE=${BUILD_IMAGE:-public.ecr.aws/amazonlinux/amazonlinux:2023}

# --no-cli-pager: AWS CLI v2 pipes multi-line output into `less`, which steals
# the terminal and waits on `q` — hostile in a status command, fatal in a script.
aws() { command aws --region "$REGION" --no-cli-pager "$@"; }

ensure_bucket() {
    : "${S3_BUCKET:?set S3_BUCKET}"
    if aws s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
        return
    fi
    echo "creating bucket $S3_BUCKET ($REGION)"
    if [ "$REGION" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$S3_BUCKET" >/dev/null
    else
        aws s3api create-bucket --bucket "$S3_BUCKET" \
            --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
    fi
    aws s3api put-public-access-block --bucket "$S3_BUCKET" \
        --public-access-block-configuration \
        BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
}

# Files the box needs alongside the binary.
BUNDLE_FILES=(
    report.py install.sh fetch-bundle.sh
    pypiron-soak@.service pypiron-soak-reporter.service
    pypiron-soak-refresh.service pypiron-soak-refresh.timer
)

push_bundle() {
    : "${S3_BUCKET:?set S3_BUCKET}"
    ensure_bucket # idempotent: create the dedicated bucket if it's not there yet
    local stage
    stage=$(mktemp -d)
    # Self-clearing: a RETURN trap stays armed for the caller's return too,
    # where the local is gone (set -u would abort).
    trap 'rm -rf "$stage"; trap - RETURN' RETURN
    echo "building aarch64 vopr in a linux/arm64 $BUILD_IMAGE container (no host target/ touched)..."
    # Stamp the git hash from the host (the container has the repo read-only and
    # would trip git's dubious-ownership guard). The build itself — toolchain,
    # flags, and the smoke test that it runs on this userland — is build-vopr.sh,
    # the same recipe CI uses.
    local githash
    githash=$(git -C "$REPO_ROOT" describe --always --dirty --exclude='*' 2>/dev/null || echo unknown)
    docker run --rm --platform linux/arm64 \
        -e PYPIRON_GIT_HASH="$githash" \
        -v "$REPO_ROOT":/src:ro -v "$stage":/out \
        -w /src "$BUILD_IMAGE" \
        bash dev/ops/soak/build-vopr.sh
    for f in "${BUNDLE_FILES[@]}"; do cp "$HERE/$f" "$stage/$f"; done
    printf '%s' "$githash" >"$stage/commit" # the reporter keys status objects by this
    tar czf "$stage/bundle.tar.gz" -C "$stage" vopr commit "${BUNDLE_FILES[@]}"
    aws s3 cp "$stage/bundle.tar.gz" "s3://$S3_BUCKET/$BUNDLE_KEY"
    echo "pushed s3://$S3_BUCKET/$BUNDLE_KEY — the fleet refreshes within ~10 min."
}

discover_network() {
    VPC_ID=${VPC_ID:-$(aws ec2 describe-vpcs --filters Name=is-default,Values=true \
        --query 'Vpcs[0].VpcId' --output text)}
    if [ -z "${VPC_ID}" ] || [ "${VPC_ID}" = "None" ]; then
        echo "no default VPC found — set VPC_ID and SUBNET_IDS" >&2
        exit 1
    fi
    SUBNET_IDS=${SUBNET_IDS:-$(aws ec2 describe-subnets \
        --filters "Name=vpc-id,Values=${VPC_ID}" \
        --query 'Subnets[].SubnetId' --output text | tr '\t' ',')}
    echo "vpc=${VPC_ID} subnets=${SUBNET_IDS}"
}

apply() {
    : "${S3_BUCKET:?set S3_BUCKET}"
    if ! aws s3api head-object --bucket "$S3_BUCKET" --key "$BUNDLE_KEY" >/dev/null 2>&1; then
        echo "bundle s3://$S3_BUCKET/$BUNDLE_KEY missing — run './fleet.sh push-bundle' first" >&2
        exit 1
    fi
    discover_network
    aws cloudformation deploy \
        --template-file "${HERE}/fleet.cfn.yaml" \
        --stack-name "$STACK_NAME" \
        --capabilities CAPABILITY_IAM \
        --parameter-overrides \
        "VpcId=${VPC_ID}" "SubnetIds=${SUBNET_IDS}" \
        "S3Bucket=${S3_BUCKET}" "BundleKey=${BUNDLE_KEY}" \
        "FindingsPrefix=${FINDINGS_PREFIX}" "StatusPrefix=${STATUS_PREFIX}" \
        "SnsTopicArn=${SNS_TOPIC}" \
        "DesiredCapacity=${DESIRED}" "MaxSize=${MAX}" \
        "MonthlyBudgetUsd=${BUDGET}" "NotificationEmail=${EMAIL}"
    echo "deployed. Instances take ~1-2 min to fetch the bundle and start soaking."
    status
}

status() {
    : "${S3_BUCKET:?set S3_BUCKET}"
    echo "== stack ==" && aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
        --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "(no stack)"
    echo "== instances ==" && aws ec2 describe-instances \
        --filters "Name=tag:Name,Values=${STACK_NAME}" "Name=instance-state-name,Values=running,pending" \
        --query 'Reservations[].Instances[].[InstanceId,InstanceType,InstanceLifecycle,State.Name]' \
        --output text || true
    echo "== bundle ==" && bundle_status
    echo "== findings ==" && findings 5
    echo "== seeds ==" && seeds
}

# Paths whose contents decide what the fleet soaks — the same set
# .github/workflows/soak-bundle.yml watches. A commit touching none of them
# never needs to reach the box.
SOAK_PATHS=(src examples/vopr.rs Cargo.toml Cargo.lock dev/ops/soak)

# bundle: is the fleet soaking current code?
#
# The box is not the thing that lags. It re-heads the bundle every ~10 min
# (pypiron-soak-refresh.timer) and restarts onto new bytes, and CI republishes
# the bundle on every master push touching SOAK_PATHS. So a stale soak is always
# a fix that never reached the *bucket*: still sitting unpushed in a checkout, or
# pushed without touching a watched path. Neither is visible from the box, from
# the seed counts, or from a finding — and a soak on old code spends its seeds
# rediscovering bugs you already fixed. Measured once: a finding arrived stamped
# with a commit whose bug had been fixed locally 12 hours earlier and stayed
# unpushed for 19, and nothing in `status` said so.
bundle_status() {
    : "${S3_BUCKET:?set S3_BUCKET}"
    local pushed tmp
    pushed=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$BUNDLE_KEY" \
        --query LastModified --output text 2>/dev/null || true)
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"; trap - RETURN' RETURN
    aws s3 sync "s3://${S3_BUCKET}/${STATUS_PREFIX}" "$tmp" --quiet 2>/dev/null || true
    python3 - "$tmp" "$REPO_ROOT" "${pushed:-}" "${SOAK_PATHS[@]}" <<'PY'
import datetime, json, pathlib, subprocess, sys

statusdir, repo, pushed = sys.argv[1], sys.argv[2], sys.argv[3]
soak_paths = sys.argv[4:]
now = datetime.datetime.now(datetime.timezone.utc)


def git(*args):
    """Read-only git in the checkout, or None if it failed. Never raises: a
    status command must survive a detached HEAD, a missing upstream, or no git."""
    try:
        r = subprocess.run(["git", "-C", repo, *args],
                           capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def age(iso):
    try:
        return (now - datetime.datetime.fromisoformat(iso)).total_seconds()
    except (TypeError, ValueError):
        return None


def ago(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return f"{d}d{h:02d}h" if d else (f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s")


def count(*args):
    out = git("rev-list", "--count", *args, "--", *soak_paths)
    return int(out) if out and out.isdigit() else 0


if not pushed or pushed == "None":
    print("  s3 bundle: MISSING — run ./fleet.sh push-bundle")
else:
    secs = age(pushed)
    print(f"  s3 bundle: pushed {ago(secs)} ago" if secs is not None
          else f"  s3 bundle: pushed {pushed}")

# What the fleet is *running* (the bundle it already fetched), not what is in the
# bucket: the two differ for up to one refresh interval.
live = []
for p in sorted(pathlib.Path(statusdir).glob("*.json")):
    try:
        seg = json.loads(p.read_text())
    except (ValueError, OSError):
        continue
    secs = age(seg.get("updated_at"))
    if not seg.get("final") and secs is not None and secs < 900:
        live.append(seg)

commits = sorted({seg.get("commit", "?") for seg in live})
if not commits:
    print("  fleet runs: nothing reporting — see == seeds ==")
for commit in commits:
    base = commit.split("-")[0]  # strip `git describe --dirty`'s suffix
    if base in ("", "?", "unknown") or git("cat-file", "-e", base + "^{commit}") is None:
        print(f"  fleet runs: {commit} — not a commit in this checkout, cannot measure drift")
        continue
    when = git("log", "-1", "--format=%cr", base) or "?"
    behind = count(f"{base}..HEAD")
    if behind == 0:
        print(f"  fleet runs: {commit} ({when}) — current")
        continue
    print(f"  fleet runs: {commit} ({when}) — {behind} soak-relevant commit(s) behind here")
    upstream = git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
    unpushed = count(f"{upstream}..HEAD") if upstream else 0
    if unpushed:
        print(f"    {unpushed} of them UNPUSHED. CI publishes the bundle on push to master,")
        print("    so the fleet cannot reach them — push to close the gap.")
    else:
        print("    all pushed. The bundle republishes on push and the box refreshes within")
        print("    ~10 min; if this persists, check the Soak bundle workflow run.")
PY
}

# findings [N] — the deduped findings, newest first, with the commit that found
# them: a repro's `--seed` is only meaningful against the binary that produced
# it. N>0 prints the compact list `status` shows; no argument prints every
# finding with its repro command.
findings() {
    : "${S3_BUCKET:?set S3_BUCKET}"
    local tmp
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"; trap - RETURN' RETURN
    aws s3 sync "s3://${S3_BUCKET}/${FINDINGS_PREFIX}" "$tmp" --quiet 2>/dev/null || true
    python3 - "$tmp" "${1:-0}" <<'PY'
import datetime, json, pathlib, sys

limit = int(sys.argv[2])
found = []
for p in sorted(pathlib.Path(sys.argv[1]).glob("*.json")):
    try:
        found.append(json.loads(p.read_text()))
    except (ValueError, OSError):
        print(f"  (unreadable finding: {p.name})")
if not found:
    print("  no findings yet")
    raise SystemExit

found.sort(key=lambda f: f.get("ts", 0), reverse=True)
print(f"  {len(found)} distinct finding(s) in S3:")
for f in found[: limit or len(found)]:
    ts = f.get("ts")
    when = (datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
            if ts else "?")
    print(f"  - {when}  {f.get('commit', '?'):9} {f.get('title', '?')}")
    if not limit:
        print(f"      {f.get('repro', '?')}")
if limit and len(found) > limit:
    print(f"  (+{len(found) - limit} older — ./fleet.sh findings)")
PY
}

# seeds: aggregate the per-segment status objects the reporters put to S3
# (one JSON per reporter process lifetime, updated every ~60s, final-flushed on
# shutdown). Pure S3 reads — no SSM, works even mid-reclaim. Summing every
# segment is the lifetime total; segments never overlap, so it can only
# undercount (restart gaps), never double count.
seeds() {
    : "${S3_BUCKET:?set S3_BUCKET}"
    local tmp
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"; trap - RETURN' RETURN
    aws s3 sync "s3://${S3_BUCKET}/${STATUS_PREFIX}" "$tmp" --quiet 2>/dev/null || true
    python3 - "$tmp" <<'PY'
import datetime, json, pathlib, sys

now = datetime.datetime.now(datetime.timezone.utc)
segments = []
for p in sorted(pathlib.Path(sys.argv[1]).glob("*.json")):
    try:
        segments.append(json.loads(p.read_text()))
    except (ValueError, OSError):
        print(f"  (unreadable status object: {p.name})")
if not segments:
    print("  no status objects yet — the fleet writes one within ~1 min of soaking")
    raise SystemExit

def age(iso):
    try:
        return (now - datetime.datetime.fromisoformat(iso)).total_seconds()
    except (TypeError, ValueError):
        return None

def ago(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m" if h else (f"{m}m{s:02d}s" if m else f"{s}s")

live = [s for s in segments if not s.get("final") and (age(s.get("updated_at")) or 1e9) < 900]
for s in live:
    print(f"  LIVE {s.get('commit', '?')} {s.get('uuid', '?')[:8]} "
          f"{s.get('instance_id') or '?'} {s.get('instance_type') or '?'} "
          f"{s.get('cores', '?')}c  {s.get('seeds', 0):,} seeds  "
          f"({int(age(s.get('updated_at')) or 0)}s ago)")
    # The reporter is alive whenever the box is; only a progress line proves the
    # *soak* is. vopr heartbeats once a minute, so silence past a few of them
    # means the binary isn't running — and the tail below says why.
    quiet = age(s.get("last_progress_at") or s.get("started_at"))
    if quiet is not None and quiet > 300:
        since = "last progress" if s.get("last_progress_at") else "segment start"
        print(f"    STALLED: no soak progress line in {ago(quiet)} (since {since}) "
              f"— the soak is not running")
    # The tail holds everything but the heartbeat; the heartbeat is appended
    # because it is the newest line and, on a healthy box, the only one.
    for line in [*s.get("tail", []), s.get("last_progress_line")]:
        if line:
            print(f"      | {line}")
if not live:
    print("  no live workers reporting (last update > 15 min ago)")

# Per-commit rollup: "the current run" is the commit the fleet is soaking now,
# which spans segments (each reporter restart starts a new one).
by_commit = {}
for s in segments:
    by_commit.setdefault(s.get("commit", "?"), []).append(s)
recent = sorted(by_commit.items(),
                key=lambda kv: max(s.get("updated_at", "") for s in kv[1]), reverse=True)
for commit, segs in recent[:5]:
    tag = " (current)" if any(s in live for s in segs) else ""
    print(f"  commit {commit}: {sum(s.get('seeds', 0) for s in segs):,} seeds "
          f"in {len(segs)} segment(s){tag}")
if len(recent) > 5:
    print(f"  (+{len(recent) - 5} older commits, counted in lifetime)")

total = {k: sum(s.get(k, 0) for s in segments) for k in ("seeds", "interleavings", "acked_uploads")}
print(f"  lifetime: {len(segments)} segments, {total['seeds']:,} seeds, "
      f"{total['interleavings']:,} interleavings, {total['acked_uploads']:,} acked uploads")
PY
}

# destroy: tear down the compute. By default the bucket (your findings) is
# preserved; `destroy --all` also empties and deletes it.
destroy() {
    local nuke=""
    [ "${1:-}" = "--all" ] && nuke=1
    aws cloudformation delete-stack --stack-name "$STACK_NAME"
    echo "delete requested; waiting..."
    aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" \
        && echo "stack destroyed."
    if [ -n "$nuke" ]; then
        : "${S3_BUCKET:?set S3_BUCKET to remove the bucket}"
        echo "emptying + deleting s3://$S3_BUCKET (findings included)..."
        aws s3 rb "s3://$S3_BUCKET" --force && echo "bucket deleted."
    else
        echo "bucket ${S3_BUCKET:-<S3_BUCKET>} left intact (findings preserved)."
        echo "  full cleanup: $0 destroy --all   (or: aws s3 rb s3://${S3_BUCKET:-<bucket>} --force)"
    fi
}

case "${1:-}" in
    push-bundle) push_bundle ;;
    apply) apply ;;
    status) status ;;
    bundle) bundle_status ;;
    findings) findings ;;
    seeds) seeds ;;
    destroy) shift; destroy "$@" ;;
    *)
        echo "usage: $0 {push-bundle|apply|status|bundle|findings|seeds|destroy [--all]}" >&2
        exit 2
        ;;
esac
