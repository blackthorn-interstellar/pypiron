#!/usr/bin/env bash
# Manage the VOPR soak spot fleet (CloudFormation stack in fleet.cfn.yaml).
#
#   ./fleet.sh push-bundle          # build the aarch64 binary + upload the bundle to S3
#   ./fleet.sh apply                # create/update the fleet (default VPC if unset)
#   ./fleet.sh status               # stack + instances + finding count + seeds
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
S3_BUCKET=${S3_BUCKET:-pypiron-soak-$(command aws sts get-caller-identity --query Account --output text 2>/dev/null || true)}
BUNDLE_KEY=${BUNDLE_KEY:-soak/bundle.tar.gz}
FINDINGS_PREFIX=${FINDINGS_PREFIX:-soak/findings/}
STATUS_PREFIX=${STATUS_PREFIX:-soak/status/}
SNS_TOPIC=${SNS_TOPIC:-}
DESIRED=${DESIRED:-1}
MAX=${MAX:-2}
EMAIL=${EMAIL:-}
BUDGET=${BUDGET:-25}
RUST_IMAGE=${RUST_IMAGE:-rust:1-bookworm}

aws() { command aws --region "$REGION" "$@"; }

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
    echo "building aarch64 vopr in a linux/arm64 container (no host target/ touched)..."
    # Stamp the git hash from the host (the container has the repo read-only and
    # would trip git's dubious-ownership guard). Repo mounted read-only; build
    # into the container's own target dir; copy the binary out. --locked keeps
    # the build reproducible and the lockfile untouched.
    local githash
    githash=$(git -C "$REPO_ROOT" describe --always --dirty --exclude='*' 2>/dev/null || echo unknown)
    docker run --rm --platform linux/arm64 \
        -e PYPIRON_GIT_HASH="$githash" \
        -v "$REPO_ROOT":/src:ro -v "$stage":/out \
        -w /src "$RUST_IMAGE" \
        bash -c "CARGO_TARGET_DIR=/tmp/t cargo build --release --example vopr --locked && cp /tmp/t/release/examples/vopr /out/vopr"
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
    # `s3 ls` exits 1 on an empty prefix; under pipefail that would fail status.
    echo "== findings ==" && { aws s3 ls "s3://${S3_BUCKET}/${FINDINGS_PREFIX}" 2>/dev/null || true; } \
        | wc -l | sed 's/^/  distinct findings in S3: /'
    echo "== seeds ==" && seeds
}

findings() {
    : "${S3_BUCKET:?set S3_BUCKET}"
    local keys
    keys=$(aws s3api list-objects-v2 --bucket "$S3_BUCKET" --prefix "$FINDINGS_PREFIX" \
        --query 'Contents[].Key' --output text 2>/dev/null || true)
    # `--output text` prints the literal "None" when Contents is null (no keys).
    case "$keys" in "" | None) echo "(no findings yet)"; return ;; esac
    for k in $keys; do
        aws s3 cp "s3://$S3_BUCKET/$k" - 2>/dev/null \
            | python3 -c 'import sys,json; d=json.load(sys.stdin); print("-", d["title"]); print("   ", d["repro"])'
    done
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

def age(seg):
    try:
        ts = datetime.datetime.fromisoformat(seg["updated_at"])
    except (KeyError, ValueError):
        return None
    return (now - ts).total_seconds()

live = [s for s in segments if not s.get("final") and (age(s) or 1e9) < 900]
for s in live:
    print(f"  LIVE {s.get('commit', '?')} {s.get('uuid', '?')[:8]} "
          f"{s.get('instance_id') or '?'} {s.get('instance_type') or '?'} "
          f"{s.get('cores', '?')}c  {s.get('seeds', 0):,} seeds  ({int(age(s) or 0)}s ago)")
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
    findings) findings ;;
    seeds) seeds ;;
    destroy) shift; destroy "$@" ;;
    *)
        echo "usage: $0 {push-bundle|apply|status|findings|seeds|destroy [--all]}" >&2
        exit 2
        ;;
esac
