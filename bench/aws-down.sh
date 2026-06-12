#!/usr/bin/env bash
# Terminate the rig instances. Keeps bucket, IAM, SG, key pair (pennies or free;
# reused by the next run). Pass --all to also delete those.
set -euo pipefail

REGION=us-east-1
NAME=pypiron-bench
HERE="$(cd "$(dirname "$0")" && pwd)"

IDS=$(aws ec2 describe-instances --region "$REGION" \
  --filters "Name=tag:project,Values=${NAME}" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].InstanceId' --output text)
if [[ -n "$IDS" ]]; then
  echo "terminating: $IDS"
  # shellcheck disable=SC2086
  aws ec2 terminate-instances --region "$REGION" --instance-ids $IDS >/dev/null
  # shellcheck disable=SC2086
  aws ec2 wait instance-terminated --region "$REGION" --instance-ids $IDS
else
  echo "no rig instances found"
fi
rm -f "${HERE}/.rig.env"

if [[ "${1:-}" == "--all" ]]; then
  ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
  BUCKET="${NAME}-${ACCOUNT}-${REGION}"
  echo "deleting bucket ${BUCKET}, SG, key pair, IAM"
  aws s3 rb "s3://${BUCKET}" --force || true
  SG=$(aws ec2 describe-security-groups --region "$REGION" --filters Name=group-name,Values="$NAME" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
  [[ "$SG" != "None" && -n "$SG" ]] && aws ec2 delete-security-group --region "$REGION" --group-id "$SG" || true
  aws ec2 delete-key-pair --region "$REGION" --key-name "$NAME" || true
  rm -f "${HERE}/.keys/${NAME}.pem"
  aws iam remove-role-from-instance-profile --instance-profile-name "${NAME}-server" --role-name "${NAME}-server" || true
  aws iam delete-instance-profile --instance-profile-name "${NAME}-server" || true
  aws iam delete-role-policy --role-name "${NAME}-server" --policy-name s3-bench-bucket || true
  aws iam delete-role --role-name "${NAME}-server" || true
fi
echo "done"
