#!/usr/bin/env bash
# Stand up the reference rig in us-east-1:
#   server  = t4g.small (unlimited credits) + instance profile scoped to the bench bucket
#   loadgen = c7gn.4xlarge (deliberately oversized; the loadgen is never the suspect)
# Writes connection info to bench/.rig.env. Idempotent-ish: reuses IAM/SG/bucket/keypair.
set -euo pipefail

REGION=us-east-1
NAME=pypiron-bench
# Override for non-reference rigs (e.g. RIG_SERVER_TYPE=c7gn.2xlarge for the
# Phase 2 brag box). The instance is reused only if type matches.
SERVER_TYPE="${RIG_SERVER_TYPE:-t4g.small}"
LOADGEN_TYPE="${RIG_LOADGEN_TYPE:-c7gn.4xlarge}"
HERE="$(cd "$(dirname "$0")" && pwd)"

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="${NAME}-${ACCOUNT}-${REGION}"
MYIP=$(curl -fsS https://checkip.amazonaws.com)

echo "== bucket: ${BUCKET}"
aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null || aws s3 mb "s3://${BUCKET}" --region "$REGION"

echo "== IAM role + instance profile"
if ! aws iam get-role --role-name "${NAME}-server" >/dev/null 2>&1; then
  aws iam create-role --role-name "${NAME}-server" --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]
  }' >/dev/null
fi
aws iam put-role-policy --role-name "${NAME}-server" --policy-name s3-bench-bucket --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\"], \"Resource\": \"arn:aws:s3:::${BUCKET}/*\"},
    {\"Effect\": \"Allow\", \"Action\": [\"s3:ListBucket\"], \"Resource\": \"arn:aws:s3:::${BUCKET}\"}
  ]
}"
if ! aws iam get-instance-profile --instance-profile-name "${NAME}-server" >/dev/null 2>&1; then
  aws iam create-instance-profile --instance-profile-name "${NAME}-server" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "${NAME}-server" --role-name "${NAME}-server"
  sleep 10  # instance-profile propagation
fi

echo "== key pair"
mkdir -p "${HERE}/.keys"
KEYFILE="${HERE}/.keys/${NAME}.pem"
if ! aws ec2 describe-key-pairs --region "$REGION" --key-names "$NAME" >/dev/null 2>&1; then
  aws ec2 create-key-pair --region "$REGION" --key-name "$NAME" \
    --query KeyMaterial --output text > "$KEYFILE"
  chmod 600 "$KEYFILE"
fi
[[ -f "$KEYFILE" ]] || { echo "key pair exists in AWS but ${KEYFILE} is missing; delete the key pair and rerun" >&2; exit 1; }

echo "== security group"
VPC=$(aws ec2 describe-vpcs --region "$REGION" --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
SG=$(aws ec2 describe-security-groups --region "$REGION" --filters Name=group-name,Values="$NAME" Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
if [[ "$SG" == "None" || -z "$SG" ]]; then
  SG=$(aws ec2 create-security-group --region "$REGION" --group-name "$NAME" \
    --description "pypiron bench rig" --vpc-id "$VPC" --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG" \
    --protocol -1 --source-group "$SG" >/dev/null
fi
aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG" \
  --protocol tcp --port 22 --cidr "${MYIP}/32" >/dev/null 2>&1 || true

echo "== AMI"
AMI=$(aws ssm get-parameter --region "$REGION" \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
  --query Parameter.Value --output text)

launch() {  # name instance-type extra-args...
  local tag="$1" itype="$2"; shift 2
  local existing
  existing=$(aws ec2 describe-instances --region "$REGION" \
    --filters "Name=tag:Name,Values=${tag}" "Name=instance-state-name,Values=pending,running" \
              "Name=instance-type,Values=${itype}" \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  if [[ -n "$existing" ]]; then echo "$existing"; return; fi
  aws ec2 run-instances --region "$REGION" --image-id "$AMI" --instance-type "$itype" \
    --key-name "$NAME" --security-group-ids "$SG" \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${tag}},{Key=project,Value=${NAME}}]" \
    "$@" --query 'Instances[0].InstanceId' --output text
}

echo "== launching instances"
CREDIT_ARGS=()
[[ "$SERVER_TYPE" == t* ]] && CREDIT_ARGS=(--credit-specification CpuCredits=unlimited)
SERVER_ID=$(launch "${NAME}-server" "$SERVER_TYPE" \
  --iam-instance-profile "Name=${NAME}-server" \
  ${CREDIT_ARGS[@]+"${CREDIT_ARGS[@]}"})
# Loadgen carries the same bucket-scoped profile: Phase 3 seeds the corpus
# directly to S3 from there.
LOADGEN_ID=$(launch "${NAME}-loadgen" "$LOADGEN_TYPE" \
  --iam-instance-profile "Name=${NAME}-server")
echo "server=${SERVER_ID} loadgen=${LOADGEN_ID}"

aws ec2 wait instance-running --region "$REGION" --instance-ids "$SERVER_ID" "$LOADGEN_ID"

q() { aws ec2 describe-instances --region "$REGION" --instance-ids "$1" \
  --query "Reservations[0].Instances[0].$2" --output text; }

cat > "${HERE}/.rig.env" <<EOF
export RIG_REGION=${REGION}
export RIG_BUCKET=${BUCKET}
export RIG_KEY=${KEYFILE}
export RIG_SERVER_ID=${SERVER_ID}
export RIG_LOADGEN_ID=${LOADGEN_ID}
export RIG_SERVER_IP=$(q "$SERVER_ID" PublicIpAddress)
export RIG_SERVER_PRIVATE_IP=$(q "$SERVER_ID" PrivateIpAddress)
export RIG_LOADGEN_IP=$(q "$LOADGEN_ID" PublicIpAddress)
EOF
echo "== rig up; wrote bench/.rig.env"
cat "${HERE}/.rig.env"
