#!/usr/bin/env bash
# AWS rig for the realistic install benchmark: ONE Docker host that runs the full
# compose stack (server-under-test + loadgen container) for each system in turn.
# Single box on purpose — the install bench measures install wall-time under
# concurrency, not NIC saturation, so co-locating the uv loadgen with the server
# on one well-sized instance is clean and apples-to-apples (one box, every
# system sees identical CPU/disk/network). Graviton (arm64) + the arm64 AL2023
# AMI matches local aarch64 validation and is cost-effective.
#
# The instance profile carries S3 + DynamoDB so Track 2 (pypiron S3+redirect,
# pypicloud S3+DynamoDB) works with real AWS services.
#
# Subcommands:
#   up       provision bucket + IAM + SG + key + instance (Docker via user-data)
#   deploy   rsync repo, build pypiron image, fetch the wheelhouse on the box
#   run S..  run bench.py for each server (default: all six), Track 1
#   results  scp the results JSON back to bench/install/results/
#   down     terminate the instance (keeps bucket/IAM/key for reuse)
#
# Reuses the proven plumbing shape of bench/aws-up.sh. Writes bench/install/.rig.env.
set -euo pipefail

REGION="${RIG_REGION:-us-east-1}"
NAME="${RIG_NAME:-pypiron-ibench}"
ARCH="${RIG_ARCH:-aarch64}"                       # aarch64 (Graviton) | x86_64
INSTANCE_TYPE="${RIG_INSTANCE_TYPE:-c7g.4xlarge}" # arm64; use c7i.4xlarge for x86_64
DISK_GB="${RIG_DISK_GB:-80}"                       # corpus + mirror + images
TIER="${RIG_TIER:-lite}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"
ENVF="${HERE}/.rig.env"
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

ami_param() {
  case "$ARCH" in
    aarch64) echo /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 ;;
    x86_64)  echo /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 ;;
    *) echo "bad RIG_ARCH=$ARCH" >&2; exit 2 ;;
  esac
}

cmd_up() {
  local account bucket myip vpc sg ami keyfile
  account=$(aws sts get-caller-identity --query Account --output text)
  bucket="${NAME}-${account}-${REGION}"
  myip=$(curl -fsS https://checkip.amazonaws.com)

  echo "== bucket ${bucket}"
  aws s3api head-bucket --bucket "$bucket" 2>/dev/null || aws s3 mb "s3://${bucket}" --region "$REGION"

  echo "== IAM role + instance profile (S3 + DynamoDB)"
  if ! aws iam get-role --role-name "${NAME}" >/dev/null 2>&1; then
    aws iam create-role --role-name "${NAME}" --assume-role-policy-document '{
      "Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  fi
  aws iam put-role-policy --role-name "${NAME}" --policy-name rig --policy-document "{
    \"Version\":\"2012-10-17\",\"Statement\":[
      {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\"],\"Resource\":\"arn:aws:s3:::${bucket}/*\"},
      {\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\"],\"Resource\":\"arn:aws:s3:::${bucket}\"},
      {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:*\"],\"Resource\":\"arn:aws:dynamodb:${REGION}:${account}:table/${NAME}*\"}
    ]}"
  if ! aws iam get-instance-profile --instance-profile-name "${NAME}" >/dev/null 2>&1; then
    aws iam create-instance-profile --instance-profile-name "${NAME}" >/dev/null
    aws iam add-role-to-instance-profile --instance-profile-name "${NAME}" --role-name "${NAME}"
    sleep 10
  fi

  echo "== key pair"
  mkdir -p "${HERE}/.keys"; keyfile="${HERE}/.keys/${NAME}.pem"
  if ! aws ec2 describe-key-pairs --region "$REGION" --key-names "$NAME" >/dev/null 2>&1; then
    aws ec2 create-key-pair --region "$REGION" --key-name "$NAME" --query KeyMaterial --output text > "$keyfile"
    chmod 600 "$keyfile"
  fi

  echo "== security group (SSH from ${myip})"
  vpc=$(aws ec2 describe-vpcs --region "$REGION" --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
  sg=$(aws ec2 describe-security-groups --region "$REGION" --filters Name=group-name,Values="$NAME" Name=vpc-id,Values="$vpc" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
  if [[ "$sg" == "None" || -z "$sg" ]]; then
    sg=$(aws ec2 create-security-group --region "$REGION" --group-name "$NAME" --description "pypiron install bench rig" --vpc-id "$vpc" --query GroupId --output text)
  fi
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$sg" --protocol tcp --port 22 --cidr "${myip}/32" >/dev/null 2>&1 || true

  ami=$(aws ssm get-parameter --region "$REGION" --name "$(ami_param)" --query Parameter.Value --output text)
  local userdata; userdata=$(base64 <<'UD'
#!/bin/bash
dnf install -y docker git rsync
systemctl enable --now docker
usermod -aG docker ec2-user
UD
)
  echo "== launching ${INSTANCE_TYPE} (${ARCH}, ${DISK_GB}GB gp3)"
  local id
  id=$(aws ec2 describe-instances --region "$REGION" \
    --filters "Name=tag:Name,Values=${NAME}" "Name=instance-state-name,Values=pending,running" \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  if [[ -z "$id" ]]; then
    id=$(aws ec2 run-instances --region "$REGION" --image-id "$ami" --instance-type "$INSTANCE_TYPE" \
      --key-name "$NAME" --security-group-ids "$sg" --iam-instance-profile "Name=${NAME}" \
      --user-data "$userdata" \
      --metadata-options "HttpTokens=optional,HttpPutResponseHopLimit=2" \
      --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=${DISK_GB},VolumeType=gp3,Throughput=250,Iops=4000}" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${NAME}}]" \
      --query 'Instances[0].InstanceId' --output text)
  fi
  aws ec2 wait instance-running --region "$REGION" --instance-ids "$id"
  local ip; ip=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$id" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  cat > "$ENVF" <<EOF
export RIG_REGION=${REGION}
export RIG_BUCKET=${bucket}
export RIG_KEY=${keyfile}
export RIG_ID=${id}
export RIG_IP=${ip}
export RIG_ARCH=${ARCH}
export RIG_TIER=${TIER}
EOF
  echo "== up: instance ${id} @ ${ip}; wrote ${ENVF}"
}

load_env() { [[ -f "$ENVF" ]] || { echo "no ${ENVF}; run 'rig.sh up' first" >&2; exit 1; }; source "$ENVF"; }
ssh_host() { echo "ec2-user@${RIG_IP}"; }
rig_ssh() { ssh "${SSH_OPTS[@]}" -i "$RIG_KEY" "$(ssh_host)" "$@"; }

cmd_deploy() {
  load_env
  echo "== waiting for ssh + docker"
  for _ in $(seq 1 60); do rig_ssh 'command -v docker' >/dev/null 2>&1 && break; sleep 5; done
  echo "== rsync repo"
  rsync -az -e "ssh ${SSH_OPTS[*]} -i ${RIG_KEY}" --delete \
    --exclude target --exclude .git --exclude data --exclude bench/install/wheelhouse \
    --exclude bench/install/results --exclude bench/.keys --exclude '*.rig.env' \
    "${REPO}/" "$(ssh_host):pypiron/"
  echo "== build pypiron image + fetch wheelhouse (${RIG_ARCH}/${RIG_TIER})"
  rig_ssh "cd pypiron && sudo docker build --platform linux/${RIG_ARCH/x86_64/amd64} -t pypiron:bench-${RIG_ARCH} . && \
    sudo docker run --rm -v \$PWD:/repo -w /repo/bench/install ghcr.io/astral-sh/uv:0.9.30-python3.11-bookworm-slim \
      python3 wheelhouse.py --tier ${RIG_TIER} --arch ${RIG_ARCH}"
  echo "== deploy complete"
}

cmd_run() {
  load_env
  local track="${RIG_TRACK:-1}"
  local servers=("$@"); [[ ${#servers[@]} -eq 0 ]] && servers=(pypiron pypiserver devpi pypicloud bandersnatch proxpi)
  local rigenv="PYPIRON_S3_BUCKET=${RIG_BUCKET} AWS_REGION=${RIG_REGION}"
  for s in "${servers[@]}"; do
    echo "== run ${s} (track ${track})"
    rig_ssh "cd pypiron/bench/install && sudo ${rigenv} python3 bench.py --server ${s} --track ${track} --tier ${RIG_TIER} --arch ${RIG_ARCH} --concurrency 1,8,32,64,128 --samples 60"
  done
  rig_ssh "cd pypiron/bench/install && sudo python3 report.py"
}

cmd_results() {
  load_env
  mkdir -p "${HERE}/results"
  rsync -az -e "ssh ${SSH_OPTS[*]} -i ${RIG_KEY}" "$(ssh_host):pypiron/bench/install/results/" "${HERE}/results/"
  echo "== pulled results to ${HERE}/results/"
}

cmd_down() {
  load_env
  aws ec2 terminate-instances --region "$RIG_REGION" --instance-ids "$RIG_ID" >/dev/null
  echo "== terminated ${RIG_ID} (bucket/IAM/key kept; delete manually to fully clean up)"
}

case "${1:-}" in
  up) cmd_up ;;
  deploy) cmd_deploy ;;
  run) shift; cmd_run "$@" ;;
  results) cmd_results ;;
  down) cmd_down ;;
  *) echo "usage: $0 {up|deploy|run [servers...]|results|down}" >&2; exit 2 ;;
esac
