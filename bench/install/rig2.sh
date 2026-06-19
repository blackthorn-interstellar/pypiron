#!/usr/bin/env bash
# Multi-node AWS rig for the CLOUD-BACKED (Track 2) install breaking point.
#
# Unlike rig.sh (one box, loadgen co-located), this splits roles so the SERVER's
# breaking point is what we measure, not the harness:
#   - 1 SERVER instance runs the system-under-test in its cloud config
#     (pypiron = S3 + presigned redirect: the node serves index + 302s, S3 serves
#     wheel bytes). Reachable by loadgens on its private IP:8080.
#   - N LOADGEN instances run REAL `uv` installs (drive.py --mode ramp), ramping
#     concurrency until the server breaks. uv follows pypiron's 302 to S3 itself.
#
# Start with one loadgen; if its CPU saturates (drive.py reports bound=loadgen-
# bound) before the server breaks, add nodes (RIG2_LOADGENS) and re-ramp.
#
# Shares the bucket/IAM/role/key that rig.sh creates (same NAME). Writes .rig2.env.
#
#   RIG2_LOADGENS=1 ./rig2.sh up
#   ./rig2.sh deploy            # build pypiron on server, uv on loadgens
#   ./rig2.sh serve pypiron     # start pypiron Track2 + seed corpus to S3
#   ./rig2.sh ramp pypiron      # real-uv ramp until the server breaks
#   ./rig2.sh results && ./rig2.sh down
set -euo pipefail

REGION="${RIG_REGION:-us-east-1}"
NAME="${RIG_NAME:-pypiron-ibench}"            # shares rig.sh's bucket/IAM/key
ARCH="${RIG_ARCH:-x86_64}"                          # loadgen + corpus arch (x86)
SERVER_ARCH="${RIG2_SERVER_ARCH:-aarch64}"          # the box customers run pypiron on
SERVER_TYPE="${RIG2_SERVER_TYPE:-t4g.small}"        # realistic small Graviton box
LOADGEN_TYPE="${RIG2_LOADGEN_TYPE:-c7i.2xlarge}"    # x86 oha drivers
LOADGENS="${RIG2_LOADGENS:-2}"
DISK_GB="${RIG_DISK_GB:-40}"
TIER="${RIG_TIER:-lite}"
SERVER_IMG="${RIG2_SERVER_IMG:-/tmp/pypiron-${SERVER_ARCH}.tgz}"  # prebuilt image to load
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"
ENVF="${HERE}/.rig2.env"
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

ami_param() {  # arch -> SSM AMI param
  case "$1" in
    aarch64) echo /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 ;;
    x86_64)  echo /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 ;;
    *) echo "bad arch=$1" >&2; exit 2 ;;
  esac
}
resolve_ami() { aws ssm get-parameter --region "$REGION" --name "$(ami_param "$1")" --query Parameter.Value --output text; }

# Server runs docker (pypiron image). Loadgens run drive.py natively (uv + py311).
userdata() {
  cat <<'UD'
#!/bin/bash
dnf install -y docker git rsync python3 python3.11
systemctl enable --now docker
usermod -aG docker ec2-user
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh
UD
}

launch() {  # role instance_type ami -> instance id (reused if already running)
  local role="$1" itype="$2" ami="$3" sg="$4" id credit=()
  id=$(aws ec2 describe-instances --region "$REGION" \
    --filters "Name=tag:Name,Values=${NAME}2-${role}" "Name=instance-state-name,Values=pending,running" \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  if [[ -z "$id" ]]; then
    # T-series (t4g/t3) burst: unlimited so a sustained ramp sees full vCPU
    # instead of throttling to baseline once credits drain.
    [[ "$itype" == t* ]] && credit=(--credit-specification "CpuCredits=unlimited")
    id=$(aws ec2 run-instances --region "$REGION" --image-id "$ami" --instance-type "$itype" \
      --key-name "$NAME" --security-group-ids "$sg" --iam-instance-profile "Name=${NAME}" \
      --user-data "$(userdata)" "${credit[@]}" \
      --metadata-options "HttpTokens=optional,HttpPutResponseHopLimit=2" \
      --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=${DISK_GB},VolumeType=gp3,Throughput=250,Iops=4000}" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${NAME}2-${role}}]" \
      --query 'Instances[0].InstanceId' --output text)
  fi
  echo "$id"
}

cmd_up() {
  local account bucket myip vpc sg ami sid lids=() lid i
  account=$(aws sts get-caller-identity --query Account --output text)
  bucket="${NAME}-${account}-${REGION}"
  myip=$(curl -fsS https://checkip.amazonaws.com)

  echo "== bucket + IAM (shared with rig.sh; create if absent)"
  aws s3api head-bucket --bucket "$bucket" 2>/dev/null || aws s3 mb "s3://${bucket}" --region "$REGION"
  if ! aws iam get-role --role-name "${NAME}" >/dev/null 2>&1; then
    aws iam create-role --role-name "${NAME}" --assume-role-policy-document '{
      "Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
    aws iam put-role-policy --role-name "${NAME}" --policy-name rig --policy-document "{
      \"Version\":\"2012-10-17\",\"Statement\":[
        {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\"],\"Resource\":\"arn:aws:s3:::${bucket}/*\"},
        {\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\"],\"Resource\":\"arn:aws:s3:::${bucket}\"}
      ]}"
    aws iam create-instance-profile --instance-profile-name "${NAME}" >/dev/null
    aws iam add-role-to-instance-profile --instance-profile-name "${NAME}" --role-name "${NAME}"
    sleep 10
  fi
  mkdir -p "${HERE}/.keys"
  aws ec2 describe-key-pairs --region "$REGION" --key-names "$NAME" >/dev/null 2>&1 || \
    { aws ec2 create-key-pair --region "$REGION" --key-name "$NAME" --query KeyMaterial --output text > "${HERE}/.keys/${NAME}.pem"; chmod 600 "${HERE}/.keys/${NAME}.pem"; }

  echo "== security group (SSH from ${myip} + intra-SG so loadgens reach :8080)"
  vpc=$(aws ec2 describe-vpcs --region "$REGION" --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
  sg=$(aws ec2 describe-security-groups --region "$REGION" --filters Name=group-name,Values="$NAME" Name=vpc-id,Values="$vpc" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
  [[ "$sg" == "None" || -z "$sg" ]] && sg=$(aws ec2 create-security-group --region "$REGION" --group-name "$NAME" --description "pypiron install bench rig" --vpc-id "$vpc" --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$sg" --protocol tcp --port 22 --cidr "${myip}/32" >/dev/null 2>&1 || true
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$sg" --protocol all --source-group "$sg" >/dev/null 2>&1 || true

  local sami lami
  sami=$(resolve_ami "$SERVER_ARCH"); lami=$(resolve_ami "$ARCH")
  echo "== launching server (${SERVER_TYPE}/${SERVER_ARCH}) + ${LOADGENS} loadgen(s) (${LOADGEN_TYPE}/${ARCH})"
  sid=$(launch server "$SERVER_TYPE" "$sami" "$sg")
  for ((i=1;i<=LOADGENS;i++)); do lids+=("$(launch "loadgen${i}" "$LOADGEN_TYPE" "$lami" "$sg")"); done

  aws ec2 wait instance-running --region "$REGION" --instance-ids "$sid" "${lids[@]}"
  local spriv spub
  spriv=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$sid" --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
  spub=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$sid" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  {
    echo "export RIG_REGION=${REGION}"
    echo "export RIG_BUCKET=${bucket}"
    echo "export RIG_KEY=${HERE}/.keys/${NAME}.pem"
    echo "export RIG_ARCH=${ARCH}"
    echo "export RIG2_SERVER_ARCH=${SERVER_ARCH}"
    echo "export RIG_TIER=${TIER}"
    echo "export RIG2_SERVER_ID=${sid}"
    echo "export RIG2_SERVER_IP=${spub}"
    echo "export RIG2_SERVER_PRIV=${spriv}"
    echo "export RIG2_LOADGEN_IDS=\"${lids[*]}\""
  } > "$ENVF"
  for ((i=0;i<${#lids[@]};i++)); do
    local lp; lp=$(aws ec2 describe-instances --region "$REGION" --instance-ids "${lids[$i]}" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    echo "export RIG2_LOADGEN_IP_$((i+1))=${lp}" >> "$ENVF"
  done
  echo "export RIG2_LOADGEN_N=${#lids[@]}" >> "$ENVF"
  echo "== up: server ${sid} @ ${spub} (priv ${spriv}); loadgens ${lids[*]}; wrote ${ENVF}"
}

load_env() { [[ -f "$ENVF" ]] || { echo "no ${ENVF}; run 'rig2.sh up' first" >&2; exit 1; }; source "$ENVF"; }
ssh_to() { ssh "${SSH_OPTS[@]}" -i "$RIG_KEY" "ec2-user@$1" "${@:2}"; }
wait_docker() { for _ in $(seq 1 90); do ssh_to "$1" 'sudo docker info' >/dev/null 2>&1 && return; sleep 5; done; echo "docker never up on $1" >&2; exit 1; }
wait_uv() { for _ in $(seq 1 90); do ssh_to "$1" 'test -x /usr/local/bin/uv && command -v python3.11' >/dev/null 2>&1 && return; sleep 5; done; echo "uv/py311 never up on $1" >&2; exit 1; }
loadgen_ips() { local v; for ((i=1;i<=RIG2_LOADGEN_N;i++)); do v="RIG2_LOADGEN_IP_$i"; echo "${!v}"; done; }

ship() {  # host -- ship HEAD repo (+ good src ref for the image)
  git -C "$REPO" archive --format=tar HEAD | ssh_to "$1" "rm -rf pypiron && mkdir -p pypiron && tar -x -C pypiron"
  if [[ -n "${RIG_BUILD_REF:-}" ]]; then
    git -C "$REPO" archive --format=tar "$RIG_BUILD_REF" src Cargo.toml Cargo.lock | ssh_to "$1" "cd pypiron && tar -x"
  fi
}

cmd_deploy() {
  load_env
  echo "== server: wait docker, LOAD prebuilt ${SERVER_ARCH} image (tiny box can't compile w/ LTO), ship, wheelhouse"
  wait_docker "$RIG2_SERVER_IP"
  [[ -f "$SERVER_IMG" ]] || { echo "prebuilt image ${SERVER_IMG} missing — build off-box: docker build --platform linux/${SERVER_ARCH/aarch64/arm64} -t pypiron:bench-${SERVER_ARCH} . && docker save pypiron:bench-${SERVER_ARCH} | gzip > ${SERVER_IMG}" >&2; exit 1; }
  ship "$RIG2_SERVER_IP"
  echo "== loading ${SERVER_IMG} onto server"
  gzip -dc "$SERVER_IMG" | ssh_to "$RIG2_SERVER_IP" "sudo docker load"
  ssh_to "$RIG2_SERVER_IP" "cd pypiron/bench/install && sudo docker run --rm -v \$(pwd)/../..:/repo \
    -w /repo/bench/install ghcr.io/astral-sh/uv:0.9.30-python3.11-bookworm-slim \
    python3 wheelhouse.py --tier ${TIER} --arch ${ARCH}"
  echo "== loadgens: wait uv, ship harness, fetch oha"
  for ip in $(loadgen_ips); do
    wait_uv "$ip"; ship "$ip"
    ssh_to "$ip" "test -x /home/ec2-user/oha || { curl -fsSL https://github.com/hatoo/oha/releases/download/v1.4.7/oha-linux-amd64 -o /home/ec2-user/oha && chmod +x /home/ec2-user/oha; }"
  done
  echo "== deploy complete"
}

cmd_serve() {  # serve <server>  (pypiron only for now)
  load_env
  local server="${1:-pypiron}"
  [[ "$server" == pypiron ]] || { echo "rig2 serve currently supports only pypiron (Track 2)" >&2; exit 2; }
  echo "== start pypiron Track 2 (S3 + presigned redirect) on the server"
  ssh_to "$RIG2_SERVER_IP" "sudo docker rm -f pypiron 2>/dev/null; sudo docker run -d --name pypiron -p 8080:8080 \
    -e PYPIRON_BIND_ADDR=0.0.0.0:8080 -e PYPIRON_S3_BUCKET=${RIG_BUCKET} -e AWS_REGION=${RIG_REGION} \
    pypiron:bench-${RIG2_SERVER_ARCH} pypiron serve --storage=s3 --artifact-delivery=redirect \
    --uploader-user=admin --uploader-pass=secret --admin-user=admin --admin-pass=secret"
  echo "== seed corpus -> pypiron -> S3 (upload from the server)"
  ssh_to "$RIG2_SERVER_IP" "cd pypiron/bench/install && sudo docker run --rm --network host -v \$(pwd)/../..:/repo \
    -w /repo/bench/install ghcr.io/astral-sh/uv:0.9.30-python3.11-bookworm-slim \
    python3 seed.py --server pypiron --base-url http://localhost:8080 --tier ${TIER} --arch ${ARCH}"
  echo "== served + seeded"
}

cmd_ramp() {  # ramp <server>
  load_env
  local server="${1:-pypiron}"
  local idx="http://${RIG2_SERVER_PRIV}:8080/simple/" host="${RIG2_SERVER_PRIV}:8080"
  local ladder="${RIG2_LADDER:-8,16,32,64,128,192,256,384,512}"
  echo "== real-uv ramp vs ${server} @ ${idx} from ${RIG2_LOADGEN_N} loadgen(s)"
  local pids=() i=0
  for ip in $(loadgen_ips); do
    i=$((i+1))
    ssh_to "$ip" "cd pypiron/bench/install && PATH=/usr/local/bin:\$PATH UV_PYTHON_DOWNLOADS=never \
      python3 drive.py --mode ramp --index-url ${idx} --host ${host} --tier ${TIER} --arch ${ARCH} \
      --ladder ${ladder} --label ${server}-n${i} --output results/ramp-${server}-n${i}.json" \
      > "/tmp/rig2-ramp-${server}-n${i}.log" 2>&1 &
    pids+=("$!")
  done
  local rc=0; for p in "${pids[@]}"; do wait "$p" || rc=1; done
  for ((j=1;j<=RIG2_LOADGEN_N;j++)); do echo "--- loadgen ${j} ---"; tail -14 "/tmp/rig2-ramp-${server}-n${j}.log"; done
  return $rc
}

cmd_results() {
  load_env
  mkdir -p "${HERE}/results"
  for ip in $(loadgen_ips); do
    rsync -az -e "ssh ${SSH_OPTS[*]} -i ${RIG_KEY}" "ec2-user@${ip}:pypiron/bench/install/results/" "${HERE}/results/" 2>/dev/null || true
  done
  echo "== pulled ramp results to ${HERE}/results/"
}

cmd_down() {
  load_env
  local ids="$RIG2_SERVER_ID $RIG2_LOADGEN_IDS"
  aws ec2 terminate-instances --region "$RIG_REGION" --instance-ids $ids >/dev/null
  echo "== terminated ${ids} (bucket/IAM/key kept)"
}

case "${1:-}" in
  up) cmd_up ;;
  deploy) cmd_deploy ;;
  serve) shift; cmd_serve "$@" ;;
  ramp) shift; cmd_ramp "$@" ;;
  results) cmd_results ;;
  down) cmd_down ;;
  *) echo "usage: $0 {up|deploy|serve <srv>|ramp <srv>|results|down}" >&2; exit 2 ;;
esac
