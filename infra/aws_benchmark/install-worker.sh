#!/bin/bash
# run by the operator while preparing a trusted Ubuntu 24.04 x86_64 AMI
set -euo pipefail
stage=starting
progress() {
  stage=$1
  printf '%s [install:%s]\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$stage" || true
}
failed() {
  local status=$1 line=$2
  printf 'RCP_INSTALL_FAILED stage=%s line=%s exit=%s\n' "$stage" "$line" "$status" >&2 || true
  exit "$status"
}
trap 'failed "$?" "$LINENO"' ERR
progress preflight
cd "$(dirname "$0")/../.."
test "$(id -u)" = 0
test "$(uname -m)" = x86_64
test ! -e /opt/rcp-benchmark
progress apt-quiesce
python3 -m infra.aws_benchmark.maintenance --quiesce
progress remove-ssm
python3 -m infra.aws_benchmark.guest_security --remove-ssm
progress packages
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 python3-venv time e2fsprogs util-linux mdadm
# avoid background monitoring/check wakeups; leave md assembly and udev intact
for unit in mdcheck_start.timer mdcheck_continue.timer mdmonitor-oneshot.timer \
    mdcheck_start.service mdcheck_continue.service mdmonitor.service mdmonitor-oneshot.service; do
  if systemctl cat "$unit" >/dev/null 2>&1; then
    systemctl stop "$unit"
    systemctl mask "$unit"
  fi
done
useradd --system --no-create-home --shell /usr/sbin/nologin rcp-benchmark
install -d -m 0755 /opt/rcp-benchmark/infra/aws_benchmark /opt/rcp-benchmark/bin
progress python-environment
python3 -m venv /opt/rcp-benchmark/venv
/opt/rcp-benchmark/venv/bin/pip install --require-hashes -r infra/aws_benchmark/requirements.txt
progress harness
cp -r benchmarks /opt/rcp-benchmark/
cp infra/aws_benchmark/{__init__,common,worker,production_mailbox,large_storage,cache_client,maintenance,guest_security}.py /opt/rcp-benchmark/infra/aws_benchmark/
install -m 0755 infra/aws_benchmark/cache_client.py /opt/rcp-benchmark/bin/sudo
python3 -m infra.aws_benchmark.maintenance
python3 -m infra.aws_benchmark.guest_security
git rev-parse HEAD >/opt/rcp-benchmark/revision.txt
chown -R root:root /opt/rcp-benchmark
chmod -R go-w /opt/rcp-benchmark
# checkout can inherit the private AMI build umask; the benchmark user needs these modules
chmod -R a+rX,go-w /opt/rcp-benchmark/benchmarks /opt/rcp-benchmark/infra
progress verify-unprivileged
cd /opt/rcp-benchmark
runuser -u rcp-benchmark -- /usr/bin/python3 -B -c \
  'import benchmarks.run; import infra.aws_benchmark.cache_client; import infra.aws_benchmark.worker'
progress complete
# bake without credentials, SSH keys, prior instance data, or cloud-init state
echo 'Worker installed. Sanitize the image and reset cloud-init before capturing the AMI.'
