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
# the cells on a network filesystem mount the arrays through a server on the loopback that the
# supervisor starts for one run, for NFS 4 alone, and it refuses a machine where a server runs
# already (other_storage.py). Nothing of the NFS packages may start or listen by itself, here or
# on a worker: their services and sockets are masked before the packages arrive, so that
# installing them starts none either. The mount units of the two kernel filesystems
# (proc-fs-nfsd.mount, run-rpc_pipefs.mount) are left as they are: only the masked services
# start them, and the supervisor mounts both filesystems itself, at those very places
nfs_units=(nfs-server.service nfs-mountd.service nfsdcld.service fsidd.service nfs-idmapd.service
    nfs-blkmap.service nfs-utils.service nfs-client.target rpcbind.service rpcbind.socket
    rpc-statd.service rpc-statd-notify.service rpc-gssd.service rpc-svcgssd.service
    auth-rpcgss-module.service)
systemctl mask "${nfs_units[@]}"
apt-get update
# openssh: remote benchmark cells run their own unprivileged sshd on a private loopback.
# nfs-kernel-server, iproute2, kmod: the server, the delay and the kernel modules of those cells
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 python3-venv time e2fsprogs util-linux mdadm libsystemd0 systemd openssh-client openssh-server nfs-kernel-server iproute2 kmod
# avoid background monitoring/check wakeups; leave md assembly and udev intact
for unit in mdcheck_start.timer mdcheck_continue.timer mdmonitor-oneshot.timer \
    mdcheck_start.service mdcheck_continue.service mdmonitor.service mdmonitor-oneshot.service; do
  if systemctl cat "$unit" >/dev/null 2>&1; then
    systemctl stop "$unit"
    systemctl mask "$unit"
  fi
done
# a unit of the NFS packages that runs here would run on every worker
for unit in "${nfs_units[@]}"; do
  test "$(systemctl is-active "$unit" || true)" != active
done
# sshd runs a command through the account's shell and starts it in the account's home, so the
# shell must accept a command and the home must exist; the home is empty, root-owned and read-only.
# The account has no password and no authorized key outside a benchmark's own private directory.
useradd --system --no-create-home --home-dir /opt/rcp-benchmark/home --shell /bin/sh rcp-benchmark
install -d -m 0755 /opt/rcp-benchmark/infra/aws_benchmark /opt/rcp-benchmark/bin /opt/rcp-benchmark/home
progress python-environment
python3 -m venv /opt/rcp-benchmark/venv
/opt/rcp-benchmark/venv/bin/pip install --require-hashes -r infra/aws_benchmark/requirements.txt
progress harness
cp -r benchmarks /opt/rcp-benchmark/
cp infra/aws_benchmark/{__init__,common,worker,production_startup,production_mailbox,large_storage,supervisor_notify,cache_client,maintenance,guest_security,remote_check,other_storage,storage_check}.py /opt/rcp-benchmark/infra/aws_benchmark/
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
  'import benchmarks.run; import benchmarks.matrix; import infra.aws_benchmark.cache_client; import infra.aws_benchmark.worker'
# a login through the private sshd, as the benchmark account, under the sandbox of a remote suite
progress verify-remote-cells
python3 -B -m infra.aws_benchmark.remote_check
# the server, the delay and the mounts for the cells on other storage, on this image's own
# kernel, with a probe as the benchmark account under the sandbox; everything is taken down again
progress verify-storage-cells
python3 -B -m infra.aws_benchmark.storage_check
progress supervisor-notify-acceptance
python3 -B -m infra.aws_benchmark.supervisor_notify --image-self-test > /opt/rcp-benchmark/supervisor-notify-acceptance.json
chmod 0444 /opt/rcp-benchmark/supervisor-notify-acceptance.json
progress output-contract
python3 -B -c 'from infra.aws_benchmark.production_startup import install_output_contract; install_output_contract("/opt/rcp-benchmark")'
progress complete
# bake without credentials, SSH keys, prior instance data, or cloud-init state
echo 'Worker installed. Sanitize the image and reset cloud-init before capturing the AMI.'
