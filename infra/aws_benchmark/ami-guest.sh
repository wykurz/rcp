#!/bin/bash
set -euo pipefail
# this outer script runs inside cloud-final; never wait here for the deferred unit
stage=bootstrap
bootstrap_failed() {
  local status=$1 line=$2
  trap - ERR TERM
  printf 'RCP_AMI_FAILED:__BUILD_ID__ stage=%s line=%s exit=%s\n' \
    "$stage" "$line" "$status" >/dev/console || true
  systemctl poweroff || true
  exit "$status"
}
trap 'bootstrap_failed "$?" "$LINENO"' ERR
trap 'bootstrap_failed 143 0' TERM
printf '%s [guest:bootstrap]\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" || true
install -d -m 0700 /run/rcp-ami-build
cat >/run/rcp-ami-build/maintenance.py <<'RCP_APT_MAINTENANCE_PY'
__MAINTENANCE_SOURCE__
RCP_APT_MAINTENANCE_PY
cat >/run/rcp-ami-build/prepare.sh <<'PREPARE'
#!/bin/bash
set -euo pipefail
umask 077
stage=starting
progress() {
  stage=$1
  printf '%s [guest:%s]\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$stage" || true
}
failed() {
  local status=$1 line=$2
  trap - ERR TERM
  printf 'RCP_AMI_FAILED:__BUILD_ID__ stage=%s line=%s exit=%s\n' \
    "$stage" "$line" "$status" >/dev/console || true
  exit "$status"
}
trap 'failed "$?" "$LINENO"' ERR
trap 'failed 143 0' TERM
progress cloud-init
cloud-init status --wait
progress platform
source /etc/os-release
test "$ID" = ubuntu
test "$VERSION_ID" = 24.04
test "$(uname -m)" = x86_64
progress apt-quiesce
python3 /run/rcp-ami-build/maintenance.py --quiesce
export DEBIAN_FRONTEND=noninteractive
progress checkout-packages
apt-get update
apt-get install -y --no-install-recommends git ca-certificates
progress checkout
test ! -e /var/tmp/rcp-ami-source
git init /var/tmp/rcp-ami-source
cd /var/tmp/rcp-ami-source
git -c credential.helper= fetch --depth=1 https://github.com/wykurz/rcp.git __REVISION__
git checkout --detach FETCH_HEAD
test "$(git rev-parse HEAD)" = __REVISION__
progress worker-install
bash infra/aws_benchmark/install-worker.sh
test "$(cat /opt/rcp-benchmark/revision.txt)" = __REVISION__
cd /
progress sanitize
# no operator credentials, instance role or keypair are supplied to this builder
# DHMC must remain disabled; the installer removes inherited SSM and registration state
# sanitation is deferred until cloud-final is finished, including its final writes
apt-get clean
rm -rf /root/.aws /root/.ssh /home/ubuntu/.aws /home/ubuntu/.ssh
rm -f /root/.bash_history /home/ubuntu/.bash_history /etc/ssh/ssh_host_*
cloud-init clean --logs --machine-id --seed --configs all
rm -f /var/lib/dbus/machine-id /var/lib/systemd/random-seed
ln -s /etc/machine-id /var/lib/dbus/machine-id
rm -rf /var/log/journal
find /var/log -type f -exec truncate -s 0 {} +
rm -f /var/lib/dhcp/*.leases /var/lib/dhcp/*.leases~
rm -rf /var/tmp/rcp-ami-source
sync
progress ready
echo RCP_AMI_READY:__BUILD_ID__:__REVISION__ >/dev/console
PREPARE
chmod 0700 /run/rcp-ami-build/prepare.sh
# /run and transient units disappear on shutdown; nothing starts this job in the AMI
stage=defer
systemd-run --no-block --unit=rcp-ami-prepare \
  --property=After=cloud-final.service --property=Type=exec \
  --property=StandardOutput=journal+console --property=StandardError=journal+console \
  --property=RuntimeMaxSec=1800 --property=TimeoutStopSec=15 \
  --property=SuccessAction=poweroff --property=FailureAction=poweroff \
  /bin/bash /run/rcp-ami-build/prepare.sh
