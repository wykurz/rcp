#!/bin/bash
set -euo pipefail
# this outer script runs inside cloud-final; never wait here for the deferred unit
trap 'systemctl poweroff' ERR
install -d -m 0700 /run/rcp-ami-build
cat >/run/rcp-ami-build/prepare.sh <<'PREPARE'
#!/bin/bash
set -euo pipefail
umask 077
trap 'echo RCP_AMI_FAILED:__BUILD_ID__ >/dev/console' ERR
cloud-init status --wait
source /etc/os-release
test "$ID" = ubuntu
test "$VERSION_ID" = 24.04
test "$(uname -m)" = x86_64
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends git ca-certificates
test ! -e /var/tmp/rcp-ami-source
git init /var/tmp/rcp-ami-source
cd /var/tmp/rcp-ami-source
git -c credential.helper= fetch --depth=1 https://github.com/wykurz/rcp.git __REVISION__
git checkout --detach FETCH_HEAD
test "$(git rev-parse HEAD)" = __REVISION__
bash infra/aws_benchmark/install-worker.sh
test "$(cat /opt/rcp-benchmark/revision.txt)" = __REVISION__
cd /
# no credentials, instance role or keypair ever enter this builder
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
echo RCP_AMI_READY:__BUILD_ID__:__REVISION__ >/dev/console
PREPARE
chmod 0700 /run/rcp-ami-build/prepare.sh
# /run and transient units disappear on shutdown; nothing starts this job in the AMI
systemd-run --no-block --unit=rcp-ami-prepare \
  --property=After=cloud-final.service --property=Type=exec \
  --property=RuntimeMaxSec=1800 --property=TimeoutStopSec=15 \
  --property=SuccessAction=poweroff --property=FailureAction=poweroff \
  /bin/bash /run/rcp-ami-build/prepare.sh
