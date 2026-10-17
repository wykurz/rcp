#!/bin/bash
# run by the operator while preparing a trusted Ubuntu 24.04 x86_64 AMI
set -euo pipefail
cd "$(dirname "$0")/../.."
test "$(id -u)" = 0
test "$(uname -m)" = x86_64
test ! -e /opt/rcp-benchmark
python3 -m infra.aws_benchmark.maintenance --quiesce
apt-get update
apt-get install -y --no-install-recommends python3 python3-venv time e2fsprogs util-linux
useradd --system --no-create-home --shell /usr/sbin/nologin rcp-benchmark
install -d -m 0755 /opt/rcp-benchmark/infra/aws_benchmark /opt/rcp-benchmark/bin
python3 -m venv /opt/rcp-benchmark/venv
/opt/rcp-benchmark/venv/bin/pip install -r infra/aws_benchmark/requirements.txt
cp -r benchmarks /opt/rcp-benchmark/
cp infra/aws_benchmark/{__init__,common,worker,cache_client,maintenance}.py /opt/rcp-benchmark/infra/aws_benchmark/
install -m 0755 infra/aws_benchmark/cache_client.py /opt/rcp-benchmark/bin/sudo
python3 -m infra.aws_benchmark.maintenance
git rev-parse HEAD >/opt/rcp-benchmark/revision.txt
chown -R root:root /opt/rcp-benchmark
chmod -R go-w /opt/rcp-benchmark
# bake without credentials, SSH keys, prior instance data, or cloud-init state
echo 'Worker installed. Sanitize the image and reset cloud-init before capturing the AMI.'
