# Manually armed AWS benchmarks

This is an opt-in, single-worker experiment. Nothing deploys or launches from a PR, push, schedule,
or default configuration. Infrastructure deployment, operator window arming, and benchmark execution
are separate operations. Start with the A/A smoke pilot before using these results for comparisons.

The fixed On-Demand VM is `c5d.24xlarge`: 96 vCPU, 192 GiB RAM and four 900 GB local NVMe SSDs. It
is not metal. Two disks provide separate ext4 source and destination filesystems; two remain unused.
There is no RAID, Spot, fallback instance type, warm pool, NAT gateway, load balancer, remote shell,
or monthly allowance service. Availability and the account's 96-vCPU On-Demand quota must be checked
in the selected Availability Zone before the first window.

## Essential setup inputs

Only these account decisions are needed. Account IDs, subnet IDs and role ARNs are non-secret; never
put access keys in chat or GitHub settings for this workflow.

1. Choose the AWS account and region. No region is hard-coded.
2. Supply an existing VPC and **public subnet** with an Internet Gateway route. The worker receives
   a temporary public IPv4 address, has no inbound security-group rules, and uses HTTPS for S3. This
   stack does not build a network. Private-only networking requires a reviewed template change and
   suitable existing S3 access; do not casually add a paid NAT gateway.
3. Supply an existing GitHub OIDC provider ARN in that account, with audience `sts.amazonaws.com`.
   If absent, the account owner creates it separately. This stack creates the two narrowly scoped
   roles, but never creates or changes the OIDC provider.
4. Prepare and pin an Ubuntu 24.04 x86_64, EBS-backed AMI with `/dev/sda1` as its root device and a
   root snapshot no larger than 16 GiB. On a trusted image-building machine, check out the reviewed
   repository commit and run `sudo bash infra/aws_benchmark/install-worker.sh`. The script installs
   the fixed harness and supervisor under `/opt/rcp-benchmark`, the pinned AWS SDK, and a dedicated
   unprivileged user. It does not capture an AMI or launch anything. Before capturing the image,
   remove image-builder credentials, SSH keys, shell history and previous cloud-init state, and
   ensure cloud-init processes new user data. No SSH/SSM agent or credentials are needed on workers.
   Record the source commit and AMI ID. AMI preparation/capture is an account-owner action.

Deploy the reviewed template using a trusted operator identity, while the group is closed:

```bash
aws cloudformation deploy --region REGION --stack-name rcp-benchmark \
  --template-file infra/aws_benchmark/stack.yaml --capabilities CAPABILITY_IAM \
  --parameter-overrides VpcId=VPC_ID SubnetId=SUBNET_ID WorkerAmiId=AMI_ID \
    GitHubOidcProviderArn=OIDC_PROVIDER_ARN
aws cloudformation describe-stacks --region REGION --stack-name rcp-benchmark \
  --query 'Stacks[0].Outputs'
```

The template creates a private, encrypted S3 bucket, security group, instance role/profile, launch
template, runtime role, and ASG with minimum/desired/maximum all **zero**. The launch-template
`LatestVersionNumber` attribute resolves to a **numeric version at stack deployment**; the ASG does
not track `$Latest` or `$Default`. The AMI is an explicit ID, not an SSM lookup. Change the image or
template only with no active window and after physical teardown. No rolling-update policy is used.

Create a GitHub environment named `aws-benchmark` with a required trusted reviewer and deployment
branch rules allowing **only main**. This is required: the OIDC environment subject does not encode
a branch. Keep environment protection and changes to trusted main under review. Configure these
environment variables from the stack outputs: `RCP_AWS_REGION`, `RCP_AWS_ASG`, `RCP_AWS_BUCKET`,
`RCP_AWS_ROLE_ARN`. Finally set the **repository** variable `RCP_AWS_BENCHMARK_ENABLED=true`.

## Run one window

1. Dispatch **AWS benchmark (operator window required)** from main with full lowercase 40-character
   baseline/candidate SHAs and an allowlisted suite. For the first run choose `aa-smoke` and the
   same SHA twice. The pilot uses one binary build for both roles, two balanced adjacent pairs,
   `tiny-10k`, and `source-warm`; it is labeled smoke. Builds finish on separate GitHub-hosted jobs
   before the environment approval gate. They have no OIDC or cloud credentials.
2. Once both builds succeed, note the GitHub `run ID-attempt`, for example `123456789-1`. The
   trusted operator chooses an absolute UTC opening 2–5 minutes ahead and a cutoff 5–60 minutes
   after opening. Use about 15 minutes for the first pilot. All boot, download, formatting, file
   generation, hashing, validation, cleanup and upload time must fit inside this window.
3. In a local Python 3.12 virtual environment with
   `pip install -r infra/aws_benchmark/requirements.txt`, use an operator SSO/session identity:

```bash
python -m infra.aws_benchmark.operator \
  --region REGION --asg ASG_NAME --bucket BUCKET_NAME \
  --run RUN_ID-ATTEMPT --baseline FULL_SHA --candidate FULL_SHA --suite aa-smoke \
  --opens YYYY-MM-DDTHH:MM:SS+00:00 --deadline YYYY-MM-DDTHH:MM:SS+00:00
```

4. After successful read-back, approve the waiting environment job. CI waits for the opening, checks
   the cutoff, uploads bounded binaries, and requests desired capacity 1. It never opens the ASG
   maximum. Download `aws-benchmark-results` afterward and inspect `status.json`, `worker.json`
   inside the ZIP, the unchanged harness `benchmark/results/results.json`, and partial logs. The ZIP
   is data; review its contents before opening anything executable or HTML.
5. Confirm the controller reports physical teardown. At the cutoff, verify all three ASG capacities
   are zero and all instances tagged for this group are terminated. Normal CI cleanup sets desired
   zero and waits up to ten minutes for the group and live EC2 inventory to empty. The independent
   cutoff also closes maximum, even after cancellation or lost GitHub credentials.

Only one trusted operator may arm this group at a time. The script refuses active capacities,
live/terminating/stopped instances, pending schedules, and a control document whose deadline has not
passed. A re-run needs a new GitHub attempt and a fresh window after the previous deadline and
teardown. Do not manually rewrite the control document to extend a lease.

The operator needs the following account permissions; none belong to CI: the ASG read APIs used by
`operator.py`, `autoscaling:SuspendProcesses`, and `autoscaling:PutScheduledUpdateGroupAction` on
this group; EC2 `DescribeInstances` and `DescribeLaunchTemplateVersions`; S3 `GetObject` and
`PutObject` on `control/window.json`, plus `ListBucket` constrained to that key so its initial
absence is distinguishable from access denial. Infrastructure deployment uses a separate trusted
identity with permission to create the resources declared in the template and pass the worker role.
There is no need to grant CI `PassRole`, SSM, direct EC2 launch, IAM, or infrastructure-deployment
access.

## Cutoff and failure behavior

CloudFormation has no permanent `SuspendedProcesses` property. While the group is closed, the
operator script suspends and verifies `AZRebalance`, `InstanceRefresh`, `HealthCheck`,
`ReplaceUnhealthy`, and `AlarmNotification`. `Launch`, `Terminate`, and `ScheduledActions` stay
enabled. It rejects scaling policies, lifecycle hooks, retention, warm pools, protection, active
refreshes, custom termination policies and automatic maximum-lifetime replacement.

The operator first creates and reads back `close-RUN_ID-ATTEMPT`, a one-time action setting minimum,
desired and maximum to zero at the absolute deadline. It then writes the run-bound control document
and schedules `open-RUN_ID-ATTEMPT` at the earlier absolute opening time, setting minimum/desired to
zero and maximum to one. Scheduling the opening avoids a delayed direct
`UpdateAutoScalingGroup(max=1)` arriving after the cutoff and reopening capacity. AWS orders
scheduled actions within one group and rejects start times in the past. Neither action uses a
recurrence or `EndTime`.

If arming fails, **leave the cutoff in place**. Inspect both schedules, the control document,
capacity and EC2 instances before doing anything else; the opening request may have succeeded even
if its response was lost. Do not blindly retry or delete schedules. To cancel a window early, the
operator first deletes any still-pending opening action, verifies its absence, then sets
minimum/desired/maximum to zero and verifies physical teardown. Keep the cutoff. If deletion or
closure has an uncertain outcome, inspect and reconcile it; do not arm another window.

The scheduled cutoff is the safety boundary when CI is cancelled, crashes, or cannot clean up. It
can execute up to **two minutes late**, and instance termination is asynchronous. MaxSize limits
requested desired capacity, not a strict physical-concurrency invariant: rebalancing, replacement,
pending/terminating instances and external operator actions can cause transient overlap. Disabling
the extra replacement paths reduces this risk but does not prove a physical maximum of one. The
runtime role can request repeatedly while maximum is open; this is a bounded window, not a
cryptographic one-launch allowance. The host timer and CI timeout are additional controls, never
substitutes for the AWS scheduled cutoff. There is no exact dollar cap.

AWS API/service failures, account administrators changing policies or schedules, and OS/kernel
compromise remain outside the guarantee. A capacity failure fails the run; there is no fallback to
another type or a longer window. A worker failure does not trigger an intentional restart.

## Execution and artifact boundary

The runtime role trusts only `repo:wykurz/rcp:environment:aws-benchmark`. It may call
`SetDesiredCapacity` on the exact stack-created ASG ARN, three required read APIs with resource `*`
(those APIs do not support resource-level scoping), read the fixed control/output objects, and write
seven exact input keys. It cannot edit launch templates, processes, schedules, IAM or the ASG
limits. Do not attach extra policies to either role.

The fresh controller executes only code checked out at the dispatched main commit. It never runs
downloaded binaries, builds candidate source, extracts result ZIPs, or exports AWS credentials to
other jobs. Build jobs can execute arbitrary Rust build scripts, so pin and review the chosen
revisions; a revision string and digest provide traceability, not proof that a hostile build told
the truth. There are no shared build caches or self-hosted GitHub runners in this workflow.

The worker AMI contains the reviewed root-owned supervisor and harness. Six exact raw binary objects
are accepted, each at most 128 MiB, with SHA-256 checked before use. No input archives, paths, shell
commands or root scripts are accepted. A request must match the operator's run, revisions, suite and
deadline. Disk preparation runs before candidate execution. It selects only four expected-size
devices with the exact EC2 instance-store model, rejects filesystems, children, mountpoints and
read-only devices, then formats two. EBS/root devices never qualify. Only this trusted supervisor
has device access.

The harness and binaries run as `rcp-benchmark` in a systemd cgroup with no capabilities, no new
privileges, private networking/devices, read-only system files, protected control groups, a 180 GiB
memory limit and an 8192-task limit. The process cannot reach instance metadata, S3, or GitHub. The
instance role itself has only fixed S3 reads/writes, no provisioning rights. This is process
isolation for a dedicated disposable host, not a separate kernel or a hostile-code VM.

Targeted suites use the existing `linux-drop-caches` policy. A root-owned executable named `sudo`
accepts exactly the harness's fixed cache-drop argv and sends a fixed request to a root supervisor
UNIX socket. The helper verifies the peer UID, calls `sync`, and writes `3` to `drop_caches`; it
accepts no paths or commands. No sudoers grant is installed. All fixture generation, full SHA-256
validation, source reuse, pairing and cleanup stay in the existing harness. Its random-byte
`filegen` fixture policy is unchanged; no sparse or all-zero replacements are introduced.

The worker reserves three minutes for stop/collection/upload. The whole measurement cgroup,
including preparation and descendants, has a remaining-window runtime limit and a ten-second kill
grace. A wall-clock deadline plus monotonic ceiling prevents clock movement extending work; an outer
supervisor alarm also interrupts a stalled upload. Failures never extend the lease. Results are
collected only after the service is stopped and its cgroup is empty, using held directory/file
descriptors, no symlink following, regular-file checks and bounded traversal.

Output storage is a 128 MiB tmpfs; private temporary directories are each 64 MiB. A result archive
is at most 64 MiB and 1000 candidate entries, with at most five directory levels below the output
root, matching `results/logs/<case>/<variant>/<iteration>/`. Candidate files are archived under
`benchmark/`; trusted supervisor metadata is the separate root `worker.json`. Standard service
output is discarded; the harness's per-trial files are retained within the output bound. Oversized
or unsafe results fail explicitly rather than silently truncating measurements. Failed/incomplete
harness records can be downloaded but must not be treated as successful comparisons.

The unversioned S3 mailbox has a fixed number of exact keys and seven-day expiration, plus one-day
incomplete-multipart cleanup. GitHub build artifacts live one day; result artifacts live seven. Size
limits are enforced by trusted code, **not by IAM PutObject conditions**. A stolen runtime
credential can overwrite permitted objects or issue excess requests; neither fixed keys nor
lifecycle rules are a hard storage/billing cap. S3 storage/requests, AMI snapshots, EBS while
allocated and public IPv4 addresses can cost money even though there is no always-running VM. Empty
the bucket before deleting the stack, and delete obsolete AMI snapshots separately.

The current largest suites generate 30–31.25 GiB, below host RAM. This PR adds no larger case. After
a validated A/A pilot, compare one targeted suite at a time. A future approximately 400 GiB bulk
experiment needs its own reviewed fixture, storage and deadline plan.

## Offline validation and first live acceptance

Run `just aws-benchmark-test` and `just benchmark-test`. Both are included in lint/CI. The focused
tests use fake AWS clients and local temporary files, never credentials or network. Validate the
template with `cfn-lint infra/aws_benchmark/stack.yaml` as well. No Rust or harness semantics
change.

Offline tests cannot establish account permissions, AMI boot behavior, instance availability, actual
scheduled scaling, or physical teardown. The account owner should observe the initial A/A pilot,
verify metadata/network isolation and the cold-cache helper in the prepared image, then test
cancellation and an unavailable-capacity/failure window before trusting unattended use.

## Design references

- [ASG capacity limits](https://docs.aws.amazon.com/autoscaling/ec2/userguide/asg-capacity-limits.html)
- [Auto Scaling IAM actions/resources](https://docs.aws.amazon.com/service-authorization/latest/reference/list_autoscaling.html)
- [Suspended process interactions](https://docs.aws.amazon.com/autoscaling/ec2/userguide/understand-how-suspending-processes-affects-other-processes.html)
- [Scheduled scaling and its delay](https://docs.aws.amazon.com/autoscaling/ec2/userguide/ec2-auto-scaling-scheduled-scaling.html)
- [Scheduled-action API, including past-time rejection](https://docs.aws.amazon.com/autoscaling/ec2/APIReference/API_PutScheduledUpdateGroupAction.html)
- [CloudFormation ASG attributes](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-autoscaling-autoscalinggroup.html)
- [EC2 compute instance specifications](https://docs.aws.amazon.com/ec2/latest/instancetypes/co.html)
