# Manually armed AWS benchmarks

The separate [production admission and collection path](PRODUCTION.md) is initially disabled and
requires its own deployment and activation review. The ASG instructions below describe the original
manual experiment.

This is an opt-in, single-worker experiment. Nothing deploys or launches from a PR, push, schedule,
or default configuration. Infrastructure deployment, operator window arming, and benchmark execution
are separate operations. Start with the A/A smoke pilot before using these results for comparisons.

The fixed On-Demand VM is `c5d.24xlarge`: 96 vCPU, 192 GiB RAM and four 900 GB local NVMe SSDs. It
is not metal. Existing suites use two disks as separate ext4 source and destination filesystems; two
remain unused. The opt-in [large qualification suites](LARGE_WORKLOADS.md) use all four as two
disjoint two-disk RAID0 arrays, each with ext4. There is no Spot, fallback instance type, warm pool,
NAT gateway, load balancer, or monthly allowance service. No remote shell is provisioned; the SSM
prerequisite below also excludes inherited account-level management. Availability and the account's
96-vCPU On-Demand quota must be checked in the selected Availability Zone before the first window.

## Essential setup inputs

These account decisions and prerequisites are required. Account IDs, subnet IDs and role ARNs are
non-secret; never put access keys in chat or GitHub settings for this workflow.

1. Choose the AWS account and region. No region is hard-coded.
2. Supply an existing VPC and **public subnet** with an Internet Gateway route. The worker receives
   a temporary public IPv4 address, has no inbound security-group rules, and uses HTTPS for S3. This
   stack does not build a network. Private-only networking requires a reviewed template change and
   suitable existing S3 access; do not casually add a paid NAT gateway.
3. Supply an existing GitHub OIDC provider ARN in that account, with audience `sts.amazonaws.com`.
   If absent, the account owner creates it separately. This stack creates the two narrowly scoped
   roles, but never creates or changes the OIDC provider.
4. Prepare and pin an Ubuntu 24.04 x86_64, EBS-backed AMI with `/dev/sda1` as its root device and a
   root snapshot no larger than 16 GiB. The optional [AMI bootstrap helper](AMI.md) prepares an
   offline plan, then lets the account owner launch one temporary `t3a.small` with an independent
   CloudFormation rollback guard. It installs the fixed harness/supervisor, pinned AWS SDK and
   unprivileged worker user, sanitizes the guest after cloud-init finishes, and captures a private
   AMI. Review its costs, failure recovery and operator permissions before running it. Record the
   source commit and AMI ID. AMI preparation/capture is separate from deployment and CI; no worker
   credentials or future bucket configuration are baked into the image. Both templates explicitly
   select the AWS-managed EBS key `alias/aws/ebs`, and capture verifies the actual snapshot key.
   Customer-managed root snapshot keys are outside this initial setup: do not add KMS grants or
   alter account defaults to make an incompatible image work. Auto Scaling can use the AWS-managed
   EBS key without additional key-policy grants.
5. The account and region must have Systems Manager **Default Host Management Configuration (DHMC)
   disabled** before preparing an image or arming a window. Canonical images can contain an enabled
   SSM agent, and DHMC can supply management credentials without an instance profile. The helper
   checks the setting without changing it; future prepared images disable inherited agents and
   workers verify that state. Disabling the agent in user data cannot prevent its initial boot
   before user data, so the account prerequisite is still required. If DHMC is enabled, stop and
   seek a separate account-security decision; never disable it automatically because it may manage
   unrelated instances. Account administrators must keep this prerequisite true during the build and
   window; a preflight observation cannot prevent a later administrator change.

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

### AMI updates

Changes to worker-executed code or worker-read data in `benchmarks/` (including `cases.json`) or the
baked modules in `infra/aws_benchmark/`, `requirements.txt`, or the installer require a new AMI from
the reviewed commit and a closed-stack update of `WorkerAmiId` before dispatch. Updating the GitHub
checkout does not update the baked harness or supervisor. A change confined to operator/controller
checks, such as `verify_group` in `common.py`, or offline reporting does not require an image
rebuild when worker behavior, worker-read data and the mailbox protocol are unchanged. Record the
actual image source SHA and AMI together, even when the controller revision differs; retain
predecessors only deliberately and do not use an old image with a newer controller protocol. A
run-matching incompatible terminal status is saved with a protocol-mismatch diagnostic to help
identify this case.

`worker.py` builds its `benchmarks.run` arguments from `measurement_contract()` in the baked
`common.py`, which offline approval also reads. The shared contract preserves the arguments of the
preceding inline implementation for every allowlisted suite, but running the updated worker code
still requires a new pinned AMI under the rule above. Offline-only files such as `report.py`,
`changes.py` and `dashboard.html` do not by themselves require an AMI rebuild.

The group check accepts an absent lifecycle policy or the exact explicit default
`RetentionTriggers.TerminateHookAbandon=terminate`, which AWS documents as
[normal termination behavior](https://docs.aws.amazon.com/autoscaling/ec2/APIReference/API_RetentionTriggers.html).
It rejects `retain` and unknown populated policy shapes; lifecycle hooks remain prohibited.

### GitHub environment

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
   trusted operator chooses an absolute UTC opening 2–5 minutes ahead and a cutoff 15–60 minutes
   after opening. Use about 20 minutes for the first pilot. The 15-minute minimum leaves room for
   the ten-minute launch admission floor, up to two minutes of scheduled-action delay, and three
   minutes of headroom. It is an admission minimum, not a guarantee that preparation or a suite will
   complete. All boot, download, formatting, file generation, hashing, validation, cleanup and
   upload time must fit inside this window. The large `bulk-1g-metadata` suite instead requires
   exactly 60 minutes and the stricter admission floors documented below.
3. In a local CPython 3.12 virtual environment, install the operator dependencies with
   `python -m pip install --require-hashes -r infra/aws_benchmark/operator-requirements.txt`. This
   includes the pinned SDK and PyYAML for validating the reviewed launch-template YAML. The worker
   and Lambda retain the separate SDK-only `requirements.txt`; changing operator-only dependencies
   does not require an AMI or Lambda rebuild. Use an operator SSO/session identity:

```bash
python -m infra.aws_benchmark.operator \
  --region REGION --asg ASG_NAME --bucket BUCKET_NAME \
  --run RUN_ID-ATTEMPT --baseline FULL_SHA --candidate FULL_SHA --suite aa-smoke \
  --opens YYYY-MM-DDTHH:MM:SS+00:00 --deadline YYYY-MM-DDTHH:MM:SS+00:00
```

4. After successful read-back, approve the waiting environment job **before the opening**. CI
   refuses to request capacity with less than ten minutes remaining; the worker refuses preparation
   with less than eight minutes remaining. These floors reserve time for boot/preparation and final
   reporting, but cannot guarantee a suite completes. For `bulk-1g-metadata`, require at least 58
   minutes 40 seconds immediately before requesting capacity and 56 minutes 40 seconds before
   storage preparation; equality is accepted at these suite-specific floors. A late approval or
   delayed capacity must fail within the existing lease, never extend it. CI waits for the opening,
   checks the cutoff, uploads bounded binaries, and requests desired capacity 1. It never opens the
   ASG maximum. Afterward, use the raw artifact ID, SHA-256 digest and direct link from the trusted
   `benchmark` job's summary; if its upload failed or no ID appears, there is no trusted raw
   artifact. Never select by artifact name or rely on a candidate build job's summary. Inspect
   `status.json`, `worker.json` inside `results.zip`, the unchanged harness
   `benchmark/results/results.json`, and partial logs. The ZIP is data; review its contents before
   opening anything executable or HTML. The trusted `report` job similarly identifies its report
   artifact; see [result reporting](RESULTS.md).
5. Confirm the controller reports physical teardown. At the cutoff, verify all three ASG capacities
   are zero and all instances tagged for this group are terminated. Normal CI cleanup sets desired
   zero and waits up to ten minutes for the group and live EC2 inventory to empty. The independent
   cutoff also closes maximum, even after cancellation or lost GitHub credentials.
6. Before another run, archive the evidence and complete the separate
   [mailbox retirement procedure](MAILBOX_RETIREMENT.md). Expiry and physical teardown alone do not
   retire the persistent owner or prove that old producers are quiescent. Retire only the inspected
   owner with a conditional delete after all reconciliation requirements hold; neither arm command
   performs this step automatically.

Both legacy and direct operators first reserve `control/mailbox-owner.json` with one conditional
`IfNoneMatch=*` write and a unique invocation token. A conflict refuses arming, even for the same
run. A failed or interrupted arm retains ownership; do not delete it or retry with another producer
until schedules, instances, volumes, delivery horizons and result collection are reconciled. The
owner does not expire. Migrate older whole-bucket expiration rules only while the mailbox is
quiescent; the new claim cannot fence an already-running producer using older code.

Successful CLI output is now a receipt with `window` and `mailbox_owner`, rather than a bare window.
Save it for exact-owner reconciliation. The runtime window object keeps its existing six-field
schema. A partial failure may have stored a claim without returning a receipt; inspect that owner
instead of assuming nothing happened. Owner retirement is a separate deliberate conditional delete
using its current ETag after all producers and cleanup are reconciled, never automatic takeover. The
exact checks and conditional command are in [mailbox retirement](MAILBOX_RETIREMENT.md).

Only one trusted operator may arm this group at a time. The script refuses active capacities,
live/terminating/stopped instances, pending schedules, and a control document whose deadline has not
passed. For every new benchmark attempt, start a **fresh workflow dispatch** with a new run ID so
both revisions are rebuilt. Do not rerun the benchmark job or reuse build artifacts from a previous
attempt: reruns can lose artifacts from successful build jobs that are not rerun. Arm the new run
only after the previous deadline, confirmed physical teardown, producer/drain reconciliation, saved
results and explicit [mailbox-owner retirement](MAILBOX_RETIREMENT.md). Do not manually rewrite the
control document to extend a lease.

To retry only reporting, choose **Re-run job** on the `report` job itself, including when the
`benchmark` job also failed. Do not choose **Re-run failed jobs** or **Re-run all jobs**, which can
rerun the benchmark. A report-only retry uses the original benchmark job's identity, conclusion and
artifact ID, needs no window, and does not acquire the benchmark job's mailbox concurrency lock. If
reporting never started or no trusted raw upload exists, inspect available evidence offline instead;
retrying reporting cannot recreate missing worker evidence.

The operator needs the following account permissions; none of these operator mutation permissions
belong to CI:

- `autoscaling:SuspendProcesses`, `autoscaling:PutScheduledUpdateGroupAction`,
  `autoscaling:DeleteScheduledAction` and `autoscaling:UpdateAutoScalingGroup`, scoped to the exact
  stack-created ASG ARN. The latter two are required for the early-cancellation procedure below.
- The ASG read APIs used by `operator.py`: `DescribeAutoScalingGroups`, `DescribeScheduledActions`,
  `DescribePolicies`, `DescribeLifecycleHooks`, `DescribeWarmPool` and `DescribeInstanceRefreshes`;
  EC2 `DescribeInstances` and `DescribeLaunchTemplateVersions`.
- `ssm:GetServiceSetting` on the account/region setting
  `arn:PARTITION:ssm:REGION:ACCOUNT:servicesetting/ssm/managed-instance/default-ec2-instance-management-role`.
  This read verifies that DHMC is disabled; it does not grant session, shell or setting-change
  access.
- S3 `GetObject` and `PutObject` on exactly `control/window.json`, `control/mailbox-owner.json` and
  `output/status.json`, plus bucket-scoped `GetLifecycleConfiguration` to prove the owner cannot
  expire, and `ListBucket` constrained to `control/window.json` so its initial absence is
  distinguishable from access denial. The status object is initialized and read back before opening;
  CI needs no bucket listing permission.
- For separately reviewed owner retirement only, `s3:DeleteObject` on exactly
  `control/mailbox-owner.json`, plus `ListBucket` constrained to that key to distinguish absence
  from access denial. Use its inspected ETag with `IfMatch`; do not grant deletion of other keys.
  Neither runtime role may delete the owner, and neither arm command does so. This documents
  prerequisites for the operator; it does not apply a permission change.

Infrastructure deployment uses a separate trusted identity with permission to create the resources
declared in the template and pass the worker role. There is no need to grant CI `PassRole`, SSM,
direct EC2 launch, IAM, or infrastructure-deployment access.

## Cutoff and failure behavior

CloudFormation has no permanent `SuspendedProcesses` property. While the group is closed, the
operator script suspends and verifies `AZRebalance`, `InstanceRefresh`, `HealthCheck`,
`ReplaceUnhealthy`, and `AlarmNotification`. `Launch`, `Terminate`, and `ScheduledActions` stay
enabled. It rejects scaling policies, lifecycle hooks, retention, warm pools, protection, active
refreshes, custom termination policies and automatic maximum-lifetime replacement.

The operator first creates and reads back `close-RUN_ID-ATTEMPT`, a one-time action setting minimum,
desired and maximum to zero at the absolute deadline. It then writes the run-bound control document
and initializes `output/status.json` for this run with state `pending`, reading both objects back
before scheduling `open-RUN_ID-ATTEMPT` at the earlier absolute opening time. The opening sets
minimum/desired to zero and maximum to one. Scheduling the opening avoids a delayed direct
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

The controller verifies the run-bound pending status before requesting capacity, then polls until
the worker publishes state `completed`. Missing objects, access denial and invalid status fail
explicitly; they are never interpreted as an unfinished run. Run-matching non-pending status is
saved before terminal-schema validation, including diagnostics from an incompatible old image. Safe
reads and fixed-key mailbox writes receive at most three attempts with one- and two-second backoff
for transient transport/service faults, including TLS errors and short response bodies. Retries
check the original deadline and phase reserve before each attempt; an already completed transfer is
retained when it consumes its reserve. The next phase checks its own allowance. Initial
authentication/control reads have a short bootstrap budget. The desired-one request and schedule
mutations remain single-attempt: an uncertain response requires the existing cleanup or
read-back/reconciliation path, not an automatic second launch or re-armed window.

The fresh controller executes only code checked out at the dispatched main commit. It never runs
downloaded binaries, builds candidate source, extracts result ZIPs, or exports AWS credentials to
other jobs. Build jobs can execute arbitrary Rust build scripts, so pin and review the chosen
revisions; a revision string and digest provide traceability, not proof that a hostile build told
the truth. There are no shared build caches or self-hosted GitHub runners in this workflow.

The worker AMI contains the reviewed root-owned supervisor and harness. The installer persistently
masks Ubuntu's `apt-daily` and `apt-daily-upgrade` timers and services, allowing active package work
up to five minutes to drain without killing it. Every worker verifies the four units remain masked
and inactive before downloading binaries or preparing disks; failure produces a failed run. The
installer also removes inherited snap/deb SSM agents and registration data, persistently masks both
service names, and verifies absence. Each worker repeats that read-only verification with
budget-clamped commands before disk preparation. APT and SSM state are recorded in `worker.json`.
Rebuild from a current Canonical image for security updates outside benchmark windows; automatic APT
maintenance is deliberately frozen in these disposable images. This does not eliminate every source
of host noise.

Six exact raw binary objects are accepted, each at most 128 MiB, with SHA-256 checked before use. No
input archives, paths, shell commands or root scripts are accepted. A request must match the
operator's run, revisions, suite and deadline, use exactly the defined fields, and provide one
lowercase SHA-256 digest for each binary. Disk preparation runs before candidate execution. It
selects exactly four expected-size devices with the exact EC2 instance-store model from an explicit
`lsblk --tree` inventory. Both layouts reject partitions, filesystems, mountpoints, active swap,
holders, changed identity and read-only devices, and pin all four devices before any format. The
legacy layout formats the first two lexical paths; large profiles build two disjoint RAID0 arrays.
Both use held descriptors for formatting and mounting, verify the resulting ext4 type and mounted
device identity, and reject existing output/mount paths. EBS/root devices never qualify. Only this
trusted supervisor has device access.

The harness and binaries run as `rcp-benchmark` in a systemd cgroup with no capabilities, no new
privileges, private networking/devices, read-only system files, protected control groups, a 180 GiB
memory limit and an 8192-task limit. The process cannot reach instance metadata, S3, or GitHub. The
instance role itself has only fixed S3 reads/writes, no provisioning rights. This is process
isolation for a dedicated disposable host, not a separate kernel or a hostile-code VM.

Targeted suites use the existing `linux-drop-caches` policy. A root-owned executable named `sudo`
accepts exactly the harness's fixed cache-drop argv and sends a fixed request to a root supervisor
UNIX socket. The helper verifies the peer UID, calls `sync`, and writes `3` to `drop_caches`; it
accepts no paths or commands. No sudoers grant is installed. Fixture generation, validation, source
reuse, pairing and cleanup stay in the existing harness. Existing suites retain full SHA-256
validation; the explicitly named `bulk-1g-metadata` performance suite checks structure, metadata and
sizes without reading payload contents. Its reports disclose that content integrity is not checked.
The random-byte `filegen` fixture policy is unchanged; no sparse or all-zero replacements are used.

The initial worker control read has a separate 60-second bootstrap deadline and bounded retries.
Once a complete, valid control document arrives, its absolute cutoff replaces that bootstrap
deadline. The worker reserves three minutes for stop/collection/upload. The whole measurement
cgroup, including preparation and descendants, has a remaining-window runtime limit and a ten-second
kill grace. A wall-clock deadline plus monotonic ceiling prevents clock movement extending work. The
outer alarm raises a dedicated `BaseException` so SDK/socket `Exception` handlers cannot swallow the
deadline. Failures never extend the lease. Candidate-owned results are collected only after the
service is stopped and its cgroup is empty, using held directory/file descriptors, no symlink
following, regular-file checks and bounded traversal. If stop verification or collection fails, a
metadata-only archive preserves the trusted error and service outcome without traversing
candidate-owned files. If that fallback also fails, the worker skips the archive and still attempts
bounded terminal status publication. Supervisor command diagnostics retain only command name, exit
code or timeout, and bounded AWS error codes; they never copy candidate stderr or request
credentials.

Failure to start the supervisor requests poweroff directly from user data. Once started, the
supervisor unit requests poweroff on both success and failure, including an exhausted final-status
upload. Successful exit occurs after result/status publication. The EBS-backed worker uses the
default stop-on-guest-shutdown behavior. This provides a compute-stop backstop, not verified
physical termination: stopping is asynchronous and storage can still incur charges. Unhealthy
replacement is suspended; the controller and scheduled cutoff remain responsible for desired zero
and termination.

Output storage is a 128 MiB tmpfs; private temporary directories are each 64 MiB with writable mode
`1777`. A result archive is at most 64 MiB and 1000 candidate entries, with at most five directory
levels below the output root, matching `results/logs/<case>/<variant>/<iteration>/`. Candidate files
are archived under `benchmark/`; trusted supervisor metadata is the separate root `worker.json`. The
collector reserves space for that trusted metadata and prioritizes `results.json` and `summary.md`
before logs. Each log is capped at 1 MiB using its head and tail; oversized non-log files that
exceed the remaining archive budget are omitted. `worker.json` records the total number of
omissions/truncations and up to 128 entries with name, original size and archived bytes, so these
limits are explicit. Standard service output is discarded. Unsafe files or failed traversal produce
a metadata-only failure archive. Failed/incomplete harness records can be downloaded but must not be
treated as successful comparisons.

The unversioned S3 mailbox has a fixed number of exact keys. Input/output prefixes and the published
window/request control keys expire after seven days; incomplete multipart uploads are aborted after
one day. The separate `control/mailbox-owner.json` marker never expires and requires deliberate
retirement after reconciliation. Waiting seven days does not free the mailbox. GitHub build
artifacts live one day; result/report artifacts live 90 days (subject to the repository limit). Size
limits are enforced by trusted code, **not by IAM PutObject conditions**. A stolen runtime
credential can overwrite permitted objects or issue excess requests; neither fixed keys nor
lifecycle rules are a hard storage/billing cap. S3 storage/requests, AMI snapshots, EBS while
allocated and public IPv4 addresses can cost money even though there is no always-running VM. Empty
the bucket before deleting the stack, and delete obsolete AMI snapshots separately.

The existing suites generate at most 30–31.25 GiB, below host RAM. The separate
[large-workload manifest and qualification plan](LARGE_WORKLOADS.md) add decimal 1 TB/1.024 TB bulk
layouts and a 10,240,000-file tiny layout. The four large full-content suites are retained for local
correctness diagnostics and historical parsing, with distinct storage/verification identities; they
cannot enter performance history and are disabled for new AWS windows because runtime within 60
minutes has not been established. The separate `bulk-1g-metadata` performance suite reuses the 1 TB
case with metadata/size validation. The legacy offline history gate requires independent per-attempt
approval; the separate, initially disabled [production publisher](PRODUCTION.md) has its own
reviewed publication authorization and history-protection gates. These suites require a new reviewed
AMI and an independently authorized, bounded qualification window.

## Result reports

The read-only report job validates bounded evidence and produces a standalone report with separate
candidate, baseline, controller and baked-worker provenance. Raw results remain unchanged. See
[result ingestion and reporting](RESULTS.md) for artifact contents, cancellation behavior, parser
limits, the security boundary and legacy offline history staging. The separate production publisher
remains disabled pending its acceptance gates. Depot benchmarking is retired; ordinary Depot builds
and non-benchmark tests remain available.

## Offline validation and first live acceptance

Run `just aws-benchmark-test` and `just benchmark-test`. Both are included in lint/CI. The focused
tests use fake AWS clients and local temporary files, never credentials or network. Validate the
template with `cfn-lint infra/aws_benchmark/stack.yaml` as well. The large-profile tests use
arithmetic, simulated block inventories and small real file trees; they do not format devices or
generate the large payloads.

If no worker status arrives, the trusted operator should inspect the exact ASG's activity history
for launch failures (capacity, quota, AMI or KMS errors), alongside EC2 inventory and the scheduled
cutoff. This uses operator `autoscaling:DescribeScalingActivities`; the runtime role is unchanged.
Keep the cutoff armed and do not retry with a different type, key grant or extended deadline.

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
- [EBS encryption and Auto Scaling key access](https://docs.aws.amazon.com/autoscaling/ec2/userguide/key-policy-requirements-EBS-encryption.html)
- [Default Host Management Configuration](https://docs.aws.amazon.com/systems-manager/latest/userguide/fleet-manager-default-host-management-configuration.html)

## Optional durable capacity acquisition

The disabled [durable protocol](DURABLE.md) uses direct EC2 capacity retries for up to 12 hours and
an immutable 180-minute envelope from each launch attempt's start. An independent expiry guard is
verified before allocation; GitHub uses short submission and collection jobs. This original ASG
operator-window protocol remains available. Review the new coordinator launch permission and funding
before deployment; no recurring benchmark schedule is created.
