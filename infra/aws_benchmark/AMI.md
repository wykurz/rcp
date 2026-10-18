# Prepare the worker AMI

This optional helper is an **account-owner operation**, separate from the GitHub workflow and the
benchmark ASG. Preparing files is offline. Running the helper launches a paid temporary `t3a.small`
and captures a private AMI. The repository never invokes it automatically. Its automated tests use
fake responses; account pilots require separate authorization and incur charges.

## Inputs and review

Use an authenticated AWS CloudShell session in your chosen **commercial AWS account and region**.
CloudShell already provides Python 3 and AWS CLI v2; this helper needs no Python packages. Use an
operator session, never secret keys in chat or source files. Supply only:

- The expected 12-digit account ID and explicit region.
- An existing VPC and public subnet in that account, with an Internet Gateway route and working DNS.
  The builder needs outbound HTTP/HTTPS for Ubuntu packages, GitHub and PyPI. No network, NAT
  gateway, static address, role, key pair, inbound rule or SSM access is created.
- An explicitly chosen Canonical Ubuntu 24.04 amd64 **server release** AMI in that region. Verify
  owner `099720109477`; pin the ID, not a dynamic "latest" reference. The helper checks its owner,
  name, x86_64/HVM/ENA, EBS `/dev/sda1` root of at most 16 GiB, and boot compatibility with both
  `t3a.small` and `c5d.24xlarge`. It allows explicit legacy `ephemeralN` instance-store references
  (such as Canonical's `/dev/sdb` and `/dev/sdc`) while requiring exactly one EBS volume: the root.
  It rejects extra EBS volumes, ambiguous or duplicate mappings, and Marketplace product codes. The
  source root may be unencrypted; the builder template explicitly creates an encrypted 16 GiB root,
  and capture validation requires its owned encrypted snapshot.
- A full reviewed public `wykurz/rcp` commit SHA containing this helper and the worker installer.
  Check out that exact commit locally. Preparation reads the template, guest script and embedded
  maintenance helper from that commit's Git blobs, so staged or unstaged edits cannot enter the
  generated template. The guest fetches only that SHA without credentials.

Review the source and generated template before the paid command. `prepare` does not contact AWS:

```bash
git clone https://github.com/wykurz/rcp.git
cd rcp
git checkout --detach REVIEWED_FULL_SHA
python3 -m infra.aws_benchmark.ami_build prepare \
  --account ACCOUNT_ID --region REGION --base-ami CANONICAL_AMI_ID \
  --vpc VPC_ID --subnet SUBNET_ID --revision REVIEWED_FULL_SHA \
  --directory "$HOME/rcp-ami-build-1"
cat "$HOME/rcp-ami-build-1/manifest.json"
cat "$HOME/rcp-ami-build-1/template.json"
# Paid operation below: run yourself only after reviewing the two files.
python3 -m infra.aws_benchmark.ami_build run --directory "$HOME/rcp-ami-build-1"
```

Keep the directory in persistent CloudShell home and download a copy of `manifest.json`. One process
may operate on it at a time. `run` is one-shot after submitting a stack request, including an
uncertain or failed request. It cannot resume or launch a replacement builder. A successful result
prints the private AMI ID and snapshot IDs; retain the source SHA with them. Use that AMI as
`WorkerAmiId` in the separate [benchmark setup](README.md). No bucket configuration or runtime
credentials are baked into the image.

## What the builder does

The temporary CloudFormation stack contains one security group and one instance. The instance uses
Standard CPU credits (no unlimited-credit surcharge), a 16 GiB encrypted gp3 root with
`DeleteOnTermination`, IMDSv2, no instance profile or key pair, and no inbound access. Budget for
compute, the temporary public IPv4 address, EBS storage and retained snapshots. EBS storage and
snapshots continue to incur charges while the instance is stopped.

An EC2 `CreationPolicy` waits for a signal that **is never sent**. `CreateStack` also has a
60-minute timeout and rollback enabled. This AWS-side rollback does not depend on CloudShell
remaining connected. Do not deploy `ami-builder.json` directly, signal success, disable rollback, or
retain its resources: those actions defeat the cleanup guard. Only the helper submits the reviewed
generated template with the required timeout.

User data schedules a transient systemd service after `cloud-final` finishes, using `--no-block` to
avoid a dependency deadlock. Before its first package command, it stops automatic APT timers, then
persistently masks the timers and services and waits at most five minutes for any existing package
operation to finish; it never kills active apt/dpkg work. The separate timer stop is intentional:
systemd 255 can reload an active masked timer into `failed/resources` before `mask --now` stops it.
Stopping timers first avoids this failure without clearing failed states or relaxing the final
masked-and-inactive checks. It then installs git and CA certificates before running the worker
installer, which verifies APT remains quiet. The same check runs before each benchmark. Rebuild the
disposable AMI from a current Canonical image for security updates outside benchmark windows. After
installation it resets cloud-init, machine identity, SSH keys, shell histories and logs. It emits a
build-specific readiness marker only after sanitation, then powers off. Failure or the service's
30-minute runtime limit also requests poweroff. These guest actions are secondary guards; they do
not replace CloudFormation rollback. The image installer is pinned by SHA; Ubuntu package updates
are resolved at build time, so rebuilds are not bit-for-bit reproducible.

The helper requires both the matching UUID/SHA console marker and EC2's `stopped` state, then
captures with `NoReboot` so sanitation is not undone by another boot. It refuses to start capture
with less than 20 minutes remaining. Normal stack deletion happens only after the private AMI is
`available` and every root snapshot is `completed`, with ownership and source-volume checks. It
verifies stack deletion, instance termination and root-volume removal before reporting success.

The 60-minute timeout **initiates** rollback; it is not an exact termination time or dollar cap.
CloudFormation or EC2 failures can delay cleanup or require manual repair. AMI capture can take much
longer than this window, occasionally up to 24 hours. A cutoff during capture is a failed build
requiring reconciliation, even if a snapshot later completes. Do not extend the timeout to rescue
it. Images and snapshots are outside the temporary stack and survive its deletion; they incur
storage charges until explicitly removed. Keep at most the current worker AMI and one deliberately
retained predecessor; review and delete unwanted build artifacts after each attempt.

## Interrupted or failed builds

`run` prints UTC timestamps, elapsed time and stage changes to stderr. While waiting it prints the
latest observed stack, instance or capture status at most once every 30 seconds, between the
existing bounded AWS calls. A slow request can delay an update. These messages do not reset any
deadline. Read-only AWS CLI operations use standard retry mode with at most three total attempts
inside the existing 90-second subprocess timeout. All mutations remain single-attempt, especially
`CreateImage`, which has no idempotency token. Initial EC2 instance discovery tolerates an empty
response or `InvalidInstanceID.NotFound` until the original readiness cutoff; authorization and
other permanent failures still stop the helper. Missing instances during cleanup never prove
successful termination.

After discovering the builder, the helper prints a read-only `get-console-output` command for that
instance. Run it in a second session for a boot-log snapshot. EC2 console snapshots can arrive late,
be empty initially or truncate older output; they are not a live stream. An empty snapshot does not
establish that the builder stalled. New builds send preparation and installer output to the console,
including timestamped stages and failure stage/line/exit status, without shell tracing or command
and environment dumps. No SSH, SSM, instance profile or inbound access is added. Older prepared
templates keep their original logging behavior; changing the checkout cannot update an existing
builder. Once the instance is first observed stopped, the helper allows ten minutes of console lag
(earlier account pilots observed about seven), measured monotonically and still bounded by the
original 40-minute capture refusal. If no matching marker arrives, it fails with the last observed
guest stage, or `unknown` if none was available. Bounded AWS requests can delay the next check; the
grace never extends the capture deadline or rollback. Outer bootstrap failures and preparation
SIGTERM emit failure markers; SIGKILL or an external stop may leave only the console-lag diagnostic.

Preserve the manifest and available console/stack-event evidence after a failure. A generic failure
marker alone does not identify a cause. Keep the rollback deadline; do not retain a disk or launch a
recovery host speculatively to retrieve its journal. Any separately approved recovery must account
for its permissions, cost and cleanup. Cloud-init nonzero status remains a build failure, including
exit 2 for recoverable errors, until the underlying errors are understood.

A failure during local validation or read-only preflight leaves the manifest at `stage: prepared`,
without `submitted_at`. That run submitted no stack request, so no rollback was armed. Correct the
reported error before retrying the unchanged preparation. If changing inputs or the reviewed source
revision, run `prepare` into a fresh directory instead of editing the manifest or template.

Once launch intent is recorded (`create_requested` or any later stage), never rerun `run`, start
another builder, or delete snapshots speculatively. The manifest records intent **before** each
mutation, including the stable stack name/token and image name. A lost `CreateStack` response may
have created the stack; inspect before cleanup. A lost `CreateImage` response may still have created
the image; that API has no idempotency token. Never reset the manifest stage to bypass this guard.

This command makes read-only AWS calls and refreshes the manifest, including owned orphan snapshots:

```bash
python3 -m infra.aws_benchmark.ami_build inspect --directory "$HOME/rcp-ami-build-1"
```

Match the recorded account, region, exact stack ARN, UUID tag `rcp-ami-build`, source instance/root
volume, AMI name and IDs before any manual mutation. Inspect stack events if rollback or deletion
failed. The helper never automatically deregisters images, deletes snapshots, or starts
replacements.

- Before capture, or after all capture snapshots finish, the owner may delete the exact verified
  temporary stack and confirm instance termination/root deletion. `DELETE_COMPLETE` means cleanup
  finished; a request returning successfully alone does not.
- While capture is pending or its outcome is unknown, leave normal cleanup alone and reconcile. The
  independent timeout may still roll the stack back. Report that attempt as failed; do not assume an
  eventually available image passed the normal completion checks.
- After an uncertain image response, inspect images owned by your account with the exact recorded
  image name and UUID tag, and snapshots with that tag, including snapshots with no registered AMI.
  Wait for pending snapshots to settle before deciding what to retain or remove. Record every ID.
- To discard an unwanted completed image, verify its owner/tag and root snapshot ID, deregister that
  exact AMI, then delete only its verified owned snapshots after checking they are not used by
  another AMI. Confirm removal. If no image was registered, apply the same ownership/source-volume
  checks to orphan snapshots. Do not delete resources based on names alone.
- If the session ended after `delete_requested`, inspect the recorded stack ARN. A lost delete
  response may already have completed cleanup. Retrying `DeleteStack` for that same verified ARN can
  use the stored token `delete-BUILD_UUID`; never substitute a new stack.

Resolve any `inspection_errors` before declaring cleanup complete. AWS may no longer return a
terminated instance after some time; use the stack events and recorded IDs to reconcile rather than
assuming absence proves every disk was deleted. There is no automatic successful recovery command:
the operator reviews a failed attempt before using or removing its output.

## Operator permissions

These permissions are for the human image builder, **never the GitHub runtime role**. No IAM/OIDC
changes, `PassRole`, SSM or guest credentials are required. The caller needs STS
`GetCallerIdentity`; the EC2 read operations used by the helper (`DescribeImages`,
`DescribeImageAttribute`, `DescribeInstanceTypes`, `DescribeSubnets`, `DescribeInstances`,
`DescribeSnapshots`, `DescribeVolumes`, `GetConsoleOutput`); EC2 `CreateImage` and tagging for its
own image/snapshots; and CloudFormation `CreateStack`, `DescribeStacks`, `DescribeStackResources`,
`DeleteStack` for the temporary stack. Event inspection additionally uses `DescribeStackEvents`.

CloudFormation runs with the caller's permissions (no service role is passed). That identity also
needs the EC2 create/read/tag/delete permissions to provision and roll back the declared security
group, instance, network interface and root volume, including Standard credit configuration. An
existing infrastructure operator can run this; do not grant these deployment permissions to CI or
the instance. The account owner controls any IAM policy setup. Discarding retained artifacts later
requires `DeregisterImage` and `DeleteSnapshot`, which the helper itself never invokes.

## Offline validation and references

`just aws-benchmark-test` covers account mismatch, lost launch/image/delete responses, readiness
markers, incomplete/foreign snapshots, cutoff rollback, retained roots, the template constraints and
sanitation order. It never invokes AWS. Also validate the template with `cfn-lint` and the guest
script with `bash -n`. A real account pilot remains necessary to verify regional image availability,
networking, boot behavior and service cleanup; unit tests do not prove those integration behaviors.
Tests also exercise bounded progress, disconnected output, guest/installer failure diagnostics and
readability of harness modules copied from a private checkout. The installer verifies imports as the
unprivileged benchmark user before the image can become ready. APT tests cover active timers at
startup, the systemd 255 mask-before-stop failure, failed timers, stop errors/timeouts and service
draining. The timer failure and corrected ordering were also reproduced with disposable user units
on Ubuntu systemd 255.4, without changing host APT units. Builder regressions cover dirty checkout
inputs, read/mutation retry separation, eventual instance discovery, bounded stopped-console lag,
and shell error/TERM diagnostics without invoking installers or host service operations.

- [CloudFormation CreationPolicy](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-attribute-creationpolicy.html)
- [CreateStack timeout and rollback](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_CreateStack.html)
- [Deleting a stack still in progress](https://repost.aws/knowledge-center/cloudformation-stack-stuck-progress)
- [EBS AMI creation and capture duration](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-an-ami-ebs.html)
- [Canonical image ownership and names](https://ubuntu.com/aws/docs/aws-how-to/instances/find-ubuntu-images/)
- [cloud-init cleanup](https://docs.cloud-init.io/en/latest/reference/cli.html#clean)
- [cloud-init failure states](https://docs.cloud-init.io/en/latest/explanation/failure_states.html)
- [EC2 console output](https://docs.aws.amazon.com/cli/latest/reference/ec2/get-console-output.html)
