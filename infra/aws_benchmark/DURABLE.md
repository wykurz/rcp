# Durable capacity acquisition

The optional direct-launch protocol waits up to **12 hours for capacity** and gives an allocated
worker at most **180 minutes from the start of its launch attempt**, including boot. It is manual
and disabled by default. Deploying the default template starts no worker and schedules no benchmark.
The existing ASG operator-window workflow and historical result formats remain supported.

This path replaces ASG acquisition for the durable request. It does not raise the existing ASG's
maximum or give GitHub EC2 permissions. A dedicated coordinator gains restricted EC2 launch
permission, which requires separate deployment approval. Read
[DURABLE_PERMISSIONS.md](DURABLE_PERMISSIONS.md) before activating anything.

## Request and execution

Build pinned revisions without AWS credentials first. Identical A/A revisions build once and use the
same baseline artifact for both roles, avoiding drift between independent build runners. A trusted
operator prepares and arms one exclusive request with the six binary hashes, controller revision,
region, subnet, security group, instance profile, AMI, numeric launch-template version, suite and
immutable timing plan. The first qualification supports only `bulk-1g-metadata` with identical
revisions and identical binary bytes. It retains four adjacent, balanced local A/A copies and the
existing cold-cache behavior. Initial and per-trial file-content hashing are disabled. Path, type,
count, size and required metadata checks, copy exit status and timeouts still apply; these checks do
not prove content integrity or detect same-size corruption. The existing 1,200-second per-copy
timeout is unchanged.

A short GitHub job uploads the prebuilt binaries and starts one Standard Step Functions execution.
Its name binds the original workflow run ID and attempt. GitHub does not hold credentials or a
hosted runner open during the capacity wait. A separate manual collection workflow fetches partial
or terminal evidence. Submission is not successful qualification, and collection does not publish
performance history. No recurring benchmark schedule is introduced.

The coordinator persists the exact launch parameters, idempotency token, attempt ID, start time and
deadline before calling `RunInstances`. Region, subnet and all parameters stay fixed for that
attempt. Only an explicit, authenticated `InsufficientInstanceCapacity` response permits another
attempt within the capacity window. Timeouts, transport failures, other service errors and an empty
instance listing do not establish that allocation failed. An ambiguous response holds the same
attempt for reconciliation; it cannot create a fresh allocation opportunity or deadline. A single
positively observed instance can recover the accepted outcome only when its ClientToken, immutable
attempt tags and observable launch pins match the saved exact request. Empty inventory is never
proof of no allocation. There is no automatic replacement after an allocation, including an instance
that EC2 terminates early in pending: a StateReason alone does not reopen the allocation allowance.
[EC2 idempotency](https://docs.aws.amazon.com/ec2/latest/devguide/ec2-api-idempotency.html),
[eventual consistency](https://docs.aws.amazon.com/ec2/latest/devguide/eventual-consistency.html).

## Independent guard and immutable attempt deadline

Before each launch, an independently scheduled expiry guard must have completed a recent, successful
scan of the fixed managed namespace. It scans EC2 and terminates expired managed instances before
accessing the lease table, so losing coordinator state or DynamoDB availability does not disable
expiry enforcement. A missing or stale guard heartbeat defers allocation without failing the
capacity request. Transient read errors defer observation; known accepted launches are never
downgraded because a state write failed. Contract violations still fail closed. The launcher cannot
write the guard heartbeat or change the guard's code, role, rule or configuration.

`RunInstances` supplies the namespace, run, attempt, creation time and absolute deadline as atomic
creation tags on both the instance and its root volume. If creation tagging fails, AWS rolls back
resource creation. Neither the guest nor coordinator may edit those tags afterward. The guard
selects only the configured namespace; malformed expiry metadata on a managed instance fails closed.
The deadline is capped against actual launch time as well. Once the instance ID is known, a second
immutable Scheduler action targets that exact instance for termination. No replacement is requested
after it terminates.
[EC2 creation tags](https://docs.aws.amazon.com/us_en/AWSEC2/latest/UserGuide/Using_Tags.html).

The independent scan exists before the instance ID is known, covering a lost launch response,
controller crash, stalled boot and late visibility. The direct-ID schedule is a second path, not a
prerequisite for discovering an expired instance. There is no ASG replacement loop or lifecycle hook
that can repeatedly allocate workers while a handler is unavailable. The retained global lease
prevents overlapping direct requests. Both operator protocols additionally acquire the same atomic
S3 mailbox reservation before publishing control data or arming schedules; it persists through
capacity waiting, execution, delivery drain and collection. Neither owner expires or transfers
automatically. A partial arm failure retains ownership for deliberate reconciliation.

The nominal timing contract is a 60-second tick, 45-second Lambda limit, 90-second ambiguous-launch
acknowledgement threshold and 120-second guard freshness/guest assignment allowances. The latter
leave 15 seconds beyond one tick plus one invocation. These are availability allowances, not AWS
delivery guarantees: late or failed operations can still close or hold a run without extending any
allocation deadline. A positively matched instance is reconciled before the acknowledgement
threshold is evaluated.

The guest polls for an assignment for at most 120 seconds, treating a missing fixed-key object
(including S3 403 without ListBucket) as not yet published. This does not grant bucket-list access
or admit any unbound work. The guest checks its IMDSv2 instance ID against the assignment. Actual
EC2 launch time is evidence, not a new clock: the assignment's start is the persisted attempt start.
A root-owned persistent marker prevents reboot or service restart from reusing the assignment. Its
wall-clock and monotonic timers include downloads, disk preparation, fixture generation, copies,
verification, packaging and upload. The guest stops five minutes before the external deadline;
failure never extends it.

The 12-hour capacity expiry plus runtime is retained as an **execution/reconciliation horizon**. It
is not permission to bill a running worker for 15 hours and is not its shutdown fallback. A delayed
attempt cannot obtain more time by replacing its token, moving to another Availability Zone,
changing tags or rebasing the deadline to boot completion.

## Runtime planning

The planning contract is separate from measurement and verification identity:

| Assumption                                   |              Seconds |
| -------------------------------------------- | -------------------: |
| Projected fixture generation and four copies |                3,104 |
| Projected startup                            |                  100 |
| Projected reporting/upload                   |                  180 |
| Forecast total (not a measured complete run) | 3,384 / 56.4 minutes |
| Work and reporting multiplier                |                    3 |
| Required time remaining at guest admission   |                9,852 |
| Boot planning reserve                        |                  300 |
| Shutdown margin within the attempt envelope  |                  300 |
| Minimum compatible attempt envelope          |               10,452 |
| Default and maximum attempt envelope         | 10,800 / 180 minutes |

The admission floor is `(3104 + 180) * 3`; it is not proof that the workload will finish. Time spent
in the launch API and boot consumes the attempt's envelope. Runtime can be configured within this
contract, up to 180 minutes. Different assumptions require a reviewed plan identity rather than an
unreported timeout change. All four copies use identical binaries, so this is an end-to-end A/A
qualification with limited evidence about variance, not a baseline-versus-candidate performance
claim.

## Failure limits and cost planning

The independent guard removes the known dependence on a post-launch controller successfully arming
the first cutoff. It does not guarantee an exact physical runtime or dollar cap. EC2 visibility and
termination are asynchronous; Scheduler has minute precision and bounded retries, not a guaranteed
maximum delivery latency. Simultaneous guard, IAM or EC2 control-plane failures still require
operator intervention. Before the direct-ID schedule exists, a persistent loss of the janitor alone
can delay termination: its recent heartbeat proves recent health, not future availability. Once
installed, the direct-ID schedule no longer needs the janitor to run. Runtime principals cannot
disable the guard, but privileged administrators can. Keep monitoring and preserve the lease until
physical instances and root volumes are reconciled.
[Scheduler precision](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html),
[retry policy](https://docs.aws.amazon.com/scheduler/latest/APIReference/API_RetryPolicy.html).

The October 21 planning example uses the recorded $4.608/hour worker rate, which needs a fresh
regional quote before any launch. It includes one minute of delivery precision, five minutes of
retry age and ten minutes of assumed termination margin. Three retries share the five-minute age
budget; the ten-minute termination margin is an operational assumption, not an AWS guarantee.
Compute while waiting for an explicit no-capacity response is zero; an allocated running instance is
billable even while idle or booting.
[EC2 billing states](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html).

| Component                                                     | Planning estimate |
| ------------------------------------------------------------- | ----------------: |
| Worker compute, 180 minutes plus 16-minute shutdown allowance |          $15.0528 |
| Builder, root EBS and public IPv4                             |           $0.1344 |
| New 16 GiB snapshot retained 30 days                          |           $0.8000 |
| Lambda supervision for 16 hours                               |           $0.5000 |
| Step Functions, DynamoDB and event delivery                   |           $0.3900 |
| S3, logs and GitHub artifacts                                 |           $0.3500 |
| Total, excluding tax and free tiers                           |        **$17.23** |

This suggests an $18 allowance after fresh prices and the actual image/storage needs are verified.
The effort has $62 conservatively reserved against an $80 cap; $18 remains unallocated. The already
allocated $18 qualification allowance includes code-storage and image preparation and must not be
reserved a second time. This example makes no new reservation. The operator admission helper
requires a fresh itemized reservation including runtime, shutdown allowance, builder, retained
image, storage, transfer and coordinator costs. Do not treat an increased timeout as an increased
budget. [Lambda pricing](https://aws.amazon.com/lambda/pricing/),
[Step Functions pricing](https://aws.amazon.com/step-functions/pricing/),
[DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/).

The minute guard has small ongoing serverless cost while enabled, including while idle. Suspend new
admission independently of guard supervision. Disabling the guard to save idle cost is safe only
after all attempts, late arrivals and physical cleanup are reconciled. No NAT gateway, continuously
running VM, remote shell, SSM execution channel or monthly allowance service is added.

## Setup checklist

Only the following non-secret account settings and actions are needed. Never put secret access keys
in chat, the repository or GitHub settings.

1. Choose the AWS account and region. Reuse the approved public subnet, inbound-closed security
   group, artifact bucket, worker instance profile and existing GitHub OIDC provider. Keep the old
   ASG at 0/0/0. First reconcile all older producers, windows, instances, volumes and cutoff
   delivery; the new owner key cannot fence an already-running old-code producer. Review the
   artifact bucket lifecycle update so `control/mailbox-owner.json` has no expiration, while payload
   keys remain bounded. Arming refuses an incompatible lifecycle. No network or OIDC-provider change
   is needed.
2. Review the exact service roles and deployment actions in
   [DURABLE_PERMISSIONS.md](DURABLE_PERMISSIONS.md). Keep both enable parameters false initially.
   Supply an existing versioned S3 code bucket and pinned code object version; the stack does not
   implicitly create code storage or grant access to customer-managed KMS keys.
3. Build a reviewed image containing `rcp-direct-v1`. Prepare a separate unused numeric launch
   version with `WorkerProtocol=direct`, a pinned AMI and the 10,800-second supervisor; record the
   exact version in the direct configuration. Do not select it on the ASG or change the template's
   default. Updating the worker stack itself would select its latest version on the ASG and is not
   an interchangeable preparation step. Legacy arming requires the exact reviewed operator-window
   startup and refuses direct user data before opening capacity. Preserve the AWS-managed EBS key,
   disposable root volume, disabled DHMC and image provenance checks. Worker-role permissions do not
   expand.
4. Record the dedicated namespace, lease table, runtime schedule group, termination role,
   state-machine ARN and GitHub role ARN. Configure protected environment `aws-benchmark-durable`
   with required review and exact main branch. Do not enable admission during deployment testing.
5. Supply `RCP_AWS_DURABLE_ROLE_ARN` and `RCP_AWS_DURABLE_STATE_MACHINE_ARN`, alongside the existing
   region, artifact bucket and global benchmark enable variables. Keep
   `RCP_AWS_DURABLE_ENABLED=false` until guard operation, IAM negative cases and funding are
   verified. Build jobs have no OIDC permission; submission and collection use fresh 15-minute
   sessions.
6. Verify the deployed guard's completed scans, heartbeat access separation, namespace filtering and
   direct termination policy before approving a paid qualification. Static unit tests do not prove
   deployed IAM behavior. Review encrypted-AMI launch prerequisites without expanding key policy or
   changing account defaults to work around an incompatible image.

`AdmissionEnabled=false` blocks new submissions while `RuntimeEnabled=true` keeps both the guard and
existing executions active, including requests still waiting for capacity. Lambda service retries
retain the same execution and persisted allocation identity; they do not create new leases. Do not
disable runtime supervision, update guard code/roles, change the image/template/network, delete
schedule groups or retire the lease while an attempt is unresolved. Service-role credentials
continue independently of the operator's SSO expiry, but deployment and subsequent manual recovery
still require a usable operator identity.

### Packaging and operation

Use a build Python environment with pip and PyYAML; the minimal runtime environment may omit pip.
Download the hash-pinned wheels from `requirements.txt` into a scratch wheel directory separately.
Then:

```sh
python -m infra.aws_benchmark.durable_package --wheels WHEEL_DIRECTORY --output NEW_ARCHIVE.zip
```

This builds offline with `--require-hashes` and `--no-index`, prints a digest and uploads nothing.
The archive includes the pinned SDK rather than relying on Lambda's mutable bundled SDK. Excluded
console-script RECORD rows are normalized, so the digest does not depend on the interpreter path.
Tests build an offline package and import its handlers outside the checkout. Review and upload it
separately, retaining its immutable S3 object version and digest.

After the required binary artifact or artifacts exist, prepare an operator JSON with the exact
fields in `durable_aws.Configuration`: `region`, `account`, `namespace`, `table`, `bucket`, `lt_id`,
`lt_version`, `ami`, `source`, `subnet`, `security_group`, `profile_arn`, `runtime_group` and
`termination_role`. Prepare a request:

```sh
python -m infra.aws_benchmark.durable_operator prepare \
  --identity producer-identity.json --binaries binaries \
  --configuration operator-config.json --output request.json
```

The identity is the original main workflow's full producer identity. Candidate programs are read as
data, never executed by the operator. Review the request and a fresh reservation document. The
separate `arm` command requires `--request`, `--configuration`, `--reservation` and a new `--output`
receipt path. It first claims the shared mailbox, then writes an exclusive lease and control
objects; it is an AWS mutation and must not run before deployment and admission approval. It does
not itself request an instance.

Approve the short durable submission job only after arming. Collect partial or final evidence later
using `aws-benchmark-durable-collect.yml` and the original run ID-attempt. The credentialed job
fetches bounded opaque archives; a separate job parses and qualifies them without cloud credentials.
An in-progress collection is labeled pending, not failed qualification. Cleanup evidence is retained
even if qualification failed. Historical recorded plans remain readable; new qualification requires
the current exact plan. Metadata-only labels remain explicit.

Completed cleanup is terminal and preserves its original verification timestamp. If an early
terminated instance never exposed its root ID, exact attempt-tag discovery may recover that ID;
subsequent deletion is checked by ID. An empty eventually-consistent tag search alone cannot prove
that an unobserved root never existed, so that case retains a manual reconciliation hold.

The first version deliberately has no automatic lease release or recurring opening. Keep both owners
through its reconciliation horizon and delivery drain, confirm all physical workers and roots are
gone, and prove old executions cannot issue further launch calls. Archive requests, attempt records,
cutoffs, execution and cleanup receipts before a separately reviewed manual retirement. Do not
bypass a held lease with another table, namespace or request ID. After physical and producer/drain
reconciliation and saving results, inspect the exact mailbox-owner document and its current ETag. A
separately approved operator retirement may delete only that key conditionally with `IfMatch` for
the inspected ETag. Never unconditionally delete or replace an owner to make arming succeed.
