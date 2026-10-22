# Durable benchmark deployment permissions

`durable-stack.yaml` is a disabled proposal for the `rcp-direct-v1` protocol. It reuses the short
coordinator ticks, Standard state machine, lease table and independent watchdog, but replaces ASG
runtime mutations with direct EC2 allocation and termination. It creates no instance, ASG, network,
NAT gateway, worker profile or recurring benchmark. Deployment, activation, a new image, paid
qualification and recurring submissions require their own explicit approval. Code changes grant no
AWS access.

## Service boundaries

| Principal                  | Allowed actions and scope                                                                                                                                                                                                                  | Excluded authority                                                                                                   |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| GitHub submit/collect role | Start this state machine; describe its executions; write six fixed binary objects; read the operator request and three outputs                                                                                                             | No EC2, Scheduler, DynamoDB, IAM, shell, request-manifest or result writes                                           |
| Coordinator Lambda         | RunInstances with pinned resource/type/profile constraints; creation-only instance/volume tags; pass the exact worker role to EC2; read instances, numeric launch template and input binaries; read lease/guard freshness and update lease | No termination, tag changes after creation, guard-heartbeat writes, schedules, IAM changes or launch-template writes |
| Watchdog Lambda            | Independently scan namespace instances and terminate them; read root volumes, lease and worker status; update lease and publish guard freshness; create/read direct-ID runtime schedules; write three fixed control/receipt objects        | No launch, instance/profile/configuration changes, tag changes, cutoff updates/deletion or worker shell              |
| Scheduler termination role | TerminateInstances on instances carrying this deployment's exact namespace tag                                                                                                                                                             | No launch, tags, instance changes, IAM or schedules                                                                  |
| State machine role         | Invoke only the coordinator Lambda                                                                                                                                                                                                         | No direct provisioning or artifact authority                                                                         |

Both Lambda roles can write only their own log streams. EC2 describe actions use `Resource: "*"`
because the API actions do not support resource-scoped authorization. Request code still uses the
pinned account/region and exact identities. The coordinator reads only `lease` and `guard` table
keys and can update only `lease`. The watchdog can read/update `lease` and put only `guard`; the
coordinator cannot forge a healthy guard heartbeat. There is no TTL or runtime DeleteItem: expiry
must not release ownership or erase cleanup evidence.

GitHub trusts only the existing OIDC provider, audience `sts.amazonaws.com`, repository `wykurz/rcp`
and protected environment `aws-benchmark-durable`, with maximum session duration one hour. The
trusted workflow requires its exact reviewed branch before credentials. Builds and untrusted result
parsing remain separate jobs without cloud credentials. Short-lived submission credentials and
operator SSO expiry do not stop the AWS service roles.

## Launch permission and its limits

RunInstances authorization covers every resource involved. The policy allows one exact private AMI,
launch-template ARN, subnet and security group; generated instance, interface and volume IDs are
restricted to this account/region. Instance conditions require `c5d.24xlarge`, On-Demand, default
tenancy, the exact instance profile, required IMDSv2 and hop limit one. Interface creation is tied
to the exact subnet. Volumes must be encrypted 16 GiB gp3. No key pair, fleet, Spot request or
general CreateVolume permission is granted.

The five tags `rcp:namespace`, `rcp:run`, `rcp:attempt`, `rcp:deadline` and `rcp:created-at` are
required at creation on instances and volumes. Only those keys are accepted. CreateTags is allowed
solely when `ec2:CreateAction=RunInstances`; direct later tag edits and DeleteTags are absent. IAM
PassRole is limited to the exact worker **role** ARN and `iam:PassedToService=ec2.amazonaws.com`.
The separate instance-profile ARN is constrained by `ec2:InstanceProfile`; a profile ARN is not a
PassRole target.
[EC2 authorization reference](https://docs.aws.amazon.com/service-authorization/latest/reference/list_ec2.html),
[creation-time tagging](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/supported-iam-actions-tagging.html).

These permissions are more powerful than the previous SetDesiredCapacity-only design. IAM does not
supply documented RunInstances condition keys that enforce `MinCount=MaxCount=1`, numeric launch
version, arbitrary user-data contents, initial termination protection, or a deadline exactly 180
minutes after the attempt. The template must not claim those fields are IAM-enforced. The reviewed
coordinator constructs one exact request, validates the numeric launch template, disables
termination protection, and uses a retained exclusive lease and idempotency token. Ambiguous
allocation outcomes consume the attempt; no replacement token or second allocation is allowed.
Static IAM requires nonempty expiry tags but does not prove their chronology. The independent
janitor validates them, uses EC2 launch chronology and rejects malformed or excessive deadlines.
This limits operational failures; a compromised coordinator role could violate count or initial
launch-field assumptions. Approving deployment must explicitly accept that additional trusted-code
boundary.

The initial image path requires the AWS-managed `alias/aws/ebs` key. Operator preflight and image
receipts must verify the actual snapshot key and authorization for this direct launch principal.
Customer-managed keys are outside this setup. The stack adds no broad KMS permissions or automatic
key-policy changes to make an incompatible image launch; a different key needs a separate precise
permission proposal.
[EBS encryption requirements](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-encryption-requirements.html).

## Independent expiry and shutdown authority

Before any allocation, the namespace janitor must already be active and a successful complete scan
must have published fresh guard evidence. Its expiry scan runs independently of the lease table,
coordinator and GitHub. Instances and volumes are atomically tagged during allocation. Each attempt
has a fixed creation time and expiry no later than 180 minutes afterward; delayed capacity does not
renew an already issued attempt's runtime. The 12-hour request limit permits waiting/retrying only
before an allocation has consumed the lease. There is no ASG replacement path or 15-hour running
fallback in this protocol.

Once an instance ID is known, the watchdog also creates an absolute one-time Scheduler cutoff that
calls `ec2:terminateInstances` for that exact ID. Schedules have no flexible window, retain their
record with `ActionAfterCompletion=NONE`, and use a 300-second event age and at most three retries.
Create conflicts require exact readback; no runtime principal can update/delete a cutoff. The
watchdog passes only the generated termination role to Scheduler. That role trusts this account and
the exact runtime **schedule-group ARN**. Its authorization remains namespace-scoped because the ID
is not known at deployment; exact-ID targeting is enforced by the schedule payload, not its role.
[Scheduler authorization](https://docs.aws.amazon.com/service-authorization/latest/reference/list_scheduler.html),
[Scheduler trust guidance](https://docs.aws.amazon.com/scheduler/latest/UserGuide/cross-service-confused-deputy-prevention.html).

No coordinator, GitHub or worker permission can disable the janitor, change its
function/configuration, modify its role, or edit the cutoff. The stack retains the watchdog
function, execution role, rule, invocation permission, async configuration, log group and lease
table on deletion. It also retains the runtime schedule group and termination role. This preserves
independent cleanup when the coordination stack is deleted; retained monitoring can continue to
incur charges.

Retention is not update protection. An operator/deployer can still disable RuntimeEnabled, replace
code or policies, change tags, or directly remove retained resources. No deployment update,
namespace change, role change, guard deactivation or infrastructure deletion is permitted while an
attempt or unreconciled allocation exists. Do not describe the guard as immutable to administrators.
Before the direct-ID cutoff has been created, persistent loss of the janitor alone can delay
termination; a fresh scan does not guarantee future execution. Once created, the direct-ID cutoff
executes independently of the janitor. Both paths still depend on AWS delivery and EC2 control-plane
availability; no mechanism promises an exact termination time or dollar cap.

## Activation and costs

RuntimeEnabled and AdmissionEnabled default false. Initially both functions have zero concurrency,
the guard rule is disabled and GitHub StartExecution is denied. Activate the guard and validate it
before enabling admission. Disabling admission stops new submissions while the active guard keeps
scanning and the coordinator continues existing executions, including capacity waiting. Admission
does not set the coordinator concurrency to zero. Do not disable RuntimeEnabled until all allocation
outcomes are reconciled, roots are deleted, scheduled-delivery horizons have drained and the
execution is terminal.

A 16-hour supervision allowance covers 960 planned minute ticks per function. At 256 MiB and a
45-second limit each, that is 21,600 GB-seconds combined; using the verified public us-east-1 x86
rate of $0.0000166667/GB-second plus requests gives approximately $0.3604. Budgeting $0.50 covers
full concurrency saturation over the interval conservatively. Roughly 2,890 Standard transitions
cost $0.07225. Add table operations, scheduled delivery, logs, artifacts and storage. The new guard
heartbeat writes must be included in table estimates. No free-tier allowance is assumed.
[Lambda pricing](https://aws.amazon.com/lambda/pricing/),
[Step Functions pricing](https://aws.amazon.com/step-functions/pricing/),
[DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/).

At the historical worker rate of $4.608/hour, 180 minutes plus a 16-minute shutdown allowance costs
$15.0528 for compute. The allowance conservatively sums one minute of Scheduler precision, five
minutes of retry age and ten minutes of termination handling; the first two may overlap and the last
is an operational assumption, not an AWS guarantee. The previous reviewed itemized estimate,
including 16-hour supervision, one 72-minute builder, one full 16 GiB snapshot for 30 days,
EBS/IPv4, logs and artifacts, was approximately $17.23 with an $18 proposed allocation. Reprice and
update the request/write counts for this protocol before admitting paid work. Existing reservations
are $62 of the $80 cap, including the once-allocated $18 preparation/qualification allowance; this
code allocates nothing. Existing retained images are already covered by the prior ledger and must
not be counted twice or extended implicitly.

The 16-hour supervision allowance does not stop the rule automatically. After verified cleanup,
operator deactivation is required; leaving it enabled creates ongoing serverless costs. Logs expire
after seven days. Lease records, schedules and retained infrastructure require deliberate retirement
only after reconciliation. The artifact lifecycle expires input/output payloads and the two fixed
control documents after seven days, but never expires `control/mailbox-owner.json`. Both trusted
operators conditionally create that same owner before any schedule, lease or control publication.
Runtime roles cannot write or delete it. A partial failure retains ownership; no TTL or automatic
takeover is allowed.

## Concrete deployment review

Review an exact change set and role policies before approval. Supply account/region, unique
namespace, existing artifact bucket, numeric launch-template version, AMI/source revision, subnet,
security group, worker instance-profile ARN and its contained role ARN, and the existing GitHub OIDC
provider. Use an immutable S3 package version. Preserve a receipt linking the reviewed full
controller SHA to package SHA-256, S3 object version and both deployed Lambda CodeSha256 values.
Runtime worker pins do not independently prove that package receipt. These identifiers are
non-secret; never supply secret keys in chat.

The deployer needs permissions for five new roles and inline policies, two functions, log groups,
the retained lease table, one Scheduler group, state machine, disabled EventBridge rule and its
Lambda invocation permission. PassRole must target only the corresponding execution roles. The new
GitHub OIDC role/environment, coordinator launch privilege and worker-role passing, namespace
termination authority, Scheduler role passing and retained janitor resources are explicit access
changes requiring approval. Do not grant a wildcard administrative deployment policy. No worker role
or existing OIDC-provider edit is included. The existing artifact bucket needs the separately
reviewed owner-preserving lifecycle update; deployment must start from a quiescent mailbox because
old-code producers do not participate in the new reservation.

The operator separately validates the exact launch template and managed-key image, network and
instance profile, active guard deployment, fresh scan, exclusive namespace and funded reservation.
It needs `ec2:DescribeInstances`, `ec2:DescribeLaunchTemplateVersions`, `ec2:DescribeImages`,
`ec2:DescribeImageAttribute`, `ec2:DescribeSnapshots`, `ec2:DescribeSubnets` and
`ec2:DescribeSecurityGroups`; `kms:DescribeKey` for the account's AWS-managed EBS key;
`ssm:GetServiceSetting` to verify Default Host Management Configuration remains disabled;
`dynamodb:GetItem` for `lease` and `guard`, conditional `dynamodb:PutItem` for `lease`, and
`s3:PutObject` for `control/durable-request.json`, `output/status.json` and the fixed
`control/mailbox-owner.json`, with conditional creation of the owner; `s3:GetObject` for the owner
and existing worker window; bucket `s3:ListBucket` constrained by `s3:prefix` to
`control/window.json` so a missing prior window is distinguishable from denied access; and
bucket-scoped `s3:GetLifecycleConfiguration` to verify that owner expiration is disabled. Legacy
arming uses the same operator-owned reservation without adding DynamoDB permissions to the legacy
runtime. Explicit later retirement needs `s3:DeleteObject` only on the owner key with the inspected
`IfMatch` ETag, after physical/producer/drain/result checks; neither arm command deletes an owner.
The shared [retirement procedure](MAILBOX_RETIREMENT.md) requires producer quiescence and retained
physical/drain/result evidence; an expired timestamp is insufficient. To confirm owner absence,
operator `s3:ListBucket` must also allow the exact owner key, never an unrestricted listing. Direct
lease retirement needs separately scoped `dynamodb:DeleteItem` on the exact table/key, conditioned
on the inspected lease revision and payload/run. This privilege belongs only to the separately
reviewed operator recovery action, never the coordinator, watchdog or GitHub roles. These operator
actions and the artifact lifecycle update belong in the concrete deployment review. It does not open
an ASG or create an outer cutoff in this protocol. The existing ASG must stay disabled. No
unattended benchmark cron is enabled by this stack.

Offline tests check positive/negative policy cases and CloudFormation schema; they are not AWS
policy simulation. Deployment validation must verify actual role/key policies, creation tagging,
exact network/profile/template behavior, termination authority, observed launch outcomes, physical
root deletion and the funded operating envelope. A timeout alone never releases ownership.
