# Retire a reconciled mailbox owner

This is a separate, reviewed operator recovery action for both protocols. Neither arming command
releases ownership. The owner has no TTL; waiting seven days does not make the mailbox reusable.
Keep the old owner while investigating a failed or interrupted arm. Do not use another bucket,
namespace, table or run ID to evade it. This procedure documents required actions and access; adding
it to the repository applies no AWS permission or mutation.

## Reconcile before deletion

Stop new operator arms and new GitHub submissions using this mailbox for the entire review and
retirement. All operators must respect this maintenance hold. Keep independent cleanup operational
until physical teardown and delivery drain are established. Obtain an explicit review of the saved
reconciliation record containing all of the following:

1. **Exact owner and producer identity.** Read `control/mailbox-owner.json`, save its complete raw
   body and current ETag, and bind its bucket, unique token, protocol, run and request digest to the
   original arm receipt and request. For a partial arm without a receipt, reconstruct the exact
   invocation from retained operator intent and execution records; uncertainty blocks retirement.
   Keep historical owner bytes unchanged. Earlier revisions hashed request JSON without its final
   newline; new owner and submission receipts use the same newline-terminated canonical encoding.
   `request_sha256` hashes that canonical request representation, not the raw S3 object bytes;
   harmless JSON whitespace in the stored request can differ. Reconcile historical digests with
   their original source revision, never rewrite them as new ones.
2. **Producer quiescence.** Identify all build/upload, submit, collect and legacy controller jobs,
   operator processes and their retries that could still write or launch for this owner. Establish
   that they are terminal and cannot resume. Include queued jobs and delayed/in-flight API calls;
   merely cancelling a job or observing no current worker is insufficient. Do not rerun a producer
   after retirement. Finish result collection before removing ownership. If older code or unknown
   producers were active during migration, resolve that uncertainty first.
3. **Protocol cutoff and physical proof.** For a legacy window, retain the original cutoff receipt,
   confirm the cutoff executed, ASG minimum/desired/maximum are all zero, no opening action remains,
   every known/tagged instance is terminated and each temporary root volume is deleted. Wait for all
   scheduled and in-flight delivery to be reconciled; the nominal two-minute scheduled-action delay
   is not a maximum termination bound. Remove obsolete schedules only as a separate reviewed action
   after their effects are known. For a direct request, retain its exact lease, attempts, execution,
   schedules and watchdog/cleanup receipts. Require terminal execution, the unchanged outer deadline
   plus the recorded delivery drain, no live namespace instance and deletion of every known root.
   Prove each launch either allocated the reconciled instance or definitively allocated none. A
   missing Describe result, timeout, elapsed deadline or terminal execution is never proof that an
   ambiguous allocation created nothing.
4. **Saved results and independent review.** Archive raw results, failed/partial evidence, cutoff,
   inventory, volume and execution receipts, plus the funding/retention records. Record who reviewed
   producer quiescence, physical proof and delivery drain. An incomplete run may be retired only
   after its allocation and producer outcomes are resolved; success is not a prerequisite.
5. **Direct lease before mailbox owner.** If the owner is direct, reconcile and retire only that
   exact DynamoDB lease as a separately reviewed conditional `DeleteItem`, matching its inspected
   revision and payload/run. A changed revision, unresolved allocation or producer blocks deletion.
   Read back the exact lease key consistently to confirm absence before retiring the mailbox owner.
   Do not delete the guard heartbeat. Keep both owners on any uncertainty; do not infer permission
   to retire the lease from the mailbox command below.

No command can infer producer quiescence from AWS inventory alone. If any evidence above is absent,
leave the owner in place and report the unresolved condition. There is no automatic takeover path.

## Exact-owner conditional delete

The operator needs `s3:GetObject` and separately approved `s3:DeleteObject` on only this bucket's
`control/mailbox-owner.json`. No runtime role receives DeleteObject. The direct lease operation also
needs separately reviewed DeleteItem on its exact table/key; these are not permission grants to run
unreviewed recovery. Use the pinned operator SDK, whose S3 model supports `IfMatch`.

After the reconciliation review, supply the already-inspected bucket, raw owner file and quoted ETag
to this single-action example. Use an exclusive new receipt path. Do not run it while other
operators can arm or while old producers can resume:

```python
import json
from pathlib import Path

import boto3
from botocore.config import Config

from infra.aws_benchmark.common import require

bucket = "REVIEWED_BUCKET"
expected_etag = '"REVIEWED_ETAG"'
expected_body = Path("reviewed-owner.json").read_bytes()
receipt_path = Path("owner-retirement-intent.json")
require(0 < len(expected_body) <= 4096, "owner body exceeds bounds")
expected = json.loads(expected_body)
require(expected["bucket"] == bucket, "owner bucket differs")
s3 = boto3.client("s3", config=Config(connect_timeout=5, read_timeout=15,
                                     retries={"total_max_attempts": 1}))
observed = s3.get_object(Bucket=bucket, Key="control/mailbox-owner.json",
                         IfMatch=expected_etag)
try:
    require(observed["ETag"] == expected_etag, "owner ETag differs")
    require(observed["ContentLength"] == len(expected_body), "owner length differs")
    require(observed["Body"].read(4097) == expected_body, "owner body differs")
finally:
    observed["Body"].close()
with receipt_path.open("x") as stream:
    json.dump({"bucket": bucket, "owner": expected, "etag": expected_etag,
               "action": "conditional owner retirement", "outcome": "not yet confirmed"}, stream)
response = s3.delete_object(Bucket=bucket, Key="control/mailbox-owner.json",
                            IfMatch=expected_etag)
with receipt_path.with_suffix(".response.json").open("x") as stream:
    json.dump(response, stream, default=str)
```

This deliberately makes one deletion attempt and never launches anything or deletes payload data. A
changed owner returns a precondition failure: stop, retain both records and investigate. A lost
response is uncertain: do not blindly repeat the command. Re-read the exact owner under the same
maintenance hold and reconcile the service response before deciding what happened. Confirm absence
with an authorized read that distinguishes missing from denied access (`ListBucket` constrained to
this exact key may be needed); a 403 is not proof of deletion. Keep the retirement receipt and
release the maintenance hold only after confirming the old owner and, for direct, its lease are
absent. The next run uses a fresh dispatch and claims a new token normally.
