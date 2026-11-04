# Isolated admission execution wrapper

This wrapper tests real production Admission, read_policy and RunStore code against a fresh DynamoDB
fixture table. It does not invoke a production handler, seal inputs, start Step Functions, launch
EC2, read secrets or authenticate to GitHub. Synthetic identity/run records and inert upload URLs
are explicit. Every observation retains live_accepted=false. A passing result is an isolated
admission/accounting observation, not whole-system acceptance.

Production admission/runtime, concurrency and schedules stay off. The existing
[production gates](PRODUCTION.md) and [acceptance procedure](ACCEPTANCE.md) still apply. This is a
separate schema and entrypoint; an older storage transaction canary cannot be relabeled as this
test. All code/package commands below are offline.

## Cases and proof boundaries

admission_acceptance.py constructs the real service with its default RunStore. The fixed phases in
PHASES cover duplicate/rerun handling, four-way contention followed by an isolated serial owner
rejection, exact-cap success, one-micro-dollar/one-run-over rejection, a missing cumulative ledger,
disabled/stale policy, synthetic actor/event rejection, a stale captured policy fence and injected
response loss after an acknowledged fixture commit.

Separate operator seeds keep an existing owner from masking money/day limits. The contention fixture
permits four requests by its daily and money limits. TransactionConflict losers alone do not prove
the owner condition; the following serial request must fail exactly at that condition.

Expected service rejections require ordered TransactionCanceledException CancellationReasons, with
ConditionalCheckFailed at the intended item and None at unrelated items. Permission denial,
validation failure, throttling, missing reasons and unrelated cancellations hold. Offline
ClientError fixtures and SDK input-shape validation do not prove live service emission. Never weaken
the reason check to make a canary pass.

The policy-fence phase deliberately injects an older validated Policy. The response-loss phase
injects an exception after an acknowledged write. Neither reproduces a genuine control-plane or
network failure. No retirement, real GitHub identity, upload or physical-cleanup claim is made.
Known-key transactional snapshots cover nine keys, not a complete table inventory.

The fixture's $68.21 seed, $350/$80 caps and daily limit of four are synthetic data. They neither
change the operational ledger nor approve a monthly allowance. Never copy them into production.

Run offline regressions with:

```sh
python -m unittest discover -s infra/aws_benchmark -t . -p 'test_admission_*.py'
```

## Immutable bindings

admission_probe.validate_bindings is the authoritative schema. All fields are mandatory and unknown
fields fail before client creation:

| Field                   | Required value                                                         |
| ----------------------- | ---------------------------------------------------------------------- |
| schema                  | rcp-isolated-admission-probe-v1                                        |
| account, region         | 381183037896, us-east-1                                                |
| nonce                   | Fresh 12-character lowercase hexadecimal fixture namespace             |
| function_name           | rcp-admission-acceptance- followed by nonce                            |
| function_version        | 1; aliases and $LATEST are rejected                                    |
| role_arn                | Exact dedicated role ARN whose name equals function_name               |
| role_id                 | Actual immutable AROA role ID                                          |
| log_group               | /aws/lambda/ followed by function_name                                 |
| table                   | rcp-production-admission-acceptance- followed by nonce                 |
| table_id                | Actual UUID returned by DescribeTable                                  |
| source_commit           | Full committed source SHA, equal to local HEAD when packaged           |
| approval_capture_sha256 | Digest of the actual bounded setup approval capture                    |
| not_before, expires_at  | Integer UTC epoch seconds, at most two hours apart                     |
| fixture_at              | Canonical UTC timestamp within that window, fixing synthetic case time |

The fixed clock makes seeds and immutable request timestamps reproducible across phases. Actual
approval/deadline checks use wall time, monotonic elapsed time and Lambda's remaining execution
time.

The runtime requires its exact qualified function ARN/version, log group and request UUID. It
verifies the execution role's account, ARN and immutable ID before creating the DynamoDB client. It
verifies TableId/ARN/name, key schema, active state, on-demand billing and deletion protection
before and after the case. TableId is a readback, not a DynamoDB request precondition. Deletion
protection and exclusive operator lifecycle control must prevent replacement throughout testing. An
administrator capable of disabling those controls remains trusted.

Clients use pinned regional AWS endpoints, disabled configured endpoints/proxies and one SDK
attempt. An outer 120-second Lambda timeout is mandatory: socket timeouts and call counts alone do
not terminate blocked threads. The core permits 160 calls and four internal contenders; the gate
permits at most 164 DynamoDB calls including controls. STS identity is one additional service call.

## Durable one-shot claims

The only accepted event has exactly one fixed phase selector, for example:

```json
{"phase": "dedup"}
```

This deliberate phase-only payload allows one immutable function/version to serve all 15 phases. It
differs from the earlier empty-payload sketch. It cannot select another resource, operation, source,
manifest, price, allowance or identity.

Before case work, the probe conditionally puts probe#nonce#phase with the binding digest, fixed seed
digest, source SHA and actual invocation request UUID. attribute_not_exists(pk) must succeed once.
No failed/uncertain claim is adopted, retried, deleted or refunded. A warm or cold duplicate holds.
The function never seeds/resets fixtures or deletes claims.

IAM constrains resources, keys, attributes and enclosing operations; it cannot enforce exact update
expressions. Immutable reviewed code supplies the no-reset behavior. Transactional counter
permission could reset an allowed counter if exercised by different code. Never attach this role to
another function or allow it to reach production resources.

## Offline packaging and disabled proposal

Commit and review all shipped source first. Supply actual non-secret bindings and the existing
hash-pinned offline wheels:

```sh
python -m infra.aws_benchmark.admission_probe_package package \
  --bindings /absolute/path/admission-bindings.json \
  --output /absolute/path/admission.zip --receipt /absolute/path/admission-receipt.json \
  --wheels /absolute/path/pinned-wheels --python312 /absolute/path/python3.12
```

The builder verifies tracked HEAD byte equality, import closure, pinned wheels, native Python 3.12
Linux x86-64 dependencies and rejected networking. It adds the existing acceptance helper closure,
admission_acceptance.py, admission_probe.py and admission-bindings.json. Tests/builders stay out.

After separately approved immutable upload, render offline:

```sh
python -m infra.aws_benchmark.admission_probe_package render \
  --bindings /absolute/path/admission-bindings.json --archive /absolute/path/admission.zip \
  --receipt /absolute/path/admission-receipt.json \
  --wheels /absolute/path/pinned-wheels --python312 /absolute/path/python3.12 \
  --code-bucket EXACT_BUCKET --code-key EXACT_KEY --code-version EXACT_VERSION \
  --template /absolute/path/admission-template.json --plan /absolute/path/admission-plan.json
```

Rendering independently rebuilds from reviewed source/wheels and compares every ZIP byte and
receipt. It never parses or executes the supplied opaque ZIP. Outputs use existing held-descriptor
helpers and exclusive creation; partial evidence is never overwritten. Local receipts are comparison
evidence, not remote attestation.

The template creates only a new Lambda and published version using an already approved fixture
role/log group. Concurrency is zero, memory 256 MiB, timeout 120 seconds and code version pinned. It
creates no role, table, event source, invocation grant, function URL or schedule. Both resources are
retained on rollback/stack deletion. Existing targets or unexpected versions hold. This module has
no upload/deploy/invoke/cleanup command.

## Separately approved live lifecycle

One explicit approval may bundle the fixture stage, derived immutable IDs, up to 15 invocations,
restoration and exact cleanup. Code review/build does not authorize it. Do not ask again merely
because approved creation returned an actual role ID, TableId or package version: verify those
derived bindings independently. Material scope changes still hold.

The packet must cover a fresh encrypted on-demand table with deletion protection, dedicated
role/function/log group, code location, 30-day code/evidence retention, cost/recovery window and
production-disabled preflight.

The fixture role needs only:

- GetItem on exact fixture/claim keys and ConditionCheckItem on policy/active.
- Transactional PutItem on synthetic run/active keys; standalone PutItem only on fixed claim keys.
  Use separate exact LeadingKeys and appropriate EnclosingOperation conditions.
- Transactional UpdateItem on known budget/rate keys with production attribute conditions.
- DescribeTable on the fixture, and log-stream/event writes on its dedicated group.
- Explicit denial of other service actions, including EC2, Step Functions, S3, Secrets Manager,
  IAM/role passing, Lambda invocation and scheduling.

Transactions authorize through underlying GetItem, PutItem, UpdateItem and ConditionCheckItem
actions, not standalone transaction-action grants. Conditioned IAM authorization remains a live
gate.

An approved operator, not the probe, owns complete paginated inventory, seeding, serialization,
concurrency restoration and cleanup:

1. Verify session/window, disabled/idle production, fresh namespace, exact IDs, role trust/policy,
   complete table inventory and function/version/code/timeout/memory/logging/concurrency zero.
2. Preserve every claim. Only after the preceding invocation is terminal and reconciled, reset its
   exact known fixture rows and seed the next phase with fixture_items. Compare full inventory and
   seed digest. Unexpected rows or uncertainty hold.
3. Durably record intent, set probe concurrency one and synchronously invoke qualified version 1
   once with its phase only. No SDK retry or log tail. Never automatically retry an uncertain
   response, timeout, hold, FunctionError or spent claim.
4. Restore and read back concurrency zero in a finally/recovery path on every exit. Retain intent,
   observations, claims and readbacks. Verify source/bindings/role/version and final accounting. The
   rendered plan records these mandatory operator gates; it does not execute them.
5. After terminal reconciliation and evidence capture, disable deletion protection only for approved
   final cleanup. Delete the new table/data, probe, matching stack records, dedicated role and logs.
   Preserve package and sanitized evidence for 30 days. Never touch production ownership, actual
   reservations, keys, benchmark results or earlier code versions.

The earlier $1 planning allowance is not an AWS-enforced ceiling. Recheck price/bounds in the live
packet. Merging this code neither reserves money nor authorizes running it.

## Sources

- [Transaction permissions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html)
- [Transaction isolation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html)
- [Ordered cancellation reasons](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html)
