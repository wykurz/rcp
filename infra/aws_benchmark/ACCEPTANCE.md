# Production acceptance adapters

These tools prepare and run bounded observations under separately approved live setup. They do not
authorize a deployment, invoke production handlers, launch workers or set readiness flags. Every
result retains `live_accepted: false`. Review the result together with independently captured
identity, policy and resource state; a locally editable receipt is not remote attestation.

The first supported account/region is `381183037896` / `us-east-1`. The existing production
[acceptance gates](PRODUCTION.md) and [reader/writer trust procedure](TRUST.md) still apply.

The [isolated admission wrapper](ADMISSION-ACCEPTANCE.md) separately exercises real
Admission/RunStore limits and races. It has its own schema, durable phase claims and fixture-only
role; the storage canary below does not establish that integration proof.

## Offline preparation

The modules separate case logic, fixed Lambda bindings, runtime adapters, package construction and
GitHub OIDC observation. Unit tests use injected clients, realistic service responses and local
signing keys. They make no AWS or GitHub service calls. Run them with:

```sh
python -m unittest discover -s infra/aws_benchmark -t . -p 'test_acceptance_*.py'
```

For a real candidate, first commit and review all shipped sources. Construct a non-secret bindings
JSON from actual approved readbacks, then run from that exact clean source:

```sh
python -m infra.aws_benchmark.acceptance_package package \
  --bindings /absolute/path/bindings.json \
  --output /absolute/path/probe.zip \
  --receipt /absolute/path/probe-receipt.json \
  --wheels /absolute/path/pinned-wheels \
  --python312 /absolute/path/python3.12
```

The builder reuses the production package's hash-pinned dependencies and complete first-party
closure. It adds only the fixed case/bindings/runtime modules and embedded
`acceptance-bindings.json`. It requires their committed bytes, exact source SHA and verified Python
3.12 Linux x86-64 imports, native crypto, SDK capabilities and denied networking. Output files must
be new regular files under real, non-symlink parents. A failed build may leave evidence; do not
overwrite it. This command neither uploads code nor creates cloud resources.

Bindings use schema `rcp-production-acceptance-v1`. All fields are mandatory; unknown fields hold.
The validator in `acceptance_bindings.py` is the authoritative schema. Bind the exact account,
region, source commit, role ARN and immutable role ID, new function name/version `1`, existing
approved log group, finite validity window, approval-capture SHA256, case fixtures and proof run.
The approval hash binds an operator capture; it does not verify that capture by itself. Do not
substitute sample IDs, approximate expiry or inferred role identity in a live approval.

| Kind                                   | Required target and evidence                                                                                                                                                                                                     |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `reader-admission`, `reader-collector` | Exact disabled production reader role; current and previous secret versions; separately verified previous stage and unrelated-secret existence; App client/installation IDs; fixed completed GitHub run, attempt 1 and head SHA. |
| `s3`                                   | New isolated versioned bucket `rcp-production-acceptance-<16-hex-nonce>`, matching dedicated role/function/log group and reviewed conditional-write policy.                                                                      |
| `dynamodb`                             | New isolated table with that same name pattern; dedicated role/function/log group; exact policy and zero-total seed described below.                                                                                             |

Reader functions are named `rcp-production-benchmark-f13-admission-probe` and
`rcp-production-benchmark-f13-collector-probe`. They reuse the exact reader execution role to test
effective access. Those roles also have production mutation powers: no-worker code is not an IAM
sandbox. Keep production admission/runtime off, schedules disabled and ownership clear; separately
review the probe bytes and any role-passing grant. The probe does not call production handlers.

## Disabled deployment proposal

After separately approved upload, independently verify the exact code-object version and bytes. Then
render the template and action plan offline:

```sh
python -m infra.aws_benchmark.acceptance_package render \
  --bindings /absolute/path/bindings.json --archive /absolute/path/probe.zip \
  --receipt /absolute/path/probe-receipt.json \
  --wheels /absolute/path/pinned-wheels --python312 /absolute/path/python3.12 \
  --code-bucket EXACT_BUCKET --code-key EXACT_KEY --code-version EXACT_VERSION \
  --template /absolute/path/probe-template.json --plan /absolute/path/probe-plan.json
```

Rendering rebuilds the expected archive from reviewed sources and pinned wheels, using the same
Python/zlib toolchain, and compares every byte and the receipt. It never extracts or executes the
supplied archive. An editable receipt alone is not accepted as source provenance.

This renders only a new Lambda function and published version. Concurrency starts at zero, with
Python 3.12/x86-64, 256 MiB and a 45-second reader or 120-second storage timeout. It creates no IAM
role, secret, fixture, log group, event source, function URL, invocation grant or schedule. The
existing log group and exact role are explicit prerequisites. A name already in use, uncertain
lookup, wrong version or mismatched role/code is a hold; never update an existing probe. Resources
are retained on stack deletion, so rollback cannot silently discard evidence.

The rendered plan distinguishes three separate approvals:

1. Create only the disabled function/version from the reviewed immutable package. Read back all
   runtime, code, role, logging and concurrency settings; version must be exactly 1.
2. Permit one serialized operator to set concurrency to 1, synchronously invoke qualified version 1
   with exactly `{}` and no log tail, then restore/read back concurrency 0 on every exit. The
   operator must durably record intent before invoking. No async invocation or SDK retry is allowed.
3. After reconciling all in-flight/uncertain work, delete only the exact probe resources and any
   separately approved fixture versions/keys. Preserve evidence and package receipts first.

These are plans, not executable cloud controllers. A lost response or outer timeout is uncertain,
not permission to retry. Lambda concurrency 1 and a warm-process replay guard are not exactly-once
execution: cold starts can run again. Keep a durable single-operator intent/receipt and reconcile
cloud state before any new approval. An invocation may continue after client disconnect. Keep the
function timeout, binding expiry and retained fixtures intact during reconciliation.

## What the observations prove

Reader cases positively read the exact `AWSCURRENT` version, use the production App token provider
and terminal-run reader on fixed GitHub endpoints, then require service-specific access denial for
omitted stage, `AWSPREVIOUS`, version-only and unrelated-secret reads. The probe never returns the
PEM, token, raw service exception or authenticated headers. Negative cases need independently
verified existing fixtures: a missing secret/version is `unproven`, not an authorization success. A
read-only installation token still permits Actions logs/artifacts; a stolen App key has broader
installation-administration authority. See TRUST.md before importing any key.

S3 uses fixed small non-secret bytes, create-only writes, version-pinned conditional copy and
version-pinned content readback. It tests the actual pinned SDK's `AnnotationDirective=EXCLUDE`
contract and unconditional-write denial. It retains both object versions. The isolated bucket policy
must be reviewed against the intended production policy; passing an arbitrary permissive clone does
not validate production.

DynamoDB reuses the production six-action admission transaction in an isolated table. Its initial
state must contain only `policy` with version `acceptance-v1` and enabled true, and `budget#total`
with reserved 0. It verifies all six postconditions, direct-write denial, immutable-field
transaction denial and unchanged records after negative operations. Fixed synthetic run/month/day
identifiers are fixture keys, not production records. The probe has no launch capability and never
deletes fixtures. Compare the scoped canary policy with production before claiming effective IAM
coverage.

The Lambda validates an empty event, qualified function identity, validity window and actual STS
role ID before accessing case services. Clients use fixed regional endpoints, no proxy override and
one SDK attempt. Outer Lambda timeout bounds DNS/socket stalls. SDK error text is discarded; keep
independent CloudTrail/operator readbacks for uncertainty diagnosis. Output may be a generic hold
after partial mutation; do not infer that nothing happened.

## OIDC observation and remaining live gates

`python -m infra.aws_benchmark.acceptance_oidc` accepts a bounded ephemeral stdin JSON object with
`config`, `admission_token` and `sts_token`. It requires a pipe, rejects TTY or redirected-file
input and accepts no token path, environment variable or command-line argument. A separately
reviewed trusted workflow must obtain two fresh GitHub OIDC tokens for the exact admission and STS
audiences and pipe them in memory, without shell tracing, files, artifacts or logs. No such workflow
is dispatched by this PR.

The config binds schema `rcp-production-oidc-acceptance-v1`, reviewed source/workflow main ref,
owner/requester/run/attempt, exact role ARN and immutable ID, finite window and approval capture.
The production verifier authenticates the admission token. STS independently authenticates the
second token; decoded claims are compared only after the authenticated STS exchange. The STS session
carries a deny-all session policy and is used only for `GetCallerIdentity`; AWS
[documents that identity lookup needs no permission even with explicit denial](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html).
The
[minimum web-identity session lifetime is 900 seconds](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html).
Credentials are held in memory, never serialized; removing references does not guarantee erasure.
The CLI has a 45-second process deadline; the invoking workflow also needs a bounded job timeout.

A positive main observation is not signed negative-ref or role-policy acceptance. Genuine
untrusted-ref tokens, wrong audience/actor/workflow cases, effective read-only publication-role
denials and actual caller/reusable-workflow claim behavior remain separately approved live gates. Do
not claim an early local branch rejection proved AWS trust enforcement. Do not create test branches,
loosen trust, add secrets or broaden workflow access just to obtain a positive result.

No-worker acceptance also does not prove independent worker shutdown, ownership retirement,
production budgets/rate controls, durable publication or the new AMI. Those retain their separate
disabled-deployment, image, one-run qualification and protected-history/Pages acceptance stages.
