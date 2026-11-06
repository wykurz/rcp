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

## Bounded publication reader observation

The separate `reader-test` mode in `.github/workflows/aws-benchmark-publish.yml` tests the
publication role. The submission adapters above are not publication acceptance. The workflow
defaults to `publish`, whose original publication/readiness flags and protected writer environment
still apply. `reader-test` is manual, owner-only, main-only, first-attempt-only and requires all
three repository flags (`RCP_AWS_PRODUCTION_ENABLED`, `RCP_AWS_PRODUCTION_PUBLISH_ENABLED` and
`RCP_AWS_HISTORY_TRUST_READY`) to be empty or exactly `false` inside the adapter. A GitHub job
expression is only an early guard; the Python checks reject case variants before requesting a token.
The reader job has no dependency on the publishing jobs and no writer environment or App key.

All live reader executions remain **HOLD**. The current `GitHubPublicationRole` trust and read
statements are `Deny`. A later, separate approval may enable the AWS `PublicationEnabled` reader
gate while keeping `RuntimeEnabled=false`, `AdmissionEnabled=false` and all three repository flags
closed. This adapter changes no trust, IAM policy, template, stack, live flag, schedule or fixture.
Keep the existing native owner/repository/main/workflow conditions intact. Independently capture the
disabled runtime and admission state, the exact role ID and effective policies before approving a
reader run; the adapter does not perform administrative state readbacks.

`python3 -B -m infra.aws_benchmark.acceptance_publication_workflow` accepts no arguments, token
files or operator credentials. The runner supplies the built-in OIDC request capability in memory.
Its nonsecret `reader_approval` input is strict JSON with schema `rcp-publication-read-approval-v2`;
`acceptance_publication.FIELDS` and `CALL_PLAN` are authoritative. Every field is mandatory,
unknown/duplicate fields hold, and the encoded input is limited to 8192 bytes. Bind all of the
following before dispatch:

- The reviewed full `source_commit`, a new 32-hex `approval_nonce`, integer `not_before` and
  `expires_at` with a maximum one-hour window and more than 45 seconds remaining, plus the
  `approval_capture_sha256` of the explicit one-run approval.
- `role_arn` equal to
  `arn:aws:iam::381183037896:role/rcp-production-benchmark-GitHubPublicationRole-34PMVuYgLR5R` and
  immutable `role_id` equal to `AROAVRQCHSHEHGOUK7XQR`.
- `bucket` equal to `rcp-production-benchmark-artifactbucket-augcame1lsq0`, `table` equal to
  `rcp-production-benchmark-LeaseTable-137SHCUGC8HHU`, `table_key_arn` equal to
  `arn:aws:kms:us-east-1:381183037896:key/fda6ab01-08d2-49d9-9866-f75012f06ed6`, and `manifest_key`
  equal to `production/runs/repo-655073755-run-41973977673/collection/manifest.json`.
- `manifest_version` equal to `oXca.tk9xmd6NXHo9uji.RHBM8ru6Bcp`, `manifest_bytes` equal to 3536,
  and `manifest_sha256` equal to `0e56b863e2b66186875a7155d5c9806f545ac8e49f12f0cb40f25148ee8f8895`.
- `negative_key` equal to `production/runs/repo-655073755-run-41973977673/durable-receipt.json`, its
  exact non-null `negative_version`, and `fixture_capture_sha256` of independently reviewed evidence
  that both this exact object version and the table's `policy` item exist. Revalidate fixture
  existence for the approved window; historical existence or a syntactically valid capture hash is
  insufficient. Captures must be nonsecret. This adapter does not create fixtures or read their
  content to prove existence. Approval/capture hashes are audit bindings, not remote attestation.
- `call_plan` equal to the complete fixed ordered list in `acceptance_publication.CALL_PLAN`, with
  no extra operations, attempts, handlers or endpoints.

The nonce correlates the approval with the run; it is **not service-enforced global single use**.
GitHub assigns the run ID after dispatch, so the approval does not contain a guessed future run ID.
Before the one approved manual dispatch, the owner must record durable one-shot intent keyed by
nonce, source, window and call plan. Record the assigned run ID/attempt and reconcile any ambiguous
dispatch before considering a new approval. Do not redispatch the approval or rerun the job. Job
concurrency and attempt-1 checks cannot prevent a second dispatch with the same nonce. No
state-write permission has been added to claim otherwise. The adapter incorporates the assigned run
ID and attempt into the effective binding digest and requires the authenticated token to match that
exact run, source, workflow, owner and repository.

The fixed maximum call sequence is:

| Call                            | Fixed target and result                                                                                     |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| One runner OIDC request         | `sts.amazonaws.com`; no admission audience or second token.                                                 |
| One `AssumeRoleWithWebIdentity` | The exact publication role; 900-second session, fixed inline policy and run-bound session name.             |
| One `GetCallerIdentity`         | Exact account, assumed-role ARN and immutable role ID, matching the STS response.                           |
| One positive `GetItem`          | Only `pk` from `run#repo-655073755-run-41973977673`, consistent read from the exact table; item must exist. |
| One positive `GetObjectVersion` | Exact retained collection manifest version, owner, byte count and SHA256 above.                             |
| One negative `GetItem`          | Only `pk` from `policy` in the same table; require `AccessDeniedException` / HTTP 400.                      |
| One negative `GetObjectVersion` | Exact existing durable receipt version; require `AccessDenied` / HTTP 403.                                  |

STS authenticates the token before its decoded claims become identity evidence. Claims must match
the publication workflow path and display name, main source/workflow SHA, owner/repository IDs,
actor, event, hosted runner, run ID and attempt. The checkout must be clean and exactly match the
approved source. A malformed/expired/mismatched local approval holds before any token request or
service client exists. No local token decoding is presented as independent signature proof.

The inline session policy leaves `dynamodb:GetItem` and `s3:GetObjectVersion` eligible on `*`. The
two negative resource reads therefore remain eligible in this session policy; their observed denials
are not manufactured by an inline resource restriction. The observations cover effective access
under this session, including applicable role/resource/organization policy; they do not identify
which base policy caused a denial. Session policies do not independently grant access.

The table uses an AWS-managed DynamoDB KMS key. A blanket session deny of KMS actions also denies
DynamoDB's delegated decryption, preventing an otherwise eligible `GetItem`. The v2 session adds
only `kms:Decrypt` on the exact `table_key_arn`, with all four exact conditions:

- `kms:ViaService`: `dynamodb.us-east-1.amazonaws.com`.
- `kms:CallerAccount`: `381183037896`.
- `kms:EncryptionContext:aws:dynamodb:tableName`:
  `rcp-production-benchmark-LeaseTable-137SHCUGC8HHU`.
- `kms:EncryptionContext:aws:dynamodb:subscriberId`: `381183037896`.

An explicit deny covers every other key. Four separate `StringNotEqualsIfExists` deny statements
reject each absent or mismatched condition, including direct KMS calls without `kms:ViaService`.
Combining those mismatches in one statement would AND them and leave single-condition failures
without that explicit deny. The compact session policy must remain within STS's 2048-character
plaintext limit; STS packed-policy limits may still reject it and must produce HOLD without retry.
The SDK factory still rejects KMS clients and makes no direct KMS API calls. DynamoDB can make
additional service-side KMS calls, so the fixed client call sequence is not a total service-call
cap. Table encryption context does not authorize particular items; native DynamoDB item restrictions
remain necessary. Table-key caching also means a successful read does not prove a fresh KMS call.

Before any new live approval, independently revalidate the exact table's enabled encryption/key
binding, the AWS-managed key's service/account delegation and effective role policies, alongside the
existing state and fixture checks. Record the readbacks in the nonsecret fixture capture. A changed
key, table, region or account needs reviewed bindings and a fresh approval, not a caller policy
override. v1 approvals are rejected before requesting a token. Successful v2 observations include
the exact key ARN and SHA256 of the serialized session policy; the effective binding digest also
covers the mandatory key binding. Neither these hashes nor offline policy tests are live KMS
authorization evidence. This correction changes no role/key/table policy or encryption setting.

The explicit deny still constrains writes, launches, invocations, secret reads and every other KMS
action. This lane **does not prove base-policy denials for those actions** and never attempts them.
`GetCallerIdentity` requires no grant even under explicit deny, as documented in the AWS reference
linked above. Direct/wrong-context KMS denials also remain unproven live; this adapter must not
broaden its call plan to test them.

References:
[DynamoDB encryption context and delegated decryption](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/encryption.usagenotes.html),
[KMS conditions](https://docs.aws.amazon.com/kms/latest/developerguide/conditions-kms.html),
[negated IfExists in Deny statements](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_IfExists),
and
[STS session policy limits](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html).

These positives cover only the tested `GetItem` and versioned manifest scopes. They do not establish
the role's separate `ListBucket`, current-version `GetObject`, other collection-file access, or
durable publication/Pages behavior. The exact ordered negative calls need independently proven
existing fixtures; missing fixtures, unexpected allows, redirects, transient errors or uncertain
responses produce HOLD. A failed trust exchange is HOLD, not positive negative-role acceptance. The
SDK uses fixed regional HTTPS endpoints, no proxy/config/credential fallback, one attempt and guards
against implicit S3 redirect/`HeadBucket` requests or repeat sends before transport. No anonymous
retry exists.

The process has a 45-second deadline, with a five-minute outer job timeout. JWTs, credentials,
fixture bytes and raw service errors never enter the JSON output; only fixed diagnostic stages,
approved identity/binding hashes and booleans are retained. Unexpected allowed object bodies are
closed without reading them. Closing clients and dropping references does not erase memory or revoke
a minted 15-minute STS session. Preserve the sanitized observation and independent journal even
after timeout/cancellation, and reconcile before any new approval. Every result keeps
`live_accepted: false`.

A genuine wrong-workflow rejection remains a separate live gate. This lane's early local workflow
rejection is not AWS trust evidence, and the native `workflow` condition is a display name rather
than an immutable path. Do not rename a trusted workflow, loosen trust, add a reusable caller or
broaden another workflow's access to fabricate that proof. The same applies to the other signed
native-claim negatives and base write/launch/invoke/secret-policy gates. Existing no-worker,
retirement, qualification, publication and production activation gates remain separate.
