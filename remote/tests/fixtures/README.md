# Remote wire fixtures

`wire-v12.txt` records named, fixed bitcode payloads (hexadecimal, one message per line) for
`remote::version::WIRE_REVISION = 12`. `wire_compatibility.rs` selects the fixture set by revision;
an unrecognized revision fails even if its bytes have not changed. The tests encode deterministic
values to those bytes and separately decode the stored bytes to named field values, compared through
Serde JSON values. They never regenerate fixtures while testing.

Run from the repository root, inside `nix develop` when needed:

```sh
just test -p rcp-tools-remote --test wire_compatibility
```

The samples cover both master hello variants, source readiness, tracing/control hello roles, success
and failure results, directory limits, every source/destination control variant, file headers, and
tracing log/progress messages. Nested samples include populated copy/removal summaries, all nonzero
runtime statistics, distinct metadata fields, unknown/captured/absent ACLs, filters, preserve
settings, all dry-run modes, optional fingerprints, IPv4/IPv6, and UTF-8 paths. ACL blobs are opaque
byte samples; no kernel ACL operation is exercised. Fixtures use explicit values rather than runtime
statistics, filesystem metadata, clocks, or production defaults.

## Maintaining compatibility

When a fixture fails, identify the wire-visible change, including changes in nested common types or
the serializer dependency. Do not simply replace the expected bytes. Apply the compatibility policy
at `remote/src/version.rs`: an incompatible change must change the crate version or `WIRE_REVISION`
so same-version stale daemons are rejected. For a revision bump, rename the fixture file to that
revision, update the explicit selection in the test, and review each changed payload and test value
together. If only the crate version changes, explain that compatibility boundary in the PR. If a
revision changes solely for the spawn contract, the payloads may stay identical, but still move the
fixture set to the new revision. There is no requirement to keep fixtures for unsupported revisions.

Use the encoding assertion's actual hex output to inspect a changed payload. Review additions to the
test corpus separately from changes to existing layouts. Keep case names unique; the harness rejects
duplicate, missing, and unused fixture entries. Submit intentional fixture changes with the
compatibility decision and rerun both encode and decode tests.

This is a sampled serialization guard, not a complete schema or compatibility proof. A developer can
update both expectations and implementation incorrectly. Unexercised values or new variants can
escape it; add samples when extending messages. Serde JSON comparison checks serialized field
values, not every internal invariant of custom deserializers. Payload fixtures do not cover length
framing, raw file bytes, message ordering/meaning, command-line spawn arguments, readiness text,
TLS, or the version-probe JSON contract. Keep the stream, protocol-state, version, spawn, and remote
integration tests; those contracts still require compatibility review even when these fixtures pass.
