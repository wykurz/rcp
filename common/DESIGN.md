# Common Component Design

`common` owns filesystem operations and metadata policy shared by the tools. This page covers the
POSIX ACL paths in [`safedir.rs`](src/safedir.rs); see the [ACL user guide](../docs/acls.md) for
flags, permission hazards, warnings, and limitations.

## POSIX ACLs

Read and write opaque kernel ACL blobs through held descriptors. A successful capture with no ACL
means **clear** that attribute, not leave it unchanged. A failed read is an error, not an empty
capture. Read probes list attributes first and fetch only names present; ACL reads and writes use
the metadata gates except the best-effort rollback from the lockdown guard. The remote
[protocol reference](../docs/remote_protocol.md#25-entry-metadata-and-posix-acls) owns wire
representation and failure accounting.

### Application order

Both kinds start restricted (files `0o600`, fresh directories `0o700`). With an access ACL, the
preparatory `fchmod` sets only the requested special bits alongside the owner-only rwx mode; the ACL
then sets the final rwx bits. Reversing those writes would rewrite the ACL's mask. Without a source
access ACL, clear any inherited access ACL when preservation is requested and use the final `fchmod`
to set the mode. A directory's default ACL is mode-neutral.

```text
File, after payload       requested owner/time → permission step (last fallible step)
Directory, after children   requested owner    → permission step → requested timestamps

Permission step:
  source access ACL      restricted chmod → default ACL* → install access ACL
  no source access ACL   default ACL* → clear access ACL* → final chmod
  * only when preserving ACLs; default ACL is directory-only
```

`set_file_metadata_owned` and `set_dir_metadata_fd_inner` implement these paths. The ordinary
directory applier applies or clears the default ACL before the access ACL; a locked reused directory
handles the default ACL separately through its guard, before ownership changes. Only the file
applier promises that widening is its last fallible step. Directory timestamps are post-order so
child creation cannot invalidate them. If a directory's ACL apply fails after restricted chmod, its
mode can remain owner-only plus special bits.

### Containment and rollback

Strict mode breaks the destination's inheritance chain at three sites:

| Destination                   | Site                        | Rule                                                                                                                                                                                                               |
| ----------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Fresh directory               | `Dir::make_dir_with_credit` | Create, reopen, and strip both ACLs in one blocking closure. If it starts, cancellation cannot stop it between creation and sanitation. On failure, remove the empty slot best effort.                             |
| Reused directory              | `lockdown_reused_dir_with`  | Secure the directory, snapshot its default ACL, arm the guard, then strip the default ACL before children are created. The access ACL is not stripped; the restricted mode makes named access entries ineffective. |
| File under unsanitized parent | `Dir::create_file_and_then` | Remove inherited access ACL on the new fd in the creation closure; unlink the slot best effort if sanitation fails. Files under sanitized parents normally skip this work.                                         |

The guarded default ACL is resolved before ownership changes in `set_reused_dir_metadata_fd`:
install the source default ACL when preserving, otherwise restore the original. If the chosen ACL is
absent, no write is needed because lockdown left it absent. Removing a nonexistent or unsupported
ACL succeeds; failing to set an ACL that is required fails the operation.

Lockdown strips inline after acquiring the metadata gate: a detached strip could otherwise land
after rollback. Finalize writes use `ReusedDirLock::write_default_acl_guarded`, serialized against
`Drop` rollback. The guard stays armed through final verification and cancellation points, even when
the original ACL was absent; rollback then removes a partially installed default ACL. A write that
loses to rollback skips the syscall. On rollback the parent is marked as potentially inheriting
before restoration, so a concurrently created file still sanitizes itself if needed.

Rollback is best effort, not a transaction over directory metadata. With no guarded write in flight
it tries restoration inline; if the write holds the guard mutex, it defers the same serialized
restore to a thread so cancellation need not wait for storage. Restoration can fail or be abandoned
at process exit. See the [TOCTOU contract](../docs/tocttou.md#scope-of-toctou-safety) for the full
scope and limitations.
