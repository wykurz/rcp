//! Tests that ARM the process-global strict operand resolution switch
//! (`--require-toctou-safe`).
//!
//! These live in their own integration-test binary — not in the lib's unit-test
//! mod — because the switch is deliberately one-way: once armed it stays armed
//! for the life of the process. Under cargo-nextest every test is its own
//! process, but the plain `cargo test` harness (used by the nix checkPhase)
//! runs a binary's tests as threads of ONE shared process, where arming would
//! leak into unrelated lib tests (e.g. the symlink-following `open_parent_dir`
//! test, which must observe default behavior). A separate integration binary
//! gives these tests their own process under both runners; within this binary
//! every test either arms the switch itself or accepts an already-armed state.

use common::toctou_check::{LinterAction, run_linter};

/// Once strict operand resolution is armed, the two operand opens refuse to
/// resolve through a symlink anywhere in the path (ELOOP), while symlink-free
/// paths still open normally.
#[tokio::test]
async fn strict_resolution_rejects_symlinked_prefix() -> anyhow::Result<()> {
    if !common::safedir::openat2_available() {
        // on pre-5.6 kernels the linter refuses --require-toctou-safe outright, so
        // strict opens can never be reached in production; nothing to test here
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    // canonicalize: TMPDIR itself may contain symlinked components (e.g. under
    // nix-shell), which strict resolution would — correctly — refuse.
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    tokio::fs::create_dir_all(tmp.join("real/sub")).await?;
    tokio::fs::write(tmp.join("real/a.txt"), b"x").await?;
    tokio::fs::symlink(tmp.join("real"), tmp.join("link")).await?;

    common::safedir::enable_strict_operand_resolution();

    // a symlink component anywhere in the operand path fails closed with ELOOP
    let err =
        common::safedir::Dir::open_root_dir(&tmp.join("link/sub"), false, common::Side::Source)
            .await
            .expect_err("strict resolution must refuse a symlinked prefix component");
    assert_eq!(
        err.raw_os_error(),
        Some(libc::ELOOP),
        "expected ELOOP, got: {err:?}"
    );
    let err = common::safedir::Dir::open_parent_dir(&tmp.join("link"), common::Side::Source)
        .await
        .expect_err("strict resolution must refuse a symlinked parent operand");
    assert_eq!(
        err.raw_os_error(),
        Some(libc::ELOOP),
        "expected ELOOP, got: {err:?}"
    );

    // symlink-free operand paths still open and stay fully functional
    let root =
        common::safedir::Dir::open_root_dir(&tmp.join("real"), false, common::Side::Source).await?;
    let (_file, _meta) = root.open_file_read(std::ffi::OsStr::new("a.txt")).await?;
    let parent = common::safedir::Dir::open_parent_dir(&tmp.join("real"), common::Side::Source)
        .await?
        .into_tree();
    parent.open_dir(std::ffi::OsStr::new("sub")).await?;
    Ok(())
}

/// `strict_probe_dst_kind` decomposes the path so an INTERMEDIATE-prefix symlink
/// fails closed (ELOOP), while a final component that is merely a symlink is
/// reported as existing (`Some(Symlink)`, not followed) and a genuinely absent
/// entry is `Ok(None)` — never conflated.
#[tokio::test]
async fn strict_probe_separates_intermediate_from_final() -> anyhow::Result<()> {
    use common::safedir::strict_probe_dst_kind;
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    tokio::fs::create_dir_all(tmp.join("real/dir")).await?;
    tokio::fs::write(tmp.join("real/file.txt"), b"x").await?;
    tokio::fs::symlink(tmp.join("real"), tmp.join("prefixlink")).await?; // intermediate symlink
    tokio::fs::symlink(tmp.join("real/dir"), tmp.join("real/finallink")).await?; // final symlink

    common::safedir::enable_strict_operand_resolution();

    // intermediate-prefix symlink → ELOOP (fail closed)
    let err = strict_probe_dst_kind(&tmp.join("prefixlink/file.txt"), common::Side::Destination)
        .await
        .expect_err("intermediate-prefix symlink must fail closed");
    assert_eq!(err.raw_os_error(), Some(libc::ELOOP), "got: {err:?}");

    // a real final file classifies as a file
    assert_eq!(
        strict_probe_dst_kind(&tmp.join("real/file.txt"), common::Side::Destination).await?,
        Some(common::walk::EntryKind::File)
    );

    // a FINAL-component symlink: exists (not followed), classified Symlink — NOT ELOOP
    assert_eq!(
        strict_probe_dst_kind(&tmp.join("real/finallink"), common::Side::Destination).await?,
        Some(common::walk::EntryKind::Symlink)
    );

    // a genuinely absent entry (real parent) → Ok(None), not an error
    assert_eq!(
        strict_probe_dst_kind(&tmp.join("real/absent"), common::Side::Destination).await?,
        None
    );
    Ok(())
}

/// The linter arms strict operand resolution when `--require-toctou-safe`
/// proceeds with well-formed operands.
#[test]
fn require_mode_arms_strict_resolution_on_proceed() {
    // under nextest this test owns its process, so the switch starts unarmed and
    // this proves the off→on transition; under a shared-process `cargo test` a
    // sibling test in this binary may have armed it already, which the one-way
    // switch makes indistinguishable from proceeding — so no precondition assert.
    let operands = vec![
        std::path::PathBuf::from("/ok/src"),
        std::path::PathBuf::from("/ok/dst"),
    ];
    match run_linter(false, false, true, &operands) {
        LinterAction::Proceed => {
            assert!(
                common::safedir::strict_operand_resolution(),
                "linter Proceed in require mode must arm strict operand resolution"
            );
        }
        LinterAction::Exit { output, code } => {
            // on kernels without openat2 the refusal is the correct outcome
            assert!(
                !common::safedir::openat2_available(),
                "good operands must proceed on openat2-capable kernels, got: {output}"
            );
            assert_eq!(code, 1);
            assert!(output.contains("openat2"), "got: {output}");
        }
    }
}

/// rcpd passes no operands (they arrive via the master, already validated);
/// an empty operand list must proceed — and still arm strict resolution.
#[test]
fn require_mode_with_no_operands_proceeds() {
    match run_linter(false, false, true, &[]) {
        LinterAction::Proceed => {
            assert!(
                common::safedir::strict_operand_resolution(),
                "linter Proceed in require mode must arm strict operand resolution"
            );
        }
        LinterAction::Exit { output, code } => {
            // on kernels without openat2 the refusal is the correct outcome
            assert!(
                !common::safedir::openat2_available(),
                "empty operand list must proceed on openat2-capable kernels, got: {output}"
            );
            assert_eq!(code, 1);
            assert!(output.contains("openat2"), "got: {output}");
        }
    }
}

// ── Reused-destination-directory lockdown (--require-toctou-safe) ─────────────
//
// Under strict operand resolution a REUSED destination directory is taken over by
// the copier and restricted to 0o700 for the copy's duration, then restored to its
// original owner and the source mode at finalize (see
// `common::safedir::lockdown_reused_dir`). These arm the strict switch and drive a
// real copy/link into a directory this (non-root) user owns — the cases exercisable
// without multiple uids; the foreign-owner restore is covered by the `sudo_`-gated
// tests in the rcp/rlink integration suites.

use std::os::unix::fs::{MetadataExt, PermissionsExt};

fn overwrite_copy_settings() -> common::copy::Settings {
    common::copy::Settings {
        dereference: false,
        fail_early: true,
        overwrite: true,
        overwrite_compare: Default::default(),
        overwrite_filter: None,
        ignore_existing: false,
        chunk_size: 0,
        skip_specials: false,
        remote_copy_buffer_size: 0,
        filter: None,
        dry_run: None,
        delete: None,
    }
}

/// The current process's effective uid/gid (the copier's identity), read via libc
/// as the rest of this binary already does for raw errnos.
fn effective_owner() -> (u32, u32) {
    // SAFETY: geteuid/getegid are always-successful, argument-free syscalls.
    unsafe { (libc::geteuid(), libc::getegid()) }
}

/// Strict reuse of an owned `0o500` directory makes it writable for the copy and
/// leaves it at the SOURCE mode with the owner unchanged — the write-into-readonly
/// regression guard, and proof the interim `0o700` state is not left behind.
#[tokio::test]
async fn strict_reuse_owned_readonly_dir_becomes_writable_final_mode_source() -> anyhow::Result<()>
{
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    // source directory (distinctive mode 0o755) with one child to write into the reused dir
    let src = tmp.join("src");
    tokio::fs::create_dir(&src).await?;
    tokio::fs::write(src.join("a.txt"), b"payload").await?;
    tokio::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o755)).await?;
    // reused destination directory: exists, owned by us, read-only (un-writable as-is)
    let dst = tmp.join("dst");
    tokio::fs::create_dir(&dst).await?;
    tokio::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o500)).await?;
    common::safedir::enable_strict_operand_resolution();
    let result = common::copy::copy(
        common::get_progress(),
        &src,
        &dst,
        &overwrite_copy_settings(),
        &common::preserve::preserve_none(),
        false,
    )
    .await;
    if let Err(e) = result {
        panic!("strict reuse copy must succeed, got: {:#}", e.source);
    }
    // the child was written despite the 0o500 start → the lockdown made the dir writable
    assert!(
        dst.join("a.txt").exists(),
        "child must be copied into the locked-then-restored directory"
    );
    let md = std::fs::symlink_metadata(&dst)?;
    // final mode is the source's masked mode (0o755), not the interim 0o700 or original 0o500
    assert_eq!(
        md.permissions().mode() & 0o777,
        0o755,
        "final mode must equal the source directory mode"
    );
    // owner restored to the original (== us): unchanged from before the copy
    let (euid, egid) = effective_owner();
    assert_eq!(md.uid(), euid, "owner uid must be unchanged");
    assert_eq!(md.gid(), egid, "owner gid must be unchanged");
    Ok(())
}

/// `preserve_none` reuse of an owned `0o777` directory ends with the owner
/// UNCHANGED — the v1-blocker guard (a naive "chown gated on preserve" would leave
/// the copier as the permanent owner under the default `preserve_none`).
#[tokio::test]
async fn strict_reuse_preserve_none_owner_unchanged() -> anyhow::Result<()> {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let src = tmp.join("src");
    tokio::fs::create_dir(&src).await?;
    tokio::fs::write(src.join("a.txt"), b"payload").await?;
    tokio::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o755)).await?;
    // reused destination directory: exists, owned by us, world-open (0o777)
    let dst = tmp.join("dst");
    tokio::fs::create_dir(&dst).await?;
    tokio::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o777)).await?;
    let (euid_before, egid_before) = effective_owner();
    common::safedir::enable_strict_operand_resolution();
    let result = common::copy::copy(
        common::get_progress(),
        &src,
        &dst,
        &overwrite_copy_settings(),
        &common::preserve::preserve_none(),
        false,
    )
    .await;
    if let Err(e) = result {
        panic!("strict reuse copy must succeed, got: {:#}", e.source);
    }
    let md = std::fs::symlink_metadata(&dst)?;
    assert_eq!(
        md.uid(),
        euid_before,
        "preserve_none must leave the reused directory's owner uid unchanged"
    );
    assert_eq!(
        md.gid(),
        egid_before,
        "preserve_none must leave the reused directory's owner gid unchanged"
    );
    Ok(())
}

/// rlink mirror: strict reuse of an owned `0o500` directory is made writable for the
/// hard-link pass and restored to the source mode with the owner unchanged. rlink has
/// its OWN finalize (`link_dir_contents`), a distinct restore site from copy's.
#[tokio::test]
async fn strict_reuse_rlink_owned_readonly_dir_becomes_writable() -> anyhow::Result<()> {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let src = tmp.join("src");
    tokio::fs::create_dir(&src).await?;
    tokio::fs::write(src.join("a.txt"), b"payload").await?;
    tokio::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o755)).await?;
    let dst = tmp.join("dst");
    tokio::fs::create_dir(&dst).await?;
    tokio::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o500)).await?;
    let link_settings = common::link::Settings {
        copy_settings: overwrite_copy_settings(),
        update_compare: Default::default(),
        update_exclusive: false,
        filter: None,
        dry_run: None,
        preserve: common::preserve::preserve_none(),
    };
    common::safedir::enable_strict_operand_resolution();
    let result = common::link::link(
        common::get_progress(),
        &tmp,
        &src,
        &dst,
        &None,
        &link_settings,
        false,
    )
    .await;
    if let Err(e) = result {
        panic!("strict reuse rlink must succeed, got: {:#}", e.source);
    }
    let linked = dst.join("a.txt");
    assert!(
        linked.exists(),
        "child must be hard-linked into the locked-then-restored directory"
    );
    // the hard link shares the source inode (rlink, not copy)
    assert_eq!(
        std::fs::symlink_metadata(&linked)?.ino(),
        std::fs::symlink_metadata(src.join("a.txt"))?.ino(),
        "rlink must hard-link the source file"
    );
    let md = std::fs::symlink_metadata(&dst)?;
    assert_eq!(
        md.permissions().mode() & 0o777,
        0o755,
        "final mode must equal the source directory mode"
    );
    let (euid, egid) = effective_owner();
    assert_eq!(md.uid(), euid, "owner uid must be unchanged");
    assert_eq!(md.gid(), egid, "owner gid must be unchanged");
    Ok(())
}

// ── ACL containment for a reused directory (`--require-toctou-safe`) ─────────
//
// The lockdown snapshots and strips a reused destination directory's access AND
// default ACLs, so nothing written during the copy inherits them, and puts them
// back at finalize. rlink reaches that finalize through `link_dir_contents`,
// which is a DIFFERENT restore site from copy's `finalize_dir` and the remote
// destination's `complete_directory_single` — so it needs its own test rather
// than inheriting confidence from theirs.

const ACL_ACCESS: &std::ffi::CStr = c"system.posix_acl_access";
const ACL_DEFAULT: &std::ffi::CStr = c"system.posix_acl_default";

/// Encode an ACL in the kernel's `system.posix_acl_*` layout: a `__le32` version
/// followed by `{__le16 tag, __le16 perm, __le32 id}` entries. Written directly
/// because `setfacl` is not in the dev shell.
fn encode_acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut out = 2u32.to_le_bytes().to_vec();
    for &(tag, perm, id) in entries {
        out.extend_from_slice(&tag.to_le_bytes());
        out.extend_from_slice(&perm.to_le_bytes());
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

/// `u::rwx u:65534:rwx g::r-x m::rwx o::r-x` — a permissive entry, as an
/// administrator's default ACL on a destination tree.
fn granting_acl() -> Vec<u8> {
    encode_acl(&[
        (0x01, 7, 0xffff_ffff),
        (0x02, 7, 65534),
        (0x04, 5, 0xffff_ffff),
        (0x10, 7, 0xffff_ffff),
        (0x20, 5, 0xffff_ffff),
    ])
}

fn set_acl(path: &std::path::Path, name: &std::ffi::CStr, value: &[u8]) {
    use std::os::unix::ffi::OsStrExt as _;
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: both pointers are NUL-terminated C strings that outlive the call, and `value` points
    // at `value.len()` readable bytes.
    let rc = unsafe {
        libc::setxattr(
            cpath.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    };
    assert_eq!(
        rc,
        0,
        "cannot write {name:?} on {path:?}: {}. This test needs a filesystem that holds POSIX \
         ACLs; it fails rather than skips.",
        std::io::Error::last_os_error()
    );
}

/// Read `name` from `path`, or `None` if the entry genuinely has none. Only `ENODATA` yields
/// `None`: a getter that shrugged at a wrong path would let every assertion below pass vacuously.
fn get_acl(path: &std::path::Path, name: &std::ffi::CStr) -> Option<Vec<u8>> {
    use std::os::unix::ffi::OsStrExt as _;
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut buf = [0u8; 1024];
    // SAFETY: as above; the kernel writes at most `buf.len()` bytes into `buf`.
    let n = unsafe {
        libc::getxattr(
            cpath.as_ptr(),
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            buf.len(),
        )
    };
    if n >= 0 {
        return Some(buf[..n as usize].to_vec());
    }
    let err = std::io::Error::last_os_error();
    assert_eq!(
        err.raw_os_error(),
        Some(libc::ENODATA),
        "getxattr({name:?}) on {path:?} failed with {err} — only ENODATA means \"no such attribute\""
    );
    None
}

/// rlink's restore site: strict reuse of a directory carrying a default ACL neither lets the
/// hard links it materializes inherit it nor loses it — with `d:acl` off, so what comes back is
/// the DESTINATION's own ACL rather than anything from the source.
#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
#[tokio::test]
async fn strict_reuse_rlink_restores_a_reused_dirs_acls() -> anyhow::Result<()> {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let src = tmp.join("src");
    tokio::fs::create_dir(&src).await?;
    tokio::fs::write(src.join("a.txt"), b"payload").await?;
    tokio::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o755)).await?;
    // reused destination directory carrying a permissive DEFAULT ACL, which `chmod` alone cannot
    // contain: it is untouched by a mode change, so every child created during the copy would
    // inherit it.
    let dst = tmp.join("dst");
    tokio::fs::create_dir(&dst).await?;
    let default = granting_acl();
    set_acl(&dst, ACL_DEFAULT, &default);
    let link_settings = common::link::Settings {
        copy_settings: overwrite_copy_settings(),
        update_compare: Default::default(),
        update_exclusive: false,
        filter: None,
        dry_run: None,
        preserve: common::preserve::preserve_none(),
    };
    common::safedir::enable_strict_operand_resolution();
    let result = common::link::link(
        common::get_progress(),
        &tmp,
        &src,
        &dst,
        &None,
        &link_settings,
        false,
    )
    .await;
    if let Err(e) = result {
        panic!("strict reuse rlink must succeed, got: {:#}", e.source);
    }
    let linked = dst.join("a.txt");
    assert_eq!(
        get_acl(&linked, ACL_ACCESS),
        None,
        "the hard link inherited the reused directory's default ACL — and because it SHARES the \
         source's inode, that ACL is on the source tree too"
    );
    assert_eq!(
        get_acl(&dst, ACL_DEFAULT).as_deref(),
        Some(default.as_slice()),
        "the reused directory permanently lost the default ACL it had before the link run"
    );
    assert_eq!(
        std::fs::symlink_metadata(&dst)?.permissions().mode() & 0o777,
        0o755,
        "final mode must equal the source directory mode"
    );
    Ok(())
}

/// Cancelling `lockdown_reused_dir` at ANY point must never cost the directory its default ACL.
///
/// The lockdown removes that ACL from the filesystem and holds the only copy in memory, so there is
/// a window in which the bytes exist nowhere durable. If the guard that owns them is constructed
/// AFTER the removal, a task cancelled in between loses them as a bare `Option<Vec<u8>>` with no
/// destructor — and the removal still happens, because `spawn_blocking` cannot be cancelled once
/// submitted and a dropped `JoinHandle` detaches rather than cancels. `--fail-early` reaches exactly
/// this: a sibling fails, `join_and_fold` drops the `JoinSet`, and every in-flight lockdown is
/// aborted mid-await.
///
/// Cancellation is driven by POLL COUNT rather than elapsed time, which matters: the window is a
/// single `spawn_blocking` round trip, tens of microseconds inside a call that takes a few hundred,
/// and a timer-based sweep walks straight past it (measured — it does not catch the bug). Each
/// `.await` on the blocking pool yields exactly one `Pending`, so dropping the future after exactly
/// N polls lands the cancellation on the Nth await, deterministically, and sweeping N covers every
/// one of them including the strip.
///
/// Every iteration must end with the ACL present, whichever of the three outcomes it lands in: the
/// lockdown never got that far, it completed and the guard was dropped, or it was cancelled between
/// removing the ACL and completing lockdown while the guard remained armed.
#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lockdown_reused_dir_never_loses_the_default_acl_when_cancelled() -> anyhow::Result<()> {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    common::safedir::enable_strict_operand_resolution();
    let default = granting_acl();
    let mut completed = 0usize;
    for budget in 0..24usize {
        let name = format!("reused{budget}");
        let dir_path = tmp.join(&name);
        tokio::fs::create_dir(&dir_path).await?;
        set_acl(&dir_path, ACL_DEFAULT, &default);
        let root =
            common::safedir::Dir::open_root_dir(&tmp, false, common::Side::Destination).await?;
        let entry = std::ffi::OsString::from(&name);
        let handle = root.child(&entry).await?;
        let dir = root.open_dir(&entry).await?;
        // poll the lockdown at most `budget` times, then drop it mid-flight
        let outcome = {
            let mut fut = Box::pin(common::safedir::lockdown_reused_dir(&dir, &handle));
            let mut polls = 0usize;
            std::future::poll_fn(move |cx| {
                if polls >= budget {
                    return std::task::Poll::Ready(None);
                }
                polls += 1;
                fut.as_mut().poll(cx).map(Some)
            })
            .await
        };
        if matches!(&outcome, Some(Ok(Some(_)))) {
            completed += 1;
        }
        drop(outcome);
        // let any detached blocking closure land: a `spawn_blocking` already submitted when the
        // future was dropped still runs. Without this wait the assertion could pass before a
        // post-cancellation ACL operation completes.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let got = get_acl(&dir_path, ACL_DEFAULT);
        assert_eq!(
            got.as_deref(),
            Some(default.as_slice()),
            "cancelling after {budget} poll(s) lost the directory's default ACL — those bytes \
             existed only in memory between the removal and the guard that owns them"
        );
    }
    // the sweep must actually reach completed lockdowns, or it only ever tested the trivial
    // "cancelled before anything happened" case and proves nothing about the window
    assert!(
        completed > 0,
        "no iteration ran the lockdown to completion, so the sweep never covered the window"
    );
    Ok(())
}

/// The direct-file half of the containment invariant: a strict copy of a plain file INTO an
/// existing parent that carries a permissive default ACL must not leave the destination file
/// with inherited ACL entries. That parent is the one directory kind rcp neither creates nor
/// locks down (the ambient operand parent), so the strip happens inside `create_file` itself.
/// The regression this guards was real: the create-mode mask kept the inherited entries inert
/// at `0o600`, and the final chmod to the source mode re-derived `ACL_MASK` from the group bits
/// and ACTIVATED them — a named user gained effective read access the `0o640` source never
/// granted.
#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
#[tokio::test]
async fn strict_direct_file_into_default_acl_parent_carries_no_acl() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let src = tmp.join("plain.txt");
    tokio::fs::write(&src, b"payload").await?;
    tokio::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o640)).await?;
    // the destination parent is a pre-existing user directory with a granting default ACL —
    // exactly the directory the copy neither creates nor reuses-and-locks
    let dst_parent = tmp.join("dst");
    tokio::fs::create_dir(&dst_parent).await?;
    let default = granting_acl();
    set_acl(&dst_parent, ACL_DEFAULT, &default);
    common::safedir::enable_strict_operand_resolution();
    let dst = dst_parent.join("plain.txt");
    let result = common::copy::copy(
        common::get_progress(),
        &src,
        &dst,
        &overwrite_copy_settings(),
        &common::preserve::preserve_none(),
        false,
    )
    .await;
    if let Err(e) = result {
        panic!("strict direct-file copy must succeed, got: {:#}", e.source);
    }
    assert_eq!(
        get_acl(&dst, ACL_ACCESS),
        None,
        "the destination file inherited the parent's default ACL — the final chmod makes those \
         entries effective"
    );
    assert_eq!(
        std::fs::symlink_metadata(&dst)?.permissions().mode() & 0o7777,
        0o640,
        "final mode must equal the source file mode"
    );
    // the parent itself is untouched: rcp was not asked to change a directory it only wrote into
    assert_eq!(
        get_acl(&dst_parent, ACL_DEFAULT).as_deref(),
        Some(default.as_slice()),
        "the ambient parent's own default ACL must survive the copy"
    );
    Ok(())
}

/// An aborted strict finalize must ROLL BACK a source default ACL it installed on a reused
/// directory that originally had NONE. This is the state a bare `Option`-as-guard could not
/// represent — its `None` doubled as "disarmed", so the partially-applied ACL survived the
/// abort and the originally ACL-less destination kept an ACL the source run never completed.
#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
#[tokio::test]
async fn aborted_strict_finalize_removes_the_default_acl_it_installed() -> anyhow::Result<()> {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let dst = tmp.join("reused_no_acl");
    tokio::fs::create_dir(&dst).await?;
    common::safedir::enable_strict_operand_resolution();
    let root = common::safedir::Dir::open_root_dir(&tmp, false, common::Side::Destination).await?;
    let entry = std::ffi::OsString::from("reused_no_acl");
    let handle = root.child(&entry).await?;
    let dir = root.open_dir(&entry).await?;
    let lock = common::safedir::lockdown_reused_dir(&dir, &handle)
        .await?
        .expect("strict mode must lock a reused directory");
    // simulate the finalize's partial progress: the source's default ACL already landed...
    set_acl(&dst, ACL_DEFAULT, &granting_acl());
    // ...and then the copy aborted before the guard was disarmed
    drop(lock);
    assert_eq!(
        get_acl(&dst, ACL_DEFAULT),
        None,
        "the rollback must remove the ACL the aborted finalize installed — the directory had \
         none before the copy"
    );
    Ok(())
}

/// The successful counterpart: with `d:acl` on, a strict finalize installs the SOURCE's default
/// ACL on an originally ACL-less reused directory, and it stays installed (the guard disarms
/// instead of rolling it back).
#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
#[tokio::test]
async fn strict_finalize_installs_source_default_acl_and_keeps_it() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let src = tmp.join("src_dir");
    tokio::fs::create_dir(&src).await?;
    tokio::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o755)).await?;
    let dst = tmp.join("reused_dst");
    tokio::fs::create_dir(&dst).await?;
    common::safedir::enable_strict_operand_resolution();
    let root = common::safedir::Dir::open_root_dir(&tmp, false, common::Side::Destination).await?;
    let src_meta = root
        .child(std::ffi::OsStr::new("src_dir"))
        .await?
        .meta()
        .clone();
    let entry = std::ffi::OsString::from("reused_dst");
    let handle = root.child(&entry).await?;
    let dir = root.open_dir(&entry).await?;
    let lock = common::safedir::lockdown_reused_dir(&dir, &handle)
        .await?
        .expect("strict mode must lock a reused directory");
    let source_default = granting_acl();
    common::safedir::set_reused_dir_metadata_fd(
        &common::preserve::preserve_all_with_acls(),
        &src_meta,
        Some(&common::safedir::Acls {
            access: None,
            default: Some(source_default.clone()),
        }),
        Some(lock),
        &dir,
    )
    .await?;
    assert_eq!(
        get_acl(&dst, ACL_DEFAULT).as_deref(),
        Some(source_default.as_slice()),
        "a completed finalize must keep the source's default ACL it installed"
    );
    assert_eq!(
        get_acl(&dst, ACL_ACCESS),
        None,
        "the source had no access ACL, so the destination must end without one"
    );
    assert_eq!(
        std::fs::symlink_metadata(&dst)?.permissions().mode() & 0o777,
        0o755,
        "final mode must equal the source directory mode"
    );
    Ok(())
}

/// A file created AFTER a reused-directory rollback must still strip what it inherits: the
/// rollback restores the directory's original default ACL, so the "children cannot inherit" state
/// the lockdown recorded is RE-ARMED by the guard (store before the restore syscall). Without the
/// re-arm, a create landing after a fail-early rollback — including one whose blocking closure was
/// already submitted when the guard dropped — inherited the restored ACL with the strip skipped:
/// mask-inert at the create mode, activated by any later chmod. The mid-flight interleaving is not
/// deterministically schedulable from a test; this pins the reachable end state that decides both
/// (create_file consults the flag after its openat, so an openat that sees the restored ACL also
/// sees the re-armed flag).
#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
#[tokio::test]
async fn create_after_reused_dir_rollback_strips_inherited_acl() -> anyhow::Result<()> {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let dst = tmp.join("reused_rollback");
    tokio::fs::create_dir(&dst).await?;
    set_acl(&dst, ACL_DEFAULT, &granting_acl());
    common::safedir::enable_strict_operand_resolution();
    let root = common::safedir::Dir::open_root_dir(&tmp, false, common::Side::Destination).await?;
    let entry = std::ffi::OsString::from("reused_rollback");
    let handle = root.child(&entry).await?;
    let dir = root.open_dir(&entry).await?;
    let lock = common::safedir::lockdown_reused_dir(&dir, &handle)
        .await?
        .expect("strict mode must lock a reused directory");
    // the copy aborts: the rollback restores the default ACL — and must re-arm the strip
    drop(lock);
    let file = dir.create_file(std::ffi::OsStr::new("late.txt")).await?;
    drop(file);
    assert_eq!(
        get_acl(&dst.join("late.txt"), ACL_ACCESS),
        None,
        "a file created after the rollback inherited the restored default ACL without a strip — \
         inert now, activated by the next chmod"
    );
    Ok(())
}

/// Cancelling the strict finalize at ANY point before its successful return must leave the reused
/// directory holding its OWN original default ACL — never the source's.
///
/// The finalize installs the source's default ACL early (through the lockdown guard, while the
/// copier still owns the directory) and then runs several more fallible steps: the owner restore,
/// the inner metadata applier, and the final re-stat verification. A cancellation landing anywhere
/// in that tail is a FAILED copy of this directory, and failing toward *unchanged* requires the
/// guard to remain armed there so its `Drop` rolls the just-installed source ACL back. This includes
/// cancellation at the re-stat await, an fstat error, or a verification failure.
///
/// Cancellation is driven by POLL COUNT, exactly as in
/// `lockdown_reused_dir_never_loses_the_default_acl_when_cancelled` (see there for why a
/// timer-based sweep walks past these windows). The per-iteration assertion is state-based — for
/// EVERY budget the directory must end in one of the two legal states (cancelled → the original
/// ACL, completed → the source's) — so the sweep stays sound even if the poll↔await mapping
/// shifts; the `completed > 0` backstop proves it reached the latest windows.
#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_strict_finalize_restores_the_destinations_default_acl() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let src = tmp.join("src_dir");
    tokio::fs::create_dir(&src).await?;
    tokio::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o755)).await?;
    common::safedir::enable_strict_operand_resolution();
    let root = common::safedir::Dir::open_root_dir(&tmp, false, common::Side::Destination).await?;
    let src_meta = root
        .child(std::ffi::OsStr::new("src_dir"))
        .await?
        .meta()
        .clone();
    // the destination's own ACL and the source's must be DISTINGUISHABLE byte-wise, or a leaked
    // source install would satisfy an "ACL is present" assertion (named user gets 5, mask 5,
    // vs the granting ACL's 7/7)
    let original = encode_acl(&[
        (0x01, 7, 0xffff_ffff),
        (0x02, 5, 65534),
        (0x04, 5, 0xffff_ffff),
        (0x10, 5, 0xffff_ffff),
        (0x20, 5, 0xffff_ffff),
    ]);
    let source_default = granting_acl();
    assert_ne!(original, source_default);
    let mut completed = 0usize;
    for budget in 0..48usize {
        let name = format!("reused{budget}");
        let dir_path = tmp.join(&name);
        tokio::fs::create_dir(&dir_path).await?;
        set_acl(&dir_path, ACL_DEFAULT, &original);
        let entry = std::ffi::OsString::from(&name);
        let handle = root.child(&entry).await?;
        let dir = root.open_dir(&entry).await?;
        let lock = common::safedir::lockdown_reused_dir(&dir, &handle)
            .await?
            .expect("strict mode must lock a reused directory");
        // poll the finalize at most `budget` times, then drop it mid-flight (the dropped future
        // drops the still-armed lock, whose rollback is what this sweep pins)
        let preserve = common::preserve::preserve_all_with_acls();
        let acls = common::safedir::Acls {
            access: None,
            default: Some(source_default.clone()),
        };
        let outcome = {
            let mut fut = Box::pin(common::safedir::set_reused_dir_metadata_fd(
                &preserve,
                &src_meta,
                Some(&acls),
                Some(lock),
                &dir,
            ));
            let mut polls = 0usize;
            std::future::poll_fn(move |cx| {
                if polls >= budget {
                    return std::task::Poll::Ready(None);
                }
                polls += 1;
                fut.as_mut().poll(cx).map(Some)
            })
            .await
        };
        let finished = match &outcome {
            Some(Ok(())) => true,
            Some(Err(e)) => panic!("finalize failed on its own (budget {budget}): {e:#}"),
            None => false,
        };
        drop(outcome);
        // let any DETACHED blocking closure land before checking, as in the lockdown sweep: a
        // `spawn_blocking` already submitted when the future was dropped still runs, serialized
        // against the guard's rollback by the state mutex.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let got = get_acl(&dir_path, ACL_DEFAULT);
        if finished {
            completed += 1;
            assert_eq!(
                got.as_deref(),
                Some(source_default.as_slice()),
                "a finalize that ran to completion must keep the source's default ACL it \
                 installed (budget {budget})"
            );
        } else {
            assert_eq!(
                got.as_deref(),
                Some(original.as_slice()),
                "cancelling after {budget} poll(s) must roll the directory back to its OWN \
                 original default ACL — a failed copy must not leave the source's installed"
            );
        }
    }
    assert!(
        completed > 0,
        "no iteration ran the finalize to completion, so the sweep never covered the \
         late-cancellation windows"
    );
    Ok(())
}

/// A directory rcp CREATES under a default-ACL parent is sanitized inside the creation call
/// itself: both inherited ACLs are stripped, so nothing created beneath it inherits anything —
/// files beneath it pay no per-file strip.
#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
#[tokio::test]
async fn strict_make_dir_under_default_acl_parent_strips_both_inherited_acls() -> anyhow::Result<()>
{
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    let holder = tmp.join("holder");
    tokio::fs::create_dir(&holder).await?;
    set_acl(&holder, ACL_DEFAULT, &granting_acl());
    common::safedir::enable_strict_operand_resolution();
    let root = common::safedir::Dir::open_root_dir(&tmp, false, common::Side::Destination).await?;
    let holder_dir = root.open_dir(std::ffi::OsStr::new("holder")).await?;
    let made = holder_dir
        .make_dir(
            std::ffi::OsStr::new("fresh"),
            common::safedir::DST_DIR_CREATE_MODE,
        )
        .await?;
    let fresh = holder.join("fresh");
    assert_eq!(
        get_acl(&fresh, ACL_ACCESS),
        None,
        "a created directory must not keep the access ACL it inherited"
    );
    assert_eq!(
        get_acl(&fresh, ACL_DEFAULT),
        None,
        "a created directory must not keep the default ACL it inherited — every child would"
    );
    // and a file created beneath it inherits nothing, because the chain was broken above
    let _file = made.create_file(std::ffi::OsStr::new("f.txt")).await?;
    assert_eq!(
        get_acl(&fresh.join("f.txt"), ACL_ACCESS),
        None,
        "a file beneath a sanitized directory must carry no inherited ACL"
    );
    Ok(())
}
