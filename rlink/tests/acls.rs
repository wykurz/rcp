//! ACL preservation under `rlink`, whose hard links make it a different problem from `rcp`.
//!
//! A hard-linked destination entry SHARES the source's inode. There is no separate destination to
//! write metadata to — writing an ACL "to the destination" would rewrite the SOURCE's permissions,
//! silently changing a tree the user did not ask to modify. `f:acl` therefore applies only on
//! rlink's real copy path (changed files under `--update`), and the hard-link path applies no
//! metadata at all.

use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

const ACL_ACCESS: &std::ffi::CStr = c"system.posix_acl_access";
const ACL_USER_OBJ: u16 = 0x01;
const ACL_USER: u16 = 0x02;
const ACL_GROUP_OBJ: u16 = 0x04;
const ACL_MASK: u16 = 0x10;
const ACL_OTHER: u16 = 0x20;
const UNDEF: u32 = 0xffff_ffff;

/// Encode an ACL in the kernel's `system.posix_acl_*` layout: a `__le32` version followed by
/// `{__le16 tag, __le16 perm, __le32 id}` entries. Written directly because `setfacl` is not in the
/// dev shell — and these are the same bytes the code under test carries.
fn encode_acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut out = 2u32.to_le_bytes().to_vec();
    for &(tag, perm, id) in entries {
        out.extend_from_slice(&tag.to_le_bytes());
        out.extend_from_slice(&perm.to_le_bytes());
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

/// `u::rwx u:65534:--- g::--- m::r-x o::r-x` — a named entry narrower than `other`.
fn denying_acl() -> Vec<u8> {
    encode_acl(&[
        (ACL_USER_OBJ, 7, UNDEF),
        (ACL_USER, 0, 65534),
        (ACL_GROUP_OBJ, 0, UNDEF),
        (ACL_MASK, 5, UNDEF),
        (ACL_OTHER, 5, UNDEF),
    ])
}

fn set_acl(path: &Path, value: &[u8]) {
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: both pointers are NUL-terminated C strings that outlive the call, and `value` points
    // at `value.len()` readable bytes.
    let rc = unsafe {
        libc::setxattr(
            cpath.as_ptr(),
            ACL_ACCESS.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    };
    assert_eq!(
        rc,
        0,
        "cannot write an ACL on {path:?}: {}. This test needs a filesystem that holds POSIX ACLs; \
         it fails rather than skips.",
        std::io::Error::last_os_error()
    );
}

/// Read the access ACL of `path`, or `None` if it genuinely has none.
///
/// ONLY `ENODATA` yields `None`; every other errno panics. `ENOENT` in particular must not read as
/// "no ACL" — this test's whole claim is that the SOURCE inode still carries what it carried, and a
/// getter that shrugged at a wrong path would prove that about nothing.
fn get_acl(path: &Path) -> Option<Vec<u8>> {
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut buf = [0u8; 1024];
    // SAFETY: as above; the kernel writes at most `buf.len()` bytes into `buf`.
    let n = unsafe {
        libc::getxattr(
            cpath.as_ptr(),
            ACL_ACCESS.as_ptr(),
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
        "getxattr on {path:?} failed with {err} — only ENODATA means \"no such attribute\""
    );
    None
}

fn write_file(path: &Path, content: &str, mode: u32) {
    std::fs::write(path, content).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn same_times(from: &Path, to: &Path) {
    let meta = std::fs::metadata(from).unwrap();
    let times = std::fs::FileTimes::new()
        .set_accessed(meta.accessed().unwrap())
        .set_modified(meta.modified().unwrap());
    std::fs::File::options()
        .write(true)
        .open(to)
        .unwrap()
        .set_times(times)
        .unwrap();
}

#[test]
fn hard_linking_never_writes_an_acl_through_the_shared_inode() {
    let src = tempfile::tempdir().unwrap();
    let update = tempfile::tempdir().unwrap();
    let dst_parent = tempfile::tempdir().unwrap();
    // rlink refuses to write into a destination that already exists, so name one it creates
    let dst = dst_parent.path().join("out");
    // `unchanged.txt` matches between src and update, so rlink hard-links it FROM src — the
    // destination and the source become one inode.
    write_file(&src.path().join("unchanged.txt"), "same", 0o700);
    write_file(&update.path().join("unchanged.txt"), "same", 0o700);
    same_times(
        &src.path().join("unchanged.txt"),
        &update.path().join("unchanged.txt"),
    );
    // `changed.txt` differs, so rlink COPIES it from the update tree — a fresh inode, where `f:acl`
    // applies like any other copy.
    write_file(&src.path().join("changed.txt"), "old", 0o700);
    write_file(&update.path().join("changed.txt"), "new content", 0o700);
    let blob = denying_acl();
    // The source's hard-linked file carries an ACL and its update-tree counterpart does NOT. An
    // applier that read the update tree's ACLs and wrote them "to the destination" would clear this
    // one — through the shared inode, on a tree rlink was never asked to modify.
    set_acl(&src.path().join("unchanged.txt"), &blob);
    set_acl(&update.path().join("changed.txt"), &blob);
    let src_before = std::fs::metadata(src.path().join("unchanged.txt")).unwrap();
    let output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rlink"))
        .args([
            "--preserve-settings=all+acl",
            "--update",
            update.path().to_str().unwrap(),
            src.path().to_str().unwrap(),
            dst.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "rlink failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let linked = dst.join("unchanged.txt");
    let copied = dst.join("changed.txt");
    let src_after = std::fs::metadata(src.path().join("unchanged.txt")).unwrap();
    assert_eq!(
        std::fs::metadata(&linked).unwrap().ino(),
        src_after.ino(),
        "fixture must actually hard-link, or this test proves nothing"
    );
    assert_eq!(
        get_acl(&src.path().join("unchanged.txt")).as_deref(),
        Some(blob.as_slice()),
        "rlink modified the SOURCE's ACL through the inode its hard link shares"
    );
    assert_eq!(
        src_before.permissions().mode(),
        src_after.permissions().mode(),
        "rlink modified the SOURCE's mode through the inode its hard link shares"
    );
    // the real copy path is a separate inode, so `f:acl` applies there normally
    assert_ne!(
        std::fs::metadata(&copied).unwrap().ino(),
        std::fs::metadata(update.path().join("changed.txt"))
            .unwrap()
            .ino(),
        "a changed file must be copied, not linked"
    );
    assert_eq!(
        get_acl(&copied).as_deref(),
        Some(blob.as_slice()),
        "`f:acl` must still apply on rlink's real copy path"
    );
}

#[test]
fn directory_acls_apply_normally_because_rlink_creates_directories_fresh() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let src_sub = src.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    write_file(&src_sub.join("f.txt"), "payload", 0o644);
    let blob = denying_acl();
    set_acl(&src_sub, &blob);
    let dst_sub = dst.path().join("tree");
    let output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rlink"))
        .args([
            "--preserve-settings=all+acl",
            src_sub.to_str().unwrap(),
            dst_sub.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "rlink failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_ne!(
        std::fs::metadata(&dst_sub).unwrap().ino(),
        std::fs::metadata(&src_sub).unwrap().ino(),
        "directories are created fresh, never linked"
    );
    assert_eq!(get_acl(&dst_sub).as_deref(), Some(blob.as_slice()));
}

const ACL_DEFAULT: &std::ffi::CStr = c"system.posix_acl_default";

/// `u::rwx u:65534:rwx g::r-x m::rwx o::r-x` — what an administrator sets as a DEFAULT ACL on a
/// destination tree so new children inherit it.
fn granting_acl() -> Vec<u8> {
    encode_acl(&[
        (ACL_USER_OBJ, 7, UNDEF),
        (ACL_USER, 7, 65534),
        (ACL_GROUP_OBJ, 5, UNDEF),
        (ACL_MASK, 7, UNDEF),
        (ACL_OTHER, 5, UNDEF),
    ])
}

fn set_named_acl(path: &Path, name: &std::ffi::CStr, value: &[u8]) {
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

fn get_named_acl(path: &Path, name: &std::ffi::CStr) -> Option<Vec<u8>> {
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

/// rlink resolves its destination directories through the same `safedir` creation site as rcp, so
/// `--require-toctou-safe` contains an inherited destination ACL here too. Worth its own test rather
/// than an assumption: rlink reaches that site through its own `link_dir_contents`, and the entries
/// it materializes underneath are HARD LINKS, whose inode is the source's — a containment scheme
/// that had to touch each entry would have nowhere safe to do it.
#[test]
fn require_toctou_safe_contains_an_inherited_destination_acl() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    let src = tempfile::tempdir().unwrap();
    let dst_parent = tempfile::tempdir().unwrap();
    // canonicalize: TMPDIR may contain symlinked components, which strict resolution refuses
    let src_base = src.path().canonicalize().unwrap();
    let dst_base = dst_parent.path().canonicalize().unwrap();
    let src_tree = src_base.join("tree");
    std::fs::create_dir(&src_tree).unwrap();
    std::fs::create_dir(src_tree.join("nested")).unwrap();
    write_file(&src_tree.join("f.txt"), "payload", 0o644);
    write_file(&src_tree.join("nested/deep.txt"), "deeper", 0o644);
    // the administrator's default ACL on the destination tree rlink writes into
    set_named_acl(&dst_base, ACL_DEFAULT, &granting_acl());
    let dst_tree = dst_base.join("tree");
    let output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rlink"))
        .args([
            "--require-toctou-safe",
            src_tree.to_str().unwrap(),
            dst_tree.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "rlink failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    for rel in ["", "nested"] {
        let path = dst_tree.join(rel);
        assert_eq!(
            get_named_acl(&path, ACL_ACCESS),
            None,
            "{path:?} carries an inherited access ACL; its source had none"
        );
        assert_eq!(
            get_named_acl(&path, ACL_DEFAULT),
            None,
            "{path:?} carries an inherited default ACL, which would widen anything created under \
             it later"
        );
    }
    // the hard links themselves: created inside stripped directories, so they inherited nothing —
    // which matters doubly here, since writing an ACL to one would rewrite the SOURCE's
    for rel in ["f.txt", "nested/deep.txt"] {
        let linked = dst_tree.join(rel);
        assert_eq!(
            std::fs::metadata(&linked).unwrap().ino(),
            std::fs::metadata(src_tree.join(rel)).unwrap().ino(),
            "fixture must actually hard-link, or this test proves nothing"
        );
        assert_eq!(get_named_acl(&linked, ACL_ACCESS), None);
    }
}

/// The marker every form of the ACL-preservation notice shares, so a wording change does not silently
/// make the tests below assert nothing.
const ACL_NOTICE: &str = "does not preserve POSIX ACLs";

/// Run `rlink` at the DEFAULT verbosity and return everything it wrote. Both streams, because the
/// log layer writes through the progress bar's writer, which targets stdout.
fn rlink_log(args: &[&str]) -> String {
    let output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rlink"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "rlink failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The ACL-preservation notice is armed by a run asking for metadata fidelity, and rlink's default
/// is `all`. A bare `rlink` therefore warns where a bare `rcp` stays silent; preserve-none turns it
/// off. The notice is settings-only and does not inspect the source root.
#[test]
fn a_bare_rlink_reports_that_its_settings_may_omit_directory_acls() {
    let src = tempfile::tempdir().unwrap();
    let dst_parent = tempfile::tempdir().unwrap();
    let src_tree = src.path().join("tree");
    std::fs::create_dir(&src_tree).unwrap();
    write_file(&src_tree.join("f.txt"), "payload", 0o644);
    let log = rlink_log(&[
        src_tree.to_str().unwrap(),
        dst_parent.path().join("bare").to_str().unwrap(),
    ]);
    assert!(
        log.contains(ACL_NOTICE) && log.contains("for directories"),
        "rlink defaults to metadata preservation but does not carry directory ACLs:\n{log}"
    );
    // and settings that ask for nothing switch it off, so the gate is the SETTINGS rather than the
    // tool — otherwise this test would pass with the notice unconditional
    let log = rlink_log(&[
        "--preserve-settings=none",
        src_tree.to_str().unwrap(),
        dst_parent.path().join("none").to_str().unwrap(),
    ]);
    assert!(
        !log.contains(ACL_NOTICE),
        "`none` asked for no preservation, so the notice is noise about a fidelity this link never \
         claimed:\n{log}"
    );
}
