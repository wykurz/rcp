//! POSIX.1e ACL fixtures for the integration tests.
//!
//! ACLs are written as raw extended attributes rather than through `setfacl`, which the nix dev
//! shell does not ship. The blob layout is the kernel's `system.posix_acl_*` format: a `__le32`
//! version followed by `{__le16 tag, __le16 perm, __le32 id}` entries.
//!
//! Per CLAUDE.md these fail fast rather than skip: a filesystem that cannot hold an ACL makes the
//! fixture panic with a message saying so, instead of silently turning the test vacuous.
#![allow(dead_code)]

use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;

pub const ACL_ACCESS: &std::ffi::CStr = c"system.posix_acl_access";
pub const ACL_DEFAULT: &std::ffi::CStr = c"system.posix_acl_default";

// entry tags, from `<linux/posix_acl.h>`
pub const ACL_USER_OBJ: u16 = 0x01;
pub const ACL_USER: u16 = 0x02;
pub const ACL_GROUP_OBJ: u16 = 0x04;
pub const ACL_MASK: u16 = 0x10;
pub const ACL_OTHER: u16 = 0x20;
pub const ACL_UNDEFINED_ID: u32 = 0xffff_ffff;

/// The uid used in every named entry — `nobody`, which exists everywhere and owns nothing here.
pub const NOBODY: u32 = 65534;

pub fn encode_acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut out = 2u32.to_le_bytes().to_vec();
    for &(tag, perm, id) in entries {
        out.extend_from_slice(&tag.to_le_bytes());
        out.extend_from_slice(&perm.to_le_bytes());
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

/// `u::rwx u:65534:--- g::--- m::r-x o::r-x` — a named entry NARROWER than `other`, which no mode
/// can express. Applying it leaves the entry's own mode at `0o755` (group bits come from `MASK`),
/// so a destination that keeps the mode and drops the ACL grants 65534 read+execute on something
/// the source denied it entirely.
pub fn denying_acl() -> Vec<u8> {
    encode_acl(&[
        (ACL_USER_OBJ, 7, ACL_UNDEFINED_ID),
        (ACL_USER, 0, NOBODY),
        (ACL_GROUP_OBJ, 0, ACL_UNDEFINED_ID),
        (ACL_MASK, 5, ACL_UNDEFINED_ID),
        (ACL_OTHER, 5, ACL_UNDEFINED_ID),
    ])
}

/// `u::rwx u:65534:rwx g::r-x m::rwx o::r-x` — what an administrator sets as a DEFAULT ACL on a
/// destination tree so that new children inherit it.
pub fn granting_acl() -> Vec<u8> {
    encode_acl(&[
        (ACL_USER_OBJ, 7, ACL_UNDEFINED_ID),
        (ACL_USER, 7, NOBODY),
        (ACL_GROUP_OBJ, 5, ACL_UNDEFINED_ID),
        (ACL_MASK, 7, ACL_UNDEFINED_ID),
        (ACL_OTHER, 5, ACL_UNDEFINED_ID),
    ])
}

pub fn set_acl(path: &Path, name: &std::ffi::CStr, value: &[u8]) {
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
        "cannot write {name:?} on {path:?}: {}. These tests need a filesystem that holds POSIX \
         ACLs; they fail rather than skip so a lost feature cannot pass unnoticed.",
        std::io::Error::last_os_error()
    );
}

/// Read `name` from `path`, or `None` if the entry genuinely has no such attribute.
///
/// ONLY `ENODATA` yields `None`. Every other errno panics, `ENOENT` above all: a getter that
/// reported "no ACL" for a path that does not exist would let `assert_eq!(get_acl(p), None)` pass
/// for a misspelled `p`, so a test walking a subtree could check nothing and still be green.
pub fn get_acl(path: &Path, name: &std::ffi::CStr) -> Option<Vec<u8>> {
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut buf = [0u8; 1024];
    // SAFETY: both pointers are NUL-terminated C strings that outlive the call, and the kernel
    // writes at most `buf.len()` bytes into `buf`.
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
        "getxattr({name:?}) on {path:?} failed with {err} — only ENODATA means \"no such \
         attribute\"; anything else (ENOENT above all) means the assertion never happened"
    );
    None
}

/// Decode a blob into `(tag, perm, id)` entries, for assertion messages that a human can read.
pub fn describe_acl(blob: Option<&Vec<u8>>) -> String {
    let Some(blob) = blob else {
        return "(none)".to_string();
    };
    let name = |tag: u16, id: u32| match tag {
        ACL_USER_OBJ => "u::".to_string(),
        ACL_USER => format!("u:{id}:"),
        ACL_GROUP_OBJ => "g::".to_string(),
        ACL_MASK => "m::".to_string(),
        ACL_OTHER => "o::".to_string(),
        other => format!("?{other:#x}:"),
    };
    let rwx = |p: u16| {
        format!(
            "{}{}{}",
            if p & 4 != 0 { 'r' } else { '-' },
            if p & 2 != 0 { 'w' } else { '-' },
            if p & 1 != 0 { 'x' } else { '-' }
        )
    };
    blob[4..]
        .chunks_exact(8)
        .map(|e| {
            let tag = u16::from_le_bytes([e[0], e[1]]);
            let perm = u16::from_le_bytes([e[2], e[3]]);
            let id = u32::from_le_bytes([e[4], e[5], e[6], e[7]]);
            format!("{}{}", name(tag, id), rwx(perm))
        })
        .collect::<Vec<_>>()
        .join(" ")
}
