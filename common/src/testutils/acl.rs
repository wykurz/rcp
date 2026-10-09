use std::ffi::CStr;
use std::os::unix::ffi::OsStrExt as _;

// POSIX.1e ACL entry tags, from `<linux/posix_acl.h>`.
pub(crate) const ACL_USER_OBJ: u16 = 0x01;
pub(crate) const ACL_USER: u16 = 0x02;
pub(crate) const ACL_GROUP_OBJ: u16 = 0x04;
pub(crate) const ACL_MASK: u16 = 0x10;
pub(crate) const ACL_OTHER: u16 = 0x20;
pub(crate) const ACL_UNDEFINED_ID: u32 = 0xffff_ffff;

// encode an ACL exactly the way the kernel stores it in `system.posix_acl_*`: a `__le32`
// version followed by `{__le16 tag, __le16 perm, __le32 id}` entries. Written directly rather
// than through `setfacl`, which the dev shell does not ship — and which would prove less
// anyway, since these are the same bytes the code under test round-trips.
pub(crate) fn encode_acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut out = 2u32.to_le_bytes().to_vec();
    for &(tag, perm, id) in entries {
        out.extend_from_slice(&tag.to_le_bytes());
        out.extend_from_slice(&perm.to_le_bytes());
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

// `u::rwx u:65534:--- g::--- m::r-x o::r-x`, i.e. mode 0o755 plus a named entry DENYING 65534
// what `other` grants everyone else. No mode can express that, which is why dropping the ACL
// hands 65534 exactly what the source withheld.
pub(crate) fn restrictive_access_acl() -> Vec<u8> {
    encode_acl(&[
        (ACL_USER_OBJ, 7, ACL_UNDEFINED_ID),
        (ACL_USER, 0, 65534),
        (ACL_GROUP_OBJ, 0, ACL_UNDEFINED_ID),
        (ACL_MASK, 5, ACL_UNDEFINED_ID),
        (ACL_OTHER, 5, ACL_UNDEFINED_ID),
    ])
}

// `u::rwx u:65534:rwx g::r-x m::rwx o::r-x` — permissive, and the shape an administrator sets
// as a DEFAULT ACL on a destination tree so new children inherit it.
pub(crate) fn permissive_acl() -> Vec<u8> {
    encode_acl(&[
        (ACL_USER_OBJ, 7, ACL_UNDEFINED_ID),
        (ACL_USER, 7, 65534),
        (ACL_GROUP_OBJ, 5, ACL_UNDEFINED_ID),
        (ACL_MASK, 7, ACL_UNDEFINED_ID),
        (ACL_OTHER, 5, ACL_UNDEFINED_ID),
    ])
}

// an ACL the kernel refuses (EINVAL): a lone named entry, with none of the three required
// `USER_OBJ`/`GROUP_OBJ`/`OTHER` ones. Gives the appliers a deterministic `fsetxattr` failure
// with no privileged uid or hostile filesystem needed — the ACL counterpart of the
// out-of-range nanosecond field the utimens ordering test uses.
pub(crate) fn rejected_access_acl() -> Vec<u8> {
    encode_acl(&[(ACL_USER, 7, 65534)])
}

pub(crate) fn set_xattr_at(path: &std::path::Path, name: &CStr, value: &[u8]) {
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: both pointers are NUL-terminated C strings that outlive the call, and `value`
    // points at `value.len()` readable bytes.
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
        "fixture setxattr({name:?}) on {path:?} failed: {} — this filesystem cannot hold \
         POSIX ACLs, so these tests cannot run here",
        std::io::Error::last_os_error()
    );
}

/// Read `name` from `path`, or `None` if the entry genuinely has no such attribute.
///
/// ONLY `ENODATA` yields `None`. Every other errno panics — a getter that answered "no ACL" for
/// `ENOENT` would let a test assert "this entry has no ACL" about a path that does not exist,
/// which is a passing test that checks nothing. The size is queried first so an ACL of any
/// length round-trips (the ERANGE fixture below writes one far past a 512-byte buffer).
pub(crate) fn get_xattr_at(path: &std::path::Path, name: &CStr) -> Option<Vec<u8>> {
    let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: `cpath` and `name` are NUL-terminated C strings that outlive the call; a null
    // buffer with size 0 asks for the size without writing.
    let size = unsafe { libc::getxattr(cpath.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        let err = std::io::Error::last_os_error();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ENODATA),
            "getxattr({name:?}) on {path:?} failed with {err} — only ENODATA means \"no such \
             attribute\"; anything else means the check never happened"
        );
        return None;
    }
    let mut buf = vec![0u8; size as usize];
    if buf.is_empty() {
        return Some(buf);
    }
    // SAFETY: as above; `buf` has `len()` writable bytes.
    let n = unsafe {
        libc::getxattr(
            cpath.as_ptr(),
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            buf.len(),
        )
    };
    assert!(
        n >= 0,
        "getxattr({name:?}) on {path:?} failed after its size was read: {}",
        std::io::Error::last_os_error()
    );
    buf.truncate(n as usize);
    Some(buf)
}
