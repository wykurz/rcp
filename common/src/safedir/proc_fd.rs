//! Operations through a held descriptor's trusted procfs magic link.
//!
//! Callers borrow descriptors owned by their existing blocking jobs. Keeping the borrow with the
//! operation prevents the owner from closing before its syscall completes; no descriptor is cloned
//! here. Procfs must be mounted and trusted, as described in docs/tocttou.md.

use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;

use nix::fcntl::{AT_FDCWD, AtFlags, OFlag, openat};
use nix::sys::stat::Mode;

/// Borrow a descriptor for operations that follow its procfs magic link.
///
/// The formatted path stays private. Only `diagnostic_path` returns the link's text, which must
/// never become authority for another filesystem operation.
pub(super) struct ProcFd<'fd>(BorrowedFd<'fd>);

impl<'fd> ProcFd<'fd> {
    pub(super) fn new(fd: BorrowedFd<'fd>) -> Self {
        Self(fd)
    }

    fn path(&self) -> String {
        format!("/proc/self/fd/{}", self.0.as_raw_fd())
    }

    pub(super) fn open(&self, flags: OFlag) -> nix::Result<OwnedFd> {
        // following the magic link selects the held inode; O_NOFOLLOW would reject it
        openat(AT_FDCWD, self.path().as_str(), flags, Mode::empty())
    }

    pub(super) fn hard_link_at(
        &self,
        parent: BorrowedFd<'_>,
        name: &std::ffi::OsStr,
    ) -> std::io::Result<()> {
        // without AT_SYMLINK_FOLLOW, linkat would try to link the proc magic link itself
        nix::unistd::linkat(
            AT_FDCWD,
            self.path().as_str(),
            parent,
            name.as_bytes(),
            AtFlags::AT_SYMLINK_FOLLOW,
        )
        .map_err(super::nix_to_io)
    }

    pub(super) fn chmod(&self, mode: u32) -> std::io::Result<()> {
        // using NoFollowSymlink would chmod the magic link itself, a silent no-op
        nix::sys::stat::fchmodat(
            AT_FDCWD,
            self.path().as_str(),
            Mode::from_bits_truncate(mode),
            nix::sys::stat::FchmodatFlags::FollowSymlink,
        )
        .map_err(super::nix_to_io)
    }

    pub(super) fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        std::fs::metadata(self.path())
    }

    pub(super) fn xattr_names(&self) -> std::io::Result<Vec<u8>> {
        // flistxattr rejects O_PATH descriptors; following the magic link reaches their inode
        let path = std::ffi::CString::new(self.path())
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        // SAFETY: path is NUL-terminated and the borrowed descriptor remains open throughout the
        // retries. listxattr_retry supplies writable storage of len bytes, or null with len == 0
        let probe = |buf, len| {
            unsafe { libc::listxattr(path.as_ptr(), buf, len) } // rcp-toctou-allow: proc magic link to a borrowed held fd
        };
        super::listxattr_retry(probe)
    }

    /// Read the link text for diagnostics, never for subsequent filesystem operations.
    pub(super) fn diagnostic_path(&self) -> std::io::Result<std::path::PathBuf> {
        std::fs::read_link(self.path())
    }
}
