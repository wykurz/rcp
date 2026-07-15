//! Safe, race-resistant directory traversal primitives.
//!
//! This module provides `O_NOFOLLOW`-based directory and file handle types that
//! prevent TOCTOU races by using file-descriptor-relative syscalls (`openat`,
//! `fstatat`) rather than path-based lookups. Every `open_dir`/`child` call
//! refuses to follow symlinks, so an attacker who races a directory walk cannot
//! redirect operations outside the intended tree.

use std::ffi::OsStr;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

use nix::fcntl::{AT_FDCWD, AtFlags, OFlag, openat, readlinkat};
use nix::sys::stat::{FileStat, Mode, fchmod, fstat, fstatat, futimens, mkdirat};
use nix::sys::time::TimeSpec;
use nix::unistd::{Gid, Uid, UnlinkatFlags, fchown, fchownat, linkat, symlinkat, unlinkat};

use crate::walk::EntryKind;

// ── Strict operand resolution (--require-toctou-safe) ────────────────────────
//
// A process-global, one-way switch armed by the TOCTOU linter (see
// `crate::toctou_check::run_linter`) when `--require-toctou-safe` proceeds. When
// armed, the two multi-component path resolutions in this module —
// `Dir::open_root_dir` and `Dir::open_parent_dir`, the only places an operand
// path is resolved — use `openat2(2)` with `RESOLVE_NO_SYMLINKS` instead of a
// plain `openat`, so a symlink in ANY component of an operand path fails closed
// with `ELOOP` at the open itself (not in a racy pre-check). This is a global
// rather than a threaded setting because it is per-process security policy that
// must cover every operand open in every engine (copy/link/rm/chmod, local and
// rcpd) without widening each Settings struct and the rcpd spawn contract; it is
// armed once before the async runtime starts and never unset.

static STRICT_OPERAND_RESOLUTION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Arm strict operand resolution for the rest of this process (one-way).
///
/// Called by the TOCTOU linter when `--require-toctou-safe` proceeds; not
/// intended to be called from anywhere else.
pub fn enable_strict_operand_resolution() {
    STRICT_OPERAND_RESOLUTION.store(true, std::sync::atomic::Ordering::Release);
}

/// Whether strict operand resolution is armed for this process.
#[must_use]
pub fn strict_operand_resolution() -> bool {
    STRICT_OPERAND_RESOLUTION.load(std::sync::atomic::Ordering::Acquire)
}

/// Open `path` with `openat2(2)`, refusing to resolve ANY symlink component
/// (`RESOLVE_NO_SYMLINKS`, which also implies `RESOLVE_NO_MAGICLINKS`). A
/// symlink anywhere in the path fails with `ELOOP`.
#[cfg(target_os = "linux")]
fn openat2_no_symlinks(path: &Path, flags: OFlag) -> nix::Result<OwnedFd> {
    use nix::fcntl::{OpenHow, ResolveFlag, openat2};
    let how = OpenHow::new()
        .flags(flags)
        .resolve(ResolveFlag::RESOLVE_NO_SYMLINKS);
    // bounded retry: with resolve restrictions openat2 can return EAGAIN when the
    // kernel detects a rename race during resolution (see openat2(2)); retrying a
    // handful of times resolves transient races without risking an unbounded loop.
    let mut attempts = 0;
    loop {
        match openat2(AT_FDCWD, path, how) {
            Err(nix::errno::Errno::EAGAIN) if attempts < 4 => attempts += 1,
            other => return other,
        }
    }
}

/// Whether this kernel supports `openat2(2)` (Linux 5.6+), probed once.
///
/// Strict operand resolution is impossible without it, so
/// `--require-toctou-safe` refuses to run when this returns `false`.
#[cfg(target_os = "linux")]
#[must_use]
pub fn openat2_available() -> bool {
    static PROBE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *PROBE.get_or_init(|| {
        // "/" always exists and is a directory; ENOSYS is the only expected failure
        !matches!(
            openat2_no_symlinks(Path::new("/"), OFlag::O_PATH | OFlag::O_CLOEXEC),
            Err(nix::errno::Errno::ENOSYS)
        )
    })
}

/// Non-Linux builds have no `openat2`; `--require-toctou-safe` already refuses
/// to run there (the hardened walk is Linux-only), so this is only consulted to
/// render an accurate `--toctou-check` note.
#[cfg(not(target_os = "linux"))]
#[must_use]
pub fn openat2_available() -> bool {
    false
}

// ── FileMeta ──────────────────────────────────────────────────────────────────

/// A snapshot of filesystem metadata obtained via `fstat`/`fstatat`.
///
/// Implements [`crate::preserve::Metadata`] so callers can apply these fields
/// to another entry with the existing `set_*_metadata` helpers.
#[derive(Clone, Debug)]
pub struct FileMeta {
    uid: u32,
    gid: u32,
    atime: i64,
    atime_nsec: i64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
    mode: u32,
    size: u64,
}

impl FileMeta {
    fn from_stat(st: &FileStat) -> Self {
        Self {
            uid: st.st_uid,
            gid: st.st_gid,
            atime: st.st_atime,
            atime_nsec: st.st_atime_nsec,
            mtime: st.st_mtime,
            mtime_nsec: st.st_mtime_nsec,
            ctime: st.st_ctime,
            ctime_nsec: st.st_ctime_nsec,
            mode: st.st_mode,
            size: st.st_size as u64,
        }
    }
}

impl crate::preserve::Metadata for FileMeta {
    fn uid(&self) -> u32 {
        self.uid
    }
    fn gid(&self) -> u32 {
        self.gid
    }
    fn atime(&self) -> i64 {
        self.atime
    }
    fn atime_nsec(&self) -> i64 {
        self.atime_nsec
    }
    fn mtime(&self) -> i64 {
        self.mtime
    }
    fn mtime_nsec(&self) -> i64 {
        self.mtime_nsec
    }
    fn permissions(&self) -> std::fs::Permissions {
        use std::os::unix::fs::PermissionsExt;
        std::fs::Permissions::from_mode(self.mode)
    }
    fn ctime(&self) -> i64 {
        self.ctime
    }
    fn ctime_nsec(&self) -> i64 {
        self.ctime_nsec
    }
    fn size(&self) -> u64 {
        self.size
    }
}

// ── EntryKind classification ───────────────────────────────────────────────────

fn kind_from_stat(st: &FileStat) -> EntryKind {
    let mode = st.st_mode;
    // use libc mode-classification macros via their bit patterns (POSIX S_IFMT)
    // S_IFREG = 0o0100000, S_IFDIR = 0o0040000, S_IFLNK = 0o0120000
    let ifmt = mode & libc::S_IFMT;
    match ifmt {
        libc::S_IFREG => EntryKind::File,
        libc::S_IFDIR => EntryKind::Dir,
        libc::S_IFLNK => EntryKind::Symlink,
        _ => EntryKind::Special,
    }
}

// ── Handle ────────────────────────────────────────────────────────────────────

/// An open, classified handle to a filesystem entry obtained via `O_PATH|O_NOFOLLOW`.
///
/// The fd is opened with `O_PATH`, so it cannot be used for reading; it exists
/// solely to identify the entry (for further `openat` calls relative to it) and
/// to carry the stat snapshot. A symlink entry is never followed: it yields a
/// `Handle` with `kind() == EntryKind::Symlink`.
#[derive(Debug)]
pub struct Handle {
    fd: OwnedFd,
    kind: EntryKind,
    dev: u64,
    ino: u64,
    meta: FileMeta,
}

impl Handle {
    /// The entry's classification (File / Dir / Symlink / Special).
    #[must_use]
    pub fn kind(&self) -> EntryKind {
        self.kind
    }

    /// The device number of the entry.
    #[must_use]
    pub fn dev(&self) -> u64 {
        self.dev
    }

    /// The inode number of the entry.
    #[must_use]
    pub fn ino(&self) -> u64 {
        self.ino
    }

    /// A snapshot of the entry's metadata at the time the handle was opened.
    #[must_use]
    pub fn meta(&self) -> &FileMeta {
        &self.meta
    }

    /// Borrow the underlying file descriptor.
    #[must_use]
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    /// Duplicate this handle, sharing the same pinned inode via a `dup`'d
    /// (`F_DUPFD_CLOEXEC`) `O_PATH` file descriptor and copying the cached
    /// classification + stat snapshot.
    ///
    /// This is a pure fd dup — it opens nothing, follows nothing, and stats
    /// nothing on the filesystem, so it preserves every TOCTOU property of the
    /// original `O_PATH|O_NOFOLLOW` handle (the clone pins the exact same inode and
    /// cannot be redirected by a concurrent rename/symlink swap). It lets a walk
    /// that classifies an entry once hand an owned handle to a deferred (post-order)
    /// step without a second `openat`/`fstatat` on the entry.
    pub fn try_clone(&self) -> std::io::Result<Handle> {
        Ok(Handle {
            fd: self.fd.try_clone()?,
            kind: self.kind,
            dev: self.dev,
            ino: self.ino,
            meta: self.meta.clone(),
        })
    }

    /// Read this symlink's target and metadata from the one pinned `O_PATH` fd: the target via the
    /// empty-path `readlinkat` ([`read_link_handle`]) and the metadata from this handle's `fstat`
    /// snapshot. Both describe the same pinned inode, so they are a faithful pair (the symlink
    /// analogue of [`Dir::open_file_read`]'s `(File, FileMeta)`). Errors if the handle is not a
    /// symlink (the empty-path read requires a symlink fd).
    pub async fn read_symlink(
        &self,
        side: congestion::Side,
    ) -> std::io::Result<(std::path::PathBuf, FileMeta)> {
        let target = read_link_handle(self, side).await?;
        Ok((target, self.meta.clone()))
    }
}

// ── Dir ───────────────────────────────────────────────────────────────────────

/// A directory file descriptor opened `O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`.
///
/// All entry-level operations are relative to this fd, preventing TOCTOU races
/// that path-based lookups are vulnerable to.
///
/// The fd is held behind an `Arc` so per-entry operations can move an owned
/// reference into their `spawn_blocking` closure. `spawn_blocking` tasks are
/// not cancellable: if the surrounding future is dropped (timeout, `fail_early`
/// abort, Ctrl-C) the closure keeps running detached. Cloning the `Arc` (a
/// refcount bump, no syscall) keeps the open file description alive for the
/// closure's full duration even if the originating `Dir` is dropped mid-flight,
/// preserving the `openat` TOCTOU guarantee. Later fd-relative methods
/// (`open_file_read`, `create_file`, `make_dir`, `read_entries`, …) must follow
/// this same clone-Arc-into-closure shape.
#[derive(Debug)]
pub struct Dir {
    fd: Arc<OwnedFd>,
    /// Which filesystem side this directory lives on, for congestion gating.
    side: congestion::Side,
}

impl Dir {
    /// Which filesystem side this directory lives on (for congestion gating).
    #[must_use]
    pub fn side(&self) -> congestion::Side {
        self.side
    }

    /// Open `path` as a directory fd.
    ///
    /// The final component is always opened with `O_NOFOLLOW`. If `dereference`
    /// is `false` and the final component is a symlink, the call fails with
    /// `ELOOP`. If `dereference` is `true` and the final component is a symlink,
    /// the call is retried without `O_NOFOLLOW` so the symlink is followed.
    ///
    /// The parent prefix is resolved normally (it is trusted) — unless strict
    /// operand resolution is armed (`--require-toctou-safe`), in which case the
    /// whole path is resolved `RESOLVE_NO_SYMLINKS` and a symlink in ANY
    /// component fails closed with `ELOOP`.
    pub async fn open_root_dir(
        path: &Path,
        dereference: bool,
        side: congestion::Side,
    ) -> std::io::Result<Dir> {
        let path = path.to_owned();
        // run the blocking openat inside spawn_blocking, gated by the congestion
        // controller, matching the per-metadata-syscall pattern used across the crate.
        run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
            let mode = Mode::empty();
            #[cfg(target_os = "linux")]
            {
                if strict_operand_resolution() {
                    // --require-toctou-safe: `-L` is refused by the linter before strict
                    // mode can arm, so a dereference request here is an internal
                    // inconsistency; fail closed rather than follow a symlink.
                    if dereference {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "--dereference cannot be combined with strict operand resolution",
                        ));
                    }
                    return openat2_no_symlinks(&path, flags)
                        .map(|fd| Dir {
                            fd: Arc::new(fd),
                            side,
                        })
                        .map_err(nix_to_io);
                }
            }
            match openat(AT_FDCWD, &path, flags, mode) {
                Ok(fd) => Ok(Dir {
                    fd: Arc::new(fd),
                    side,
                }),
                Err(nix::errno::Errno::ELOOP) if dereference => {
                    // final component is a symlink; follow it only when dereference=true
                    let follow_flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC;
                    openat(AT_FDCWD, &path, follow_flags, mode)
                        .map(|fd| Dir {
                            fd: Arc::new(fd),
                            side,
                        })
                        .map_err(nix_to_io)
                }
                Err(e) => Err(nix_to_io(e)),
            }
        })
        .await
    }

    /// Open a TRUSTED command-line parent-prefix directory, resolving symlinks
    /// normally (the final component IS followed if it is a symlink).
    ///
    /// The trusted-boundary model (docs/tocttou.md, "Trusted boundary") trusts the directory named on
    /// the command line up to and including itself; only entries strictly BELOW
    /// it are hardened with `O_NOFOLLOW`. The parent prefix that CONTAINS the
    /// operand is therefore resolved like a normal path open — a symlinked parent
    /// (e.g. `rcp file symlink_to_dir/out`, where `symlink_to_dir` is a symlink to
    /// a real directory) must be followed into the real directory, not rejected
    /// with `ELOOP`/`ENOTDIR`.
    ///
    /// This differs from [`Self::open_root_dir`], which `O_NOFOLLOW`s the final
    /// component (the named operand itself) and only follows it when
    /// `dereference` is set. Use `open_parent_dir` for the operand's CONTAINER
    /// directory; use `open_root_dir` for the operand entry. Every descendant
    /// `openat` during the walk still uses `O_NOFOLLOW`, so the hardening below
    /// the named root is unaffected.
    ///
    /// Returns a [`TrustedDir`]: this is the ONLY constructor of that type, so a
    /// symlink-following open can be obtained nowhere else. Crossing into the
    /// hardened tree below the named root is the explicit [`TrustedDir::into_tree`]
    /// step.
    ///
    /// Under strict operand resolution (`--require-toctou-safe`) the prefix must
    /// already be symlink-free: it is resolved `RESOLVE_NO_SYMLINKS`, and a
    /// symlink in any component fails closed with `ELOOP` instead of being
    /// followed. Pass fully-resolved operands (`realpath` output) in that mode.
    pub async fn open_parent_dir(
        path: &Path,
        side: congestion::Side,
    ) -> std::io::Result<TrustedDir> {
        let path = path.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC;
            #[cfg(target_os = "linux")]
            {
                if strict_operand_resolution() {
                    return openat2_no_symlinks(&path, flags)
                        .map(|fd| {
                            TrustedDir(Dir {
                                fd: Arc::new(fd),
                                side,
                            })
                        })
                        .map_err(nix_to_io);
                }
            }
            // a normal directory open: the kernel resolves the whole path following
            // symlinks, including the final (trusted parent) component. No O_NOFOLLOW.
            openat(AT_FDCWD, &path, flags, Mode::empty())
                .map(|fd| {
                    TrustedDir(Dir {
                        fd: Arc::new(fd),
                        side,
                    })
                })
                .map_err(nix_to_io)
        })
        .await
    }

    /// Open a child directory entry by name, refusing to follow symlinks.
    ///
    /// Fails with `ELOOP` if `name` refers to a symlink, or `ENOTDIR` if it
    /// refers to a non-directory entry. The returned `Dir` carries the same
    /// congestion side as `self`.
    pub async fn open_dir(&self, name: &OsStr) -> std::io::Result<Dir> {
        // `O_NOFOLLOW`/`O_PATH` only guard the final path component, so a `name`
        // containing `/` could let openat traverse an intermediate symlink. Reject
        // multi-component names at runtime (debug_assert is compiled out in release).
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        // clone the Arc (refcount bump, no syscall) and move it into the blocking
        // closure so the open file description stays alive for the closure's full
        // duration even if this Dir is dropped mid-flight (spawn_blocking is not
        // cancellable). see the Dir doc comment.
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
            openat(dir.as_fd(), name.as_bytes(), flags, Mode::empty())
                .map(|fd| Dir {
                    fd: Arc::new(fd),
                    side,
                })
                .map_err(nix_to_io)
        })
        .await
    }

    /// Open a child regular file for reading, refusing to follow symlinks and never
    /// blocking on a FIFO. Returns the open file plus its metadata snapshot.
    ///
    /// `O_NONBLOCK` is included so that if an attacker races the directory entry to
    /// a FIFO between `getdents` and this `open`, the open returns immediately
    /// (`O_RDONLY|O_NONBLOCK` on a FIFO never blocks on Linux) rather than blocking
    /// forever waiting for a writer. `O_NOFOLLOW` prevents symlink following but
    /// does not catch FIFOs (they are not symlinks); the subsequent `fstat` +
    /// `S_ISREG` check rejects any non-regular file (FIFO, device, directory) with
    /// `EINVAL`. `O_NONBLOCK` persists on the returned `File`, which is harmless for
    /// regular-file I/O on a local fs.
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, `ELOOP` if
    /// `name` is a symlink, or `EINVAL` (after open, via the `fstat`+`S_ISREG`
    /// check) if the entry is any non-regular type such as a FIFO, device, or
    /// directory.
    ///
    /// This is the canonical regular-file payload+metadata read: the returned `FileMeta` (not the
    /// classify [`Handle`]'s metadata) is what callers must apply/send, so bytes and metadata come
    /// from the same fd (read-side fidelity, see docs/tocttou.md).
    pub async fn open_file_read(&self, name: &OsStr) -> std::io::Result<(std::fs::File, FileMeta)> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            let flags = OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC;
            let fd =
                openat(dir.as_fd(), name.as_bytes(), flags, Mode::empty()).map_err(nix_to_io)?;
            // fstat the open fd to confirm the entry is a regular file; this is the
            // safety check — O_NOFOLLOW does not catch FIFOs or other special files.
            let st = fstat(&fd).map_err(nix_to_io)?;
            if kind_from_stat(&st) != EntryKind::File {
                // fd is dropped here, closing it
                return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
            }
            let meta = FileMeta::from_stat(&st);
            let file = std::fs::File::from(fd);
            Ok((file, meta))
        })
        .await
    }

    /// `fstat` this directory's own held fd, returning its metadata snapshot.
    ///
    /// Lets a caller apply/send a directory's metadata from the SAME fd whose `read_entries`
    /// produced its contents (read-side fidelity, see docs/tocttou.md), rather than from a
    /// separately-opened classify [`Handle`] that a concurrent swap could desync from the
    /// enumerated contents. Gated as `Stat`.
    pub async fn meta(&self) -> std::io::Result<FileMeta> {
        let dir = self.fd.clone();
        let side = self.side;
        run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            let st = fstat(dir.as_fd()).map_err(nix_to_io)?;
            Ok(FileMeta::from_stat(&st))
        })
        .await
    }

    /// Open a child entry by name, classifying it without following symlinks.
    ///
    /// Uses `O_PATH|O_NOFOLLOW`, which yields a valid fd even for symlinks. The
    /// stat is then obtained via `fstatat` with `AT_EMPTY_PATH` on the resulting
    /// fd so the classification is always consistent with the opened entry.
    pub async fn child(&self, name: &OsStr) -> std::io::Result<Handle> {
        // see open_dir: `O_NOFOLLOW`/`O_PATH` only guard the final component, so a
        // `name` containing `/` could traverse an intermediate symlink. Reject
        // multi-component names at runtime (debug_assert is compiled out in release).
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        // clone the Arc (refcount bump, no syscall) and move it into the blocking
        // closure so the open file description stays alive for the closure's full
        // duration even if this Dir is dropped mid-flight (spawn_blocking is not
        // cancellable). see the Dir doc comment.
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            let flags = OFlag::O_PATH | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
            let fd =
                openat(dir.as_fd(), name.as_bytes(), flags, Mode::empty()).map_err(nix_to_io)?;
            // stat the fd itself (empty path + AT_EMPTY_PATH): works for symlinks too
            let st = fstatat(&fd, "", AtFlags::AT_EMPTY_PATH).map_err(nix_to_io)?;
            let kind = kind_from_stat(&st);
            let dev = st.st_dev;
            let ino = st.st_ino;
            let meta = FileMeta::from_stat(&st);
            Ok(Handle {
                fd,
                kind,
                dev,
                ino,
                meta,
            })
        })
        .await
    }

    /// Re-open `name` and confirm it still refers to the same inode as `expected`
    /// (same `dev` + `ino`). Returns the fresh [`Handle`] on match.
    ///
    /// On mismatch — the directory entry was swapped to a different inode between
    /// when `expected` was obtained and this call — returns `ESTALE`. Callers fail
    /// closed: they must not proceed with an operation that assumed a specific identity
    /// for the entry.
    ///
    /// # Soundness
    ///
    /// `expected`'s `O_PATH` fd pins the old inode alive for the duration of the
    /// call: as long as any fd referencing an inode is open, the kernel cannot
    /// recycle that inode number. A matching `(dev, ino)` therefore genuinely
    /// proves the two fds refer to the same inode — there is no window in which
    /// the number could have been reused.
    pub async fn recheck(&self, name: &OsStr, expected: &Handle) -> std::io::Result<Handle> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let fresh = self.child(name).await?;
        if fresh.dev() == expected.dev() && fresh.ino() == expected.ino() {
            Ok(fresh)
        } else {
            Err(std::io::Error::from_raw_os_error(libc::ESTALE))
        }
    }

    /// Create a child directory and return an open `Dir` handle to it (same side as self).
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, or `EEXIST` if
    /// a directory (or any other entry) at `name` already exists.
    ///
    /// This is a two-step operation: `mkdirat` (gated as `MkDir`) to create the
    /// directory, followed by `open_dir` (gated as `Stat`) to open and return it.
    pub async fn make_dir(&self, name: &OsStr, mode: u32) -> std::io::Result<Dir> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let side = self.side;
        let name_owned = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::MkDir, move || {
            mkdirat(
                dir.as_fd(),
                name_owned.as_bytes(),
                Mode::from_bits_truncate(mode),
            )
            .map_err(nix_to_io)
        })
        .await?;
        self.open_dir(name).await
    }

    /// Enumerate the directory's entries (excluding `.` and `..`).
    ///
    /// Returns each entry's name and its `getdents` `d_type` as a best-effort
    /// `EntryKind` hint (`None` when the filesystem reports `DT_UNKNOWN`). The
    /// hint is advisory only — callers MUST confirm type via `child`/`fstat`
    /// before acting (TOCTOU safety).
    ///
    /// This method acquires only the static ops rate gate (not the congestion
    /// probe). Directory enumeration is deliberately not probed because buffered
    /// `getdents` produces bimodal latency (cache hit vs. real kernel call) that
    /// would pollute the congestion controller's baseline — see
    /// `walk::next_entry_probed` for the full rationale.
    pub async fn read_entries(
        &self,
    ) -> std::io::Result<Vec<(std::ffi::OsString, Option<EntryKind>)>> {
        throttle::get_ops_token().await;
        let dir = self.fd.clone();
        tokio::task::spawn_blocking(move || {
            // Dup the fd with FD_CLOEXEC so nix::dir::Dir can consume (and close)
            // it on drop without touching self's Arc<OwnedFd>. A bare dup(2)
            // would clear FD_CLOEXEC; F_DUPFD_CLOEXEC atomically sets it.
            //
            // Re-entrancy: the dup shares the original's open file description,
            // and therefore its directory read offset. Reading to EOF advances
            // that shared offset, so a naive `fdopendir` loop would leave self's
            // fd at EOF and make a *second* read_entries() on the same Dir
            // return an empty listing. nix's borrowing `Iter` (from
            // `nix_dir.iter()`) rewinds the shared description in its `Drop`
            // (rewinddir(3) → offset 0), and that `Drop` runs on BOTH normal
            // completion AND the early `?`-return taken on a mid-iteration error
            // — so the dup is always rewound before it is closed, leaving self's
            // fd at offset 0 either way. This re-entrancy is load-bearing: the
            // hardened remote source enumerates a directory in Pass 1 and again
            // in Pass 2 on the *same* `Arc<Dir>`. (Additionally every caller
            // treats an enumeration error as terminal and never re-enumerates the
            // directory, so a partially-advanced offset is never observed
            // regardless.)
            let dup_raw: RawFd =
                nix::fcntl::fcntl(dir.as_fd(), nix::fcntl::FcntlArg::F_DUPFD_CLOEXEC(0))
                    .map_err(nix_to_io)?;
            // SAFETY: dup_raw is a freshly-dup'd fd that we own exclusively; no
            // other reference to it exists.
            let dup_owned = unsafe { OwnedFd::from_raw_fd(dup_raw) };
            let mut nix_dir = nix::dir::Dir::from_fd(dup_owned).map_err(nix_to_io)?;

            let mut entries = Vec::new();
            for entry_result in nix_dir.iter() {
                let entry = entry_result.map_err(nix_to_io)?;
                let name_cstr = entry.file_name();
                // skip "." and ".."
                if name_cstr == c"." || name_cstr == c".." {
                    continue;
                }
                let name = std::ffi::OsStr::from_bytes(name_cstr.to_bytes()).to_owned();
                let kind = entry.file_type().map(|t| match t {
                    nix::dir::Type::Directory => EntryKind::Dir,
                    nix::dir::Type::Symlink => EntryKind::Symlink,
                    nix::dir::Type::File => EntryKind::File,
                    _ => EntryKind::Special,
                });
                entries.push((name, kind));
            }
            // nix_dir drops here, closing the dup'd fd; self's fd is unaffected
            Ok(entries)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    /// Remove a child non-directory entry by name, gated on this directory's own congestion side.
    ///
    /// For a symlink, this unlinks the link itself — never its target.
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, or `EISDIR`
    /// if `name` refers to a directory.
    pub async fn unlink_at(&self, name: &OsStr) -> std::io::Result<()> {
        self.unlink_at_on(name, self.side).await
    }

    /// Like [`Self::unlink_at`], but gates the `unlinkat` on an explicitly chosen congestion
    /// `side` rather than the directory's own side.
    ///
    /// `rm` reads its tree on the `Source` side (its `Dir` handles are `Source`-sided, matching
    /// the old path-based `symlink_metadata`/`read_dir`), but the destructive `unlinkat` must be
    /// bucketed on `Destination` to match the side the path-based rm used for `remove_file` — so
    /// it competes for the same metadata cwnd as other destructive work. The fd-relative TOCTOU
    /// guarantee is unaffected: the syscall is still resolved against this directory's pinned fd.
    pub(crate) async fn unlink_at_on(
        &self,
        name: &OsStr,
        side: congestion::Side,
    ) -> std::io::Result<()> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let name = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::Unlink, move || {
            unlinkat(dir.as_fd(), name.as_bytes(), UnlinkatFlags::NoRemoveDir).map_err(nix_to_io)
        })
        .await
    }

    /// Remove a child empty directory by name, gated on this directory's own congestion side.
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, `ENOTEMPTY`
    /// if the directory is not empty, or `ENOTDIR` if `name` is not a directory.
    pub async fn rmdir_at(&self, name: &OsStr) -> std::io::Result<()> {
        self.rmdir_at_on(name, self.side).await
    }

    /// Like [`Self::rmdir_at`], but gates the `rmdir` on an explicitly chosen congestion `side`
    /// rather than the directory's own side. See [`Self::unlink_at_on`] for why `rm` needs this
    /// (`Destination`-sided removal from a `Source`-sided read walk).
    pub(crate) async fn rmdir_at_on(
        &self,
        name: &OsStr,
        side: congestion::Side,
    ) -> std::io::Result<()> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let name = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::RmDir, move || {
            unlinkat(dir.as_fd(), name.as_bytes(), UnlinkatFlags::RemoveDir).map_err(nix_to_io)
        })
        .await
    }

    /// Create a symlink `name` → `target` in this directory, returning a
    /// fd-pinned `Handle` to the just-created link.
    ///
    /// The returned handle has `kind() == EntryKind::Symlink` and can be used to
    /// apply metadata to the link race-free. `target` is the link contents — it
    /// is an arbitrary path and is not restricted to a single component.
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, or `EEXIST`
    /// if an entry at `name` already exists.
    pub async fn symlink_at(&self, name: &OsStr, target: &Path) -> std::io::Result<Handle> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        let target = target.to_owned();
        // clone `name` so we can call self.child() after the closure consumes it
        let name_for_child = name.clone();
        run_metadata_probed_blocking(side, congestion::MetadataOp::Symlink, move || {
            // symlinkat(target, dirfd, name): creates `name` → `target`
            symlinkat(target.as_os_str().as_bytes(), dir.as_fd(), name.as_bytes())
                .map_err(nix_to_io)
        })
        .await?;
        // open the just-created link with O_PATH|O_NOFOLLOW so we get a Handle
        // that is pinned to the symlink inode itself (not its target).
        let handle = self.child(&name_for_child).await?;
        if handle.kind() != EntryKind::Symlink {
            // should never happen — we just created a symlink; if it somehow
            // changed underneath us, report ENOENT to signal the caller.
            return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
        }
        Ok(handle)
    }

    /// Read the target of a child symlink.
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, or `EINVAL`
    /// (from `readlinkat`) if `name` is not a symlink.
    pub async fn read_link_at(&self, name: &OsStr) -> std::io::Result<std::path::PathBuf> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::ReadLink, move || {
            readlinkat(dir.as_fd(), name.as_bytes())
                .map(std::path::PathBuf::from)
                .map_err(nix_to_io)
        })
        .await
    }

    /// Create a hard link at `dst`/`dst_name` pointing to this directory's `name`.
    ///
    /// Uses `AtFlags::empty()` (flags=0, no `AT_SYMLINK_FOLLOW`), so if `name` is a
    /// symlink, the link target is the symlink inode itself — the target file
    /// gains no new hard link.
    ///
    /// Fails with `EINVAL` if either `name` or `dst_name` is not a single path
    /// component.
    pub async fn hard_link_at(
        &self,
        name: &OsStr,
        dst: &Dir,
        dst_name: &OsStr,
    ) -> std::io::Result<()> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        if !is_single_component(dst_name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let src_dir = self.fd.clone();
        let dst_dir = dst.fd.clone();
        let side = dst.side;
        let name = name.to_owned();
        let dst_name = dst_name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::HardLink, move || {
            linkat(
                src_dir.as_fd(),
                name.as_bytes(),
                dst_dir.as_fd(),
                dst_name.as_bytes(),
                AtFlags::empty(),
            )
            .map_err(nix_to_io)
        })
        .await
    }

    /// Create a hard link at `self`/`dst_name` pointing to the EXACT inode that
    /// `src_handle` pins — never re-resolving the source by name.
    ///
    /// `self` is the DESTINATION directory. The source is identified solely by
    /// `src_handle`'s `O_PATH` file descriptor: the link is made via
    /// `linkat(AT_FDCWD, "/proc/self/fd/N", dst_fd, dst_name, AT_SYMLINK_FOLLOW)`,
    /// where `N` is the handle's fd. `AT_SYMLINK_FOLLOW` makes `linkat` follow the
    /// `/proc` magic symlink to the handle's pinned inode, so the new hard link
    /// targets that exact inode regardless of any concurrent rename / symlink swap
    /// of the original directory entry.
    ///
    /// # Why /proc and not the source-name `linkat` or `AT_EMPTY_PATH`
    ///
    /// `Dir::hard_link_at` re-resolves the source by `name`, which is a TOCTOU
    /// window: an attacker who controls the source tree can replace `name` with a
    /// different inode (symlink, FIFO, another file) between classification and the
    /// `linkat`, so the link would target the replacement. Linking the pinned fd
    /// closes that window. `linkat(fd, "", .., AT_EMPTY_PATH)` would also be
    /// inode-exact but requires `CAP_DAC_READ_SEARCH`; the `/proc/self/fd` form does
    /// not, mirroring `chmod_via_proc_fd`.
    ///
    /// # Behavior
    ///
    /// - Inode-exact happy path: a stable regular-file handle links exactly as the
    ///   by-name path did (same inode, same content).
    /// - Fail-closed under attack: if the pinned inode's last directory entry was
    ///   removed (link count 0, e.g. the attacker renamed `name` away), the kernel
    ///   refuses to resurrect it and `linkat` fails with `ENOENT`. It never links a
    ///   swapped-in replacement.
    /// - Directories: `linkat` refuses to hard-link a directory (`EPERM`), exactly
    ///   as the by-name path did. Callers must only pass a regular-file handle.
    ///
    /// # Errors
    ///
    /// `EINVAL` if `dst_name` is not a single path component; `ENOENT` if the pinned
    /// inode has no remaining links (fail-closed); `EEXIST` if an entry at
    /// `dst_name` already exists; `EPERM` if the handle refers to a directory.
    /// Requires `/proc` mounted (same precondition as `chmod_via_proc_fd`).
    pub async fn hard_link_handle_at(
        &self,
        src_handle: &Handle,
        dst_name: &OsStr,
    ) -> std::io::Result<()> {
        if !is_single_component(dst_name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        // clone the source O_PATH fd into an owned fd the blocking closure can hold,
        // keeping the pinned inode alive for the syscall's full duration even if the
        // originating Handle is dropped (spawn_blocking is not cancellable).
        let src_owned = src_handle.as_fd().try_clone_to_owned()?;
        let dst_dir = self.fd.clone();
        let side = self.side;
        let dst_name = dst_name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::HardLink, move || {
            let proc_path = format!("/proc/self/fd/{}", src_owned.as_raw_fd());
            // AT_SYMLINK_FOLLOW: the /proc entry is a magic symlink that must be
            // dereferenced to reach the pinned inode (without the flag, linkat would
            // try to hard-link the magic symlink itself, which is not permitted).
            linkat(
                AT_FDCWD,
                proc_path.as_str(),
                dst_dir.as_fd(),
                dst_name.as_bytes(),
                AtFlags::AT_SYMLINK_FOLLOW,
            )
            .map_err(nix_to_io)
        })
        .await
    }

    /// Create a new child file, failing if it already exists and never following a symlink.
    ///
    /// `mode` is the creation mode (subject to umask); exact permissions are set
    /// later via fchmod. Returns the open writable `File` on success.
    ///
    /// `O_EXCL` is the primary guard: combined with `O_CREAT`, it fails with
    /// `EEXIST` on any pre-existing entry — including a symlink — without
    /// following it. `O_NOFOLLOW` is the fallback that would still refuse to
    /// follow a symlink (with `ELOOP`) should `O_EXCL` ever be bypassed.
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, or `EEXIST`
    /// if a file or symlink at `name` already exists.
    pub async fn create_file(&self, name: &OsStr, mode: u32) -> std::io::Result<std::fs::File> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::OpenCreate, move || {
            let flags = OFlag::O_CREAT
                | OFlag::O_EXCL
                | OFlag::O_WRONLY
                | OFlag::O_NOFOLLOW
                | OFlag::O_CLOEXEC;
            let file_mode = Mode::from_bits_truncate(mode);
            openat(dir.as_fd(), name.as_bytes(), flags, file_mode)
                .map(std::fs::File::from)
                .map_err(nix_to_io)
        })
        .await
    }
}

// ── TrustedDir ──────────────────────────────────────────────────────────────────

/// A directory opened by FOLLOWING symlinks normally — the command-line-named
/// path's trusted parent prefix.
///
/// The trusted-boundary model (docs/tocttou.md, "Trusted boundary") trusts the path named on the
/// command line up to and including its container directory; only entries
/// strictly BELOW the named root are hardened with `O_NOFOLLOW`. A `TrustedDir`
/// is that trusted container, and it is the ONLY way in this crate to obtain a
/// directory fd that was opened following symlinks — its sole constructor is
/// [`Dir::open_parent_dir`]. Every other directory open ([`Dir::open_dir`],
/// [`Dir::child`], [`Dir::open_file_read`], [`Dir::create_file`],
/// [`Dir::make_dir`], …) is `O_NOFOLLOW`.
///
/// Because the trusted/hardened distinction is a type rather than a convention,
/// the compiler enforces it: a parent-prefix slot typed `TrustedDir` can only be
/// filled by the follow-open, and a hardened `Dir` cannot be used where a trusted
/// parent is required. Crossing from the trusted prefix into the hardened tree is
/// the single explicit [`Self::into_tree`] step.
///
/// Under strict operand resolution (`--require-toctou-safe`) the "trusted"
/// prefix is additionally required to be symlink-free: the open resolves it
/// `RESOLVE_NO_SYMLINKS`, so a symlink component fails closed with `ELOOP`
/// rather than being followed (see [`enable_strict_operand_resolution`]).
#[derive(Debug)]
pub struct TrustedDir(Dir);

impl TrustedDir {
    /// Cross from the trusted parent prefix into the hardened tree, consuming the `TrustedDir` and
    /// handing back the owned hardened `Dir` (e.g. to wrap it in an `Arc` for the walk). Every open
    /// below the returned `Dir` is `O_NOFOLLOW`, so nothing below the named root can be redirected
    /// by a symlink swap. This is the one explicit trusted→hardened transition.
    #[must_use]
    pub fn into_tree(self) -> Dir {
        self.0
    }
}

// ── Strict operand probes ────────────────────────────────────────────────────
//
// Existence/kind and directory-open probes on an operand path that stay faithful
// to strict operand resolution (`--require-toctou-safe`): they resolve the
// operand's parent prefix with `open_parent_dir` (which is
// `openat2(RESOLVE_NO_SYMLINKS)` while armed) and touch the final component only
// fd-relative, so a symlink in a directory component of the operand path fails
// closed with `ELOOP` instead of being followed by a path-based probe
// (`Path::exists`, `symlink_metadata`, `open_root_dir` on the full path). These
// decompose the path into parent + final component so an INTERMEDIATE-prefix
// symlink (a strict violation → `Err(ELOOP)`) is never conflated with a final
// component that is merely a symlink / non-directory (→ `Ok(None)`). Callers use
// these under `strict_operand_resolution()`; the default path keeps its
// path-based probes unchanged.

/// Split a lexically-normal absolute operand into `(parent, final_component)` for
/// an fd-relative probe. Strict operands are already absolute + normal (the linter
/// enforced it), so a plain `parent()`/`file_name()` split is correct. Returns
/// `None` when the path has no distinct parent+name (e.g. `/`), where there is
/// nothing to probe fd-relative.
fn split_parent_and_name(path: &Path) -> Option<(&Path, &OsStr)> {
    let name = path.file_name()?;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        // a single-component relative path means the current directory
        _ => Path::new("."),
    };
    Some((parent, name))
}

/// Probe an operand's existence and kind fd-relative under strict operand
/// resolution: open its parent with `open_parent_dir` (`RESOLVE_NO_SYMLINKS`
/// while armed) and classify the final component via `child` (`O_NOFOLLOW`).
///
/// - `Ok(Some(kind))` — the entry exists (a final-component symlink counts as
///   existing, classified `Symlink`, and is never followed, matching
///   `symlink_metadata`).
/// - `Ok(None)` — the entry, or its parent, does not exist (`ENOENT`/`ENOTDIR`).
/// - `Err(ELOOP)` — a directory component of the operand path is a symlink; the
///   caller must fail closed.
pub async fn strict_probe_dst_kind(
    path: &Path,
    side: congestion::Side,
) -> std::io::Result<Option<EntryKind>> {
    let Some((parent, name)) = split_parent_and_name(path) else {
        return Ok(None);
    };
    match Dir::open_parent_dir(parent, side).await {
        Ok(parent) => match parent.into_tree().child(name).await {
            Ok(handle) => Ok(Some(handle.kind())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        },
        Err(err)
            if err.kind() == std::io::ErrorKind::NotFound
                || err.raw_os_error() == Some(libc::ENOTDIR) =>
        {
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

// ── fd-based metadata application ───────────────────────────────────────────────
//
// These primitives apply ownership / mode / timestamps to an entry through a
// file descriptor we already hold, rather than re-resolving a path. That closes
// the TOCTOU window a path-based applier would have between opening/creating the
// entry and re-touching it by name (which is why the fd-based appliers replaced
// the path-based ones entirely).
//
// Every applier follows the chown → chmod → utimens ordering: chown first (it
// clears setuid/setgid on regular files), chmod second (restores them), utimens
// last (chown and chmod both touch ctime/mtime). All syscalls are gated through
// `run_metadata_probed_blocking` with `MetadataOp::Chmod`, bucketing
// chown/chmod/utimens together.

/// `fchown` on a real (readable/writable) file descriptor.
///
/// No-op is the caller's responsibility: this always issues the syscall. Pass
/// `None` for a component that must not change.
async fn fchown_fd(
    fd: BorrowedFd<'_>,
    side: congestion::Side,
    uid: Option<u32>,
    gid: Option<u32>,
) -> std::io::Result<()> {
    // BorrowedFd is not 'static, so dup it into an owned fd the closure can hold.
    let owned = fd.try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::Chmod, move || {
        fchown(
            owned.as_fd(),
            uid.map(Uid::from_raw),
            gid.map(Gid::from_raw),
        )
        .map_err(nix_to_io)
    })
    .await
}

/// `fchmod` on a real file descriptor. `mode` is masked to the permission bits
/// (`0o7777`); file-type bits, if present, are dropped by `from_bits_truncate`.
///
/// `fd` must be a real (not `O_PATH`) descriptor — `fchmod` returns `EBADF` on an
/// `O_PATH` fd. This is used by the copy path, which holds the destination's own
/// writable file / directory fd. For an `O_PATH` [`Handle`] (e.g. rchm's classified
/// entry), use [`chmod_via_proc_fd`] instead.
async fn fchmod_fd(fd: BorrowedFd<'_>, side: congestion::Side, mode: u32) -> std::io::Result<()> {
    let owned = fd.try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::Chmod, move || {
        fchmod(owned.as_fd(), Mode::from_bits_truncate(mode)).map_err(nix_to_io)
    })
    .await
}

/// `futimens` on a real file descriptor.
async fn futimens_fd(
    fd: BorrowedFd<'_>,
    side: congestion::Side,
    atime: i64,
    atime_nsec: i64,
    mtime: i64,
    mtime_nsec: i64,
) -> std::io::Result<()> {
    let owned = fd.try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::Chmod, move || {
        let atime_spec = TimeSpec::new(atime, atime_nsec);
        let mtime_spec = TimeSpec::new(mtime, mtime_nsec);
        futimens(owned.as_fd(), &atime_spec, &mtime_spec).map_err(nix_to_io)
    })
    .await
}

/// Inode-exact `fchownat` on any [`Handle`]'s `O_PATH` fd, operating on the entry
/// the fd points at — file, directory, or symlink — never following a symlink.
///
/// Uses `AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW` so the empty pathname resolves to
/// the fd's own pinned inode: no path re-resolution by `name` happens, so a
/// concurrent rename/symlink-swap of the directory entry cannot redirect the
/// chown to a different target. `AT_SYMLINK_NOFOLLOW` makes a symlink `Handle`
/// chown the link itself rather than its target. Pass `None` for a component
/// that must not change (the caller decides when to issue the syscall at all).
pub(crate) async fn fchown_handle(
    handle: &Handle,
    side: congestion::Side,
    uid: Option<u32>,
    gid: Option<u32>,
) -> std::io::Result<()> {
    let owned = handle.as_fd().try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::Chmod, move || {
        fchownat(
            owned.as_fd(),
            "",
            uid.map(Uid::from_raw),
            gid.map(Gid::from_raw),
            AtFlags::AT_EMPTY_PATH | AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(nix_to_io)
    })
    .await
}

/// `chmod` any non-symlink entry (file, directory, or special) through an `O_PATH`
/// [`Handle`] by going via the `/proc/self/fd/N` magic symlink, changing the mode
/// of the EXACT inode the handle pins — never re-resolving the entry by name.
///
/// (Symlink mode bits are not settable on Linux, so callers never invoke this on a
/// symlink handle.)
///
/// # Why /proc and not `fchmod`/`fchmodat`
///
/// The `Handle` fd is `O_PATH`, which is the only way to pin an arbitrary entry's
/// inode without read/write/search rights on it. But `O_PATH` rules out the
/// obvious chmod paths:
///
/// - `fchmod(fd, mode)` returns `EBADF` on an `O_PATH` fd (it requires a real
///   open file description).
/// - `fchmodat(dirfd, name, mode, AT_SYMLINK_NOFOLLOW)` re-resolves `name`
///   relative to a directory fd — that re-resolution is exactly the TOCTOU window
///   we are closing, and the `AT_SYMLINK_NOFOLLOW` flag is only honored on Linux
///   6.6+ for `fchmodat` (older kernels reject it with `ENOTSUP`).
///
/// `chmod("/proc/self/fd/N", mode)` follows the kernel's per-fd magic symlink,
/// which resolves to the open file description's pinned inode regardless of what
/// the original `name` now refers to. Because the `O_PATH` handle keeps that
/// inode alive (the kernel cannot recycle an inode with an open reference), this
/// is inode-exact and immune to a concurrent rename/symlink swap. It also works
/// regardless of the file's own permission bits — e.g. a non-root owner's
/// `0000`-mode file — because the operation authorizes against the caller's
/// ownership, not the path's mode, and needs no traversal/read rights on the
/// target. (`fchmodat(.., FollowSymlink)` on the magic symlink is used because
/// the magic link must be dereferenced to reach the pinned inode.)
///
/// # Precondition
///
/// Requires `/proc` to be mounted (the standard Linux default). Without `/proc`
/// the call fails with `ENOENT`; this is a documented operational precondition of
/// the fd-based chmod path.
pub(crate) async fn chmod_via_proc_fd(
    handle: &Handle,
    side: congestion::Side,
    mode: u32,
) -> std::io::Result<()> {
    // clone the O_PATH fd into an owned fd the blocking closure can hold, keeping
    // the pinned inode alive for the syscall's full duration even if the
    // originating Handle is dropped (spawn_blocking is not cancellable).
    let owned = handle.as_fd().try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::Chmod, move || {
        let proc_path = format!("/proc/self/fd/{}", owned.as_raw_fd());
        // FollowSymlink: the /proc entry is a magic symlink that must be
        // dereferenced to reach the pinned inode (NoFollowSymlink would chmod the
        // magic link itself, a silent no-op).
        nix::sys::stat::fchmodat(
            AT_FDCWD,
            proc_path.as_str(),
            Mode::from_bits_truncate(mode),
            nix::sys::stat::FchmodatFlags::FollowSymlink,
        )
        .map_err(nix_to_io)
    })
    .await
}

/// Synchronous, ungated chmod of the EXACT inode an `O_PATH` `fd` pins, via its
/// `/proc/self/fd/N` magic symlink — the blocking-`Drop` counterpart of
/// [`chmod_via_proc_fd`].
///
/// `fd` must reference an `O_PATH` handle (the only fd kind `rm`'s relax path
/// holds). The same inode-exactness argument applies: the open fd keeps the
/// pinned inode alive, so `/proc/self/fd/N` resolves to that inode regardless of
/// any concurrent rename/symlink swap of the original name — there is no path
/// re-resolution to redirect, so this is race-safe even on a directory whose own
/// mode is being restored. Works on a `0000`-mode directory we own (it authorizes
/// against ownership, not the path's mode).
///
/// This is deliberately not gated through the congestion controller or the
/// blocking pool: it runs from a synchronous `Drop` (which cannot `.await`) as a
/// one-shot best-effort cleanup — a single `fchmodat` whose cost is negligible.
/// Requires `/proc` mounted (same precondition as [`chmod_via_proc_fd`]).
pub(crate) fn chmod_via_proc_fd_sync(fd: BorrowedFd<'_>, mode: u32) -> std::io::Result<()> {
    let proc_path = format!("/proc/self/fd/{}", fd.as_raw_fd());
    // FollowSymlink: the /proc entry is a magic symlink that must be dereferenced
    // to reach the pinned inode (NoFollowSymlink would chmod the magic link
    // itself, a silent no-op).
    nix::sys::stat::fchmodat(
        AT_FDCWD,
        proc_path.as_str(),
        Mode::from_bits_truncate(mode),
        nix::sys::stat::FchmodatFlags::FollowSymlink,
    )
    .map_err(nix_to_io)
}

/// Read full [`std::fs::Metadata`] for the exact inode an `O_PATH` [`Handle`] pins,
/// via the `/proc/self/fd/N` magic symlink.
///
/// The fd-pinned [`FileMeta`] snapshot ([`Handle::meta`]) covers uid/gid/mode and
/// the a/m/ctime timestamps, but NOT the birth time (`btime`) — `fstat` does not
/// return it. Callers that need `Metadata::created()` (the `--created-before`
/// time filter) get it here while staying inode-exact: the open `O_PATH` handle
/// keeps the inode alive, so resolving `/proc/self/fd/N` lands on that same inode
/// regardless of a concurrent rename/symlink swap of the original name. Gated as
/// `Stat`. Requires `/proc` mounted (same precondition as [`chmod_via_proc_fd`]).
pub(crate) async fn stat_meta_via_proc_fd(
    handle: &Handle,
    side: congestion::Side,
) -> std::io::Result<std::fs::Metadata> {
    let owned = handle.as_fd().try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
        let proc_path = format!("/proc/self/fd/{}", owned.as_raw_fd());
        std::fs::metadata(proc_path)
    })
    .await
}

/// Read the target of a symlink [`Handle`] inode-exact, via `readlinkat(fd, "")` on the pinned
/// `O_PATH | O_NOFOLLOW` fd.
///
/// The empty-pathname form of `readlinkat` (Linux 2.6.39+) operates on the symlink the fd itself
/// refers to, so the target comes from the *same* pinned inode as [`Handle::meta`] — there is no
/// path re-resolution by name that a concurrent same-name swap could redirect. This is the symlink
/// analogue of reading a regular file's bytes and metadata from one [`Dir::open_file_read`] fd: it
/// lets a caller send/apply a symlink's target and metadata as a faithful pair. Fails if the handle
/// does not refer to a symlink (the empty-path form requires a symlink fd); callers only invoke it
/// on a `Symlink`-classified handle. Gated as `ReadLink`.
///
/// Raw `libc::readlinkat` is required: nix's wrapper rejects the empty pathname that selects the
/// fd's own link (the same reason `symlink_utimes_fd` uses raw `utimensat`).
pub async fn read_link_handle(
    handle: &Handle,
    side: congestion::Side,
) -> std::io::Result<std::path::PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    let owned = handle.as_fd().try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::ReadLink, move || {
        // a symlink target is bounded by PATH_MAX, so a single buffer of that size never truncates.
        let mut buf = vec![0u8; libc::PATH_MAX as usize];
        // SAFETY: `owned` is a valid open fd for the duration of this call; the empty C string
        // selects the fd's own symlink (it was opened O_PATH|O_NOFOLLOW); `buf` has `len()` bytes.
        let n = unsafe {
            libc::readlinkat(
                owned.as_raw_fd(),
                c"".as_ptr(),
                buf.as_mut_ptr().cast::<libc::c_char>(),
                buf.len(),
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        buf.truncate(n as usize);
        Ok(std::path::PathBuf::from(std::ffi::OsString::from_vec(buf)))
    })
    .await
}

/// Set timestamps on a symlink `Handle`'s `O_PATH` fd, operating on the link
/// itself, via a raw `utimensat(fd, "", times, AT_EMPTY_PATH)`.
///
/// Raw libc is required here: nix's `utimensat` wrapper cannot pass
/// `AT_EMPTY_PATH`, and `futimens` on an `O_PATH` fd returns `EBADF`. The
/// `/proc/self/fd` form silently no-ops under `NOFOLLOW`, so it must not be used.
async fn symlink_utimes_fd(
    handle: &Handle,
    side: congestion::Side,
    atime: i64,
    atime_nsec: i64,
    mtime: i64,
    mtime_nsec: i64,
) -> std::io::Result<()> {
    let owned = handle.as_fd().try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::Chmod, move || {
        let times: [libc::timespec; 2] = [
            libc::timespec {
                tv_sec: atime,
                tv_nsec: atime_nsec,
            },
            libc::timespec {
                tv_sec: mtime,
                tv_nsec: mtime_nsec,
            },
        ];
        // SAFETY: `owned` is a valid open fd for the duration of this call; the
        // pathname is the empty C string and `times` points to a 2-element array.
        let res = unsafe {
            libc::utimensat(
                owned.as_raw_fd(),
                c"".as_ptr(),
                times.as_ptr(),
                libc::AT_EMPTY_PATH,
            )
        };
        if res == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    })
    .await
}

/// Apply file metadata (owner, mode, timestamps) to an already-open writable
/// file descriptor, following the chown → chmod → utimens ordering.
///
/// `fd` must be the destination file's own fd (typically the write fd returned
/// by [`Dir::create_file`]); this avoids the redundant `File::open` re-open a
/// path-based applier would need, and closes the TOCTOU window in the process.
/// Gating on `settings.file`: chown only when uid or gid is requested, chmod
/// always (the masked
/// mode honors `mode_mask`), timestamps only when requested.
pub async fn set_file_metadata_fd<Meta: crate::preserve::Metadata>(
    settings: &crate::preserve::Settings,
    meta: &Meta,
    fd: BorrowedFd<'_>,
    side: congestion::Side,
) -> std::io::Result<()> {
    let ut = &settings.file.user_and_time;
    if ut.uid || ut.gid {
        let uid = if ut.uid { Some(meta.uid()) } else { None };
        let gid = if ut.gid { Some(meta.gid()) } else { None };
        fchown_fd(fd, side, uid, gid).await?;
    }
    let mode = crate::preserve::masked_mode(settings.file.mode_mask, meta);
    fchmod_fd(fd, side, mode).await?;
    if ut.time {
        futimens_fd(
            fd,
            side,
            meta.atime(),
            meta.atime_nsec(),
            meta.mtime(),
            meta.mtime_nsec(),
        )
        .await?;
    }
    Ok(())
}

/// Apply directory metadata (owner, mode, timestamps) to an open [`Dir`] fd,
/// following the chown → chmod → utimens ordering. Gates on `settings.dir` and
/// uses the directory's own congestion side.
pub async fn set_dir_metadata_fd<Meta: crate::preserve::Metadata>(
    settings: &crate::preserve::Settings,
    meta: &Meta,
    dir: &Dir,
) -> std::io::Result<()> {
    let side = dir.side();
    let fd = dir.fd.as_fd();
    let ut = &settings.dir.user_and_time;
    if ut.uid || ut.gid {
        let uid = if ut.uid { Some(meta.uid()) } else { None };
        let gid = if ut.gid { Some(meta.gid()) } else { None };
        fchown_fd(fd, side, uid, gid).await?;
    }
    let mode = crate::preserve::masked_mode(settings.dir.mode_mask, meta);
    fchmod_fd(fd, side, mode).await?;
    if ut.time {
        futimens_fd(
            fd,
            side,
            meta.atime(),
            meta.atime_nsec(),
            meta.mtime(),
            meta.mtime_nsec(),
        )
        .await?;
    }
    Ok(())
}

/// Apply symlink metadata (owner and timestamps only — never mode) to a symlink
/// [`Handle`], operating on the link itself via `AT_EMPTY_PATH`.
///
/// Symlinks have no meaningful permission bits, so there is no chmod step;
/// ordering is chown → utimens. Gates on `settings.symlink`.
pub async fn set_symlink_metadata_fd<Meta: crate::preserve::Metadata>(
    settings: &crate::preserve::Settings,
    meta: &Meta,
    handle: &Handle,
    side: congestion::Side,
) -> std::io::Result<()> {
    let ut = &settings.symlink.user_and_time;
    if ut.uid || ut.gid {
        let uid = if ut.uid { Some(meta.uid()) } else { None };
        let gid = if ut.gid { Some(meta.gid()) } else { None };
        // chown the link itself: fchown_handle already operates inode-exact on the O_PATH handle
        // via AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW.
        fchown_handle(handle, side, uid, gid).await?;
    }
    if ut.time {
        symlink_utimes_fd(
            handle,
            side,
            meta.atime(),
            meta.atime_nsec(),
            meta.mtime(),
            meta.mtime_nsec(),
        )
        .await?;
    }
    Ok(())
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Run a blocking metadata syscall closure on the blocking pool, gated by the
/// congestion controller for the given side and operation kind.
///
/// Wraps `spawn_blocking` inside [`crate::walk::run_metadata_probed`] so each
/// per-entry `openat`/`fstatat` is rate-gated, counted against the cwnd permit,
/// and feeds the latency probe — the same per-metadata-syscall gating shape used
/// throughout this crate.
async fn run_metadata_probed_blocking<F, T>(
    side: congestion::Side,
    op: congestion::MetadataOp,
    f: F,
) -> std::io::Result<T>
where
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    crate::walk::run_metadata_probed(side, op, async {
        tokio::task::spawn_blocking(f)
            .await
            .map_err(std::io::Error::other)?
    })
    .await
}

/// Convert a `nix::errno::Errno` to `std::io::Error`.
fn nix_to_io(e: nix::errno::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(e as i32)
}

/// Return `true` when `name` is a single non-empty path component (no `/`,
/// not `.` or `..`).
fn is_single_component(name: &OsStr) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    !name.as_bytes().contains(&b'/')
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preserve::Metadata;
    use crate::testutils;
    use std::io::Read;

    #[tokio::test]
    async fn child_classifies_file_dir_symlink_and_rejects_nofollow() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        // setup_test_dir() returns the temp dir; the fixture lives at tmp/foo/
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        assert_eq!(
            root.child(OsStr::new("0.txt")).await?.kind(),
            EntryKind::File
        );
        assert_eq!(root.child(OsStr::new("bar")).await?.kind(), EntryKind::Dir);
        tokio::fs::symlink("0.txt", tmp.join("foo/lnk")).await?;
        assert_eq!(
            root.child(OsStr::new("lnk")).await?.kind(),
            EntryKind::Symlink
        );
        // open_dir on a symlinked "dir" must fail closed (ELOOP/ENOTDIR), never follow
        tokio::fs::symlink("/etc", tmp.join("foo/evil")).await?;
        assert!(root.open_dir(OsStr::new("evil")).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn open_dir_succeeds_on_real_directory() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        // bar is a real directory; open_dir must succeed and yield a usable Dir
        let bar = root.open_dir(OsStr::new("bar")).await?;
        // and the resulting Dir is functional: it can classify its own children
        assert_eq!(
            bar.child(OsStr::new("1.txt")).await?.kind(),
            EntryKind::File
        );
        Ok(())
    }

    // FIX A (PR #247 review): `open_parent_dir` resolves a TRUSTED command-line parent prefix
    // following symlinks (the final component IS followed), while `open_root_dir` keeps the operand
    // entry `O_NOFOLLOW` and descendants stay hardened. This pins the parent-prefix-vs-operand
    // distinction and proves the hardening below the followed prefix is unchanged.
    #[tokio::test]
    async fn open_parent_dir_follows_symlinked_prefix_but_descendants_stay_hardened()
    -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        // a symlink-to-dir standing in for a trusted parent prefix component.
        tokio::fs::symlink("foo", tmp.join("foo_link")).await?;
        // open_parent_dir FOLLOWS the symlinked final component into the real `foo` directory,
        // yielding a TrustedDir; into_tree() crosses into the hardened tree below it.
        let parent = Dir::open_parent_dir(&tmp.join("foo_link"), congestion::Side::Source).await?;
        let tree = parent.into_tree();
        // the followed dir is functional: it sees `foo`'s real children.
        assert_eq!(
            tree.child(OsStr::new("0.txt")).await?.kind(),
            EntryKind::File
        );
        // open_root_dir on the SAME symlinked path (dereference=false) must instead fail closed —
        // it `O_NOFOLLOW`s the final component (the operand-entry contract), proving the two entry
        // points differ exactly at the final-component follow decision.
        assert!(
            Dir::open_root_dir(&tmp.join("foo_link"), false, congestion::Side::Source)
                .await
                .is_err()
        );
        // hardening below the followed prefix is UNCHANGED: a symlinked child reached via the
        // followed parent still fails closed (O_NOFOLLOW) rather than being followed.
        tokio::fs::symlink("/etc", tmp.join("foo/evil_below")).await?;
        assert!(tree.open_dir(OsStr::new("evil_below")).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_multi_component_names() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        // names with a path separator could traverse an intermediate symlink, so
        // they are rejected with EINVAL before any syscall (release-safe check)
        for bad in ["bar/1.txt", "..", ".", ""] {
            let child_err = root.child(OsStr::new(bad)).await.unwrap_err();
            assert_eq!(child_err.raw_os_error(), Some(libc::EINVAL));
            let dir_err = root.open_dir(OsStr::new(bad)).await.unwrap_err();
            assert_eq!(dir_err.raw_os_error(), Some(libc::EINVAL));
            let file_err = root.open_file_read(OsStr::new(bad)).await.unwrap_err();
            assert_eq!(file_err.raw_os_error(), Some(libc::EINVAL));
            let create_err = root.create_file(OsStr::new(bad), 0o644).await.unwrap_err();
            assert_eq!(create_err.raw_os_error(), Some(libc::EINVAL));
        }
        Ok(())
    }

    // Regression for the spawn_blocking cancellation soundness bug: the Dir's fd
    // lives behind an Arc that each operation clones into its closure, so an op
    // stays sound even after the originating Dir is dropped. We model the
    // detached-closure case by cloning a Dir, dropping the original, and
    // confirming the clone still opens children correctly.
    #[tokio::test]
    async fn operations_remain_valid_after_original_dir_dropped() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        // clone the underlying Arc-held fd into a second Dir, then drop the original
        let shared = Dir {
            fd: root.fd.clone(),
            side: root.side,
        };
        drop(root);
        // the shared handle's open file description is still alive; ops succeed
        assert_eq!(
            shared.child(OsStr::new("0.txt")).await?.kind(),
            EntryKind::File
        );
        let bar = shared.open_dir(OsStr::new("bar")).await?;
        assert_eq!(
            bar.child(OsStr::new("2.txt")).await?.kind(),
            EntryKind::File
        );
        Ok(())
    }

    // open_file_read: verify that a regular file can be opened, metadata size is
    // correct, and the returned File is readable.
    #[tokio::test]
    async fn open_file_read_reads_regular_file() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let (mut file, meta) = root.open_file_read(OsStr::new("0.txt")).await?;
        // "0.txt" contains the single byte "0"
        assert_eq!(meta.size(), 1);
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "0");
        Ok(())
    }

    // open_file_read: a FIFO must not cause open to block (O_NONBLOCK) AND the
    // S_ISREG check must reject it, so the call returns Err without hanging.
    #[tokio::test]
    async fn open_file_read_rejects_fifo_without_blocking() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let fifo_path = tmp.join("foo/test.fifo");
        nix::unistd::mkfifo(
            &fifo_path,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )?;
        // the call must return (not block) within the timeout, and must be an Err
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            root.open_file_read(OsStr::new("test.fifo")),
        )
        .await;
        assert!(result.is_ok(), "open_file_read blocked on FIFO (timed out)");
        assert!(
            result.unwrap().is_err(),
            "open_file_read must reject a FIFO"
        );
        Ok(())
    }

    // open_file_read: a symlink must be rejected (ELOOP from O_NOFOLLOW).
    #[tokio::test]
    async fn open_file_read_rejects_symlink() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        // create a symlink pointing to a real file
        tokio::fs::symlink("0.txt", tmp.join("foo/link_to_0")).await?;
        let result = root.open_file_read(OsStr::new("link_to_0")).await;
        assert!(result.is_err(), "open_file_read must reject a symlink");
        Ok(())
    }

    // create_file: successfully creates a new writable file.
    #[tokio::test]
    async fn create_file_creates_new_writable_file() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        // use a dest-side dir for the write target
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let mut file = root.create_file(OsStr::new("new.txt"), 0o644).await?;
        use std::io::Write;
        file.write_all(b"hello safedir")?;
        drop(file);
        // re-open via std and verify the content
        let content = std::fs::read(tmp.join("foo/new.txt"))?;
        assert_eq!(content, b"hello safedir");
        Ok(())
    }

    // create_file: fails with EEXIST when the file already exists.
    #[tokio::test]
    async fn create_file_fails_if_exists() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // "0.txt" already exists in the fixture
        let err = root
            .create_file(OsStr::new("0.txt"), 0o644)
            .await
            .unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EEXIST),
            "expected EEXIST, got {err:#}"
        );
        Ok(())
    }

    // make_dir: creates the directory and returns a usable Dir handle.
    #[tokio::test]
    async fn make_dir_creates_and_returns_usable_dir() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let sub = root.make_dir(OsStr::new("sub"), 0o755).await?;
        // the returned Dir must be usable: create a file inside it
        sub.create_file(OsStr::new("child.txt"), 0o644).await?;
        // and read_entries on the sub dir must show that file
        let entries = sub.read_entries().await?;
        let names: Vec<_> = entries
            .iter()
            .map(|(n, _)| n.to_string_lossy().into_owned())
            .collect();
        assert!(
            names.contains(&"child.txt".to_string()),
            "child.txt not found in {names:?}"
        );
        Ok(())
    }

    // make_dir: multi-component names must be rejected with EINVAL.
    #[tokio::test]
    async fn make_dir_rejects_multi_component_names() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        for bad in ["a/b", "..", ".", ""] {
            let err = root.make_dir(OsStr::new(bad), 0o755).await.unwrap_err();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINVAL),
                "expected EINVAL for {:?}, got {err:#}",
                bad
            );
        }
        Ok(())
    }

    // read_entries: returns all entries with correct d_type hints.
    #[tokio::test]
    async fn read_entries_lists_children_with_dtype_hints() -> anyhow::Result<()> {
        use std::collections::HashMap;
        let tmp = testutils::setup_test_dir().await?;
        // baz contains: 4.txt (file), 5.txt (symlink), 6.txt (symlink)
        // use bar which has only files; instead build a custom fixture in foo
        let fixture = tmp.join("foo/fixture_dir");
        tokio::fs::create_dir(&fixture).await?;
        tokio::fs::write(fixture.join("afile.txt"), "x").await?;
        tokio::fs::create_dir(fixture.join("asubdir")).await?;
        tokio::fs::symlink("afile.txt", fixture.join("alink")).await?;

        let root = Dir::open_root_dir(&fixture, false, congestion::Side::Source).await?;
        let entries = root.read_entries().await?;
        let map: HashMap<String, Option<EntryKind>> = entries
            .into_iter()
            .map(|(n, k)| (n.to_string_lossy().into_owned(), k))
            .collect();

        assert_eq!(map.len(), 3, "expected 3 entries, got {map:?}");
        assert_eq!(
            map.get("afile.txt"),
            Some(&Some(EntryKind::File)),
            "afile.txt wrong"
        );
        assert_eq!(
            map.get("asubdir"),
            Some(&Some(EntryKind::Dir)),
            "asubdir wrong"
        );
        assert_eq!(
            map.get("alink"),
            Some(&Some(EntryKind::Symlink)),
            "alink wrong"
        );
        Ok(())
    }

    // read_entries: calling it twice on the same Dir must succeed (fd not consumed).
    #[tokio::test]
    async fn read_entries_does_not_close_self_fd() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        // first call
        let first = root.read_entries().await?;
        assert!(!first.is_empty(), "first read_entries returned empty");
        // second call on the SAME Dir must yield the identical entry set, not
        // just an equal count. read_entries dups a fd that shares the directory
        // read offset, so absent nix's rewinddir-on-completion this second call
        // would see an empty (or partial) listing. The hardened remote source
        // depends on exactly this re-entrancy (Pass 1 then Pass 2 enumerate the
        // same Arc<Dir>).
        let second = root.read_entries().await?;
        let mut first_names: Vec<_> = first.iter().map(|(name, _)| name.clone()).collect();
        let mut second_names: Vec<_> = second.iter().map(|(name, _)| name.clone()).collect();
        first_names.sort();
        second_names.sort();
        assert_eq!(
            first_names, second_names,
            "second read_entries differs from first"
        );
        // also prove child() still works on the same Dir
        root.child(OsStr::new("0.txt")).await?;
        Ok(())
    }

    // create_file: refuses to follow or clobber an existing symlink.
    #[tokio::test]
    async fn create_file_refuses_existing_symlink() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // plant a symlink pointing at a non-existent target
        let link_path = tmp.join("foo/evil_link");
        let target_path = tmp.join("foo/should_not_be_created");
        tokio::fs::symlink(&target_path, &link_path).await?;
        // create_file must fail, not follow the symlink and create the target
        let err = root
            .create_file(OsStr::new("evil_link"), 0o644)
            .await
            .unwrap_err();
        // O_CREAT|O_EXCL returns EEXIST on an existing symlink without following it
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EEXIST),
            "expected EEXIST, got {err:#}"
        );
        // the symlink target must NOT have been created
        assert!(
            !target_path.exists(),
            "symlink target was unexpectedly created"
        );
        Ok(())
    }

    // unlink_at: removes a regular file and confirms it is gone.
    #[tokio::test]
    async fn unlink_at_removes_file() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // "0.txt" exists in the fixture
        root.unlink_at(OsStr::new("0.txt")).await?;
        // afterwards child() must fail with ENOENT
        let err = root.child(OsStr::new("0.txt")).await.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ENOENT),
            "expected ENOENT after unlink, got {err:#}"
        );
        Ok(())
    }

    // unlink_at: removes the symlink itself, not its target.
    #[tokio::test]
    async fn unlink_at_on_symlink_removes_link_not_target() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // create a sentinel file with content, then symlink to it
        tokio::fs::write(tmp.join("foo/sentinel.txt"), b"alive").await?;
        tokio::fs::symlink("sentinel.txt", tmp.join("foo/lnk")).await?;
        // unlink the link
        root.unlink_at(OsStr::new("lnk")).await?;
        // link is gone
        let err = root.child(OsStr::new("lnk")).await.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ENOENT),
            "expected ENOENT for removed link, got {err:#}"
        );
        // sentinel target still exists with content
        let content = tokio::fs::read(tmp.join("foo/sentinel.txt")).await?;
        assert_eq!(content, b"alive", "sentinel.txt was unexpectedly removed");
        Ok(())
    }

    // rmdir_at: removes an empty directory; rejects non-empty (ENOTEMPTY) and a
    // regular file (ENOTDIR).
    #[tokio::test]
    async fn rmdir_at_removes_empty_dir_and_rejects_nonempty() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // create an empty subdirectory and remove it
        tokio::fs::create_dir(tmp.join("foo/empty_sub")).await?;
        root.rmdir_at(OsStr::new("empty_sub")).await?;
        let err = root.child(OsStr::new("empty_sub")).await.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ENOENT),
            "expected ENOENT after rmdir, got {err:#}"
        );
        // "bar" is non-empty in the fixture → ENOTEMPTY
        let err = root.rmdir_at(OsStr::new("bar")).await.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ENOTEMPTY),
            "expected ENOTEMPTY for non-empty dir, got {err:#}"
        );
        // "0.txt" is a regular file → ENOTDIR
        let err = root.rmdir_at(OsStr::new("0.txt")).await.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ENOTDIR),
            "expected ENOTDIR for regular file, got {err:#}"
        );
        Ok(())
    }

    // symlink_at: creates a symlink and returns a Handle with kind Symlink;
    // read_link_at then returns the original target path.
    #[tokio::test]
    async fn symlink_at_creates_link_and_returns_pinned_handle() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let target = std::path::Path::new("some/arbitrary/target");
        let handle = root.symlink_at(OsStr::new("mylink"), target).await?;
        assert_eq!(
            handle.kind(),
            EntryKind::Symlink,
            "symlink_at must return a Symlink handle"
        );
        // read_link_at must return the same target
        let read_back = root.read_link_at(OsStr::new("mylink")).await?;
        assert_eq!(
            read_back, target,
            "read_link_at returned wrong target: {read_back:?}"
        );
        Ok(())
    }

    // read_link_handle reads the target inode-exact from the pinned O_PATH symlink handle (the
    // empty-path readlinkat form), so the target pairs with `handle.meta()` from the SAME fd. A
    // non-symlink handle is rejected (EINVAL).
    #[tokio::test]
    async fn read_link_handle_reads_target_from_pinned_handle() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let target = std::path::Path::new("some/arbitrary/target");
        tokio::fs::symlink(target, tmp.join("foo/mylink")).await?;
        // classify the link, then read its target through that same pinned handle.
        let handle = root.child(OsStr::new("mylink")).await?;
        assert_eq!(handle.kind(), EntryKind::Symlink);
        let read_back = read_link_handle(&handle, congestion::Side::Source).await?;
        assert_eq!(read_back, target, "wrong target: {read_back:?}");
        // a non-symlink handle (a regular file) is rejected (the empty-path readlinkat form requires
        // a symlink fd; the kernel returns an error rather than a target). Callers only ever invoke
        // this on a Symlink-classified handle, so this is the defensive path.
        let file_handle = root.child(OsStr::new("0.txt")).await?;
        assert!(
            read_link_handle(&file_handle, congestion::Side::Source)
                .await
                .is_err(),
            "read_link_handle on a non-symlink must fail"
        );
        Ok(())
    }

    // Handle::read_symlink returns target AND metadata from the one pinned O_PATH fd, so they are a
    // faithful pair (the symlink analogue of open_file_read).
    #[tokio::test]
    async fn read_symlink_returns_target_and_meta_from_one_handle() -> anyhow::Result<()> {
        use crate::preserve::Metadata as _;
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let target = std::path::Path::new("some/target");
        tokio::fs::symlink(target, tmp.join("foo/lnk")).await?;
        let handle = root.child(OsStr::new("lnk")).await?;
        let (read_target, meta) = handle.read_symlink(congestion::Side::Source).await?;
        assert_eq!(read_target, target);
        // metadata is the symlink's own, from the same handle.
        assert_eq!(meta.uid(), handle.meta().uid());
        assert_eq!(meta.mtime(), handle.meta().mtime());
        Ok(())
    }

    // Dir::meta fstats the directory's own held fd (the fd whose contents we enumerate).
    #[tokio::test]
    async fn dir_meta_returns_opened_dir_fstat() -> anyhow::Result<()> {
        use crate::preserve::Metadata as _;
        let tmp = testutils::setup_test_dir().await?;
        let bar = Dir::open_root_dir(&tmp.join("foo/bar"), false, congestion::Side::Source).await?;
        let meta = bar.meta().await?;
        let std_meta = std::fs::metadata(tmp.join("foo/bar"))?;
        // `meta.uid()` resolves via preserve::Metadata (the only trait FileMeta implements);
        // fully-qualify the std::fs::Metadata side, which implements both that trait and MetadataExt.
        assert_eq!(meta.uid(), std::os::unix::fs::MetadataExt::uid(&std_meta));
        assert_eq!(meta.gid(), std::os::unix::fs::MetadataExt::gid(&std_meta));
        Ok(())
    }

    // hard_link_at: creates a hard link sharing the same inode.
    #[tokio::test]
    async fn hard_link_at_creates_hardlink() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        // use two subdirs as src and dst Dir handles
        tokio::fs::create_dir(tmp.join("foo/src_sub")).await?;
        tokio::fs::create_dir(tmp.join("foo/dst_sub")).await?;
        tokio::fs::write(tmp.join("foo/src_sub/orig.txt"), b"hardlink test").await?;

        let src =
            Dir::open_root_dir(&tmp.join("foo/src_sub"), false, congestion::Side::Source).await?;
        let dst = Dir::open_root_dir(
            &tmp.join("foo/dst_sub"),
            false,
            congestion::Side::Destination,
        )
        .await?;

        src.hard_link_at(OsStr::new("orig.txt"), &dst, OsStr::new("link.txt"))
            .await?;

        // both handles must exist and share the same inode
        let orig_handle = src.child(OsStr::new("orig.txt")).await?;
        let link_handle = dst.child(OsStr::new("link.txt")).await?;
        assert_eq!(orig_handle.kind(), EntryKind::File, "orig must be a file");
        assert_eq!(link_handle.kind(), EntryKind::File, "link must be a file");
        assert_eq!(
            orig_handle.ino(),
            link_handle.ino(),
            "hard link must share the inode"
        );
        Ok(())
    }

    // hard_link_at: when the source name is a symlink, linkat with flags=0 does
    // NOT follow it — it links the symlink inode itself, so the new entry is also
    // a symlink.
    #[tokio::test]
    async fn hard_link_at_does_not_follow_source_symlink() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        tokio::fs::create_dir(tmp.join("foo/src_hl")).await?;
        tokio::fs::create_dir(tmp.join("foo/dst_hl")).await?;
        // create a real file and a symlink to it in src_hl
        tokio::fs::write(tmp.join("foo/src_hl/real.txt"), b"target").await?;
        tokio::fs::symlink("real.txt", tmp.join("foo/src_hl/sym.txt")).await?;

        let src =
            Dir::open_root_dir(&tmp.join("foo/src_hl"), false, congestion::Side::Source).await?;
        let dst = Dir::open_root_dir(
            &tmp.join("foo/dst_hl"),
            false,
            congestion::Side::Destination,
        )
        .await?;

        // Linux does not allow hard-linking a symlink without AT_EMPTY_PATH or
        // special capabilities; linkat with flags=0 on a symlink yields EPERM.
        // Verify that the call does NOT silently follow the symlink into real.txt.
        let result = src
            .hard_link_at(OsStr::new("sym.txt"), &dst, OsStr::new("new_link.txt"))
            .await;
        match result {
            Ok(()) => {
                // If it succeeded (some kernels/configs allow it), the new entry
                // must be a symlink — NOT a hard link to the underlying file.
                let new_handle = dst.child(OsStr::new("new_link.txt")).await?;
                assert_eq!(
                    new_handle.kind(),
                    EntryKind::Symlink,
                    "hard_link_at must link the symlink itself, not its target"
                );
                // and real.txt must still have link-count 1 (no new hard link)
                let real_meta = std::fs::metadata(tmp.join("foo/src_hl/real.txt"))?;
                use std::os::unix::fs::MetadataExt;
                assert_eq!(
                    real_meta.nlink(),
                    1,
                    "real.txt must not gain a new hard link"
                );
            }
            Err(ref e) if e.raw_os_error() == Some(libc::EPERM) => {
                // expected on most Linux configurations; the important thing is
                // that it did NOT follow the symlink and link real.txt.
                // real.txt must still have exactly 1 hard link.
                let real_meta = std::fs::metadata(tmp.join("foo/src_hl/real.txt"))?;
                use std::os::unix::fs::MetadataExt;
                assert_eq!(
                    real_meta.nlink(),
                    1,
                    "real.txt must not gain a new hard link"
                );
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "unexpected error from hard_link_at on symlink: {e:#}"
                ));
            }
        }
        Ok(())
    }

    // hard_link_handle_at (FIX 2, PR #247 review): links the EXACT inode the
    // classified Handle pins, immune to a concurrent swap of the source name. This is
    // deterministic — the swap happens (in fixed order) AFTER classification but
    // BEFORE the link — so it directly demonstrates the TOCTOU fix.
    //
    // The decoy is a DIFFERENT regular file with different content placed at the same
    // name. The old by-name `hard_link_at(name, ..)` re-resolves `name` and would link
    // the decoy inode (this test would fail against it). `hard_link_handle_at` links
    // the pinned original inode regardless.
    #[tokio::test]
    async fn hard_link_handle_at_links_pinned_inode_after_name_swap() -> anyhow::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let tmp = testutils::create_temp_dir().await?;
        tokio::fs::create_dir(tmp.join("src")).await?;
        tokio::fs::create_dir(tmp.join("dst")).await?;
        tokio::fs::write(tmp.join("src/entry"), b"ORIGINAL").await?;
        let orig_ino = tokio::fs::metadata(tmp.join("src/entry")).await?.ino();

        let src = Dir::open_root_dir(&tmp.join("src"), false, congestion::Side::Source).await?;
        let dst =
            Dir::open_root_dir(&tmp.join("dst"), false, congestion::Side::Destination).await?;
        // classify `entry` — pins the ORIGINAL regular-file inode via O_PATH.
        let handle = src.child(OsStr::new("entry")).await?;
        assert_eq!(handle.kind(), EntryKind::File);

        // SWAP `entry` to a DIFFERENT regular file (the decoy) before linking. We keep
        // the original inode alive only through `handle` (its directory entry is gone),
        // mimicking an attacker renaming a decoy over the source name.
        tokio::fs::write(tmp.join("src/decoy"), b"DECOY_SECRET").await?;
        tokio::fs::rename(tmp.join("src/decoy"), tmp.join("src/entry")).await?;
        let decoy_ino = tokio::fs::metadata(tmp.join("src/entry")).await?.ino();
        assert_ne!(orig_ino, decoy_ino, "decoy must be a different inode");

        // inode-exact link: either links the ORIGINAL pinned inode, or fails closed.
        match dst.hard_link_handle_at(&handle, OsStr::new("linked")).await {
            Ok(()) => {
                let lm = tokio::fs::symlink_metadata(tmp.join("dst/linked")).await?;
                assert!(
                    lm.file_type().is_file(),
                    "linked entry must be a regular file"
                );
                assert_eq!(
                    lm.ino(),
                    orig_ino,
                    "hard_link_handle_at must link the PINNED original inode, never the \
                     swapped-in decoy (the by-name link would have linked the decoy here)"
                );
                let content = tokio::fs::read_to_string(tmp.join("dst/linked")).await?;
                assert_eq!(
                    content, "ORIGINAL",
                    "must reflect the original inode's content"
                );
                assert_ne!(content, "DECOY_SECRET");
            }
            Err(e) => {
                // fail-closed is acceptable (e.g. the pinned inode's last link was
                // already gone). It must NEVER have linked the decoy.
                assert!(
                    !tmp.join("dst/linked").exists(),
                    "no destination entry may exist when the link failed closed (got {e:#})"
                );
            }
        }
        Ok(())
    }

    // hard_link_handle_at must refuse to hard-link a DIRECTORY (linkat returns EPERM),
    // matching the by-name path — a hard link to a directory is never created.
    #[tokio::test]
    async fn hard_link_handle_at_refuses_directory() -> anyhow::Result<()> {
        let tmp = testutils::create_temp_dir().await?;
        tokio::fs::create_dir(tmp.join("src")).await?;
        tokio::fs::create_dir(tmp.join("dst")).await?;
        tokio::fs::create_dir(tmp.join("src/adir")).await?;
        let src = Dir::open_root_dir(&tmp.join("src"), false, congestion::Side::Source).await?;
        let dst =
            Dir::open_root_dir(&tmp.join("dst"), false, congestion::Side::Destination).await?;
        let dir_handle = src.child(OsStr::new("adir")).await?;
        assert_eq!(dir_handle.kind(), EntryKind::Dir);
        let result = dst
            .hard_link_handle_at(&dir_handle, OsStr::new("linked_dir"))
            .await;
        assert!(
            result.is_err(),
            "hard_link_handle_at must refuse to hard-link a directory"
        );
        assert!(
            !tmp.join("dst/linked_dir").exists(),
            "no destination entry may be created for a directory hard link"
        );
        Ok(())
    }

    // hard_link_handle_at (FIX 2, PR #247 review): classify a regular File, then swap the
    // source name to a FIFO (a special, a different kind AND inode) before linking. The
    // old by-name `linkat(flags=0)` re-resolves the name and would hard-link the FIFO —
    // surfacing a special at the destination that rlink would report as a hard-linked
    // file (specials CAN be hard-linked, unlike directories). The inode-exact link must
    // instead link the pinned regular file or fail closed: the destination must NEVER be
    // a special. Deterministic (swap happens between classify and link).
    #[tokio::test]
    async fn hard_link_handle_at_never_links_swapped_in_fifo() -> anyhow::Result<()> {
        use std::os::unix::fs::FileTypeExt;
        let tmp = testutils::create_temp_dir().await?;
        tokio::fs::create_dir(tmp.join("src")).await?;
        tokio::fs::create_dir(tmp.join("dst")).await?;
        tokio::fs::write(tmp.join("src/entry"), b"REALFILE").await?;
        let src = Dir::open_root_dir(&tmp.join("src"), false, congestion::Side::Source).await?;
        let dst =
            Dir::open_root_dir(&tmp.join("dst"), false, congestion::Side::Destination).await?;
        // classify `entry` — pins the regular-file inode.
        let handle = src.child(OsStr::new("entry")).await?;
        assert_eq!(handle.kind(), EntryKind::File);
        // swap `entry` to a FIFO (keep the regular inode alive only via the handle).
        tokio::fs::remove_file(tmp.join("src/entry")).await?;
        nix::unistd::mkfifo(
            &tmp.join("src/entry"),
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )?;
        match dst.hard_link_handle_at(&handle, OsStr::new("linked")).await {
            Ok(()) => {
                let lm = tokio::fs::symlink_metadata(tmp.join("dst/linked")).await?;
                assert!(
                    lm.file_type().is_file(),
                    "linked entry must be the pinned regular file, never the swapped-in FIFO"
                );
                assert!(
                    !lm.file_type().is_fifo(),
                    "the destination must never be a special (the by-name link would link the FIFO)"
                );
                let content = tokio::fs::read_to_string(tmp.join("dst/linked")).await?;
                assert_eq!(content, "REALFILE");
            }
            Err(_) => {
                // fail-closed is acceptable; nothing may be left at the destination.
                assert!(
                    !tmp.join("dst/linked").exists(),
                    "no destination entry may exist when the link failed closed"
                );
            }
        }
        Ok(())
    }

    // hard_link_handle_at on a STABLE regular file links exactly like the by-name path
    // did (same inode, same content) — the happy path is unchanged.
    #[tokio::test]
    async fn hard_link_handle_at_stable_file_happy_path() -> anyhow::Result<()> {
        let tmp = testutils::create_temp_dir().await?;
        tokio::fs::create_dir(tmp.join("src")).await?;
        tokio::fs::create_dir(tmp.join("dst")).await?;
        tokio::fs::write(tmp.join("src/f"), b"STABLE").await?;
        let src = Dir::open_root_dir(&tmp.join("src"), false, congestion::Side::Source).await?;
        let dst =
            Dir::open_root_dir(&tmp.join("dst"), false, congestion::Side::Destination).await?;
        let handle = src.child(OsStr::new("f")).await?;
        dst.hard_link_handle_at(&handle, OsStr::new("f_link"))
            .await?;
        let orig = src.child(OsStr::new("f")).await?;
        let linked = dst.child(OsStr::new("f_link")).await?;
        assert_eq!(linked.kind(), EntryKind::File);
        assert_eq!(orig.ino(), linked.ino(), "hard link must share the inode");
        let content = tokio::fs::read_to_string(tmp.join("dst/f_link")).await?;
        assert_eq!(content, "STABLE");
        Ok(())
    }

    // ── fd-based metadata application ───────────────────────────────────────

    // set_file_metadata_fd: applying owner/mode/time from a source FileMeta to an
    // already-open destination fd must reflect on the destination file: masked
    // mode, mtime, and (where testable) uid/gid all match the source.
    #[tokio::test]
    async fn set_file_metadata_fd_applies_owner_mode_time() -> anyhow::Result<()> {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let tmp = testutils::setup_test_dir().await?;
        // source file with a distinctive mode and a known, old mtime
        let src_path = tmp.join("foo/src_meta.txt");
        tokio::fs::write(&src_path, b"source").await?;
        std::fs::set_permissions(&src_path, std::fs::Permissions::from_mode(0o741))?;
        let src_mtime = filetime::FileTime::from_unix_time(1_000_000_000, 123_456_789);
        filetime::set_file_mtime(&src_path, src_mtime)?;
        filetime::set_file_atime(
            &src_path,
            filetime::FileTime::from_unix_time(1_000_000_500, 0),
        )?;

        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // snapshot the source metadata via a Handle (the realistic source flow)
        let src_handle = root.child(OsStr::new("src_meta.txt")).await?;
        let src_meta = src_handle.meta().clone();

        // create the destination file and write some content into it
        let mut dst_file = root.create_file(OsStr::new("dst_meta.txt"), 0o600).await?;
        dst_file.write_all(b"destination")?;
        dst_file.flush()?;

        // apply source metadata to the already-open dst fd; preserve everything
        let settings = crate::preserve::preserve_all();
        set_file_metadata_fd(
            &settings,
            &src_meta,
            dst_file.as_fd(),
            congestion::Side::Destination,
        )
        .await?;
        drop(dst_file);

        // re-stat the destination and assert mode (masked to 0o7777), mtime
        let dst_md = std::fs::metadata(tmp.join("foo/dst_meta.txt"))?;
        assert_eq!(
            dst_md.permissions().mode() & 0o7777,
            0o741,
            "destination mode mismatch"
        );
        // disambiguate: both preserve::Metadata and std MetadataExt are in scope
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            MetadataExt::mtime(&dst_md),
            1_000_000_000,
            "mtime seconds mismatch"
        );
        assert_eq!(
            MetadataExt::mtime_nsec(&dst_md),
            123_456_789,
            "mtime nanos mismatch"
        );
        // uid/gid: chown to source's uid/gid (same as current user here) must hold
        assert_eq!(MetadataExt::uid(&dst_md), src_meta.uid(), "uid mismatch");
        assert_eq!(MetadataExt::gid(&dst_md), src_meta.gid(), "gid mismatch");
        Ok(())
    }

    // set_file_metadata_fd: the chown → chmod ordering must preserve a setuid bit.
    // An unprivileged fchown (even to the current uid) clears setuid/setgid; doing
    // chown FIRST and chmod AFTER restores it. This test proves that ordering.
    #[tokio::test]
    async fn set_file_metadata_fd_ordering_preserves_setuid() -> anyhow::Result<()> {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let tmp = testutils::setup_test_dir().await?;
        // source file with the setuid bit set (0o4755)
        let src_path = tmp.join("foo/setuid_src");
        tokio::fs::write(&src_path, b"x").await?;
        std::fs::set_permissions(&src_path, std::fs::Permissions::from_mode(0o4755))?;

        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_handle = root.child(OsStr::new("setuid_src")).await?;
        let src_meta = src_handle.meta().clone();
        assert_eq!(
            src_meta.permissions().mode() & 0o7777,
            0o4755,
            "source setuid bit was not set up correctly"
        );

        // destination starts without the setuid bit
        let mut dst_file = root.create_file(OsStr::new("setuid_dst"), 0o600).await?;
        dst_file.write_all(b"x")?;
        dst_file.flush()?;

        // preserve_all keeps the full mode (mask 0o7777) AND preserves uid/gid, so
        // the chown runs before the chmod; the setuid bit must survive.
        let settings = crate::preserve::preserve_all();
        set_file_metadata_fd(
            &settings,
            &src_meta,
            dst_file.as_fd(),
            congestion::Side::Destination,
        )
        .await?;
        drop(dst_file);

        let dst_md = std::fs::metadata(tmp.join("foo/setuid_dst"))?;
        assert_eq!(
            dst_md.permissions().mode() & 0o7777,
            0o4755,
            "setuid bit was lost — chown must run before chmod"
        );
        Ok(())
    }

    // set_dir_metadata_fd: applying mode/time to a freshly made directory via its
    // Dir fd must reflect on the directory.
    #[tokio::test]
    async fn set_dir_metadata_fd_applies() -> anyhow::Result<()> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let tmp = testutils::setup_test_dir().await?;
        // source directory with a distinctive mode and known mtime
        let src_dir_path = tmp.join("foo/src_dir");
        tokio::fs::create_dir(&src_dir_path).await?;
        std::fs::set_permissions(&src_dir_path, std::fs::Permissions::from_mode(0o2750))?;
        filetime::set_file_mtime(
            &src_dir_path,
            filetime::FileTime::from_unix_time(1_111_111_111, 222_000_000),
        )?;

        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_handle = root.child(OsStr::new("src_dir")).await?;
        let src_meta = src_handle.meta().clone();

        // create the destination directory and apply metadata via its Dir fd
        let dst_dir = root.make_dir(OsStr::new("dst_dir"), 0o700).await?;
        let settings = crate::preserve::preserve_all();
        set_dir_metadata_fd(&settings, &src_meta, &dst_dir).await?;

        let dst_md = std::fs::metadata(tmp.join("foo/dst_dir"))?;
        assert_eq!(
            dst_md.permissions().mode() & 0o7777,
            0o2750,
            "destination dir mode mismatch"
        );
        assert_eq!(
            MetadataExt::mtime(&dst_md),
            1_111_111_111,
            "dir mtime seconds mismatch"
        );
        assert_eq!(
            MetadataExt::mtime_nsec(&dst_md),
            222_000_000,
            "dir mtime nanos mismatch"
        );
        Ok(())
    }

    // set_symlink_metadata_fd: applying time (and owner) to a symlink via its
    // O_PATH Handle must change the LINK's own atime/mtime — NOT the target's
    // mtime. This is the key proof that utimensat(AT_EMPTY_PATH) hit the link.
    #[tokio::test]
    async fn set_symlink_metadata_fd_changes_link_not_target() -> anyhow::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;

        // sentinel target file with a known mtime we must NOT disturb
        let target_path = tmp.join("foo/sentinel_target.txt");
        tokio::fs::write(&target_path, b"keep my mtime").await?;
        let target_mtime = filetime::FileTime::from_unix_time(1_500_000_000, 0);
        filetime::set_file_mtime(&target_path, target_mtime)?;
        let target_before = std::fs::metadata(&target_path)?;

        // the link to apply metadata to
        let link = root
            .symlink_at(
                OsStr::new("the_link"),
                std::path::Path::new("sentinel_target.txt"),
            )
            .await?;

        // desired link timestamps come from a source FileMeta; build one by
        // stating a second symlink we set up with a distinctive mtime.
        let src_link_path = tmp.join("foo/src_link");
        tokio::fs::symlink("sentinel_target.txt", &src_link_path).await?;
        let src_link_mtime = filetime::FileTime::from_unix_time(1_234_567_890, 0);
        // set the LINK's own mtime (symlink=true) — not the target's
        filetime::set_symlink_file_times(
            &src_link_path,
            filetime::FileTime::from_unix_time(1_234_500_000, 0),
            src_link_mtime,
        )?;
        let src_meta = root.child(OsStr::new("src_link")).await?.meta().clone();

        let settings = crate::preserve::preserve_all();
        set_symlink_metadata_fd(&settings, &src_meta, &link, congestion::Side::Destination).await?;

        // the LINK's own mtime must now equal the source link's mtime
        let link_md = std::fs::symlink_metadata(tmp.join("foo/the_link"))?;
        assert_eq!(
            MetadataExt::mtime(&link_md),
            1_234_567_890,
            "link's own mtime was not applied"
        );
        // the TARGET file's mtime must be UNCHANGED
        let target_after = std::fs::metadata(&target_path)?;
        assert_eq!(
            MetadataExt::mtime(&target_after),
            MetadataExt::mtime(&target_before),
            "target mtime changed — utimensat followed the symlink!"
        );
        assert_eq!(
            MetadataExt::mtime_nsec(&target_after),
            MetadataExt::mtime_nsec(&target_before),
            "target mtime_nsec changed — utimensat followed the symlink!"
        );
        Ok(())
    }

    // recheck: returns a fresh Handle with the same dev/ino when the entry is unchanged.
    #[tokio::test]
    async fn recheck_succeeds_when_unchanged() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let h = root.child(OsStr::new("0.txt")).await?;
        let fresh = root.recheck(OsStr::new("0.txt"), &h).await?;
        assert_eq!(
            fresh.dev(),
            h.dev(),
            "recheck: dev mismatch on unchanged entry"
        );
        assert_eq!(
            fresh.ino(),
            h.ino(),
            "recheck: ino mismatch on unchanged entry"
        );
        Ok(())
    }

    // recheck: returns ESTALE when the entry's inode has been replaced.
    #[tokio::test]
    async fn recheck_fails_when_swapped_to_different_inode() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // create a file whose Handle we will hold
        tokio::fs::write(tmp.join("foo/f"), b"original").await?;
        let h = root.child(OsStr::new("f")).await?;
        let original_ino = h.ino();
        // replace f with a completely new file (different inode)
        root.unlink_at(OsStr::new("f")).await?;
        root.create_file(OsStr::new("f"), 0o644).await?;
        // verify the replacement has a different inode
        let fresh_via_child = root.child(OsStr::new("f")).await?;
        assert_ne!(
            fresh_via_child.ino(),
            original_ino,
            "test setup error: new file has same inode as old one"
        );
        // recheck must detect the swap and return ESTALE
        let err = root.recheck(OsStr::new("f"), &h).await.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ESTALE),
            "expected ESTALE on inode swap, got {err:#}"
        );
        Ok(())
    }

    // recheck: returns ESTALE when the entry has been swapped to a symlink.
    #[tokio::test]
    async fn recheck_fails_when_swapped_to_symlink() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // create a regular file g
        tokio::fs::write(tmp.join("foo/g"), b"regular").await?;
        let h = root.child(OsStr::new("g")).await?;
        // replace g with a symlink (different inode and kind)
        root.unlink_at(OsStr::new("g")).await?;
        root.symlink_at(OsStr::new("g"), std::path::Path::new("0.txt"))
            .await?;
        // recheck must detect the mismatch (different inode) and return ESTALE
        let err = root.recheck(OsStr::new("g"), &h).await.unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ESTALE),
            "expected ESTALE on symlink swap, got {err:#}"
        );
        Ok(())
    }

    // rejects_multi_component_names: extend to cover the five new methods.
    #[tokio::test]
    async fn new_methods_reject_multi_component_names() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // a second Dir for hard_link_at's dst parameter
        let dst =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // a valid Handle to satisfy recheck's `expected` parameter; the bad `name`
        // must be rejected before any dev/ino comparison is attempted.
        let any_handle = root.child(OsStr::new("0.txt")).await?;

        for bad in ["a/b", "..", ".", ""] {
            let bad_os = OsStr::new(bad);

            let err = root.unlink_at(bad_os).await.unwrap_err();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINVAL),
                "unlink_at: expected EINVAL for {bad:?}, got {err:#}"
            );

            let err = root.rmdir_at(bad_os).await.unwrap_err();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINVAL),
                "rmdir_at: expected EINVAL for {bad:?}, got {err:#}"
            );

            // symlink_at: only `name` is guarded; target is arbitrary
            let err = root
                .symlink_at(bad_os, std::path::Path::new("irrelevant"))
                .await
                .unwrap_err();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINVAL),
                "symlink_at(name): expected EINVAL for {bad:?}, got {err:#}"
            );

            let err = root.read_link_at(bad_os).await.unwrap_err();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINVAL),
                "read_link_at: expected EINVAL for {bad:?}, got {err:#}"
            );

            // hard_link_at: both `name` and `dst_name` are guarded
            let err = root
                .hard_link_at(bad_os, &dst, OsStr::new("good"))
                .await
                .unwrap_err();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINVAL),
                "hard_link_at(name): expected EINVAL for {bad:?}, got {err:#}"
            );

            let err = root
                .hard_link_at(OsStr::new("good"), &dst, bad_os)
                .await
                .unwrap_err();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINVAL),
                "hard_link_at(dst_name): expected EINVAL for {bad:?}, got {err:#}"
            );

            // recheck: bad `name` must be rejected before dev/ino comparison
            let err = root.recheck(bad_os, &any_handle).await.unwrap_err();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINVAL),
                "recheck(name): expected EINVAL for {bad:?}, got {err:#}"
            );
        }
        Ok(())
    }

    // chmod_via_proc_fd: changing the mode of a 0000-mode file through its O_PATH
    // handle must succeed (the /proc magic-symlink path does not need any rights on
    // the target itself) and the new mode must be observable on disk. This is the
    // case `fchmod` (EBADF on O_PATH) and a bare path chmod under restrictive modes
    // would struggle with; the pinned inode makes it inode-exact and permission-free.
    #[tokio::test]
    async fn chmod_via_proc_fd_changes_mode_of_zero_mode_file() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // a file with no permission bits at all (0000).
        let path = tmp.join("foo/locked.txt");
        tokio::fs::write(&path, b"locked").await?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000))?;
        // O_PATH handle pins the inode even though the file is 0000.
        let handle = root.child(OsStr::new("locked.txt")).await?;
        assert_eq!(handle.kind(), EntryKind::File, "fixture must be a file");
        // chmod it to 0o640 via the /proc magic symlink.
        chmod_via_proc_fd(&handle, congestion::Side::Destination, 0o640).await?;
        // the mode change must be visible on disk.
        let md = std::fs::symlink_metadata(&path)?;
        assert_eq!(
            md.permissions().mode() & 0o7777,
            0o640,
            "chmod_via_proc_fd must change the mode of a 0000-mode file"
        );
        Ok(())
    }

    // stat_meta_via_proc_fd: on a symlink Handle (opened O_PATH|O_NOFOLLOW), resolving
    // /proc/self/fd/N must land on the LINK's own inode — never the target's. This is
    // load-bearing for symlink time-filtering (rm/rrm reads a symlink's own mtime/btime to
    // decide removal). We give the link and its target DISTINCT mtimes and assert the metadata
    // returned is the link's (is_symlink + the link's mtime), proving the magic-symlink resolve
    // is pinned to the O_PATH inode and does not follow the link to the target.
    #[tokio::test]
    async fn stat_meta_via_proc_fd_on_symlink_resolves_link_not_target() -> anyhow::Result<()> {
        use std::os::unix::fs::MetadataExt;
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;

        // target file with one mtime ...
        let target_path = tmp.join("foo/stat_target.txt");
        tokio::fs::write(&target_path, b"target body").await?;
        filetime::set_file_mtime(
            &target_path,
            filetime::FileTime::from_unix_time(1_700_000_000, 0),
        )?;

        // ... and a symlink to it with a DISTINCT mtime set on the LINK itself (not the target).
        let link_path = tmp.join("foo/stat_link");
        tokio::fs::symlink("stat_target.txt", &link_path).await?;
        filetime::set_symlink_file_times(
            &link_path,
            filetime::FileTime::from_unix_time(1_600_000_000, 0),
            filetime::FileTime::from_unix_time(1_600_000_123, 0),
        )?;

        // open the symlink via child() — an O_PATH|O_NOFOLLOW handle pinned to the link inode.
        let handle = root.child(OsStr::new("stat_link")).await?;
        assert_eq!(
            handle.kind(),
            EntryKind::Symlink,
            "fixture must classify as a symlink"
        );

        let md = stat_meta_via_proc_fd(&handle, congestion::Side::Source).await?;
        // the returned metadata must be the LINK's, not the dereferenced target's.
        assert!(
            md.file_type().is_symlink(),
            "stat_meta_via_proc_fd followed the symlink to its target (got a non-symlink)"
        );
        assert_eq!(
            MetadataExt::mtime(&md),
            1_600_000_123,
            "expected the LINK's own mtime; a target-following stat would return 1_700_000_000"
        );
        Ok(())
    }

    // NOTE: the test that ARMS strict operand resolution and exercises the strict
    // (openat2) open path lives in tests/strict_resolution.rs — its own integration
    // binary and therefore its own process. The switch is one-way, and under the
    // plain `cargo test` harness (used by the nix checkPhase) a lib's unit tests
    // share one process, so arming here would leak into the symlink-following
    // default-behavior tests above.

    #[test]
    fn openat2_probe_is_stable() {
        // the probe is memoized; both calls must agree. On kernels without openat2 a
        // `false` result is the correct answer (the linter then refuses strict mode),
        // so no hard assertion on availability itself.
        let first = openat2_available();
        assert_eq!(first, openat2_available(), "probe must be stable");
        if !first {
            eprintln!("this kernel lacks openat2(2); strict-mode tests skip themselves");
        }
    }
}
