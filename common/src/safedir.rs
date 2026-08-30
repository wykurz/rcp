//! Safe, race-resistant directory traversal primitives.
//!
//! This module provides `O_NOFOLLOW`-based directory and file handle types that
//! contain path-redirection TOCTOU races by using file-descriptor-relative
//! syscalls (`openat`, `fstatat`) rather than multi-component path lookups. Every
//! `open_dir`/`child` call refuses to follow symlinks, so an attacker who races a
//! directory walk cannot redirect operations outside the intended tree.

use std::ffi::{CStr, OsStr};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

use nix::fcntl::{AT_FDCWD, AtFlags, OFlag, openat};
use nix::sys::stat::{FileStat, Mode, fchmod, fstat, fstatat, futimens, mkdirat};
use nix::sys::time::TimeSpec;
use nix::unistd::{Gid, Uid, UnlinkatFlags, fchown, fchownat, linkat, symlinkat, unlinkat};

use crate::walk::EntryKind;

// ── Destination creation modes ───────────────────────────────────────────────

/// The mode a destination FILE is created with, before it has any contents.
///
/// Owner-only. The source mode is applied by [`set_file_metadata_fd`] once the last byte has been
/// written, so the file is never visible to anyone but the copier while it is being filled in —
/// the file counterpart of the directory split-chmod (see [`DST_DIR_CREATE_MODE`]).
///
/// Creating at the *final* mode instead would publish the destination before its contents exist,
/// giving a half-written file the audience its finished form was meant to have — at the default
/// `0o0777` mask, a world-readable source yields a world-readable destination from creation onward.
/// The sharper case needs the special bits preserved: a root copier holds `CAP_FSETID`, so writing
/// does not clear `S_ISUID`, and the destination carries `setuid` root while the source's owner is
/// still authoring its contents. That is not an exec window while the copy *runs* — our own write
/// descriptor is open, and `execve` refuses a file any process holds open for writing with
/// `ETXTBSY`. If the copier dies, closing that descriptor drops the protection; creation at the
/// final mode would leave a finished-looking setuid binary (see `docs/tocttou.md`). Withholding the
/// owner execute bit is deliberate too — a half-written executable should not be executable.
///
/// This is a constant rather than a [`Dir::create_file`] parameter so that no call site, present or
/// future, can create a destination file at a wide mode. Like any creation mode it is subject to
/// the umask, which can only narrow it further.
pub const DST_FILE_CREATE_MODE: u32 = 0o600;

/// The mode a destination DIRECTORY is created with, before it has any children.
///
/// Owner-only, plus the execute bit the copier needs to populate it. The source mode is applied
/// after every child has been written — by `CopyVisitor::dir_post` locally, and on directory
/// completion remotely.
pub const DST_DIR_CREATE_MODE: u32 = 0o700;

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

tokio::task_local! {
    /// The weak admission reference inherited by fd-owning blocking metadata work in this task.
    static FD_ADMISSION: throttle::FdAdmission;
}

/// Run `future` with a weak admission reference available to safedir blocking work.
///
/// Tokio cannot cancel a `spawn_blocking` closure after it starts. Without this scope, cancelling
/// the async waiter drops its operation guard even though the detached closure may still own a
/// duplicated directory or entry fd. [`run_metadata_probed_blocking`] upgrades the reference for
/// each such closure, keeping the original admission slot occupied until that fd-owning owner
/// finishes. The ambient reference is deliberately weak: dropping the concrete guard before
/// directory recursion still releases capacity even while this scope remains on the stack.
/// Tokio task locals are not inherited by `tokio::spawn`; every spawned entry worker must install
/// its own scope after it receives admission.
pub async fn with_fd_admission<F>(admission: throttle::FdAdmission, future: F) -> F::Output
where
    F: std::future::Future,
{
    FD_ADMISSION.scope(admission, future).await
}

/// Run `future` under `admission` when this traversal uses an fd-admission pool.
pub(crate) async fn with_optional_fd_admission<F>(
    admission: Option<throttle::FdAdmission>,
    future: F,
) -> F::Output
where
    F: std::future::Future,
{
    match admission {
        Some(admission) => with_fd_admission(admission, future).await,
        None => future.await,
    }
}

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
    dev: u64,
    ino: u64,
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
            dev: st.st_dev,
            ino: st.st_ino,
        }
    }

    /// The device number of the filesystem this entry lives on.
    #[must_use]
    pub fn dev(&self) -> u64 {
        self.dev
    }

    /// The entry's inode number.
    #[must_use]
    pub fn ino(&self) -> u64 {
        self.ino
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

/// The classification and size needed to remove an occupied destination slot.
///
/// Unlike [`Handle`], this snapshot does not keep an inode pinned. Overwrite removal operates by
/// name through a pinned parent directory, so retaining the entry fd cannot bind the later by-name
/// removal to that inode. Converting to this snapshot closes the classification fd while preserving
/// the planning data used for syscall dispatch and direct-leaf accounting. A compatible concurrent
/// replacement may be removed using this classification. Direct-leaf counters describe this
/// snapshot rather than the replacement; recursive directory fallback freshly admits and accounts
/// each entry it encounters.
#[derive(Debug)]
pub struct RemovalSnapshot {
    kind: EntryKind,
    size: u64,
}

impl RemovalSnapshot {
    /// The entry classification captured during overwrite planning.
    #[must_use]
    pub(crate) fn kind(&self) -> EntryKind {
        self.kind
    }

    /// The entry size captured during overwrite planning.
    #[must_use]
    pub(crate) fn size(&self) -> u64 {
        self.size
    }
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

    /// Consume this handle and retain only the planning data needed for overwrite removal.
    #[must_use]
    pub fn into_removal_snapshot(self) -> RemovalSnapshot {
        RemovalSnapshot {
            kind: self.kind,
            size: self.meta.size,
        }
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
    /// empty-path `readlinkat` and the metadata from this handle's `fstat`
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

    /// Read this symlink's target from its pinned fd and return that same owned handle.
    ///
    /// Consuming the handle lets the blocking read own the existing fd without a `dup`. The returned
    /// target and handle therefore still identify one inode, so an fd-bound action can safely use the
    /// target as its authorization input.
    pub async fn read_symlink_owned(
        self,
        side: congestion::Side,
    ) -> std::io::Result<(std::path::PathBuf, Self)> {
        run_metadata_probed_blocking(side, congestion::MetadataOp::ReadLink, move || {
            let target = read_link_fd(self.as_fd())?;
            Ok((target, self))
        })
        .await
    }

    /// Require this pinned symlink to contain `expected_target`, returning the same handle.
    ///
    /// This is intended to bind metadata application after a create-by-name operation: another
    /// writer may replace the name before it is opened, but a mismatched replacement cannot
    /// authorize owner or timestamp changes on the returned fd. The read consumes no duplicate fd.
    async fn verify_symlink_target(
        self,
        expected_target: &Path,
        side: congestion::Side,
    ) -> std::io::Result<Self> {
        let (actual_target, handle) = self.read_symlink_owned(side).await?;
        if actual_target == expected_target {
            Ok(handle)
        } else {
            Err(std::io::Error::other(format!(
                "symlink target changed before metadata application (expected {expected_target:?}, \
                 found {actual_target:?})"
            )))
        }
    }
}

// ── Dir ───────────────────────────────────────────────────────────────────────

/// A directory file descriptor opened `O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`.
///
/// All entry-level operations are relative to this fd, preventing untrusted
/// multi-component path redirection. A by-name operation can still select a
/// compatible replacement in the fd's directory when its syscall runs.
///
/// The fd is held behind an `Arc` so per-entry operations can move an owned
/// reference into their `spawn_blocking` closure. Once a closure starts it is
/// not cancellable: if the surrounding future is then dropped (timeout,
/// `fail_early` abort, Ctrl-C) the closure keeps running detached. User work
/// still queued at `run_fd_admitted_blocking`'s handoff is instead reclaimed
/// synchronously with its captures; the Tokio wrapper does nothing if scheduled
/// later. Cloning the `Arc` (a refcount bump, no syscall) keeps the open file
/// description alive for a started closure's full duration even if the
/// originating `Dir` is dropped mid-flight, preserving the `openat` TOCTOU
/// guarantee. Later fd-relative methods (`open_file_read`, `create_file`,
/// `make_dir`, `read_entries`, …) must follow this same clone-Arc-into-closure
/// shape.
#[derive(Debug)]
pub struct Dir {
    fd: Arc<OwnedFd>,
    /// Which filesystem side this directory lives on, for congestion gating.
    side: congestion::Side,
    /// Whether a file created in THIS directory may inherit an access ACL that rcp has not
    /// sanitized — i.e. whether the directory may still carry a default ACL rcp took no
    /// responsibility for.
    ///
    /// `true` for every directory rcp merely OPENED: the ambient parent above a named operand
    /// (`open_parent_dir` / `open_root_dir`) and any reused directory before its lockdown. `false`
    /// only once rcp has made inheritance impossible: a `make_dir` (which strips both ACLs at
    /// creation under strict operand resolution) or a [`lockdown_reused_dir`] (which removes the
    /// default ACL for the copy's duration) — the ONLY two sites that clear it, via
    /// `Self::mark_children_cannot_inherit`.
    ///
    /// Consulted ONLY by [`Self::create_file`], ONLY under `--require-toctou-safe`. The polarity is
    /// deliberately fail-safe: a stale `true` on an already-sanitized parent costs one wasted
    /// `fremovexattr` per created file; a wrong `false` would silently reopen the containment hole
    /// (docs/acls.md, "containment"), so nothing but the two sanitizing sites may clear it — and
    /// the ONE re-arming site may set it back: [`ReusedDirLock`]'s rollback restores the
    /// directory's original default ACL, so it stores `true` (before the restore syscall) or a
    /// file created after the rollback would inherit that ACL with the strip skipped. `Arc`d so
    /// `create_file`'s blocking closure can consult it AFTER its `openat` (a pre-closure snapshot
    /// races the rollback: the closure can run after the restore with a stale `false`).
    children_may_inherit: Arc<std::sync::atomic::AtomicBool>,
}

impl Dir {
    /// Wrap a directory fd rcp merely OPENED (as opposed to created): its inherited-ACL state is
    /// unknown, so files created in it must assume the worst — see `children_may_inherit`.
    fn opened(fd: OwnedFd, side: congestion::Side) -> Dir {
        Dir {
            fd: Arc::new(fd),
            side,
            children_may_inherit: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    /// Record that nothing created in this directory can inherit an ACL anymore — called by the
    /// two sanitizing sites (`make_dir`'s creation strip and [`lockdown_reused_dir`]'s default-ACL
    /// removal) and nothing else; see `children_may_inherit`.
    fn mark_children_cannot_inherit(&self) {
        self.children_may_inherit
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

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
                        .map(|fd| Dir::opened(fd, side))
                        .map_err(nix_to_io);
                }
            }
            match openat(AT_FDCWD, &path, flags, mode) {
                Ok(fd) => Ok(Dir::opened(fd, side)),
                Err(nix::errno::Errno::ELOOP) if dereference => {
                    // final component is a symlink; follow it only when dereference=true
                    let follow_flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC;
                    openat(AT_FDCWD, &path, follow_flags, mode)
                        .map(|fd| Dir::opened(fd, side))
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
    /// Returns a [`TrustedDir`], the only retained/exposed symlink-following transition used by
    /// ordinary callers. The private dry-run preview parent opener follows the trusted prefix
    /// internally before beginning its nofollow descent. Crossing into the hardened tree below the
    /// named root is the explicit [`TrustedDir::into_tree`] step.
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
                        .map(|fd| TrustedDir(Dir::opened(fd, side)))
                        .map_err(nix_to_io);
                }
            }
            // a normal directory open: the kernel resolves the whole path following
            // symlinks, including the final (trusted parent) component. No O_NOFOLLOW.
            openat(AT_FDCWD, &path, flags, Mode::empty())
                .map(|fd| TrustedDir(Dir::opened(fd, side)))
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
        // duration even if this Dir is dropped after the closure starts (started
        // spawn_blocking work is not cancellable). see the Dir doc comment.
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            let flags = OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
            openat(dir.as_fd(), name.as_bytes(), flags, Mode::empty())
                .map(|fd| Dir::opened(fd, side))
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

    /// Read this directory's own access and default POSIX ACLs from its held fd.
    ///
    /// The directory counterpart of [`Self::meta`], and paired with it for the same reason: the
    /// ACLs a caller applies to the destination come from the SAME fd whose `read_entries` produced
    /// the contents being copied. Callers gate this on `d:acl` — see [`read_acls_fd`] for what the
    /// probe costs.
    pub async fn read_acls(&self) -> std::io::Result<Acls> {
        read_acls_owned(
            Arc::clone(&self.fd),
            self.side,
            AclCapture::AccessAndDefault,
        )
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
        // duration even if this Dir is dropped after the closure starts (started
        // spawn_blocking work is not cancellable). see the Dir doc comment.
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

    /// Take uid ownership of this directory as the copier and restrict it to `mode` in one
    /// non-cancellable blocking operation, with fd-bound postcondition verification. Used by the
    /// reused-destination-directory lockdown under [`strict_operand_resolution`]: taking ownership
    /// first means the directory's PRIOR owner can no longer `chmod` it back open while children
    /// are written.
    ///
    /// The sequence — inside ONE `spawn_blocking` closure (which runs to completion once it
    /// starts) — is ordered so that **every failure exit leaves the directory no wider than a
    /// successful copy would**:
    ///
    /// 1. When the opened directory's captured owner differs from the effective uid, `fchown` it to the
    ///    effective uid (lock the prior owner out). An already-copier-owned directory needs no
    ///    ownership syscall. Failing a required takeover leaves the directory untouched.
    /// 2. `fchmod` to a plain `0o700` IMMEDIATELY. From here on any failure leaves the
    ///    directory owner-only, i.e. no wider than success. If this `fchmod` fails, roll the
    ///    ownership back to `orig_uid` and CHECK the rollback: a clean rollback fully restores
    ///    the pre-lockdown state (the failed `chmod` changed nothing); a rollback that ALSO
    ///    fails is a genuinely stuck state (a read-only/failing backend) reported with both
    ///    errnos — the caller fails closed, so no child is ever written there.
    /// 3. `fstat` verifies the effective uid and `0o700` mode before any further mutation.
    /// 4. Reset the gid to `orig_gid` **only if it differs** (another process raced a `chgrp`
    ///    after the metadata snapshot). This matters for a **setgid** directory: children
    ///    inherit the directory's gid, and finalization cannot repair children already created.
    ///    Writing only-when-different keeps the common case a no-op. The fd-bound write can fail
    ///    with `EPERM` when a non-root copier cannot select the captured group; the caller then
    ///    fails closed without descending.
    /// 5. Re-add the setgid bit (`fchmod` to `mode`) when wanted, now that the gid is
    ///    `orig_gid`. Doing this AFTER the gid reset stops a non-privileged copier's `chmod`
    ///    from silently dropping `S_ISGID`.
    /// 6. When step 4 or 5 mutates the directory, a final `fstat` verifies the resulting gid and
    ///    mode. Otherwise the verified `fstat` from step 3 is already the final state and is reused.
    ///    A filesystem that reports a successful `chown`/`chmod` without honoring it (e.g. CIFS
    ///    without unix extensions), or a dropped `S_ISGID`, is caught before descent. A verify
    ///    failure still leaves the directory at the restrictive interim mode from step 2/5.
    ///
    /// `orig_uid`/`orig_gid` are the owner/group captured from this opened directory fd. Returns
    /// `orig_uid` only when the uid was changed, so finalize carries exactly the state it may need
    /// to restore.
    /// Fails (typically `EPERM`) when the copier neither owns the directory nor is privileged —
    /// caller fails closed.
    pub async fn secure_as_copier(
        &self,
        mode: u32,
        orig_uid: u32,
        orig_gid: u32,
    ) -> std::io::Result<Option<u32>> {
        let fd = self.fd.clone();
        let euid = nix::unistd::geteuid().as_raw();
        run_metadata_probed_blocking(self.side, congestion::MetadataOp::Chmod, move || {
            let raw = fd.as_fd();
            // take uid ownership only when the fd-captured owner differs. a directory already owned
            // by this effective uid has no prior owner to exclude, and a same-owner fchown adds no
            // security boundary.
            let restore_uid = (orig_uid != euid).then_some(orig_uid);
            if restore_uid.is_some() {
                fchown(raw, Some(Uid::from_raw(euid)), None).map_err(nix_to_io)?;
            }
            // Restrict to owner-only IMMEDIATELY, so EVERY later failure leaves the directory no wider
            // than a successful copy would. Drop to a plain 0o700 first — there is no special bit to
            // lose yet; the setgid bit (if wanted) is re-added below, once the gid is correct. If the
            // chmod fails, roll the ownership back AND CHECK the rollback, so we never silently leave
            // the directory copier-owned at its original (possibly permissive) mode.
            if let Err(chmod_err) = fchmod(raw, Mode::from_bits_truncate(0o700)).map_err(nix_to_io)
            {
                if restore_uid.is_none() {
                    // no ownership mutation occurred, so there is nothing to roll back or verify.
                    return Err(chmod_err);
                }
                // roll the ownership back AND VERIFY it landed with an fstat: a non-honoring backend
                // can report a false chown success, so we must never claim a restore that did not
                // happen.
                match fchown(raw, Some(Uid::from_raw(orig_uid)), None)
                    .map_err(nix_to_io)
                    .and_then(|()| fstat(raw).map_err(nix_to_io))
                {
                    // rollback landed: back to the prior owner at its original mode (the failed chmod
                    // changed nothing). Report the chmod cause.
                    Ok(st) if st.st_uid == orig_uid => return Err(chmod_err),
                    // rollback reported success but did not take effect (non-honoring backend).
                    Ok(st) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            format!(
                                "reused-directory lockdown could not restrict the directory (chmod: \
                                 {chmod_err}); the ownership rollback did not take effect (directory \
                                 shows uid={}, expected {orig_uid}) — it is left copier-owned at its \
                                 original mode, refusing to descend (the filesystem may not honor \
                                 chown/chmod)",
                                st.st_uid,
                            ),
                        ));
                    }
                    Err(rb) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            format!(
                                "reused-directory lockdown could not restrict the directory (chmod: \
                                 {chmod_err}) and could not roll ownership back (chown/stat: {rb}); \
                                 it is left owned by the copier at its original mode — refusing to \
                                 descend (the destination filesystem may be read-only or failing)"
                            ),
                        ));
                    }
                }
            }
            // VERIFY the takeover landed NOW — uid == copier and mode == exactly 0o700 — BEFORE the
            // fallible gid/setgid steps below run on a possibly-unverified directory. A backend that
            // reports a false chown/chmod success (e.g. CIFS without unix extensions) is thus caught
            // here rather than after more operations, and the directory is never descended into while
            // it might still be permissive.
            let st = fstat(raw).map_err(nix_to_io)?;
            if st.st_uid != euid || (st.st_mode & 0o7777) != 0o700 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!(
                        "reused-directory lockdown could not be verified: after chown+chmod the \
                         directory shows uid={} mode={:#o} (expected uid={} mode={:#o}); the \
                         filesystem may not honor chown/chmod (e.g. CIFS without unix extensions) — \
                         refusing to descend",
                        st.st_uid,
                        st.st_mode & 0o7777,
                        euid,
                        0o700,
                    ),
                ));
            }
            // The directory is now VERIFIED copier-owned at 0o700 (restrictive); every exit below
            // leaves it at least that restrictive. Reset the gid if another process raced a `chgrp`
            // after the snapshot — only when it differs, so the unchanged common case issues no gid
            // write. A setgid directory's children must inherit the captured gid, not the raced one;
            // an unauthorized reset fails closed before descent.
            let gid_changed = st.st_gid != orig_gid;
            if gid_changed {
                fchown(raw, None, Some(Gid::from_raw(orig_gid))).map_err(nix_to_io)?;
            }
            // Re-add the setgid bit now that the gid is orig_gid (skipped when the interim mode is a
            // plain 0o700). Doing this AFTER the gid reset means a non-privileged copier's chmod
            // cannot silently drop S_ISGID — the kernel keeps it because the directory's gid is now
            // one the copier is in (or the copier is root, with CAP_FSETID).
            if mode != 0o700 {
                fchmod(raw, Mode::from_bits_truncate(mode)).map_err(nix_to_io)?;
            }
            if !gid_changed && mode == 0o700 {
                // the first fstat already verified uid, gid, and final mode, and no syscall has
                // changed the directory since. another fstat would observe the same bound fd.
                return Ok(restore_uid);
            }
            // verify the whole takeover actually landed (uid, gid, mode); fail closed otherwise.
            let st = fstat(raw).map_err(nix_to_io)?;
            if st.st_uid != euid || st.st_gid != orig_gid || (st.st_mode & 0o7777) != mode {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!(
                        "reused-directory lockdown could not be verified: after takeover the \
                         directory shows uid={} gid={} mode={:#o} (expected uid={} gid={} \
                         mode={:#o}); the filesystem may not honor chown/chmod (e.g. CIFS without \
                         unix extensions), or the copier is not in the directory's group and lacks \
                         CAP_FSETID to keep setgid — refusing to descend",
                        st.st_uid,
                        st.st_gid,
                        st.st_mode & 0o7777,
                        euid,
                        orig_gid,
                        mode,
                    ),
                ));
            }
            Ok(restore_uid)
        })
        .await
    }

    /// Create a child directory and return an open `Dir` handle to it (same side as self).
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, or `EEXIST` if
    /// a directory (or any other entry) at `name` already exists.
    ///
    /// The `mkdirat`, the re-open, and (under strict operand resolution) the inherited-ACL strip
    /// all run inside ONE blocking closure, gated once as `MkDir`. That is a cancellation-safety
    /// requirement, not packaging: a queued job can be reclaimed before doing anything, while a
    /// closure that takes it runs every step to completion, so there is NO await point at which the
    /// directory exists but its sanitization has not happened. Splitting the steps across separate
    /// gated calls would let a `--fail-early` sibling abort abandon an rcp-created directory that
    /// still carries the destination's inherited default ACL — inert at that moment (created
    /// owner-only, `ACL_MASK` empty), but poisoning the containment invariant for anything created
    /// under it later, including a rerun that cannot tell such an orphan from a user's own
    /// directory.
    /// For the same reason, an open or strip failure triggers best-effort cleanup inside the same
    /// closure. Cleanup resolves the destination slot once relative to this directory fd; a
    /// compatible empty replacement at that name may be removed, while a file, symlink, or
    /// non-empty directory survives the directory cleanup. Cleanup errors are ignored so the
    /// triggering open or strip error remains authoritative.
    ///
    /// The re-open is not an inode-identity proof. Production callers ensure the parent namespace
    /// is not writable by the protected actor: the ambient parent by caller policy, and descendant
    /// parents by owner-only creation or reused-directory lockdown. They also exclude peer writers.
    /// Outside that contract, a compatible replacement may be opened and sanitized; this helper
    /// does not take over its owner or mode.
    ///
    /// # Inherited ACLs, and why the strip lives here
    ///
    /// A directory created under a parent that carries a DEFAULT ACL inherits both an access and a
    /// default ACL from it, and its own children then inherit in turn — so a destination tree's
    /// inheritance policy reaches straight through the directories rcp creates itself. Under
    /// [`strict_operand_resolution`] (`--require-toctou-safe`) that is contained by removing both
    /// ACLs from the new directory, which stops the chain for the directory's ENTIRE subtree:
    /// nothing created inside a stripped directory inherits anything. This is the containment half
    /// of the `--require-toctou-safe` invariant — *no successfully completed destination entry that
    /// rcp CREATES carries an ACL entry that did not come from its source* — and stripping per
    /// DIRECTORY is what makes it 2 syscalls per directory and none per file created beneath one
    /// ([`Self::create_file`] pays its own single strip only in a parent rcp did NOT sanitize — the
    /// ambient parent of a direct file operand; see `children_may_inherit`).
    ///
    /// The strip belongs HERE rather than at each caller because this is the only `mkdirat` in the
    /// crate, and therefore the single creation site for both the local (`copy`/`link`) and remote
    /// (`rcpd`) destinations — a caller-side strip is one forgotten exit path away from leaving the
    /// hole open. The default path is untouched: it pays nothing, which is the whole point of making
    /// containment a strict-mode concern.
    ///
    /// `ENODATA` and `EOPNOTSUPP` are success for the strip (see `apply_one_acl`) — a directory
    /// with no inherited ACL, or on a filesystem that cannot hold one, already satisfies the
    /// post-condition. Any other errno fails the create (after the cleanup attempt above), and
    /// the caller must fail closed rather than descend: an un-strippable default ACL is one every
    /// child written afterwards would inherit.
    ///
    /// **A consequence, deliberately taken.** Under `--require-toctou-safe` WITHOUT `d:acl` a fresh
    /// destination directory ends with NO ACL, even where the parent's default ACL would ordinarily
    /// have given it one. An administrator who set a default ACL on a destination tree expecting new
    /// subdirectories to inherit it does not get that under this flag. The flag's contract is that
    /// the destination reflects its source and nothing else, so containment beats inheritance;
    /// `d:acl` is the escape for a caller who wants the SOURCE's ACLs instead of none, and a caller
    /// who wants the destination tree's inheritance policy honored should not be using this flag on
    /// that tree.
    pub async fn make_dir(&self, name: &OsStr, mode: u32) -> std::io::Result<Dir> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let side = self.side;
        let name_owned = name.to_owned();
        let strict = strict_operand_resolution();
        let created =
            run_metadata_probed_blocking(side, congestion::MetadataOp::MkDir, move || {
                mkdirat(
                    dir.as_fd(),
                    name_owned.as_bytes(),
                    Mode::from_bits_truncate(mode),
                )
                .map_err(nix_to_io)?;
                let open_flags =
                    OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC;
                let fd = match openat(
                    dir.as_fd(),
                    name_owned.as_bytes(),
                    open_flags,
                    Mode::empty(),
                )
                .map_err(nix_to_io)
                {
                    Ok(fd) => fd,
                    Err(err) => {
                        // clean up the destination slot best-effort. this resolves the name once
                        // relative to the held parent; `RemoveDir` can remove an empty replacement,
                        // but a file, symlink, or non-empty directory survives
                        let _ =
                            unlinkat(dir.as_fd(), name_owned.as_bytes(), UnlinkatFlags::RemoveDir);
                        return Err(err);
                    }
                };
                if strict {
                    // both ACLs, the default (inheritance-carrying) one first — see the doc
                    // comment for why this must happen inside the creation closure
                    if let Err(err) = apply_one_acl(fd.as_raw_fd(), ACL_DEFAULT_XATTR, None)
                        .and_then(|()| apply_one_acl(fd.as_raw_fd(), ACL_ACCESS_XATTR, None))
                    {
                        // clean up the destination slot rather than leave inherited ACLs behind.
                        // this name-based rmdir may remove a compatible empty replacement, but it
                        // cannot remove a file, symlink, or non-empty directory
                        let _ =
                            unlinkat(dir.as_fd(), name_owned.as_bytes(), UnlinkatFlags::RemoveDir);
                        return Err(err);
                    }
                }
                Ok(fd)
            })
            .await?;
        Ok(Dir {
            fd: Arc::new(created),
            side,
            // under strict mode the closure above stripped both ACLs, so nothing created in this
            // directory can inherit; outside strict mode inheritance is the documented default
            // behavior and the flag is never consulted
            children_may_inherit: Arc::new(std::sync::atomic::AtomicBool::new(!strict)),
        })
    }

    /// Enumerate the directory's entries (excluding `.` and `..`).
    ///
    /// Returns each entry's name and its `getdents` `d_type` as a best-effort
    /// `EntryKind` hint (`None` when the filesystem reports `DT_UNKNOWN`). The
    /// hint is advisory only — callers perform type-sensitive planning from a
    /// `child`/`fstat` classification. That snapshot binds later work only when
    /// the work uses its fd; a by-name slot mutation relies on the syscall's own
    /// type constraints and does not claim final-name identity.
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
        run_fd_admitted_blocking(move || {
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
    }

    /// Enumerate at most `cap` entries (excluding `.` and `..`), or report the directory as
    /// over-cap. Returns `Ok(None)` as soon as a `cap + 1`-th entry is seen — the whole point:
    /// a caller that will DISCARD an over-cap listing (the overwrite-manifest build against its
    /// `--overwrite-manifest-max-entries` bound) must not pay for, or sit uncancellably in, a
    /// full enumeration of a directory arbitrarily larger than its bound. The early break leaves
    /// the shared read offset mid-directory; the borrowing iterator's `Drop` rewinds it exactly
    /// as on the full-read and error paths of [`Self::read_entries`], so re-enumeration on the
    /// same `Dir` stays sound.
    pub async fn read_entries_capped(
        &self,
        cap: usize,
    ) -> std::io::Result<Option<Vec<(std::ffi::OsString, Option<EntryKind>)>>> {
        throttle::get_ops_token().await;
        let dir = self.fd.clone();
        run_fd_admitted_blocking(move || {
            // dup + rewind contract: see read_entries
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
                if name_cstr == c"." || name_cstr == c".." {
                    continue;
                }
                if entries.len() >= cap {
                    return Ok(None);
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
            Ok(Some(entries))
        })
        .await
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
    /// `rm` reads its tree on the `Source` side because its `Dir` handles are source-sided, but the
    /// destructive `unlinkat` is bucketed on `Destination` so it competes for the same metadata cwnd
    /// as other destructive work. The fd-relative TOCTOU guarantee is unaffected: the syscall is
    /// still resolved against this directory's pinned fd.
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

    /// Create a symlink `name` → `target` in this directory.
    ///
    /// `target` is the link contents — it is an arbitrary path and is not restricted to a single
    /// component. This create-only form does not reopen the final name.
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, or `EEXIST`
    /// if an entry at `name` already exists.
    pub async fn symlink_at(&self, name: &OsStr, target: &Path) -> std::io::Result<()> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        let target = target.to_owned();
        run_metadata_probed_blocking(side, congestion::MetadataOp::Symlink, move || {
            // symlinkat(target, dirfd, name): creates `name` → `target`
            symlinkat(target.as_os_str().as_bytes(), dir.as_fd(), name.as_bytes())
                .map_err(nix_to_io)?;
            #[cfg(test)]
            let _gate_visit =
                crate::testutils::wait_on_blocking_path_gate(&target, dir.as_fd().as_raw_fd());
            Ok(())
        })
        .await
    }

    /// Apply requested metadata to a same-target symlink through its pinned fd.
    ///
    /// With no requested symlink metadata this performs no syscall. Otherwise the final name is
    /// opened, its immutable target is read from that fd, and owner/timestamps are applied only if
    /// that target equals `target`. This does not prove final-name identity: it binds the target
    /// authorization to the exact metadata recipient, accepting a compatible same-target replacement
    /// and rejecting a different-target one.
    pub async fn set_symlink_metadata_at<Meta: crate::preserve::Metadata>(
        &self,
        name: &OsStr,
        target: &Path,
        settings: &crate::preserve::Settings,
        meta: &Meta,
    ) -> std::io::Result<()> {
        if !settings.symlink.any() {
            return Ok(());
        }
        let handle = self.child(name).await?;
        if handle.kind() != EntryKind::Symlink {
            return Err(std::io::Error::from_raw_os_error(libc::ESTALE));
        }
        let handle = handle.verify_symlink_target(target, self.side).await?;
        set_symlink_metadata_fd(settings, meta, &handle, self.side).await
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
        // originating Handle is dropped after the closure starts. started spawn_blocking work is
        // not cancellable.
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
    /// The file is ALWAYS created at [`DST_FILE_CREATE_MODE`] — there is deliberately no mode
    /// parameter, so a caller cannot publish a destination file at its final (possibly setuid)
    /// mode before its contents exist. [`set_file_metadata_fd`] widens it to the source mode after
    /// the last byte. Returns the open writable `File` on success; the returned fd is writable
    /// whatever the mode says, having been opened `O_WRONLY` at creation.
    ///
    /// `O_EXCL` is the primary guard: combined with `O_CREAT`, it fails with
    /// `EEXIST` on any pre-existing entry — including a symlink — without
    /// following it. `O_NOFOLLOW` is the fallback that would still refuse to
    /// follow a symlink (with `ELOOP`) should `O_EXCL` ever be bypassed.
    ///
    /// # The ambient-parent ACL strip
    ///
    /// Under [`strict_operand_resolution`] a file created in a parent rcp did NOT sanitize — the
    /// ambient parent of a direct file operand, the one directory kind rcp neither creates nor
    /// locks down (`children_may_inherit`) — has its inherited access ACL removed in the SAME
    /// blocking closure as the create. Without it, a parent's default ACL hands the fresh file
    /// named `user:`/`group:` entries; they are inert at [`DST_FILE_CREATE_MODE`] (the create mode
    /// leaves `ACL_MASK` empty), but the final `fchmod` to the source mode re-derives the mask
    /// from the group bits and ACTIVATES them — a strict copy of a plain `0640` file granting a
    /// named user read access its source never did. Files created beneath a sanitized directory
    /// pay nothing: the parent-side strip already broke the inheritance chain, which is the flag's
    /// whole point. A strip failure fails the create after best-effort cleanup of the destination
    /// slot, because the caller's later chmod would otherwise publish the inherited entries. The
    /// cleanup resolves the name once relative to this directory fd and may unlink a compatible
    /// replacement, but it never follows a symlink and cannot remove a directory. Cleanup errors
    /// are ignored so the ACL-strip error remains authoritative.
    ///
    /// Fails with `EINVAL` if `name` is not a single path component, or `EEXIST`
    /// if a file or symlink at `name` already exists.
    pub async fn create_file(&self, name: &OsStr) -> std::io::Result<std::fs::File> {
        if !is_single_component(name) {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let dir = self.fd.clone();
        let side = self.side;
        let name = name.to_owned();
        // the flag travels INTO the closure and is read AFTER the openat: a pre-submit snapshot
        // races the reused-dir rollback — a fail-early abort can restore the parent's default ACL
        // between this submit and the queued openat, and the file created then DOES inherit. The
        // rollback stores `true` before its restore syscall and this load runs after the openat;
        // syscalls order as full fences, so an openat that observed the restored ACL cannot then
        // load a stale `false`. A wasted strip on a not-yet-restored parent remains harmless.
        let strict = strict_operand_resolution();
        let may_inherit = Arc::clone(&self.children_may_inherit);
        run_metadata_probed_blocking(side, congestion::MetadataOp::OpenCreate, move || {
            let flags = OFlag::O_CREAT
                | OFlag::O_EXCL
                | OFlag::O_WRONLY
                | OFlag::O_NOFOLLOW
                | OFlag::O_CLOEXEC;
            let file_mode = Mode::from_bits_truncate(DST_FILE_CREATE_MODE);
            let file = openat(dir.as_fd(), name.as_bytes(), flags, file_mode)
                .map(std::fs::File::from)
                .map_err(nix_to_io)?;
            if strict && may_inherit.load(std::sync::atomic::Ordering::SeqCst) {
                // same-closure for the same reason as `make_dir`: queued work creates nothing,
                // while work that starts runs to completion, so cancellation cannot abandon a
                // created-but-unsanitized file. cleanup is a single name-based unlink relative to
                // the ambient operand parent. it may remove a compatible replacement occupying the
                // slot, but it never follows a symlink and cannot remove a directory
                if let Err(err) = apply_one_acl(file.as_raw_fd(), ACL_ACCESS_XATTR, None) {
                    let _ = unlinkat(dir.as_fd(), name.as_bytes(), UnlinkatFlags::NoRemoveDir);
                    return Err(err);
                }
            }
            Ok(file)
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
/// is that trusted container, and it is the only retained/exposed follow-open transition used by
/// ordinary callers. The private dry-run preview parent opener follows the trusted prefix
/// internally before its nofollow descent. Every other ordinary directory open ([`Dir::open_dir`],
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

// ── Operand probes and contained preview opens ───────────────────────────────
//
// dry-run preview scans resolve the trusted operand parent with the active default/strict policy,
// then open the named operand and every scan-relative component fd-relative without following
// symlinks. The strict existence/kind probes below similarly decompose an operand into parent +
// final component: strict mode resolves the parent through `openat2(RESOLVE_NO_SYMLINKS)` and
// touches only the final component fd-relative, while default-mode callers retain their existing
// path-based probes.

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

/// Open the trusted parent of a destination operand for a contained preview scan.
///
/// `O_PATH` is load-bearing in default mode: a searchable-but-unreadable parent may contain an
/// operand that is itself readable, and preview containment must not add a parent read-permission
/// requirement. Strict mode applies the same flag through `openat2(RESOLVE_NO_SYMLINKS)`.
async fn open_preview_operand_parent(path: &Path, side: congestion::Side) -> std::io::Result<Dir> {
    let path = path.to_owned();
    run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
        let flags = OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC;
        #[cfg(target_os = "linux")]
        if strict_operand_resolution() {
            return openat2_no_symlinks(&path, flags)
                .map(|fd| Dir::opened(fd, side))
                .map_err(nix_to_io);
        }
        openat(AT_FDCWD, &path, flags, Mode::empty())
            .map(|fd| Dir::opened(fd, side))
            .map_err(nix_to_io)
    })
    .await
}

fn preview_scan_absence(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
        || error.kind() == std::io::ErrorKind::NotADirectory
        || matches!(
            error.raw_os_error(),
            Some(libc::ENOTDIR) | Some(libc::ELOOP)
        )
}

fn preview_relative_components(relative: &Path) -> std::io::Result<Vec<std::ffi::OsString>> {
    if relative.as_os_str().is_empty() {
        return Ok(Vec::new());
    }
    relative
        .as_os_str()
        .as_bytes()
        .split(|byte| *byte == b'/')
        .map(|component| {
            let component = OsStr::from_bytes(component);
            is_single_component(component)
                .then(|| component.to_owned())
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))
        })
        .collect()
}

/// Open an existing directory below the exact destination operand named by the user.
///
/// The operand's trusted parent is resolved with the normal strict/default policy, but the named
/// root itself and every `relative` component are opened one at a time with `O_NOFOLLOW`. This keeps
/// dry-run delete scans beneath the operand even when that final operand component is a symlink.
/// Missing, non-directory, and symlink components mean there is no real directory to preview and
/// return `None`; every other error remains observable to the caller.
pub(crate) async fn open_existing_dir_beneath_operand(
    named_root: &Path,
    relative: &Path,
    side: congestion::Side,
) -> std::io::Result<Option<Dir>> {
    let relative = preview_relative_components(relative)?;
    let Some((parent_path, root_name)) = split_parent_and_name(named_root) else {
        return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
    };
    if !is_single_component(root_name) {
        return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
    }
    let parent = match open_preview_operand_parent(parent_path, side).await {
        Ok(parent) => parent,
        Err(error) if preview_scan_absence(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut current = match parent.open_dir(root_name).await {
        Ok(dir) => dir,
        Err(error) if preview_scan_absence(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    drop(parent);
    for component in relative {
        current = match current.open_dir(&component).await {
            Ok(dir) => dir,
            Err(error) if preview_scan_absence(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
    }
    Ok(Some(current))
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

/// State a strict-mode reused-directory finalize must resolve and an abort must roll back.
///
/// # This type is an RAII guard, and that is the point
///
/// The snapshotted default ACL is the ONLY copy of those bytes in existence — the lockdown removed
/// them from the filesystem. If this value were ever dropped without the restore running, the
/// destination directory would permanently lose an ACL it had before the copy: unrecoverable, and
/// invisible, since nothing else reports it.
///
/// There are many ways to drop it. A `--fail-early` copy returns `Err` from the walk driver without
/// ever reaching `dir_post`, and aborts its in-flight siblings by dropping their `JoinSet` — so one
/// failed leaf can end a dozen directories' lockdowns at once. `link_dir_contents` returns early on
/// a failed `read_entries` with no flag needed at all. The remote destination can fail between
/// locking a directory and registering it, or shut down with directories still pending. Restoring
/// separately at every exit would make omission easy and the invariant difficult to verify.
///
/// So the restore lives in [`Drop`] instead, and holds on every exit path that drops the value: an
/// early return, a `?`, a `break`, and — the case the abort paths above actually need — a cancelled
/// task, since dropping a future drops its locals. [`set_reused_dir_metadata_fd`] disarms the guard
/// once the default ACL has been dealt with deliberately AND nothing after it can fail, which is
/// why the state is private: nothing outside this module can arm, disarm, or duplicate it. The
/// armed/disarmed distinction is an explicit enum (`DefaultAclGuard`) rather than the emptiness
/// of the snapshot, because a directory that HAD no default ACL still needs an armed rollback: a
/// partial finalize may have installed the source's, and only an armed guard removes it. `Clone`
/// is deliberately NOT derived — exactly one of these exists per lockdown, and a copy would
/// restore twice.
///
/// **The guard must exist before anything is destroyed, not after.** "Dropped on every exit path"
/// says nothing about the window in which the value does not exist. [`lockdown_reused_dir`] arms
/// the guard before removing the ACL synchronously, with no cancellation point in between.
///
/// # Exits it does NOT cover
///
/// A panic (the workspace builds `panic = "abort"`, so nothing unwinds and no destructor runs), a
/// signal, and `std::process::exit` — which, unlike the first two, is ordinary control flow in
/// repository code. Every `process::exit` call site runs either before a copy starts (the TOCTOU
/// linter, config validation) or after `common::run` has dropped the tokio runtime, which drops every
/// task and therefore every guard; `rcpd`'s stdin watchdog follows the same lifetime rule. A call
/// that exits mid-copy would reintroduce the hole. Closing it for panic and signal would need the
/// snapshot on disk rather than in memory — out of proportion to the risk, but worth knowing rather
/// than assuming this is total.
///
/// # The `Drop` restore is best-effort recovery, not a guarantee
///
/// A destructor cannot await, so it issues a synchronous `fsetxattr` that bypasses the congestion
/// throttle every other metadata write here goes through, and it cannot return an error. A failure
/// is logged at `warn!` naming the directory — resolved from the held fd through `/proc/self/fd`,
/// one cheap syscall, because on a mass abort an operator otherwise gets N indistinguishable
/// warnings and no target — along with the ACL bytes to rebuild it from. A SUCCESSFUL restore is
/// logged at `debug!` so that "did the guard fire?" is observable rather than inferred. This is the
/// backstop for an aborted copy. Successful finalize deliberately resolves the attribute to either
/// the source default ACL (`d:acl` on) or this destination snapshot (`d:acl` off).
///
/// # What is deliberately NOT restored here
///
/// Only the default ACL. An aborted lockdown still leaves the directory owned by the copier at
/// `0o700` — that outcome is already documented in `docs/tocttou.md`, and unlike ACL loss it is
/// narrow, visible, and repairable: an operator sees the mode and fixes it. Nothing is destroyed.
/// The armed/disarmed state of a [`ReusedDirLock`]'s default-ACL rollback, shared (behind one
/// mutex) between the guard's `Drop` restore and every finalize write to the guarded attribute.
///
/// Two conflations this enum exists to prevent:
///
/// - **"Had no original ACL" is not "nothing to undo".** With `d:acl` on, finalize INSTALLS the
///   source's default ACL before later steps that can still fail; a directory that originally had
///   none must then get that installation REMOVED on rollback, or a failed copy leaves it carrying
///   an ACL the destination never had. A bare `Option<Vec<u8>>` cannot express that arm because its
///   `None` would have to mean both "no original ACL" and "disarmed".
/// - **A detached write must not undo the restore.** A finalize ACL write still queued when its
///   waiter is dropped is reclaimed before it starts, but one already taken by a blocking worker
///   cannot be cancelled and may land AFTER the guard's `Drop` has put the original back.
///   Serializing every guarded write and the restore through this one mutex, with a write that
///   observes [`Self::Disarmed`] SKIPPING, closes the race in both directions: a write mid-syscall
///   holds the mutex, so the restore waits and then lands over it (correct — the copy is
///   aborting); a write not yet started finds the guard disarmed and never lands.
#[derive(Debug)]
enum DefaultAclGuard {
    /// The lockdown is live. On abort, put the directory's default ACL back to `original`:
    /// `Some` bytes are restored; `None` means the directory had no default ACL, so whatever a
    /// partial finalize may have installed is REMOVED.
    Armed { original: Option<Vec<u8>> },
    /// Finalize resolved the default ACL deliberately (or the restore already ran). Nothing is
    /// left to undo, and any still-queued guarded write must NOT land.
    Disarmed,
}

#[derive(Debug)]
pub struct ReusedDirLock {
    /// The original uid when lockdown changed it to the copier. Successful finalize restores this
    /// only when source uid preservation is disabled. Lockdown already restores and verifies gid.
    restore_uid: Option<u32>,
    /// The rollback state plus the original default ACL, shared (`Arc`) with in-flight guarded
    /// writes so the `Drop` restore serializes against them — see `DefaultAclGuard`.
    state: Arc<std::sync::Mutex<DefaultAclGuard>>,
    /// The locked directory's fd, shared with the `Dir` it came from. An `Arc` rather than a `dup`:
    /// it costs no descriptor and no syscall, and it keeps the fd open for exactly as long as the
    /// guard can still need it, so `Drop` writes to the pinned inode rather than re-resolving a path
    /// an attacker may have swapped.
    fd: Arc<OwnedFd>,
    /// The locked `Dir`'s inherit flag, shared so the rollback can RE-ARM it (`store(true)` before
    /// the restore syscall): restoring the original default ACL makes inheritance possible again,
    /// and a file created in the window after the rollback must strip what it inherited — see
    /// `Dir::children_may_inherit` and `Dir::create_file`.
    children_may_inherit: Arc<std::sync::atomic::AtomicBool>,
}

impl ReusedDirLock {
    /// The original default ACL the lockdown snapshotted (`None` when the directory had none).
    /// Peeks without disarming; a caller restores it via [`Self::write_default_acl_guarded`].
    fn original_default_acl(&self) -> Option<Vec<u8>> {
        match &*self.state.lock().unwrap() {
            DefaultAclGuard::Armed { original } => original.clone(),
            DefaultAclGuard::Disarmed => None,
        }
    }

    /// Declare the default ACL deliberately resolved: the `Drop` restore becomes a no-op and any
    /// still-queued guarded write is refused. Only called once every finalize step that could
    /// still fail has succeeded.
    fn disarm(&self) {
        *self.state.lock().unwrap() = DefaultAclGuard::Disarmed;
    }

    /// Install `blob` as the locked directory's DEFAULT ACL, serialized against the guard's
    /// rollback (see `DefaultAclGuard`): the state mutex is held across the syscall, and a write
    /// that finds the guard already disarmed is SKIPPED — landing it would silently undo a restore
    /// that already ran. Every finalize-path write to this attribute MUST go through here; an
    /// unguarded `set_or_remove_acl_fd` on the blocking pool is exactly the detached-write hazard
    /// the guard exists to close. Only ever needs to SET: after the lockdown the directory
    /// provably carries no default ACL, so "remove" cases have nothing to do.
    ///
    /// Gated as `MetadataOp::Chmod`, like every other single-inode permission write.
    async fn write_default_acl_guarded(
        &self,
        side: congestion::Side,
        blob: Vec<u8>,
    ) -> std::io::Result<()> {
        let state = Arc::clone(&self.state);
        let fd = Arc::clone(&self.fd);
        run_metadata_probed_blocking(side, congestion::MetadataOp::Chmod, move || {
            let guard = state.lock().unwrap();
            match &*guard {
                DefaultAclGuard::Armed { .. } => {
                    apply_one_acl(fd.as_raw_fd(), ACL_DEFAULT_XATTR, Some(&blob))
                }
                DefaultAclGuard::Disarmed => {
                    // this write's owner was cancelled and the dropped guard already restored;
                    // landing the write now would silently overwrite that restore
                    tracing::debug!(
                        "skipped a default-ACL write that lost its race against the lockdown \
                         guard's restore ({} bytes)",
                        blob.len(),
                    );
                    Ok(())
                }
            }
        })
        .await
    }
}

impl Drop for ReusedDirLock {
    fn drop(&mut self) {
        // re-arm the inherit flag BEFORE the restore syscall (which may run detached below): once
        // the original default ACL is back, a file created in this directory inherits it and must
        // strip — `create_file` loads the flag after its openat, so this ordering (store, then
        // restore; openat, then load) leaves no interleaving where the openat sees the restored
        // ACL but the load sees `false`
        self.children_may_inherit
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Taking the mutex is what serializes this restore against an in-flight guarded write: a
        // write mid-syscall holds it, so the restore must wait and then deliberately land OVER
        // it. But WAITING here would block whatever thread runs this destructor — on the
        // cancellation paths that is an EXECUTOR thread (`--fail-early` dropping tracker state, a
        // watchdog dropping the operation), and the wait is bounded only by the write's
        // `fsetxattr`, which on dead network storage is unbounded. So: uncontended (the ordinary
        // case — no write in flight) restores inline; contended detaches the SAME serialized
        // wait-then-restore to its own thread. The `Arc`s keep the state and fd alive for it, and
        // process exit may abandon the thread exactly as it abandons the stalled write itself.
        let state = match self.state.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => {
                let state = Arc::clone(&self.state);
                let fd = Arc::clone(&self.fd);
                let flag_note = "detached (a guarded write held the rollback mutex)";
                std::thread::spawn(move || {
                    // a poisoned mutex is unreachable under panic = "abort"; fail toward doing
                    // the restore anyway rather than silently dropping it
                    let guard = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    rollback_default_acl(guard, &fd, flag_note);
                });
                return;
            }
            // unreachable under panic = "abort"; restore anyway rather than skip
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        };
        rollback_default_acl(state, &self.fd, "inline");
    }
}

/// The rollback itself, run with the guard's mutex HELD (see `DefaultAclGuard`): flips the state
/// to `Disarmed` and puts the directory's original default ACL back (or removes what a partial
/// finalize installed, when there was none). Split from `ReusedDirLock::drop` so the contended
/// path can run it on a detached thread with the same serialization.
fn rollback_default_acl(
    mut state: std::sync::MutexGuard<'_, DefaultAclGuard>,
    fd: &OwnedFd,
    how: &str,
) {
    {
        let DefaultAclGuard::Armed { original } =
            std::mem::replace(&mut *state, DefaultAclGuard::Disarmed)
        else {
            return;
        };
        let raw = fd.as_raw_fd();
        let _ = how;
        // name the directory rather than the descriptor: a mass abort restores many at once, and
        // "fd 47" tells an operator nothing about which tree to repair. Best-effort — a fd whose
        // target is gone falls back to the raw number rather than failing the restore.
        let path = std::fs::read_link(format!("/proc/self/fd/{raw}"))
            .map_or_else(|_| format!("<fd {raw}>"), |p| p.display().to_string());
        let restore = apply_one_acl(raw, ACL_DEFAULT_XATTR, original.as_deref());
        match (restore, &original) {
            (Ok(()), Some(blob)) => tracing::debug!(
                "restored the default ACL of reused destination directory {} after an aborted \
                 copy ({} bytes)",
                path,
                blob.len(),
            ),
            (Ok(()), None) => tracing::debug!(
                "ensured reused destination directory {} carries no default ACL after an aborted \
                 copy (it had none before it)",
                path,
            ),
            (Err(error), Some(blob)) => tracing::warn!(
                "could not restore the default ACL of reused destination directory {} after an \
                 aborted copy: {:#}. It is left with NO default ACL; it had {} bytes ({:02x?}), \
                 which must be re-applied by hand to restore what entries created beneath it \
                 inherit.",
                path,
                &error,
                blob.len(),
                blob,
            ),
            (Err(error), None) => tracing::warn!(
                "could not remove a partially-applied default ACL from reused destination \
                 directory {} after an aborted copy: {:#}. The directory had NO default ACL \
                 before the copy; whatever the aborted finalize installed remains and must be \
                 removed by hand.",
                path,
                &error,
            ),
        }
    }
}

/// Lock down a REUSED destination directory under strict operand resolution, returning the state
/// finalize must resolve and an abort must roll back — `Some` when locked, `None` (a pure no-op)
/// when strict mode is off.
///
/// `dir` is the directory atomically selected by `open_dir(O_NOFOLLOW|O_DIRECTORY)`. While strict
/// operand resolution is armed the lockdown:
///
/// 1. captures the opened directory's original metadata;
/// 2. [`Dir::secure_as_copier`] — takes ownership as the copier then restricts to `0o700`, so no
///    child is written while the directory is world-readable/writable and the prior owner cannot
///    re-widen it mid-copy;
/// 3. [`read_acls_fd`] — snapshots the DEFAULT ACL, and
/// 4. removes it, so nothing created during the copy inherits it.
///
/// Steps 3 and 4 both run after the takeover, and both for the same reason: only an owner (or a
/// `CAP_FOWNER` copier) may write these attributes, so a strip issued before step 2 would leave the
/// prior owner free to put the default ACL straight back, and a snapshot read before it could be
/// raced by that same owner. `chmod` does not touch a default ACL (verified), so nothing about the
/// recorded value depends on reading it first.
///
/// # Why only the DEFAULT ACL
///
/// The *access* ACL is deliberately left alone because stripping it buys no containment. Step 3's
/// `chmod` rewrites `ACL_MASK` from the new group bits, so at
/// `0o700` the mask is `---` and every named `user:`/`group:` entry — and the owning group — grants
/// nothing, while `OTHER` is `---` too. Only the copier, who now owns the directory, has any access
/// at all (verified). And it needs no restoring, because finalize resolves it either way: with
/// `d:acl` the source's ACL is set or cleared over it, and without `d:acl` the finalize `chmod`
/// re-derives its mask from the source's mode — which is exactly what an unhardened copy's single
/// `chmod` does to a pre-existing ACL, so the end state is identical either way.
///
/// Leaving it alone therefore costs nothing and means an aborted copy can destroy at most the
/// default ACL rather than both. (A freshly CREATED directory is the opposite case and still strips
/// both — see [`Dir::make_dir`]: what it inherited is not its own, and with `d:acl` off nothing at
/// finalize would remove an inherited access ACL, so the finalize `chmod` would make it effective.)
///
/// Alias limitation: two source operands whose destinations alias the same directory can lock it
/// twice, and the remote tracker's `pending_directories.insert` would drop the first guard mid-copy
/// (restoring the default ACL while the second lockdown is still live). `--require-toctou-safe`
/// refuses byte-equal duplicate destinations up front and serializes strict multi-source copies,
/// which covers the reachable cases; filesystem-level aliasing (casefold, bind mount) is not
/// detected.
///
/// [`set_reused_dir_metadata_fd`] resolves everything the lockdown changed: owner components not
/// preserved from the source return to the destination owner; the default ACL returns to its
/// destination state when `d:acl` is off or is replaced/cleared from the source when it is on; and
/// source-derived mode/metadata is applied. A successful copy therefore has the same result as one
/// without the interim lockdown. An `fchown`/`fchmod` `EPERM` (non-owner, non-privileged copier), a
/// metadata read failure, or an ACL snapshot/strip failure propagates as `Err`; callers skip
/// descent, so no child is written into an unsecured directory. Failing closed on the snapshot is
/// why it is read before the strip: removing an ACL we could not record would permanently destroy
/// the destination directory's own state. This is the single lockdown used by the local
/// (`copy`/`link`) and remote (`rcpd`) reuse sites.
pub async fn lockdown_reused_dir(dir: &Dir) -> std::io::Result<Option<ReusedDirLock>> {
    if !strict_operand_resolution() {
        return Ok(None);
    }
    use crate::preserve::Metadata as _;
    use std::os::unix::fs::PermissionsExt as _;
    let orig_meta = dir.meta().await?;
    // Restrict to 0o700 (owner-only) for the copy's duration, PRESERVING the setgid bit if the reused
    // directory had it (0o700 already denies all group/other access, so keeping setgid costs nothing
    // security-wise). Unlike the source mode applied at finalize, this INTERIM setgid bit governs the
    // group that every child created during the copy inherits — and finalization cannot repair those
    // children's GIDs under `preserve_none`. So a setgid directory must be locked down with BOTH its
    // gid value and its S_ISGID bit intact: `secure_as_copier` (given the fd-captured gid) resets the
    // gid if a prior owner raced a `chgrp`, and re-stats to fail closed if `chmod` cleared S_ISGID (a
    // non-privileged copier not in the directory's group, lacking `CAP_FSETID`) or if the filesystem
    // did not honor the takeover at all. Root has `CAP_FSETID`, keeps the bit, and never trips this.
    let want_setgid = orig_meta.permissions().mode() & 0o2000 != 0;
    let interim_mode = if want_setgid { 0o2700 } else { 0o700 };
    let restore_uid = dir
        .secure_as_copier(interim_mode, orig_meta.uid(), orig_meta.gid())
        .await?;
    // snapshot only the default ACL, now that the directory is ours and the prior owner can no
    // longer race the read (step 3). Nothing is destroyed yet, so cancellation here loses nothing.
    let original = read_acls_owned(Arc::clone(&dir.fd), dir.side, AclCapture::Default)
        .await?
        .default;
    let needs_strip = original.is_some();
    // arm the guard before destroying anything (step 4). the snapshot is the only copy of these
    // bytes once the removal lands, so it must already be inside a value whose `Drop` puts it back
    // before the removal is issued — otherwise a task cancelled between the two loses it as a bare
    // `Option<Vec<u8>>` with no destructor. Armed even when the directory has no default ACL: the
    // rollback then means "keep it that way" — removing whatever a partial finalize installs.
    let lock = ReusedDirLock {
        restore_uid,
        state: Arc::new(std::sync::Mutex::new(DefaultAclGuard::Armed { original })),
        fd: Arc::clone(&dir.fd),
        children_may_inherit: Arc::clone(&dir.children_may_inherit),
    };
    if needs_strip {
        let raw = dir.fd.as_raw_fd();
        // The removal is issued SYNCHRONOUSLY, inside the gate rather than on the blocking pool,
        // and this is load-bearing rather than an optimization. the shared blocking boundary can
        // reclaim user work before a worker takes it, but a closure cannot be stopped after that
        // handoff, so an `.await` on it is a cancellation point at which the `fremovexattr` may
        // still run — the exact hazard the arming above closes. Running it in-line means there is
        // no cancellation point between arming and destroying at all: polling this future runs the
        // syscall to completion.
        //
        // Arming first while KEEPING `spawn_blocking` would be strictly worse than either: a pool
        // thread's `fremovexattr` could land after this task's `Drop` restore and silently undo it.
        //
        // The cost is one `fremovexattr` (microseconds) on an async worker, the same call `Drop`
        // already makes, and only for a reused directory that actually has a default ACL. The gate
        // still applies — `run_metadata_probed` takes the ops token and cwnd permit and feeds the
        // latency probe exactly as `set_or_remove_acl_fd` would.
        crate::walk::run_metadata_probed(dir.side, congestion::MetadataOp::Chmod, async {
            apply_one_acl(raw, ACL_DEFAULT_XATTR, None)
        })
        .await?;
        // NB: on `Err` the guard is still armed and `Drop` re-writes the snapshot. `fremovexattr`
        // is atomic, so the ACL is either gone (the restore puts it back) or never left (the
        // restore is a no-op) — never a partial state.
    }
    // the directory now provably has no default ACL (either it never had one, or the strip above
    // succeeded), so files created in it during the copy cannot inherit — `create_file` need not
    // pay the ambient-parent strip here
    dir.mark_children_cannot_inherit();
    Ok(Some(lock))
}

// ── POSIX ACLs ──────────────────────────────────────────────────────────────────
//
// POSIX.1e ACLs live in two extended attributes and are read and written here as
// OPAQUE kernel blobs: `system.posix_acl_access` (on any inode) and
// `system.posix_acl_default` (directories only, governing what children inherit).
// Using the raw xattrs rather than libacl keeps this to plain fd syscalls with no
// new dependency, and the on-disk format is defined little-endian, so the bytes
// are portable verbatim. The blobs are never interpreted: rcp carries what the
// source kernel produced and lets the destination kernel validate it on
// `fsetxattr`.

/// The access ACL: the entry's own permissions. Present on any inode kind.
const ACL_ACCESS_XATTR: &CStr = c"system.posix_acl_access";

/// The default ACL: what a directory's freshly created CHILDREN inherit. Directories only, and
/// mode-neutral — setting or clearing it never moves the directory's own mode.
const ACL_DEFAULT_XATTR: &CStr = c"system.posix_acl_default";

/// A source entry's POSIX.1e ACLs, as opaque kernel bytes.
///
/// `None` means the source has no such ACL — which a destination applier reproduces by REMOVING
/// the attribute, not by leaving it alone. A destination directory's default ACL is inherited by
/// every child created beneath it, so "do nothing when the source has no ACL" hands the copy
/// permissions the source never granted. See [`apply_acls_fd`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Acls {
    /// `system.posix_acl_access`.
    pub access: Option<Vec<u8>>,
    /// `system.posix_acl_default`; only ever read or applied for a directory.
    pub default: Option<Vec<u8>>,
}

/// POSIX ACL attributes to capture from an already-open entry.
#[derive(Clone, Copy, Debug)]
pub enum AclCapture {
    /// Capture only the access ACL used by files and other non-directory entries.
    Access,
    /// Capture both the access and default ACLs used by source directories.
    AccessAndDefault,
    /// Capture only the default ACL used by reused-directory lockdown.
    Default,
}

impl AclCapture {
    fn wants_access(self) -> bool {
        matches!(self, Self::Access | Self::AccessAndDefault)
    }

    fn wants_default(self) -> bool {
        matches!(self, Self::Default | Self::AccessAndDefault)
    }
}

/// Read an entry's POSIX ACLs through an fd it is already open on.
///
/// `capture` selects the attributes the caller consumes. Source files request access ACLs, source
/// directories request both, and reused-directory lockdown requests only the default ACL it may
/// need to roll back. An unrequested present attribute is not fetched.
///
/// `flistxattr` runs FIRST and an `fgetxattr` is issued only for a name actually present. There is
/// no bit in `stat` saying an entry has an ACL, so this probe is what ACL preservation costs per
/// entry, and the shape is chosen to make the common case cheap: measured on ext4, a `getxattr`
/// that misses costs ~1057ns against ~591ns for the whole `listxattr`, and missing is the
/// overwhelming majority. Each syscall is gated through the module's `run_metadata_probed_blocking`
/// as `MetadataOp::Stat`, like every other metadata read here — an ACL probe that bypassed the
/// throttle would be the one metadata operation in rcp that does.
///
/// A filesystem that cannot hold ACLs (`EOPNOTSUPP`) and an entry that simply has none (`ENODATA`)
/// both yield `None`: neither has an ACL to preserve.
pub async fn read_acls_fd(
    fd: BorrowedFd<'_>,
    side: congestion::Side,
    capture: AclCapture,
) -> std::io::Result<Acls> {
    read_acls_owned(Arc::new(fd.try_clone_to_owned()?), side, capture).await
}

/// Read ACLs using an already shareable owned fd, avoiding the borrowed-fd wrapper's duplicate.
async fn read_acls_owned(
    owned: Arc<OwnedFd>,
    side: congestion::Side,
    capture: AclCapture,
) -> std::io::Result<Acls> {
    // share the owned fd across the (up to three) gated syscalls: each runs in its own
    // `spawn_blocking` closure, which must own what it touches to be 'static.
    let names = {
        let owned = Arc::clone(&owned);
        run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            flistxattr_names(owned.as_raw_fd())
        })
        .await?
    };
    let mut acls = Acls::default();
    if capture.wants_access() && names_contain(&names, ACL_ACCESS_XATTR) {
        let owned = Arc::clone(&owned);
        acls.access = run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            fgetxattr_blob(owned.as_raw_fd(), ACL_ACCESS_XATTR)
        })
        .await?;
    }
    if capture.wants_default() && names_contain(&names, ACL_DEFAULT_XATTR) {
        acls.default =
            run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
                fgetxattr_blob(owned.as_raw_fd(), ACL_DEFAULT_XATTR)
            })
            .await?;
    }
    Ok(acls)
}

/// Apply `acls` to a destination entry through an fd it is already open on, SETTING each attribute
/// the source had and REMOVING each one it did not.
///
/// Clearing is not an optimization to skip: a destination directory's default ACL is inherited by
/// every child created beneath it, including children rcp itself creates, so a source with no ACL
/// needs an explicit removal or the copy silently ends up more permissive than what it copied.
///
/// The access ACL is applied LAST because it is the step that WIDENS the destination — see
/// [`set_file_metadata_fd`]. The default ACL is mode-neutral, so it goes first and constrains
/// nothing.
///
/// Gated as `MetadataOp::Chmod`, the bucket for single-inode permission writes.
pub async fn apply_acls_fd(
    fd: BorrowedFd<'_>,
    side: congestion::Side,
    acls: &Acls,
    want_default: bool,
) -> std::io::Result<()> {
    if want_default {
        set_or_remove_acl_fd(fd, side, ACL_DEFAULT_XATTR, acls.default.clone()).await?;
    }
    set_or_remove_acl_fd(fd, side, ACL_ACCESS_XATTR, acls.access.clone()).await
}

/// Install `blob` as `name` on `fd`, or REMOVE `name` when `blob` is `None`, through the module's
/// metadata gate.
///
/// The async half of `apply_one_acl`, at single-attribute granularity: [`apply_acls_fd`] (its
/// one caller) resolves the two ACLs one at a time through it. A LOCKED reused directory's
/// default ACL must NOT come through here — that write has to serialize against the lockdown
/// guard's rollback; `ReusedDirLock::write_default_acl_guarded` is the only correct writer.
///
/// Gated as `MetadataOp::Chmod`, the bucket for single-inode permission writes.
async fn set_or_remove_acl_fd(
    fd: BorrowedFd<'_>,
    side: congestion::Side,
    name: &'static CStr,
    blob: Option<Vec<u8>>,
) -> std::io::Result<()> {
    let owned = fd.try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::Chmod, move || {
        apply_one_acl(owned.as_raw_fd(), name, blob.as_deref())
    })
    .await
}

/// Resolve the ACL payload an applier should install: `None` when ACL preservation is off for this
/// entry kind, `Some(acls)` when it is on and the caller carried the source's ACLs.
///
/// The third combination — preservation on, but the call site did not read the source's ACLs — is
/// an ERROR rather than either silent alternative. Treating it as "leave the destination alone"
/// keeps whatever the destination inherited, and treating it as an empty [`Acls`] CLEARS what the
/// source had; both produce a destination whose ACLs did not come from its source, which is exactly
/// the quiet lie ACL preservation exists to remove. Consolidated here so a reader verifies it once
/// rather than at each applier.
///
/// The local copy carries ACLs from the source fd and the remote destination carries them in the
/// wire header. The error arm is a fail-closed backstop for any applier that omits that payload.
fn acls_to_apply(want: bool, acls: Option<&Acls>) -> std::io::Result<Option<&Acls>> {
    match (want, acls) {
        (false, _) => Ok(None),
        (true, Some(acls)) => Ok(Some(acls)),
        (true, None) => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "ACL preservation was requested but this copy path does not carry the source's ACLs",
        )),
    }
}

/// The `listxattr` family's two-step protocol, shared by the fd and path forms: try a stack
/// buffer, and only on `ERANGE` ask the kernel for the current size and retry on the heap.
///
/// `call(buf, len)` issues one syscall; `call(null, 0)` asks for the size without writing.
///
/// An `EOPNOTSUPP` filesystem yields an empty list: it holds no xattrs, so it holds no ACL either.
fn listxattr_retry(
    mut call: impl FnMut(*mut libc::c_char, usize) -> isize,
) -> std::io::Result<Vec<u8>> {
    // a name list is well under 256 bytes even with `security.selinux` and a couple of user
    // attributes present; the ERANGE path below covers anything larger.
    let mut buf = [0u8; 256];
    let n = call(buf.as_mut_ptr().cast(), buf.len());
    if n >= 0 {
        return Ok(buf[..n as usize].to_vec());
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EOPNOTSUPP) => Ok(Vec::new()),
        Some(libc::ERANGE) => {
            let size = call(std::ptr::null_mut(), 0);
            if size < 0 {
                return Err(std::io::Error::last_os_error());
            }
            let mut heap = vec![0u8; size as usize];
            if heap.is_empty() {
                return Ok(heap);
            }
            let n = call(heap.as_mut_ptr().cast(), heap.len());
            if n < 0 {
                // a concurrent writer grew the list between the two calls; report it rather than
                // looping against a live competing writer.
                return Err(std::io::Error::last_os_error());
            }
            heap.truncate(n as usize);
            Ok(heap)
        }
        _ => Err(err),
    }
}

/// `flistxattr`, returning the raw NUL-separated name list the kernel produced.
fn flistxattr_names(fd: RawFd) -> std::io::Result<Vec<u8>> {
    // SAFETY: `fd` is a valid open descriptor for the duration of the call and the kernel writes
    // at most `len` bytes into `buf` (or nothing at all when `buf` is null and `len` is 0).
    listxattr_retry(|buf, len| unsafe { libc::flistxattr(fd, buf, len) })
}

/// Whether the NUL-separated `list` produced by [`flistxattr_names`] contains `name`.
fn names_contain(list: &[u8], name: &CStr) -> bool {
    list.split(|byte| *byte == 0).any(|n| n == name.to_bytes())
}

/// `fgetxattr` one attribute, retrying on a larger buffer if the stack one did not fit.
///
/// `Ok(None)` for both "the entry has no such attribute" (`ENODATA`) and "this filesystem cannot
/// hold one" (`EOPNOTSUPP`) — neither has an ACL to preserve.
fn fgetxattr_blob(fd: RawFd, name: &CStr) -> std::io::Result<Option<Vec<u8>>> {
    // a POSIX.1e ACL blob is `4 + 8n` bytes, so 512 holds 63 entries — every realistic ACL in a
    // single syscall.
    let mut buf = [0u8; 512];
    // SAFETY: `fd` is a valid open descriptor for the duration of the call, `name` is a
    // NUL-terminated C string, and the kernel writes at most `buf.len()` bytes into `buf`.
    let n = unsafe { libc::fgetxattr(fd, name.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
    if n >= 0 {
        return Ok(Some(buf[..n as usize].to_vec()));
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ENODATA | libc::EOPNOTSUPP) => Ok(None),
        Some(libc::ERANGE) => {
            // SAFETY: as above; a null buffer with size 0 asks the kernel for the current size
            // without writing anything.
            let size = unsafe { libc::fgetxattr(fd, name.as_ptr(), std::ptr::null_mut(), 0) };
            if size < 0 {
                return Err(std::io::Error::last_os_error());
            }
            let mut heap = vec![0u8; size as usize];
            if heap.is_empty() {
                return Ok(Some(heap));
            }
            // SAFETY: as above; `heap` has `len()` writable bytes.
            let n =
                unsafe { libc::fgetxattr(fd, name.as_ptr(), heap.as_mut_ptr().cast(), heap.len()) };
            if n < 0 {
                // a concurrent writer grew the attribute between the two calls; report it rather
                // than looping against a live competing writer.
                return Err(std::io::Error::last_os_error());
            }
            heap.truncate(n as usize);
            Ok(Some(heap))
        }
        _ => Err(err),
    }
}

/// Install `blob` as `name`, or REMOVE `name` when `blob` is `None`.
///
/// The error handling is asymmetric, and the asymmetry is the point:
///
/// - A failing SET fails the entry, `EOPNOTSUPP` included. A destination that cannot hold the
///   source's ACL ends up MORE PERMISSIVE than its source — a named entry narrower than `other`
///   acts as a deny in effect, so dropping it grants exactly what the source withheld. Dropping it
///   quietly is the bug this feature exists to remove; a destination filesystem without ACL support
///   is a failure, not a shrug.
/// - A failing REMOVE with `ENODATA` or `EOPNOTSUPP` is SUCCESS. Both say there is no ACL on the
///   destination to clear, so nothing can have widened — the post-condition the caller asked for
///   already holds.
fn apply_one_acl(fd: RawFd, name: &CStr, blob: Option<&[u8]>) -> std::io::Result<()> {
    let Some(blob) = blob else {
        // SAFETY: `fd` is a valid open descriptor for the duration of the call and `name` is a
        // NUL-terminated C string.
        if unsafe { libc::fremovexattr(fd, name.as_ptr()) } == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        return match err.raw_os_error() {
            Some(libc::ENODATA | libc::EOPNOTSUPP) => Ok(()),
            _ => Err(err),
        };
    };
    // SAFETY: as above; `blob` points at `blob.len()` readable bytes. Flags 0 means "create or
    // replace", which is what preserving a source's ACL onto a fresh destination needs.
    let rc = unsafe {
        libc::fsetxattr(
            fd,
            name.as_ptr(),
            blob.as_ptr().cast(),
            blob.len(),
            0, // neither XATTR_CREATE nor XATTR_REPLACE: set it whatever the destination holds
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

// ── The ACL-preservation settings notice ────────────────────────────────────────
//
// `all` does not preserve ACLs, which can silently make a destination wider than its source. The
// useful warning follows directly from the requested settings; inspecting one mutable source root
// would neither establish anything about its descendants nor provide a durable security fact. Keep
// the notice syscall-free and conditional instead of paying for an opportunistic filesystem probe.

/// Guards the settings notice so it runs at most once per process, no matter how many source
/// operands or `--dereference` recursions reach a call site.
static ROOT_ACL_NOTICE_EMITTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// What a run's settings say about the ACL-preservation notice.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct AclPreservationNotice {
    /// Whether this run asked for the kind of fidelity a dropped ACL would undermine, normally
    /// [`crate::preserve::Settings::requests_preservation`].
    ///
    /// `false` silences the notice unless strict mode arms it independently. A preserve-none copy
    /// already declines metadata fidelity generally, so singling out ACLs there would promise a
    /// completeness nobody requested.
    pub wanted: bool,
    /// Whether file ACLs are already safe. `rlink` also sets this for no-update hard links, whose
    /// destination shares the source inode.
    pub file_acl_preserved: bool,
    /// Whether directory ACLs (access and default) are already safe.
    pub dir_acl_preserved: bool,
}

impl AclPreservationNotice {
    /// The notice a copy governed by `preserve` wants.
    #[must_use]
    pub fn for_preserve(preserve: &crate::preserve::Settings) -> Self {
        Self {
            wanted: preserve.requests_preservation(),
            file_acl_preserved: preserve.file.acl,
            dir_acl_preserved: preserve.dir.acl,
        }
    }

    fn could_warn(self, strict: bool) -> bool {
        (self.wanted || strict) && (!self.file_acl_preserved || !self.dir_acl_preserved)
    }
}

/// Warn once per process when requested settings can omit POSIX ACLs.
///
/// This is intentionally settings-only. A source-root observation could be raced and would say
/// nothing about descendants, so it cannot justify filesystem syscalls or an identity claim.
pub fn warn_if_acls_may_be_unpreserved(notice: AclPreservationNotice) {
    let strict = strict_operand_resolution();
    if !notice.could_warn(strict)
        || ROOT_ACL_NOTICE_EMITTED.swap(true, std::sync::atomic::Ordering::Relaxed)
    {
        return;
    }
    let omitted = match (notice.file_acl_preserved, notice.dir_acl_preserved) {
        (false, false) => "files and directories",
        (false, true) => "files",
        (true, false) => "directories",
        (true, true) => unreachable!("could_warn rejects fully preserved ACL settings"),
    };
    if strict {
        tracing::warn!(
            target: crate::NOTICE_TARGET,
            "This copy does not preserve POSIX ACLs for {omitted}. If such source entries carry \
             ACLs, the destination may become more permissive. --require-toctou-safe prevents \
             destination ACL inheritance but does not carry source ACLs across. Use \
             `--preserve-settings=all+acl`."
        );
    } else {
        tracing::warn!(
            target: crate::NOTICE_TARGET,
            "This copy does not preserve POSIX ACLs for {omitted}. If such source entries carry \
             ACLs, the destination may become more permissive. Use \
             `--preserve-settings=all+acl`."
        );
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
// Every applier does chown BEFORE chmod: an unprivileged `fchown` clears
// setuid/setgid on a regular file, so the chmod has to come after to restore
// them. `set_file_metadata_fd` additionally puts the widening step LAST, after
// utimens — it is what takes a destination file from the owner-only mode it was
// created at (`DST_FILE_CREATE_MODE`) to the source mode, so it must not land
// until every other fallible step has succeeded. Ordering the two is free:
// `fchmod` touches ctime only, never atime/mtime, so the timestamps `futimens`
// installs are the same either way. All syscalls are gated through
// `run_metadata_probed_blocking` with `MetadataOp::Chmod`, bucketing
// chown/chmod/utimens/ACL together.

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
    // originating Handle is dropped after the closure starts. started spawn_blocking work is not
    // cancellable.
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
async fn read_link_handle(
    handle: &Handle,
    side: congestion::Side,
) -> std::io::Result<std::path::PathBuf> {
    let owned = handle.as_fd().try_clone_to_owned()?;
    run_metadata_probed_blocking(side, congestion::MetadataOp::ReadLink, move || {
        read_link_fd(owned.as_fd())
    })
    .await
}

fn read_link_fd(fd: BorrowedFd<'_>) -> std::io::Result<std::path::PathBuf> {
    use std::os::unix::ffi::OsStringExt;

    // a symlink target is bounded by PATH_MAX, so a single buffer of that size never truncates.
    let mut buf = vec![0u8; libc::PATH_MAX as usize];
    // SAFETY: `fd` is valid for the duration of this call; the empty C string selects the fd's own
    // symlink (it was opened O_PATH|O_NOFOLLOW); `buf` has `len()` bytes.
    let n = unsafe {
        libc::readlinkat(
            fd.as_raw_fd(),
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

/// Apply file metadata (owner, timestamps, mode, ACL) to an already-open writable
/// file descriptor, in the chown → utimens → mode order.
///
/// `fd` must be the destination file's own fd (typically the write fd returned
/// by [`Dir::create_file`]); this avoids the redundant `File::open` re-open a
/// path-based applier would need, and closes the TOCTOU window in the process.
/// Gating on `settings.file`: chown only when uid or gid is requested, chmod
/// always (the masked mode honors `mode_mask`), timestamps only when requested, ACLs only when
/// `settings.file.acl` is on — in which case `acls` must carry what was read from the source, or
/// the call fails rather than guessing (see `acls_to_apply`).
///
/// # The widening invariant
///
/// > EXACTLY ONE step widens the destination from its owner-only create mode, and it is the LAST
/// > fallible step here. Without a source access ACL that step is the `fchmod`; with one it is the
/// > `fsetxattr`, and the `fchmod` before it is narrowed to `(mode & 0o7000) | DST_FILE_CREATE_MODE`
/// > so that it cannot widen.
///
/// This is what makes [`DST_FILE_CREATE_MODE`] safe to use for every destination file: a failure
/// part-way through metadata application must not publish the final mode on a file the copy is
/// about to report as failed. The mode is also decided UNCONDITIONALLY here — this call is the
/// single place it happens, so no `--preserve` setting can leave a successfully copied file
/// owner-only.
///
/// An access ACL has to take that slot because applying one IS a mode-changing operation: the ACL's
/// `MASK` entry becomes the mode's group bits, so `fsetxattr` widens, and a `chmod` AFTER it would
/// re-derive `MASK` from the mode and destroy the ACL's fidelity. The two want the same slot, and
/// the resolution is verified kernel behavior: `fsetxattr` sets the rwx bits exactly (`USER_OBJ`,
/// `MASK`, `OTHER`) and PRESERVES setuid/setgid/sticky, so the preceding `fchmod` can carry the
/// special bits alone. Its result — special bits over an owner-only, non-executable mode — grants
/// nothing on its own, so a failing `fsetxattr` leaves a file no one else can reach.
///
/// Clearing an ACL, by contrast, is mode-neutral and can only narrow, so when the source has none
/// the removal is safe to run BEFORE the widening chmod (verified: a `0o600` file stays `0o600`
/// across `fremovexattr`).
///
/// A file whose copy fails anywhere before that widening step therefore stays owner-only —
/// deliberately. It is reported as an error, and its size/mtime then *normally* differ from the
/// source so a later run re-copies it. Two cases where they do not, leaving the file owner-only
/// until something else re-copies it: a failure at the widening step ITSELF, which comes after the
/// timestamps have been applied, so the destination matches its source on both size and mtime; and
/// the nanosecond concession in [`metadata_equal`], which skips that comparison when either side's
/// `mtime_nsec` is zero, so even a file that never reached `futimens` compares equal when its write
/// and the source's mtime land in the same whole second. See `docs/tocttou.md`.
///
/// [`metadata_equal`]: crate::filecmp::metadata_equal
pub async fn set_file_metadata_fd<Meta: crate::preserve::Metadata>(
    settings: &crate::preserve::Settings,
    meta: &Meta,
    acls: Option<&Acls>,
    fd: BorrowedFd<'_>,
    side: congestion::Side,
) -> std::io::Result<()> {
    let ut = &settings.file.user_and_time;
    if ut.uid || ut.gid {
        let uid = if ut.uid { Some(meta.uid()) } else { None };
        let gid = if ut.gid { Some(meta.gid()) } else { None };
        fchown_fd(fd, side, uid, gid).await?;
    }
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
    let acls = acls_to_apply(settings.file.acl, acls)?;
    let mode = crate::preserve::masked_mode(settings.file.mode_mask, meta);
    if let Some(acls) = acls.filter(|acls| acls.access.is_some()) {
        // the ACL is the widening step; this chmod carries only the special bits
        fchmod_fd(fd, side, (mode & 0o7000) | DST_FILE_CREATE_MODE).await?;
        apply_acls_fd(fd, side, acls, false).await?;
    } else {
        // clearing an inherited ACL can only narrow, so it is safe before the widening chmod
        if let Some(acls) = acls {
            apply_acls_fd(fd, side, acls, false).await?;
        }
        fchmod_fd(fd, side, mode).await?;
    }
    Ok(())
}

/// Apply directory metadata (owner, mode, ACLs, timestamps) to an open [`Dir`] fd,
/// following the chown → mode → utimens ordering. Gates on `settings.dir` and
/// uses the directory's own congestion side.
///
/// The mode step is the same two-branch shape as [`set_file_metadata_fd`] — see the widening
/// invariant there — with [`DST_DIR_CREATE_MODE`] as the create mode. It stays in the directory
/// applier's existing chmod slot rather than moving to the end: `futimens` is last for a directory
/// because a directory's mtime is bumped by every child created in it, and unlike a file's mode a
/// directory's is applied post-order, once its children exist. The DEFAULT ACL rides along with the
/// access one; being mode-neutral, it constrains nothing.
///
/// # One asymmetry a directory has and a file does not
///
/// A destination file is always freshly created, so the ACL branch's narrowed `fchmod` only ever
/// holds it at the mode it already had. A destination DIRECTORY may be REUSED — an existing `dst/`
/// that rcp is copying into — and there that same `fchmod` actively NARROWS it, to `0o700` plus its
/// special bits, for the moment between the chmod and the `fsetxattr`. If the `fsetxattr` then
/// fails, the copy is reported failed and a long-lived directory is left at `0o700`, which is not
/// the mode it started at.
///
/// That is fail-closed, and deliberately so: the alternative is publishing the source's mode on a
/// directory whose ACL never landed, which is the wider outcome. But it means an operator whose
/// destination directory comes back owner-only after a failed `d:acl` copy is looking at this step,
/// not at a bug. Branch 2 has no such window — it only ever chmods once, to the final mode.
pub async fn set_dir_metadata_fd<Meta: crate::preserve::Metadata>(
    settings: &crate::preserve::Settings,
    meta: &Meta,
    acls: Option<&Acls>,
    dir: &Dir,
) -> std::io::Result<()> {
    set_dir_metadata_fd_inner(settings, meta, acls, dir, true).await
}

/// The body of [`set_dir_metadata_fd`], parameterized over whether the DEFAULT ACL is applied
/// here. `apply_default` is `false` for exactly one caller — [`set_reused_dir_metadata_fd`]'s
/// locked path, which owns the default ACL through the lockdown guard: that write must serialize
/// against the guard's rollback, and an unguarded write from here is precisely the detached-write
/// hazard the guard closes (see `DefaultAclGuard`). Everything else — including the access ACL's
/// claim on the widening slot — is identical in both modes.
async fn set_dir_metadata_fd_inner<Meta: crate::preserve::Metadata>(
    settings: &crate::preserve::Settings,
    meta: &Meta,
    acls: Option<&Acls>,
    dir: &Dir,
    apply_default: bool,
) -> std::io::Result<()> {
    let side = dir.side();
    let fd = dir.fd.as_fd();
    let ut = &settings.dir.user_and_time;
    if ut.uid || ut.gid {
        let uid = if ut.uid { Some(meta.uid()) } else { None };
        let gid = if ut.gid { Some(meta.gid()) } else { None };
        fchown_fd(fd, side, uid, gid).await?;
    }
    let acls = acls_to_apply(settings.dir.acl, acls)?;
    let mode = crate::preserve::masked_mode(settings.dir.mode_mask, meta);
    if let Some(acls) = acls.filter(|acls| acls.access.is_some()) {
        // the ACL is the widening step; this chmod carries only the special bits
        fchmod_fd(fd, side, (mode & 0o7000) | DST_DIR_CREATE_MODE).await?;
        apply_acls_fd(fd, side, acls, apply_default).await?;
    } else {
        // no source access ACL, so the chmod is the widening step and stays last. Both applies that
        // can run here are safe before it: clearing an inherited ACL can only narrow, and SETTING a
        // default ACL is mode-neutral — it governs what children inherit, never this directory's
        // own mode (a source can have a default ACL and no access one).
        if let Some(acls) = acls {
            apply_acls_fd(fd, side, acls, apply_default).await?;
        }
        fchmod_fd(fd, side, mode).await?;
    }
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

/// Apply directory metadata to a REUSED destination directory that may have been locked down under
/// [`strict_operand_resolution`], restoring any uid takeover correctly and WITHOUT a transient
/// window in which a hostile prior owner regains control.
///
/// `lock` is what [`lockdown_reused_dir`] recorded, or `None` for a freshly created directory
/// (nothing to restore). When present, this restores the original uid only when uid is NOT being
/// preserved from the source; otherwise [`set_dir_metadata_fd`] changes the copier uid directly to
/// the source uid. The directory's transient owner is therefore always the copier or the final
/// owner, never a prior owner who will not own the final directory. Lockdown already restored and
/// verified the original gid on the same held fd, and nothing before final metadata mutates it, so a
/// non-preserved gid needs no no-op write.
///
/// # Resolving the two ACLs
///
/// The ACCESS ACL keeps its usual applier semantics (set or cleared by the inner applier when
/// `d:acl` is on, untouched otherwise). The DEFAULT ACL of a LOCKED directory is different: it is
/// resolved HERE, first, through the lockdown guard, and the inner applier is told to leave it
/// alone (`set_dir_metadata_fd_inner(…, apply_default: false)`):
///
/// 1. `settings.dir.acl` on → the SOURCE's default ACL is installed. When the source has none
///    there is nothing to write: the lockdown already left the directory without one, which is the
///    correct end state. The copy is preserving ACLs, so what the destination carried before is
///    not what it should end with.
/// 2. otherwise → the directory's ORIGINAL default ACL is restored (nothing to do when there was
///    none). rcp was not asked to change this directory's ACLs; the lockdown removed it only so
///    children created during the copy could not inherit it, and failing to put it back would
///    permanently destroy an ACL the destination had before the copy. That makes a failure here an
///    ERROR, unlike the lockdown strip, where `EOPNOTSUPP` is tolerated (a filesystem that cannot
///    hold an ACL cannot have had one).
///
/// Both writes go through `ReusedDirLock::write_default_acl_guarded`, which serializes them
/// against the guard's `Drop` rollback: this future can be cancelled with the write already
/// detached on the blocking pool, and an unserialized write could land AFTER the rollback and
/// silently undo it (see `DefaultAclGuard`). The guard stays armed through every remaining
/// fallible step and every cancellation point up to the successful return — and is disarmed only
/// immediately before `Ok(())`, so a failure or a cancellation anywhere in between rolls the
/// directory back to ITS OWN original default ACL —
/// including the case where that original is "none" and the rollback must REMOVE a source ACL
/// that was just installed.
///
/// The guarded write runs FIRST, unwinding the lockdown in the reverse of the order it was applied
/// (ACL removed last, so resolved first; then the owner; then the mode, via the chmod inside the
/// inner applier). Two things make that placement deliberate:
///
/// - **It runs while the copier still owns the directory**, before the owner is handed back. Only
///   an owner or a `CAP_FOWNER` copier may write these attributes.
/// - **It fails toward *unchanged*.** If a later step fails, the armed guard leaves the directory
///   holding (or rolled back to) the default ACL it had before the copy. Deferring it past the
///   chmod would instead fail toward *damaged* — a directory permanently stripped of an ACL it
///   owned before rcp touched it, which is the one outcome this function is required to treat as
///   an error rather than produce.
///
/// The guarded write is mode-neutral, so unlike the SOURCE-ACL applies in the inner applier it
/// does not compete with the finalize chmod for the widening slot: only a DEFAULT ACL goes through
/// it, and a default ACL never moves its own directory's mode. Any implementation that also
/// restores an ACCESS ACL must do so before chmod: the snapshot's `USER_OBJ`/`MASK`/`OTHER` agree
/// with the destination directory's original mode, not the source's, so a later restore would
/// install the wrong rwx bits.
pub async fn set_reused_dir_metadata_fd<Meta: crate::preserve::Metadata>(
    settings: &crate::preserve::Settings,
    meta: &Meta,
    acls: Option<&Acls>,
    lock: Option<ReusedDirLock>,
    dir: &Dir,
) -> std::io::Result<()> {
    // a fresh / non-strict directory (lock == None) uses the shared metadata applier directly. only a
    // strict-mode locked reused directory needs the uid/default-ACL state below.
    // NB: `ReusedDirLock` is an RAII guard, so it cannot be destructured by move — and must not be,
    // since dropping its parts separately is exactly the bug it exists to prevent. It stays whole
    // until it is deliberately disarmed below.
    let Some(lock) = lock else {
        return set_dir_metadata_fd(settings, meta, acls, dir).await;
    };
    // resolve the default ACL first, through the guard — see "Resolving the two ACLs" above for
    // the two cases, the serialization against the guard's rollback, and why a failure must leave
    // the guard armed (`Drop` then gets one synchronous rollback attempt rather than the bytes
    // dying with the error)
    let final_default = if settings.dir.acl {
        // fail closed exactly as the applier does when the call site carried no ACLs
        acls_to_apply(true, acls)?.and_then(|acls| acls.default.clone())
    } else {
        lock.original_default_acl()
    };
    if let Some(blob) = final_default {
        lock.write_default_acl_guarded(dir.side, blob).await?;
    }
    let ut = &settings.dir.user_and_time;
    if !ut.uid
        && let Some(orig_uid) = lock.restore_uid
    {
        // lockdown already restored and verified orig_gid on this same fd, and nothing in the
        // interim mutates the directory's gid. restore only the uid that lockdown actually changed;
        // the inner applier handles either component selected from the source.
        fchown_fd(dir.fd.as_fd(), dir.side, Some(orig_uid), None).await?;
    }
    // the default ACL is excluded (`apply_default: false`): it was resolved through the guard
    // above, and only the guard's serialized writer may touch it while the lockdown is live
    set_dir_metadata_fd_inner(settings, meta, acls, dir, false).await?;
    // disarm only after the last fallible/cancellable finalize step. every earlier exit leaves the
    // guard armed so its `Drop` restores the destination's original default ACL. a final re-stat
    // would not strengthen this: handing back the final uid/mode also hands its owner the ability to
    // race that observation or change the state immediately after it.
    lock.disarm();
    Ok(())
}

/// Apply symlink metadata (owner and timestamps only — never mode) to a target-validated symlink
/// [`Handle`], operating on the link itself via `AT_EMPTY_PATH`.
///
/// Symlinks have no meaningful permission bits, so there is no chmod step;
/// ordering is chown → utimens. Gates on `settings.symlink`. A caller that obtained the handle by a
/// separate create/open sequence must use [`Dir::set_symlink_metadata_at`] to bind the intended
/// target. A caller that read the target through [`Handle::read_symlink_owned`] may call this
/// directly after comparing that observation.
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

/// One blocking job whose queued resources have a defined destruction order.
///
/// Field order is load-bearing: queued user work may own fds, so it must be dropped before the
/// strong admission lease that accounts for them.
struct BlockingJob<F> {
    work: F,
    admission: Option<throttle::BlockingFdAdmissionLease>,
}

type BlockingJobOutput<T> = (
    std::io::Result<T>,
    Option<throttle::BlockingFdAdmissionLease>,
);

impl<F> BlockingJob<F> {
    fn run<T>(self) -> BlockingJobOutput<T>
    where
        F: FnOnce() -> std::io::Result<T>,
    {
        let Self { work, admission } = self;
        (work(), admission)
    }
}

fn take_blocking_job<F>(
    shared_job: &std::sync::Mutex<Option<BlockingJob<F>>>,
) -> Option<BlockingJob<F>> {
    shared_job
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

/// Wait for a blocking job while retaining control of work that has not started.
struct BlockingJobWaiter<F, T> {
    shared_job: Arc<std::sync::Mutex<Option<BlockingJob<F>>>>,
    handle: tokio::task::JoinHandle<Option<BlockingJobOutput<T>>>,
}

impl<F, T> std::future::Future for BlockingJobWaiter<F, T> {
    type Output = Result<Option<BlockingJobOutput<T>>, tokio::task::JoinError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        std::future::Future::poll(std::pin::Pin::new(&mut this.handle), cx)
    }
}

impl<F, T> Drop for BlockingJobWaiter<F, T> {
    fn drop(&mut self) {
        let queued_job = take_blocking_job(&self.shared_job);
        drop(queued_job);
        self.handle.abort();
    }
}

/// Run fd-owning blocking work while retaining any task-scoped admission through cancellation.
///
/// The strong lease lives in the blocking task's output, after `result`, so an abandoned returned
/// [`Dir`], [`Handle`], or file is dropped before the admission slot is released. This lower-level
/// boundary intentionally adds no rate or congestion gating; directory enumeration uses it after
/// consuming its static rate token, while metadata syscalls layer their congestion lifecycle on
/// top in [`run_metadata_probed_blocking_no_rate`].
///
/// Until the worker takes the shared job, dropping the waiter synchronously destroys its user work
/// before its lease. Once the worker takes it, the job is considered started and runs detached if
/// the waiter is cancelled.
pub(crate) async fn run_fd_admitted_blocking<F, T>(f: F) -> std::io::Result<T>
where
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    // upgrade immediately before submission. if the ambient weak reference has expired (ordinary
    // directory recursion dropped its provisional guard), this remains an unadmitted directory
    // operation; an admitted outer leaf such as copy -> recursive rm stays represented.
    let blocking_admission = FD_ADMISSION
        .try_with(throttle::FdAdmission::blocking_lease)
        .ok();
    let shared_job = Arc::new(std::sync::Mutex::new(Some(BlockingJob {
        work: f,
        admission: blocking_admission,
    })));
    let worker_job = Arc::clone(&shared_job);
    let handle =
        tokio::task::spawn_blocking(move || take_blocking_job(&worker_job).map(BlockingJob::run));
    let (result, _blocking_admission) = BlockingJobWaiter { shared_job, handle }
        .await
        .map_err(std::io::Error::other)?
        .expect("blocking job disappeared while its waiter was alive");
    result
}

/// Run a blocking metadata syscall closure on the blocking pool, gated by the
/// congestion controller for the given side and operation kind.
///
/// Each per-entry `openat`/`fstatat` is rate-gated, counted against the cwnd permit, and feeds the
/// latency probe — the same per-metadata-syscall gating shape used throughout this crate. The cwnd
/// guard and probe move into the blocking closure because cancelling the async waiter cannot stop
/// a syscall that has started; releasing either in the waiter would undercount the actual in-flight
/// work and its latency.
pub async fn run_metadata_probed_blocking<F, T>(
    side: congestion::Side,
    op: congestion::MetadataOp,
    f: F,
) -> std::io::Result<T>
where
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    throttle::get_ops_token().await;
    run_metadata_probed_blocking_no_rate(side, op, f).await
}

/// Variant of [`run_metadata_probed_blocking`] for a caller that already consumed the static
/// operations-rate token before spawning its task.
pub async fn run_metadata_probed_blocking_no_rate<F, T>(
    side: congestion::Side,
    op: congestion::MetadataOp,
    f: F,
) -> std::io::Result<T>
where
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let ops_permit = throttle::ops_in_flight_permit(crate::walk::meta_resource(side, op)).await;
    let probe = congestion::Probe::start_metadata(side, op);
    run_fd_admitted_blocking(move || {
        let result = f();
        match &result {
            Ok(_) => probe.complete_ok(0),
            Err(_) => probe.discard(),
        }
        // the cwnd models syscall concurrency, so release it when `f` finishes. The shared blocking
        // boundary keeps admission in the task output because `f` can return an fd that an
        // abandoned waiter never receives.
        drop(ops_permit);
        result
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
    async fn preview_operand_descent_rejects_a_symlink_in_the_relative_prefix() -> anyhow::Result<()>
    {
        let fixture = testutils::create_temp_dir().await?;
        let named_root = fixture.join("named-root");
        let outside = fixture.join("outside");
        tokio::fs::create_dir_all(&named_root).await?;
        tokio::fs::create_dir_all(outside.join("level-two")).await?;
        tokio::fs::symlink(&outside, named_root.join("level-one")).await?;

        let opened = open_existing_dir_beneath_operand(
            &named_root,
            Path::new("level-one/level-two"),
            congestion::Side::Destination,
        )
        .await?;

        assert!(opened.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn preview_operand_relative_path_accepts_empty_and_rejects_non_normal_components()
    -> anyhow::Result<()> {
        let fixture = testutils::create_temp_dir().await?;
        let named_root = fixture.join("named-root");
        tokio::fs::create_dir(&named_root).await?;

        assert!(
            open_existing_dir_beneath_operand(
                &named_root,
                Path::new(""),
                congestion::Side::Destination,
            )
            .await?
            .is_some(),
            "an empty relative path must open the named root"
        );
        for invalid in ["/absolute", ".", "..", "one/./two", "one/../two"] {
            let error = open_existing_dir_beneath_operand(
                &named_root,
                Path::new(invalid),
                congestion::Side::Destination,
            )
            .await
            .expect_err("a non-normal relative path must be rejected");
            assert_eq!(error.raw_os_error(), Some(libc::EINVAL), "{invalid:?}");
        }
        Ok(())
    }

    mod max_files_in_flight_tests {
        use super::*;
        use anyhow::Context as _;
        use futures::FutureExt as _;

        struct BlockingDropFd {
            _file: std::fs::File,
            drop_started: Option<tokio::sync::oneshot::Sender<()>>,
            release_drop: std::sync::mpsc::Receiver<()>,
            _completion: crate::testutils::CompletionSignal,
        }

        impl Drop for BlockingDropFd {
            fn drop(&mut self) {
                if let Some(drop_started) = self.drop_started.take() {
                    let _ = drop_started.send(());
                }
                self.release_drop
                    .recv()
                    .expect("drop release sender must stay alive");
            }
        }

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        struct QueuedFdDropObservation {
            fd_was_closed: bool,
            admission_was_retained: bool,
        }

        struct QueuedFdCapture {
            file: Option<std::fs::File>,
            probe: crate::testutils::FdIdentityProbe,
            observation: Arc<std::sync::Mutex<Option<std::io::Result<QueuedFdDropObservation>>>>,
        }

        impl Drop for QueuedFdCapture {
            fn drop(&mut self) {
                drop(self.file.take());
                let admission_was_retained = throttle::open_file_permit().now_or_never().is_none();
                *self
                    .observation
                    .lock()
                    .expect("queued fd drop observation lock poisoned") =
                    Some(self.probe.original_is_closed().map(|fd_was_closed| {
                        QueuedFdDropObservation {
                            fd_was_closed,
                            admission_was_retained,
                        }
                    }));
            }
        }

        /// Dropping a queued blocking waiter must synchronously close its captured fd and return
        /// admission before the occupied worker is released, without ever running user work.
        #[test]
        fn dropping_queued_blocking_work_closes_fd_before_returning_admission() -> anyhow::Result<()>
        {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(1)
                .enable_all()
                .build()?;
            runtime.block_on(async {
                let root = crate::testutils::create_temp_dir().await?;
                let file_path = root.join("queued-capture");
                tokio::fs::write(&file_path, b"x").await?;
                let admission = crate::testutils::AdmissionLimit::new().await;
                admission.set_files_in_flight(1);
                let open_file_guard = throttle::open_file_permit().await;
                let file = std::fs::File::open(file_path)?;
                let raw_fd = file.as_raw_fd();
                let probe = match crate::testutils::FdIdentityProbe::capture(raw_fd) {
                    Ok(probe) => probe,
                    Err(error) => {
                        drop(file);
                        drop(open_file_guard);
                        drop(admission);
                        let cleanup_result = tokio::fs::remove_dir_all(root).await;
                        return match cleanup_result {
                            Ok(()) => Err(error.into()),
                            Err(cleanup_error) => Err(anyhow::Error::new(error).context(format!(
                                "queued fd fixture did not clean up: {cleanup_error:#}"
                            ))),
                        };
                    }
                };
                let drop_observation = Arc::new(std::sync::Mutex::new(None));

                let (blocker_started_tx, blocker_started_rx) = tokio::sync::oneshot::channel();
                let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = blocker_started_tx.send(());
                    release_blocker_rx
                        .recv()
                        .expect("blocking worker release sender must stay alive");
                });
                blocker_started_rx
                    .await
                    .context("the sole blocking worker did not start")?;

                let (queued_started_tx, queued_started_rx) = tokio::sync::oneshot::channel();
                {
                    let admission = open_file_guard.admission();
                    let queued_capture = QueuedFdCapture {
                        file: Some(file),
                        probe,
                        observation: Arc::clone(&drop_observation),
                    };
                    let mut waiter = Box::pin(with_fd_admission(admission, async move {
                        run_fd_admitted_blocking(move || {
                            let _queued_capture = queued_capture;
                            let _ = queued_started_tx.send(());
                            Ok(())
                        })
                        .await
                    }));
                    assert!(
                        futures::poll!(waiter.as_mut()).is_pending(),
                        "the admitted blocking closure must queue behind the occupied worker"
                    );
                    // leave the blocking lease as the sole strong admission owner before
                    // exercising the queued-job drop order
                    drop(open_file_guard);
                }

                // preserve probe failures until the occupied worker has been released and joined.
                let fd_was_closed_before_release = probe.original_is_closed();
                let drop_observation_before_release = drop_observation
                    .lock()
                    .expect("queued fd drop observation lock poisoned")
                    .take()
                    .transpose();
                let mut next_permit = Box::pin(throttle::open_file_permit());
                let (admission_returned_before_release, returned_admission) =
                    match futures::poll!(next_permit.as_mut()) {
                        std::task::Poll::Ready(permit) => (true, Some(permit)),
                        std::task::Poll::Pending => (false, None),
                    };

                let release_result = release_blocker_tx.send(());
                let blocker_result = blocker.await;
                let queued_start_result =
                    tokio::time::timeout(std::time::Duration::from_secs(5), queued_started_rx)
                        .await;
                let capacity_error = match returned_admission {
                    Some(permit) => {
                        drop(permit);
                        None
                    }
                    None => tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        next_permit.as_mut(),
                    )
                    .await
                    .map(drop)
                    .map_err(anyhow::Error::new)
                    .map_err(|error| {
                        error.context("queued blocking work did not eventually return admission")
                    })
                    .err(),
                };
                drop(admission);
                let cleanup_result = tokio::fs::remove_dir_all(root).await;

                let release_error = release_result.err().map(|error| {
                    anyhow::Error::new(error)
                        .context("the blocking worker ended before its release")
                });
                let blocker_error = blocker_result
                    .err()
                    .map(|error| anyhow::Error::new(error).context("the blocking worker panicked"));
                let fd_was_closed_before_release = fd_was_closed_before_release?;
                let drop_observation_before_release = drop_observation_before_release?;
                if let Some(error) = capacity_error {
                    return Err(error);
                }
                if let Some(error) = release_error {
                    return Err(error);
                }
                if let Some(error) = blocker_error {
                    return Err(error);
                }
                cleanup_result?;

                assert!(
                    fd_was_closed_before_release,
                    "dropping the queued waiter left its captured fd open"
                );
                assert!(
                    admission_returned_before_release,
                    "dropping the queued waiter retained fd admission"
                );
                assert_eq!(
                    drop_observation_before_release,
                    Some(QueuedFdDropObservation {
                        fd_was_closed: true,
                        admission_was_retained: true,
                    }),
                    "the queued work must close its fd before its admission lease drops"
                );
                assert!(
                    matches!(queued_start_result, Ok(Err(_))),
                    "the dropped queued blocking work started"
                );
                Ok(())
            })
        }

        /// Cancelling an unprobed blocking waiter must retain any ambient fd admission until its
        /// detached closure releases the descriptor it owns. Directory enumeration uses this
        /// lower-level boundary because `getdents` is deliberately excluded from congestion
        /// probing.
        #[tokio::test]
        async fn cancelled_unprobed_waiter_keeps_admission_until_fd_owner_finishes()
        -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let file_path = root.join("held-unprobed");
            tokio::fs::write(&file_path, b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_files_in_flight(1);
            let open_file_guard = throttle::open_file_permit().await;
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (completion, completion_rx) = crate::testutils::CompletionSignal::new();
            let task = tokio::spawn(async move {
                let admission = open_file_guard.admission();
                with_fd_admission(admission, async move {
                    let _open_file_guard = open_file_guard;
                    run_fd_admitted_blocking(move || {
                        let _completion = completion;
                        let _file = std::fs::File::open(file_path)?;
                        let _ = started_tx.send(());
                        release_rx.recv().map_err(std::io::Error::other)?;
                        Ok(())
                    })
                    .await
                })
                .await
            });
            let started_result = started_rx.await;
            task.abort();
            let waiter_was_cancelled = matches!(task.await, Err(error) if error.is_cancelled());

            let mut second_permit = Box::pin(throttle::open_file_permit());
            let admission_was_retained =
                started_result.is_ok() && futures::poll!(second_permit.as_mut()).is_pending();
            let release_result = release_tx.send(());
            let (completion_result, permit) = crate::testutils::await_completion_and_capacity(
                completion_rx,
                second_permit.as_mut(),
            )
            .await;
            started_result.context("detached unprobed work did not start")?;
            release_result.context("detached unprobed work ended before its release")?;
            completion_result.context("detached unprobed work did not report completion")?;
            drop(permit);
            assert!(waiter_was_cancelled);
            assert!(
                admission_was_retained,
                "cancelling the waiter released admission while detached work held an fd"
            );
            Ok(())
        }

        /// Cancelling a metadata waiter must not release admission while its detached blocking
        /// closure still owns a file descriptor.
        #[tokio::test]
        async fn cancelled_metadata_waiter_keeps_admission_until_fd_owner_finishes()
        -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let file_path = root.join("held");
            tokio::fs::write(&file_path, b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_files_in_flight(1);
            let stat_resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            admission.set_max_ops_in_flight(stat_resource, 1);
            let open_file_guard = throttle::open_file_permit().await;
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (completion, completion_rx) = crate::testutils::CompletionSignal::new();
            let task = tokio::spawn(async move {
                let admission = open_file_guard.admission();
                with_fd_admission(admission, async move {
                    let _open_file_guard = open_file_guard;
                    run_metadata_probed_blocking(
                        congestion::Side::Source,
                        congestion::MetadataOp::Stat,
                        move || {
                            let _completion = completion;
                            let _file = std::fs::File::open(file_path)?;
                            let _ = started_tx.send(());
                            release_rx.recv().map_err(std::io::Error::other)?;
                            Ok(())
                        },
                    )
                    .await
                })
                .await
            });
            let started_result = started_rx.await;
            task.abort();
            let waiter_was_cancelled = matches!(task.await, Err(error) if error.is_cancelled());

            let second_permit = throttle::open_file_permit();
            tokio::pin!(second_permit);
            let admission_was_retained =
                started_result.is_ok() && futures::poll!(second_permit.as_mut()).is_pending();
            let second_stat_permit = throttle::ops_in_flight_permit(stat_resource);
            tokio::pin!(second_stat_permit);
            let metadata_was_retained =
                started_result.is_ok() && futures::poll!(second_stat_permit.as_mut()).is_pending();
            let release_result = release_tx.send(());
            let (completion_result, (permit, stat_permit)) =
                crate::testutils::await_completion_and_capacity(
                    completion_rx,
                    futures::future::join(second_permit.as_mut(), second_stat_permit.as_mut()),
                )
                .await;
            started_result.context("detached metadata work did not start")?;
            release_result.context("detached metadata work ended before its release")?;
            completion_result.context("detached metadata work did not report completion")?;
            drop(permit);
            drop(stat_permit);
            assert!(waiter_was_cancelled);
            assert!(
                admission_was_retained,
                "cancelling the waiter released admission while detached work held an fd"
            );
            assert!(
                metadata_was_retained,
                "cancelling the waiter released metadata capacity while its syscall was live"
            );
            Ok(())
        }

        /// A detached blocking task's returned fd must be dropped before its admission lease. The
        /// fd lives in the abandoned task output rather than inside the syscall closure itself.
        #[tokio::test]
        async fn abandoned_metadata_output_keeps_admission_until_returned_fd_drops()
        -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let file_path = root.join("returned");
            tokio::fs::write(&file_path, b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_files_in_flight(1);
            let open_file_guard = throttle::open_file_permit().await;
            let (work_started_tx, work_started_rx) = tokio::sync::oneshot::channel();
            let (release_work_tx, release_work_rx) = std::sync::mpsc::channel();
            let (drop_started_tx, drop_started_rx) = tokio::sync::oneshot::channel();
            let (release_drop_tx, release_drop_rx) = std::sync::mpsc::channel();
            let (completion, completion_rx) = crate::testutils::CompletionSignal::new();
            let task = tokio::spawn(async move {
                let admission = open_file_guard.admission();
                with_fd_admission(admission, async move {
                    let _open_file_guard = open_file_guard;
                    run_metadata_probed_blocking(
                        congestion::Side::Source,
                        congestion::MetadataOp::Stat,
                        move || {
                            let file = std::fs::File::open(file_path)?;
                            let _ = work_started_tx.send(());
                            release_work_rx.recv().map_err(std::io::Error::other)?;
                            Ok(BlockingDropFd {
                                _file: file,
                                drop_started: Some(drop_started_tx),
                                release_drop: release_drop_rx,
                                _completion: completion,
                            })
                        },
                    )
                    .await
                })
                .await
            });
            let work_started_result = work_started_rx.await;
            task.abort();
            let waiter_was_cancelled = matches!(task.await, Err(error) if error.is_cancelled());

            let release_work_result = release_work_tx.send(());
            let drop_started_result = drop_started_rx.await;
            let admission_was_retained = {
                let second_permit = throttle::open_file_permit();
                tokio::pin!(second_permit);
                drop_started_result.is_ok() && futures::poll!(second_permit.as_mut()).is_pending()
            };
            let release_drop_result = release_drop_tx.send(());
            let (completion_result, permit) = crate::testutils::await_completion_and_capacity(
                completion_rx,
                throttle::open_file_permit(),
            )
            .await;
            drop(permit);
            let cleanup_result = tokio::fs::remove_dir_all(root).await;
            work_started_result.context("detached metadata work did not start")?;
            release_work_result.context("detached metadata work ended before its release")?;
            drop_started_result.context("the abandoned returned fd did not begin dropping")?;
            release_drop_result.context("the abandoned returned fd ended before its release")?;
            completion_result.context("the abandoned returned fd did not report completion")?;
            cleanup_result?;
            assert!(
                waiter_was_cancelled,
                "the async metadata waiter must observe cancellation"
            );
            assert!(
                admission_was_retained,
                "admission was released before the abandoned returned fd was dropped"
            );
            Ok(())
        }

        /// An abandoned unprobed blocking result must drop its fd owner before returning the
        /// strong admission lease. The data-copy path uses this lower-level runner directly.
        #[tokio::test]
        async fn abandoned_unprobed_output_keeps_admission_until_returned_fd_drops()
        -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let file_path = root.join("returned-unprobed");
            tokio::fs::write(&file_path, b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_files_in_flight(1);
            let open_file_guard = throttle::open_file_permit().await;
            let (work_started_tx, work_started_rx) = tokio::sync::oneshot::channel();
            let (release_work_tx, release_work_rx) = std::sync::mpsc::channel();
            let (drop_started_tx, drop_started_rx) = tokio::sync::oneshot::channel();
            let (release_drop_tx, release_drop_rx) = std::sync::mpsc::channel();
            let (completion, completion_rx) = crate::testutils::CompletionSignal::new();
            let task = tokio::spawn(async move {
                let admission = open_file_guard.admission();
                with_fd_admission(admission, async move {
                    let _open_file_guard = open_file_guard;
                    run_fd_admitted_blocking(move || {
                        let file = std::fs::File::open(file_path)?;
                        let _ = work_started_tx.send(());
                        release_work_rx.recv().map_err(std::io::Error::other)?;
                        Ok(BlockingDropFd {
                            _file: file,
                            drop_started: Some(drop_started_tx),
                            release_drop: release_drop_rx,
                            _completion: completion,
                        })
                    })
                    .await
                })
                .await
            });
            let work_started_result = work_started_rx.await;
            task.abort();
            let waiter_was_cancelled = matches!(task.await, Err(error) if error.is_cancelled());
            let release_work_result = release_work_tx.send(());
            let drop_started_result = drop_started_rx.await;
            let admission_was_retained = {
                let second_permit = throttle::open_file_permit();
                tokio::pin!(second_permit);
                drop_started_result.is_ok() && futures::poll!(second_permit.as_mut()).is_pending()
            };
            let release_drop_result = release_drop_tx.send(());
            let (completion_result, permit) = crate::testutils::await_completion_and_capacity(
                completion_rx,
                throttle::open_file_permit(),
            )
            .await;
            drop(permit);
            let cleanup_result = tokio::fs::remove_dir_all(root).await;
            work_started_result.context("detached unprobed work did not start")?;
            release_work_result.context("detached unprobed work ended before its release")?;
            drop_started_result.context("the abandoned returned fd did not begin dropping")?;
            release_drop_result.context("the abandoned returned fd ended before its release")?;
            completion_result.context("the abandoned returned fd did not report completion")?;
            cleanup_result?;
            assert!(
                waiter_was_cancelled,
                "the async waiter must observe cancellation"
            );
            assert!(
                admission_was_retained,
                "admission was released before the abandoned returned fd was dropped"
            );
            Ok(())
        }
    }

    #[test]
    fn a_run_that_asked_for_nothing_wants_no_root_acl_notice() {
        // the gate this whole type exists for: a copy at the preserve-none baseline drops uid,
        // gid, timestamps and the special mode bits silently, so an ACL notice there is advice
        // about a completeness nobody claimed — and it must cost nothing, kind unknown included.
        let notice = AclPreservationNotice::for_preserve(&crate::preserve::preserve_none());
        assert!(!notice.wanted);
        assert!(!notice.could_warn(false));
    }

    #[test]
    fn strict_resolution_arms_the_notice_on_its_own() {
        // `--require-toctou-safe` is a request ABOUT permissions whatever `--preserve-settings`
        // says, and the two flags deliberately do not imply each other, so it must arm the notice
        // by itself — with its own wording.
        let notice = AclPreservationNotice::for_preserve(&crate::preserve::preserve_none());
        assert!(notice.could_warn(true));
    }

    #[test]
    fn the_notice_reports_when_either_entry_kind_omits_acls() {
        // `f:acl` and `d:acl` are independent, so preserving files alone still leaves directory ACLs
        // worth mentioning.
        let file_only = AclPreservationNotice {
            wanted: true,
            file_acl_preserved: true,
            dir_acl_preserved: false,
        };
        assert!(file_only.could_warn(false));
    }

    #[test]
    fn preserving_both_kinds_acls_leaves_nothing_to_warn_about() {
        let notice =
            AclPreservationNotice::for_preserve(&crate::preserve::preserve_all_with_acls());
        assert!(notice.wanted, "`all+acl` did ask for preservation");
        assert!(
            !notice.could_warn(false) && !notice.could_warn(true),
            "both kinds' ACLs are carried across, so even an armed run has nothing to say"
        );
    }

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

    #[tokio::test]
    async fn secure_as_copier_avoids_same_owner_chown_and_preserves_gid() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;

        // a plain reused subdir locked down to 0o700: ownership becomes the copier, the mode is
        // exactly 0o700, and the gid is unchanged (no race means no gid write at all).
        let plain = root.make_dir(OsStr::new("reused_plain"), 0o755).await?;
        let before = plain.meta().await?;
        let restore_uid = plain
            .secure_as_copier(0o700, before.uid(), before.gid())
            .await?;
        assert!(
            restore_uid.is_none(),
            "an already-copier-owned directory needs no ownership change"
        );
        let after = plain.meta().await?;
        assert_eq!(
            after.permissions().mode() & 0o7777,
            0o700,
            "mode restricted"
        );
        assert_eq!(
            after.uid(),
            nix::unistd::geteuid().as_raw(),
            "owned by copier"
        );
        assert_eq!(after.gid(), before.gid(), "gid unchanged when it matches");

        // a setgid reused subdir: BOTH the S_ISGID bit and the gid value must survive the
        // lockdown, since children created during the copy inherit the directory's group.
        let sg = root.make_dir(OsStr::new("reused_setgid"), 0o755).await?;
        std::fs::set_permissions(
            tmp.join("foo/reused_setgid"),
            std::fs::Permissions::from_mode(0o2755),
        )?;
        let sg_before = sg.meta().await?;
        assert_eq!(
            sg_before.permissions().mode() & 0o2000,
            0o2000,
            "test setup: S_ISGID is set before lockdown"
        );
        let restore_uid = sg
            .secure_as_copier(0o2700, sg_before.uid(), sg_before.gid())
            .await?;
        assert!(
            restore_uid.is_none(),
            "an already-copier-owned setgid directory needs no ownership change"
        );
        let sg_after = sg.meta().await?;
        assert_eq!(
            sg_after.permissions().mode() & 0o7777,
            0o2700,
            "S_ISGID + 0o700 both preserved"
        );
        assert_eq!(
            sg_after.gid(),
            sg_before.gid(),
            "gid preserved under setgid"
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
            let create_err = root.create_file(OsStr::new(bad)).await.unwrap_err();
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
            children_may_inherit: Arc::new(std::sync::atomic::AtomicBool::new(true)),
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
        let mut file = root.create_file(OsStr::new("new.txt")).await?;
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
        let err = root.create_file(OsStr::new("0.txt")).await.unwrap_err();
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
        sub.create_file(OsStr::new("child.txt")).await?;
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
        let err = root.create_file(OsStr::new("evil_link")).await.unwrap_err();
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

    // symlink_at creates a symlink fd-relative; a separate child/read verifies its result.
    #[tokio::test]
    async fn symlink_at_creates_link_fd_relative() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let target = std::path::Path::new("some/arbitrary/target");
        root.symlink_at(OsStr::new("mylink"), target).await?;
        let handle = root.child(OsStr::new("mylink")).await?;
        assert_eq!(handle.kind(), EntryKind::Symlink);
        let (read_back, _handle) = handle
            .read_symlink_owned(congestion::Side::Destination)
            .await?;
        assert_eq!(
            read_back, target,
            "read_symlink_owned returned wrong target: {read_back:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn symlink_without_metadata_does_not_reopen_the_created_name() -> anyhow::Result<()> {
        let admission = testutils::AdmissionLimit::new().await;
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let metadata = tokio::fs::symlink_metadata(tmp.join("foo/0.txt")).await?;
        let stat_resource =
            throttle::Resource::meta(throttle::Side::Destination, throttle::MetadataOp::Stat);
        admission.set_max_ops_in_flight(stat_resource, 1);
        let held_stat = throttle::ops_in_flight_permit(stat_resource).await;

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            root.symlink_at(OsStr::new("plain-link"), std::path::Path::new("target"))
                .await?;
            root.set_symlink_metadata_at(
                OsStr::new("plain-link"),
                std::path::Path::new("target"),
                &crate::preserve::preserve_none(),
                &metadata,
            )
            .await
        })
        .await
        .expect("a no-preserve symlink create must not wait for destination Stat")?;
        drop(held_stat);
        assert_eq!(
            tokio::fs::read_link(tmp.join("foo/plain-link")).await?,
            std::path::Path::new("target")
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

    #[tokio::test]
    async fn read_symlink_owned_keeps_the_target_bound_to_the_returned_handle_after_rename()
    -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let link = tmp.join("foo/lnk");
        let moved = tmp.join("foo/moved");
        tokio::fs::symlink("original-target", &link).await?;
        let handle = root.child(OsStr::new("lnk")).await?;
        let original_ino = handle.ino();

        tokio::fs::rename(&link, &moved).await?;
        tokio::fs::symlink("replacement-target", &link).await?;

        let (target, handle) = handle
            .read_symlink_owned(congestion::Side::Destination)
            .await?;
        assert_eq!(target, std::path::Path::new("original-target"));
        assert_eq!(handle.ino(), original_ino);
        assert_eq!(tokio::fs::read_link(&moved).await?, target);
        assert_eq!(
            tokio::fs::read_link(&link).await?,
            std::path::Path::new("replacement-target")
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_symlink_target_checks_the_pinned_handle_not_the_current_name()
    -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let link = tmp.join("foo/lnk");
        tokio::fs::symlink("untrusted-target", &link).await?;
        let handle = root.child(OsStr::new("lnk")).await?;

        tokio::fs::rename(&link, tmp.join("foo/moved")).await?;
        tokio::fs::symlink("intended-target", &link).await?;

        let error = handle
            .verify_symlink_target(
                std::path::Path::new("intended-target"),
                congestion::Side::Destination,
            )
            .await
            .expect_err("the replacement name must not validate the pinned handle");
        assert!(
            error.to_string().contains("symlink target changed"),
            "unexpected validation error: {error:#}"
        );
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

    // hard_link_handle_at links the exact inode the classified Handle pins, immune to a concurrent
    // source-name swap. The deterministic swap occurs after classification but before the link.
    //
    // the decoy is a different regular file with different content placed at the same name. A
    // by-name link would re-resolve `name` and link the decoy; `hard_link_handle_at` links the pinned
    // original inode regardless.
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

    // classify a regular file, then swap the source name to a FIFO before linking. A by-name
    // `linkat(flags=0)` would re-resolve the name and hard-link the FIFO, surfacing a special at the
    // destination that rlink reports as a hard-linked file. The inode-exact link must instead link
    // the pinned regular file or fail closed, so the destination is never special.
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
        let mut dst_file = root.create_file(OsStr::new("dst_meta.txt")).await?;
        dst_file.write_all(b"destination")?;
        dst_file.flush()?;

        // apply source metadata to the already-open dst fd; preserve everything
        let settings = crate::preserve::preserve_all();
        set_file_metadata_fd(
            &settings,
            &src_meta,
            None,
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

    // set_file_metadata_fd: chown before chmod must preserve a setuid bit.
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
        let mut dst_file = root.create_file(OsStr::new("setuid_dst")).await?;
        dst_file.write_all(b"x")?;
        dst_file.flush()?;

        // preserve_all keeps the full mode (mask 0o7777) AND preserves uid/gid, so
        // the chown runs before the chmod; the setuid bit must survive.
        let settings = crate::preserve::preserve_all();
        set_file_metadata_fd(
            &settings,
            &src_meta,
            None,
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

    // set_file_metadata_fd: the widening chmod must be the LAST step. A destination file is created
    // owner-only and this call is the only thing that widens it to the source mode, so a fallible
    // step running AFTER the chmod would publish that final mode — here a setuid one — on a file the
    // copy is about to report as failed. `futimens` is that step. An out-of-range nanosecond field
    // makes it fail deterministically, with no privileged uid or hostile filesystem needed.
    #[tokio::test]
    async fn set_file_metadata_fd_keeps_the_file_owner_only_when_utimens_fails()
    -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        struct RejectedTimestamps;
        impl crate::preserve::Metadata for RejectedTimestamps {
            fn uid(&self) -> u32 {
                // SAFETY: `getuid` has no preconditions and cannot fail.
                unsafe { libc::getuid() }
            }
            fn gid(&self) -> u32 {
                // SAFETY: `getgid` has no preconditions and cannot fail.
                unsafe { libc::getgid() }
            }
            fn atime(&self) -> i64 {
                0
            }
            fn atime_nsec(&self) -> i64 {
                0
            }
            fn mtime(&self) -> i64 {
                0
            }
            // utimensat rejects a nanosecond field outside [0, 999999999] that is neither UTIME_NOW
            // nor UTIME_OMIT with EINVAL.
            fn mtime_nsec(&self) -> i64 {
                2_000_000_000
            }
            fn permissions(&self) -> std::fs::Permissions {
                std::fs::Permissions::from_mode(0o4755)
            }
        }

        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let dst_file = root.create_file(OsStr::new("late_chmod")).await?;
        let dst_path = tmp.join("foo/late_chmod");
        assert_eq!(
            std::fs::metadata(&dst_path)?.permissions().mode() & 0o7777,
            DST_FILE_CREATE_MODE,
            "every destination file starts owner-only"
        );

        // preserve_all: uid/gid so the chown runs first, time so the futimens is reached, and
        // mode_mask 0o7777 so the chmod that must NOT run would have set the setuid bit.
        let error = set_file_metadata_fd(
            &crate::preserve::preserve_all(),
            &RejectedTimestamps,
            None,
            dst_file.as_fd(),
            congestion::Side::Destination,
        )
        .await
        .expect_err("an out-of-range nanosecond field must fail futimens");
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        drop(dst_file);

        assert_eq!(
            std::fs::metadata(&dst_path)?.permissions().mode() & 0o7777,
            DST_FILE_CREATE_MODE,
            "a failure during metadata application must not leave the final mode behind"
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
        set_dir_metadata_fd(&settings, &src_meta, None, &dst_dir).await?;

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
        root.symlink_at(
            OsStr::new("the_link"),
            std::path::Path::new("sentinel_target.txt"),
        )
        .await?;
        let link = root.child(OsStr::new("the_link")).await?;

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

    // rejects_multi_component_names: cover every by-name helper added with fd-relative mutation.
    #[tokio::test]
    async fn new_methods_reject_multi_component_names() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        // a second Dir for hard_link_at's dst parameter
        let dst =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
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

    // ── POSIX ACLs ──────────────────────────────────────────────────────────

    // POSIX.1e ACL entry tags, from `<linux/posix_acl.h>`.
    const ACL_USER_OBJ: u16 = 0x01;
    const ACL_USER: u16 = 0x02;
    const ACL_GROUP_OBJ: u16 = 0x04;
    const ACL_MASK: u16 = 0x10;
    const ACL_OTHER: u16 = 0x20;
    const ACL_UNDEFINED_ID: u32 = 0xffff_ffff;

    // Encode an ACL exactly the way the kernel stores it in `system.posix_acl_*`: a `__le32`
    // version followed by `{__le16 tag, __le16 perm, __le32 id}` entries. Written directly rather
    // than through `setfacl`, which the dev shell does not ship — and which would prove less
    // anyway, since these are the same bytes the code under test round-trips.
    fn encode_acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
        let mut out = 2u32.to_le_bytes().to_vec();
        for &(tag, perm, id) in entries {
            out.extend_from_slice(&tag.to_le_bytes());
            out.extend_from_slice(&perm.to_le_bytes());
            out.extend_from_slice(&id.to_le_bytes());
        }
        out
    }

    // `u::rwx u:65534:--- g::--- m::r-x o::r-x`, i.e. mode 0o705 plus a named entry DENYING 65534
    // what `other` grants everyone else. No mode can express that, which is why dropping the ACL
    // hands 65534 exactly what the source withheld.
    fn restrictive_access_acl() -> Vec<u8> {
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
    fn permissive_acl() -> Vec<u8> {
        encode_acl(&[
            (ACL_USER_OBJ, 7, ACL_UNDEFINED_ID),
            (ACL_USER, 7, 65534),
            (ACL_GROUP_OBJ, 5, ACL_UNDEFINED_ID),
            (ACL_MASK, 7, ACL_UNDEFINED_ID),
            (ACL_OTHER, 5, ACL_UNDEFINED_ID),
        ])
    }

    // An ACL the kernel refuses (EINVAL): a lone named entry, with none of the three required
    // `USER_OBJ`/`GROUP_OBJ`/`OTHER` ones. Gives the appliers a deterministic `fsetxattr` failure
    // with no privileged uid or hostile filesystem needed — the ACL counterpart of the
    // out-of-range nanosecond field the utimens ordering test uses.
    fn rejected_access_acl() -> Vec<u8> {
        encode_acl(&[(ACL_USER, 7, 65534)])
    }

    fn set_xattr_at(path: &std::path::Path, name: &CStr, value: &[u8]) {
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
    fn get_xattr_at(path: &std::path::Path, name: &CStr) -> Option<Vec<u8>> {
        let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `cpath` and `name` are NUL-terminated C strings that outlive the call; a null
        // buffer with size 0 asks for the size without writing.
        let size =
            unsafe { libc::getxattr(cpath.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
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

    fn mode_of(path: &std::path::Path) -> u32 {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
    }

    #[tokio::test]
    async fn read_acls_fd_round_trips_an_access_acl() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let src_path = tmp.join("foo/acl_src");
        tokio::fs::write(&src_path, b"x").await?;
        let blob = restrictive_access_acl();
        set_xattr_at(&src_path, ACL_ACCESS_XATTR, &blob);
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let (file, _) = root.open_file_read(OsStr::new("acl_src")).await?;
        let acls = read_acls_fd(file.as_fd(), congestion::Side::Source, AclCapture::Access).await?;
        // byte-identical: the blob is carried opaquely, never re-encoded
        assert_eq!(acls.access.as_deref(), Some(blob.as_slice()));
        assert_eq!(acls.default, None, "a file has no default ACL to read");
        Ok(())
    }

    // An ACL far larger than the stack buffers, so BOTH `ERANGE` fallbacks run: the name list is
    // padded past 256 bytes with `user.*` attributes, and the blob past 512 by entry count (a blob
    // is `4 + 8n` bytes, so 64 entries fill the buffer exactly and 80 overflow it). Without the
    // retry paths this fails rather than silently truncating — `fgetxattr` refuses a short buffer.
    #[tokio::test]
    async fn read_acls_fd_reads_an_acl_larger_than_the_stack_buffers() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let src_path = tmp.join("foo/big_acl");
        tokio::fs::write(&src_path, b"x").await?;
        // canonical POSIX.1e order — USER_OBJ, named users by ascending uid, GROUP_OBJ, MASK,
        // OTHER. The kernel validates it and rejects anything else with EINVAL.
        let mut entries = vec![(ACL_USER_OBJ, 7, ACL_UNDEFINED_ID)];
        entries.extend((0..80).map(|i| (ACL_USER, 5, 100_000 + i)));
        entries.extend([
            (ACL_GROUP_OBJ, 0, ACL_UNDEFINED_ID),
            (ACL_MASK, 5, ACL_UNDEFINED_ID),
            (ACL_OTHER, 0, ACL_UNDEFINED_ID),
        ]);
        let blob = encode_acl(&entries);
        assert!(blob.len() > 512, "fixture must overflow the read buffer");
        set_xattr_at(&src_path, ACL_ACCESS_XATTR, &blob);
        // push the NAME list past its own 256-byte buffer too
        for i in 0..30 {
            let name = std::ffi::CString::new(format!("user.padding_attribute_number_{i:03}"))?;
            set_xattr_at(&src_path, &name, b"v");
        }
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let (file, _) = root.open_file_read(OsStr::new("big_acl")).await?;
        let acls = read_acls_fd(file.as_fd(), congestion::Side::Source, AclCapture::Access).await?;
        assert_eq!(acls.access.as_deref(), Some(blob.as_slice()));
        Ok(())
    }

    #[tokio::test]
    async fn read_acls_fd_reports_no_acl_when_there_is_none() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let (file, _) = root.open_file_read(OsStr::new("0.txt")).await?;
        let acls = read_acls_fd(file.as_fd(), congestion::Side::Source, AclCapture::Access).await?;
        assert_eq!(acls, Acls::default(), "an entry with no xattrs has no ACL");
        Ok(())
    }

    #[tokio::test]
    async fn read_acls_fd_reads_a_directorys_default_acl_only_when_asked() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let dir_path = tmp.join("foo/acl_dir");
        tokio::fs::create_dir(&dir_path).await?;
        let access = restrictive_access_acl();
        let default = permissive_acl();
        set_xattr_at(&dir_path, ACL_ACCESS_XATTR, &access);
        set_xattr_at(&dir_path, ACL_DEFAULT_XATTR, &default);
        let dir = Dir::open_root_dir(&dir_path, false, congestion::Side::Source).await?;
        let both = dir.read_acls().await?;
        assert_eq!(both.access.as_deref(), Some(access.as_slice()));
        assert_eq!(both.default.as_deref(), Some(default.as_slice()));
        // the file path asks only for access, so it must not fetch the present default ACL.
        let access_only =
            read_acls_fd(dir.fd.as_fd(), congestion::Side::Source, AclCapture::Access).await?;
        assert_eq!(access_only.access.as_deref(), Some(access.as_slice()));
        assert_eq!(access_only.default, None);
        let default_only = read_acls_fd(
            dir.fd.as_fd(),
            congestion::Side::Source,
            AclCapture::Default,
        )
        .await?;
        assert_eq!(default_only.access, None);
        assert_eq!(default_only.default.as_deref(), Some(default.as_slice()));
        Ok(())
    }

    #[tokio::test]
    async fn apply_acls_fd_installs_the_source_acl_and_clears_what_the_source_lacked()
    -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let dst_path = tmp.join("foo/applied");
        tokio::fs::write(&dst_path, b"x").await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let (file, _) = root.open_file_read(OsStr::new("applied")).await?;
        let blob = restrictive_access_acl();
        apply_acls_fd(
            file.as_fd(),
            congestion::Side::Destination,
            &Acls {
                access: Some(blob.clone()),
                default: None,
            },
            false,
        )
        .await?;
        assert_eq!(
            get_xattr_at(&dst_path, ACL_ACCESS_XATTR).as_deref(),
            Some(blob.as_slice())
        );
        // an all-`None` payload is a request to CLEAR, not to do nothing
        apply_acls_fd(
            file.as_fd(),
            congestion::Side::Destination,
            &Acls::default(),
            false,
        )
        .await?;
        assert_eq!(get_xattr_at(&dst_path, ACL_ACCESS_XATTR), None);
        // and clearing again, with nothing there, still succeeds (ENODATA is not a failure)
        apply_acls_fd(
            file.as_fd(),
            congestion::Side::Destination,
            &Acls::default(),
            false,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn apply_acls_fd_fails_a_set_the_filesystem_cannot_hold_but_not_a_remove()
    -> anyhow::Result<()> {
        // `/proc` holds no xattrs at all, so both operations get EOPNOTSUPP — the asymmetry under
        // test is entirely in how each is interpreted.
        let probe = std::fs::File::open("/proc/self/environ")?;
        let error = apply_acls_fd(
            probe.as_fd(),
            congestion::Side::Destination,
            &Acls {
                access: Some(restrictive_access_acl()),
                default: None,
            },
            false,
        )
        .await
        .expect_err("a destination that cannot hold the source's ACL must fail the entry");
        assert_eq!(error.raw_os_error(), Some(libc::EOPNOTSUPP));
        // the same errno on a REMOVE says there is nothing to clear and nothing can have widened
        apply_acls_fd(
            probe.as_fd(),
            congestion::Side::Destination,
            &Acls::default(),
            false,
        )
        .await
        .expect("clearing an ACL a filesystem cannot hold is already satisfied");
        Ok(())
    }

    // ORDERING, branch 1 (the source HAS an access ACL): the narrowed chmod carries the special
    // bits, the `fsetxattr` lands the rwx bits, and both survive together.
    #[tokio::test]
    async fn set_file_metadata_fd_applies_a_setuid_source_mode_and_its_acl() -> anyhow::Result<()> {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let tmp = testutils::setup_test_dir().await?;
        let src_path = tmp.join("foo/acl_setuid_src");
        tokio::fs::write(&src_path, b"x").await?;
        std::fs::set_permissions(&src_path, std::fs::Permissions::from_mode(0o4700))?;
        let blob = restrictive_access_acl();
        set_xattr_at(&src_path, ACL_ACCESS_XATTR, &blob);
        // writing the ACL moved the source's own rwx bits to match it (USER_OBJ / MASK / OTHER);
        // that equality is exactly what the destination has to reproduce.
        assert_eq!(mode_of(&src_path), 0o4755, "fixture source mode");
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_handle = root.child(OsStr::new("acl_setuid_src")).await?;
        let src_meta = src_handle.meta().clone();
        let mut dst_file = root.create_file(OsStr::new("acl_setuid_dst")).await?;
        dst_file.write_all(b"x")?;
        dst_file.flush()?;
        set_file_metadata_fd(
            &crate::preserve::preserve_all_with_acls(),
            &src_meta,
            Some(&Acls {
                access: Some(blob.clone()),
                default: None,
            }),
            dst_file.as_fd(),
            congestion::Side::Destination,
        )
        .await?;
        drop(dst_file);
        let dst_path = tmp.join("foo/acl_setuid_dst");
        assert_eq!(
            mode_of(&dst_path),
            mode_of(&src_path),
            "the special bits come from the chmod and the rwx bits from the ACL; both must land"
        );
        assert_eq!(mode_of(&dst_path), 0o4755);
        assert_eq!(
            get_xattr_at(&dst_path, ACL_ACCESS_XATTR).as_deref(),
            Some(blob.as_slice()),
            "the destination must not be more permissive than its source"
        );
        Ok(())
    }

    // ORDERING, branch 1 under failure: the `fsetxattr` IS the widening step, so when it fails the
    // file must be left unreachable by anyone but the copier. This is what pins the preceding chmod
    // to `(mode & 0o7000) | DST_FILE_CREATE_MODE`: widening there and failing here would publish a
    // setuid-root file whose copy is about to be reported as failed (PR #287's regression).
    #[tokio::test]
    async fn set_file_metadata_fd_keeps_the_file_owner_only_when_the_acl_fails()
    -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let tmp = testutils::setup_test_dir().await?;
        let src_path = tmp.join("foo/acl_fail_src");
        tokio::fs::write(&src_path, b"x").await?;
        std::fs::set_permissions(&src_path, std::fs::Permissions::from_mode(0o4755))?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_meta = root.child(OsStr::new("acl_fail_src")).await?.meta().clone();
        let dst_file = root.create_file(OsStr::new("acl_fail_dst")).await?;
        let dst_path = tmp.join("foo/acl_fail_dst");
        let error = set_file_metadata_fd(
            &crate::preserve::preserve_all_with_acls(),
            &src_meta,
            Some(&Acls {
                access: Some(rejected_access_acl()),
                default: None,
            }),
            dst_file.as_fd(),
            congestion::Side::Destination,
        )
        .await
        .expect_err("an ACL the kernel rejects must fail the entry");
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        drop(dst_file);
        assert_eq!(
            mode_of(&dst_path),
            0o4000 | DST_FILE_CREATE_MODE,
            "the chmod preceding a source ACL must carry the special bits ALONE — a failed ACL \
             application left the file at a mode it had not earned"
        );
        Ok(())
    }

    // ORDERING, branch 2 (the source has NO access ACL): the chmod is the widening step and stays
    // last, and the clear that precedes it is mode-neutral. Inverting the branch condition sends
    // this case down the narrowed-chmod path and leaves the file at 0o4600.
    #[tokio::test]
    async fn set_file_metadata_fd_clears_an_inherited_acl_and_still_applies_the_full_mode()
    -> anyhow::Result<()> {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let tmp = testutils::setup_test_dir().await?;
        let src_path = tmp.join("foo/plain_setuid_src");
        tokio::fs::write(&src_path, b"x").await?;
        std::fs::set_permissions(&src_path, std::fs::Permissions::from_mode(0o4755))?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_meta = root
            .child(OsStr::new("plain_setuid_src"))
            .await?
            .meta()
            .clone();
        let mut dst_file = root.create_file(OsStr::new("plain_setuid_dst")).await?;
        dst_file.write_all(b"x")?;
        dst_file.flush()?;
        let dst_path = tmp.join("foo/plain_setuid_dst");
        // stand in for what a permissive destination directory's default ACL would have left here
        set_xattr_at(&dst_path, ACL_ACCESS_XATTR, &permissive_acl());
        set_file_metadata_fd(
            &crate::preserve::preserve_all_with_acls(),
            &src_meta,
            Some(&Acls::default()),
            dst_file.as_fd(),
            congestion::Side::Destination,
        )
        .await?;
        drop(dst_file);
        assert_eq!(
            get_xattr_at(&dst_path, ACL_ACCESS_XATTR),
            None,
            "a source with no ACL must leave the destination with no ACL"
        );
        assert_eq!(
            mode_of(&dst_path),
            0o4755,
            "without a source ACL the chmod is the widening step and applies the FULL mode"
        );
        Ok(())
    }

    #[tokio::test]
    async fn set_dir_metadata_fd_installs_access_and_default_acls() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_dir_path = tmp.join("foo/acl_src_dir");
        tokio::fs::create_dir(&src_dir_path).await?;
        let access = restrictive_access_acl();
        let default = permissive_acl();
        set_xattr_at(&src_dir_path, ACL_ACCESS_XATTR, &access);
        set_xattr_at(&src_dir_path, ACL_DEFAULT_XATTR, &default);
        let src_meta = root.child(OsStr::new("acl_src_dir")).await?.meta().clone();
        let dst_dir = root.make_dir(OsStr::new("acl_dst_dir"), 0o700).await?;
        let dst_path = tmp.join("foo/acl_dst_dir");
        set_dir_metadata_fd(
            &crate::preserve::preserve_all_with_acls(),
            &src_meta,
            Some(&Acls {
                access: Some(access.clone()),
                default: Some(default.clone()),
            }),
            &dst_dir,
        )
        .await?;
        assert_eq!(
            get_xattr_at(&dst_path, ACL_ACCESS_XATTR).as_deref(),
            Some(access.as_slice())
        );
        assert_eq!(
            get_xattr_at(&dst_path, ACL_DEFAULT_XATTR).as_deref(),
            Some(default.as_slice()),
            "the default ACL decides what CHILDREN inherit; dropping it changes the destination \
             tree's inheritance policy"
        );
        assert_eq!(mode_of(&dst_path), mode_of(&src_dir_path));
        assert_eq!(mode_of(&dst_path), 0o755);
        Ok(())
    }

    // The access ACL is applied LAST inside `apply_acls_fd` for the same reason it is applied last
    // in the file applier: it is the step that WIDENS. A default ACL the kernel rejects must
    // therefore fail while the directory is still at its owner-only create mode.
    #[tokio::test]
    async fn set_dir_metadata_fd_keeps_the_dir_owner_only_when_the_default_acl_fails()
    -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_dir_path = tmp.join("foo/acl_dfail_src");
        tokio::fs::create_dir(&src_dir_path).await?;
        std::fs::set_permissions(&src_dir_path, std::fs::Permissions::from_mode(0o755))?;
        let src_meta = root
            .child(OsStr::new("acl_dfail_src"))
            .await?
            .meta()
            .clone();
        let dst_dir = root.make_dir(OsStr::new("acl_dfail_dst"), 0o700).await?;
        let error = set_dir_metadata_fd(
            &crate::preserve::preserve_all_with_acls(),
            &src_meta,
            Some(&Acls {
                access: Some(permissive_acl()),
                default: Some(rejected_access_acl()),
            }),
            &dst_dir,
        )
        .await
        .expect_err("a default ACL the kernel rejects must fail the entry");
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        assert_eq!(
            mode_of(&tmp.join("foo/acl_dfail_dst")),
            DST_DIR_CREATE_MODE,
            "the access ACL must be applied LAST: a failing default apply left the directory wider"
        );
        Ok(())
    }

    // the reused-directory finalize applies setgid and an access ACL through the same held fd;
    // access entries supply rwx while chmod supplies special bits, so pin their combined result.
    #[tokio::test]
    async fn set_reused_dir_metadata_fd_applies_setgid_with_an_acl() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_dir_path = tmp.join("foo/setgid_acl_src");
        tokio::fs::create_dir(&src_dir_path).await?;
        std::fs::set_permissions(&src_dir_path, std::fs::Permissions::from_mode(0o2700))?;
        let access = restrictive_access_acl();
        set_xattr_at(&src_dir_path, ACL_ACCESS_XATTR, &access);
        let src_meta = root
            .child(OsStr::new("setgid_acl_src"))
            .await?
            .meta()
            .clone();
        assert_eq!(
            mode_of(&src_dir_path),
            0o2755,
            "fixture must keep setgid alongside the ACL (whose MASK set the group bits)"
        );
        let dst_dir = root.make_dir(OsStr::new("setgid_acl_dst"), 0o700).await?;
        // the snapshot is empty and irrelevant here: `d:acl` is on, so the source's ACL wins and
        // the snapshot is discarded.
        set_reused_dir_metadata_fd(
            &crate::preserve::preserve_all_with_acls(),
            &src_meta,
            Some(&Acls {
                access: Some(access.clone()),
                default: None,
            }),
            Some(ReusedDirLock {
                restore_uid: None,
                state: Arc::new(std::sync::Mutex::new(DefaultAclGuard::Armed {
                    original: None,
                })),
                fd: Arc::clone(&dst_dir.fd),
                children_may_inherit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }),
            &dst_dir,
        )
        .await?;
        let dst_path = tmp.join("foo/setgid_acl_dst");
        assert_eq!(
            mode_of(&dst_path),
            0o2755,
            "setgid from the chmod, rwx from the ACL"
        );
        assert_eq!(
            get_xattr_at(&dst_path, ACL_ACCESS_XATTR).as_deref(),
            Some(access.as_slice())
        );
        Ok(())
    }

    #[tokio::test]
    async fn appliers_refuse_a_call_site_that_did_not_carry_acls() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let src_meta = root.child(OsStr::new("0.txt")).await?.meta().clone();
        let dst_file = root.create_file(OsStr::new("no_acls_carried")).await?;
        // `acl` on but no payload: neither "leave it alone" nor "clear it" reproduces the source,
        // so the applier must refuse rather than silently pick one.
        let error = set_file_metadata_fd(
            &crate::preserve::preserve_all_with_acls(),
            &src_meta,
            None,
            dst_file.as_fd(),
            congestion::Side::Destination,
        )
        .await
        .expect_err("requesting ACL preservation on a path that carries no ACLs must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        // with `acl` off the same call site is correct and must stay silent
        set_file_metadata_fd(
            &crate::preserve::preserve_all(),
            &src_meta,
            None,
            dst_file.as_fd(),
            congestion::Side::Destination,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn guarded_default_acl_write_is_skipped_once_disarmed() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        let root =
            Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Destination).await?;
        let dir = root.make_dir(OsStr::new("locked"), 0o700).await?;
        // a guard whose restore has already run (the owning future was cancelled and the guard
        // dropped): a finalize write that already started on the blocking pool must observe that
        // and SKIP — landing it would silently overwrite the restore
        let lock = ReusedDirLock {
            restore_uid: None,
            state: Arc::new(std::sync::Mutex::new(DefaultAclGuard::Disarmed)),
            fd: Arc::clone(&dir.fd),
            children_may_inherit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        lock.write_default_acl_guarded(congestion::Side::Destination, restrictive_access_acl())
            .await?;
        assert_eq!(
            get_xattr_at(&tmp.join("foo/locked"), ACL_DEFAULT_XATTR),
            None,
            "a disarmed guard must refuse the write — landing it would undo the restore"
        );
        Ok(())
    }
}
