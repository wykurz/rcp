//! Low-level data-copy primitive used by the copy path.
//!
//! This replaces `std::fs::copy` so the copy path can keep the destination fd
//! open across the data copy and the subsequent metadata operations (closing
//! the TOCTOU window between writing bytes and setting times/owner/mode).
//!
//! With [`ReflinkMode::Auto`], the copy uses a three-tier fallback chain, fastest first:
//! 1. the in-kernel `copy_file_range` syscall, which is reflink- and
//!    server-side-copy capable (e.g. on Btrfs/XFS/NFSv4.2);
//! 2. when the kernel or filesystem cannot satisfy that, a sparse-aware
//!    userspace copy that walks the source with `SEEK_DATA`/`SEEK_HOLE` and
//!    preserves holes;
//! 3. when the filesystem does not even support `SEEK_DATA`/`SEEK_HOLE` (some
//!    FUSE/older/unusual filesystems return `EINVAL`/`ENOTSUP` for those
//!    `lseek` whences), a plain dense read/write copy loop, which always works
//!    on any regular file (this mirrors what `std::fs::copy` would have done
//!    and exists purely for robustness — it does not preserve holes).
//!
//! [`ReflinkMode::Never`] starts at tier 2, bypassing `copy_file_range` and all
//! of its acceleration. It still preserves holes when the filesystem supports
//! `SEEK_DATA`/`SEEK_HOLE`.
//!
//! Tiers 2 and 3 are also what a copy between two different filesystems usually
//! takes: current kernels refuse `copy_file_range` across filesystems unless the
//! filesystem implements it itself. Both tiers copy through a buffer of up to
//! 1 MiB that each thread keeps between files (see `KeptBuffer`).
//!
//! # Snapshot-size semantics
//!
//! Callers pass `len`, the size captured when the source was classified. Both
//! the primary and fallback paths copy *up to* `len` and agree on the result:
//! - a source that *grew* after classification is intentionally **not**
//!   over-copied — we copy at most `len` bytes and `dst` ends at `len`;
//! - a source that *shrank* below `len` (e.g. a concurrent truncate — holding
//!   the fd does not prevent it) copies and returns only what the source
//!   provides and sizes `dst` to that actual end, **not** to `len` (no
//!   spurious trailing padding);
//! - a *legitimate* sparse trailing hole (the source's logical size still
//!   equals `len`) keeps `dst` sized to `len`. Whether the trailing region is
//!   left *unallocated* depends on the path: the sparse-aware fallback (tier 2)
//!   preserves the hole, and the primary `copy_file_range` (tier 1) does so only
//!   on reflink/sparse-capable filesystems (e.g. Btrfs/XFS) — on others (e.g.
//!   ext4) the size is still `len` but the region may be fully allocated.
//!
//! These size bounds describe successful copies, not a consistent content snapshot
//! of a source that is modified during the copy. Errors do not roll back writes.
//!
//! # Durability
//!
//! These are synchronous syscalls, so once they return the writes are in the
//! kernel. This function deliberately does **not** `fsync`/`flush` — durability
//! is out of scope here. mtime correctness is the caller's responsibility (it
//! sets times after this returns).
use std::fs::File;
use std::os::fd::AsFd;
use std::os::unix::fs::FileExt;

/// Maximum userspace copy buffer size (1 MiB), bounded further by each extent.
const FALLBACK_BUF_SIZE: usize = 1024 * 1024;

thread_local! {
    /// The buffer of the userspace read/write path, kept by each thread between the files it copies.
    static KEPT_BUFFER: std::cell::Cell<Vec<u8>> = const { std::cell::Cell::new(Vec::new()) };
}

/// A thread's read/write buffer, taken for one copy and given back when that copy ends, however
/// it ends.
///
/// A buffer made anew for every file costs more than its size suggests. It is zero-filled each
/// time, and an allocator that hands large blocks straight back to the kernel, as the one in the
/// static musl builds does, turns every file into a memory mapping, its page faults and an
/// unmapping. Measured on one 20-CPU host between two memory-backed filesystems, that was 2 to 3
/// times the copy time for 64 KiB to 256 KiB files.
///
/// So each thread keeps the buffer it copied with. It never holds more than
/// [`FALLBACK_BUF_SIZE`], and only as much as the largest extent it has copied; the memory goes
/// when the thread does, which for the runtime's blocking threads is after a few idle seconds.
/// [`BufferRetentionLimit`] (`--local-copy-buffer-retention-limit`) lowers what a thread may keep,
/// down to nothing, without changing how much is read or written at a time.
struct KeptBuffer(Vec<u8>);

impl KeptBuffer {
    fn take() -> Self {
        // a thread that is being torn down has nothing to lend: start empty
        Self(
            KEPT_BUFFER
                .try_with(std::cell::Cell::take)
                .unwrap_or_default(),
        )
    }

    /// The first `len` bytes, growing the buffer to exactly that if it is smaller. Ordinary
    /// growth could leave a thread holding almost twice the chunk size.
    fn first(&mut self, len: usize) -> &mut [u8] {
        if self.0.len() < len {
            self.0.reserve_exact(len - self.0.len());
            self.0.resize(len, 0);
        }
        &mut self.0[..len]
    }

    /// Free what the current thread keeps if it is more than `limit` allows.
    fn free_if_larger_than(limit: BufferRetentionLimit) {
        let _ = KEPT_BUFFER.try_with(|kept| {
            let buffer = kept.take();
            if buffer.capacity() <= limit.0 {
                kept.set(buffer);
            }
        });
    }
}

impl Drop for KeptBuffer {
    fn drop(&mut self) {
        let buffer = std::mem::take(&mut self.0);
        // ignore a thread that is being torn down: the buffer is simply freed
        let _ = KEPT_BUFFER.try_with(|kept| kept.set(buffer));
    }
}

/// What the current thread keeps for its next read/write copy: where the allocation is and how
/// large it is.
#[cfg(test)]
pub(crate) fn kept_buffer() -> (*const u8, usize) {
    let buffer = KEPT_BUFFER.take();
    let identity = (buffer.as_ptr(), buffer.capacity());
    KEPT_BUFFER.set(buffer);
    identity
}

/// The largest read/write buffer a thread may keep between the files it copies.
///
/// The default is the whole chunk of the read/write path (1 MiB). A buffer larger than the limit
/// is freed when its copy ends, so zero keeps nothing and every file then gets a fresh buffer.
/// This bounds idle memory only; it does not change how much is read or written at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferRetentionLimit(pub usize);

impl Default for BufferRetentionLimit {
    fn default() -> Self {
        Self(FALLBACK_BUF_SIZE)
    }
}

impl std::str::FromStr for BufferRetentionLimit {
    type Err = String;

    /// A size as written on a command line: "256KiB", "1MiB", or a number of bytes. One that does
    /// not fit this platform's addresses is as good as no limit.
    fn from_str(size: &str) -> Result<Self, Self::Err> {
        let size: bytesize::ByteSize = size.parse()?;
        Ok(Self(usize::try_from(size.as_u64()).unwrap_or(usize::MAX)))
    }
}

impl std::fmt::Display for BufferRetentionLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", bytesize::ByteSize::b(self.0 as u64))
    }
}

/// Whether local file copies may use reflink-capable acceleration.
///
/// A clone-required `always` mode is intentionally deferred until there is a
/// concrete need for its additional guarantees and failure semantics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum ReflinkMode {
    /// Allow filesystem acceleration, falling back to read/write copying.
    #[default]
    Auto,
    /// Use sparse-aware read/write copying without reflink-capable acceleration.
    Never,
}

/// Copy up to `len` bytes between held file descriptors with the chosen policy.
///
/// Copies from byte zero in the source to byte zero in the destination, regardless
/// of their current seek positions, and returns the logical number of bytes copied.
/// File cursor positions after copying are unspecified. Errors may leave a partially
/// written destination; writes are not rolled back.
/// The destination must be empty and exclusively owned by this copy operation.
/// `Never` bypasses all `copy_file_range` acceleration, including non-reflink
/// kernel and server-side copying. See the module docs for size and durability semantics.
///
/// The userspace path copies through a buffer that the calling thread keeps for its next file
/// unless it is larger than `retention` allows.
pub fn copy_file_data(
    src: &File,
    dst: &File,
    len: u64,
    reflink: ReflinkMode,
    retention: BufferRetentionLimit,
) -> std::io::Result<u64> {
    let result = match reflink {
        ReflinkMode::Auto => copy_file_range_all(src, dst, len),
        ReflinkMode::Never => copy_sparse_fallback(src, dst, 0, len),
    };
    // the one place the limit is applied: every copy ends here, and has given its buffer back by
    // now, whether it succeeded or not
    KeptBuffer::free_if_larger_than(retention);
    result
}

/// Copy up to `len` bytes from `src` to `dst` using the in-kernel
/// `copy_file_range` (reflink/server-side capable), falling back to a
/// sparse-aware userspace copy when the kernel/filesystem can't. Both files are
/// already open; copies start at byte `0` of each via explicit offsets, ignoring
/// the descriptors' cursors. Returns the number of bytes copied.
/// The destination must be empty and exclusively owned by this copy operation.
///
/// See the module docs for snapshot-size and durability semantics.
fn copy_file_range_all(src: &File, dst: &File, len: u64) -> std::io::Result<u64> {
    copy_file_range_all_with(src, dst, len, |src_offset, dst_offset, remaining| {
        #[cfg(test)]
        crate::testutils::record_copy_file_range_call();
        nix::fcntl::copy_file_range(
            src.as_fd(),
            Some(src_offset),
            dst.as_fd(),
            Some(dst_offset),
            remaining,
        )
    })
}

/// Run positioned kernel-copy attempts from the confirmed prefix. The syscall
/// must report no confirmed progress on error. Counts above the requested length
/// are rejected with `EIO`; rejecting a count does not undo any kernel writes.
/// Mutated offset arguments are discarded; only an accepted byte count advances
/// the prefix used to initialize both offsets on the next attempt.
fn copy_file_range_all_with(
    src: &File,
    dst: &File,
    len: u64,
    mut copy_range: impl FnMut(&mut i64, &mut i64, usize) -> nix::Result<usize>,
) -> std::io::Result<u64> {
    let mut copied: u64 = 0;
    while copied < len {
        let remaining = usize::try_from(len - copied).unwrap_or(usize::MAX);
        // explicit offsets make copying independent of the descriptors' cursors.
        // derive both from confirmed progress, including after an interrupted call.
        let mut src_offset = i64::try_from(copied).map_err(|_| nix::errno::Errno::EOVERFLOW)?;
        let mut dst_offset = src_offset;
        match copy_range(&mut src_offset, &mut dst_offset, remaining) {
            Ok(0)
            | Err(
                nix::errno::Errno::ENOSYS
                | nix::errno::Errno::EXDEV
                | nix::errno::Errno::EINVAL
                | nix::errno::Errno::EOPNOTSUPP,
            ) => {
                // eof or unsupported kernel copying: finish through the userspace path.
                // it returns the reconciled total size, which can be below `copied`
                // if the source shrank beneath the prefix already written.
                return copy_sparse_fallback(src, dst, copied, len);
            }
            Err(nix::errno::Errno::EPERM) if copied == 0 => {
                // syscall permission may be denied while positioned I/O is allowed.
                return copy_sparse_fallback(src, dst, 0, len);
            }
            Ok(n) if n > remaining => return Err(nix::errno::Errno::EIO.into()),
            Ok(n) => copied += n as u64,
            Err(nix::errno::Errno::EINTR) => continue,
            Err(errno) => return Err(std::io::Error::from(errno)),
        }
    }
    Ok(copied)
}

/// Classification of a `SEEK_DATA` probe, used to decide whether the
/// sparse fallback can run or must degrade to a dense read/write copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SparseProbe {
    /// `SEEK_DATA` found a data region starting at this offset.
    Data(u64),
    /// `SEEK_DATA` returned `ENXIO`: no data at or after the probe offset, i.e.
    /// the rest of the file up to its logical end is a hole.
    TrailingHole,
    /// The filesystem does not support `SEEK_DATA`/`SEEK_HOLE` (`EINVAL` or
    /// `ENOTSUP`/`EOPNOTSUPP`): the caller must fall back to a dense copy.
    Unsupported,
}

/// Classify the result of an `lseek(.., SEEK_DATA)` probe (the testable seam for
/// the sparse-vs-dense routing decision).
///
/// - `Ok(off)` -> [`SparseProbe::Data`] at `off`;
/// - `ENXIO` -> [`SparseProbe::TrailingHole`] (a legitimate trailing hole, not
///   an error: the file simply has no more data);
/// - `EINVAL` / `EOPNOTSUPP` (== `ENOTSUP` on Linux) -> [`SparseProbe::Unsupported`],
///   meaning the filesystem rejects the `SEEK_DATA`/`SEEK_HOLE` whences and the
///   sparse walk can't run on it;
/// - any other errno (e.g. `EIO`, `EBADF`) is a genuine failure and is
///   propagated — we deliberately do **not** mask real I/O errors as
///   "unsupported".
fn classify_seek_data(result: nix::Result<libc::off_t>) -> std::io::Result<SparseProbe> {
    match result {
        Ok(off) => Ok(SparseProbe::Data(off as u64)),
        Err(nix::errno::Errno::ENXIO) => Ok(SparseProbe::TrailingHole),
        // note: ENOTSUP == EOPNOTSUPP numerically on Linux, so this arm also
        // covers ENOTSUP.
        Err(nix::errno::Errno::EINVAL | nix::errno::Errno::EOPNOTSUPP) => {
            Ok(SparseProbe::Unsupported)
        }
        Err(errno) => Err(std::io::Error::from(errno)),
    }
}

/// Sparse-aware userspace copy of the range `[start, len)` from `src` to `dst`,
/// with a dense read/write copy as a final fallback.
///
/// Used for `Never` and as the fallback when `copy_file_range` is unsupported. It first probes
/// the source with `SEEK_DATA`; if the filesystem supports it, it walks the
/// source's data regions with `SEEK_DATA`/`SEEK_HOLE` and copies only the data
/// extents, so holes (e.g. in a sparse VM or Lustre image) are preserved
/// instead of being expanded to fully-allocated zeros. If the filesystem does
/// not support those whences (some FUSE/older/unusual filesystems return
/// `EINVAL`/`ENOTSUP`), it degrades to a plain read/write loop
/// that always works on a regular file (this matches what `std::fs::copy` would
/// have done — see the module docs). A genuine I/O error during the probe (e.g.
/// `EIO`) is propagated rather than masked as "unsupported".
///
/// The final size is reconciled with the source's *actual* end so this path
/// agrees with the primary one on a shrunk source: after the data loop it
/// bounds the final size by `len`, `SEEK_END`, and any earlier EOF observed by a
/// read, then sizes `dst` to that end if needed. An observed read EOF also bounds
/// virtual files whose advertised size exceeds their readable data.
/// A legitimate trailing hole (source logical size still == `len`) leaves
/// `actual_eof == len`, so `dst` ends at `len` with the trailing region
/// unallocated; a source that shrank below `len` (`actual_eof < len`) sizes
/// `dst` to `actual_eof` rather than padding a spurious hole up to `len`.
///
/// Returns the reconciled total logical size, including the already-copied prefix.
/// The result can be below `start` if the source shrank beneath that prefix.
/// The destination must contain only the already-copied prefix `[0, start)`
/// and remain exclusively owned by this copy operation. `start` must not exceed `len`.
fn copy_sparse_fallback(src: &File, dst: &File, start: u64, len: u64) -> std::io::Result<u64> {
    copy_sparse_fallback_with_seek(src, dst, start, len, |offset, whence| {
        nix::unistd::lseek(src.as_fd(), offset, whence)
    })
}

fn copy_sparse_fallback_with_seek(
    src: &File,
    dst: &File,
    start: u64,
    len: u64,
    mut seek: impl FnMut(libc::off_t, nix::unistd::Whence) -> nix::Result<libc::off_t>,
) -> std::io::Result<u64> {
    debug_assert!(start <= len);
    if start == len {
        // the destination already contains the prefix; an empty source needs no work.
        return Ok(len);
    }
    let mut off = start;
    let mut written_end = start;
    let mut read_eof = None;
    let mut buf = KeptBuffer::take();
    while off < len {
        let extent = match next_copy_extent(&mut seek, off, len)? {
            CopyExtent::Sparse(extent) => extent,
            CopyExtent::Dense => off..len,
            CopyExtent::End => break,
        };
        // allocate only for data, cap small extents, and reuse the buffer across the walk and,
        // through this thread, across files.
        let capacity = (extent.end - extent.start).min(FALLBACK_BUF_SIZE as u64) as usize;
        let end = copy_data_extent(src, dst, extent.start, extent.end, buf.first(capacity))?;
        if end > extent.start {
            written_end = end;
        }
        if end < extent.end {
            // virtual files can advertise extents and a SEEK_END beyond readable data.
            read_eof = Some(end);
            break;
        }
        off = extent.end;
    }
    // keep the post-loop EOF observation: a source can shrink even after the last write.
    let actual_eof = seek(0, nix::unistd::Whence::SeekEnd).map_err(std::io::Error::from)? as u64;
    let final_size = len.min(actual_eof).min(read_eof.unwrap_or(len));
    if written_end != final_size {
        // extend trailing holes or remove bytes beyond a concurrently shortened source.
        nix::unistd::ftruncate(dst.as_fd(), to_off_t(final_size)?).map_err(std::io::Error::from)?;
    }
    // the same finalizer accounts for the entire walk, including any dense remainder.
    Ok(final_size)
}

enum CopyExtent {
    Sparse(std::ops::Range<u64>),
    Dense,
    End,
}

/// Find the next extent, degrading to a dense remainder if sparse probes cannot advance.
fn next_copy_extent(
    seek: &mut impl FnMut(libc::off_t, nix::unistd::Whence) -> nix::Result<libc::off_t>,
    off: u64,
    len: u64,
) -> std::io::Result<CopyExtent> {
    let data = match classify_seek_data(seek(to_off_t(off)?, nix::unistd::Whence::SeekData))? {
        SparseProbe::Data(data) => data,
        SparseProbe::TrailingHole => return Ok(CopyExtent::End),
        SparseProbe::Unsupported => return Ok(CopyExtent::Dense),
    };
    if data < off {
        return Ok(CopyExtent::Dense);
    }
    if data >= len {
        return Ok(CopyExtent::End);
    }
    let hole = match seek(to_off_t(data)?, nix::unistd::Whence::SeekHole) {
        Ok(hole) => (hole as u64).min(len),
        Err(nix::errno::Errno::ENXIO) => len,
        Err(nix::errno::Errno::EINVAL | nix::errno::Errno::EOPNOTSUPP) => {
            return Ok(CopyExtent::Dense);
        }
        Err(errno) => return Err(std::io::Error::from(errno)),
    };
    if hole <= data {
        // concurrent hole punching can invalidate the preceding SEEK_DATA result.
        // copying the remainder densely also bounds progress on anomalous filesystems.
        return Ok(CopyExtent::Dense);
    }
    Ok(CopyExtent::Sparse(data..hole))
}

/// Copy an extent with positioned I/O, retrying interruptions and handling short transfers.
/// Returns the offset reached, which can be below `to` if a read reaches EOF.
fn copy_data_extent(
    src: &impl FileExt,
    dst: &impl FileExt,
    from: u64,
    to: u64,
    buf: &mut [u8],
) -> std::io::Result<u64> {
    let mut pos = from;
    while pos < to {
        let want = (to - pos).min(buf.len() as u64) as usize;
        let n = match src.read_at(&mut buf[..want], pos) {
            Ok(0) => break,
            Ok(n) => n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        };
        let mut written = 0;
        while written < n {
            match dst.write_at(&buf[written..n], pos + written as u64) {
                Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
                Ok(w) => written += w,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
        pos += n as u64;
    }
    Ok(pos)
}

/// Convert a `u64` byte offset/length to the libc `off_t` expected by nix's
/// lseek/ftruncate, mapping overflow to an io error rather than panicking.
fn to_off_t(value: u64) -> std::io::Result<libc::off_t> {
    libc::off_t::try_from(value).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "file offset exceeds off_t range",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::os::unix::fs::MetadataExt;

    fn dense_copy(src: &File, dst: &File, start: u64, len: u64) -> std::io::Result<u64> {
        copy_sparse_fallback_with_seek(src, dst, start, len, |offset, whence| match whence {
            nix::unistd::Whence::SeekData | nix::unistd::Whence::SeekHole => {
                Err(nix::errno::Errno::EOPNOTSUPP)
            }
            _ => nix::unistd::lseek(src.as_fd(), offset, whence),
        })
    }

    fn make_file(dir: &std::path::Path, name: &str) -> File {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.join(name))
            .expect("open temp file")
    }

    fn copies_with_seek_results(
        start: u64,
        probes: &[(nix::unistd::Whence, libc::off_t, nix::Result<libc::off_t>)],
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let contents = b"0123456789abcdef";
        let mut src = make_file(tmp.path(), "src");
        src.write_all(contents).unwrap();
        let mut dst = make_file(tmp.path(), "dst");
        dst.write_all(&contents[..start as usize]).unwrap();
        let mut probes = probes.iter();
        let copied = copy_sparse_fallback_with_seek(
            &src,
            &dst,
            start,
            contents.len() as u64,
            |offset, whence| {
                if matches!(whence, nix::unistd::Whence::SeekEnd) {
                    return nix::unistd::lseek(src.as_fd(), offset, whence);
                }
                // exhausting the script fails promptly if the walker loops instead of degrading.
                let &(expected_whence, expected_offset, result) = probes.next().unwrap();
                assert_eq!(
                    (whence as i32, offset),
                    (expected_whence as i32, expected_offset)
                );
                result
            },
        )
        .unwrap();
        assert!(probes.next().is_none());
        assert_eq!(copied, contents.len() as u64);
        assert_eq!(std::fs::read(tmp.path().join("dst")).unwrap(), contents);
    }

    fn copies_with_kernel_results(
        src: &File,
        dst: &File,
        len: u64,
        steps: &[(i64, usize, nix::Result<usize>)],
        mut before_step: impl FnMut(usize),
    ) -> std::io::Result<u64> {
        let mut steps = steps.iter().enumerate();
        let copied =
            copy_file_range_all_with(src, dst, len, |src_offset, dst_offset, remaining| {
                let (step, &(offset, expected_remaining, result)) = steps.next().unwrap();
                assert_eq!(
                    (*src_offset, *dst_offset, remaining),
                    (offset, offset, expected_remaining)
                );
                before_step(step);
                match result {
                    Ok(n) if n <= remaining => {
                        let mut bytes = vec![0; n];
                        src.read_exact_at(&mut bytes, *src_offset as u64).unwrap();
                        dst.write_all_at(&bytes, *dst_offset as u64).unwrap();
                        *src_offset += n as i64;
                        *dst_offset += n as i64;
                    }
                    _ => {}
                }
                result
            });
        assert!(steps.next().is_none());
        copied
    }

    /// A source file of `len` bytes that are not all the same.
    fn source_of(dir: &std::path::Path, name: &str, len: usize) -> File {
        let mut file = make_file(dir, name);
        let contents: Vec<u8> = (0..len).map(|index| (index % 251) as u8).collect();
        file.write_all(&contents).unwrap();
        file
    }

    /// Copy `len` bytes of `src` through the read/write path into a new file and check them.
    fn read_write_copy(dir: &std::path::Path, src: &File, name: &str, len: usize) {
        read_write_copy_keeping(dir, src, name, len, BufferRetentionLimit::default());
    }

    /// The same, with the largest buffer the thread may keep afterwards.
    fn read_write_copy_keeping(
        dir: &std::path::Path,
        src: &File,
        name: &str,
        len: usize,
        limit: BufferRetentionLimit,
    ) {
        let dst = make_file(dir, name);
        assert_eq!(
            copy_file_data(src, &dst, len as u64, ReflinkMode::Never, limit).unwrap(),
            len as u64
        );
        let mut expected = vec![0; len];
        src.read_exact_at(&mut expected, 0).unwrap();
        assert!(
            std::fs::read(dir.join(name)).unwrap() == expected,
            "{name} differs"
        );
    }

    // what a thread keeps is the subject of these tests, so each runs on a thread of its own
    fn on_a_fresh_thread(body: impl FnOnce() + Send + 'static) {
        std::thread::spawn(body).join().unwrap();
    }

    // a fresh buffer per file is zero-filled, and with an allocator that hands large blocks
    // straight back to the kernel (the static musl builds' one) it costs a memory mapping, its
    // page faults and an unmapping per file: 2 to 3 times the copy time for 64 KiB to 256 KiB files
    #[test]
    fn a_thread_keeps_its_read_write_buffer_between_files() {
        on_a_fresh_thread(|| {
            let tmp = tempfile::tempdir().unwrap();
            let src = source_of(tmp.path(), "src", 64 * 1024);
            assert_eq!(kept_buffer().1, 0, "nothing is kept before the first copy");
            read_write_copy(tmp.path(), &src, "first", 64 * 1024);
            let after_first = kept_buffer();
            assert_eq!(after_first.1, 64 * 1024);
            read_write_copy(tmp.path(), &src, "second", 64 * 1024);
            assert_eq!(
                kept_buffer(),
                after_first,
                "the second file must be copied through the first one's buffer"
            );
        });
    }

    #[test]
    fn the_kept_buffer_grows_to_the_chunk_size_and_no_further() {
        on_a_fresh_thread(|| {
            let tmp = tempfile::tempdir().unwrap();
            let src = source_of(tmp.path(), "src", 3 * FALLBACK_BUF_SIZE);
            // 600 KiB first: growing by doubling from there would overshoot the chunk size
            read_write_copy(tmp.path(), &src, "smaller", 600 * 1024);
            assert_eq!(kept_buffer().1, 600 * 1024);
            read_write_copy(tmp.path(), &src, "larger", 3 * FALLBACK_BUF_SIZE);
            let full = kept_buffer();
            assert_eq!(full.1, FALLBACK_BUF_SIZE);
            read_write_copy(tmp.path(), &src, "again", 3 * FALLBACK_BUF_SIZE);
            read_write_copy(tmp.path(), &src, "small", 1024);
            assert_eq!(kept_buffer(), full, "a full-size buffer is kept as it is");
        });
    }

    #[test]
    fn a_small_file_does_not_make_a_thread_keep_a_whole_chunk() {
        on_a_fresh_thread(|| {
            let tmp = tempfile::tempdir().unwrap();
            let src = source_of(tmp.path(), "src", 1024);
            read_write_copy(tmp.path(), &src, "copy", 1024);
            assert_eq!(kept_buffer().1, 1024);
        });
    }

    #[test]
    fn a_thread_keeps_no_buffer_larger_than_the_limit() {
        on_a_fresh_thread(|| {
            let tmp = tempfile::tempdir().unwrap();
            let src = source_of(tmp.path(), "src", 256 * 1024);
            let limit = BufferRetentionLimit(64 * 1024);
            read_write_copy_keeping(tmp.path(), &src, "within", 16 * 1024, limit);
            assert_eq!(
                kept_buffer().1,
                16 * 1024,
                "a buffer within the limit is kept"
            );
            read_write_copy_keeping(tmp.path(), &src, "at", 64 * 1024, limit);
            assert_eq!(
                kept_buffer().1,
                64 * 1024,
                "a buffer of exactly the limit is kept"
            );
            read_write_copy_keeping(tmp.path(), &src, "beyond", 256 * 1024, limit);
            assert_eq!(
                kept_buffer().1,
                0,
                "a larger buffer is freed when its copy ends"
            );
            read_write_copy_keeping(tmp.path(), &src, "after", 16 * 1024, limit);
            assert_eq!(kept_buffer().1, 16 * 1024);
        });
    }

    #[test]
    fn a_limit_of_zero_keeps_nothing() {
        on_a_fresh_thread(|| {
            let tmp = tempfile::tempdir().unwrap();
            let src = source_of(tmp.path(), "src", 64 * 1024);
            for name in ["first", "second"] {
                read_write_copy_keeping(tmp.path(), &src, name, 64 * 1024, BufferRetentionLimit(0));
                assert_eq!(kept_buffer().1, 0);
            }
            // an empty file needs no buffer at all, whatever the limit
            read_write_copy_keeping(tmp.path(), &src, "empty", 0, BufferRetentionLimit(0));
            assert_eq!(kept_buffer().1, 0);
        });
    }

    #[test]
    fn a_size_from_the_command_line_becomes_the_limit() {
        for (size, limit) in [
            ("0", 0),
            ("4096", 4096),
            ("64KiB", 64 * 1024),
            ("1MiB", FALLBACK_BUF_SIZE),
            ("1.0 MiB", FALLBACK_BUF_SIZE),
            ("16MiB", 16 * FALLBACK_BUF_SIZE),
        ] {
            assert_eq!(size.parse(), Ok(BufferRetentionLimit(limit)), "{size}");
        }
        for not_a_size in ["", "plenty", "-1", "1 mile"] {
            assert!(
                not_a_size.parse::<BufferRetentionLimit>().is_err(),
                "{not_a_size:?}"
            );
        }
    }

    // a command line parser shows the default and reads it back
    #[test]
    fn the_default_limit_reads_back_from_what_it_is_shown_as() {
        let default = BufferRetentionLimit::default();
        assert_eq!(default.to_string().parse(), Ok(default));
    }

    #[test]
    fn the_default_limit_is_the_whole_chunk() {
        assert_eq!(
            BufferRetentionLimit::default(),
            BufferRetentionLimit(FALLBACK_BUF_SIZE)
        );
    }

    // the kernel's own copy needs no buffer, but the limit applies to whatever the thread holds
    #[test]
    fn the_limit_applies_after_a_kernel_copy_as_well() {
        on_a_fresh_thread(|| {
            let tmp = tempfile::tempdir().unwrap();
            let src = source_of(tmp.path(), "src", 64 * 1024);
            read_write_copy(tmp.path(), &src, "kept", 64 * 1024);
            assert_eq!(kept_buffer().1, 64 * 1024);
            let dst = make_file(tmp.path(), "kernel");
            copy_file_data(
                &src,
                &dst,
                64 * 1024,
                ReflinkMode::Auto,
                BufferRetentionLimit(0),
            )
            .unwrap();
            assert_eq!(kept_buffer().1, 0);
        });
    }

    #[test]
    fn a_failed_copy_gives_the_buffer_back_to_its_thread() {
        on_a_fresh_thread(|| {
            let tmp = tempfile::tempdir().unwrap();
            let src = source_of(tmp.path(), "src", 64 * 1024);
            read_write_copy(tmp.path(), &src, "first", 64 * 1024);
            let kept = kept_buffer();
            assert_eq!(kept.1, 64 * 1024);
            // the first extent is copied, then looking for the next one fails
            let dst = make_file(tmp.path(), "failed");
            let error =
                copy_sparse_fallback_with_seek(&src, &dst, 0, 64 * 1024, |offset, whence| {
                    match (whence, offset) {
                        (nix::unistd::Whence::SeekData, 0) => Ok(0),
                        (nix::unistd::Whence::SeekHole, _) => Ok(4096),
                        _ => Err(nix::errno::Errno::EIO),
                    }
                })
                .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EIO));
            assert_eq!(
                std::fs::metadata(tmp.path().join("failed")).unwrap().len(),
                4096
            );
            assert_eq!(kept_buffer(), kept, "the buffer must survive a failed copy");
            read_write_copy(tmp.path(), &src, "next", 64 * 1024);
            assert_eq!(kept_buffer(), kept);
        });
    }

    // a copy that fails has grown the buffer like any other
    #[test]
    fn the_limit_applies_after_a_failed_copy_as_well() {
        on_a_fresh_thread(|| {
            let tmp = tempfile::tempdir().unwrap();
            let src = source_of(tmp.path(), "src", 64 * 1024);
            // a destination that is not open for writing
            drop(make_file(tmp.path(), "read-only"));
            let dst = File::open(tmp.path().join("read-only")).unwrap();
            for (limit, kept) in [
                (BufferRetentionLimit::default(), 64 * 1024),
                (BufferRetentionLimit(0), 0),
            ] {
                let error =
                    copy_file_data(&src, &dst, 64 * 1024, ReflinkMode::Never, limit).unwrap_err();
                assert_eq!(error.raw_os_error(), Some(libc::EBADF));
                assert_eq!(kept_buffer().1, kept, "{limit:?}");
            }
        });
    }

    #[test]
    fn falls_back_when_kernel_copy_is_denied_before_progress() {
        for interrupted in [false, true] {
            for len in [7, 16, 20] {
                let tmp = tempfile::tempdir().unwrap();
                let contents = b"0123456789abcdef";
                let mut src = make_file(tmp.path(), "src");
                src.write_all(contents).unwrap();
                let dst = make_file(tmp.path(), "dst");
                nix::unistd::lseek(src.as_fd(), 3, nix::unistd::Whence::SeekSet).unwrap();
                nix::unistd::lseek(dst.as_fd(), 11, nix::unistd::Whence::SeekSet).unwrap();
                let mut steps = Vec::new();
                if interrupted {
                    steps.push((0, len as usize, Err(nix::errno::Errno::EINTR)));
                }
                steps.push((0, len as usize, Err(nix::errno::Errno::EPERM)));
                let copied = copies_with_kernel_results(&src, &dst, len, &steps, |_| {}).unwrap();
                let expected_len = len.min(contents.len() as u64);
                assert_eq!(copied, expected_len);
                assert_eq!(
                    std::fs::read(tmp.path().join("dst")).unwrap(),
                    &contents[..expected_len as usize]
                );
            }
        }
    }

    #[test]
    fn propagates_permission_errors_after_partial_kernel_copy() {
        for interrupted in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let contents = b"0123456789abcdef";
            let mut src = make_file(tmp.path(), "src");
            src.write_all(contents).unwrap();
            let dst = make_file(tmp.path(), "dst");
            let mut steps = vec![(0, 16, Ok(8))];
            if interrupted {
                steps.push((8, 8, Err(nix::errno::Errno::EINTR)));
            }
            steps.push((8, 8, Err(nix::errno::Errno::EPERM)));
            let error = copies_with_kernel_results(&src, &dst, 16, &steps, |_| {}).unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EPERM));
            assert_eq!(
                std::fs::read(tmp.path().join("dst")).unwrap(),
                &contents[..8]
            );
        }
    }

    #[test]
    fn propagates_fallback_errors_after_initial_kernel_permission_denial() {
        let tmp = tempfile::tempdir().unwrap();
        let mut src = make_file(tmp.path(), "src");
        src.write_all(b"0123456789abcdef").unwrap();
        drop(make_file(tmp.path(), "dst"));
        let dst = File::open(tmp.path().join("dst")).unwrap();
        let error = copies_with_kernel_results(
            &src,
            &dst,
            16,
            &[(0, 16, Err(nix::errno::Errno::EPERM))],
            |_| {},
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));
        assert_eq!(dst.metadata().unwrap().len(), 0);
    }

    #[test]
    fn rejects_kernel_copy_counts_larger_than_remaining() {
        for prefix in [0, 8] {
            let remaining = 16 - prefix;
            for reported in [remaining + 1, usize::MAX] {
                let tmp = tempfile::tempdir().unwrap();
                let contents = b"0123456789abcdef";
                let mut src = make_file(tmp.path(), "src");
                src.write_all(contents).unwrap();
                let dst = make_file(tmp.path(), "dst");
                let mut steps = Vec::new();
                if prefix != 0 {
                    steps.push((0, 16, Ok(prefix)));
                }
                steps.push((prefix as i64, remaining, Ok(reported)));
                let error = copies_with_kernel_results(&src, &dst, 16, &steps, |_| {}).unwrap_err();
                assert_eq!(error.raw_os_error(), Some(libc::EIO));
                assert_eq!(
                    std::fs::read(tmp.path().join("dst")).unwrap(),
                    &contents[..prefix]
                );
            }
        }
    }

    #[test]
    fn falls_back_from_the_confirmed_prefix_for_each_outcome() {
        for outcome in [
            Ok(0),
            Err(nix::errno::Errno::ENOSYS),
            Err(nix::errno::Errno::EXDEV),
            Err(nix::errno::Errno::EINVAL),
            Err(nix::errno::Errno::EOPNOTSUPP),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let contents = b"0123456789abcdef";
            let mut src = make_file(tmp.path(), "src");
            src.write_all(contents).unwrap();
            let dst = make_file(tmp.path(), "dst");
            let copied = copies_with_kernel_results(
                &src,
                &dst,
                16,
                &[(0, 16, Ok(8)), (8, 8, outcome)],
                |_| {},
            )
            .unwrap();
            assert_eq!(copied, contents.len() as u64);
            assert_eq!(std::fs::read(tmp.path().join("dst")).unwrap(), contents);
        }
    }

    #[test]
    fn reports_final_size_when_source_shrinks_after_partial_kernel_copy() {
        for final_len in [0, 3, 8, 12] {
            let tmp = tempfile::tempdir().unwrap();
            let contents = b"0123456789abcdef";
            let mut src = make_file(tmp.path(), "src");
            src.write_all(contents).unwrap();
            let dst = make_file(tmp.path(), "dst");
            let copied = copies_with_kernel_results(
                &src,
                &dst,
                16,
                &[(0, 16, Ok(8)), (8, 8, Err(nix::errno::Errno::EXDEV))],
                |step| {
                    if step == 1 {
                        src.set_len(final_len).unwrap();
                    }
                },
            )
            .unwrap();
            assert_eq!(copied, final_len);
            assert_eq!(dst.metadata().unwrap().len(), final_len);
            assert_eq!(
                std::fs::read(tmp.path().join("dst")).unwrap(),
                &contents[..final_len as usize]
            );
        }
    }

    #[test]
    fn retries_interrupted_kernel_copies_without_restarting_the_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let contents = b"0123456789abcdef";
        let mut src = make_file(tmp.path(), "src");
        src.write_all(contents).unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = copies_with_kernel_results(
            &src,
            &dst,
            16,
            &[
                (0, 16, Ok(8)),
                (8, 8, Err(nix::errno::Errno::EINTR)),
                (8, 8, Ok(8)),
            ],
            |_| {},
        )
        .unwrap();
        assert_eq!(copied, contents.len() as u64);
        assert_eq!(std::fs::read(tmp.path().join("dst")).unwrap(), contents);
    }

    #[test]
    fn counts_the_sparse_prefix_when_later_probes_are_unsupported() {
        use nix::unistd::Whence::{SeekData, SeekHole};
        for errno in [nix::errno::Errno::EINVAL, nix::errno::Errno::EOPNOTSUPP] {
            copies_with_seek_results(
                4,
                &[
                    (SeekData, 4, Ok(4)),
                    (SeekHole, 4, Ok(8)),
                    (SeekData, 8, Err(errno)),
                ],
            );
        }
    }

    #[test]
    fn copies_densely_when_hole_probes_are_unsupported() {
        use nix::unistd::Whence::{SeekData, SeekHole};
        for errno in [nix::errno::Errno::EINVAL, nix::errno::Errno::EOPNOTSUPP] {
            copies_with_seek_results(4, &[(SeekData, 4, Ok(4)), (SeekHole, 4, Err(errno))]);
        }
    }

    #[test]
    fn copies_densely_when_sparse_probes_do_not_advance() {
        use nix::unistd::Whence::{SeekData, SeekHole};
        copies_with_seek_results(4, &[(SeekData, 4, Ok(0))]);
        for hole in [0, 4] {
            copies_with_seek_results(4, &[(SeekData, 4, Ok(4)), (SeekHole, 4, Ok(hole))]);
        }
    }

    #[test]
    fn propagates_io_errors_from_hole_probes() {
        let tmp = tempfile::tempdir().unwrap();
        let mut src = make_file(tmp.path(), "src");
        src.write_all(b"data").unwrap();
        let dst = make_file(tmp.path(), "dst");
        let error = copy_sparse_fallback_with_seek(&src, &dst, 0, 4, |_, whence| match whence {
            nix::unistd::Whence::SeekData => Ok(0),
            nix::unistd::Whence::SeekHole => Err(nix::errno::Errno::EIO),
            _ => unreachable!(),
        })
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
    }

    #[test]
    fn retries_interrupted_extent_io_and_completes_short_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let contents = b"interrupted reads and short writes";
        let mut src = make_file(tmp.path(), "src");
        src.write_all(contents).unwrap();
        let dst = make_file(tmp.path(), "dst");
        let src = crate::testutils::InterruptedFile::new(src);
        let dst = crate::testutils::InterruptedFile::new(dst);
        copy_data_extent(&src, &dst, 0, contents.len() as u64, &mut [0; 8]).unwrap();
        assert_eq!(std::fs::read(tmp.path().join("dst")).unwrap(), contents);
    }

    #[test]
    fn does_not_pad_past_read_eof_when_seek_reports_a_larger_size() {
        for start in [0, 3] {
            for sparse in [false, true] {
                let tmp = tempfile::tempdir().unwrap();
                let contents = b"short";
                let mut src = make_file(tmp.path(), "src");
                src.write_all(contents).unwrap();
                let mut dst = make_file(tmp.path(), "dst");
                dst.write_all(&contents[..start as usize]).unwrap();
                // model regular virtual files whose advertised extents exceed readable data.
                let copied =
                    copy_sparse_fallback_with_seek(
                        &src,
                        &dst,
                        start,
                        16,
                        |_, whence| match whence {
                            nix::unistd::Whence::SeekData if sparse => Ok(start as libc::off_t),
                            nix::unistd::Whence::SeekData => Err(nix::errno::Errno::EOPNOTSUPP),
                            nix::unistd::Whence::SeekHole | nix::unistd::Whence::SeekEnd => Ok(16),
                            _ => unreachable!(),
                        },
                    )
                    .unwrap();
                assert_eq!(copied, contents.len() as u64);
                assert_eq!(std::fs::read(tmp.path().join("dst")).unwrap(), contents);
            }
        }
    }

    #[test]
    fn reconciles_a_source_shrunk_after_the_last_extent() {
        for start in [0, 4] {
            let tmp = tempfile::tempdir().unwrap();
            let contents = b"0123456789abcdef";
            let mut src = make_file(tmp.path(), "src");
            src.write_all(contents).unwrap();
            let mut dst = make_file(tmp.path(), "dst");
            dst.write_all(&contents[..start as usize]).unwrap();
            let copied = copy_sparse_fallback_with_seek(
                &src,
                &dst,
                start,
                contents.len() as u64,
                |offset, whence| match whence {
                    nix::unistd::Whence::SeekData => Ok(start as libc::off_t),
                    nix::unistd::Whence::SeekHole => Ok(contents.len() as libc::off_t),
                    nix::unistd::Whence::SeekEnd => {
                        src.set_len(3).unwrap();
                        nix::unistd::lseek(src.as_fd(), offset, whence)
                    }
                    _ => unreachable!(),
                },
            )
            .unwrap();
            assert_eq!(copied, 3);
            assert_eq!(std::fs::read(tmp.path().join("dst")).unwrap(), b"012");
        }
    }

    #[test]
    fn copies_bytes_identically() {
        let tmp = tempfile::tempdir().unwrap();
        let contents = b"the quick brown fox jumps over the lazy dog";
        let mut src = make_file(tmp.path(), "src");
        src.write_all(contents).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = copy_file_range_all(&src, &dst, contents.len() as u64).unwrap();
        assert_eq!(copied, contents.len() as u64);
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got, contents);
    }

    #[test]
    fn copies_without_copy_file_range_when_reflink_is_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let contents = b"a non-empty source with an unaligned size";
        let mut src = make_file(tmp.path(), "src");
        src.write_all(contents).unwrap();
        for len in [0, 7, contents.len() as u64, contents.len() as u64 + 100] {
            let dst = make_file(tmp.path(), "never");
            let calls_before = crate::testutils::copy_file_range_calls();
            let copied =
                copy_file_data(&src, &dst, len, ReflinkMode::Never, Default::default()).unwrap();
            let expected_len = len.min(contents.len() as u64);
            assert_eq!(copied, expected_len);
            assert_eq!(
                std::fs::read(tmp.path().join("never")).unwrap(),
                &contents[..expected_len as usize]
            );
            assert_eq!(crate::testutils::copy_file_range_calls(), calls_before);
        }
        // prove that the counter observes an attempted syscall, even without reflink support.
        let dst = make_file(tmp.path(), "auto");
        let calls_before = crate::testutils::copy_file_range_calls();
        copy_file_data(
            &src,
            &dst,
            contents.len() as u64,
            ReflinkMode::Auto,
            Default::default(),
        )
        .unwrap();
        assert!(crate::testutils::copy_file_range_calls() > calls_before);
        assert_eq!(std::fs::read(tmp.path().join("auto")).unwrap(), contents);
    }

    #[test]
    fn copies_from_zero_with_nonzero_source_and_destination_cursors() {
        let tmp = tempfile::tempdir().unwrap();
        let contents = b"independent source and destination cursors";
        let mut src = make_file(tmp.path(), "src");
        src.write_all(contents).unwrap();
        for (policy, len) in [
            (ReflinkMode::Auto, 7),
            (ReflinkMode::Auto, contents.len() as u64 + 10),
            (ReflinkMode::Never, contents.len() as u64),
        ] {
            let dst = make_file(tmp.path(), "dst");
            nix::unistd::lseek(src.as_fd(), 3, nix::unistd::Whence::SeekSet).unwrap();
            nix::unistd::lseek(dst.as_fd(), 11, nix::unistd::Whence::SeekSet).unwrap();
            let copied = copy_file_data(&src, &dst, len, policy, Default::default()).unwrap();
            let expected_len = len.min(contents.len() as u64);
            assert_eq!(copied, expected_len);
            assert_eq!(
                std::fs::read(tmp.path().join("dst")).unwrap(),
                &contents[..expected_len as usize]
            );
        }
    }

    #[test]
    fn preserves_sparse_suffix_after_partial_kernel_copy() {
        let tmp = tempfile::tempdir().unwrap();
        // leave whole 2 MiB regions as holes even on huge-page tmpfs.
        let logical = 8 * 1024 * 1024;
        let src = make_file(tmp.path(), "src");
        src.set_len(logical).unwrap();
        src.write_all_at(b"head", 0).unwrap();
        src.write_all_at(b"tail", 4 * 1024 * 1024).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = copies_with_kernel_results(
            &src,
            &dst,
            logical,
            &[
                (0, logical as usize, Ok(4096)),
                (4096, logical as usize - 4096, Err(nix::errno::Errno::EXDEV)),
            ],
            |_| {},
        )
        .unwrap();
        assert_eq!(copied, logical);
        assert_eq!(dst.metadata().unwrap().len(), logical);
        assert_eq!(
            std::fs::read(tmp.path().join("dst")).unwrap(),
            std::fs::read(tmp.path().join("src")).unwrap()
        );
        let src_allocated = src.metadata().unwrap().blocks() * 512;
        let dst_allocated = dst.metadata().unwrap().blocks() * 512;
        assert!(
            src_allocated < logical,
            "source fixture is not sparse: allocated {src_allocated} >= logical {logical}"
        );
        assert!(
            dst_allocated < logical,
            "destination is not sparse: dst={dst_allocated}, src={src_allocated}, logical={logical}"
        );
    }

    #[test]
    fn copies_sparse_contents_and_trailing_hole_with_auto() {
        let tmp = tempfile::tempdir().unwrap();
        let logical = 64 * 1024;
        let src = make_file(tmp.path(), "src");
        src.set_len(logical).unwrap();
        src.write_all_at(b"head", 0).unwrap();
        src.write_all_at(b"middle", logical / 2).unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied =
            copy_file_data(&src, &dst, logical, ReflinkMode::Auto, Default::default()).unwrap();
        assert_eq!(copied, logical);
        assert_eq!(dst.metadata().unwrap().len(), logical);
        assert_eq!(
            std::fs::read(tmp.path().join("dst")).unwrap(),
            std::fs::read(tmp.path().join("src")).unwrap()
        );
        // auto may allocate holes on filesystems without sparse copy_file_range support.
    }

    #[test]
    fn copies_large_file_fully() {
        let tmp = tempfile::tempdir().unwrap();
        let len: usize = 8 * 1024 * 1024;
        // varied bytes so a truncated/short copy would be detectable.
        let data: Vec<u8> = (0..len)
            .map(|i| (i.wrapping_mul(31) ^ (i >> 7)) as u8)
            .collect();
        let mut src = make_file(tmp.path(), "src");
        src.write_all(&data).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = copy_file_range_all(&src, &dst, len as u64).unwrap();
        assert_eq!(copied, len as u64);
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got.len(), len);
        assert!(got == data, "destination bytes differ from source");
    }

    #[test]
    fn preserves_holes_when_reflink_is_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let logical: u64 = 8 * 1024 * 1024;
        let head = b"HEAD-region-bytes";
        let tail = b"TAIL-region-bytes";
        let tail_off = logical - tail.len() as u64;
        let src = make_file(tmp.path(), "src");
        // create a sparse file: size = logical, small data near start and end,
        // big hole in the middle.
        nix::unistd::ftruncate(src.as_fd(), to_off_t(logical).unwrap()).unwrap();
        src.write_at(head, 0).unwrap();
        src.write_at(tail, tail_off).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        // never must preserve holes without relying on copy_file_range support.
        let copied =
            copy_file_data(&src, &dst, logical, ReflinkMode::Never, Default::default()).unwrap();
        assert_eq!(copied, logical);
        // (a) content byte-equal to src across the whole logical range.
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        let expected = std::fs::read(tmp.path().join("src")).unwrap();
        assert_eq!(got.len() as u64, logical);
        assert_eq!(got, expected, "destination content differs from source");
        // spot-check the data regions explicitly.
        assert_eq!(&got[..head.len()], head);
        assert_eq!(&got[tail_off as usize..], tail);
        // (b) dst size == logical.
        let dst_meta = std::fs::metadata(tmp.path().join("dst")).unwrap();
        assert_eq!(dst_meta.len(), logical);
        // (c) dst is actually sparse: allocated bytes << logical size.
        let src_meta = std::fs::metadata(tmp.path().join("src")).unwrap();
        let dst_allocated = dst_meta.blocks() * 512;
        let src_allocated = src_meta.blocks() * 512;
        eprintln!(
            "sparse blocks: src={} blocks ({} bytes), dst={} blocks ({} bytes), logical={} bytes",
            src_meta.blocks(),
            src_allocated,
            dst_meta.blocks(),
            dst_allocated,
            logical
        );
        assert!(
            dst_allocated < logical,
            "destination is not sparse: allocated {dst_allocated} >= logical {logical}"
        );
        // dst should be roughly as sparse as src (within a generous factor to
        // tolerate filesystem block-allocation differences).
        assert!(
            dst_allocated <= src_allocated * 4 + 4096,
            "destination far less sparse than source: dst={dst_allocated} src={src_allocated}"
        );
    }

    #[test]
    fn terminates_on_short_source() {
        let tmp = tempfile::tempdir().unwrap();
        let contents = b"short source contents";
        let real_len = contents.len() as u64;
        let mut src = make_file(tmp.path(), "src");
        src.write_all(contents).unwrap();
        src.sync_all().unwrap();
        // create the destination so the worker thread can open it for writing.
        let _dst = make_file(tmp.path(), "dst");
        // claim a length larger than the real file: must return promptly having
        // copied exactly the real bytes (the Ok(0)/EOF guard prevents a hang).
        let claimed = real_len + 4096;
        let (tx, rx) = std::sync::mpsc::channel();
        let src_path = tmp.path().join("src");
        let dst_path = tmp.path().join("dst");
        std::thread::spawn(move || {
            let src = std::fs::File::open(&src_path).unwrap();
            let dst = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&dst_path)
                .unwrap();
            let copied = copy_file_range_all(&src, &dst, claimed).unwrap();
            tx.send(copied).unwrap();
        });
        let copied = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("copy_file_range_all hung on a short source");
        assert_eq!(copied, real_len);
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got, contents);
    }

    #[test]
    fn copies_partial_len_on_both_paths() {
        // caller asks for fewer bytes than the source has: both paths must copy
        // exactly `len` bytes, leave dst at `len`, and return `len`.
        let tmp = tempfile::tempdir().unwrap();
        let full: Vec<u8> = (0u32..4096).map(|i| (i % 251) as u8).collect();
        let len: u64 = 1000;
        let mut src = make_file(tmp.path(), "src");
        src.write_all(&full).unwrap();
        src.sync_all().unwrap();
        // primary path.
        let dst_cfr = make_file(tmp.path(), "dst_cfr");
        let copied_cfr = copy_file_range_all(&src, &dst_cfr, len).unwrap();
        assert_eq!(copied_cfr, len);
        let got_cfr = std::fs::read(tmp.path().join("dst_cfr")).unwrap();
        assert_eq!(got_cfr.len() as u64, len);
        assert_eq!(got_cfr, &full[..len as usize]);
        // fallback path called directly.
        let dst_fb = make_file(tmp.path(), "dst_fb");
        let copied_fb = copy_sparse_fallback(&src, &dst_fb, 0, len).unwrap();
        assert_eq!(copied_fb, len);
        let got_fb = std::fs::read(tmp.path().join("dst_fb")).unwrap();
        assert_eq!(got_fb.len() as u64, len);
        assert_eq!(got_fb, &full[..len as usize]);
    }

    #[test]
    fn fallback_shrunk_source_not_padded() {
        // locks the primary/fallback reconciliation: when `len` exceeds the
        // source size, the fallback must size dst to the actual source end (not
        // pad a spurious trailing hole up to `len`) and return the actual bytes.
        let tmp = tempfile::tempdir().unwrap();
        let contents: Vec<u8> = (0u32..3000).map(|i| (i % 240) as u8).collect();
        let s = contents.len() as u64;
        let mut src = make_file(tmp.path(), "src");
        src.write_all(&contents).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = copy_sparse_fallback(&src, &dst, 0, s + 8192).unwrap();
        assert_eq!(
            copied, s,
            "must return actual source bytes, not the claimed len"
        );
        let dst_meta = std::fs::metadata(tmp.path().join("dst")).unwrap();
        assert_eq!(
            dst_meta.len(),
            s,
            "dst must be sized to the source, not padded"
        );
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got, contents);
    }

    #[test]
    fn copies_zero_length() {
        let tmp = tempfile::tempdir().unwrap();
        let mut src = make_file(tmp.path(), "src");
        src.write_all(b"non-empty source contents").unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let calls_before = crate::testutils::copy_file_range_calls();
        let copied = copy_file_range_all(&src, &dst, 0).unwrap();
        assert_eq!(copied, 0);
        assert_eq!(crate::testutils::copy_file_range_calls(), calls_before);
        let dst_meta = std::fs::metadata(tmp.path().join("dst")).unwrap();
        assert_eq!(dst_meta.len(), 0, "zero-length copy must leave dst empty");
    }

    #[test]
    fn copies_all_hole_source_when_reflink_is_disabled() {
        // a pure-hole source: SEEK_DATA returns ENXIO immediately, so the loop
        // does no copies and only ftruncate sizes dst. dst must be all zeros,
        // sized to `len`, and sparse.
        let tmp = tempfile::tempdir().unwrap();
        let logical: u64 = 4 * 1024 * 1024;
        let src = make_file(tmp.path(), "src");
        nix::unistd::ftruncate(src.as_fd(), to_off_t(logical).unwrap()).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied =
            copy_file_data(&src, &dst, logical, ReflinkMode::Never, Default::default()).unwrap();
        assert_eq!(copied, logical);
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got.len() as u64, logical);
        assert!(
            got.iter().all(|&b| b == 0),
            "all-hole copy must be all zeros"
        );
        let dst_meta = std::fs::metadata(tmp.path().join("dst")).unwrap();
        assert_eq!(dst_meta.len(), logical);
        let dst_allocated = dst_meta.blocks() * 512;
        eprintln!(
            "all-hole: dst={} blocks ({dst_allocated} bytes), logical={logical}",
            dst_meta.blocks()
        );
        assert!(
            dst_allocated < logical,
            "all-hole destination is not sparse: allocated {dst_allocated} >= logical {logical}"
        );
    }

    #[test]
    fn classify_seek_data_routes_by_errno() {
        // the testable seam for the sparse-vs-dense decision. data offset and a
        // legitimate trailing hole stay on the sparse path; the "unsupported"
        // errnos route to dense; a genuine I/O error propagates.
        assert_eq!(
            classify_seek_data(Ok(4096)).unwrap(),
            SparseProbe::Data(4096)
        );
        assert_eq!(
            classify_seek_data(Err(nix::errno::Errno::ENXIO)).unwrap(),
            SparseProbe::TrailingHole
        );
        // EINVAL and EOPNOTSUPP (== ENOTSUP on Linux) mean "fs can't SEEK_DATA".
        assert_eq!(
            classify_seek_data(Err(nix::errno::Errno::EINVAL)).unwrap(),
            SparseProbe::Unsupported
        );
        assert_eq!(
            classify_seek_data(Err(nix::errno::Errno::EOPNOTSUPP)).unwrap(),
            SparseProbe::Unsupported
        );
        assert_eq!(
            classify_seek_data(Err(nix::errno::Errno::ENOTSUP)).unwrap(),
            SparseProbe::Unsupported
        );
        // a genuine I/O error is NOT masked as "unsupported" — it propagates.
        let err = classify_seek_data(Err(nix::errno::Errno::EIO)).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
    }

    #[test]
    fn dense_copy_is_byte_exact_with_embedded_zeros() {
        // dense_copy must reproduce the source byte-for-byte, including embedded
        // zero regions (it does not preserve them as holes, just copies zeros).
        let tmp = tempfile::tempdir().unwrap();
        let len: usize = 3 * FALLBACK_BUF_SIZE + 777; // multiple buffers + a tail.
        let mut data: Vec<u8> = (0..len)
            .map(|i| (i.wrapping_mul(37) ^ (i >> 5)) as u8)
            .collect();
        // carve out a couple of embedded zero regions (crossing a buffer edge).
        for b in data.iter_mut().take(FALLBACK_BUF_SIZE + 4096).skip(100) {
            *b = 0;
        }
        for b in data
            .iter_mut()
            .take(2 * FALLBACK_BUF_SIZE)
            .skip(2 * FALLBACK_BUF_SIZE - 500)
        {
            *b = 0;
        }
        let mut src = make_file(tmp.path(), "src");
        src.write_all(&data).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = dense_copy(&src, &dst, 0, len as u64).unwrap();
        assert_eq!(copied, len as u64);
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got.len(), len, "dense copy must size dst to the source");
        assert!(got == data, "dense copy bytes differ from source");
    }

    #[test]
    fn dense_copy_partial_len_sizes_dst_exactly() {
        // when `len` is below the source size, dense_copy copies exactly `len`
        // bytes and ftruncates dst to `len` (matching the sparse path).
        let tmp = tempfile::tempdir().unwrap();
        let full: Vec<u8> = (0u32..8192).map(|i| (i % 251) as u8).collect();
        let len: u64 = 5000;
        let mut src = make_file(tmp.path(), "src");
        src.write_all(&full).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = dense_copy(&src, &dst, 0, len).unwrap();
        assert_eq!(copied, len);
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got.len() as u64, len);
        assert_eq!(got, &full[..len as usize]);
    }

    #[test]
    fn dense_copy_shrunk_source_not_padded() {
        // mirrors `fallback_shrunk_source_not_padded` for the dense path: a `len`
        // larger than the source must size dst to the actual source end and
        // return the actual bytes, not pad up to `len`.
        let tmp = tempfile::tempdir().unwrap();
        let contents: Vec<u8> = (0u32..3000).map(|i| (i % 240) as u8).collect();
        let s = contents.len() as u64;
        let mut src = make_file(tmp.path(), "src");
        src.write_all(&contents).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = dense_copy(&src, &dst, 0, s + 8192).unwrap();
        assert_eq!(
            copied, s,
            "dense copy must return actual source bytes, not the claimed len"
        );
        let dst_meta = std::fs::metadata(tmp.path().join("dst")).unwrap();
        assert_eq!(dst_meta.len(), s, "dst must be sized to the source");
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got, contents);
    }

    #[test]
    fn copies_densely_when_the_initial_probe_is_unsupported() {
        for errno in [nix::errno::Errno::EINVAL, nix::errno::Errno::EOPNOTSUPP] {
            copies_with_seek_results(4, &[(nix::unistd::Whence::SeekData, 4, Err(errno))]);
        }
    }
}
