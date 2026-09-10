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
//! These size bounds do not provide a consistent content snapshot of a source
//! that is modified during the copy.
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
/// File cursor positions after copying are unspecified.
/// The destination must be empty and exclusively owned by this copy operation.
/// `Never` bypasses all `copy_file_range` acceleration, including non-reflink
/// kernel and server-side copying. See the module docs for size and durability semantics.
pub fn copy_file_data(
    src: &File,
    dst: &File,
    len: u64,
    reflink: ReflinkMode,
) -> std::io::Result<u64> {
    match reflink {
        ReflinkMode::Auto => copy_file_range_all(src, dst, len),
        ReflinkMode::Never => copy_sparse_fallback(src, dst, 0, len),
    }
}

/// Copy up to `len` bytes from `src` to `dst` using the in-kernel
/// `copy_file_range` (reflink/server-side capable), falling back to a
/// sparse-aware userspace copy when the kernel/filesystem can't. Both files are
/// already open; offsets start at `0`. Returns the number of bytes copied.
/// The destination must be empty and exclusively owned by this copy operation.
///
/// See the module docs for snapshot-size and durability semantics.
fn copy_file_range_all(src: &File, dst: &File, len: u64) -> std::io::Result<u64> {
    copy_file_range_all_with(src, dst, len, |remaining| {
        #[cfg(test)]
        crate::testutils::record_copy_file_range_call();
        nix::fcntl::copy_file_range(src.as_fd(), None, dst.as_fd(), None, remaining)
    })
}

fn copy_file_range_all_with(
    src: &File,
    dst: &File,
    len: u64,
    mut copy_range: impl FnMut(usize) -> nix::Result<usize>,
) -> std::io::Result<u64> {
    // establish the documented offset-0 start on both fds. The caller may hand
    // us descriptors whose offsets were advanced by an earlier read, and
    // copy_file_range with `None` offsets uses each fd's current position.
    nix::unistd::lseek(src.as_fd(), 0, nix::unistd::Whence::SeekSet)
        .map_err(std::io::Error::from)?;
    nix::unistd::lseek(dst.as_fd(), 0, nix::unistd::Whence::SeekSet)
        .map_err(std::io::Error::from)?;
    let mut copied: u64 = 0;
    while copied < len {
        let remaining = usize::try_from(len - copied).unwrap_or(usize::MAX);
        match copy_range(remaining) {
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
/// computes `final = min(len, actual_eof)` and sizes `dst` to `final` if needed.
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
    let mut buf = Vec::new();
    while off < len {
        let extent = match next_copy_extent(&mut seek, off, len)? {
            CopyExtent::Sparse(extent) => extent,
            CopyExtent::Dense => off..len,
            CopyExtent::End => break,
        };
        // allocate only for data, cap small extents, and reuse the buffer across the walk.
        let capacity = (extent.end - extent.start).min(FALLBACK_BUF_SIZE as u64) as usize;
        if buf.len() < capacity {
            buf.resize(capacity, 0);
        }
        let end = copy_data_extent(src, dst, extent.start, extent.end, &mut buf[..capacity])?;
        if end > extent.start {
            written_end = end;
        }
        if end < extent.end {
            break; // the source shrank during the read loop.
        }
        off = extent.end;
    }
    // keep the post-loop EOF observation: a source can shrink even after the last write.
    let actual_eof = seek(0, nix::unistd::Whence::SeekEnd).map_err(std::io::Error::from)? as u64;
    let final_size = len.min(actual_eof);
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
/// Returns the offset reached, which can be below `to` if the source shrank.
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

    #[test]
    fn reports_final_size_when_source_shrinks_after_partial_kernel_copy() {
        for outcome in [Ok(0), Err(nix::errno::Errno::EXDEV)] {
            for final_len in [0, 3, 8, 12] {
                let tmp = tempfile::tempdir().unwrap();
                let contents = b"0123456789abcdef";
                let mut src = make_file(tmp.path(), "src");
                src.write_all(contents).unwrap();
                let dst = make_file(tmp.path(), "dst");
                let mut calls = 0;
                let copied = copy_file_range_all_with(&src, &dst, 16, |remaining| {
                    calls += 1;
                    match calls {
                        1 => {
                            assert_eq!(remaining, 16);
                            dst.write_all_at(&contents[..8], 0).unwrap();
                            Ok(8)
                        }
                        2 => {
                            assert_eq!(remaining, 8);
                            src.set_len(final_len).unwrap();
                            outcome
                        }
                        _ => panic!("kernel copying must stop after falling back"),
                    }
                })
                .unwrap();
                assert_eq!(calls, 2);
                assert_eq!(copied, final_len);
                assert_eq!(dst.metadata().unwrap().len(), final_len);
                assert_eq!(
                    std::fs::read(tmp.path().join("dst")).unwrap(),
                    &contents[..final_len as usize]
                );
            }
        }
    }

    #[test]
    fn retries_interrupted_kernel_copies_without_restarting_the_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let contents = b"0123456789abcdef";
        let mut src = make_file(tmp.path(), "src");
        src.write_all(contents).unwrap();
        let dst = make_file(tmp.path(), "dst");
        let mut calls = 0;
        let copied = copy_file_range_all_with(&src, &dst, 16, |remaining| {
            calls += 1;
            match calls {
                1 => {
                    assert_eq!(remaining, 16);
                    dst.write_all_at(&contents[..8], 0).unwrap();
                    Ok(8)
                }
                2 => {
                    assert_eq!(remaining, 8);
                    Err(nix::errno::Errno::EINTR)
                }
                3 => {
                    assert_eq!(remaining, 8);
                    dst.write_all_at(&contents[8..], 8).unwrap();
                    Ok(8)
                }
                _ => panic!("kernel copying must stop after completing the range"),
            }
        })
        .unwrap();
        assert_eq!(calls, 3);
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
            let copied = copy_file_data(&src, &dst, len, ReflinkMode::Never).unwrap();
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
        copy_file_data(&src, &dst, contents.len() as u64, ReflinkMode::Auto).unwrap();
        assert!(crate::testutils::copy_file_range_calls() > calls_before);
        assert_eq!(std::fs::read(tmp.path().join("auto")).unwrap(), contents);
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
        let copied = copy_file_data(&src, &dst, logical, ReflinkMode::Never).unwrap();
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
        let copied = copy_file_range_all(&src, &dst, 0).unwrap();
        assert_eq!(copied, 0);
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
        let copied = copy_file_data(&src, &dst, logical, ReflinkMode::Never).unwrap();
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
