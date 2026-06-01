//! Low-level data-copy primitive used by the copy path.
//!
//! This replaces `std::fs::copy` so the copy path can keep the destination fd
//! open across the data copy and the subsequent metadata operations (closing
//! the TOCTOU window between writing bytes and setting times/owner/mode).
//!
//! The copy uses a three-tier fallback chain, fastest first:
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
//! # Durability
//!
//! These are synchronous syscalls, so once they return the writes are in the
//! kernel. This function deliberately does **not** `fsync`/`flush` — durability
//! is out of scope here. mtime correctness is the caller's responsibility (it
//! sets times after this returns).
use std::fs::File;
use std::os::fd::AsFd;
use std::os::unix::fs::FileExt;

/// sparse-aware userspace fallback copy buffer size (1 MiB).
const FALLBACK_BUF_SIZE: usize = 1024 * 1024;

/// dense read/write fallback copy buffer size (128 KiB). Smaller than the
/// sparse buffer because this path is the last-resort robustness fallback for
/// filesystems that can't do `SEEK_DATA`/`SEEK_HOLE`, not a throughput path.
const DENSE_BUF_SIZE: usize = 128 * 1024;

/// Copy up to `len` bytes from `src` to `dst` using the in-kernel
/// `copy_file_range` (reflink/server-side capable), falling back to a
/// sparse-aware userspace copy when the kernel/filesystem can't. Both files are
/// already open; offsets start at `0`. Returns the number of bytes copied.
///
/// See the module docs for snapshot-size and durability semantics.
pub fn copy_file_range_all(src: &File, dst: &File, len: u64) -> std::io::Result<u64> {
    // establish the documented offset-0 start on both fds. The caller may hand
    // us descriptors whose offsets were advanced by an earlier read/stat, and
    // copy_file_range with `None` offsets uses each fd's current position.
    nix::unistd::lseek(src.as_fd(), 0, nix::unistd::Whence::SeekSet)
        .map_err(std::io::Error::from)?;
    nix::unistd::lseek(dst.as_fd(), 0, nix::unistd::Whence::SeekSet)
        .map_err(std::io::Error::from)?;
    let mut copied: u64 = 0;
    while copied < len {
        let remaining = usize::try_from(len - copied).unwrap_or(usize::MAX);
        // None offsets => the kernel uses and advances each fd's own offset,
        // so successive calls naturally continue where the last left off.
        match nix::fcntl::copy_file_range(src.as_fd(), None, dst.as_fd(), None, remaining) {
            Ok(0) => {
                // EOF: the source is shorter than `len` (a shrink). Stop here
                // rather than spinning forever on a zero-byte copy.
                return Ok(copied);
            }
            Ok(n) => copied += n as u64,
            Err(errno) => {
                // these errnos mean copy_file_range is unsupported for this
                // pair (no kernel support, cross-filesystem, bad arguments,
                // or the fs rejects it) -> fall back for the remaining range.
                // note: ENOTSUP == EOPNOTSUPP numerically on Linux.
                if matches!(
                    errno,
                    nix::errno::Errno::ENOSYS
                        | nix::errno::Errno::EXDEV
                        | nix::errno::Errno::EINVAL
                        | nix::errno::Errno::EOPNOTSUPP
                ) {
                    // the fallback returns only the bytes IT moved ([copied, final]);
                    // add the prefix the primary path already copied so the total is
                    // reported correctly.
                    return copy_sparse_fallback(src, dst, copied, len)
                        .map(|fallback| copied + fallback);
                }
                return Err(std::io::Error::from(errno));
            }
        }
    }
    Ok(copied)
}

/// Classification of an initial `SEEK_DATA` probe, used to decide whether the
/// sparse fallback can run or must degrade to a dense read/write copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SparseProbe {
    /// `SEEK_DATA` found a data region starting at this offset.
    Data(u64),
    /// `SEEK_DATA` returned `ENXIO`: no data at or after the probe offset, i.e.
    /// the rest of the file up to its logical end is a hole.
    TrailingHole,
    /// the filesystem does not support `SEEK_DATA`/`SEEK_HOLE` (`EINVAL` or
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
/// Used as the fallback when `copy_file_range` is unsupported. It first probes
/// the source with `SEEK_DATA`; if the filesystem supports it, it walks the
/// source's data regions with `SEEK_DATA`/`SEEK_HOLE` and copies only the data
/// extents, so holes (e.g. in a sparse VM or Lustre image) are preserved
/// instead of being expanded to fully-allocated zeros. If the filesystem does
/// not support those whences (some FUSE/older/unusual filesystems return
/// `EINVAL`/`ENOTSUP`), it degrades to [`dense_copy`], a plain read/write loop
/// that always works on a regular file (this matches what `std::fs::copy` would
/// have done — see the module docs). A genuine I/O error during the probe (e.g.
/// `EIO`) is propagated rather than masked as "unsupported".
///
/// The final size is reconciled with the source's *actual* end so this path
/// agrees with the primary one on a shrunk source: after the data loop it
/// computes `final = min(len, actual_eof)` and `ftruncate`s `dst` to `final`.
/// A legitimate trailing hole (source logical size still == `len`) leaves
/// `actual_eof == len`, so `dst` ends at `len` with the trailing region
/// unallocated; a source that shrank below `len` (`actual_eof < len`) sizes
/// `dst` to `actual_eof` rather than padding a spurious hole up to `len`.
///
/// Returns `final - start`, i.e. the number of bytes of the logical copy that
/// the source actually provided — matching the primary path's return.
pub(crate) fn copy_sparse_fallback(
    src: &File,
    dst: &File,
    start: u64,
    len: u64,
) -> std::io::Result<u64> {
    if start >= len {
        // only reachable with `start == len`: the primary path copied the whole
        // logical range before erroring, so the source was at least `len` and
        // there is nothing left to copy. `min(len, actual_eof) == len` here, so
        // sizing `dst` to `len` is correct and can't drop already-copied bytes.
        nix::unistd::ftruncate(dst.as_fd(), to_off_t(len)?).map_err(std::io::Error::from)?;
        return Ok(0);
    }
    // probe the source for sparse support before committing to the sparse walk.
    // if the filesystem can't do SEEK_DATA/SEEK_HOLE, fall back to a dense copy
    // of the whole range; a genuine I/O error propagates from classify_seek_data.
    let probe = classify_seek_data(nix::unistd::lseek(
        src.as_fd(),
        to_off_t(start)?,
        nix::unistd::Whence::SeekData,
    ))?;
    let first_data = match probe {
        SparseProbe::Unsupported => return dense_copy(src, dst, start, len),
        // no data at all before EOF: nothing to copy, the trailing ftruncate
        // below sizes dst (a fully-hole range).
        SparseProbe::TrailingHole => len,
        SparseProbe::Data(d) => d,
    };
    // the data loop reads/writes at explicit offsets via read_at/write_at, so we
    // don't pre-seek the fds here; the scan starts from the probed first data
    // offset and re-runs SEEK_DATA for subsequent regions.
    let mut buf = vec![0u8; FALLBACK_BUF_SIZE];
    let mut off = start;
    let mut next_data = first_data;
    while off < len {
        // `next_data` holds the next data region at or after `off` (seeded by the
        // initial probe, then refreshed by SEEK_DATA at the bottom of the loop).
        let data = next_data;
        if data >= len {
            break;
        }
        // find the hole that ends this data region; clamp to `len`.
        let hole =
            match nix::unistd::lseek(src.as_fd(), to_off_t(data)?, nix::unistd::Whence::SeekHole) {
                Ok(h) => (h as u64).min(len),
                // a data region with no following hole means data extends to EOF;
                // clamp to `len`.
                Err(nix::errno::Errno::ENXIO) => len,
                Err(errno) => return Err(std::io::Error::from(errno)),
            };
        copy_data_extent(src, dst, data, hole, &mut buf)?;
        off = hole;
        if off >= len {
            break;
        }
        // find the next data region for the following iteration. the filesystem
        // already proved it supports SEEK_DATA on the initial probe, so an
        // Unsupported result here would be anomalous; handle it defensively by
        // dense-copying the remaining range (dense_copy reconciles dst's final
        // size for the whole file, so the already-copied prefix is preserved).
        next_data = match classify_seek_data(nix::unistd::lseek(
            src.as_fd(),
            to_off_t(off)?,
            nix::unistd::Whence::SeekData,
        ))? {
            SparseProbe::Data(d) => d,
            SparseProbe::TrailingHole => break, // no more data -> trailing hole
            SparseProbe::Unsupported => return dense_copy(src, dst, off, len),
        };
    }
    // reconcile the final size with the source's actual end so we agree with the
    // primary path on a shrunk source. `min(len, actual_eof)`: a legitimate
    // trailing hole keeps `actual_eof == len` (dst ends at `len`, trailing region
    // unallocated); a source that shrank below `len` sizes dst to `actual_eof`
    // rather than padding a spurious hole up to `len`.
    let actual_eof = nix::unistd::lseek(src.as_fd(), 0, nix::unistd::Whence::SeekEnd)
        .map_err(std::io::Error::from)? as u64;
    let final_size = len.min(actual_eof);
    nix::unistd::ftruncate(dst.as_fd(), to_off_t(final_size)?).map_err(std::io::Error::from)?;
    // saturating: if the source shrank below `start` between the primary copy
    // and this check, the fallback added no bytes (final < start).
    Ok(final_size.saturating_sub(start))
}

/// Copy the data extent `[from, to)` from `src` to `dst` at the same offsets
/// using explicit positioned reads/writes, handling short reads and writes.
fn copy_data_extent(
    src: &File,
    dst: &File,
    from: u64,
    to: u64,
    buf: &mut [u8],
) -> std::io::Result<()> {
    let mut pos = from;
    while pos < to {
        let want = usize::try_from((to - pos).min(buf.len() as u64)).unwrap_or(buf.len());
        let n = src.read_at(&mut buf[..want], pos)?;
        if n == 0 {
            // source ended earlier than SEEK_HOLE implied (e.g. a concurrent
            // shrink) -> stop; the trailing ftruncate will fix up the size.
            break;
        }
        let mut written = 0;
        while written < n {
            let w = dst.write_at(&buf[written..n], pos + written as u64)?;
            if w == 0 {
                // a zero-length write on a non-empty buffer would spin forever.
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
            }
            written += w;
        }
        pos += n as u64;
    }
    Ok(())
}

/// Dense read/write copy of the range `[start, len)` from `src` to `dst`, the
/// final robustness fallback for filesystems that don't support
/// `SEEK_DATA`/`SEEK_HOLE`.
///
/// Reads fixed-size buffers from `src` and writes them to `dst` at the same
/// offsets until the source ends or `len` is reached, retrying on `EINTR` and
/// handling short reads/writes. Unlike the sparse path it does not preserve
/// holes — it reads zeros out of a hole and writes them — but it always works
/// on any regular file (this matches what `std::fs::copy` would have done).
///
/// Size reconciliation matches [`copy_sparse_fallback`]: it `ftruncate`s `dst`
/// to `min(len, actual_eof)` so a source that shrank below `len` sizes `dst` to
/// its real end (no spurious trailing padding), and a source at least `len`
/// long sizes `dst` to exactly `len`. Returns `final - start`, the number of
/// bytes of the logical copy the source actually provided.
fn dense_copy(src: &File, dst: &File, start: u64, len: u64) -> std::io::Result<u64> {
    let mut buf = vec![0u8; DENSE_BUF_SIZE];
    let mut pos = start;
    while pos < len {
        let want = usize::try_from((len - pos).min(buf.len() as u64)).unwrap_or(buf.len());
        let n = match src.read_at(&mut buf[..want], pos) {
            Ok(0) => break, // EOF: source is shorter than `len` (a shrink).
            Ok(n) => n,
            // a signal interrupted the read before any bytes moved: retry.
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        };
        let mut written = 0;
        while written < n {
            match dst.write_at(&buf[written..n], pos + written as u64) {
                // a zero-length write on a non-empty buffer would spin forever.
                Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero)),
                Ok(w) => written += w,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
        pos += n as u64;
    }
    // reconcile the final size with the source's actual end, matching the sparse
    // path: `min(len, actual_eof)` sizes dst to the real source end on a shrink
    // and to exactly `len` otherwise. this also fixes up the size when the loop
    // stopped early on an EOF that the read loop detected before reaching `len`.
    let actual_eof = nix::unistd::lseek(src.as_fd(), 0, nix::unistd::Whence::SeekEnd)
        .map_err(std::io::Error::from)? as u64;
    let final_size = len.min(actual_eof);
    nix::unistd::ftruncate(dst.as_fd(), to_off_t(final_size)?).map_err(std::io::Error::from)?;
    // saturating: if the source shrank below `start`, no bytes were added.
    Ok(final_size.saturating_sub(start))
}

/// convert a `u64` byte offset/length to the libc `off_t` expected by nix's
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

    fn make_file(dir: &std::path::Path, name: &str) -> File {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.join(name))
            .expect("open temp file")
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
    fn sparse_fallback_preserves_holes() {
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
        // call the fallback directly rather than relying on triggering EXDEV.
        let copied = copy_sparse_fallback(&src, &dst, 0, logical).unwrap();
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
    fn fallback_all_hole_source() {
        // a pure-hole source: SEEK_DATA returns ENXIO immediately, so the loop
        // does no copies and only ftruncate sizes dst. dst must be all zeros,
        // sized to `len`, and sparse.
        let tmp = tempfile::tempdir().unwrap();
        let logical: u64 = 4 * 1024 * 1024;
        let src = make_file(tmp.path(), "src");
        nix::unistd::ftruncate(src.as_fd(), to_off_t(logical).unwrap()).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = copy_sparse_fallback(&src, &dst, 0, logical).unwrap();
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
        let len: usize = 3 * DENSE_BUF_SIZE + 777; // multiple buffers + a tail.
        let mut data: Vec<u8> = (0..len)
            .map(|i| (i.wrapping_mul(37) ^ (i >> 5)) as u8)
            .collect();
        // carve out a couple of embedded zero regions (crossing a buffer edge).
        for b in data.iter_mut().take(DENSE_BUF_SIZE + 4096).skip(100) {
            *b = 0;
        }
        for b in data
            .iter_mut()
            .take(2 * DENSE_BUF_SIZE)
            .skip(2 * DENSE_BUF_SIZE - 500)
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
    fn unsupported_probe_routes_to_dense_byte_exact() {
        // exercises the fallback selection end-to-end at the seam level: an
        // "unsupported" SEEK_DATA probe must route to `dense_copy`, which on this
        // (tmpfs) environment we drive directly because tmpfs supports SEEK_DATA
        // and a true EINVAL can't be simulated here (see note below). We assert
        // both halves of the routing contract: (1) the classifier maps the
        // unsupported errnos to `SparseProbe::Unsupported`, and (2) the
        // `Unsupported` branch's target (`dense_copy`) yields a byte-exact copy.
        //
        // NOTE: a genuine end-to-end SEEK_DATA EINVAL is not simulable in this
        // environment (tmpfs/ext4 both support SEEK_DATA/SEEK_HOLE); it would
        // require a FUSE/older filesystem that rejects those whences. The routing
        // is therefore validated via the testable seam rather than a forced
        // errno, with the byte-exactness of the dense target asserted directly.
        assert_eq!(
            classify_seek_data(Err(nix::errno::Errno::EINVAL)).unwrap(),
            SparseProbe::Unsupported,
            "unsupported probe must route to the dense fallback"
        );
        let tmp = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0u32..200_000).map(|i| (i % 256) as u8).collect();
        let mut src = make_file(tmp.path(), "src");
        src.write_all(&data).unwrap();
        src.sync_all().unwrap();
        let dst = make_file(tmp.path(), "dst");
        let copied = dense_copy(&src, &dst, 0, data.len() as u64).unwrap();
        assert_eq!(copied, data.len() as u64);
        let got = std::fs::read(tmp.path().join("dst")).unwrap();
        assert_eq!(got, data, "dense fallback target must be byte-exact");
    }
}
