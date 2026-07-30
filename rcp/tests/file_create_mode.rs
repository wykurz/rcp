//! A destination file must exist owner-only until its contents are written.
//!
//! The window these tests pin down is a privilege-escalation one. A root copier holds
//! `CAP_FSETID`, so writing to a file does NOT clear `S_ISUID`: a destination created at its final
//! `04755` would be a complete, functional setuid-root executable — whose contents the source's
//! owner authored — from the moment the last byte lands until the closing `fchmod`. See
//! `docs/tocttou.md`.

use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::time::{Duration, Instant};

#[path = "support/fixtures.rs"]
mod fixtures;
use fixtures::{
    create_test_file, describe_samples, get_file_content, get_file_mode, sample_while_running,
    setup_test_env,
};

/// One metadata syscall per second, which stretches the create → `fchmod` window a fast copy
/// closes in microseconds into a whole second. This is what makes the observation deterministic:
/// a multi-hundred-megabyte fixture would not be, because `copy_file_range` reflinks on btrfs/xfs
/// and closes the window whatever the file size.
const OPS_THROTTLE: &str = "--ops-throttle=1";

/// The mode a destination file is created at (`common::safedir::DST_FILE_CREATE_MODE`).
const CREATE_MODE: u32 = 0o600;

/// The umask the observing tests pin their child to, so the exact create mode they assert on does
/// not depend on the umask the test runner inherited. `copy_applies_source_mode_to_file_regardless_of_umask`
/// varies it on purpose.
const PINNED_UMASK: libc::mode_t = 0o022;

/// An `rcp` command whose umask is fixed for the child only — the test process keeps its own, so
/// this stays safe when the test binary runs several tests in one process.
fn rcp_with_umask(umask: libc::mode_t) -> std::process::Command {
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin("rcp"));
    // SAFETY: `umask(2)` is async-signal-safe and touches only the freshly forked child.
    unsafe {
        cmd.pre_exec(move || {
            libc::umask(umask);
            Ok(())
        });
    }
    cmd
}

/// Wait for `path` to appear while `child` runs, failing rather than hanging if it never does.
fn wait_for_creation(child: &mut std::process::Child, path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while std::fs::symlink_metadata(path).is_err() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "rcp exited before creating {path:?}"
        );
        assert!(
            Instant::now() < deadline,
            "{path:?} was never created; rcp is still running"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn copy_creates_file_owner_only_until_contents_are_written() {
    /// The source mode, and so the mode the destination must end at.
    const SRC_MODE: u32 = 0o4755;
    let (src_dir, dst_dir) = setup_test_env();
    // the reachable case: a world-searchable destination directory, i.e. any repeat or incremental
    // copy into an existing tree
    std::fs::set_permissions(dst_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let src_file = src_dir.path().join("setuid.bin");
    let dst_file = dst_dir.path().join("setuid.bin");
    create_test_file(&src_file, "payload", SRC_MODE);
    let full_size = std::fs::metadata(&src_file).unwrap().len();
    let child = rcp_with_umask(PINNED_UMASK)
        .args([
            "--preserve-settings=all",
            OPS_THROTTLE,
            src_file.to_str().unwrap(),
            dst_file.to_str().unwrap(),
        ])
        .spawn()
        .unwrap();
    let (status, samples) = sample_while_running(child, &dst_file);
    assert!(status.success(), "rcp failed: {status:?}");
    // the destination must only ever be seen in one of two states: owner-only while it is being
    // filled in, or complete at the source mode. anything between is a window in which a
    // half-written file carries permissions it has not earned.
    let observed = describe_samples(&samples);
    for &(mode, size) in &samples {
        assert!(
            mode == CREATE_MODE || mode == SRC_MODE,
            "destination observed at {mode:o} (size {size}); expected {CREATE_MODE:o} while being \
             written or {SRC_MODE:o} once complete. samples: {observed}"
        );
        assert!(
            size == full_size || mode & 0o7077 == 0,
            "an incomplete destination (size {size} of {full_size}) was observed at {mode:o}, \
             readable outside the copier. samples: {observed}"
        );
    }
    // fail loudly rather than vacuously: without the owner-only create there is no such sample at
    // all, so this is what turns the loop above into a regression guard.
    assert!(
        samples.iter().any(|&(mode, _)| mode == CREATE_MODE),
        "the destination was never observed owner-only, so the copy published it at its final \
         mode before writing its contents. samples: {observed}"
    );
    assert_eq!(get_file_mode(&dst_file), SRC_MODE);
}

#[test]
fn preserves_setuid_file_mode_when_created_owner_only() {
    // a zero-length source is covered too: `copy_file_range` moves no bytes for it, so the final
    // chmod is the only thing standing between the create mode and the source mode.
    for (contents, mode) in [("payload", 0o4755), ("", 0o4755), ("payload", 0o6755)] {
        let (src_dir, dst_dir) = setup_test_env();
        let src_file = src_dir.path().join("setuid.bin");
        let dst_file = dst_dir.path().join("setuid.bin");
        create_test_file(&src_file, contents, mode);
        // a source mode the kernel may refuse to reproduce for a non-root copier would make this
        // test about privileges rather than about the create mode
        assert_eq!(get_file_mode(&src_file), mode, "cannot set up {mode:o}");
        let status = rcp_with_umask(PINNED_UMASK)
            .args([
                "--preserve-settings=all",
                src_file.to_str().unwrap(),
                dst_file.to_str().unwrap(),
            ])
            .status()
            .unwrap();
        assert!(status.success(), "rcp failed for {mode:o}: {status:?}");
        assert_eq!(
            get_file_mode(&dst_file),
            mode,
            "special bits lost for a {} file",
            if contents.is_empty() {
                "zero-length"
            } else {
                "non-empty"
            }
        );
        assert_eq!(get_file_content(&dst_file), contents);
    }
}

#[test]
fn interrupted_copy_leaves_partial_file_owner_only() {
    let (src_dir, dst_dir) = setup_test_env();
    std::fs::set_permissions(dst_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let src_file = src_dir.path().join("setuid.bin");
    let dst_file = dst_dir.path().join("setuid.bin");
    create_test_file(&src_file, "payload", 0o4755);
    let mut child = rcp_with_umask(PINNED_UMASK)
        .args([
            "--preserve-settings=all",
            OPS_THROTTLE,
            src_file.to_str().unwrap(),
            dst_file.to_str().unwrap(),
        ])
        .spawn()
        .unwrap();
    // kill the copy inside the create → `fchmod` window the throttle holds open
    wait_for_creation(&mut child, &dst_file);
    child.kill().unwrap();
    child.wait().unwrap();
    // the accepted behavior change: what an interrupted copy leaves behind is readable only by the
    // copier, never the source's 4755. a later run normally re-copies it — a partial file differs
    // in size, and a data-complete one never reached `futimens`, so it differs in mtime. "normally"
    // because `metadata_equal` skips the nanosecond comparison when either side's `mtime_nsec` is
    // zero; see `docs/tocttou.md` for the same-whole-second exception that leaves it owner-only.
    assert_eq!(
        get_file_mode(&dst_file),
        CREATE_MODE,
        "an interrupted copy left the destination at a mode it had not earned"
    );
}

#[test]
fn copy_applies_source_mode_to_file_regardless_of_umask() {
    // 0o646 has bits in every umask-maskable position, so a create mode that survived into the
    // final state would show up as a difference between the two runs
    const SRC_MODE: u32 = 0o646;
    let mut results = Vec::new();
    for umask in [0o077, 0o000] {
        let (src_dir, dst_dir) = setup_test_env();
        let src_file = src_dir.path().join("plain.bin");
        let dst_file = dst_dir.path().join("plain.bin");
        create_test_file(&src_file, "payload", SRC_MODE);
        let status = rcp_with_umask(umask)
            .args([
                "--preserve-settings=all",
                src_file.to_str().unwrap(),
                dst_file.to_str().unwrap(),
            ])
            .status()
            .unwrap();
        assert!(
            status.success(),
            "rcp failed under umask {umask:o}: {status:?}"
        );
        results.push((umask, get_file_mode(&dst_file)));
    }
    for &(umask, mode) in &results {
        assert_eq!(
            mode, SRC_MODE,
            "umask {umask:o} leaked into the destination mode ({mode:o})"
        );
    }
    assert_eq!(
        results[0].1, results[1].1,
        "the destination mode depends on the copier's umask: {:o} vs {:o}",
        results[0].1, results[1].1
    );
}
