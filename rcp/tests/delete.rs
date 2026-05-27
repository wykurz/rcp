//! Integration tests for `rcp --delete` (local mirror).

use std::process::Command;

fn rcp_bin() -> &'static str {
    env!("CARGO_BIN_EXE_rcp")
}

#[test]
fn delete_mirrors_source_removing_extraneous() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("a.txt"), b"old").unwrap();
    std::fs::write(dst.join("stale.txt"), b"stale").unwrap();

    // no trailing slash: `dst` IS the mirror target; --delete implies --overwrite
    let status = Command::new(rcp_bin())
        .arg("--delete")
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    assert!(dst.join("a.txt").exists());
    assert!(!dst.join("stale.txt").exists()); // no source counterpart -> removed
}

#[test]
fn delete_rejects_multiple_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    std::fs::create_dir(&dst).unwrap();

    let output = Command::new(rcp_bin())
        .arg("--delete")
        .arg(&a)
        .arg(&b)
        .arg(format!("{}/", dst.display()))
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "expected failure for --delete with multiple sources"
    );
    // rcp may print the error to stdout (tracing) or stderr — check both.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("--delete requires a single source"),
        "expected single-source error, got: {combined}"
    );
}

#[test]
fn delete_allows_overwrite_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("stale.txt"), b"stale").unwrap();

    // --delete implies --overwrite, so --overwrite-filter must be accepted.
    let status = Command::new(rcp_bin())
        .arg("--delete")
        .arg("--overwrite-filter=newer")
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(dst.join("a.txt").exists());
    assert!(!dst.join("stale.txt").exists());
}

#[test]
fn overwrite_filter_without_overwrite_or_delete_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();

    let output = Command::new(rcp_bin())
        .arg("--overwrite-filter=newer")
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "overwrite-filter without overwrite/delete must be rejected"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("--overwrite-filter requires --overwrite"),
        "expected requirement error, got: {combined}"
    );
}

#[test]
fn delete_rejects_dereference() {
    // `-L` (--dereference) + `--delete` is rejected at parse time: mirroring through a dereferenced
    // symlink is not supported (pruning under a dereferenced subtree would mis-anchor path-based
    // excludes), so we fail fast instead of doing the wrong thing.
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();

    let output = Command::new(rcp_bin())
        .arg("-L")
        .arg("--delete")
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "expected `rcp -L --delete` to be rejected at parse time"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("cannot be used with") && combined.contains("dereference"),
        "expected a conflict error mentioning --dereference, got: {combined}"
    );
}

#[test]
fn delete_restores_perms_on_retained_readonly_dir() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("a.txt"), b"a").unwrap();
    // an extraneous, read-only destination directory holding a filter-protected child
    let extra = dst.join("extra");
    std::fs::create_dir(&extra).unwrap();
    std::fs::write(extra.join("keep.log"), b"x").unwrap();
    std::fs::set_permissions(&extra, std::fs::Permissions::from_mode(0o555)).unwrap();

    let status = Command::new(rcp_bin())
        .arg("--delete")
        .arg("--exclude")
        .arg("*.log")
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    // the excluded child is protected, so the dir survives non-empty ...
    assert!(extra.join("keep.log").exists());
    // ... and its original read-only mode must be restored, not left world-writable from the
    // 0o777 relaxation rm uses to clear a directory's contents.
    let mode = std::fs::metadata(&extra).unwrap().permissions().mode() & 0o7777;
    assert_eq!(
        mode, 0o555,
        "retained read-only dir must keep its original mode, got {mode:o}"
    );

    // restore writable perms so the tempdir can be cleaned up
    std::fs::set_permissions(&extra, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn delete_dry_run_handles_dst_type_replacement() {
    // src/node is a directory but dst/node is a file: a real --delete run replaces the file with a
    // directory before pruning, but --dry-run skips that overwrite. Pruning must then tolerate a
    // non-directory destination counterpart instead of failing the whole preview.
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(src.join("node")).unwrap();
    std::fs::write(src.join("node").join("f.txt"), b"x").unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("node"), b"old").unwrap();

    let output = Command::new(rcp_bin())
        .arg("--dry-run=brief")
        .arg("--delete")
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "dry-run --delete must not fail when a dst counterpart is a non-directory: {combined}"
    );
    // dry-run changes nothing
    assert!(dst.join("node").is_file());
}
