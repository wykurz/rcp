use assert_cmd::Command;
use std::os::unix::fs::symlink;

fn rrm() -> Command {
    Command::cargo_bin("rrm").unwrap()
}
fn set_mtime_age(path: &std::path::Path, age: std::time::Duration) {
    let past = filetime::FileTime::from_system_time(std::time::SystemTime::now() - age);
    filetime::set_file_mtime(path, past).unwrap();
}

#[test]
fn check_rrm_help() {
    rrm().arg("--help").assert().success();
}

#[test]
fn removes_files_and_directory_tree() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("tree");
    let sub = root.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(root.join("a.txt"), b"aa").unwrap();
    std::fs::write(sub.join("b.txt"), b"bb").unwrap();
    let out = rrm().arg("--summary").arg(&root).assert().success();
    assert!(!root.exists(), "the whole tree must be removed");
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert!(stdout.contains("files removed: 2"), "stdout was: {stdout}");
    assert!(
        stdout.contains("directories removed: 2"),
        "stdout was: {stdout}"
    );
}

#[test]
fn dry_run_removes_nothing() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("tree");
    std::fs::create_dir(&root).unwrap();
    let f = root.join("a.txt");
    std::fs::write(&f, b"x").unwrap();
    rrm()
        .args(["--dry-run", "brief"])
        .arg(&root)
        .assert()
        .success();
    assert!(f.exists(), "--dry-run must not remove anything");
}

#[test]
fn modified_before_spares_recent_removes_old() {
    // entry filter: entries at least <threshold> old are removed, more recent ones are spared. the
    // directory's own mtime is made old too, so the time filter makes it ELIGIBLE for removal — yet
    // it must be retained (exit 0) because the spared child leaves it non-empty. that is the
    // documented ENOTEMPTY edge, distinct from skipping a too-recent directory; "directories
    // skipped: 0" below pins that it was the non-empty path, not a time-filter skip. mtimes are
    // pinned rather than raced against the cutoff via sleep, so the comparison is deterministic.
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("tree");
    std::fs::create_dir(&root).unwrap();
    let old = root.join("old.txt");
    let fresh = root.join("fresh.txt");
    std::fs::write(&old, b"x").unwrap();
    std::fs::write(&fresh, b"x").unwrap();
    set_mtime_age(&old, std::time::Duration::from_secs(7200));
    set_mtime_age(&fresh, std::time::Duration::from_secs(60));
    // set the directory's mtime last so the file writes above don't bump it back to "now"
    set_mtime_age(&root, std::time::Duration::from_secs(7200));
    let out = rrm()
        .args(["--modified-before", "1h", "--summary"])
        .arg(&root)
        .assert()
        .success();
    assert!(!old.exists(), "the old entry must be removed");
    assert!(fresh.exists(), "the recent entry must be spared");
    assert!(
        root.exists(),
        "a non-empty directory retained for a spared child must not be an error"
    );
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert!(stdout.contains("files removed: 1"), "stdout was: {stdout}");
    assert!(stdout.contains("files skipped: 1"), "stdout was: {stdout}");
    assert!(
        stdout.contains("directories removed: 0"),
        "stdout was: {stdout}"
    );
    assert!(
        stdout.contains("directories skipped: 0"),
        "the eligible-but-non-empty dir must be retained via ENOTEMPTY, not a time-filter skip; \
         stdout was: {stdout}"
    );
}

#[test]
fn exclude_filter_spares_matching_entries() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("tree");
    std::fs::create_dir(&root).unwrap();
    let keep = root.join("keep.keep");
    let gone = root.join("gone.txt");
    std::fs::write(&keep, b"x").unwrap();
    std::fs::write(&gone, b"x").unwrap();
    rrm()
        .args(["--exclude", "*.keep"])
        .arg(&root)
        .assert()
        .success();
    assert!(keep.exists(), "*.keep must be excluded from removal");
    assert!(!gone.exists(), "a non-excluded file must be removed");
    assert!(
        root.exists(),
        "the directory is retained because it still holds the excluded file"
    );
}

#[test]
fn include_filter_removes_only_matches() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("tree");
    std::fs::create_dir(&root).unwrap();
    let target = root.join("trash.tmp");
    let kept = root.join("keep.txt");
    std::fs::write(&target, b"x").unwrap();
    std::fs::write(&kept, b"x").unwrap();
    rrm()
        .args(["--include", "*.tmp"])
        .arg(&root)
        .assert()
        .success();
    assert!(!target.exists(), "an included *.tmp file must be removed");
    assert!(kept.exists(), "a non-matching file must be spared");
    assert!(
        root.exists(),
        "the directory is retained because it still holds the spared file"
    );
}

#[test]
fn removes_symlink_without_following_target() {
    let d = tempfile::tempdir().unwrap();
    let target = d.path().join("target.txt");
    std::fs::write(&target, b"important").unwrap();
    let link = d.path().join("link");
    symlink(&target, &link).unwrap();
    rrm().arg(&link).assert().success();
    assert!(
        std::fs::symlink_metadata(&link).is_err(),
        "the symlink itself must be removed"
    );
    assert!(target.exists(), "the symlink target must be left intact");
}

#[test]
fn fails_on_nonexistent_path() {
    let d = tempfile::tempdir().unwrap();
    rrm()
        .arg(d.path().join("does-not-exist"))
        .assert()
        .failure();
}

#[cfg(target_env = "musl")]
#[test]
fn created_before_rejected_on_musl() {
    // static musl builds cannot read btime, so --created-before must fail fast rather than
    // silently skip every entry (guarded by common::filter::reject_created_before_on_musl).
    let d = tempfile::tempdir().unwrap();
    let f = d.path().join("f.txt");
    std::fs::write(&f, b"x").unwrap();
    rrm()
        .args(["--created-before", "1d"])
        .arg(d.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains("not supported on musl"));
    assert!(
        f.exists(),
        "nothing should be removed when the run is rejected up front"
    );
}
