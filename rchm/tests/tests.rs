use assert_cmd::Command;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

fn rchm() -> Command {
    Command::cargo_bin("rchm").unwrap()
}
fn mode_of(p: &std::path::Path) -> u32 {
    std::fs::symlink_metadata(p).unwrap().permissions().mode() & 0o7777
}

#[test]
fn applies_per_type_modes_recursively() {
    let d = tempfile::tempdir().unwrap();
    let sub = d.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    let f = sub.join("f.txt");
    std::fs::write(&f, b"x").unwrap();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    rchm()
        .args(["--mode", "f:g+w d:g+rwxs"])
        .arg(d.path())
        .assert()
        .success();
    assert_eq!(mode_of(&f), 0o664);
    assert_eq!(mode_of(&sub), 0o2775);
}

#[test]
fn preserves_mtime_and_moves_ctime() {
    let d = tempfile::tempdir().unwrap();
    let f = d.path().join("f.txt");
    std::fs::write(&f, b"x").unwrap();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
    let before = std::fs::symlink_metadata(&f).unwrap();
    let (mtime_ns_before, ctime_ns_before) = (before.mtime_nsec(), before.ctime_nsec());
    let (mtime_s_before, ctime_s_before) = (before.mtime(), before.ctime());
    std::thread::sleep(std::time::Duration::from_millis(1100));
    // apply a mode change (g+w); chmod/chgrp/chown move only ctime, never mtime
    rchm()
        .args(["--mode", "g+w"])
        .arg(d.path())
        .assert()
        .success();
    let after = std::fs::symlink_metadata(&f).unwrap();
    assert_eq!(
        (after.mtime(), after.mtime_nsec()),
        (mtime_s_before, mtime_ns_before),
        "mtime must be preserved"
    );
    assert_ne!(
        (after.ctime(), after.ctime_nsec()),
        (ctime_s_before, ctime_ns_before),
        "ctime is expected to move"
    );
}

#[test]
fn include_filter_changes_matches_not_traversed_dirs() {
    // with --include '*.txt', only matching files are modified; directories entered
    // only to find matches (and non-matching files) are left untouched.
    let d = tempfile::tempdir().unwrap();
    let sub = d.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    let txt = sub.join("a.txt");
    let other = sub.join("b.dat");
    std::fs::write(&txt, b"x").unwrap();
    std::fs::write(&other, b"x").unwrap();
    std::fs::set_permissions(&txt, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(&other, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    rchm()
        .args(["--mode", "g+w", "--include", "*.txt"])
        .arg(d.path())
        .assert()
        .success();
    assert_eq!(mode_of(&txt), 0o664, "matching file must change");
    assert_eq!(
        mode_of(&other),
        0o644,
        "non-matching file must be untouched"
    );
    assert_eq!(mode_of(&sub), 0o755, "traversed-only dir must be untouched");
}

#[test]
fn skips_already_correct_entries() {
    let d = tempfile::tempdir().unwrap();
    let f = d.path().join("f.txt");
    std::fs::write(&f, b"x").unwrap();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o664)).unwrap();
    let out = rchm()
        .args(["--mode", "g+w", "--summary"])
        .arg(d.path())
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert!(
        stdout.contains("files unchanged: 1"),
        "stdout was: {stdout}"
    );
    assert!(stdout.contains("files changed: 0"), "stdout was: {stdout}");
}

#[test]
fn mode_on_symlink_operand_does_not_follow_to_target() {
    let d = tempfile::tempdir().unwrap();
    let target = d.path().join("target.txt");
    std::fs::write(&target, b"x").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
    let link = d.path().join("link");
    symlink(&target, &link).unwrap();
    let target_mode_before = mode_of(&target);
    // mode applies only to files+dirs; symlink mode untouched, target untouched by traversal
    rchm().args(["--mode", "g+w"]).arg(&link).assert().success();
    assert_eq!(
        mode_of(&target),
        target_mode_before,
        "symlink target must be untouched"
    );
}

#[test]
fn preserves_setgid_through_mode_change() {
    // a mode change must not disturb the setgid bit. (the chown-clears-setgid
    // restore path needs a real chown and is covered by the compute_plan unit
    // test `plan_preserves_setgid_across_chgrp`.)
    let d = tempfile::tempdir().unwrap();
    let f = d.path().join("f.txt");
    std::fs::write(&f, b"x").unwrap();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o2775)).unwrap();
    rchm()
        .args(["--mode", "g-w"])
        .arg(d.path())
        .assert()
        .success();
    assert_eq!(mode_of(&f), 0o2755, "group write removed, setgid kept");
}

#[test]
fn preorder_dir_lockout_applies_change_then_reports() {
    // default pre-order: removing the dir's own traversal permission applies the change,
    // then can't descend -> reports an error (exit nonzero) but keeps going. Processing
    // contents first instead is what --defer-dir-changes is for (tested separately).
    let d = tempfile::tempdir().unwrap();
    std::fs::write(d.path().join("f.txt"), b"x").unwrap();
    rchm()
        .args(["--mode", "d:a-rwx"])
        .arg(d.path())
        .assert()
        .failure();
    assert_eq!(
        mode_of(d.path()),
        0o000,
        "the directory's own change is applied (pre-order) even though descent fails"
    );
    // restore so tempdir cleanup works
    std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn dry_run_makes_no_changes() {
    let d = tempfile::tempdir().unwrap();
    let f = d.path().join("f.txt");
    std::fs::write(&f, b"x").unwrap();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
    rchm()
        .args(["--mode", "g+w", "--dry-run", "brief"])
        .arg(d.path())
        .assert()
        .success();
    assert_eq!(mode_of(&f), 0o644, "dry-run must not change anything");
}

#[test]
fn exclude_filter_narrows_the_set() {
    let d = tempfile::tempdir().unwrap();
    let keep = d.path().join("keep.txt");
    let skip = d.path().join("skip.log");
    std::fs::write(&keep, b"x").unwrap();
    std::fs::write(&skip, b"x").unwrap();
    std::fs::set_permissions(&keep, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(&skip, std::fs::Permissions::from_mode(0o644)).unwrap();
    rchm()
        .args(["--mode", "g+w", "--exclude", "*.log"])
        .arg(d.path())
        .assert()
        .success();
    assert_eq!(mode_of(&keep), 0o664);
    assert_eq!(mode_of(&skip), 0o644, "*.log must be excluded");
}

#[test]
fn symlink_root_with_trailing_slash_is_not_dereferenced() {
    // a trailing slash on a symlink root must not dereference it into its target dir.
    let d = tempfile::tempdir().unwrap();
    let target = d.path().join("target");
    std::fs::create_dir(&target).unwrap();
    let inside = target.join("f.txt");
    std::fs::write(&inside, b"x").unwrap();
    std::fs::set_permissions(&inside, std::fs::Permissions::from_mode(0o644)).unwrap();
    let link = d.path().join("link");
    symlink(&target, &link).unwrap();
    let mut operand = link.into_os_string();
    operand.push("/");
    rchm()
        .args(["--mode", "g+w"])
        .arg(&operand)
        .assert()
        .success();
    assert_eq!(
        mode_of(&inside),
        0o644,
        "must not descend into the symlink target"
    );
}

#[test]
fn recovers_unreadable_directory() {
    // pre-order default: d:u+rwx makes a 000 dir traversable, then descends and fixes contents.
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join("dir000");
    std::fs::create_dir(&dir).unwrap();
    let inner = dir.join("f");
    std::fs::write(&inner, b"x").unwrap();
    std::fs::set_permissions(&inner, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    rchm()
        .args(["--mode", "d:u+rwx f:g+w"])
        .arg(&dir)
        .assert()
        .success();
    assert_eq!(mode_of(&dir), 0o700, "unreadable dir recovered");
    assert_eq!(mode_of(&inner), 0o664, "contents reached after recovery");
}

#[test]
fn child_failure_does_not_block_parent_change() {
    // default keep-going: an unreadable child fails, but the parent's own change still applies.
    let d = tempfile::tempdir().unwrap();
    let parent = d.path().join("parent");
    let child = parent.join("child");
    std::fs::create_dir_all(&child).unwrap();
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o000)).unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
    // the run reports the child failure (exit nonzero) but still changes the parent
    rchm()
        .args(["--mode", "d:g+w"])
        .arg(&parent)
        .assert()
        .failure();
    assert_eq!(
        mode_of(&parent),
        0o720,
        "parent changed despite child failure"
    );
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn defer_dir_changes_allows_removing_owner_traversal() {
    // --defer-dir-changes (post-order): process contents first, then strip the dir's perms.
    let d = tempfile::tempdir().unwrap();
    let t = d.path().join("t");
    let sub = t.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    rchm()
        .args(["--mode", "d:a-rwx", "--defer-dir-changes"])
        .arg(&t)
        .assert()
        .success();
    assert_eq!(
        mode_of(&t),
        0o000,
        "dir stripped after its contents were processed"
    );
    // restore so tempdir cleanup works — outermost first to regain traversal
    std::fs::set_permissions(&t, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn defer_dir_changes_with_fail_early_does_not_change_ancestor_after_child_error() {
    // --defer-dir-changes + --fail-early: a child failure must stop the run before the
    // deferred parent change is applied (no changes after the error we were told to stop on).
    let d = tempfile::tempdir().unwrap();
    let parent = d.path().join("parent");
    let child = parent.join("child");
    std::fs::create_dir_all(&child).unwrap();
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o000)).unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
    rchm()
        .args(["--mode", "d:g+w", "--defer-dir-changes", "--fail-early"])
        .arg(&parent)
        .assert()
        .failure();
    assert_eq!(
        mode_of(&parent),
        0o700,
        "fail-early must stop before the deferred parent change"
    );
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o755)).unwrap();
}
