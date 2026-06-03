use std::os::unix::fs::{MetadataExt, PermissionsExt};

#[test]
fn check_rlink_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.arg("--help").assert().success();
}

fn setup_test_env() -> (tempfile::TempDir, tempfile::TempDir, tempfile::TempDir) {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    let update_dir = tempfile::tempdir().unwrap();
    (src_dir, dst_dir, update_dir)
}

fn create_test_file(path: &std::path::Path, content: &str, mode: u32) {
    std::fs::write(path, content).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// copy src file times (atime/mtime) to dst
fn copy_file_times(src: &std::path::Path, dst: &std::path::Path) {
    let src_meta = std::fs::metadata(src).unwrap();
    let times = std::fs::FileTimes::new()
        .set_accessed(src_meta.accessed().unwrap())
        .set_modified(src_meta.modified().unwrap());
    let dst_file = std::fs::File::options().write(true).open(dst).unwrap();
    dst_file.set_times(times).unwrap();
}

fn get_file_mode(path: &std::path::Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

fn get_file_content(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap()
}

fn are_files_hardlinked(path1: &std::path::Path, path2: &std::path::Path) -> bool {
    let meta1 = std::fs::metadata(path1).unwrap();
    let meta2 = std::fs::metadata(path2).unwrap();
    meta1.ino() == meta2.ino() && meta1.dev() == meta2.dev()
}

#[test]
fn test_basic_hardlink() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "test content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .success();

    assert!(dst_file.exists());
    assert!(are_files_hardlinked(&src_file, &dst_file));
    assert_eq!(get_file_content(&dst_file), "test content");
}

#[test]
fn test_hardlink_directory() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let src_subdir = src_dir.path().join("subdir");
    std::fs::create_dir(&src_subdir).unwrap();

    let src_file1 = src_subdir.join("file1.txt");
    let src_file2 = src_subdir.join("file2.txt");

    create_test_file(&src_file1, "content1", 0o644);
    create_test_file(&src_file2, "content2", 0o755);

    let dst_subdir = dst_dir.path().join("subdir");

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([src_subdir.to_str().unwrap(), dst_subdir.to_str().unwrap()])
        .assert()
        .success();

    let dst_file1 = dst_subdir.join("file1.txt");
    let dst_file2 = dst_subdir.join("file2.txt");

    assert!(are_files_hardlinked(&src_file1, &dst_file1));
    assert!(are_files_hardlinked(&src_file2, &dst_file2));
    assert_eq!(get_file_content(&dst_file1), "content1");
    assert_eq!(get_file_content(&dst_file2), "content2");
}

#[test]
fn test_update_copy_modified_file() {
    let (src_dir, dst_dir, update_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let update_file = update_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "original content", 0o644);
    create_test_file(&update_file, "updated content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--update-compare", // don't compare time to avoid false positives
        "size",
        "--update",
        update_file.to_str().unwrap(),
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(get_file_content(&dst_file), "updated content");
    assert!(!are_files_hardlinked(&src_file, &dst_file));
}

#[test]
fn test_update_hardlink_unchanged_file() {
    let (src_dir, dst_dir, update_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let update_file = update_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "same content", 0o644);
    create_test_file(&update_file, "same content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--update-compare", // don't compare time to avoid false positives
        "size",
        "--update",
        update_file.to_str().unwrap(),
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(get_file_content(&dst_file), "same content");
    assert!(are_files_hardlinked(&src_file, &dst_file));
}

#[test]
fn test_update_exclusive_mode() {
    let (src_dir, dst_dir, update_dir) = setup_test_env();

    let src_file1 = src_dir.path().join("file1.txt");
    let src_file2 = src_dir.path().join("file2.txt");
    let update_file1 = update_dir.path().join("file1.txt");

    create_test_file(&src_file1, "content1", 0o644);
    create_test_file(&src_file2, "content2", 0o644);
    create_test_file(&update_file1, "updated content1", 0o644);

    let src_subdir = src_dir.path().join("subdir");
    std::fs::create_dir(&src_subdir).unwrap();
    std::fs::write(src_subdir.join("file1.txt"), "content1").unwrap();
    std::fs::write(src_subdir.join("file2.txt"), "content2").unwrap();

    let update_subdir = update_dir.path().join("subdir");
    std::fs::create_dir(&update_subdir).unwrap();
    std::fs::write(update_subdir.join("file1.txt"), "updated content1").unwrap();

    let dst_subdir = dst_dir.path().join("subdir");

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--update",
        update_dir.path().to_str().unwrap(),
        "--update-exclusive",
        src_subdir.to_str().unwrap(),
        dst_subdir.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(dst_subdir.join("file1.txt").exists());
    assert!(!dst_subdir.join("file2.txt").exists());
    assert_eq!(
        get_file_content(&dst_subdir.join("file1.txt")),
        "updated content1"
    );
}

#[test]
fn test_overwrite_behavior() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "new content", 0o644);
    create_test_file(&dst_file, "old content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--overwrite",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(are_files_hardlinked(&src_file, &dst_file));
    assert_eq!(get_file_content(&dst_file), "new content");
}

#[test]
fn test_overwrite_fail_without_flag() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "new content", 0o644);
    create_test_file(&dst_file, "old content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .failure();
}

#[test]
fn test_trailing_slash_behavior() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    create_test_file(&src_file, "content", 0o644);

    let dst_path = format!("{}/", dst_dir.path().to_str().unwrap());

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([src_file.to_str().unwrap(), &dst_path])
        .assert()
        .success();

    let dst_file = dst_dir.path().join("test.txt");
    assert!(are_files_hardlinked(&src_file, &dst_file));
}

#[test]
fn test_update_compare_settings() {
    let (src_dir, dst_dir, update_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let update_file = update_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "same content", 0o644);
    create_test_file(&update_file, "same content", 0o755);

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--update",
        update_file.to_str().unwrap(),
        "--update-compare",
        "size",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(are_files_hardlinked(&src_file, &dst_file));
}

#[test]
fn test_overwrite_compare_settings() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "same content", 0o644);
    create_test_file(&dst_file, "same content", 0o755);

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--overwrite",
        "--overwrite-compare",
        "size",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(are_files_hardlinked(&src_file, &dst_file));
}

#[test]
fn test_fail_early_flag() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let invalid_src = src_dir.path().join("nonexistent.txt");

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--fail-early",
        invalid_src.to_str().unwrap(),
        dst_dir.path().join("out.txt").to_str().unwrap(),
    ])
    .assert()
    .failure();
}

#[test]
fn test_complex_update_scenario() {
    let (src_dir, dst_dir, update_dir) = setup_test_env();

    let src_subdir = src_dir.path().join("project");
    let update_subdir = update_dir.path().join("project");
    std::fs::create_dir_all(&src_subdir).unwrap();
    std::fs::create_dir_all(&update_subdir).unwrap();

    create_test_file(
        &src_subdir.join("unchanged.txt"),
        "unchanged content",
        0o644,
    );
    create_test_file(&src_subdir.join("modified.txt"), "old content", 0o644);
    create_test_file(&src_subdir.join("deleted.txt"), "deleted content", 0o644);

    create_test_file(
        &update_subdir.join("unchanged.txt"),
        "unchanged content",
        0o644,
    );

    create_test_file(
        &update_subdir.join("modified.txt"),
        "new content of different length",
        0o644,
    );
    create_test_file(&update_subdir.join("added.txt"), "added content", 0o644);

    let dst_subdir = dst_dir.path().join("project");

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--update-compare", // don't compare time to avoid false positives
        "size",
        "--update",
        update_subdir.to_str().unwrap(),
        src_subdir.to_str().unwrap(),
        dst_subdir.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(are_files_hardlinked(
        &src_subdir.join("unchanged.txt"),
        &dst_subdir.join("unchanged.txt")
    ));
    assert!(!are_files_hardlinked(
        &src_subdir.join("modified.txt"),
        &dst_subdir.join("modified.txt")
    ));
    assert_eq!(
        get_file_content(&dst_subdir.join("modified.txt")),
        "new content of different length"
    );
    assert!(are_files_hardlinked(
        &src_subdir.join("deleted.txt"),
        &dst_subdir.join("deleted.txt")
    ));
    assert_eq!(
        get_file_content(&dst_subdir.join("added.txt")),
        "added content"
    );
}

#[test]
fn test_edge_case_empty_directories() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let src_subdir = src_dir.path().join("empty");
    std::fs::create_dir(&src_subdir).unwrap();

    let dst_subdir = dst_dir.path().join("empty");

    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([src_subdir.to_str().unwrap(), dst_subdir.to_str().unwrap()])
        .assert()
        .success();

    assert!(dst_subdir.is_dir());
}

#[test]
fn test_edge_case_special_permissions() {
    let (src_dir, dst_dir, _) = setup_test_env();

    let test_cases = [
        (0o4755, "setuid + rwxr-xr-x"),
        (0o2755, "setgid + rwxr-xr-x"),
        (0o1755, "sticky + rwxr-xr-x"),
    ];

    for (mode, description) in test_cases {
        let src_file = src_dir.path().join(format!("test_{mode:o}.txt"));
        let dst_file = dst_dir.path().join(format!("test_{mode:o}.txt"));

        create_test_file(&src_file, description, mode);

        let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
        cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
            .assert()
            .success();

        assert!(are_files_hardlinked(&src_file, &dst_file));
        assert_eq!(
            get_file_mode(&dst_file),
            mode,
            "Failed for mode {mode:o} ({description})"
        );
        assert_eq!(get_file_content(&dst_file), description);
    }
}

// ============================================================================
// Preserve Settings Tests
// ============================================================================

#[test]
fn test_default_preserves_metadata() {
    // verify backward compat: rlink without --preserve-settings preserves directory metadata
    let (src_dir, dst_dir, _) = setup_test_env();
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    // use a distinctive mode so the assertion is meaningful
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o750)).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    let src_meta = std::fs::metadata(&src_subdir).unwrap();
    let dst_subdir = dst_dir.path().join("mydir");
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([src_subdir.to_str().unwrap(), dst_subdir.to_str().unwrap()])
        .assert()
        .success();
    assert!(dst_subdir.is_dir());
    assert!(are_files_hardlinked(
        &src_subdir.join("file.txt"),
        &dst_subdir.join("file.txt")
    ));
    // verify directory metadata was actually preserved
    let dst_meta = std::fs::metadata(&dst_subdir).unwrap();
    assert_eq!(
        get_file_mode(&dst_subdir),
        get_file_mode(&src_subdir),
        "directory mode not preserved"
    );
    assert_eq!(
        dst_meta.mtime(),
        src_meta.mtime(),
        "directory mtime not preserved"
    );
}

#[test]
fn test_preserve_settings_none_basic_link() {
    let (src_dir, dst_dir, _) = setup_test_env();
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    let dst_subdir = dst_dir.path().join("mydir");
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--preserve-settings",
        "none",
        src_subdir.to_str().unwrap(),
        dst_subdir.to_str().unwrap(),
    ])
    .assert()
    .success();
    assert!(dst_subdir.is_dir());
    assert!(are_files_hardlinked(
        &src_subdir.join("file.txt"),
        &dst_subdir.join("file.txt")
    ));
}

#[test]
fn test_update_preserve_none_errors_without_allow_lossy() {
    let (src_dir, dst_dir, update_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let update_file = update_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "content", 0o644);
    create_test_file(&update_file, "content", 0o644);
    // --update with --preserve-settings=none should error because default update_compare
    // includes mtime which is not preserved by "none"
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--update",
        update_file.to_str().unwrap(),
        "--preserve-settings",
        "none",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .failure()
    .stdout(predicates::str::contains("--update compares"));
}

#[test]
fn test_update_preserve_none_succeeds_with_allow_lossy() {
    let (src_dir, dst_dir, update_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let update_file = update_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "content", 0o644);
    create_test_file(&update_file, "content", 0o644);
    // default --update-compare is size,mtime — ensure mtimes match so rlink
    // considers the files unchanged and hard-links instead of copying
    copy_file_times(&src_file, &update_file);
    // use default --update-compare (size,mtime) so the mismatch with
    // --preserve-settings=none is real and only --allow-lossy-update saves it
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--update",
        update_file.to_str().unwrap(),
        "--preserve-settings",
        "none",
        "--allow-lossy-update",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();
    assert!(are_files_hardlinked(&src_file, &dst_file));
}

#[test]
fn test_default_preserves_special_bits_on_directories() {
    let test_cases: &[(u32, &str)] = &[
        (0o2755, "setgid"),
        (0o4755, "setuid"),
        (0o1755, "sticky"),
        (0o7755, "setuid+setgid+sticky"),
    ];
    for &(mode, description) in test_cases {
        let (src_dir, dst_dir, _) = setup_test_env();
        let src_subdir = src_dir.path().join("dir");
        let dst_subdir = dst_dir.path().join("dir");
        std::fs::create_dir(&src_subdir).unwrap();
        std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(mode)).unwrap();
        create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
        let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
        cmd.args([src_subdir.to_str().unwrap(), dst_subdir.to_str().unwrap()])
            .assert()
            .success();
        assert_eq!(
            get_file_mode(&dst_subdir),
            mode,
            "directory special bits not preserved for {description} ({mode:o})"
        );
        assert!(are_files_hardlinked(
            &src_subdir.join("file.txt"),
            &dst_subdir.join("file.txt")
        ));
    }
}

#[test]
fn test_preserve_settings_none_strips_special_bits_on_directories() {
    let (src_dir, dst_dir, _) = setup_test_env();
    let src_subdir = src_dir.path().join("dir");
    let dst_subdir = dst_dir.path().join("dir");
    std::fs::create_dir(&src_subdir).unwrap();
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o2755)).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.args([
        "--preserve-settings",
        "none",
        src_subdir.to_str().unwrap(),
        dst_subdir.to_str().unwrap(),
    ])
    .assert()
    .success();
    assert_eq!(
        get_file_mode(&dst_subdir),
        0o755,
        "directory special bits should be stripped with --preserve-settings=none"
    );
    assert!(are_files_hardlinked(
        &src_subdir.join("file.txt"),
        &dst_subdir.join("file.txt")
    ));
}

#[test]
fn test_preserve_settings_dir_7777_preserves_special_bits() {
    let test_cases: &[(u32, &str)] = &[(0o2755, "setgid"), (0o1755, "sticky")];
    for &(mode, description) in test_cases {
        let (src_dir, dst_dir, _) = setup_test_env();
        let src_subdir = src_dir.path().join("dir");
        let dst_subdir = dst_dir.path().join("dir");
        std::fs::create_dir(&src_subdir).unwrap();
        std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(mode)).unwrap();
        create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
        let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
        cmd.args([
            "--preserve-settings",
            "d:7777",
            src_subdir.to_str().unwrap(),
            dst_subdir.to_str().unwrap(),
        ])
        .assert()
        .success();
        assert_eq!(
            get_file_mode(&dst_subdir),
            mode,
            "directory special bits not preserved for {description} ({mode:o})"
        );
        assert!(are_files_hardlinked(
            &src_subdir.join("file.txt"),
            &dst_subdir.join("file.txt")
        ));
    }
}

/// Build `<root>/proj/sub/file.txt` plus an empty sibling `<root>/dest`, returning
/// `(root, sub, dest)`. Tests run the tool with cwd set to `sub` via `Command::current_dir`
/// (child-process cwd only — no global state mutation), so a relative `.` resolves to `sub` and
/// `..` to `proj`. `dest` is a sibling of `proj`, so copying `proj` into `dest` never self-copies.
fn setup_dot_operand_tree() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let sub = root.path().join("proj").join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    create_test_file(&sub.join("file.txt"), "hello dot", 0o644);
    let dest = root.path().join("dest");
    std::fs::create_dir(&dest).unwrap();
    (root, sub, dest)
}

#[test]
fn links_dot_and_dotdot_source_operands_into_trailing_slash_dest() {
    // The trailing-slash "copy INTO dir" rule must work for `.`/`..` source operands, deriving the
    // basename from the SAME canonicalization the link operation uses, so `dest/<name>` matches the
    // entry that gets created. cwd is `<root>/proj/sub`, so `.` == sub and `..` == proj.
    let (_root, sub, dest) = setup_dot_operand_tree();
    // (source operand run from cwd `sub`, expected file path created under `dest/`)
    let cases: &[(&str, &str)] = &[
        (".", "sub/file.txt"),
        ("./", "sub/file.txt"),
        ("..", "proj/sub/file.txt"),
        ("../", "proj/sub/file.txt"),
        ("../sub/..", "proj/sub/file.txt"), // embedded `..` canonicalizes to `proj`
        ("../sub/.", "sub/file.txt"),       // trailing `/.` names the dir itself -> `sub`
    ];
    for (src, expected) in cases {
        // fresh dest per case (rlink refuses to clobber an existing destination entry)
        std::fs::remove_dir_all(&dest).unwrap();
        std::fs::create_dir(&dest).unwrap();
        let dst_arg = format!("{}/", dest.to_str().unwrap());
        let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
        cmd.current_dir(&sub)
            .args([*src, dst_arg.as_str()])
            .assert()
            .success();
        let created = dest.join(expected);
        assert!(
            are_files_hardlinked(&sub.join("file.txt"), &created),
            "operand {src:?} should hard-link the source into {created:?}"
        );
    }
}

#[test]
fn dot_source_without_trailing_slash_uses_dest_name_verbatim() {
    // Deterministic contrast: WITHOUT a trailing slash the destination is the final name, so the
    // result is knowable from the slash alone, independent of the source spelling. `rlink . X/named`
    // links the current directory AS `named`.
    let (_root, sub, dest) = setup_dot_operand_tree();
    let dst_arg = dest.join("named");
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.current_dir(&sub)
        .args([".", dst_arg.to_str().unwrap()])
        .assert()
        .success();
    assert!(are_files_hardlinked(
        &sub.join("file.txt"),
        &dest.join("named").join("file.txt")
    ));
}
