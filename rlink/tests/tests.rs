use std::os::unix::fs::{MetadataExt, PermissionsExt};

#[test]
fn check_rlink_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.arg("--help").assert();
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
    std::fs::write(&src_subdir.join("file1.txt"), "content1").unwrap();
    std::fs::write(&src_subdir.join("file2.txt"), "content2").unwrap();

    let update_subdir = update_dir.path().join("subdir");
    std::fs::create_dir(&update_subdir).unwrap();
    std::fs::write(&update_subdir.join("file1.txt"), "updated content1").unwrap();

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
        let src_file = src_dir.path().join(format!("test_{:o}.txt", mode));
        let dst_file = dst_dir.path().join(format!("test_{:o}.txt", mode));

        create_test_file(&src_file, description, mode);

        let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
        cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
            .assert()
            .success();

        assert!(are_files_hardlinked(&src_file, &dst_file));
        assert_eq!(
            get_file_mode(&dst_file),
            mode,
            "Failed for mode {:o} ({})",
            mode,
            description
        );
        assert_eq!(get_file_content(&dst_file), description);
    }
}
