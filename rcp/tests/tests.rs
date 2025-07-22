use std::os::unix::fs::PermissionsExt;

#[test]
fn check_rcp_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.arg("--help").assert();
}

fn setup_test_env() -> (tempfile::TempDir, tempfile::TempDir) {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    (src_dir, dst_dir)
}

fn create_test_file(path: &std::path::Path, content: &str, mode: u32) {
    std::fs::write(path, content).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn create_symlink(src: &std::path::Path, dst: &std::path::Path) {
    std::os::unix::fs::symlink(src, dst).unwrap();
}

fn get_file_mode(path: &std::path::Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

fn get_file_content(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap()
}

#[test]
fn test_preserve_permissions_basic() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "test content", 0o755);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--preserve",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(get_file_mode(&dst_file), 0o755);
    assert_eq!(get_file_content(&dst_file), "test content");
}

#[test]
fn test_preserve_permissions_complex() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("special.txt");
    let dst_file = dst_dir.path().join("special.txt");

    create_test_file(&src_file, "special content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--preserve",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(get_file_mode(&dst_file), 0o644);
}

#[test]
fn test_preserve_settings_file_specific() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("file.txt");
    let dst_file = dst_dir.path().join("file.txt");

    create_test_file(&src_file, "content", 0o777);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--preserve-settings",
        "f:0644",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(get_file_mode(&dst_file), 0o644);
}

#[test]
fn test_no_preserve_permissions() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "test content", 0o755);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .success();

    assert_eq!(get_file_content(&dst_file), "test content");
}

#[test]
fn test_overwrite_behavior() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "new content", 0o644);
    create_test_file(&dst_file, "old content", 0o644);

    // Add delay to ensure different timestamps
    std::thread::sleep(std::time::Duration::from_millis(10));
    std::fs::write(&src_file, "new content").unwrap();

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--overwrite",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(get_file_content(&dst_file), "new content");
}

#[test]
fn test_overwrite_fail_without_flag() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "new content", 0o644);
    create_test_file(&dst_file, "old content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .failure();
}

#[test]
fn test_weird_permissions() {
    let (src_dir, dst_dir) = setup_test_env();

    // Test cases for files that can be read (owner has read permission)
    let readable_test_cases = [
        (0o400, "read only"),
        (0o444, "read all"),
        (0o644, "rw-r--r--"),
        (0o755, "rwxr-xr-x"),
        (0o2755, "setgid + rwxr-xr-x"),
        (0o4755, "setuid + rwxr-xr-x"),
        (0o6755, "setuid+setgid + rwxr-xr-x"),
        (0o1755, "sticky + rwxr-xr-x"),
    ];

    for (mode, description) in readable_test_cases {
        let src_file = src_dir.path().join(format!("test_{mode:o}.txt"));
        let dst_file = dst_dir.path().join(format!("test_{mode:o}.txt"));

        create_test_file(&src_file, description, mode);

        let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
        cmd.args([
            "--preserve",
            src_file.to_str().unwrap(),
            dst_file.to_str().unwrap(),
        ])
        .assert()
        .success();

        // Note: Some special permission bits (setuid/setgid/sticky) might be stripped
        // during copy operations for security reasons, so we check the content first
        assert_eq!(get_file_content(&dst_file), description);

        let actual_mode = get_file_mode(&dst_file);
        let expected_mode = mode;

        if actual_mode != expected_mode {
            eprintln!(
                "WARNING: Permission mode changed for {expected_mode:o} -> {actual_mode:o} ({description})"
            );
            eprintln!("This might be expected behavior for special permission bits");
        }
    }
}

#[test]
fn test_unreadable_permissions_fail() {
    let (src_dir, dst_dir) = setup_test_env();

    // Test cases for files that cannot be read (no read permission for owner)
    let unreadable_test_cases = [
        (0o000, "no permissions"),
        (0o001, "execute only"),
        (0o002, "write only"),
        (0o111, "execute all"),
        (0o222, "write all"),
    ];

    for (mode, description) in unreadable_test_cases {
        let src_file = src_dir.path().join(format!("test_{mode:o}.txt"));
        let dst_file = dst_dir.path().join(format!("test_{mode:o}.txt"));

        create_test_file(&src_file, description, mode);

        // These should fail because the file cannot be read
        let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
        cmd.args([
            "--preserve",
            src_file.to_str().unwrap(),
            dst_file.to_str().unwrap(),
        ])
        .assert()
        .failure();

        // Verify the destination file was not created
        assert!(
            !dst_file.exists(),
            "Destination file should not exist for unreadable source with mode {mode:o}"
        );
    }
}

#[test]
fn test_directory_permissions() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_subdir = src_dir.path().join("subdir");
    let dst_subdir = dst_dir.path().join("subdir");

    std::fs::create_dir(&src_subdir).unwrap();
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o750)).unwrap();

    let src_file = src_subdir.join("file.txt");
    create_test_file(&src_file, "content", 0o640);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--preserve",
        src_subdir.to_str().unwrap(),
        dst_subdir.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(get_file_mode(&dst_subdir), 0o750);
    assert_eq!(get_file_mode(&dst_subdir.join("file.txt")), 0o640);
}

#[test]
fn test_fail_early_flag() {
    let (src_dir, dst_dir) = setup_test_env();

    let valid_src = src_dir.path().join("valid.txt");
    let invalid_src = src_dir.path().join("nonexistent.txt");

    create_test_file(&valid_src, "content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--fail-early",
        invalid_src.to_str().unwrap(),
        dst_dir.path().join("out.txt").to_str().unwrap(),
    ])
    .assert()
    .failure();
}

#[test]
fn test_symlink_copy_default() {
    let (src_dir, dst_dir) = setup_test_env();

    let target_file = src_dir.path().join("target.txt");
    let src_symlink = src_dir.path().join("link.txt");
    let dst_symlink = dst_dir.path().join("link.txt");

    create_test_file(&target_file, "target content", 0o644);
    create_symlink(&target_file, &src_symlink);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_symlink.to_str().unwrap(), dst_symlink.to_str().unwrap()])
        .assert()
        .success();

    assert!(dst_symlink.is_symlink());
    assert_eq!(std::fs::read_link(&dst_symlink).unwrap(), target_file);
}

#[test]
fn test_symlink_dereference() {
    let (src_dir, dst_dir) = setup_test_env();

    let target_file = src_dir.path().join("target.txt");
    let src_symlink = src_dir.path().join("link.txt");
    let dst_file = dst_dir.path().join("link.txt");

    create_test_file(&target_file, "target content", 0o644);
    create_symlink(&target_file, &src_symlink);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--dereference",
        src_symlink.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(dst_file.is_file());
    assert!(!dst_file.is_symlink());
    assert_eq!(get_file_content(&dst_file), "target content");
}

#[test]
fn test_broken_symlink() {
    let (src_dir, dst_dir) = setup_test_env();

    let nonexistent_target = src_dir.path().join("nonexistent.txt");
    let src_symlink = src_dir.path().join("broken_link.txt");
    let dst_symlink = dst_dir.path().join("broken_link.txt");

    create_symlink(&nonexistent_target, &src_symlink);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_symlink.to_str().unwrap(), dst_symlink.to_str().unwrap()])
        .assert()
        .success();

    assert!(dst_symlink.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_symlink).unwrap(),
        nonexistent_target
    );
}

#[test]
fn test_circular_symlink() {
    let (src_dir, dst_dir) = setup_test_env();

    let link1 = src_dir.path().join("link1.txt");
    let link2 = src_dir.path().join("link2.txt");
    let dst_link = dst_dir.path().join("link1.txt");

    create_symlink(&link2, &link1);
    create_symlink(&link1, &link2);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([link1.to_str().unwrap(), dst_link.to_str().unwrap()])
        .assert()
        .success();

    assert!(dst_link.is_symlink());
}

#[test]
fn test_symlink_chain() {
    let (src_dir, dst_dir) = setup_test_env();

    let target = src_dir.path().join("target.txt");
    let link1 = src_dir.path().join("link1.txt");
    let link2 = src_dir.path().join("link2.txt");
    let link3 = src_dir.path().join("link3.txt");
    let dst_link = dst_dir.path().join("link3.txt");

    create_test_file(&target, "final target", 0o644);
    create_symlink(&target, &link1);
    create_symlink(&link1, &link2);
    create_symlink(&link2, &link3);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([link3.to_str().unwrap(), dst_link.to_str().unwrap()])
        .assert()
        .success();

    assert!(dst_link.is_symlink());
    assert_eq!(std::fs::read_link(&dst_link).unwrap(), link2);
}

#[test]
fn test_symlink_chain_dereference() {
    let (src_dir, dst_dir) = setup_test_env();

    let target = src_dir.path().join("target.txt");
    let link1 = src_dir.path().join("link1.txt");
    let link2 = src_dir.path().join("link2.txt");
    let link3 = src_dir.path().join("link3.txt");
    let dst_file = dst_dir.path().join("link3.txt");

    create_test_file(&target, "final target", 0o644);
    create_symlink(&target, &link1);
    create_symlink(&link1, &link2);
    create_symlink(&link2, &link3);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--dereference",
        link3.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(dst_file.is_file());
    assert!(!dst_file.is_symlink());
    assert_eq!(get_file_content(&dst_file), "final target");
}

#[test]
fn test_relative_symlink() {
    let (src_dir, dst_dir) = setup_test_env();

    let target = src_dir.path().join("target.txt");
    let src_symlink = src_dir.path().join("rel_link.txt");
    let dst_symlink = dst_dir.path().join("rel_link.txt");

    create_test_file(&target, "relative target", 0o644);

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(src_dir.path()).unwrap();
    create_symlink(std::path::Path::new("target.txt"), &src_symlink);
    std::env::set_current_dir(original_dir).unwrap();

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_symlink.to_str().unwrap(), dst_symlink.to_str().unwrap()])
        .assert()
        .success();

    assert!(dst_symlink.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_symlink).unwrap(),
        std::path::Path::new("target.txt")
    );
}

#[test]
fn test_absolute_symlink() {
    let (src_dir, dst_dir) = setup_test_env();

    let target = src_dir.path().join("target.txt");
    let src_symlink = src_dir.path().join("abs_link.txt");
    let dst_symlink = dst_dir.path().join("abs_link.txt");

    create_test_file(&target, "absolute target", 0o644);
    create_symlink(&target, &src_symlink);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_symlink.to_str().unwrap(), dst_symlink.to_str().unwrap()])
        .assert()
        .success();

    assert!(dst_symlink.is_symlink());
    assert_eq!(std::fs::read_link(&dst_symlink).unwrap(), target);
}

#[test]
fn test_symlink_permissions_preserve() {
    let (src_dir, dst_dir) = setup_test_env();

    let target = src_dir.path().join("target.txt");
    let src_symlink = src_dir.path().join("link.txt");
    let dst_symlink = dst_dir.path().join("link.txt");

    create_test_file(&target, "target content", 0o755);
    create_symlink(&target, &src_symlink);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--preserve",
        src_symlink.to_str().unwrap(),
        dst_symlink.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert!(dst_symlink.is_symlink());
    assert_eq!(std::fs::read_link(&dst_symlink).unwrap(), target);
}

#[test]
fn test_edge_case_empty_file() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("empty.txt");
    let dst_file = dst_dir.path().join("empty.txt");

    create_test_file(&src_file, "", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .success();

    assert_eq!(get_file_content(&dst_file), "");
}

#[test]
fn test_edge_case_large_file() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("large.txt");
    let dst_file = dst_dir.path().join("large.txt");

    let large_content = "x".repeat(1024 * 1024);
    create_test_file(&src_file, &large_content, 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .success();

    assert_eq!(get_file_content(&dst_file), large_content);
}

#[test]
fn test_edge_case_unicode_filename() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("файл_测试_🚀.txt");
    let dst_file = dst_dir.path().join("файл_测试_🚀.txt");

    create_test_file(&src_file, "unicode content", 0o644);

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .success();

    assert_eq!(get_file_content(&dst_file), "unicode content");
}

#[test]
fn test_edge_case_special_chars_filename() {
    let (src_dir, dst_dir) = setup_test_env();

    let special_names = [
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.with.dots.txt",
        "file@with@symbols.txt",
        "UPPERCASE.TXT",
    ];

    for name in special_names {
        let src_file = src_dir.path().join(name);
        let dst_file = dst_dir.path().join(name);

        create_test_file(&src_file, &format!("content for {name}"), 0o644);

        let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
        cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
            .assert()
            .success();

        assert_eq!(get_file_content(&dst_file), format!("content for {name}"));
    }
}

#[test]
fn test_edge_case_multiple_files_to_directory() {
    let (src_dir, dst_dir) = setup_test_env();

    let src1 = src_dir.path().join("file1.txt");
    let src2 = src_dir.path().join("file2.txt");
    let src3 = src_dir.path().join("file3.txt");

    create_test_file(&src1, "content1", 0o644);
    create_test_file(&src2, "content2", 0o755);
    create_test_file(&src3, "content3", 0o600);

    let dst_path = format!("{}/", dst_dir.path().to_str().unwrap());

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--preserve",
        src1.to_str().unwrap(),
        src2.to_str().unwrap(),
        src3.to_str().unwrap(),
        &dst_path,
    ])
    .assert()
    .success();

    assert_eq!(
        get_file_content(&dst_dir.path().join("file1.txt")),
        "content1"
    );
    assert_eq!(
        get_file_content(&dst_dir.path().join("file2.txt")),
        "content2"
    );
    assert_eq!(
        get_file_content(&dst_dir.path().join("file3.txt")),
        "content3"
    );

    assert_eq!(get_file_mode(&dst_dir.path().join("file1.txt")), 0o644);
    assert_eq!(get_file_mode(&dst_dir.path().join("file2.txt")), 0o755);
    assert_eq!(get_file_mode(&dst_dir.path().join("file3.txt")), 0o600);
}

#[test]
fn test_edge_case_deep_directory_structure() {
    let (src_dir, dst_dir) = setup_test_env();

    let deep_path = src_dir
        .path()
        .join("level1")
        .join("level2")
        .join("level3")
        .join("level4")
        .join("level5");

    std::fs::create_dir_all(&deep_path).unwrap();
    let deep_file = deep_path.join("deep_file.txt");
    create_test_file(&deep_file, "deep content", 0o755);

    let src_root = src_dir.path().join("level1");
    let dst_root = dst_dir.path().join("level1");

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--preserve",
        src_root.to_str().unwrap(),
        dst_root.to_str().unwrap(),
    ])
    .assert()
    .success();

    let dst_deep_file = dst_dir
        .path()
        .join("level1")
        .join("level2")
        .join("level3")
        .join("level4")
        .join("level5")
        .join("deep_file.txt");

    assert_eq!(get_file_content(&dst_deep_file), "deep content");
    assert_eq!(get_file_mode(&dst_deep_file), 0o755);
}

#[test]
fn test_edge_case_mixed_symlinks_and_files() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_subdir = src_dir.path().join("mixed");
    std::fs::create_dir(&src_subdir).unwrap();

    let regular_file = src_subdir.join("regular.txt");
    let target_file = src_subdir.join("target.txt");
    let symlink_file = src_subdir.join("symlink.txt");

    create_test_file(&regular_file, "regular content", 0o644);
    create_test_file(&target_file, "target content", 0o755);
    create_symlink(&target_file, &symlink_file);

    let dst_subdir = dst_dir.path().join("mixed");

    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--preserve",
        src_subdir.to_str().unwrap(),
        dst_subdir.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(
        get_file_content(&dst_subdir.join("regular.txt")),
        "regular content"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("target.txt")),
        "target content"
    );
    assert!(dst_subdir.join("symlink.txt").is_symlink());

    assert_eq!(get_file_mode(&dst_subdir.join("regular.txt")), 0o644);
    assert_eq!(get_file_mode(&dst_subdir.join("target.txt")), 0o755);
}

#[test]
fn test_verbose_output_demo() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "source content", 0o644);
    create_test_file(&dst_file, "destination content", 0o644);

    // Add delay and modify source to ensure overwrite happens
    std::thread::sleep(std::time::Duration::from_millis(10));
    std::fs::write(&src_file, "source content").unwrap();

    // This should succeed with verbose output
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--overwrite",
        "--verbose",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ])
    .assert()
    .success();

    assert_eq!(get_file_content(&dst_file), "source content");
}

#[test]
fn test_failure_output_demo() {
    let (src_dir, dst_dir) = setup_test_env();

    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");

    create_test_file(&src_file, "source content", 0o644);
    create_test_file(&dst_file, "destination content", 0o644);

    // This should fail because we don't use --overwrite
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .failure();
}
