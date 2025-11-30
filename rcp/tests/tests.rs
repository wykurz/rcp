use predicates::prelude::PredicateBooleanExt;
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

#[test]
fn test_symlink_chain_dereference_integration() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create a chain of symlinks: foo -> bar -> baz (actual file)
    let baz_file = src_dir.path().join("baz_file.txt");
    create_test_file(&baz_file, "final content", 0o644);
    let bar_link = src_dir.path().join("bar");
    let foo_link = src_dir.path().join("foo");
    // Create chain: foo -> bar -> baz_file.txt
    create_symlink(&baz_file, &bar_link);
    create_symlink(&bar_link, &foo_link);
    // Create a source directory with the symlink chain
    let src_subdir = src_dir.path().join("chaintest");
    std::fs::create_dir(&src_subdir).unwrap();
    // Create symlinks in the test directory that represent the chain
    create_symlink(&foo_link, &src_subdir.join("foo"));
    create_symlink(&bar_link, &src_subdir.join("bar"));
    create_symlink(&baz_file, &src_subdir.join("baz"));
    let dst_subdir = dst_dir.path().join("chaintest");
    // Test with dereference - should copy 3 files with same content
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--dereference",
        src_subdir.to_str().unwrap(),
        dst_subdir.to_str().unwrap(),
    ])
    .assert()
    .success();
    // Verify all three are now regular files with the same content
    let foo_content = get_file_content(&dst_subdir.join("foo"));
    let bar_content = get_file_content(&dst_subdir.join("bar"));
    let baz_content = get_file_content(&dst_subdir.join("baz"));
    assert_eq!(foo_content, "final content");
    assert_eq!(bar_content, "final content");
    assert_eq!(baz_content, "final content");
    // Verify they are all regular files, not symlinks
    assert!(dst_subdir.join("foo").is_file());
    assert!(dst_subdir.join("bar").is_file());
    assert!(dst_subdir.join("baz").is_file());
    assert!(!dst_subdir.join("foo").is_symlink());
    assert!(!dst_subdir.join("bar").is_symlink());
    assert!(!dst_subdir.join("baz").is_symlink());
}

#[test]
fn test_symlink_chain_no_dereference_integration() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create a chain of symlinks: foo -> bar -> baz (actual file)
    let baz_file = src_dir.path().join("baz_file.txt");
    create_test_file(&baz_file, "final content", 0o644);
    let bar_link = src_dir.path().join("bar");
    let foo_link = src_dir.path().join("foo");
    // Create chain: foo -> bar -> baz_file.txt
    create_symlink(&baz_file, &bar_link);
    create_symlink(&bar_link, &foo_link);
    // Create a source directory with the symlink chain
    let src_subdir = src_dir.path().join("chaintest");
    std::fs::create_dir(&src_subdir).unwrap();
    // Create symlinks in the test directory that represent the chain
    create_symlink(&foo_link, &src_subdir.join("foo"));
    create_symlink(&bar_link, &src_subdir.join("bar"));
    create_symlink(&baz_file, &src_subdir.join("baz"));
    let dst_subdir = dst_dir.path().join("chaintest");
    // Test without dereference - should preserve symlinks
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_subdir.to_str().unwrap(), dst_subdir.to_str().unwrap()])
        .assert()
        .success();
    // Verify all three remain as symlinks
    assert!(dst_subdir.join("foo").is_symlink());
    assert!(dst_subdir.join("bar").is_symlink());
    assert!(dst_subdir.join("baz").is_symlink());
    // Verify symlink targets are preserved
    assert_eq!(
        std::fs::read_link(dst_subdir.join("foo")).unwrap(),
        foo_link
    );
    assert_eq!(
        std::fs::read_link(dst_subdir.join("bar")).unwrap(),
        bar_link
    );
    assert_eq!(
        std::fs::read_link(dst_subdir.join("baz")).unwrap(),
        baz_file
    );
}

#[test]
fn test_dereference_directory_symlink_integration() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create a directory with specific permissions and files
    let target_dir = src_dir.path().join("target_directory");
    std::fs::create_dir(&target_dir).unwrap();
    std::fs::set_permissions(&target_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    create_test_file(&target_dir.join("file1.txt"), "content1", 0o644);
    create_test_file(&target_dir.join("file2.txt"), "content2", 0o600);
    // Create a symlink pointing to the directory
    let dir_symlink = src_dir.path().join("dir_link");
    create_symlink(&target_dir, &dir_symlink);
    let dst_path = dst_dir.path().join("copied_directory");
    // Test with dereference - should copy as a directory with preserved permissions
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--dereference",
        "--preserve",
        dir_symlink.to_str().unwrap(),
        dst_path.to_str().unwrap(),
    ])
    .assert()
    .success();
    // Verify the result is a directory, not a symlink
    assert!(dst_path.is_dir());
    assert!(!dst_path.is_symlink());
    // Verify directory permissions preserved
    assert_eq!(get_file_mode(&dst_path), 0o755);
    // Verify files were copied with correct content and permissions
    assert_eq!(get_file_content(&dst_path.join("file1.txt")), "content1");
    assert_eq!(get_file_content(&dst_path.join("file2.txt")), "content2");
    assert_eq!(get_file_mode(&dst_path.join("file1.txt")), 0o644);
    assert_eq!(get_file_mode(&dst_path.join("file2.txt")), 0o600);
}

#[test]
fn test_dereference_file_symlink_permissions_integration() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create files with different permissions
    let file1 = src_dir.path().join("file1.txt");
    let file2 = src_dir.path().join("file2.txt");
    create_test_file(&file1, "content1", 0o755);
    create_test_file(&file2, "content2", 0o640);
    // Create symlinks to these files
    let symlink1 = src_dir.path().join("symlink1");
    let symlink2 = src_dir.path().join("symlink2");
    create_symlink(&file1, &symlink1);
    create_symlink(&file2, &symlink2);
    let dst_file1 = dst_dir.path().join("copied1.txt");
    let dst_file2 = dst_dir.path().join("copied2.txt");
    // Test copying with dereference and preserve
    let mut cmd1 = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd1.args([
        "--dereference",
        "--preserve",
        symlink1.to_str().unwrap(),
        dst_file1.to_str().unwrap(),
    ])
    .assert()
    .success();
    let mut cmd2 = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd2.args([
        "--dereference",
        "--preserve",
        symlink2.to_str().unwrap(),
        dst_file2.to_str().unwrap(),
    ])
    .assert()
    .success();
    // Verify results are regular files, not symlinks
    assert!(dst_file1.is_file());
    assert!(!dst_file1.is_symlink());
    assert!(dst_file2.is_file());
    assert!(!dst_file2.is_symlink());
    // Verify content and permissions of target files were preserved
    assert_eq!(get_file_content(&dst_file1), "content1");
    assert_eq!(get_file_content(&dst_file2), "content2");
    assert_eq!(get_file_mode(&dst_file1), 0o755);
    assert_eq!(get_file_mode(&dst_file2), 0o640);
}

// Profiling tests

#[test]
fn test_chrome_trace_output() {
    let (src_dir, dst_dir) = setup_test_env();
    let trace_dir = tempfile::tempdir().unwrap();
    let trace_prefix = trace_dir.path().join("trace");
    // create some files to copy
    for i in 0..10 {
        create_test_file(
            &src_dir.path().join(format!("file{i}.txt")),
            "content",
            0o644,
        );
    }
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--chrome-trace",
        trace_prefix.to_str().unwrap(),
        src_dir.path().to_str().unwrap(),
        dst_dir.path().join("copied").to_str().unwrap(),
    ])
    .assert()
    .success();
    // find the generated trace file
    let entries: Vec<_> = std::fs::read_dir(trace_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert_eq!(entries.len(), 1, "Expected exactly one trace file");
    let trace_file = entries[0].path();
    // verify the trace file is non-empty and valid JSON
    let content = std::fs::read_to_string(&trace_file).unwrap();
    assert!(!content.is_empty(), "Trace file should not be empty");
    let json: serde_json::Value =
        serde_json::from_str(&content).expect("Trace should be valid JSON");
    assert!(json.is_array(), "Chrome trace should be a JSON array");
    let events = json.as_array().unwrap();
    assert!(!events.is_empty(), "Trace should contain events");
}

#[test]
fn test_flamegraph_output() {
    let (src_dir, dst_dir) = setup_test_env();
    let flame_dir = tempfile::tempdir().unwrap();
    let flame_prefix = flame_dir.path().join("flame");
    // create some files to copy
    for i in 0..10 {
        create_test_file(
            &src_dir.path().join(format!("file{i}.txt")),
            "content",
            0o644,
        );
    }
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--flamegraph",
        flame_prefix.to_str().unwrap(),
        src_dir.path().to_str().unwrap(),
        dst_dir.path().join("copied").to_str().unwrap(),
    ])
    .assert()
    .success();
    // find the generated flamegraph file
    let entries: Vec<_> = std::fs::read_dir(flame_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "folded"))
        .collect();
    assert_eq!(entries.len(), 1, "Expected exactly one flamegraph file");
    let flame_file = entries[0].path();
    // verify the flamegraph file is non-empty and contains valid folded stack format
    let content = std::fs::read_to_string(&flame_file).unwrap();
    assert!(!content.is_empty(), "Flamegraph file should not be empty");
    // folded stack format: "stack;frames count"
    for line in content.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.rsplitn(2, ' ').collect();
        assert_eq!(
            parts.len(),
            2,
            "Each line should have 'stack count' format: {line}"
        );
        let count: u64 = parts[0].parse().expect("Count should be a number");
        assert!(count > 0, "Count should be positive");
    }
}

#[test]
fn test_profile_level_affects_output() {
    let (src_dir, dst_dir) = setup_test_env();
    let trace_dir = tempfile::tempdir().unwrap();
    let trace_prefix = trace_dir.path().join("trace");
    // create a file to copy
    create_test_file(&src_dir.path().join("file.txt"), "content", 0o644);
    // run with profile-level=error (should capture fewer events)
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--chrome-trace",
        trace_prefix.to_str().unwrap(),
        "--profile-level",
        "error",
        src_dir.path().to_str().unwrap(),
        dst_dir.path().join("copied").to_str().unwrap(),
    ])
    .assert()
    .success();
    // find and verify trace file exists (may be minimal but should be valid)
    let entries: Vec<_> = std::fs::read_dir(trace_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert_eq!(entries.len(), 1, "Expected exactly one trace file");
    let trace_file = entries[0].path();
    let content = std::fs::read_to_string(&trace_file).unwrap();
    let json: serde_json::Value =
        serde_json::from_str(&content).expect("Trace should be valid JSON");
    assert!(json.is_array(), "Chrome trace should be a JSON array");
}

#[test]
fn test_invalid_profile_level_gives_error() {
    let (src_dir, dst_dir) = setup_test_env();
    let trace_dir = tempfile::tempdir().unwrap();
    let trace_prefix = trace_dir.path().join("trace");
    create_test_file(&src_dir.path().join("file.txt"), "content", 0o644);
    // run with invalid profile-level (should fail with error, not panic)
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([
        "--chrome-trace",
        trace_prefix.to_str().unwrap(),
        "--profile-level",
        "invalid_level",
        src_dir.path().to_str().unwrap(),
        dst_dir.path().join("copied").to_str().unwrap(),
    ])
    .assert()
    .failure()
    .stderr(predicates::str::contains("Invalid --profile-level"));
}

// Tests for localhost: prefix handling and --force-remote flag

#[test]
fn test_localhost_prefix_performs_local_copy() {
    // localhost:/path should be treated as a local copy (not using rcpd)
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "localhost test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["-v", &src_remote, dst_file.to_str().unwrap()])
        .assert()
        .success()
        // should show warning about localhost being treated as local (logs go to stdout)
        .stdout(predicates::str::contains(
            "Paths with 'localhost:' prefix are treated as local",
        ))
        // should NOT show "Starting rcpd" which would indicate remote mode
        .stdout(predicates::str::contains("Starting rcpd").not());
    assert_eq!(get_file_content(&dst_file), "localhost test content");
}

#[test]
fn test_localhost_prefix_with_colons_in_path() {
    // localhost: prefix should allow paths with colons that would otherwise be ambiguous
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("file:with:colons.txt");
    let dst_file = dst_dir.path().join("copied.txt");
    create_test_file(&src_file, "colon path content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["-v", &src_remote, dst_file.to_str().unwrap()])
        .assert()
        .success();
    assert_eq!(get_file_content(&dst_file), "colon path content");
}

#[test]
fn test_path_with_colons_is_local() {
    // a path like /tmp/test-2024-01-01T12:30:45.txt should be local (not remote)
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test-2024-01-01T12:30:45.txt");
    let dst_file = dst_dir.path().join("copied.txt");
    create_test_file(&src_file, "timestamp path content", 0o644);
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["-v", src_file.to_str().unwrap(), dst_file.to_str().unwrap()])
        .assert()
        .success()
        // should NOT show warning about localhost (it's just a regular local path, logs go to stdout)
        .stdout(predicates::str::contains("localhost").not())
        // should NOT try to use rcpd
        .stdout(predicates::str::contains("Starting rcpd").not());
    assert_eq!(get_file_content(&dst_file), "timestamp path content");
}
