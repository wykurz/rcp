use std::os::unix::fs::PermissionsExt;

fn setup_test_env() -> (tempfile::TempDir, tempfile::TempDir) {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    (src_dir, dst_dir)
}

fn create_test_file(path: &std::path::Path, content: &str, mode: u32) {
    std::fs::write(path, content).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn get_file_content(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap()
}

#[test]
fn test_remote_copy_basic() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "remote test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([&src_remote, &dst_remote]).assert().success();
}

#[test]
fn test_remote_copy_localhost() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "remote test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([&src_remote, &dst_remote]).assert().success();
    assert_eq!(get_file_content(&dst_file), "remote test content");
}

#[test]
fn test_remote_copy_localhost_to_local() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("source.txt");
    let dst_file = dst_dir.path().join("destination.txt");
    create_test_file(&src_file, "localhost to local content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([&src_remote, dst_file.to_str().unwrap()])
        .assert()
        .success();
    assert_eq!(get_file_content(&dst_file), "localhost to local content");
}

#[test]
fn test_remote_copy_local_to_localhost() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("local_source.txt");
    let dst_file = dst_dir.path().join("remote_destination.txt");
    create_test_file(&src_file, "local to localhost content", 0o644);
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([src_file.to_str().unwrap(), &dst_remote])
        .assert()
        .success();
    assert_eq!(get_file_content(&dst_file), "local to localhost content");
}

#[test]
fn test_remote_copy_with_preserve() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("preserve_test.txt");
    let dst_file = dst_dir.path().join("preserve_test.txt");
    create_test_file(&src_file, "preserve permissions content", 0o755);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["--preserve", &src_remote, &dst_remote])
        .assert()
        .success();
    assert_eq!(get_file_content(&dst_file), "preserve permissions content");
    let mode = std::fs::metadata(&dst_file).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o755);
}

#[test]
fn test_remote_copy_directory() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("remote_subdir");
    let dst_subdir = dst_dir.path().join("remote_subdir");
    std::fs::create_dir(&src_subdir).unwrap();
    let src_file1 = src_subdir.join("file1.txt");
    let src_file2 = src_subdir.join("file2.txt");
    create_test_file(&src_file1, "remote dir content 1", 0o644);
    create_test_file(&src_file2, "remote dir content 2", 0o755);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["--preserve", &src_remote, &dst_remote])
        .assert()
        .success();
    let dst_file1 = dst_subdir.join("file1.txt");
    let dst_file2 = dst_subdir.join("file2.txt");
    assert_eq!(get_file_content(&dst_file1), "remote dir content 1");
    assert_eq!(get_file_content(&dst_file2), "remote dir content 2");
    let mode1 = std::fs::metadata(&dst_file1).unwrap().permissions().mode() & 0o7777;
    let mode2 = std::fs::metadata(&dst_file2).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode1, 0o644);
    assert_eq!(mode2, 0o755);
}

#[test]
fn test_remote_copy_symlink_no_dereference() {
    let (src_dir, dst_dir) = setup_test_env();
    let target_file = src_dir.path().join("target.txt");
    let symlink_file = src_dir.path().join("symlink.txt");
    let dst_symlink = dst_dir.path().join("symlink.txt");
    create_test_file(&target_file, "target content", 0o644);
    std::os::unix::fs::symlink(&target_file, &symlink_file).unwrap();
    let src_remote = format!("localhost:{}", symlink_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_symlink.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([&src_remote, &dst_remote]).assert().success();
    // verify destination is a symlink
    assert!(dst_symlink.is_symlink());
    let link_target = std::fs::read_link(&dst_symlink).unwrap();
    assert_eq!(link_target, target_file);
}

#[test]
fn test_remote_copy_symlink_with_dereference() {
    let (src_dir, dst_dir) = setup_test_env();
    let target_file = src_dir.path().join("target.txt");
    let symlink_file = src_dir.path().join("symlink.txt");
    let dst_file = dst_dir.path().join("symlink.txt");
    create_test_file(&target_file, "target content for dereference", 0o644);
    std::os::unix::fs::symlink(&target_file, &symlink_file).unwrap();
    let src_remote = format!("localhost:{}", symlink_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["-L", &src_remote, &dst_remote])
        .assert()
        .success();
    // verify destination is a regular file, not a symlink
    assert!(!dst_file.is_symlink());
    assert!(dst_file.is_file());
    assert_eq!(
        get_file_content(&dst_file),
        "target content for dereference"
    );
}

#[test]
#[ignore = "functionality not working yet"]
fn test_remote_copy_with_overwrite() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("overwrite_test.txt");
    let dst_file = dst_dir.path().join("overwrite_test.txt");
    // create source file
    create_test_file(&src_file, "new content", 0o644);
    // create existing destination file with different content
    create_test_file(&dst_file, "old content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["--overwrite", &src_remote, &dst_remote])
        .assert()
        .success();
    // verify content was overwritten
    assert_eq!(get_file_content(&dst_file), "new content");
}

#[test]
#[ignore = "functionality not working yet"]
fn test_remote_copy_without_overwrite_fails() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("no_overwrite_test.txt");
    let dst_file = dst_dir.path().join("no_overwrite_test.txt");
    // create source file
    create_test_file(&src_file, "new content", 0o644);
    // create existing destination file with different content
    create_test_file(&dst_file, "old content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([&src_remote, &dst_remote]).assert().failure(); // should fail without --overwrite
                                                             // verify content was not overwritten
    assert_eq!(get_file_content(&dst_file), "old content");
}

#[test]
#[ignore = "functionality not working yet"]
fn test_remote_copy_comprehensive() {
    let (src_dir, dst_dir) = setup_test_env();
    // create a complex directory structure with files and symlinks
    let src_subdir = src_dir.path().join("comprehensive");
    std::fs::create_dir(&src_subdir).unwrap();
    let target_file = src_subdir.join("target.txt");
    let regular_file = src_subdir.join("regular.txt");
    let symlink_file = src_subdir.join("symlink.txt");
    create_test_file(&target_file, "target content", 0o644);
    create_test_file(&regular_file, "regular content", 0o755);
    std::os::unix::fs::symlink(&target_file, &symlink_file).unwrap();
    // create destination directory with existing file to test overwrite
    let dst_subdir = dst_dir.path().join("comprehensive");
    std::fs::create_dir(&dst_subdir).unwrap();
    let existing_file = dst_subdir.join("regular.txt");
    create_test_file(&existing_file, "old content", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["--preserve", "--overwrite", "-L", &src_remote, &dst_remote])
        .assert()
        .success();
    // verify regular file was copied with permissions preserved and overwritten
    let dst_regular = dst_subdir.join("regular.txt");
    assert_eq!(get_file_content(&dst_regular), "regular content");
    let mode = std::fs::metadata(&dst_regular)
        .unwrap()
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(mode, 0o755);
    // verify symlink was dereferenced (copied as regular file due to -L)
    let dst_symlink = dst_subdir.join("symlink.txt");
    assert!(!dst_symlink.is_symlink());
    assert!(dst_symlink.is_file());
    assert_eq!(get_file_content(&dst_symlink), "target content");
    // verify target file was also copied
    let dst_target = dst_subdir.join("target.txt");
    assert_eq!(get_file_content(&dst_target), "target content");
}

#[test]
#[ignore = "functionality not working yet"]
fn test_remote_symlink_chain_dereference() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create a chain of symlinks: foo -> bar -> baz (actual file)
    let baz_file = src_dir.path().join("baz_file.txt");
    create_test_file(&baz_file, "final content", 0o644);
    let bar_link = src_dir.path().join("bar");
    let foo_link = src_dir.path().join("foo");
    // Create chain: foo -> bar -> baz_file.txt
    std::os::unix::fs::symlink(&baz_file, &bar_link).unwrap();
    std::os::unix::fs::symlink(&bar_link, &foo_link).unwrap();
    // Create a source directory with the symlink chain
    let src_subdir = src_dir.path().join("chain_test");
    std::fs::create_dir(&src_subdir).unwrap();
    // Create symlinks in the test directory that represent the chain
    std::os::unix::fs::symlink(&foo_link, &src_subdir.join("foo")).unwrap();
    std::os::unix::fs::symlink(&bar_link, &src_subdir.join("bar")).unwrap();
    std::os::unix::fs::symlink(&baz_file, &src_subdir.join("baz")).unwrap();
    let dst_subdir = dst_dir.path().join("chain_test");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // Test with dereference - should copy 3 files with same content
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args(["-L", &src_remote, &dst_remote])
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
fn test_remote_symlink_chain_no_dereference() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create a chain of symlinks: foo -> bar -> baz (actual file)
    let baz_file = src_dir.path().join("baz_file.txt");
    create_test_file(&baz_file, "final content", 0o644);
    let bar_link = src_dir.path().join("bar");
    let foo_link = src_dir.path().join("foo");
    // Create chain: foo -> bar -> baz_file.txt
    std::os::unix::fs::symlink(&baz_file, &bar_link).unwrap();
    std::os::unix::fs::symlink(&bar_link, &foo_link).unwrap();
    // Create a source directory with the symlink chain
    let src_subdir = src_dir.path().join("chain_test");
    std::fs::create_dir(&src_subdir).unwrap();
    // Create symlinks in the test directory that represent the chain
    std::os::unix::fs::symlink(&foo_link, &src_subdir.join("foo")).unwrap();
    std::os::unix::fs::symlink(&bar_link, &src_subdir.join("bar")).unwrap();
    std::os::unix::fs::symlink(&baz_file, &src_subdir.join("baz")).unwrap();
    let dst_subdir = dst_dir.path().join("chain_test");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // Test without dereference - should preserve symlinks
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.args([&src_remote, &dst_remote]).assert().success();
    // Verify all three remain as symlinks
    assert!(dst_subdir.join("foo").is_symlink());
    assert!(dst_subdir.join("bar").is_symlink());
    assert!(dst_subdir.join("baz").is_symlink());
    // Verify symlink targets are preserved
    assert_eq!(
        std::fs::read_link(&dst_subdir.join("foo")).unwrap(),
        foo_link
    );
    assert_eq!(
        std::fs::read_link(&dst_subdir.join("bar")).unwrap(),
        bar_link
    );
    assert_eq!(
        std::fs::read_link(&dst_subdir.join("baz")).unwrap(),
        baz_file
    );
}
