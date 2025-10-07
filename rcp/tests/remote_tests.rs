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

fn interpret_exit_code(code: i32) -> String {
    match code {
        0 => "Success".to_string(),
        1 => "General error".to_string(),
        2 => "Misuse of shell command".to_string(),
        124 => "Timeout (command exceeded time limit)".to_string(),
        125 => "Command not found".to_string(),
        126 => "Command found but not executable".to_string(),
        127 => "Command not found (PATH issue)".to_string(),
        128 => "Invalid exit argument".to_string(),
        130 => "Terminated by Ctrl+C (SIGINT)".to_string(),
        137 => "Killed by SIGKILL".to_string(),
        143 => "Terminated by SIGTERM".to_string(),
        code if code >= 128 => format!("Terminated by signal {}", code - 128),
        code => format!("Exit code {code}"),
    }
}

fn run_rcp_with_args(args: &[&str]) -> std::process::Output {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    // 10 second timeout - SSH connection setup can take 3-4s per connection
    cmd.args(["10", rcp_path.to_str().unwrap()]);
    cmd.arg("-vv"); // Always use maximum verbosity
    cmd.args(args);
    cmd.output().expect("Failed to execute rcp command")
}

fn print_command_output(output: &std::process::Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    eprintln!("=== RCP COMMAND OUTPUT ===");
    if let Some(code) = output.status.code() {
        eprintln!("Exit status: {} ({})", code, interpret_exit_code(code));
    } else {
        eprintln!("Exit status: terminated by signal");
    }

    if !stdout.is_empty() {
        eprintln!("--- STDOUT ---");
        eprintln!("{stdout}");
    }
    if !stderr.is_empty() {
        eprintln!("--- STDERR ---");
        eprintln!("{stderr}");
    }
    eprintln!("=== END RCP OUTPUT ===");
}

fn run_rcp_and_expect_success(args: &[&str]) -> std::process::Output {
    let output = run_rcp_with_args(args);
    print_command_output(&output);
    if !output.status.success() {
        if let Some(code) = output.status.code() {
            panic!(
                "Command failed with exit code {} ({})",
                code,
                interpret_exit_code(code)
            );
        } else {
            panic!("Command failed - terminated by signal");
        }
    }
    output
}

fn run_rcp_and_expect_failure(args: &[&str]) -> std::process::Output {
    let output = run_rcp_with_args(args);
    print_command_output(&output);
    if output.status.success() {
        panic!("Command succeeded when failure was expected");
    }
    output
}

#[test]
fn test_remote_copy_basic() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "remote test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
}

#[test]
fn test_remote_copy_localhost() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "remote test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
    assert_eq!(get_file_content(&dst_file), "remote test content");
}

#[test]
fn test_remote_copy_localhost_to_local() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("source.txt");
    let dst_file = dst_dir.path().join("destination.txt");
    create_test_file(&src_file, "localhost to local content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, dst_file.to_str().unwrap()]);
    assert_eq!(get_file_content(&dst_file), "localhost to local content");
}

#[test]
fn test_remote_copy_local_to_localhost() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("local_source.txt");
    let dst_file = dst_dir.path().join("remote_destination.txt");
    create_test_file(&src_file, "local to localhost content", 0o644);
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&[src_file.to_str().unwrap(), &dst_remote]);
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
    run_rcp_and_expect_success(&["--preserve", &src_remote, &dst_remote]);
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
    run_rcp_and_expect_success(&["--preserve", &src_remote, &dst_remote]);
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
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
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
    run_rcp_and_expect_success(&["-L", &src_remote, &dst_remote]);
    // verify destination is a regular file, not a symlink
    assert!(!dst_file.is_symlink());
    assert!(dst_file.is_file());
    assert_eq!(
        get_file_content(&dst_file),
        "target content for dereference"
    );
}

#[test]
fn test_remote_copy_with_overwrite() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("overwrite_test.txt");
    let dst_file = dst_dir.path().join("overwrite_test.txt");
    // create source file with longer content to ensure different size
    create_test_file(&src_file, "new content that is longer", 0o644);
    // create existing destination file with different, shorter content
    create_test_file(&dst_file, "old content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify content was overwritten
    assert_eq!(get_file_content(&dst_file), "new content that is longer");
}

#[test]
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
    run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify content was not overwritten
    assert_eq!(get_file_content(&dst_file), "old content");
}

#[test]
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
    run_rcp_and_expect_success(&["--preserve", "--overwrite", "-L", &src_remote, &dst_remote]);
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
    std::os::unix::fs::symlink(&foo_link, src_subdir.join("foo")).unwrap();
    std::os::unix::fs::symlink(&bar_link, src_subdir.join("bar")).unwrap();
    std::os::unix::fs::symlink(&baz_file, src_subdir.join("baz")).unwrap();
    let dst_subdir = dst_dir.path().join("chain_test");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // Test with dereference - should copy 3 files with same content
    run_rcp_and_expect_success(&["-L", &src_remote, &dst_remote]);
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
    std::os::unix::fs::symlink(&foo_link, src_subdir.join("foo")).unwrap();
    std::os::unix::fs::symlink(&bar_link, src_subdir.join("bar")).unwrap();
    std::os::unix::fs::symlink(&baz_file, src_subdir.join("baz")).unwrap();
    let dst_subdir = dst_dir.path().join("chain_test");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // Test without dereference - should preserve symlinks
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
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
fn test_remote_dereference_directory_symlink() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create a directory with specific permissions and files
    let target_dir = src_dir.path().join("target_directory");
    std::fs::create_dir(&target_dir).unwrap();
    std::fs::set_permissions(&target_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    create_test_file(&target_dir.join("file1.txt"), "content1", 0o644);
    create_test_file(&target_dir.join("file2.txt"), "content2", 0o600);
    // Create a symlink pointing to the directory
    let dir_symlink = src_dir.path().join("dir_link");
    std::os::unix::fs::symlink(&target_dir, &dir_symlink).unwrap();
    let dst_path = dst_dir.path().join("copied_directory");
    let src_remote = format!("localhost:{}", dir_symlink.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    // Test with dereference - should copy as a directory with preserved permissions
    run_rcp_and_expect_success(&["-L", "--preserve", &src_remote, &dst_remote]);
    // Verify the result is a directory, not a symlink
    assert!(dst_path.is_dir());
    assert!(!dst_path.is_symlink());
    // Verify directory permissions preserved
    let mode = std::fs::metadata(&dst_path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o755);
    // Verify files were copied with correct content and permissions
    assert_eq!(get_file_content(&dst_path.join("file1.txt")), "content1");
    assert_eq!(get_file_content(&dst_path.join("file2.txt")), "content2");
    let mode1 = std::fs::metadata(dst_path.join("file1.txt"))
        .unwrap()
        .permissions()
        .mode()
        & 0o7777;
    let mode2 = std::fs::metadata(dst_path.join("file2.txt"))
        .unwrap()
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(mode1, 0o644);
    assert_eq!(mode2, 0o600);
}

#[test]
fn test_remote_dereference_file_symlink_permissions() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create files with different permissions
    let file1 = src_dir.path().join("file1.txt");
    let file2 = src_dir.path().join("file2.txt");
    create_test_file(&file1, "content1", 0o755);
    create_test_file(&file2, "content2", 0o640);
    // Create symlinks to these files
    let symlink1 = src_dir.path().join("symlink1");
    let symlink2 = src_dir.path().join("symlink2");
    std::os::unix::fs::symlink(&file1, &symlink1).unwrap();
    std::os::unix::fs::symlink(&file2, &symlink2).unwrap();
    let dst_file1 = dst_dir.path().join("copied1.txt");
    let dst_file2 = dst_dir.path().join("copied2.txt");
    let src_remote1 = format!("localhost:{}", symlink1.to_str().unwrap());
    let dst_remote1 = format!("localhost:{}", dst_file1.to_str().unwrap());
    let src_remote2 = format!("localhost:{}", symlink2.to_str().unwrap());
    let dst_remote2 = format!("localhost:{}", dst_file2.to_str().unwrap());
    // Test copying with dereference and preserve
    run_rcp_and_expect_success(&["-L", "--preserve", &src_remote1, &dst_remote1]);
    run_rcp_and_expect_success(&["-L", "--preserve", &src_remote2, &dst_remote2]);
    // Verify results are regular files, not symlinks
    assert!(dst_file1.is_file());
    assert!(!dst_file1.is_symlink());
    assert!(dst_file2.is_file());
    assert!(!dst_file2.is_symlink());
    // Verify content and permissions of target files were preserved
    assert_eq!(get_file_content(&dst_file1), "content1");
    assert_eq!(get_file_content(&dst_file2), "content2");
    let mode1 = std::fs::metadata(&dst_file1).unwrap().permissions().mode() & 0o7777;
    let mode2 = std::fs::metadata(&dst_file2).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode1, 0o755);
    assert_eq!(mode2, 0o640);
}

#[test]
fn test_remote_debug_log_file_creation() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("debug_log_test.txt");
    let dst_file = dst_dir.path().join("debug_log_test.txt");
    create_test_file(&src_file, "debug log test content", 0o644);
    // Use a unique prefix for this test
    let temp_dir = std::env::temp_dir()
        .to_str()
        .expect("No default temp directory?")
        .to_owned();
    let log_prefix = format!("{temp_dir}/rcpd-test-{}", std::process::id());
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // Run rcp with debug log prefix
    let output = run_rcp_with_args(&[
        "--rcpd-debug-log-prefix",
        &log_prefix,
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    // Copy should succeed
    assert!(output.status.success(), "rcp command should succeed");
    assert_eq!(get_file_content(&dst_file), "debug log test content");
    // Check that debug log files were created
    let tmp_entries = std::fs::read_dir(temp_dir)
        .expect("Failed to read temp directory")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.starts_with(&format!("rcpd-test-{}", std::process::id())))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    eprintln!(
        "Found debug log files: {:?}",
        tmp_entries
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );
    assert!(!tmp_entries.is_empty(), "Debug log files should be created");
    // Verify log files contain actual log entries
    for entry in tmp_entries {
        let log_content =
            std::fs::read_to_string(entry.path()).expect("Should be able to read debug log file");
        eprintln!(
            "Log file {} contents (first 200 chars): {}",
            entry.file_name().to_str().unwrap(),
            &log_content[..std::cmp::min(200, log_content.len())]
        );
        assert!(!log_content.is_empty(), "Log files should contain content");
        // Clean up test log files
        std::fs::remove_file(entry.path()).ok();
    }
}

#[test]
fn test_remote_copy_port_range() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("port_range_test.txt");
    let dst_file = dst_dir.path().join("port_range_test.txt");
    create_test_file(&src_file, "port range test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // use a port range that's unlikely to conflict with other tests
    // we'll use a high port range to avoid conflicts with system services
    let port_range = "25000-25999";
    eprintln!("Testing remote copy with port range: {port_range}");
    run_rcp_and_expect_success(&["--quic-port-ranges", port_range, &src_remote, &dst_remote]);
    // verify the file was copied successfully
    assert_eq!(get_file_content(&dst_file), "port range test content");
}

#[test]
fn test_remote_overwrite_directory_with_directory() {
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory structure
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file1.txt"), "content1", 0o644);
    create_test_file(&src_subdir.join("file2.txt"), "content2", 0o644);
    create_test_file(&src_subdir.join("file3.txt"), "content3", 0o644);
    // create destination directory with different contents
    let dst_subdir = dst_dir.path().join("mydir");
    std::fs::create_dir(&dst_subdir).unwrap();
    create_test_file(&dst_subdir.join("file1.txt"), "old content1", 0o644);
    create_test_file(&dst_subdir.join("file4.txt"), "old file4", 0o644); // will remain
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the directory was updated recursively
    assert_eq!(get_file_content(&dst_subdir.join("file1.txt")), "content1"); // updated
    assert_eq!(get_file_content(&dst_subdir.join("file2.txt")), "content2"); // new
    assert_eq!(get_file_content(&dst_subdir.join("file3.txt")), "content3"); // new
    assert_eq!(get_file_content(&dst_subdir.join("file4.txt")), "old file4"); // unchanged
}

#[test]
fn test_remote_overwrite_file_with_directory() {
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("nested.txt"), "nested content", 0o644);
    // create destination as a file (will be replaced with directory)
    let dst_path = dst_dir.path().join("mydir");
    create_test_file(&dst_path, "this is a file", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the file was replaced with a directory
    assert!(dst_path.is_dir());
    assert_eq!(
        get_file_content(&dst_path.join("nested.txt")),
        "nested content"
    );
}

#[test]
fn test_remote_overwrite_directory_with_file() {
    let (src_dir, dst_dir) = setup_test_env();
    // create source file
    let src_file = src_dir.path().join("myfile.txt");
    create_test_file(&src_file, "file content", 0o644);
    // create destination as a directory (will be replaced with file)
    let dst_path = dst_dir.path().join("myfile.txt");
    std::fs::create_dir(&dst_path).unwrap();
    create_test_file(&dst_path.join("nested.txt"), "nested", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the directory was replaced with a file
    assert!(dst_path.is_file());
    assert_eq!(get_file_content(&dst_path), "file content");
}

#[test]
fn test_remote_overwrite_symlink_with_symlink_same_target() {
    let (src_dir, dst_dir) = setup_test_env();
    // create target file
    let target = src_dir.path().join("target.txt");
    create_test_file(&target, "target content", 0o644);
    // create source symlink
    let src_link = src_dir.path().join("link.txt");
    std::os::unix::fs::symlink("target.txt", &src_link).unwrap();
    // create destination symlink pointing to same target
    let dst_target = dst_dir.path().join("target.txt");
    create_test_file(&dst_target, "target content", 0o644);
    let dst_link = dst_dir.path().join("link.txt");
    std::os::unix::fs::symlink("target.txt", &dst_link).unwrap();
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_link.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify symlink still points to same target
    assert!(dst_link.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_link).unwrap().to_str().unwrap(),
        "target.txt"
    );
}

#[test]
fn test_remote_overwrite_symlink_with_symlink_different_target() {
    let (src_dir, dst_dir) = setup_test_env();
    // create source symlink
    let src_link = src_dir.path().join("link.txt");
    std::os::unix::fs::symlink("new_target.txt", &src_link).unwrap();
    // create destination symlink pointing to different target
    let dst_link = dst_dir.path().join("link.txt");
    std::os::unix::fs::symlink("old_target.txt", &dst_link).unwrap();
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_link.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify symlink was updated to new target
    assert!(dst_link.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_link).unwrap().to_str().unwrap(),
        "new_target.txt"
    );
}

#[test]
fn test_remote_overwrite_file_with_symlink() {
    let (src_dir, dst_dir) = setup_test_env();
    // create source symlink
    let src_link = src_dir.path().join("item.txt");
    std::os::unix::fs::symlink("target.txt", &src_link).unwrap();
    // create destination as a file (will be replaced with symlink)
    let dst_path = dst_dir.path().join("item.txt");
    create_test_file(&dst_path, "this is a file", 0o644);
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the file was replaced with a symlink
    assert!(dst_path.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_path).unwrap().to_str().unwrap(),
        "target.txt"
    );
}

#[test]
fn test_remote_overwrite_symlink_with_file() {
    let (src_dir, dst_dir) = setup_test_env();
    // create source file
    let src_file = src_dir.path().join("item.txt");
    create_test_file(&src_file, "file content", 0o644);
    // create destination as a symlink (will be replaced with file)
    let dst_path = dst_dir.path().join("item.txt");
    std::os::unix::fs::symlink("target.txt", &dst_path).unwrap();
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the symlink was replaced with a file
    assert!(dst_path.is_file());
    assert!(!dst_path.is_symlink());
    assert_eq!(get_file_content(&dst_path), "file content");
}

#[test]
fn test_remote_overwrite_directory_with_symlink() {
    let (src_dir, dst_dir) = setup_test_env();
    // create source symlink
    let src_link = src_dir.path().join("item");
    std::os::unix::fs::symlink("target", &src_link).unwrap();
    // create destination as a directory (will be replaced with symlink)
    let dst_path = dst_dir.path().join("item");
    std::fs::create_dir(&dst_path).unwrap();
    create_test_file(&dst_path.join("nested.txt"), "nested", 0o644);
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the directory was replaced with a symlink
    assert!(dst_path.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_path).unwrap().to_str().unwrap(),
        "target"
    );
}

#[test]
fn test_remote_overwrite_symlink_with_directory() {
    let (src_dir, dst_dir) = setup_test_env();
    // Create source directory
    let src_subdir = src_dir.path().join("item");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    // create destination as a symlink (will be replaced with directory)
    let dst_path = dst_dir.path().join("item");
    std::os::unix::fs::symlink("target", &dst_path).unwrap();
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the symlink was replaced with a directory
    assert!(dst_path.is_dir());
    assert!(!dst_path.is_symlink());
    assert_eq!(get_file_content(&dst_path.join("file.txt")), "content");
}

#[test]
fn test_remote_copy_nonexistent_source() {
    let (src_dir, dst_dir) = setup_test_env();
    let nonexistent_src = src_dir.path().join("does_not_exist.txt");
    let dst_file = dst_dir.path().join("destination.txt");
    let src_remote = format!("localhost:{}", nonexistent_src.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify error message mentions the source file
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);
    assert!(combined.contains("does_not_exist") && combined.contains("No such file"));
}

#[test]
fn test_remote_copy_destination_parent_missing() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("source.txt");
    create_test_file(&src_file, "content", 0o644);
    // destination parent doesn't exist
    let dst_file = dst_dir.path().join("nonexistent_dir/destination.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify error message mentions the missing directory
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);
    assert!(combined.contains("No such file") || combined.contains("nonexistent_dir"));
}

#[test]
fn test_remote_copy_unreadable_source() {
    let (src_dir, dst_dir) = setup_test_env();
    // test with a single unreadable file case (no permissions)
    let src_file = src_dir.path().join("unreadable.txt");
    let dst_file = dst_dir.path().join("unreadable.txt");
    create_test_file(&src_file, "no permissions", 0o000);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify the destination file was not created
    assert!(!dst_file.exists());
}
