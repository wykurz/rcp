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
    // 90 second timeout - SSH connection setup + auto-deployment can take ~40-50s total
    // for 2 connections (src + dst) with binary transfer, checksum verification, cleanup,
    // plus QUIC connection establishment and actual copy operations
    cmd.args(["90", rcp_path.to_str().unwrap()]);
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
    assert!(
        !output.status.success(),
        "Command succeeded when failure was expected"
    );
    output
}

macro_rules! parse_field {
    ($line:expr, $prefix:expr, $target:expr, $found_any:expr) => {
        if let Some(value) = $line.strip_prefix($prefix) {
            $target = value.parse().ok()?;
            $found_any = true;
            continue;
        }
    };
}

#[rustfmt::skip]
fn parse_summary_from_output(output: &std::process::Output) -> Option<common::copy::Summary> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut summary = common::copy::Summary::default();
    let mut found_any = false;
    for line in stdout.lines() {
        // special handling for bytes_copied which has a unit suffix (e.g., "40 B")
        if let Some(value_str) = line.strip_prefix("bytes copied: ") {
            // strip unit suffix by taking only the numeric part
            if let Some(num_str) = value_str.split_whitespace().next() {
                summary.bytes_copied = num_str.parse().ok()?;
                found_any = true;
                continue;
            }
        }
        parse_field!(line, "files copied: ", summary.files_copied, found_any);
        parse_field!(line, "symlinks created: ", summary.symlinks_created, found_any);
        parse_field!(line, "directories created: ", summary.directories_created, found_any);
        parse_field!(line, "files unchanged: ", summary.files_unchanged, found_any);
        parse_field!(line, "symlinks unchanged: ", summary.symlinks_unchanged, found_any);
        parse_field!(line, "directories unchanged: ", summary.directories_unchanged, found_any);
        parse_field!(line, "files removed: ", summary.rm_summary.files_removed, found_any);
        parse_field!(line, "symlinks removed: ", summary.rm_summary.symlinks_removed, found_any);
        parse_field!(line, "directories removed: ", summary.rm_summary.directories_removed, found_any);
        // If no prefix matched, do nothing.
    }
    if found_any {
        Some(summary)
    } else {
        None
    }
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
    let output = run_rcp_and_expect_success(&["--preserve", "--summary", &src_remote, &dst_remote]);
    let dst_file1 = dst_subdir.join("file1.txt");
    let dst_file2 = dst_subdir.join("file2.txt");
    assert_eq!(get_file_content(&dst_file1), "remote dir content 1");
    assert_eq!(get_file_content(&dst_file2), "remote dir content 2");
    let mode1 = std::fs::metadata(&dst_file1).unwrap().permissions().mode() & 0o7777;
    let mode2 = std::fs::metadata(&dst_file2).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode1, 0o644);
    assert_eq!(mode2, 0o755);
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 2);
    assert_eq!(summary.directories_created, 1);
    assert_eq!(summary.bytes_copied, 40); // "remote dir content 1" (20) + "remote dir content 2" (20)
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
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify destination is a symlink
    assert!(dst_symlink.is_symlink());
    let link_target = std::fs::read_link(&dst_symlink).unwrap();
    assert_eq!(link_target, target_file);
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.symlinks_created, 1);
    assert_eq!(summary.files_copied, 0);
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
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    // verify content was overwritten
    assert_eq!(get_file_content(&dst_file), "new content that is longer");
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
    assert_eq!(summary.rm_summary.files_removed, 0); // file-to-file overwrite is atomic, no removal counted
    assert_eq!(summary.bytes_copied, 26); // "new content that is longer"
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
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify content was not overwritten
    assert_eq!(get_file_content(&dst_file), "old content");
    // verify summary shows no files copied (error occurred before copy)
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    assert_eq!(summary.bytes_copied, 0);
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
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&format!("rcpd-test-{}", std::process::id())))
        })
        .collect::<Vec<_>>();
    eprintln!(
        "Found debug log files: {:?}",
        tmp_entries
            .iter()
            .map(std::fs::DirEntry::file_name)
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
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    // verify the directory was updated recursively
    assert_eq!(get_file_content(&dst_subdir.join("file1.txt")), "content1"); // updated
    assert_eq!(get_file_content(&dst_subdir.join("file2.txt")), "content2"); // new
    assert_eq!(get_file_content(&dst_subdir.join("file3.txt")), "content3"); // new
    assert_eq!(get_file_content(&dst_subdir.join("file4.txt")), "old file4"); // unchanged
                                                                              // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3); // file1, file2, file3
    assert_eq!(summary.rm_summary.files_removed, 0); // file1.txt overwrite is atomic, not counted as removal
    assert_eq!(summary.directories_created, 0); // directory already existed
    assert_eq!(summary.bytes_copied, 24); // "content1" (8) + "content2" (8) + "content3" (8)
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
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    // verify the file was replaced with a directory
    assert!(dst_path.is_dir());
    assert_eq!(
        get_file_content(&dst_path.join("nested.txt")),
        "nested content"
    );
    // verify summary shows file removed and directory + nested file created
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.rm_summary.files_removed, 1); // old "mydir" file was removed
    assert_eq!(summary.directories_created, 1); // new "mydir" directory created
    assert_eq!(summary.files_copied, 1); // nested.txt copied
    assert_eq!(summary.bytes_copied, 14); // "nested content"
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
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    // verify the directory was replaced with a file
    assert!(dst_path.is_file());
    assert_eq!(get_file_content(&dst_path), "file content");
    // verify summary shows directory and nested file removed, then file copied
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.rm_summary.files_removed, 1); // nested.txt was removed
    assert_eq!(summary.rm_summary.directories_removed, 1); // old directory was removed
    assert_eq!(summary.files_copied, 1); // new file copied
    assert_eq!(summary.bytes_copied, 12); // "file content"
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
    let combined = format!("{stdout}{stderr}");
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
    let combined = format!("{stdout}{stderr}");
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

#[test]
fn test_remote_copy_directory_with_unreadable_files_continue() {
    let (src_dir, dst_dir) = setup_test_env();
    // create directory structure with some unreadable files
    let src_subdir = src_dir.path().join("mixed_dir");
    std::fs::create_dir(&src_subdir).unwrap();
    // readable files
    create_test_file(&src_subdir.join("file1.txt"), "readable content 1", 0o644);
    create_test_file(&src_subdir.join("file2.txt"), "readable content 2", 0o644);
    // unreadable files
    create_test_file(&src_subdir.join("unreadable1.txt"), "secret 1", 0o000);
    create_test_file(&src_subdir.join("file3.txt"), "readable content 3", 0o644);
    create_test_file(&src_subdir.join("unreadable2.txt"), "secret 2", 0o000);
    let dst_subdir = dst_dir.path().join("mixed_dir");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // without --fail-early, should continue copying readable files
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify readable files were copied
    assert!(dst_subdir.join("file1.txt").exists());
    assert!(dst_subdir.join("file2.txt").exists());
    assert!(dst_subdir.join("file3.txt").exists());
    assert_eq!(
        get_file_content(&dst_subdir.join("file1.txt")),
        "readable content 1"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("file2.txt")),
        "readable content 2"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("file3.txt")),
        "readable content 3"
    );
    // verify unreadable files were not copied
    assert!(!dst_subdir.join("unreadable1.txt").exists());
    assert!(!dst_subdir.join("unreadable2.txt").exists());
    // verify summary shows partial success: 3 files copied, 1 directory created
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3);
    assert_eq!(summary.directories_created, 1);
    assert_eq!(summary.bytes_copied, 54); // sum of 3 readable files
                                          // verify non-zero exit code
    assert!(!output.status.success());
}

#[test]
fn test_remote_copy_directory_with_unreadable_files_fail_early() {
    let (src_dir, dst_dir) = setup_test_env();
    // create test with readable file first, then unreadable file
    // this ensures directory gets created before failure
    let src_subdir = src_dir.path().join("fail_early_test");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("a_good.txt"), "good", 0o644);
    create_test_file(&src_subdir.join("b_unreadable.txt"), "secret", 0o000);
    create_test_file(&src_subdir.join("c_good.txt"), "also good", 0o644);
    let dst_subdir = dst_dir.path().join("fail_early_test");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // with --fail-early, should stop on first error
    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--summary", &src_remote, &dst_remote]);
    // with fail-early, exact behavior depends on timing
    // we just verify:
    // 1. operation failed (non-zero exit)
    // 2. not all files were copied (< 3)
    // 3. some progress may have been made before the error
    assert!(
        !output.status.success(),
        "Operation should fail with non-zero exit code"
    );

    // try to parse summary, but it might not be available if connection closed too quickly
    if let Some(summary) = parse_summary_from_output(&output) {
        assert!(
            summary.files_copied < 3,
            "Should not copy all files with fail-early, got {}",
            summary.files_copied
        );
    }
}

#[test]
fn test_remote_copy_nested_directories_with_unreadable_files() {
    let (src_dir, dst_dir) = setup_test_env();
    // create nested directory structure with some unreadable files
    let src_root = src_dir.path().join("root");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("root_file.txt"), "root content", 0o644);
    create_test_file(&src_root.join("unreadable_root.txt"), "secret root", 0o000);
    // readable subdirectory with mixed readable/unreadable files
    let subdir = src_root.join("subdir");
    std::fs::create_dir(&subdir).unwrap();
    create_test_file(&subdir.join("good.txt"), "good content", 0o644);
    create_test_file(&subdir.join("secret.txt"), "secret content", 0o000);
    // another readable file
    create_test_file(&src_root.join("zzz_last.txt"), "last content", 0o644);
    let dst_root = dst_dir.path().join("root");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // without --fail-early, should continue despite unreadable files
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify readable content was copied
    assert!(dst_root.join("root_file.txt").exists());
    assert!(dst_root.join("subdir").exists());
    assert!(dst_root.join("subdir/good.txt").exists());
    assert!(dst_root.join("zzz_last.txt").exists());
    assert_eq!(
        get_file_content(&dst_root.join("root_file.txt")),
        "root content"
    );
    assert_eq!(
        get_file_content(&dst_root.join("subdir/good.txt")),
        "good content"
    );
    assert_eq!(
        get_file_content(&dst_root.join("zzz_last.txt")),
        "last content"
    );
    // verify unreadable files were not copied
    assert!(!dst_root.join("unreadable_root.txt").exists());
    assert!(!dst_root.join("subdir/secret.txt").exists());
    // verify summary: 3 readable files copied, 2 directories created
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3);
    assert_eq!(summary.directories_created, 2); // root + subdir
                                                // verify non-zero exit code
    assert!(!output.status.success());
}

#[test]
fn test_remote_copy_mixed_success_with_symlink_errors() {
    let (src_dir, dst_dir) = setup_test_env();
    // create directory with files and symlinks, some operations will fail
    let src_subdir = src_dir.path().join("mixed_ops");
    std::fs::create_dir(&src_subdir).unwrap();
    // regular file that will succeed
    create_test_file(&src_subdir.join("good_file.txt"), "good content", 0o644);
    // create a symlink to a file
    let target = src_subdir.join("target.txt");
    create_test_file(&target, "target content", 0o644);
    std::os::unix::fs::symlink(&target, src_subdir.join("good_symlink")).unwrap();
    // unreadable file
    create_test_file(&src_subdir.join("unreadable.txt"), "secret", 0o000);
    // another good file
    create_test_file(
        &src_subdir.join("zzz_another.txt"),
        "another content",
        0o644,
    );
    let dst_subdir = dst_dir.path().join("mixed_ops");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify successful operations
    assert!(dst_subdir.join("good_file.txt").exists());
    assert!(dst_subdir.join("good_symlink").exists());
    assert!(dst_subdir.join("target.txt").exists());
    assert!(dst_subdir.join("zzz_another.txt").exists());
    assert_eq!(
        get_file_content(&dst_subdir.join("good_file.txt")),
        "good content"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("target.txt")),
        "target content"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("zzz_another.txt")),
        "another content"
    );
    // verify symlink
    assert!(dst_subdir.join("good_symlink").is_symlink());
    // verify failed operations
    assert!(!dst_subdir.join("unreadable.txt").exists());
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3); // good_file.txt, target.txt, zzz_another.txt
    assert_eq!(summary.symlinks_created, 1); // good_symlink
    assert_eq!(summary.directories_created, 1); // mixed_ops
                                                // verify non-zero exit code
    assert!(!output.status.success());
}

#[test]
fn test_remote_copy_all_operations_fail() {
    let (src_dir, dst_dir) = setup_test_env();
    // create a directory with only unreadable files
    let src_subdir = src_dir.path().join("all_fail");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("unreadable1.txt"), "secret 1", 0o000);
    create_test_file(&src_subdir.join("unreadable2.txt"), "secret 2", 0o000);
    create_test_file(&src_subdir.join("unreadable3.txt"), "secret 3", 0o000);
    let dst_subdir = dst_dir.path().join("all_fail");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify directory was created but no files
    assert!(dst_subdir.exists());
    assert!(!dst_subdir.join("unreadable1.txt").exists());
    assert!(!dst_subdir.join("unreadable2.txt").exists());
    assert!(!dst_subdir.join("unreadable3.txt").exists());
    // verify summary shows only directory creation
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    assert_eq!(summary.directories_created, 1);
    assert_eq!(summary.bytes_copied, 0);
    // verify non-zero exit code
    assert!(!output.status.success());
}

#[test]
fn test_remote_copy_unwritable_destination() {
    // this test verifies behavior when destination directory is not writable
    let (src_dir, dst_dir) = setup_test_env();
    // create source file
    let src_file = src_dir.path().join("source.txt");
    create_test_file(&src_file, "source content", 0o644);
    // create destination directory with no write permissions
    let dst_subdir = dst_dir.path().join("readonly_dir");
    std::fs::create_dir(&dst_subdir).unwrap();
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let dst_file = dst_subdir.join("destination.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify file was not created
    assert!(!dst_file.exists());
    // verify summary shows no files copied
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    // restore permissions for cleanup
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755)).unwrap();
}

// ============================================================================
// Lifecycle Management Tests
// ============================================================================

/// find rcpd processes running on the system
fn find_rcpd_processes() -> Vec<u32> {
    let output = std::process::Command::new("pgrep")
        .arg("-x") // exact match
        .arg("rcpd")
        .output()
        .expect("Failed to run pgrep");
    if !output.status.success() {
        return vec![];
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect()
}

/// wait for rcpd processes to exit (with timeout)
fn wait_for_rcpd_exit(initial_pids: &[u32], timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    loop {
        let current_pids = find_rcpd_processes();
        let remaining: Vec<_> = initial_pids
            .iter()
            .filter(|pid| current_pids.contains(pid))
            .collect();
        if remaining.is_empty() {
            return true;
        }
        if start.elapsed().as_secs() >= timeout_secs {
            eprintln!("Timeout waiting for rcpd processes to exit. Remaining PIDs: {remaining:?}");
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// create a large test file to ensure copy takes several seconds
fn create_large_test_file(path: &std::path::Path, size_mb: usize) {
    use std::io::Write;
    let mut file = std::fs::File::create(path).unwrap();
    let chunk = vec![b'A'; 1024 * 1024]; // 1MB chunk
    for _ in 0..size_mb {
        file.write_all(&chunk).unwrap();
    }
    file.flush().unwrap();
}

#[test]
fn test_remote_rcpd_exits_when_master_killed() {
    // verify that rcpd processes exit when the master (rcp) is killed
    // the stdin watchdog should detect master death immediately
    let (src_dir, dst_dir) = setup_test_env();
    // create a very large file (200MB) to ensure copy takes ~10 seconds over localhost
    let src_file = src_dir.path().join("large_file.dat");
    eprintln!("Creating 200MB test file...");
    create_large_test_file(&src_file, 200);
    let dst_file = dst_dir.path().join("large_file.dat");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // get initial rcpd processes
    let initial_pids = find_rcpd_processes();
    eprintln!("Initial rcpd processes: {initial_pids:?}");
    // spawn rcp as subprocess
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    eprintln!("Spawning rcp subprocess...");
    let mut child = std::process::Command::new(rcp_path)
        .args(["-vv", &src_remote, &dst_remote])
        .spawn()
        .expect("Failed to spawn rcp");
    // wait 1 second to ensure copy starts and rcpd processes are spawned
    std::thread::sleep(std::time::Duration::from_millis(1500));
    // check that rcpd processes were spawned
    let running_pids = find_rcpd_processes();
    eprintln!("Running rcpd processes after 1.5s: {running_pids:?}");
    let new_pids: Vec<_> = running_pids
        .iter()
        .filter(|pid| !initial_pids.contains(pid))
        .copied()
        .collect();
    if new_pids.is_empty() {
        // copy might have completed already - this is okay, skip the test
        eprintln!("⚠ Copy completed too quickly to test master kill scenario - skipping");
        child.wait().ok();
        return;
    }
    eprintln!("New rcpd PIDs spawned by this test: {new_pids:?}");
    // kill the master with SIGKILL (simulates crash)
    eprintln!("Killing master process (PID: {}) with SIGKILL", child.id());
    child.kill().expect("Failed to kill master");
    child.wait().expect("Failed to wait for master");
    // stdin watchdog should detect master death immediately
    // wait up to 5 seconds for rcpd to exit (should be much faster with stdin watchdog)
    let exited = wait_for_rcpd_exit(&new_pids, 5);
    assert!(
        exited,
        "rcpd processes should exit within 5 seconds after master is killed"
    );
    eprintln!("✓ All rcpd processes exited successfully");
}

#[test]
fn test_remote_rcpd_exits_when_master_killed_with_throttle() {
    // alternative test that uses throttling to ensure copy is in progress when killed
    // verifies the stdin watchdog works correctly
    let (src_dir, dst_dir) = setup_test_env();
    // create a moderate file (50MB)
    let src_file = src_dir.path().join("throttled_file.dat");
    eprintln!("Creating 50MB test file...");
    create_large_test_file(&src_file, 50);
    let dst_file = dst_dir.path().join("throttled_file.dat");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // get initial rcpd processes
    let initial_pids = find_rcpd_processes();
    eprintln!("Initial rcpd processes: {initial_pids:?}");
    // spawn rcp with throttling to slow down the copy
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    eprintln!("Spawning rcp subprocess with throttling...");
    let mut child = std::process::Command::new(rcp_path)
        .args([
            "-vv",
            "--ops-throttle=100", // limit to 100 operations per second
            &src_remote,
            &dst_remote,
        ])
        .spawn()
        .expect("Failed to spawn rcp");
    // wait 2 seconds to ensure copy is in progress with throttling
    std::thread::sleep(std::time::Duration::from_secs(2));
    // check that rcpd processes were spawned
    let running_pids = find_rcpd_processes();
    eprintln!("Running rcpd processes after 2s: {running_pids:?}");
    let new_pids: Vec<_> = running_pids
        .iter()
        .filter(|pid| !initial_pids.contains(pid))
        .copied()
        .collect();
    if new_pids.is_empty() {
        eprintln!("⚠ No rcpd processes found - copy may have completed too quickly - skipping");
        child.wait().ok();
        return;
    }
    eprintln!("New rcpd PIDs spawned by this test: {new_pids:?}");
    // kill the master with SIGKILL (simulates crash)
    eprintln!("Killing master process (PID: {}) with SIGKILL", child.id());
    child.kill().expect("Failed to kill master");
    child.wait().expect("Failed to wait for master");
    // stdin watchdog should detect master death immediately
    // wait up to 3 seconds for rcpd to exit (stdin watchdog should be instant, QUIC timeout is 10s backup)
    let start = std::time::Instant::now();
    let exited = wait_for_rcpd_exit(&new_pids, 3);
    let elapsed = start.elapsed();
    assert!(
        exited,
        "rcpd processes should exit within 3 seconds after master is killed (stdin watchdog)"
    );
    eprintln!("✓ All rcpd processes exited in {elapsed:?} (stdin watchdog worked!)");
    // verify it was faster than QUIC timeout would be (10 seconds)
    assert!(
        elapsed.as_secs() < 5,
        "rcpd should exit quickly via stdin watchdog, not wait for QUIC timeout (10s)"
    );
}

#[test]
fn test_remote_rcpd_no_zombie_processes() {
    // verify that rcpd processes don't become zombies after master exits
    let (src_dir, dst_dir) = setup_test_env();
    // create a small file for quick copy
    let src_file = src_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let dst_file = dst_dir.path().join("test.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // get initial rcpd processes before starting our test
    let initial_pids = find_rcpd_processes();
    eprintln!("Initial rcpd PIDs: {initial_pids:?}");
    // run a successful copy
    let output = run_rcp_with_args(&[&src_remote, &dst_remote]);
    assert!(output.status.success(), "Copy should succeed");
    // get rcpd processes spawned during copy (right after completion)
    let during_pids = find_rcpd_processes();
    let test_spawned_pids: Vec<_> = during_pids
        .iter()
        .filter(|pid| !initial_pids.contains(pid))
        .copied()
        .collect();
    eprintln!("PIDs spawned by this test: {test_spawned_pids:?}");
    // wait for cleanup of the processes spawned by THIS test
    // rcpd processes need time to cleanly shutdown: send result, close connections, etc.
    if !test_spawned_pids.is_empty() {
        // wait up to 5 seconds for rcpd processes to exit
        let cleanup_timeout = std::time::Duration::from_secs(5);
        let start = std::time::Instant::now();
        let mut exited = false;
        while start.elapsed() < cleanup_timeout {
            let final_pids = find_rcpd_processes();
            let remaining: Vec<_> = test_spawned_pids
                .iter()
                .filter(|pid| final_pids.contains(pid))
                .copied()
                .collect();
            if remaining.is_empty() {
                exited = true;
                eprintln!("✓ All test rcpd processes exited in {:?}", start.elapsed());
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if !exited {
            let final_pids = find_rcpd_processes();
            let remaining: Vec<_> = test_spawned_pids
                .iter()
                .filter(|pid| final_pids.contains(pid))
                .copied()
                .collect();
            assert!(
                remaining.is_empty(),
                "No rcpd processes from this test should remain after successful copy. Found: {remaining:?}"
            );
        }
    }
    // check for zombie processes spawned by this test
    if !test_spawned_pids.is_empty() {
        let ps_output = std::process::Command::new("ps")
            .args(["aux"])
            .output()
            .expect("Failed to run ps");
        let ps_stdout = String::from_utf8_lossy(&ps_output.stdout);
        let zombie_lines: Vec<_> = ps_stdout
            .lines()
            .filter(|line| {
                line.contains("rcpd") && line.contains(" Z ") && {
                    // only check for zombies matching our test's PIDs
                    test_spawned_pids.iter().any(|pid| {
                        line.split_whitespace()
                            .nth(1)
                            .and_then(|s| s.parse::<u32>().ok())
                            .map(|line_pid| line_pid == *pid)
                            .unwrap_or(false)
                    })
                }
            })
            .collect();
        assert!(
            zombie_lines.is_empty(),
            "No zombie rcpd processes should exist from this test. Found:\n{}",
            zombie_lines.join("\n")
        );
    }
    eprintln!("✓ No zombie processes found");
}

#[test]
fn test_remote_rcpd_with_custom_quic_timeouts() {
    // verify that custom QUIC timeout values are accepted and work correctly
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let dst_file = dst_dir.path().join("test.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // test with custom timeout values
    let output = run_rcp_with_args(&[
        "--quic-idle-timeout-sec=5",
        "--quic-keep-alive-interval-sec=2",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "Copy with custom QUIC timeouts should succeed"
    );
    assert!(dst_file.exists(), "Destination file should exist");
    let content = get_file_content(&dst_file);
    assert_eq!(content, "test content");
    eprintln!("✓ Copy with custom QUIC timeouts succeeded");
}

#[test]
fn test_remote_rcpd_aggressive_timeout_configuration() {
    // verify that moderately aggressive timeout values work correctly (for LAN environments)
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let dst_file = dst_dir.path().join("test.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // test with moderately aggressive timeouts suitable for fast LAN environments
    // note: very aggressive values (3s-5s) can be too tight even for localhost
    // using 8s idle timeout as a reasonable "aggressive but safe" value
    let output = run_rcp_with_args(&[
        "--quic-idle-timeout-sec=8",
        "--quic-keep-alive-interval-sec=1",
        "--remote-copy-conn-timeout-sec=10",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "Copy with aggressive timeouts should succeed"
    );
    assert!(dst_file.exists(), "Destination file should exist");
    eprintln!("✓ Copy with aggressive timeouts succeeded");
}

#[test]
fn test_remote_auto_deploy_rcpd() {
    // test automatic deployment of rcpd binary to remote host
    // NOTE: This test temporarily moves rcpd binary to force deployment
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("auto_deploy_test.txt");
    let dst_file = dst_dir.path().join("auto_deploy_test.txt");
    create_test_file(&src_file, "testing auto-deployment", 0o644);

    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // get current version to check for deployed binary
    let version_output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rcp"))
        .arg("--protocol-version")
        .output()
        .expect("Failed to get version");
    let version_json: serde_json::Value =
        serde_json::from_slice(&version_output.stdout).expect("Failed to parse version JSON");
    let semantic_version = version_json["semantic"]
        .as_str()
        .expect("Missing semantic version");
    // clean up any previously deployed rcpd for this version to force deployment
    let cache_dir = std::path::PathBuf::from(
        std::env::var("HOME").expect("HOME environment variable not set - required for test"),
    )
    .join(".cache/rcp/bin");
    let deployed_rcpd = cache_dir.join(format!("rcpd-{}", semantic_version));
    if deployed_rcpd.exists() {
        eprintln!(
            "Removing existing deployed rcpd to force re-deployment: {}",
            deployed_rcpd.display()
        );
        std::fs::remove_file(&deployed_rcpd).ok();
    }
    // use --rcpd-path=/nonexistent to force discovery failure and trigger auto-deployment.
    // this allows deployment to find the correct local rcpd binary (same build as rcp) to transfer.
    // we can't reliably hide all rcpd binaries (e.g., nix profile is owned by root).
    eprintln!(
        "Testing auto-deployment with version {} (using --rcpd-path=/nonexistent/rcpd)",
        semantic_version
    );
    let output = run_rcp_with_args(&[
        "--auto-deploy-rcpd",
        "--rcpd-path=/nonexistent/rcpd",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    // verify the copy succeeded
    assert!(
        output.status.success(),
        "Copy with auto-deploy should succeed"
    );
    assert!(dst_file.exists(), "Destination file should exist");
    assert_eq!(get_file_content(&dst_file), "testing auto-deployment");
    // verify that rcpd was deployed to cache
    assert!(
        deployed_rcpd.exists(),
        "rcpd should be deployed to {}",
        deployed_rcpd.display()
    );
    // verify it's executable
    let metadata = std::fs::metadata(&deployed_rcpd).expect("Failed to get deployed rcpd metadata");
    let permissions = metadata.permissions();
    assert!(
        permissions.mode() & 0o100 != 0,
        "deployed rcpd should be executable"
    );

    eprintln!("✓ Auto-deployment test succeeded");
    eprintln!("✓ Deployed binary at: {}", deployed_rcpd.display());
}

#[test]
fn test_remote_auto_deploy_reuses_cached_binary() {
    // test that auto-deployment reuses already-deployed binary
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("cached_deploy_test.txt");
    let dst_file = dst_dir.path().join("cached_deploy_test.txt");
    create_test_file(&src_file, "testing cached deployment", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // first run with --auto-deploy-rcpd to ensure binary is deployed
    // use --rcpd-path=/nonexistent to force deployment (discovery will fail)
    eprintln!("First run: ensuring rcpd is deployed");
    let output1 = run_rcp_with_args(&[
        "--auto-deploy-rcpd",
        "--rcpd-path=/nonexistent/rcpd",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output1);
    assert!(
        output1.status.success(),
        "First copy with auto-deploy should succeed"
    );
    // get modification time of deployed binary
    let version_output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rcp"))
        .arg("--protocol-version")
        .output()
        .expect("Failed to get version");
    let version_json: serde_json::Value =
        serde_json::from_slice(&version_output.stdout).expect("Failed to parse version JSON");
    let semantic_version = version_json["semantic"]
        .as_str()
        .expect("Missing semantic version");
    let cache_dir = std::path::PathBuf::from(
        std::env::var("HOME").expect("HOME environment variable not set - required for test"),
    )
    .join(".cache/rcp/bin");
    let deployed_rcpd = cache_dir.join(format!("rcpd-{}", semantic_version));
    let first_mtime = std::fs::metadata(&deployed_rcpd)
        .expect("deployed rcpd should exist")
        .modified()
        .expect("should have modified time");
    // second run should reuse the deployed binary (no re-deployment needed)
    // to ensure we're testing caching, use a different file
    let src_file2 = src_dir.path().join("cached_deploy_test2.txt");
    let dst_file2 = dst_dir.path().join("cached_deploy_test2.txt");
    create_test_file(&src_file2, "second test", 0o644);
    let src_remote2 = format!("localhost:{}", src_file2.to_str().unwrap());
    let dst_remote2 = format!("localhost:{}", dst_file2.to_str().unwrap());
    eprintln!("Second run: should reuse deployed binary");
    let output2 = run_rcp_with_args(&["--auto-deploy-rcpd", &src_remote2, &dst_remote2]);
    print_command_output(&output2);
    assert!(output2.status.success(), "Second copy should also succeed");
    // verify mtime hasn't changed (binary wasn't re-deployed)
    let second_mtime = std::fs::metadata(&deployed_rcpd)
        .expect("deployed rcpd should still exist")
        .modified()
        .expect("should have modified time");
    assert_eq!(
        first_mtime, second_mtime,
        "deployed binary should not be re-deployed (mtime should match)"
    );
    eprintln!("✓ Cached deployment test succeeded");
    eprintln!("✓ Binary was reused, not re-deployed");
}
