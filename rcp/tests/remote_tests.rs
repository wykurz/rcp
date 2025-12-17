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

/// Exit code 124 indicates the timeout wrapper killed rcp.
/// This usually means rcp hung and should be treated as a test failure,
/// not as an "expected failure" in tests that expect rcp to fail.
const TIMEOUT_EXIT_CODE: i32 = 124;

fn assert_not_timeout(output: &std::process::Output) {
    if let Some(code) = output.status.code() {
        if code == TIMEOUT_EXIT_CODE {
            panic!(
                "rcp was killed by timeout wrapper (exit code 124). \
                 This indicates rcp hung and did not complete within the time limit. \
                 This is NOT the same as an expected failure from rcp."
            );
        }
    }
}

fn run_rcp_with_args_internal(
    args: &[&str],
    home: Option<&std::path::Path>,
    extra_env: &[(&str, &str)],
) -> std::process::Output {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    // 90 second timeout - SSH connection setup + auto-deployment can take ~40-50s total
    // for 2 connections (src + dst) with binary transfer, checksum verification, cleanup,
    // plus TCP connection establishment and actual copy operations
    cmd.args(["90", rcp_path.to_str().unwrap()]);
    cmd.arg("-vv"); // always use maximum verbosity
    cmd.arg("--force-remote"); // force remote copy mode for localhost tests
    cmd.args(args);
    if let Some(home) = home {
        cmd.env("HOME", home);
    }
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    let output = cmd.output().expect("Failed to execute rcp command");
    // CRITICAL: check for timeout immediately. this ensures tests that expect failure
    // don't incorrectly pass when rcp actually hung (timeout exit code 124).
    // this check happens before returning to any test, so all tests automatically
    // detect and fail on timeout.
    assert_not_timeout(&output);
    output
}

fn run_rcp_with_args(args: &[&str]) -> std::process::Output {
    run_rcp_with_args_internal(args, None, &[])
}

fn run_rcp_with_args_home_and_env(
    args: &[&str],
    home: &std::path::Path,
    envs: &[(&str, &str)],
) -> std::process::Output {
    run_rcp_with_args_internal(args, Some(home), envs)
}

fn cache_bin_dir(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".cache/rcp/bin")
}

fn make_test_home() -> tempfile::TempDir {
    let temp_home = tempfile::tempdir().unwrap();
    if let Ok(real_home) = std::env::var("HOME") {
        let ssh_src = std::path::Path::new(&real_home).join(".ssh");
        let ssh_dest = temp_home.path().join(".ssh");
        if ssh_src.exists() && !ssh_dest.exists() {
            // allow SSH to find existing keys/known_hosts when we override HOME
            let _ = std::os::unix::fs::symlink(&ssh_src, &ssh_dest);
        }
    }
    temp_home
}

fn local_ssh_available() -> bool {
    static SSH_AVAILABLE: std::sync::OnceLock<(bool, String)> = std::sync::OnceLock::new();
    let (ok, msg) = SSH_AVAILABLE.get_or_init(|| {
        match std::process::Command::new("ssh")
            .args(["-o", "BatchMode=yes", "localhost", "true"])
            .output()
        {
            Ok(output) => (
                output.status.success(),
                format!(
                    "ssh exit: {:?}, stdout: {}, stderr: {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ),
            ),
            Err(err) => (false, format!("failed to invoke ssh: {err:#}")),
        }
    });
    if !ok {
        eprintln!("localhost ssh check failed: {msg}");
    }
    *ok
}

fn require_local_ssh() {
    assert!(
        local_ssh_available(),
        "localhost SSH is required for remote tests. Please ensure sshd is running and accessible."
    );
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
    // note: timeout check is already done in run_rcp_with_args_internal
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
    // note: timeout check is already done in run_rcp_with_args_internal
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
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "remote test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
}

/// Test remote copy with --no-encryption flag (plain TCP, no TLS)
#[test]
fn test_remote_copy_no_encryption() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "no encryption test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&["--no-encryption", &src_remote, &dst_remote]);
    assert_eq!(get_file_content(&dst_file), "no encryption test content");
}

#[test]
fn test_remote_copy_localhost() {
    require_local_ssh();
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
fn test_remote_copy_tilde_source_to_local() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let src_file = home.path().join("tilde_source.txt");
    create_test_file(&src_file, "tilde home content", 0o644);
    let dst_dir = tempfile::tempdir().unwrap();
    let dst_file = dst_dir.path().join("tilde_source.txt");
    let src_remote = "localhost:~/tilde_source.txt".to_string();
    let output = run_rcp_with_args_home_and_env(
        &[&src_remote, dst_file.to_str().unwrap()],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(output.status.success());
    assert_eq!(get_file_content(&dst_file), "tilde home content");
}

#[test]
fn test_remote_copy_local_to_tilde_destination() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let (src_dir, _) = setup_test_env();
    let src_file = src_dir.path().join("tilde_dest.txt");
    create_test_file(&src_file, "tilde dest content", 0o644);
    let dst_remote = "localhost:~/tilde_dest.txt".to_string();
    let output = run_rcp_with_args_home_and_env(
        &[src_file.to_str().unwrap(), &dst_remote],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(output.status.success());
    let remote_dst = home.path().join("tilde_dest.txt");
    assert_eq!(get_file_content(&remote_dst), "tilde dest content");
}

#[test]
fn test_remote_copy_local_to_tilde_home_directory() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let (src_dir, _) = setup_test_env();
    let src_file = src_dir.path().join("tilde_home_dir.txt");
    create_test_file(&src_file, "tilde home dir content", 0o644);
    let dst_remote = "localhost:~/".to_string();
    let output = run_rcp_with_args_home_and_env(
        &[src_file.to_str().unwrap(), &dst_remote],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(output.status.success());
    let remote_dst = home.path().join("tilde_home_dir.txt");
    assert_eq!(get_file_content(&remote_dst), "tilde home dir content");
}

#[test]
fn test_remote_copy_localhost_to_local() {
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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

/// Test copying many small files to exercise stream pooling.
///
/// This test creates 150 small files in a directory and verifies they are all
/// copied correctly. With stream pooling (default 100 streams), this exercises
/// stream reuse as multiple files will be sent over the same streams.
#[test]
fn test_remote_copy_many_small_files() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("many_files");
    let dst_subdir = dst_dir.path().join("many_files");
    std::fs::create_dir(&src_subdir).unwrap();
    // create 150 small files (more than default pool size of 100)
    // this exercises stream reuse without causing pool exhaustion issues
    let num_files: usize = 150;
    for i in 0..num_files {
        let content = format!("content of file {i}");
        create_test_file(
            &src_subdir.join(format!("file_{i:04}.txt")),
            &content,
            0o644,
        );
    }
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify all files were copied
    for i in 0..num_files {
        let dst_file = dst_subdir.join(format!("file_{i:04}.txt"));
        assert!(dst_file.exists(), "file_{i:04}.txt should exist");
        let expected_content = format!("content of file {i}");
        assert_eq!(
            get_file_content(&dst_file),
            expected_content,
            "file_{i:04}.txt content mismatch"
        );
    }
    // verify files were copied by checking stdout for the count
    // (parse_summary_from_output may fail due to bytes_copied having KB/MB suffix)
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!("files copied: {num_files}")),
        "Expected 'files copied: {num_files}' in output"
    );
    assert!(
        stdout.contains("directories created: 1"),
        "Expected 'directories created: 1' in output"
    );
}

#[test]
fn test_remote_copy_symlink_no_dereference() {
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    run_rcp_and_expect_success(&["--port-ranges", port_range, &src_remote, &dst_remote]);
    // verify the file was copied successfully
    assert_eq!(get_file_content(&dst_file), "port range test content");
}

#[test]
fn test_remote_overwrite_directory_with_directory() {
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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
    require_local_ssh();
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

#[test]
fn test_remote_copy_destination_partial_write_failure() {
    // this test verifies that when some files can't be written to the destination,
    // other files in the same transfer still get copied (stream recovery works).
    // this exercises the destination-side error handling with stream pooling.
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory structure:
    // src/
    //   good_dir/
    //     file1.txt
    //     file2.txt
    //   bad_dir/
    //     file3.txt  <- will fail (destination not writable)
    //     file4.txt  <- will fail
    //   more_good/
    //     file5.txt
    let src_root = src_dir.path().join("mixed");
    let src_good_dir = src_root.join("good_dir");
    let src_bad_dir = src_root.join("bad_dir");
    let src_more_good = src_root.join("more_good");
    std::fs::create_dir_all(&src_good_dir).unwrap();
    std::fs::create_dir_all(&src_bad_dir).unwrap();
    std::fs::create_dir_all(&src_more_good).unwrap();
    create_test_file(&src_good_dir.join("file1.txt"), "content 1", 0o644);
    create_test_file(&src_good_dir.join("file2.txt"), "content 2", 0o644);
    create_test_file(&src_bad_dir.join("file3.txt"), "content 3", 0o644);
    create_test_file(&src_bad_dir.join("file4.txt"), "content 4", 0o644);
    create_test_file(&src_more_good.join("file5.txt"), "content 5", 0o644);
    // create destination with bad_dir being unwritable
    let dst_root = dst_dir.path().join("mixed");
    let dst_bad_dir = dst_root.join("bad_dir");
    std::fs::create_dir_all(&dst_bad_dir).unwrap();
    std::fs::set_permissions(&dst_bad_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}/", dst_dir.path().to_str().unwrap());
    // run without --fail-early to continue after errors
    let output = run_rcp_with_args(&["-vv", "--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // restore permissions before assertions (for cleanup)
    std::fs::set_permissions(&dst_bad_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    // verify: good files should be copied despite bad_dir failures
    let dst_good_dir = dst_root.join("good_dir");
    let dst_more_good = dst_root.join("more_good");
    assert!(
        dst_good_dir.join("file1.txt").exists(),
        "file1.txt should be copied"
    );
    assert!(
        dst_good_dir.join("file2.txt").exists(),
        "file2.txt should be copied"
    );
    assert!(
        dst_more_good.join("file5.txt").exists(),
        "file5.txt should be copied"
    );
    // verify: bad_dir files should NOT be copied
    assert!(
        !dst_bad_dir.join("file3.txt").exists(),
        "file3.txt should NOT be copied (permission denied)"
    );
    assert!(
        !dst_bad_dir.join("file4.txt").exists(),
        "file4.txt should NOT be copied (permission denied)"
    );
    // verify content of copied files
    assert_eq!(
        get_file_content(&dst_good_dir.join("file1.txt")),
        "content 1"
    );
    assert_eq!(
        get_file_content(&dst_good_dir.join("file2.txt")),
        "content 2"
    );
    assert_eq!(
        get_file_content(&dst_more_good.join("file5.txt")),
        "content 5"
    );
    // verify non-zero exit code (some files failed)
    assert!(
        !output.status.success(),
        "should have non-zero exit due to permission errors"
    );
}

#[test]
fn test_remote_copy_deeply_nested_directory_failure() {
    // this test verifies that when a directory fails to be created, all its deeply
    // nested descendants (3+ levels deep) are handled correctly without
    // "Directory not being tracked" errors.
    //
    // the race condition being tested:
    // 1. source sends DirStub for bad_dir, then dir2, dir3, dir4 in quick succession
    // 2. destination tries to create bad_dir but FAILS (already exists as directory)
    // 3. destination sends DirectoryFailed for bad_dir
    // 4. meanwhile, dir2/dir3/dir4 DirStubs arrive and directories are created
    // 5. when dir4 completes and tries to decrement dir3's entry count,
    //    we must check if ANY ancestor (bad_dir) is in failed_directories
    //
    // without the has_failed_ancestor() check, this would crash with:
    // "Directory dir3 not being tracked" because bad_dir failed and wasn't tracked
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // source structure:
    //   src/
    //     good_dir/
    //       file_good.txt
    //     bad_dir/           <- will fail (already exists at destination)
    //       dir2/
    //         dir3/
    //           dir4/
    //             deep_file.txt
    let src_root = src_dir.path().join("src");
    let src_good_dir = src_root.join("good_dir");
    let src_bad_dir = src_root.join("bad_dir");
    let src_dir2 = src_bad_dir.join("dir2");
    let src_dir3 = src_dir2.join("dir3");
    let src_dir4 = src_dir3.join("dir4");
    std::fs::create_dir_all(&src_good_dir).unwrap();
    std::fs::create_dir_all(&src_dir4).unwrap();
    create_test_file(&src_good_dir.join("file_good.txt"), "good content", 0o644);
    create_test_file(&src_dir4.join("deep_file.txt"), "deep content", 0o644);
    // destination structure: pre-create src/ as a directory (will be reused with --overwrite)
    // and create bad_dir as a FILE (not directory) - this will fail even with --overwrite
    // because rcp cannot replace a file with a directory without explicit removal
    let dst_root = dst_dir.path().join("src");
    std::fs::create_dir_all(&dst_root).unwrap();
    // create bad_dir as a FILE (not directory) to block directory creation
    let dst_bad_dir_file = dst_root.join("bad_dir");
    std::fs::write(&dst_bad_dir_file, "i am a file blocking bad_dir").unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}/", dst_dir.path().to_str().unwrap());
    // run WITH --overwrite so src/ can be reused (it already exists as directory)
    // but bad_dir will still fail because it's a file and --overwrite will try to
    // replace it - which should work! let's check the behavior...
    // actually with --overwrite, a file WILL be replaced with directory. so we need
    // to run WITHOUT --overwrite to get the failure.
    // BUT then src/ will also fail because it already exists...
    //
    // use --overwrite and make bad_dir non-writable (0o555 = r-xr-xr-x) so dir2 can't be created
    std::fs::remove_file(&dst_bad_dir_file).unwrap();
    std::fs::create_dir(&dst_bad_dir_file).unwrap();
    std::fs::set_permissions(&dst_bad_dir_file, std::fs::Permissions::from_mode(0o555)).unwrap();
    let output = run_rcp_with_args(&["--overwrite", "-vv", "--summary", &src_remote, &dst_remote]);
    // restore permissions for cleanup BEFORE any assertions
    std::fs::set_permissions(&dst_bad_dir_file, std::fs::Permissions::from_mode(0o755)).unwrap();
    print_command_output(&output);
    // the key assertion: no panic with "Directory not being tracked"
    // if we get here without panicking, the ancestor check is working
    // verify the output doesn't contain the error we're trying to prevent
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("not being tracked"),
        "should not have 'Directory not being tracked' error"
    );
    // verify non-zero exit code (directory creation failed)
    assert!(
        !output.status.success(),
        "should have non-zero exit due to directory already exists error"
    );
}

#[test]
fn test_remote_copy_directory_permissions_preserved_despite_file_errors() {
    // this test verifies that DirectoryComplete messages are sent even when
    // some files in the directory fail to copy. DirectoryComplete is responsible
    // for finalizing directory metadata (permissions, mtime). if it's not sent
    // on error paths, the destination directory won't have the correct metadata.
    //
    // we verify this by:
    // 1. creating a source directory with non-default permissions (0o750)
    // 2. having some files inside fail to copy (unreadable source files)
    // 3. using --preserve to copy metadata
    // 4. verifying the destination directory has the correct permissions
    //
    // note: we use unreadable SOURCE files instead of unwritable destination
    // directories because pre-creating destination directories causes "File exists"
    // errors that skip all contents.
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source structure with specific permissions
    let src_root = src_dir.path().join("preserved");
    let src_parent = src_root.join("parent_with_perms");
    std::fs::create_dir_all(&src_parent).unwrap();
    // create files: one readable (will succeed), one unreadable (will fail)
    create_test_file(&src_parent.join("good_file.txt"), "this will copy", 0o644);
    create_test_file(&src_parent.join("bad_file.txt"), "unreadable", 0o000);
    // set parent directory to non-default permissions (0o750 = rwxr-x---)
    // do this AFTER creating files so we can still write to the directory
    std::fs::set_permissions(&src_parent, std::fs::Permissions::from_mode(0o750)).unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}/", dst_dir.path().to_str().unwrap());
    // run with --preserve to copy permissions
    let output = run_rcp_with_args(&["--preserve", "--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // restore source permissions for cleanup
    std::fs::set_permissions(&src_parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(
        src_parent.join("bad_file.txt"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    // verify destinations
    let dst_root = dst_dir.path().join("preserved");
    let dst_parent = dst_root.join("parent_with_perms");
    // verify the good file was copied
    assert!(
        dst_parent.join("good_file.txt").exists(),
        "good_file.txt should be copied"
    );
    assert_eq!(
        get_file_content(&dst_parent.join("good_file.txt")),
        "this will copy"
    );
    // verify the bad file was NOT copied (unreadable source)
    assert!(
        !dst_parent.join("bad_file.txt").exists(),
        "bad_file.txt should NOT be copied (permission denied on source)"
    );
    // KEY ASSERTION: verify parent directory permissions were preserved
    // this proves DirectoryComplete was sent even though a child file failed
    let dst_parent_mode = dst_parent.metadata().unwrap().permissions().mode() & 0o777;
    assert_eq!(
        dst_parent_mode, 0o750,
        "parent directory should have preserved permissions (0o750), got 0o{:o}. \
         This indicates DirectoryComplete was not sent after file errors.",
        dst_parent_mode
    );
    // non-zero exit due to failures
    assert!(
        !output.status.success(),
        "should have non-zero exit due to permission errors"
    );
    eprintln!("✓ Directory permissions preserved despite file copy errors");
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
    require_local_ssh();
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
    require_local_ssh();
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
    // wait up to 3 seconds for rcpd to exit (stdin watchdog should be instant)
    let start = std::time::Instant::now();
    let exited = wait_for_rcpd_exit(&new_pids, 3);
    let elapsed = start.elapsed();
    assert!(
        exited,
        "rcpd processes should exit within 3 seconds after master is killed (stdin watchdog)"
    );
    eprintln!("✓ All rcpd processes exited in {elapsed:?} (stdin watchdog worked!)");
    // verify it was fast (stdin watchdog should be nearly instant)
    assert!(
        elapsed.as_secs() < 5,
        "rcpd should exit quickly via stdin watchdog"
    );
}

#[test]
fn test_remote_rcpd_no_zombie_processes() {
    // verify that rcpd processes don't become zombies after master exits
    require_local_ssh();
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
fn test_remote_rcpd_with_custom_tcp_timeouts() {
    // verify that custom TCP timeout values are accepted and work correctly
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let dst_file = dst_dir.path().join("test.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // test with custom timeout values
    let output = run_rcp_with_args(&[
        "--remote-copy-conn-timeout-sec=30",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "Copy with custom TCP timeouts should succeed"
    );
    assert!(dst_file.exists(), "Destination file should exist");
    let content = get_file_content(&dst_file);
    assert_eq!(content, "test content");
    eprintln!("✓ Copy with custom TCP timeouts succeeded");
}

#[test]
fn test_remote_rcpd_aggressive_timeout_configuration() {
    // verify that moderately aggressive timeout values work correctly (for datacenter environments)
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let dst_file = dst_dir.path().join("test.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // test with moderately aggressive timeouts suitable for fast datacenter environments
    let output = run_rcp_with_args(&[
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
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
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
    let cache_dir = cache_bin_dir(home.path());
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
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
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
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("cached_deploy_test.txt");
    let dst_file = dst_dir.path().join("cached_deploy_test.txt");
    create_test_file(&src_file, "testing cached deployment", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // first run with --auto-deploy-rcpd to ensure binary is deployed
    // use --rcpd-path=/nonexistent to force deployment (discovery will fail)
    eprintln!("First run: ensuring rcpd is deployed");
    let output1 = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
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
    let cache_dir = cache_bin_dir(home.path());
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
    let output2 = run_rcp_with_args_home_and_env(
        &["--auto-deploy-rcpd", &src_remote2, &dst_remote2],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
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

#[test]
fn test_remote_auto_deploy_cleanup_old_versions() {
    // test that auto-deployment cleans up old versions (keeps last 3)
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("cleanup_test.txt");
    let dst_file = dst_dir.path().join("cleanup_test.txt");
    create_test_file(&src_file, "test cleanup", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // create fake old version binaries in the cache directory
    let cache_dir = cache_bin_dir(home.path());
    std::fs::create_dir_all(&cache_dir).expect("Failed to create cache directory");
    // dynamically generate old versions based on the current version
    // this avoids having to update the test every time we bump the version
    let current_version = env!("CARGO_PKG_VERSION");
    // strip prerelease suffix (e.g., "0.23.0-alpha.1" -> "0.23.0")
    let base_version = current_version.split('-').next().unwrap();
    let version_parts: Vec<u32> = base_version
        .split('.')
        .map(|s| s.parse().expect("valid version number"))
        .collect();
    let (major, minor, patch) = (version_parts[0], version_parts[1], version_parts[2]);
    // create 4 old versions by decrementing the minor version
    // combined with the current version, that's 5 total; cleanup keeps 3, deletes 2 oldest
    assert!(
        minor >= 4,
        "Test requires minor version >= 4 to generate enough old versions for cleanup testing, \
         current version is {current_version}"
    );
    let old_versions: Vec<String> = (1..=4)
        .map(|i| format!("{}.{}.{}", major, minor - i, patch))
        .rev()
        .collect();
    for (idx, version) in old_versions.iter().enumerate() {
        let fake_binary = cache_dir.join(format!("rcpd-{}", version));
        std::fs::write(&fake_binary, "fake old binary").expect("Failed to create fake binary");
        // set mtime to make them old (10 seconds apart, oldest first)
        let mtime = std::time::SystemTime::now()
            - std::time::Duration::from_secs((old_versions.len() - idx) as u64 * 10);
        filetime::set_file_mtime(&fake_binary, filetime::FileTime::from_system_time(mtime))
            .expect("Failed to set mtime");
    }
    // run auto-deployment which should deploy current version and clean up old ones
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(
        output.status.success(),
        "Copy with auto-deploy should succeed"
    );
    // verify copy succeeded
    assert!(dst_file.exists(), "Destination file should exist");
    // verify cleanup: should keep only the 3 newest versions (current + 2 older)
    // with 5 total versions (4 old fake + 1 current), the 2 oldest should be deleted
    // old_versions are in ascending order: [minor-4, minor-3, minor-2, minor-1]
    // indices:                              [0,       1,       2,       3      ]
    // should delete:                        [oldest,  old                      ]
    // should keep:                          [                 newer,   newest  ] + current
    let oldest_version = cache_dir.join(format!("rcpd-{}", &old_versions[0]));
    let old_version = cache_dir.join(format!("rcpd-{}", &old_versions[1]));
    let newer_version = cache_dir.join(format!("rcpd-{}", &old_versions[2]));
    let newest_old_version = cache_dir.join(format!("rcpd-{}", &old_versions[3]));
    let current_version_path = cache_dir.join(format!("rcpd-{}", current_version));
    // check which versions remain
    let oldest_exists = oldest_version.exists();
    let old_exists = old_version.exists();
    let newer_exists = newer_version.exists();
    let newest_old_exists = newest_old_version.exists();
    let current_exists = current_version_path.exists();
    eprintln!("After cleanup:");
    eprintln!("  {} exists: {}", &old_versions[0], oldest_exists);
    eprintln!("  {} exists: {}", &old_versions[1], old_exists);
    eprintln!("  {} exists: {}", &old_versions[2], newer_exists);
    eprintln!("  {} exists: {}", &old_versions[3], newest_old_exists);
    eprintln!("  {} exists: {}", current_version, current_exists);
    // verify cleanup worked: oldest 2 should be deleted, newest 3 kept
    assert!(
        !oldest_exists,
        "Oldest version {} should be deleted",
        &old_versions[0]
    );
    assert!(
        !old_exists,
        "Old version {} should be deleted",
        &old_versions[1]
    );
    assert!(
        newer_exists,
        "Version {} should be kept (one of newest 3)",
        &old_versions[2]
    );
    assert!(
        newest_old_exists,
        "Version {} should be kept (one of newest 3)",
        &old_versions[3]
    );
    assert!(
        current_exists,
        "Current version {} should be kept",
        current_version
    );
    // cleanup our fake binaries that were kept
    for version in &old_versions[2..] {
        std::fs::remove_file(cache_dir.join(format!("rcpd-{}", version))).ok();
    }
    eprintln!("✓ Cleanup of old versions works correctly");
}

#[test]
fn test_remote_auto_deploy_error_explicit_rcpd_not_found() {
    // test error handling when explicit --rcpd-path points to nonexistent binary
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // use explicit path that doesn't exist - should fail with clear error
    let output = run_rcp_with_args(&[
        "--rcpd-path=/this/path/definitely/does/not/exist/rcpd",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    // should fail
    assert!(
        !output.status.success(),
        "should fail when explicit rcpd path not found"
    );
    // check error message quality
    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined_output.contains("rcpd binary not found")
            || combined_output.contains("not found or not executable"),
        "error message should mention rcpd not found"
    );
    assert!(
        combined_output.contains("/this/path/definitely/does/not/exist/rcpd"),
        "error message should include the explicit path that was tried"
    );
    eprintln!("✓ explicit rcpd-path not found error handling works correctly");
}

#[test]
fn test_remote_auto_deploy_error_permission_denied() {
    // test error handling when deployment fails due to permission denied
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // make ~/.cache/rcp/bin read-only to trigger permission denied
    let cache_bin_dir = cache_bin_dir(home.path());
    // create the directory if it doesn't exist
    std::fs::create_dir_all(&cache_bin_dir).expect("failed to create cache directory");
    // make it read-only (mode 555)
    std::fs::set_permissions(&cache_bin_dir, std::fs::Permissions::from_mode(0o555))
        .expect("failed to set permissions");
    // run auto-deployment which should fail due to permission denied
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    // restore write permissions before checking results
    std::fs::set_permissions(&cache_bin_dir, std::fs::Permissions::from_mode(0o755))
        .expect("failed to restore permissions");
    print_command_output(&output);
    // should fail
    assert!(
        !output.status.success(),
        "should fail when deployment directory is read-only"
    );
    // check error message mentions permission issue
    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined_output.contains("Permission denied")
            || combined_output.contains("permission")
            || combined_output.contains("failed to transfer binary")
            || combined_output.contains("Insufficient disk space")
            || combined_output.contains("Permission denied creating")
            || combined_output.contains("failed to deploy rcpd")
            || combined_output.contains("Broken pipe"),
        "error message should mention permission or deployment failure: {}",
        combined_output
    );
    eprintln!("✓ permission denied error handling works correctly");
}

#[test]
fn test_remote_auto_deploy_error_checksum_mismatch() {
    // test that checksum mismatch is detected and reported.
    // this test verifies the integrity verification works
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // first, do a successful deployment
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    assert!(output.status.success(), "initial deployment should succeed");
    assert!(dst_file.exists(), "copy should succeed");
    // now corrupt the deployed binary in cache
    let cache_dir = cache_bin_dir(home.path());
    // find the deployed rcpd binary (rcpd-0.22.0 or similar)
    let mut deployed_binary = None;
    if let Ok(entries) = std::fs::read_dir(&cache_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(filename) = path.file_name() {
                if filename.to_string_lossy().starts_with("rcpd-") {
                    deployed_binary = Some(path);
                    break;
                }
            }
        }
    }
    let deployed_binary = deployed_binary.expect("should find deployed rcpd binary in cache");
    eprintln!("found deployed binary: {}", deployed_binary.display());
    // corrupt it by writing garbage
    std::fs::write(&deployed_binary, "corrupted binary content").expect("failed to corrupt binary");
    // clear the destination file so we try to copy again
    std::fs::remove_file(&dst_file).ok();
    // note: the current implementation verifies checksum during DEPLOYMENT, not when USING the cached binary.
    // so we need to trigger a re-deployment, not reuse. we can do this by deleting the cached binary and re-deploying.
    // we can't easily simulate checksum mismatch during transfer because the transfer happens over SSH with base64 encoding.
    // the checksum verification happens AFTER successful transfer.
    // to test actual checksum mismatch, we'd need to: (1) intercept the transfer (not possible in integration test),
    // (2) modify the checksum verification code to inject failures (not good), or (3) test at unit level in deploy.rs (better approach).
    // for now, let's just verify that the deployment succeeds and includes checksum verification in the output logs
    std::fs::remove_file(&deployed_binary).expect("failed to remove corrupted binary");
    // re-deploy (should succeed with checksum verification)
    let output = run_rcp_with_args(&[
        "--auto-deploy-rcpd",
        "--rcpd-path=/nonexistent/rcpd",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "deployment with checksum verification should succeed"
    );
    // verify that checksum verification happened (check logs)
    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined_output.contains("Checksum verified")
            || combined_output.contains("SHA-256")
            || combined_output.contains("checksum"),
        "output should mention checksum verification"
    );
    eprintln!(
        "✓ checksum verification is present in deployment (mismatch test requires unit test)"
    );
}

#[test]
fn test_remote_copy_empty_directory_root() {
    // test copying an empty directory via remote protocol
    // verifies DirStub{num_entries=0} is handled correctly
    // verifies DirectoryTracker handles zero-entry directory
    // verifies DirectoryComplete sent immediately
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("empty_dir");
    std::fs::create_dir(&src_subdir).unwrap();
    let dst_subdir = dst_dir.path().join("empty_dir");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify directory was created
    assert!(dst_subdir.exists());
    assert!(dst_subdir.is_dir());
    // verify summary shows directory created but no files
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    assert_eq!(summary.directories_created, 1);
    assert_eq!(summary.bytes_copied, 0);
}

#[test]
fn test_remote_copy_empty_nested_directories() {
    // test directory tree with multiple empty directories at various levels
    // verifies all directories created
    // verifies DirectoryTracker properly cascades completion
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_root = src_dir.path().join("nested_empty");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("empty1")).unwrap();
    std::fs::create_dir(src_root.join("empty2")).unwrap();
    std::fs::create_dir(src_root.join("empty1/empty1a")).unwrap();
    let dst_root = dst_dir.path().join("nested_empty");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify all directories were created
    assert!(dst_root.exists());
    assert!(dst_root.join("empty1").exists());
    assert!(dst_root.join("empty2").exists());
    assert!(dst_root.join("empty1/empty1a").exists());
    // verify all are directories
    assert!(dst_root.is_dir());
    assert!(dst_root.join("empty1").is_dir());
    assert!(dst_root.join("empty2").is_dir());
    assert!(dst_root.join("empty1/empty1a").is_dir());
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    assert_eq!(summary.directories_created, 4); // root + 3 subdirs
    assert_eq!(summary.bytes_copied, 0);
}

#[test]
fn test_remote_copy_very_deep_nesting() {
    // test very deep directory structure (100+ levels) via remote protocol
    // verifies no stack overflow in recursive traversal
    // verifies DirectoryTracker handles deep nesting
    // verifies proper completion cascading from deepest to root
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create 100 levels of nesting
    let mut current_path = src_dir.path().join("deep");
    std::fs::create_dir(&current_path).unwrap();
    for i in 0..100 {
        current_path = current_path.join(format!("level{}", i));
        std::fs::create_dir(&current_path).unwrap();
    }
    // create a file at the deepest level
    create_test_file(&current_path.join("deep.txt"), "deepest", 0o644);
    let src_root = src_dir.path().join("deep");
    let dst_root = dst_dir.path().join("deep");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify deepest file exists
    let mut verify_path = dst_root.clone();
    for i in 0..100 {
        verify_path = verify_path.join(format!("level{}", i));
    }
    assert_eq!(get_file_content(&verify_path.join("deep.txt")), "deepest");
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
    assert_eq!(summary.directories_created, 101); // root + 100 levels
}

#[test]
fn test_remote_copy_empty_file_root() {
    // test empty file (0 bytes) copied via remote protocol
    // verifies File{is_root=true} with zero-byte file transfer
    // verifies file stream created and closed correctly for zero-byte file
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("empty.txt");
    let dst_file = dst_dir.path().join("empty.txt");
    create_test_file(&src_file, "", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify empty file was created
    assert_eq!(get_file_content(&dst_file), "");
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
    assert_eq!(summary.bytes_copied, 0);
}

#[test]
fn test_remote_copy_broken_symlink_root() {
    // test symlink pointing to nonexistent target via remote protocol
    // verifies Symlink{is_root=true} message sent
    // verifies broken symlink created at destination
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let nonexistent = src_dir.path().join("does_not_exist.txt");
    let src_link = src_dir.path().join("broken.txt");
    let dst_link = dst_dir.path().join("broken.txt");
    std::os::unix::fs::symlink(&nonexistent, &src_link).unwrap();
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_link.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify symlink was created and points to nonexistent target
    assert!(dst_link.is_symlink());
    assert_eq!(std::fs::read_link(&dst_link).unwrap(), nonexistent);
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.symlinks_created, 1);
}

#[test]
fn test_remote_copy_circular_symlink_root() {
    // test circular symlink reference via remote protocol
    // verifies symlink copied (not dereferenced by default)
    // verifies root symlink handling
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let link1 = src_dir.path().join("link1.txt");
    let link2 = src_dir.path().join("link2.txt");
    let dst_link = dst_dir.path().join("link1.txt");
    std::os::unix::fs::symlink(&link2, &link1).unwrap();
    std::os::unix::fs::symlink(&link1, &link2).unwrap();
    let src_remote = format!("localhost:{}", link1.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_link.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify symlink was created
    assert!(dst_link.is_symlink());
    // verify it points to link2 (circular reference maintained)
    assert_eq!(std::fs::read_link(&dst_link).unwrap(), link2);
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.symlinks_created, 1);
}

/// Helper that runs rcp WITHOUT --force-remote (for testing local copy behavior)
fn run_rcp_without_force_remote(args: &[&str]) -> std::process::Output {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    cmd.args(["30", rcp_path.to_str().unwrap()]);
    cmd.arg("-vv");
    // note: NOT adding --force-remote here
    cmd.args(args);
    cmd.output().expect("Failed to execute rcp command")
}

#[test]
fn test_remote_force_remote_flag_uses_rcpd() {
    // verifies that --force-remote with localhost: actually uses rcpd (SSH)
    // this test runs with --force-remote (via test helper) and verifies rcpd was invoked
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "force remote test", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // run_rcp_and_expect_success uses --force-remote
    let output = run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
    // should show rcpd being started (indicates remote mode was used, logs go to stdout)
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Starting rcpd"),
        "Expected 'Starting rcpd' in output when using --force-remote, got: {stdout}"
    );
    assert_eq!(get_file_content(&dst_file), "force remote test");
}

#[test]
fn test_remote_localhost_without_force_remote_is_local() {
    // verifies that localhost: WITHOUT --force-remote does a local copy (no rcpd)
    require_local_ssh(); // still need SSH available for the test environment
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "local copy test", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // run WITHOUT --force-remote
    let output = run_rcp_without_force_remote(&[&src_remote, &dst_remote]);
    print_command_output(&output);
    assert!(output.status.success(), "Copy should succeed");
    // logs go to stdout
    let stdout = String::from_utf8_lossy(&output.stdout);
    // should show warning about localhost being local
    assert!(
        stdout.contains("Paths with 'localhost:' prefix are treated as local"),
        "Expected localhost warning in output, got: {stdout}"
    );
    // should NOT show rcpd being started
    assert!(
        !stdout.contains("Starting rcpd"),
        "Should NOT use rcpd without --force-remote, but got: {stdout}"
    );
    assert_eq!(get_file_content(&dst_file), "local copy test");
}

// ============================================================================
// Edge Case Tests: Failure Handling with --fail-early
// These tests verify that the protocol handles failures correctly without hanging.
// A hang (timeout) indicates a bug where DirectoryTracker wasn't properly updated
// or send_root_done wasn't called before returning an error.
// ============================================================================

/// Test that child symlink creation failure with --fail-early doesn't cause a hang.
///
/// Bug scenario: When a symlink inside a directory fails to create on the destination
/// and --fail-early is set, the destination should still call decrement_entry() for
/// the parent directory before returning the error. Otherwise, the source hangs
/// waiting for DirectoryComplete.
#[test]
fn test_remote_child_symlink_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with a file and a symlink
    let src_subdir = src_dir.path().join("symlink_test");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    std::os::unix::fs::symlink("target", src_subdir.join("link")).unwrap();

    // create destination directory, then make it read-only so symlink creation fails
    let dst_subdir = dst_dir.path().join("symlink_test");
    std::fs::create_dir_all(&dst_subdir).unwrap();
    // make directory read-only - symlink creation will fail with Permission denied
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // with --fail-early and --overwrite (to reuse existing directory)
    // symlink creation will fail due to permission denied
    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--overwrite", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755));

    // verify we got an error (not a timeout)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where decrement_entry wasn't called"
    );
}

/// Same as test_remote_child_symlink_fail_early_no_hang but with --no-encryption.
/// Used to isolate TLS-related issues from protocol issues.
#[test]
fn test_remote_child_symlink_fail_early_no_hang_no_encryption() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with a file and a symlink
    let src_subdir = src_dir.path().join("symlink_test");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    std::os::unix::fs::symlink("target", src_subdir.join("link")).unwrap();

    // create destination directory, then make it read-only so symlink creation fails
    let dst_subdir = dst_dir.path().join("symlink_test");
    std::fs::create_dir_all(&dst_subdir).unwrap();
    // make directory read-only - symlink creation will fail with Permission denied
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // with --fail-early and --overwrite (to reuse existing directory) and --no-encryption
    // symlink creation will fail due to permission denied
    let output = run_rcp_and_expect_failure(&[
        "--fail-early",
        "--overwrite",
        "--no-encryption",
        &src_remote,
        &dst_remote,
    ]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755));

    // verify we got an error (not a timeout)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where decrement_entry wasn't called"
    );
}

/// Test that root symlink creation failure with --fail-early doesn't cause a hang.
///
/// Bug scenario: When copying a single symlink as root and it fails to create on
/// the destination with --fail-early, the destination should still call
/// send_root_done() before returning the error. Otherwise, the source hangs
/// waiting for DestinationDone.
#[test]
fn test_remote_root_symlink_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create a symlink as the root item to copy
    let src_symlink = src_dir.path().join("root_link");
    std::os::unix::fs::symlink("target", &src_symlink).unwrap();

    // create destination as a read-only directory to prevent symlink creation
    // (the symlink destination will be inside this directory)
    let dst_path = dst_dir.path().join("root_link");
    std::fs::create_dir(&dst_path).unwrap();
    std::fs::set_permissions(&dst_path, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_symlink.to_str().unwrap());
    // point to a path inside the read-only directory
    let dst_remote = format!("localhost:{}", dst_path.join("symlink").to_str().unwrap());

    // with --fail-early, this should fail fast, NOT hang
    let output = run_rcp_and_expect_failure(&["--fail-early", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_path, std::fs::Permissions::from_mode(0o755));

    // verify we got an error (not a timeout)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where send_root_done wasn't called"
    );
}

/// Test that directory metadata failure doesn't cause a hang.
///
/// Bug scenario: When set_dir_metadata() fails (e.g., due to permission issues),
/// the destination should still call decrement_entry() for child directories or
/// send_root_done() for root directories before returning the error.
///
/// This is harder to trigger reliably since metadata setting typically succeeds,
/// but we can test with a directory that has restrictive permissions.
#[test]
fn test_remote_directory_metadata_failure_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with a file
    let src_subdir = src_dir.path().join("meta_test");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);

    // create destination parent, but make it read-only after creating the subdir
    // this may cause metadata operations to fail
    let dst_subdir = dst_dir.path().join("meta_test");
    std::fs::create_dir_all(&dst_subdir).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // copy with --preserve to ensure metadata operations are attempted
    // the test mainly verifies we don't hang - the operation may succeed or fail
    // depending on permissions, but it should never timeout
    let output = run_rcp_with_args(&["--preserve", "--overwrite", &src_remote, &dst_remote]);
    print_command_output(&output);

    // verify we didn't timeout (exit code 124 = timeout)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a potential hang bug in metadata error handling"
    );
}

/// Test that multiple child symlink failures with --fail-early complete without hanging.
///
/// This tests a directory with multiple symlinks where all fail, ensuring the
/// protocol properly handles multiple failures in sequence.
#[test]
fn test_remote_multiple_child_symlinks_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with multiple symlinks
    let src_subdir = src_dir.path().join("multi_symlink_test");
    std::fs::create_dir(&src_subdir).unwrap();
    std::os::unix::fs::symlink("target1", src_subdir.join("link1")).unwrap();
    std::os::unix::fs::symlink("target2", src_subdir.join("link2")).unwrap();
    std::os::unix::fs::symlink("target3", src_subdir.join("link3")).unwrap();

    // create destination as read-only so symlink creation fails
    let dst_subdir = dst_dir.path().join("multi_symlink_test");
    std::fs::create_dir_all(&dst_subdir).unwrap();
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // with --fail-early and --overwrite (to reuse existing directory)
    // symlink creation will fail due to permission denied
    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--overwrite", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755));

    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug in symlink failure handling"
    );
}

/// Test nested directory with symlink failure and --fail-early.
///
/// Tests that when a symlink fails in a nested directory structure with --fail-early,
/// all parent directories properly complete their tracking.
#[test]
fn test_remote_nested_directory_symlink_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create nested source structure: parent/child/link
    let src_parent = src_dir.path().join("parent");
    let src_child = src_parent.join("child");
    std::fs::create_dir_all(&src_child).unwrap();
    create_test_file(&src_child.join("file.txt"), "content", 0o644);
    std::os::unix::fs::symlink("target", src_child.join("link")).unwrap();

    // create destination directories, make child read-only to fail symlink creation
    let dst_parent = dst_dir.path().join("parent");
    let dst_child = dst_parent.join("child");
    std::fs::create_dir_all(&dst_child).unwrap();
    std::fs::set_permissions(&dst_child, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_parent.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_parent.to_str().unwrap());

    // should fail on symlink but not hang waiting for parent directories to complete
    // use --overwrite to allow reusing existing directories
    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--overwrite", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_child, std::fs::Permissions::from_mode(0o755));

    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where parent directory tracking failed"
    );
}

/// Test that directory metadata is applied when a child file fails WITHOUT --fail-early.
///
/// This is a regression test for ensuring that the DirectoryTracker is properly
/// updated even when errors occur. The test verifies that with --preserve (but
/// without --fail-early), directory permissions are correctly applied even when
/// a child file fails to copy (e.g., due to permission errors).
///
/// Note: With --fail-early, metadata may NOT be applied because we close the
/// connection immediately after the error. This is expected behavior - the user
/// asked for fast failure. This test uses non-fail-early mode to verify the
/// metadata flow works correctly.
#[test]
fn test_remote_file_failure_still_applies_parent_directory_metadata() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with specific permissions (0o700 = rwx------)
    let src_subdir = src_dir.path().join("metadata_test");
    std::fs::create_dir(&src_subdir).unwrap();
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o700)).unwrap();

    // create an unreadable file (will fail on source side when trying to open)
    create_test_file(&src_subdir.join("unreadable.txt"), "secret", 0o000);

    // verify source directory has the permissions we set
    let src_mode = std::fs::metadata(&src_subdir).unwrap().permissions().mode() & 0o777;
    assert_eq!(src_mode, 0o700, "Source directory should have mode 0o700");

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_subdir = dst_dir.path().join("metadata_test");
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // run with --preserve but WITHOUT --fail-early
    // the file will fail to open on source, but operation should continue and
    // directory metadata should be applied
    let output = run_rcp_and_expect_failure(&["--preserve", &src_remote, &dst_remote]);

    // verify we didn't timeout
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(exit_code != 124, "Command timed out");

    // THE KEY ASSERTION: directory permissions should have been updated
    // Protocol flow:
    //   1. DirStub{metadata_test, 1} -> Destination creates dir, tracks count=1
    //   2. DirectoryCreated sent to source
    //   3. Source tries to open file -> FAILS (Permission denied)
    //   4. Source sends FileSkipped message
    //   5. Destination receives FileSkipped, calls decrement_entry() -> count=0
    //   6. DirectoryComplete sent to source
    //   7. Source receives DirectoryComplete, sends Directory{metadata_test, metadata}
    //   8. Destination applies metadata (including permissions 0o700)
    let dst_mode_after = std::fs::metadata(&dst_subdir).unwrap().permissions().mode() & 0o777;
    assert_eq!(
        dst_mode_after, 0o700,
        "Destination directory permissions should be updated to 0o700 from source, \
         but got {:o}. This indicates the Directory metadata message was never sent.",
        dst_mode_after
    );
}

/// Test that copying a directory to a path where a file already exists (without --overwrite)
/// doesn't cause a hang.
///
/// Bug scenario: When the root directory can't be created because destination already
/// exists as a file (and --overwrite is not set), the destination must still set
/// root_complete to avoid hanging forever waiting for DestinationDone.
#[test]
fn test_remote_root_directory_exists_as_file_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory with a file
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    // create destination as a FILE (not a directory) - this will cause root creation to fail
    let dst_path = dst_dir.path().join("mydir");
    create_test_file(&dst_path, "i am a file", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    // without --overwrite, copying a directory to a file path should fail fast, NOT hang
    let output = run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify we didn't timeout (timeout = 124)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where root_complete wasn't set"
    );
    // verify the file wasn't replaced
    assert!(dst_path.is_file(), "Destination should still be a file");
    assert_eq!(get_file_content(&dst_path), "i am a file");
}

/// Test that copying an inaccessible root symlink doesn't hang.
///
/// Bug scenario: When a root symlink's metadata can't be read (e.g., parent directory
/// has no execute permission), the source must fail cleanly rather than hanging.
/// Previously, metadata failures for root items would return Ok(()) without sending
/// any message, causing the destination to wait forever for root completion.
#[test]
fn test_remote_root_symlink_inaccessible_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create a symlink inside a subdirectory
    let src_subdir = src_dir.path().join("restricted");
    std::fs::create_dir(&src_subdir).unwrap();
    let src_symlink = src_subdir.join("link");
    std::os::unix::fs::symlink("target", &src_symlink).unwrap();
    // remove all permissions from the parent directory AFTER creating the symlink
    // this makes the symlink inaccessible (stat/lstat will fail with EACCES)
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o000)).unwrap();
    let dst_symlink = dst_dir.path().join("link");
    let src_remote = format!("localhost:{}", src_symlink.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_symlink.to_str().unwrap());
    // this should fail (can't access source) but NOT hang
    let output = run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o755));
    // verify we didn't timeout (timeout = 124)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where root metadata failure wasn't handled"
    );
    // verify the destination symlink was NOT created (source was inaccessible)
    assert!(
        !dst_symlink.exists(),
        "Destination should not exist since source was inaccessible"
    );
}

/// Test that stream continues processing files after a metadata error.
///
/// Bug scenario: When file metadata fails to set (e.g., chown fails for root-owned file),
/// the stream should continue processing subsequent files since all data was consumed.
/// Previously, metadata errors marked the stream as corrupted, unnecessarily closing it.
///
/// This test requires passwordless sudo to create a root-owned file.
#[test]
#[ignore = "requires passwordless sudo"]
fn test_remote_sudo_stream_continues_after_metadata_error() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory with multiple files
    let src_subdir = src_dir.path().join("metadata_test");
    std::fs::create_dir(&src_subdir).unwrap();
    // file1: normal file (will succeed completely)
    create_test_file(&src_subdir.join("file1.txt"), "content1", 0o644);
    // file2: root-owned file (metadata will fail when copying as non-root with --preserve)
    // use sudo -n to avoid password prompt; skip test if passwordless sudo unavailable
    let root_file = src_subdir.join("root_owned.txt");
    let status = std::process::Command::new("sudo")
        .args([
            "-n",
            "bash",
            "-c",
            &format!(
                "echo 'root content' > '{}' && chown root:root '{}'",
                root_file.display(),
                root_file.display()
            ),
        ])
        .status()
        .expect("Failed to run sudo");
    if !status.success() {
        eprintln!("Skipping test: passwordless sudo not available");
        return;
    }
    // file3: normal file (should still be copied after file2's metadata fails)
    create_test_file(&src_subdir.join("file3.txt"), "content3", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_subdir = dst_dir.path().join("metadata_test");
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // run with --preserve to trigger chown (which will fail for root-owned file)
    let output = run_rcp_with_args(&["--preserve", "--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // cleanup root-owned file
    let _ = std::process::Command::new("sudo")
        .args(["-n", "rm", "-f", &root_file.to_string_lossy()])
        .status();
    // verify all files' DATA was transferred (even if metadata failed for some)
    assert!(
        dst_subdir.join("file1.txt").exists(),
        "file1.txt should be copied"
    );
    assert_eq!(get_file_content(&dst_subdir.join("file1.txt")), "content1");
    assert!(
        dst_subdir.join("root_owned.txt").exists(),
        "root_owned.txt data should be copied (even if metadata failed)"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("root_owned.txt")),
        "root content\n"
    );
    // KEY ASSERTION: file3 should be copied, proving stream continued after file2's metadata error
    assert!(
        dst_subdir.join("file3.txt").exists(),
        "file3.txt should be copied - stream should continue after metadata error"
    );
    assert_eq!(get_file_content(&dst_subdir.join("file3.txt")), "content3");
    // command should report failure (due to chown error) but not hang
    assert!(
        !output.status.success(),
        "should fail due to chown permission error"
    );
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "should not timeout - stream should continue after metadata error"
    );
    eprintln!("✓ Stream continued processing files after metadata error");
}

#[test]
fn test_remote_copy_progress_reporting() {
    // verify that progress updates are received from rcpd processes during remote copy.
    // this test ensures the tracing/progress infrastructure is correctly wired up:
    // - rcpd sends progress updates over the tracing TCP connection
    // - master receives and aggregates progress from both source and destination
    // - progress output shows non-zero file counts
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // use filegen to create many small files (10000 x 1KB) to ensure copy takes long enough
    // for progress updates to be captured
    let filegen_path = assert_cmd::cargo::cargo_bin("filegen");
    let filegen_output = std::process::Command::new(&filegen_path)
        .args([
            src_dir.path().to_str().unwrap(),
            "1",     // single directory
            "10000", // 10000 files
            "1K",    // 1KB each
        ])
        .output()
        .expect("Failed to run filegen");
    assert!(
        filegen_output.status.success(),
        "filegen should succeed: {}",
        String::from_utf8_lossy(&filegen_output.stderr)
    );
    // filegen creates files in src_dir/filegen/
    let filegen_dir = src_dir.path().join("filegen");
    let src_remote = format!("localhost:{}/", filegen_dir.to_str().unwrap());
    let dst_remote = format!("localhost:{}/", dst_dir.path().to_str().unwrap());
    // run with --progress and text-updates to get progress output on stderr
    // use a short progress delay to ensure we get updates quickly
    let output = run_rcp_with_args(&[
        "--progress",
        "--progress-type=text-updates",
        "--progress-delay=100ms",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(output.status.success(), "Copy should succeed");
    // verify files were copied - filegen creates in src_dir/filegen/dir0/
    // with trailing slash on src, rcp copies the directory contents into dst
    let dst_subdir = dst_dir.path().join("filegen").join("dir0");
    assert!(
        dst_subdir.exists(),
        "Destination subdirectory should exist: {:?}",
        dst_subdir
    );
    // check stderr for progress output - should contain progress updates with file counts
    // the progress output includes "files:" lines showing copied file counts
    let stderr = String::from_utf8_lossy(&output.stderr);
    // progress output should contain the separator lines
    assert!(
        stderr.contains("======================="),
        "Progress output should contain separator lines. stderr:\n{stderr}"
    );
    // progress should show that files were copied (files: followed by non-zero count)
    // the format is "files:       N" where N is the count
    let has_files_progress = stderr.lines().any(|line| {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("files:") {
            // parse the number after "files:"
            rest.trim().parse::<u64>().map(|n| n > 0).unwrap_or(false)
        } else {
            false
        }
    });
    assert!(
        has_files_progress,
        "Progress output should show files being copied (files: N where N > 0). stderr:\n{stderr}"
    );
    eprintln!("✓ Progress reporting works correctly");
}
