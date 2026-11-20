//! Reflink policy parsing and local data-copy behavior.

use assert_cmd::Command;

const REFLINK_POLICIES: [(&str, Option<&str>); 3] = [
    ("default", None),
    ("auto", Some("--reflink=auto")),
    ("never", Some("--reflink=never")),
];

async fn source_file(dir: &std::path::Path) -> std::path::PathBuf {
    static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
        std::sync::LazyLock::new(common::progress::Progress::new);
    let path = dir.join("source");
    common::filegen::write_file(&PROGRESS, path.clone(), 128 * 1024 + 17, 4096, 0)
        .await
        .unwrap();
    path
}

#[tokio::test]
async fn copies_file_with_default_auto_and_never_reflink_policies() {
    let tmp = tempfile::tempdir().unwrap();
    let src = source_file(tmp.path()).await;
    let expected = std::fs::read(&src).unwrap();
    for (name, policy_arg) in REFLINK_POLICIES {
        let dst = tmp.path().join(name);
        let mut cmd = Command::cargo_bin("rcp").unwrap();
        cmd.args(policy_arg).arg(&src).arg(&dst).assert().success();
        assert_eq!(std::fs::read(dst).unwrap(), expected);
    }
}

#[tokio::test]
async fn rejects_unsupported_reflink_policies_before_copying() {
    let tmp = tempfile::tempdir().unwrap();
    let src = source_file(tmp.path()).await;
    let dst = tmp.path().join("destination");
    for policy in ["always", "invalid"] {
        Command::cargo_bin("rcp")
            .unwrap()
            .arg(format!("--reflink={policy}"))
            .arg(&src)
            .arg(&dst)
            .assert()
            .code(2)
            .stderr(predicates::str::contains("possible values: auto, never"));
        assert!(!dst.exists());
    }
}

#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot provide strace")]
#[tokio::test]
async fn copies_without_copy_file_range_when_reflink_is_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let src = source_file(tmp.path()).await;
    let expected = std::fs::read(&src).unwrap();
    for (name, policy_arg) in REFLINK_POLICIES {
        let dst = tmp.path().join(name);
        let mut cmd = std::process::Command::new("strace");
        cmd.args(["-f", "-e", "trace=copy_file_range"])
            .arg(assert_cmd::cargo::cargo_bin("rcp"));
        let output = cmd
            .args(policy_arg)
            .arg(&src)
            .arg(&dst)
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "cannot run strace: {err:#}. This test checks that disabling reflink bypasses \
                     copy_file_range in the worker threads; matching copied bytes cannot prove \
                     that. Install strace."
                )
            });
        let trace = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "traced rcp with {name} reflink policy failed: {}\n{trace}",
            output.status
        );
        // the default and explicit auto runs prove that tracing sees the worker threads.
        assert_eq!(
            trace.contains("copy_file_range("),
            name != "never",
            "unexpected copy_file_range use with {name} reflink policy:\n{trace}"
        );
        assert_eq!(std::fs::read(dst).unwrap(), expected);
    }
}

#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot provide strace")]
#[tokio::test]
async fn copies_files_without_resetting_their_cursors() {
    static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
        std::sync::LazyLock::new(common::progress::Progress::new);
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("source");
    let dst = tmp.path().join("destination");
    std::fs::create_dir(&src).unwrap();
    for i in 0..16 {
        common::filegen::write_file(&PROGRESS, src.join(format!("file-{i}")), 1024, 1024, 0)
            .await
            .unwrap();
    }
    let trace_path = tmp.path().join("syscalls");
    let output = std::process::Command::new("strace")
        .args(["-f", "-yy", "-e", "trace=lseek,copy_file_range", "-o"])
        .arg(&trace_path)
        .arg(assert_cmd::cargo::cargo_bin("rcp"))
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap_or_else(|err| panic!("cannot run strace: {err:#}. Install strace."));
    let trace = std::fs::read_to_string(trace_path).unwrap();
    assert!(
        output.status.success(),
        "traced rcp failed: {}\n{}\n{trace}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    // -yy identifies regular fixture fds, excluding unrelated directory/runtime seeks.
    // observing the worker's copy attempt proves that tracing includes its syscalls.
    assert!(
        trace.contains("copy_file_range("),
        "missing copy calls:\n{trace}"
    );
    assert!(
        trace.contains("/source/file-"),
        "missing fd paths:\n{trace}"
    );
    assert!(
        !trace.lines().any(|line| {
            line.contains("lseek(")
                && line.contains("SEEK_SET")
                && (line.contains("/source/file-") || line.contains("/destination/file-"))
        }),
        "copying regular files must not reset their cursors:\n{trace}"
    );
    for i in 0..16 {
        let name = format!("file-{i}");
        assert_eq!(
            std::fs::read(dst.join(&name)).unwrap(),
            std::fs::read(src.join(&name)).unwrap()
        );
    }
}

#[tokio::test]
async fn local_copy_handoff_policies_preserve_file_contents_and_mode() {
    use std::os::unix::fs::PermissionsExt as _;
    let tmp = tempfile::tempdir().unwrap();
    let src = source_file(tmp.path()).await;
    std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o751)).unwrap();
    let expected = std::fs::read(&src).unwrap();
    for (index, policy) in [None, Some("never"), Some("fresh")].into_iter().enumerate() {
        let dst = tmp.path().join(format!("handoff-{index}"));
        let mut cmd = Command::cargo_bin("rcp").unwrap();
        if let Some(policy) = policy {
            cmd.arg(format!("--local-copy-handoff={policy}"));
        }
        cmd.args(["--reflink=never", "--preserve-settings=all"])
            .arg(&src)
            .arg(&dst)
            .assert()
            .success();
        assert_eq!(std::fs::read(&dst).unwrap(), expected);
        assert_eq!(
            std::fs::metadata(&dst).unwrap().permissions().mode() & 0o7777,
            0o751
        );
    }
}

#[tokio::test]
async fn local_copy_handoff_rejects_unknown_policy_before_copying() {
    let tmp = tempfile::tempdir().unwrap();
    let src = source_file(tmp.path()).await;
    let dst = tmp.path().join("destination");
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--local-copy-handoff=always")
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(2)
        .stderr(predicates::str::contains("possible values: never, fresh"));
    assert!(!dst.exists());
}

/// The limits the tests ask for, with the bytes each must come to.
const RETENTION_LIMITS: [(Option<&str>, usize); 5] = [
    (None, 1024 * 1024),
    (Some("0"), 0),
    (Some("4KiB"), 4096),
    (Some("1MiB"), 1024 * 1024),
    (Some("16MiB"), 16 * 1024 * 1024),
];

#[tokio::test]
async fn copies_with_the_buffer_retention_limit_of_the_command_line() {
    let tmp = tempfile::tempdir().unwrap();
    // several files, so that a kept buffer is reused and a limit below the file size is exceeded
    let src = tmp.path().join("tree");
    std::fs::create_dir(&src).unwrap();
    let file = source_file(tmp.path()).await;
    for name in ["a", "b", "c"] {
        std::fs::copy(&file, src.join(name)).unwrap();
    }
    let expected = std::fs::read(&file).unwrap();
    for (index, (limit, bytes)) in RETENTION_LIMITS.into_iter().enumerate() {
        let dst = tmp.path().join(format!("kept-{index}"));
        let mut cmd = Command::cargo_bin("rcp").unwrap();
        if let Some(limit) = limit {
            cmd.arg(format!("--local-copy-buffer-retention-limit={limit}"));
        }
        let output = cmd
            .args(["-vv", "--reflink=never"])
            .arg(&src)
            .arg(&dst)
            .output()
            .unwrap();
        assert!(output.status.success(), "{limit:?}: {output:?}");
        // the settings the copy runs with are logged at debug level. the command line is logged
        // as well, with the same field in it, so only the settings line counts
        let stdout = String::from_utf8(output.stdout).unwrap();
        let settings = stdout
            .lines()
            .find(|line| line.contains("copy settings: Settings {"))
            .expect("the copy settings must be logged at debug level");
        assert!(
            settings.contains(&format!(
                "local_copy_buffer_retention_limit: BufferRetentionLimit({bytes})"
            )),
            "{limit:?}: {settings}"
        );
        for name in ["a", "b", "c"] {
            assert_eq!(
                std::fs::read(dst.join(name)).unwrap(),
                expected,
                "{limit:?} {name}"
            );
        }
    }
}

#[tokio::test]
async fn rejects_a_buffer_retention_limit_that_is_not_a_size_before_copying() {
    let tmp = tempfile::tempdir().unwrap();
    let src = source_file(tmp.path()).await;
    let dst = tmp.path().join("destination");
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--local-copy-buffer-retention-limit=plenty")
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(2)
        .stderr(predicates::str::contains(
            "--local-copy-buffer-retention-limit",
        ));
    assert!(!dst.exists());
}
