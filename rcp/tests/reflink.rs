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
