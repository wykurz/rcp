//! Reflink policy parsing and copying changed files during updates.

use assert_cmd::Command;
use std::os::unix::fs::MetadataExt;

const REFLINK_POLICIES: [(&str, Option<&str>); 3] = [
    ("default", None),
    ("auto", Some("--reflink=auto")),
    ("never", Some("--reflink=never")),
];

async fn update_tree(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
        std::sync::LazyLock::new(common::progress::Progress::new);
    let src = dir.join("source");
    let update = dir.join("update");
    for root in [&src, &update] {
        std::fs::create_dir(root).unwrap();
        common::filegen::write_file(&PROGRESS, root.join("unchanged"), 4096, 4096, 0)
            .await
            .unwrap();
    }
    common::filegen::write_file(&PROGRESS, src.join("changed"), 4096, 4096, 0)
        .await
        .unwrap();
    common::filegen::write_file(&PROGRESS, update.join("changed"), 128 * 1024 + 17, 4096, 0)
        .await
        .unwrap();
    (src, update)
}

fn update_command(
    src: &std::path::Path,
    update: &std::path::Path,
    dst: &std::path::Path,
    policy_arg: Option<&str>,
) -> std::process::Command {
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin("rlink"));
    cmd.args(policy_arg)
        .args(["--update-compare=size", "--update"])
        .arg(update)
        .arg(src)
        .arg(dst);
    cmd
}

fn inode(path: &std::path::Path) -> (u64, u64) {
    let metadata = std::fs::metadata(path).unwrap();
    (metadata.dev(), metadata.ino())
}

fn assert_updated_tree(src: &std::path::Path, update: &std::path::Path, dst: &std::path::Path) {
    assert_eq!(
        std::fs::read(dst.join("changed")).unwrap(),
        std::fs::read(update.join("changed")).unwrap()
    );
    assert_ne!(inode(&dst.join("changed")), inode(&src.join("changed")));
    assert_ne!(inode(&dst.join("changed")), inode(&update.join("changed")));
    assert_eq!(inode(&dst.join("unchanged")), inode(&src.join("unchanged")));
}

#[tokio::test]
async fn copies_changed_files_and_links_unchanged_files_with_each_reflink_policy() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, update) = update_tree(tmp.path()).await;
    for (name, policy_arg) in REFLINK_POLICIES {
        let dst = tmp.path().join(name);
        Command::from_std(update_command(&src, &update, &dst, policy_arg))
            .assert()
            .success();
        assert_updated_tree(&src, &update, &dst);
    }
}

#[tokio::test]
async fn rejects_unsupported_reflink_policies_before_updating() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, update) = update_tree(tmp.path()).await;
    let dst = tmp.path().join("destination");
    for policy_arg in ["--reflink=always", "--reflink=invalid"] {
        Command::from_std(update_command(&src, &update, &dst, Some(policy_arg)))
            .assert()
            .code(2)
            .stderr(predicates::str::contains("possible values: auto, never"));
        assert!(!dst.exists());
    }
}

#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot provide strace")]
#[tokio::test]
async fn copies_updates_without_copy_file_range_when_reflink_is_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, update) = update_tree(tmp.path()).await;
    for (name, policy_arg) in REFLINK_POLICIES {
        let dst = tmp.path().join(name);
        let cmd = update_command(&src, &update, &dst, policy_arg);
        let output = std::process::Command::new("strace")
            .args(["-f", "-e", "trace=copy_file_range"])
            .arg(cmd.get_program())
            .args(cmd.get_args())
            .output()
            .unwrap_or_else(|err| {
                panic!(
                    "cannot run strace: {err:#}. This test checks that disabling reflink bypasses \
                     copy_file_range in the update worker threads; matching copied bytes cannot \
                     prove that. Install strace."
                )
            });
        let trace = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "traced rlink with {name} reflink policy failed: {}\n{trace}",
            output.status
        );
        // the default and explicit auto runs prove that tracing sees the worker threads.
        assert_eq!(
            trace.contains("copy_file_range("),
            name != "never",
            "unexpected copy_file_range use with {name} reflink policy:\n{trace}"
        );
        assert_updated_tree(&src, &update, &dst);
    }
}
