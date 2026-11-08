//! CLI regressions for overlap checks and multiple source operands.

#[path = "../../common/tests/support/mounts.rs"]
mod mounts;

fn input_fixture(path: &std::path::Path, size: usize) -> anyhow::Result<Vec<u8>> {
    std::fs::create_dir_all(path)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut config = common::filegen::FileGenConfig::new(path, vec![], 1, size);
    config.leaf_files = true;
    runtime.block_on(common::filegen::filegen(common::get_progress(), &config))?;
    Ok(std::fs::read(path.join("file0"))?)
}

fn overwrite_operands(
    first: &std::path::Path,
    second: &std::path::Path,
    destination: &std::path::Path,
    strict: bool,
) -> std::io::Result<std::process::Output> {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_rcp"));
    command.args(["--overwrite", "--max-workers=2"]);
    if strict {
        command.arg("--require-toctou-safe");
    }
    command
        .arg(first)
        .arg(second)
        .arg(format!("{}/", destination.display()))
        .output()
}

fn assert_input_error(output: &std::process::Output) {
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        message.contains("overlap") || message.contains("input"),
        "expected input protection error: {output:?}",
    );
}

#[test]
fn overwrite_preserves_a_later_directory_operand() -> anyhow::Result<()> {
    for strict in [false, true] {
        let root = tempfile::tempdir()?;
        let root = root.path().canonicalize()?;
        let first = root.join("from/A");
        input_fixture(&first, 32)?;
        std::fs::write(first.join("victim"), b"replacement")?;
        let destination = root.join("dst");
        let later = destination.join("A/victim/B");
        let original = input_fixture(&later, 64)?;
        let output = overwrite_operands(&first, &later, &destination, strict)?;
        assert_eq!(
            std::fs::read(later.join("file0")).expect("later input must survive"),
            original,
            "{output:?}",
        );
        assert_input_error(&output);
    }
    Ok(())
}

#[test]
fn overwrite_preserves_descendants_of_a_later_directory_operand() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let root = root.path().canonicalize()?;
    let first = root.join("from/A");
    input_fixture(&first.join("B/nested"), 32)?;
    let destination = root.join("dst");
    let later = destination.join("A/B");
    let original = input_fixture(&later.join("nested"), 64)?;
    // strict dispatch makes the earlier copy reach B before the later input starts
    let output = overwrite_operands(&first, &later, &destination, true)?;
    assert_eq!(
        std::fs::read(later.join("nested/file0"))?,
        original,
        "{output:?}"
    );
    assert_input_error(&output);
    Ok(())
}

#[test]
fn root_file_overwrite_preserves_a_later_input_tree() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let root = root.path().canonicalize()?;
    let first = root.join("from");
    input_fixture(&first, 32)?;
    let later = root.join("input");
    let destination = later.join("unvisited/deep");
    let original = input_fixture(&destination, 64)?;
    // keep the later root pending until the first operand reaches its replacement check
    let output = overwrite_operands(&first.join("file0"), &later, &destination, true)?;
    assert_eq!(
        std::fs::read(destination.join("file0"))?,
        original,
        "{output:?}"
    );
    assert_input_error(&output);
    Ok(())
}

#[test]
fn overwrite_preserves_a_later_file_operand() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let root = root.path().canonicalize()?;
    let first = root.join("from/A");
    input_fixture(&first, 32)?;
    std::fs::write(first.join("victim"), b"replacement")?;
    let destination = root.join("dst");
    let parent = destination.join("A/victim");
    let original = input_fixture(&parent, 64)?;
    let later = parent.join("file0");
    let output = overwrite_operands(&first, &later, &destination, true)?;
    assert_eq!(
        std::fs::read(&later).expect("later input file must survive"),
        original,
        "{output:?}"
    );
    assert_input_error(&output);
    Ok(())
}

#[test]
fn overwrite_preserves_a_later_symlink_operand() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let root = root.path().canonicalize()?;
    let first = root.join("from/A");
    input_fixture(&first, 32)?;
    std::fs::write(first.join("victim"), b"replacement")?;
    let destination = root.join("dst");
    let parent = destination.join("A/victim");
    std::fs::create_dir_all(&parent)?;
    let later = parent.join("link");
    std::os::unix::fs::symlink("absent", &later)?;
    let output = overwrite_operands(&first, &later, &destination, true)?;
    assert_eq!(
        std::fs::read_link(&later).expect("later input symlink must survive"),
        std::path::Path::new("absent"),
        "{output:?}"
    );
    assert_input_error(&output);
    Ok(())
}

#[test]
fn empty_directory_overwrite_preserves_a_later_input() -> anyhow::Result<()> {
    for strict in [false, true] {
        let root = tempfile::tempdir()?;
        let root = root.path().canonicalize()?;
        let first = root.join("from/A");
        input_fixture(&first, 32)?;
        std::fs::write(first.join("victim"), b"replacement")?;
        let destination = root.join("dst");
        let later = destination.join("A/victim");
        std::fs::create_dir_all(&later)?;
        // the later empty root has a distinct, valid output, so it stays pending or commits
        // participation instead of failing its own admission and releasing protection
        let output = overwrite_operands(&first, &later, &destination, strict)?;
        assert!(
            later.is_dir(),
            "empty input must remain a directory: {output:?}"
        );
        assert_eq!(std::fs::read_dir(&later)?.count(), 0);
        assert_input_error(&output);
    }
    Ok(())
}

#[test]
fn ignore_existing_skips_a_non_directory_inside_the_source() -> anyhow::Result<()> {
    for preview in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("src");
        std::fs::create_dir(&src)?;
        std::fs::write(src.join("src"), b"sentinel")?;
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_rcp"));
        command.arg("--ignore-existing");
        if preview {
            command.arg("--dry-run=brief");
        }
        let output = command
            .arg(&src)
            .arg(format!("{}/", src.display()))
            .output()?;
        assert!(output.status.success(), "{output:?}");
        assert_eq!(std::fs::read(src.join("src"))?, b"sentinel");
    }
    Ok(())
}

#[test]
#[ignore = "requires unprivileged user/mount namespaces; see docs/testing.md"]
fn multiple_sources_cannot_consume_each_others_output() -> anyhow::Result<()> {
    if !mounts::isolated("multiple_sources_cannot_consume_each_others_output")? {
        return Ok(());
    }
    for strict in [false, true] {
        let root = tempfile::tempdir()?;
        let root_path = root.path().canonicalize()?;
        for path in ["src/A/in", "src/B/in", "dst/A", "dst/B"] {
            std::fs::create_dir_all(root_path.join(path))?;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        for path in ["src/A", "src/B"] {
            let mut config =
                common::filegen::FileGenConfig::new(root_path.join(path), vec![], 1, 32);
            config.leaf_files = true;
            runtime.block_on(common::filegen::filegen(common::get_progress(), &config))?;
        }
        let _first = mounts::BindMount::new(&root_path.join("dst/B"), &root_path.join("src/A/in"))?;
        let _second =
            mounts::BindMount::new(&root_path.join("dst/A"), &root_path.join("src/B/in"))?;
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_rcp"));
        command.args(["--overwrite", "--max-workers=2", "--exclude=/*/*/*/*/"]);
        if strict {
            command.arg("--require-toctou-safe");
        }
        let output = command
            .arg(root_path.join("src/A"))
            .arg(root_path.join("src/B"))
            .arg(format!("{}/", root_path.join("dst").display()))
            .output()?;
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(
            format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .contains("overlap"),
            "{output:?}"
        );
    }
    Ok(())
}
