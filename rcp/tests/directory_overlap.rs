//! CLI regressions for overlap checks and multiple source operands.

#[path = "../../common/tests/support/mounts.rs"]
mod mounts;

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
