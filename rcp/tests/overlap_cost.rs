//! Syscall bounds for shared destination ancestry and inherited traversal evidence.

use std::path::{Path, PathBuf};

async fn tree(root: &Path, file_size: usize) -> anyhow::Result<()> {
    std::fs::create_dir_all(root)?;
    let mut config = common::filegen::FileGenConfig::new(root, vec![2, 2], 1, file_size);
    config.leaf_files = true;
    common::filegen::filegen(common::get_progress(), &config).await?;
    Ok(())
}

fn fd_calls<'a>(trace: &'a str, syscall: &str) -> impl Iterator<Item = (&'a Path, &'a str)> {
    let marker = format!("{syscall}(");
    trace.lines().filter_map(move |line| {
        // only entry lines contain the opening parenthesis; resumed calls are not counted twice.
        // read the first argument, excluding fd paths in return values or later arguments.
        let (_, arguments) = line.split_once(marker.as_str())?;
        let (descriptor, rest) = arguments.split_once('>')?;
        let (_, path) = descriptor.split_once('<')?;
        Some((Path::new(path), rest))
    })
}

fn assert_ancestry_cost(trace: &str, containers: &[PathBuf; 3], operands: usize) {
    let parent_opens: Vec<_> = fd_calls(trace, "openat")
        .filter(|(_, arguments)| arguments.starts_with(", \"..\","))
        .map(|(path, _)| path)
        .collect();
    let mount_samples: Vec<_> = fd_calls(trace, "statx")
        .filter(|(_, arguments)| {
            arguments.starts_with(", \"\",")
                && arguments
                    .split(',')
                    .nth(3)
                    .is_some_and(|mask| mask.contains("STATX_MNT_ID"))
        })
        .map(|(path, _)| path)
        .collect();
    let metadata: Vec<_> = ["fstat", "fstat64", "newfstatat", "fstatat64"]
        .into_iter()
        .flat_map(|name| {
            fd_calls(trace, name)
                .filter(move |(_, arguments)| {
                    matches!(name, "fstat" | "fstat64") || arguments.starts_with(", \"\",")
                })
                .map(|(path, _)| path)
        })
        .collect();
    let counts: Vec<_> = containers
        .iter()
        .map(|container| {
            let count = |paths: &[&Path]| paths.iter().filter(|path| **path == container).count();
            (
                count(&parent_opens),
                count(&metadata),
                count(&mount_samples),
            )
        })
        .collect();
    // each known ancestor is opened and sampled once. Each operand must sample its initial held
    // parent, but all operands share the walk above it. Host ancestry outside this fixture varies.
    assert_eq!(
        counts,
        [(1, 1, 1), (1, 1, 1), (1, operands, operands)],
        "unexpected (parent opens, metadata reads, mount samples):\n{trace}"
    );
    // child traversal must inherit the evidence instead of starting more ancestry walks.
    assert_eq!(
        parent_opens
            .iter()
            .filter(|path| path.starts_with(&containers[0]))
            .count(),
        3,
        "child traversal repeated an ancestry walk:\n{trace}"
    );
    assert_eq!(
        mount_samples
            .iter()
            .filter(|path| path.starts_with(&containers[0]))
            .count(),
        operands + 2,
        "child traversal resampled ancestry:\n{trace}"
    );
}

async fn ancestry_case(preview: bool, operands: usize, workers: usize) -> anyhow::Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().canonicalize()?;
    let containers = [
        root.join("destination"),
        root.join("destination/one"),
        root.join("destination/one/two"),
    ];
    let destination = &containers[2];
    std::fs::create_dir_all(destination)?;
    let mut sources = Vec::new();
    let mut expected = Vec::new();
    for operand in 0..operands {
        let name = format!("source{operand}");
        let source = root.join(&name);
        let output = destination.join(name);
        tree(&source, 8).await?;
        if preview {
            tree(&output, 16).await?;
        }
        for first in 0..2 {
            for second in 0..2 {
                let relative = format!("dir{first}/dir{second}/file0");
                let input = source.join(&relative);
                let output = output.join(relative);
                let bytes = std::fs::read(if preview { &output } else { &input })?;
                expected.push((input, output, bytes));
            }
        }
        sources.push(source);
    }
    let trace_path = root.join("syscalls");
    let mut command = std::process::Command::new("strace");
    // %%stat selects metadata syscalls on either supported architecture, including libc's fstat
    // variants. -f and -yy expose worker calls and the held directory each fd refers to.
    command
        .args(["-f", "-yy", "-s", "4096", "-e", "trace=openat,%%stat", "-o"])
        .arg(&trace_path)
        .arg(env!("CARGO_BIN_EXE_rcp"))
        .arg(format!("--max-workers={workers}"))
        .args(["--max-blocking-threads=4", "--max-files-in-flight=8"]);
    if preview {
        command.arg("--dry-run=brief");
    }
    let result = command
        .args(sources)
        .arg(format!("{}/", destination.display()))
        .output()
        .unwrap_or_else(|error| panic!("cannot run strace: {error:#}. Install strace."));
    assert!(
        result.status.success(),
        "traced rcp failed (preview={preview}, operands={operands}, workers={workers}): {result:?}"
    );
    let stdout = String::from_utf8_lossy(&result.stdout);
    for (input, output, bytes) in expected {
        if preview {
            let action = format!("would copy file {input:?} -> {output:?}");
            assert!(
                stdout.lines().any(|line| line == action),
                "missing {action}:\n{stdout}"
            );
        }
        assert_eq!(std::fs::read(&output)?, bytes, "{output:?}");
    }
    // expected output/action checks above and nonzero syscall counts below prevent vacuous passes.
    assert_ancestry_cost(&std::fs::read_to_string(trace_path)?, &containers, operands);
    Ok(())
}

#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot provide strace")]
#[tokio::test]
async fn copy_operands_share_one_ancestry_walk_and_sample_each_step_once() -> anyhow::Result<()> {
    for operands in [1, 4] {
        for workers in [1, 4] {
            ancestry_case(false, operands, workers).await?;
        }
    }
    Ok(())
}

#[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot provide strace")]
#[tokio::test]
async fn preview_operands_share_one_ancestry_walk_and_sample_each_step_once() -> anyhow::Result<()>
{
    for operands in [1, 4] {
        for workers in [1, 4] {
            ancestry_case(true, operands, workers).await?;
        }
    }
    Ok(())
}
