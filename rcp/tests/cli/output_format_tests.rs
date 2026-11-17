//! `--output-format json` prints the summary as one JSON object; text stays the default.

use crate::fixtures::{create_test_file, setup_test_env};

const PROCESS_STATS: [&str; 7] = [
    "cpu_time_user_ms",
    "cpu_time_kernel_ms",
    "peak_rss_bytes",
    "voluntary_context_switches",
    "involuntary_context_switches",
    "minor_page_faults",
    "major_page_faults",
];

fn rcp(args: &[&str]) -> std::process::Output {
    let output = assert_cmd::Command::cargo_bin("rcp")
        .unwrap()
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "rcp {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// A source tree of three files (3, 4 and 5 bytes) in two directories.
fn source_tree(src: &std::path::Path) -> std::path::PathBuf {
    let tree = src.join("tree");
    std::fs::create_dir_all(tree.join("sub")).unwrap();
    create_test_file(&tree.join("a"), "aaa", 0o644);
    create_test_file(&tree.join("b"), "bbbb", 0o644);
    create_test_file(&tree.join("sub").join("c"), "ccccc", 0o644);
    tree
}

#[test]
fn json_summary_of_a_local_copy_is_one_object_with_exact_counters_and_stats() {
    let (src_dir, dst_dir) = setup_test_env();
    let src = source_tree(src_dir.path());
    let dst = dst_dir.path().join("copy");
    let output = rcp(&[
        "--summary",
        "--output-format",
        "json",
        src.to_str().unwrap(),
        dst.to_str().unwrap(),
    ]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    // nothing but the object: no text summary before it and no text statistics after it
    assert_eq!(stdout.lines().count(), 1, "{stdout}");
    let summary: serde_json::Value = serde_json::from_str(stdout.trim_end()).unwrap();
    assert_eq!(summary["files_copied"], 3);
    assert_eq!(summary["bytes_copied"], 12);
    assert_eq!(summary["directories_created"], 2);
    assert_eq!(summary["files_unchanged"], 0);
    assert_eq!(summary["rm_summary"]["files_removed"], 0);
    assert!(summary["walltime_ms"].is_u64(), "{summary}");
    for key in PROCESS_STATS {
        assert!(
            summary[key].is_u64(),
            "missing or non-numeric {key}: {summary}"
        );
    }
    assert!(summary["peak_rss_bytes"].as_u64().unwrap() > 1024 * 1024);
    assert!(summary.get("remote").is_none(), "{summary}");
    assert!(dst.join("sub").join("c").exists());
}

#[test]
fn the_summary_is_text_unless_json_is_asked_for() {
    let (src_dir, dst_dir) = setup_test_env();
    let src = source_tree(src_dir.path());
    for (index, format) in [None, Some("text")].into_iter().enumerate() {
        let dst = dst_dir.path().join(format!("copy-{index}"));
        let mut args = vec!["--summary"];
        if let Some(format) = format {
            args.extend(["--output-format", format]);
        }
        args.extend([src.to_str().unwrap(), dst.to_str().unwrap()]);
        let stdout = String::from_utf8(rcp(&args).stdout).unwrap();
        assert!(stdout.contains("files copied: 3"), "{stdout}");
        assert!(stdout.contains("walltime : "), "{stdout}");
        assert!(stdout.contains("context switches : "), "{stdout}");
        assert!(stdout.contains("page faults : "), "{stdout}");
        assert!(!stdout.contains('{'), "{stdout}");
    }
}

#[test]
fn the_format_alone_prints_no_summary() {
    let (src_dir, dst_dir) = setup_test_env();
    let src = source_tree(src_dir.path());
    let dst = dst_dir.path().join("copy");
    let output = rcp(&[
        "--output-format",
        "json",
        src.to_str().unwrap(),
        dst.to_str().unwrap(),
    ]);
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "");
}

#[test]
fn an_unknown_format_is_rejected_before_any_work() {
    let (src_dir, dst_dir) = setup_test_env();
    let src = source_tree(src_dir.path());
    let dst = dst_dir.path().join("copy");
    let output = assert_cmd::Command::cargo_bin("rcp")
        .unwrap()
        .args([
            "--summary",
            "--output-format",
            "yaml",
            src.to_str().unwrap(),
            dst.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("possible values: text, json"), "{stderr}");
    assert!(!dst.exists());
}

#[test]
fn json_summary_is_the_last_line_of_stdout_after_everything_the_run_logs() {
    let (src_dir, dst_dir) = setup_test_env();
    let src = source_tree(src_dir.path());
    let dst = dst_dir.path().join("copy");
    // at debug verbosity the histogram logger reports that it exits, which is after the copy
    let output = rcp(&[
        "-vv",
        "--summary",
        "--output-format",
        "json",
        "--auto-meta-throttle",
        "--auto-meta-histogram",
        src.to_str().unwrap(),
        dst.to_str().unwrap(),
    ]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("histogram-logger: exiting"),
        "the scenario needs a line logged while the run winds down: {stdout}"
    );
    let last = stdout.lines().last().expect("some output");
    let summary: serde_json::Value = serde_json::from_str(last)
        .unwrap_or_else(|error| panic!("the last line is not the object ({error}): {last}"));
    assert_eq!(summary["files_copied"], 3);
    assert!(summary["peak_rss_bytes"].is_u64(), "{summary}");
}

#[test]
fn verbose_alone_prints_the_summary_in_the_format_asked_for() {
    let (src_dir, dst_dir) = setup_test_env();
    let src = source_tree(src_dir.path());
    let dst = dst_dir.path().join("copy");
    // verbose logging implies a summary, as it does in text
    let output = rcp(&[
        "-v",
        "--output-format",
        "json",
        src.to_str().unwrap(),
        dst.to_str().unwrap(),
    ]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let last = stdout.lines().last().expect("a summary line");
    let summary: serde_json::Value = serde_json::from_str(last).unwrap();
    assert_eq!(summary["files_copied"], 3);
    assert!(!stdout.contains("files copied: 3"), "{stdout}");
}

#[test]
fn a_failed_copy_reports_in_text_with_its_statistics_whatever_the_format() {
    let (src_dir, dst_dir) = setup_test_env();
    let src = source_tree(src_dir.path());
    // one file that cannot be read fails the run; the others are still copied
    create_test_file(&src.join("unreadable"), "secret", 0o000);
    for format in ["text", "json"] {
        let dst = dst_dir.path().join(format!("copy-{format}"));
        let output = assert_cmd::Command::cargo_bin("rcp")
            .unwrap()
            .args([
                "--summary",
                "--output-format",
                format,
                src.to_str().unwrap(),
                dst.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{format}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("files copied: 3"), "{format}: {stdout}");
        for line in [
            "walltime : ",
            "cpu time : ",
            "peak RSS : ",
            "context switches : ",
            "page faults : ",
        ] {
            assert!(stdout.contains(line), "{format}: no {line:?} in {stdout}");
        }
        // no object: a reader that expects one must see that there is none
        assert!(
            !stdout.lines().any(|line| line.starts_with('{')),
            "{format}: {stdout}"
        );
    }
}
