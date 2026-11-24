use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn check_rcmp_help() {
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--help").assert().success();
}

#[test]
fn outputs_differences_to_stdout_by_default() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create file only in src so dst is missing it
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg(&src)
        .arg(&dst)
        .assert()
        .code(1) // differences found
        .stdout(predicate::str::contains("\"result\":\"dst_missing\""));
}

#[tokio::test]
async fn exit_early_stops_scheduling_after_a_child_differs() -> anyhow::Result<()> {
    const FILES: usize = 32;
    let src = tempdir()?;
    let dst = tempdir()?;
    for (dir, size) in [(&src, 1), (&dst, 2)] {
        let mut config = common::filegen::FileGenConfig::new(dir.path(), vec![], FILES, size);
        config.writebuf = size;
        common::filegen::filegen(common::get_progress(), &config).await?;
    }
    // one leaf permit and one worker let completed children report while the parent is still scanning
    let result = Command::cargo_bin("rcmp")?
        .args([
            "--exit-early",
            "--max-files-in-flight=1",
            "--max-workers=1",
            "--metadata-compare=f:size",
            "--summary",
        ])
        .args([src.path(), dst.path()])
        .assert()
        .code(1);
    let rows = serde_json::Deserializer::from_slice(&result.get_output().stdout)
        .into_iter::<serde_json::Value>()
        .collect::<Result<Vec<_>, _>>()?;
    let (summary, mismatches) = rows.split_last().expect("expected a comparison summary");
    let count = mismatches.len();
    assert!(
        (1..FILES).contains(&count),
        "compared {count} of {FILES} files"
    );
    for mismatch in mismatches {
        assert_eq!(mismatch["result"], "different", "{mismatch}");
    }
    assert_eq!(summary["mismatch"]["file"]["different"], count);
    assert_eq!(summary["mismatch"]["file"]["src_missing"], 0);
    assert_eq!(summary["src_bytes"], count);
    assert_eq!(summary["dst_bytes"], 2 * count);
    Ok(())
}

#[tokio::test]
async fn exit_early_stops_scanning_destination_only_entries() -> anyhow::Result<()> {
    const FILES: usize = 8;
    let src = tempdir()?;
    let dst = tempdir()?;
    let config = common::filegen::FileGenConfig::new(dst.path(), vec![], FILES, 0);
    common::filegen::filegen(common::get_progress(), &config).await?;
    let result = Command::cargo_bin("rcmp")?
        .args(["--exit-early", "--metadata-compare=f:size", "--no-check"])
        .args([src.path(), dst.path()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"result\":\"src_missing\""));
    let stdout = std::str::from_utf8(&result.get_output().stdout)?;
    let count = stdout.lines().count();
    assert!(
        (1..FILES).contains(&count),
        "reported {count} of {FILES} files"
    );
    Ok(())
}

#[test]
fn quiet_suppresses_stdout_output() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create file only in src so dst is missing it
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--quiet")
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(1) // differences found
        .stdout(predicate::str::is_empty());
}

#[test]
fn log_writes_to_file_not_stdout() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let log = dir.path().join("compare.log");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create file only in src so dst is missing it
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--log")
        .arg(&log)
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(1) // differences found
        .stdout(predicate::str::is_empty());
    // verify log file contains the difference in JSON format
    let log_content = std::fs::read_to_string(&log).unwrap();
    assert!(log_content.contains("\"result\":\"dst_missing\""));
}

#[test]
fn keeps_logged_mismatches_when_a_child_comparison_fails() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src");
    std::fs::create_dir(&src)?;
    std::fs::create_dir(dir.path().join("dst"))?;
    std::fs::write(src.join("ok"), "")?;
    std::fs::write(src.join("too-long"), "")?;
    // leave room for dst/ok while dst/too-long exceeds Linux's 4096-byte path limit
    let dst = format!("{}dst", "./".repeat(2044));
    Command::cargo_bin("rcmp")?
        .current_dir(dir.path())
        .args([
            "--metadata-compare=f:size",
            "--log=compare.log",
            "src",
            &dst,
        ])
        .assert()
        .code(2)
        .stdout(predicate::str::contains("dst/too-long"));
    let log = std::fs::read(dir.path().join("compare.log"))?;
    let record: serde_json::Value =
        serde_json::from_slice(&log).expect("expected the completed mismatch record in the log");
    assert_eq!(record["result"], "dst_missing");
    assert_eq!(record["src"], "src/ok");
    Ok(())
}

#[test]
fn text_output_format_produces_legacy_output() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create file only in src so dst is missing it
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--output-format")
        .arg("text")
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(1) // differences found
        .stdout(predicate::str::contains("[DstMissing]"));
}

#[test]
fn text_log_writes_legacy_format_to_file() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let log = dir.path().join("compare.log");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--output-format")
        .arg("text")
        .arg("--log")
        .arg(&log)
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(1)
        .stdout(predicate::str::is_empty());
    let log_content = std::fs::read_to_string(&log).unwrap();
    assert!(log_content.contains("[DstMissing]"));
}

#[test]
fn json_summary_is_valid_json_without_trailing_text() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    let output = cmd.arg("--summary").arg(&src).arg(&dst).output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "expected exit code 1 (differences found)"
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    // last line should be the summary JSON, and every line should be valid JSON
    for line in &lines {
        assert!(
            serde_json::from_str::<serde_json::Value>(line).is_ok(),
            "not valid JSON: {line}"
        );
    }
    // summary line should contain runtime stats
    let summary: serde_json::Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert!(summary.get("walltime_ms").is_some());
    assert!(summary.get("src_bytes").is_some());
    assert!(summary.get("mismatch").is_some());
    // every figure the text summary prints is in the JSON too, as an exact number
    for key in [
        "cpu_time_user_ms",
        "cpu_time_kernel_ms",
        "peak_rss_bytes",
        "voluntary_context_switches",
        "involuntary_context_switches",
        "minor_page_faults",
        "major_page_faults",
    ] {
        assert!(
            summary.get(key).is_some_and(serde_json::Value::is_u64),
            "missing or non-numeric {key}: {summary}"
        );
    }
    assert!(summary["minor_page_faults"].as_u64().unwrap() > 0);
    // the object has had this shape since rcmp printed JSON: nothing else is in it
    let mut keys: Vec<&str> = summary
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "cpu_time_kernel_ms",
            "cpu_time_user_ms",
            "dst_bytes",
            "involuntary_context_switches",
            "major_page_faults",
            "minor_page_faults",
            "mismatch",
            "peak_rss_bytes",
            "skipped",
            "src_bytes",
            "voluntary_context_switches",
            "walltime_ms",
        ]
    );
    assert_eq!(summary["mismatch"]["file"]["dst_missing"], 1);
}

#[test]
fn json_mismatch_output_handles_non_utf8_paths() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create a file with a non-UTF-8 byte in its name
    let bad_name = OsStr::from_bytes(b"bad\xffname.txt");
    std::fs::write(src.join(bad_name), "data").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    let output = cmd.arg(&src).arg(&dst).output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "expected exit code 1 (differences found)"
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    // the output should contain the escaped byte, not the replacement character
    assert!(
        stdout.contains("\\xff"),
        "expected \\xff escape in output, got: {stdout}"
    );
    assert!(
        !stdout.contains('\u{FFFD}'),
        "should not contain replacement character"
    );
    // should be valid JSON
    for line in stdout.lines() {
        assert!(
            serde_json::from_str::<serde_json::Value>(line).is_ok(),
            "not valid JSON: {line}"
        );
    }
}
