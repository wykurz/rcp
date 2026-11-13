//! Parser contracts do not need SSH and also run in the Nix build sandbox.

#[path = "support/summary.rs"]
mod summary;

fn output(stdout: &str) -> std::process::Output {
    use std::os::unix::process::ExitStatusExt;
    std::process::Output {
        status: std::process::ExitStatus::from_raw(0),
        stdout: stdout.as_bytes().to_vec(),
        stderr: Vec::new(),
    }
}

#[test]
fn parses_counts_with_rounded_byte_totals() {
    let result = summary::parse_summary_from_output(&output(
        "copy:\nbytes copied: 2.7 KiB\nfiles copied: 150\ndirectories created: 1\n\
         delete:\nbytes removed: 1.2 MiB\nfiles removed: 2\n",
    ))
    .expect("rounded byte totals must not prevent count assertions");
    assert_eq!(result.files_copied, 150);
    assert_eq!(result.directories_created, 1);
    assert_eq!(result.rm_summary.files_removed, 2);
}

#[test]
fn retains_byte_units_and_displayed_precision() {
    for displayed in ["0 B", "40 B", "2 KiB", "2.7 KiB", "1.2 MiB"] {
        let result = summary::parse_summary_from_output(&output(&format!(
            "copy:\nbytes copied: {displayed}\nfiles copied: 1\n\
             delete:\nbytes removed: {displayed}\nfiles removed: 1\n",
        )))
        .expect("displayed byte values must remain available");
        assert_eq!(result.bytes_copied, displayed);
        assert_eq!(result.rm_summary.bytes_removed, displayed);
    }
}

#[test]
fn keeps_copy_and_delete_skip_counts_separate() {
    let result = summary::parse_summary_from_output(&output(
        "copy:\nfiles skipped: 2\nsymlinks skipped: 3\ndirectories skipped: 4\n\
         delete:\nfiles skipped: 5\nsymlinks skipped: 6\ndirectories skipped: 7\n",
    ))
    .unwrap();
    assert_eq!(result.files_skipped, 2);
    assert_eq!(result.symlinks_skipped, 3);
    assert_eq!(result.directories_skipped, 4);
    assert_eq!(result.rm_summary.files_skipped, 5);
    assert_eq!(result.rm_summary.symlinks_skipped, 6);
    assert_eq!(result.rm_summary.directories_skipped, 7);
}

#[test]
fn parses_the_displayed_summary_from_the_real_formatter() {
    let printed = common::copy::Summary {
        bytes_copied: 2740,
        files_copied: 1,
        symlinks_created: 2,
        directories_created: 3,
        files_unchanged: 4,
        symlinks_unchanged: 5,
        directories_unchanged: 6,
        files_skipped: 7,
        symlinks_skipped: 8,
        directories_skipped: 9,
        specials_skipped: 10,
        rm_summary: common::rm::Summary {
            bytes_removed: 2 * 1024 * 1024,
            files_removed: 11,
            symlinks_removed: 12,
            directories_removed: 13,
            files_skipped: 14,
            symlinks_skipped: 15,
            directories_skipped: 16,
        },
    };
    let result = summary::parse_summary_from_output(&output(&printed.to_string())).unwrap();
    assert_eq!(result.bytes_copied, "2.7 KiB");
    assert_eq!(result.files_copied, 1);
    assert_eq!(result.symlinks_created, 2);
    assert_eq!(result.directories_created, 3);
    assert_eq!(result.files_unchanged, 4);
    assert_eq!(result.symlinks_unchanged, 5);
    assert_eq!(result.directories_unchanged, 6);
    assert_eq!(result.files_skipped, 7);
    assert_eq!(result.symlinks_skipped, 8);
    assert_eq!(result.directories_skipped, 9);
    assert_eq!(result.specials_skipped, 10);
    assert_eq!(result.rm_summary.bytes_removed, "2.0 MiB");
    assert_eq!(result.rm_summary.files_removed, 11);
    assert_eq!(result.rm_summary.symlinks_removed, 12);
    assert_eq!(result.rm_summary.directories_removed, 13);
    assert_eq!(result.rm_summary.files_skipped, 14);
    assert_eq!(result.rm_summary.symlinks_skipped, 15);
    assert_eq!(result.rm_summary.directories_skipped, 16);
}

#[test]
fn rejects_output_without_summary_fields() {
    for stdout in [
        "",
        "Starting copy\nwalltime: 1s\n",
        "copy:\n-----\ndelete:\n-------\n",
    ] {
        assert!(summary::parse_summary_from_output(&output(stdout)).is_none());
    }
}

#[test]
fn rejects_malformed_entry_counts() {
    for stdout in [
        "copy:\nfiles copied: unknown\n",
        "copy:\nfiles skipped: -1\n",
        "copy:\nfiles copied: 1\ndelete:\nfiles removed: invalid\n",
    ] {
        assert!(summary::parse_summary_from_output(&output(stdout)).is_none());
    }
}
