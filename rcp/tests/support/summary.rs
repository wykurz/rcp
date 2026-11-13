//! Parses the displayed CLI summary for integration-test assertions.
//!
//! Byte totals remain text because IEC formatting rounds larger values. They cannot be decoded
//! into the exact byte totals represented by the production summary type.

#[derive(Debug, Default)]
pub struct DisplayedSummary {
    pub bytes_copied: String,
    pub files_copied: usize,
    pub symlinks_created: usize,
    pub directories_created: usize,
    pub files_unchanged: usize,
    pub symlinks_unchanged: usize,
    pub directories_unchanged: usize,
    pub files_skipped: usize,
    pub symlinks_skipped: usize,
    pub directories_skipped: usize,
    pub specials_skipped: usize,
    pub rm_summary: DisplayedRemovalSummary,
}

#[derive(Debug, Default)]
pub struct DisplayedRemovalSummary {
    pub bytes_removed: String,
    pub files_removed: usize,
    pub symlinks_removed: usize,
    pub directories_removed: usize,
    pub files_skipped: usize,
    pub symlinks_skipped: usize,
    pub directories_skipped: usize,
}

enum Section {
    Copy,
    Delete,
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
pub fn parse_summary_from_output(output: &std::process::Output) -> Option<DisplayedSummary> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut summary = DisplayedSummary::default();
    let mut section = None;
    let mut found_any = false;
    for line in stdout.lines() {
        match line {
            "copy:" => section = Some(Section::Copy),
            "delete:" => section = Some(Section::Delete),
            _ => {}
        }
        match section {
            Some(Section::Copy) => {
                parse_field!(line, "bytes copied: ", summary.bytes_copied, found_any);
                parse_field!(line, "files copied: ", summary.files_copied, found_any);
                parse_field!(line, "symlinks created: ", summary.symlinks_created, found_any);
                parse_field!(line, "directories created: ", summary.directories_created, found_any);
                parse_field!(line, "files unchanged: ", summary.files_unchanged, found_any);
                parse_field!(line, "symlinks unchanged: ", summary.symlinks_unchanged, found_any);
                parse_field!(line, "directories unchanged: ", summary.directories_unchanged, found_any);
                parse_field!(line, "files skipped: ", summary.files_skipped, found_any);
                parse_field!(line, "symlinks skipped: ", summary.symlinks_skipped, found_any);
                parse_field!(line, "directories skipped: ", summary.directories_skipped, found_any);
                parse_field!(line, "specials skipped: ", summary.specials_skipped, found_any);
            }
            Some(Section::Delete) => {
                parse_field!(line, "bytes removed: ", summary.rm_summary.bytes_removed, found_any);
                parse_field!(line, "files removed: ", summary.rm_summary.files_removed, found_any);
                parse_field!(line, "symlinks removed: ", summary.rm_summary.symlinks_removed, found_any);
                parse_field!(line, "directories removed: ", summary.rm_summary.directories_removed, found_any);
                parse_field!(line, "files skipped: ", summary.rm_summary.files_skipped, found_any);
                parse_field!(line, "symlinks skipped: ", summary.rm_summary.symlinks_skipped, found_any);
                parse_field!(line, "directories skipped: ", summary.rm_summary.directories_skipped, found_any);
            }
            None => {}
        }
    }
    found_any.then_some(summary)
}
