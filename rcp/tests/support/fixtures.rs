//! Shared filesystem fixtures for the rcp integration tests.
//!
//! Included via `#[path = "support/fixtures.rs"] mod fixtures;` rather than `mod support;` so it
//! does not pull the heavier `docker_env` helpers into every test binary. `dead_code` is allowed
//! because not every consuming binary uses every helper.
#![allow(dead_code)]

use std::os::unix::fs::PermissionsExt;

pub fn setup_test_env() -> (tempfile::TempDir, tempfile::TempDir) {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    (src_dir, dst_dir)
}

pub fn create_test_file(path: &std::path::Path, content: &str, mode: u32) {
    std::fs::write(path, content).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

pub fn create_symlink(src: &std::path::Path, dst: &std::path::Path) {
    std::os::unix::fs::symlink(src, dst).unwrap();
}

pub fn get_file_content(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap()
}

pub fn get_file_mode(path: &std::path::Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

/// Render `(mode, size)` samples with the mode in octal — plain `{:?}` prints a decimal mode,
/// which is unreadable in a permissions failure.
pub fn describe_samples(samples: &[(u32, u64)]) -> String {
    samples
        .iter()
        .map(|(mode, size)| format!("(mode {mode:o}, size {size})"))
        .collect::<Vec<_>>()
        .join(" -> ")
}

/// Run `child` to completion, recording each distinct `(mode, size)` the entry at `path` passes
/// through, ending with the state it is left in. Consecutive identical samples are collapsed, so
/// what comes back is the sequence of states rather than thousands of repeats of each.
pub fn sample_while_running(
    mut child: std::process::Child,
    path: &std::path::Path,
) -> (std::process::ExitStatus, Vec<(u32, u64)>) {
    let mut samples: Vec<(u32, u64)> = Vec::new();
    let sample = |samples: &mut Vec<(u32, u64)>| {
        if let Ok(meta) = std::fs::symlink_metadata(path) {
            let observed = (meta.permissions().mode() & 0o7777, meta.len());
            if samples.last() != Some(&observed) {
                samples.push(observed);
            }
        }
    };
    let status = loop {
        sample(&mut samples);
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    };
    sample(&mut samples);
    (status, samples)
}
