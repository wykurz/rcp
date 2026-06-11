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
