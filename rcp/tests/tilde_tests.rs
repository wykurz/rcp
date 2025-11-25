use std::fs;
use std::os::unix::fs::PermissionsExt;

use assert_cmd::Command;

#[test]
fn test_local_copy_with_tilde_source_and_destination() {
    let tmp_home = tempfile::tempdir().unwrap();
    let src = tmp_home.path().join("src.txt");
    fs::write(&src, "tilde local copy").unwrap();
    fs::set_permissions(&src, fs::Permissions::from_mode(0o644)).unwrap();

    let dst_dir = tempfile::tempdir().unwrap();
    let dst_path = dst_dir.path().join("dst.txt");

    Command::cargo_bin("rcp")
        .unwrap()
        .env("HOME", tmp_home.path())
        .args(["~/src.txt", dst_path.to_str().unwrap()])
        .assert()
        .success();

    let content = fs::read_to_string(&dst_path).unwrap();
    assert_eq!(content, "tilde local copy");
}
