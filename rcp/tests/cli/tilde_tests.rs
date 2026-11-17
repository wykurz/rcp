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

#[test]
fn copies_quoted_home_paths_for_local_and_localhost_operands() {
    for prefix in ["", "localhost:"] {
        let home = tempfile::tempdir().unwrap();
        let outside_home = tempfile::tempdir().unwrap();
        // even faulty expansion must read and write only inside these temporary fixtures
        for (operand, expected_directory, decoy_directory) in [
            (
                "~/~/".to_string(),
                home.path().join("~"),
                home.path().to_path_buf(),
            ),
            (
                format!("~/{}/", outside_home.path().display()),
                home.path()
                    .join(outside_home.path().strip_prefix("/").unwrap()),
                outside_home.path().to_path_buf(),
            ),
        ] {
            fs::create_dir_all(&expected_directory).unwrap();
            fs::write(expected_directory.join("source.txt"), "local home content").unwrap();
            fs::write(decoy_directory.join("source.txt"), "decoy source").unwrap();
            fs::write(
                decoy_directory.join("destination.txt"),
                "keep decoy destination",
            )
            .unwrap();
            Command::cargo_bin("rcp")
                .unwrap()
                .env("HOME", home.path())
                .args([
                    "--overwrite".to_string(),
                    format!("{prefix}{operand}source.txt"),
                    format!("{prefix}{operand}destination.txt"),
                ])
                .assert()
                .success();
            assert_eq!(
                fs::read_to_string(expected_directory.join("destination.txt")).unwrap(),
                "local home content"
            );
            assert_eq!(
                fs::read_to_string(decoy_directory.join("source.txt")).unwrap(),
                "decoy source"
            );
            assert_eq!(
                fs::read_to_string(decoy_directory.join("destination.txt")).unwrap(),
                "keep decoy destination"
            );
        }
    }
}
