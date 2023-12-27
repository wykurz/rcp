#[test]
fn check_filegen_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("filegen").unwrap();
    cmd.arg("--help").assert();
}
