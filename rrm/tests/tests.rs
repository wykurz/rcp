#[test]
fn check_rrm_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rrm").unwrap();
    cmd.arg("--help").assert();
}
