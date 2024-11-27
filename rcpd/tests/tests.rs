#[test]
fn check_rcp_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rcpd").unwrap();
    cmd.arg("--help").assert();
}
