#[test]
fn check_rlink_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.arg("--help").assert();
}
