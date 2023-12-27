#[test]
fn check_filegen_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("filegen").unwrap();
    cmd.arg("--help").assert();
}

#[test]
fn check_rcp_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rcp").unwrap();
    cmd.arg("--help").assert();
}

#[test]
fn check_rlink_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rlink").unwrap();
    cmd.arg("--help").assert();
}

#[test]
fn check_rrm_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("rrm").unwrap();
    cmd.arg("--help").assert();
}
