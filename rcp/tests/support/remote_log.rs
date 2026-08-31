/// Return whether any rcpd debug log in `dir` contains `needle`.
pub fn rcpd_logs_contain(dir: &std::path::Path, needle: &str) -> bool {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|entry| {
            std::fs::read_to_string(entry.path()).is_ok_and(|content| content.contains(needle))
        })
}

/// Return whether both marked rcpd roles have consumed their master hello.
pub fn rcpd_role_hellos_received(log_dir: &std::path::Path) -> bool {
    rcpd_logs_contain(log_dir, "Received side: Source")
        && rcpd_logs_contain(log_dir, "Received side: Destination")
}
