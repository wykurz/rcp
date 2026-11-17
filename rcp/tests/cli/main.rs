//! CLI integration tests share one executable to avoid repeated release compilation.

mod acls;
mod cli_parsing_tests;
mod delete;
mod file_create_mode;
#[path = "../support/fixtures.rs"]
mod fixtures;
mod output_format_tests;
mod overlap_cost;
mod reflink;
mod remote_non_ssh_tests;
mod summary_parser;
mod tilde_tests;
