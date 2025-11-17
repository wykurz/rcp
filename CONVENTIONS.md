- For functions and types prefer to use fully qualified name, e.g. "std::net::SocketAddr".
- Import macros and traits used in macros, e.g. "use serde::{Deserialize, Serialize};" and then used in "#[derive(Debug, Serialize, Deserialize)]".
- Prefer no empty lines in functions or type definitions.
- When importing crates -- specify only major and minor version, don't specify patch e.g. 1.1 and not 1.1.12.
- Don't start comments from a capital letter and use dot only to separate multiple sentences.

## Testing Conventions

- Follow all general conventions (no empty lines in test functions, lowercase comments, etc.)
- Test function structure should be compact:
  - Setup (create test environment, files)
  - Execute (run command under test)
  - Assert (verify expected behavior)
- Minimize empty lines between test sections
- Comments should explain "why" not "what" (the code shows what)
- Use helper functions (`setup_test_env`, `run_rcp_and_expect_success`, etc.) to reduce boilerplate
- Example pattern from existing tests:
  ```rust
  #[test]
  fn test_feature() {
      let (src_dir, dst_dir) = setup_test_env();
      let src_file = src_dir.path().join("file.txt");
      create_test_file(&src_file, "content", 0o644);
      let output = run_rcp_and_expect_success(&[src_file.to_str().unwrap(), "localhost:/dest"]);
      assert_eq!(get_file_content(&dst_file), "content");
      // verify specific behavior
      let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
      assert_eq!(summary.files_copied, 1);
  }
  ```