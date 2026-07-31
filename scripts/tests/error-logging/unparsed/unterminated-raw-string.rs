// Fixture for scripts/test-check-error-logging.sh — NOT part of any crate and never compiled.
//
// The second way this lexer can stop reading a file, and the one raw-string support introduced: a
// raw string that never closes. Every line after the opener is treated as body and skipped, which is
// correct for a genuine multi-line literal — those close. One still open at end of file means the
// opener was MISREAD, and then the skipping is not correctness, it is blindness. It is announced for
// the same reason an unterminated call is: silence from a lexer that stopped reading looks exactly
// like a clean file.
//
// The opener carries two hashes and the delimiter below has one, so this pins the matched hash count
// as well: reading `"#` as the terminator would close the literal, the file would parse, and the
// guard would go quiet — the same way its neighbour fixture went vacuous once raw strings worked.

fn a_raw_string_that_never_closes() {
    let _config = r##"
        everything from here to the end of the file is this literal's body
"#;
}

// unreachable for the linter, which is the point
fn a_real_violation_hidden_behind_it() {
    tracing::error!("operation failed: {}", &error);
}
