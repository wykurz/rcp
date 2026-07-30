// Fixture for scripts/test-check-error-logging.sh — NOT part of any crate and never compiled.
//
// This is VALID Rust that the linter's lexer nonetheless cannot get through. `strip_comments` and
// `paren_delta` treat `"` as a string delimiter, which a RAW string breaks: the quote inside `r#"…"#`
// reads as the closing one, so the rest of the literal is scanned as code and its `(` is counted as a
// real paren. The call's parens then never balance and the collector wedges.
//
// The point of the fixture is not the raw string — it is that a wedged collector must SAY SO. It stops
// checking the rest of the file, so the real violation below goes unexamined, and a linter that stayed
// quiet about that would report "passed" for a file it barely read. That is the one outcome this check
// must never produce, so the guard is pinned here.
//
// It needs its own file precisely because of that blast radius: dropped into violations.rs it would
// swallow every EXPECT-VIOLATION marker below it. Written as valid Rust rather than as unbalanced
// source so it does not sit in the editor as a permanent syntax error.

fn a_raw_string_containing_a_quote_and_an_open_paren() {
    tracing::debug!(r#"quote " then an open paren ("#);
}

// unreachable for the linter, and that is the point — the guard exists so this being missed is
// announced rather than silently tolerated
fn a_real_violation_hidden_behind_it() {
    tracing::error!("operation failed: {}", &error);
}
