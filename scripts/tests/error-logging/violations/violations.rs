// Fixture for scripts/test-check-error-logging.sh — NOT part of any crate and never compiled.
//
// Every call below must be reported by scripts/check-error-logging.sh. The `EXPECT-VIOLATION`
// marker sits on the line the linter is expected to name (for a multi-line call, the line the
// macro starts on), and the test derives its expectations from those markers, so adding a case
// here is all it takes to cover a new shape.

fn positional_against_a_bare_placeholder() {
    tracing::error!("operation failed: {}", &error); // EXPECT-VIOLATION
    tracing::error!("operation failed: {}", e); // EXPECT-VIOLATION
}

// the level says nothing about whether the chain matters
fn every_level_is_in_scope() {
    tracing::warn!("operation failed: {}", &error); // EXPECT-VIOLATION
    tracing::info!("operation failed: {}", &error); // EXPECT-VIOLATION
    tracing::debug!("operation failed: {}", &error); // EXPECT-VIOLATION
    tracing::trace!("operation failed: {}", &error); // EXPECT-VIOLATION
}

// the error's placeholder is the LAST one; earlier bare `{}`s belong to other arguments, and the
// error's own does not have to be flush against the closing quote
fn placeholder_is_not_flush_against_the_quote() {
    tracing::debug!("Failed to bind to {}:{}: {}", ip, port, e); // EXPECT-VIOLATION
    tracing::warn!("stdin read error ({}), treating as disconnect", e); // EXPECT-VIOLATION
}

// argument names beyond the literal `e` / `&error`
fn general_error_argument_names() {
    tracing::error!("join failed: {}", join_err); // EXPECT-VIOLATION
    tracing::error!("write failed: {}", &write_error); // EXPECT-VIOLATION
    tracing::error!("send failed: {}", err); // EXPECT-VIOLATION
}

// rustfmt splits an overflowing call so the argument sits alone on its own line and `);` starts
// the next one — the closing paren is never adjacent to the argument
fn multi_line_calls() {
    tracing::error!( // EXPECT-VIOLATION
        "a very long message that pushes this call past the width limit: {}",
        &error
    );
    tracing::warn!( // EXPECT-VIOLATION
        "a very long message that pushes this call past the width limit: {}",
        &error,
    );
}

// an error captured inline in the format string, with no format spec
fn inline_captures() {
    tracing::warn!("operation failed: {e}"); // EXPECT-VIOLATION
    tracing::debug!("operation failed: {err}"); // EXPECT-VIOLATION
    tracing::error!("operation failed: {error}"); // EXPECT-VIOLATION
    tracing::error!("operation failed: {my_error}"); // EXPECT-VIOLATION
    tracing::error!("join failed: {join_err}"); // EXPECT-VIOLATION
}

// the error is not the LAST argument. Every placeholder is bare, so whichever one belongs to the
// error renders it with `{}` — decidable without pairing arguments to placeholders.
fn error_is_not_the_last_argument() {
    tracing::error!("failed: {} while copying {}", &error, dst); // EXPECT-VIOLATION
}

// tracing's structured-field sigils: `%` records Display, which drops the chain just as `{}` does
fn structured_field_sigils() {
    tracing::error!(%error, "operation failed"); // EXPECT-VIOLATION
    tracing::warn!(%join_err, "join failed"); // EXPECT-VIOLATION
}

// a macro used as an EXPRESSION ends in `),` or a bare `)`, not `);` — a match arm is the common
// shape. These were invisible, and worse, left the collector mid-call swallowing what followed.
fn expression_position() {
    match result {
        Err(e) => tracing::warn!("operation failed: {}", e), // EXPECT-VIOLATION
        Ok(_) => {}
    }
    let _ = match result {
        Err(err) => tracing::debug!("operation failed: {}", err), // EXPECT-VIOLATION
        Ok(_) => {}
    };
}

// a stale allow marker must not carry past the call it introduces
fn allow_marker_does_not_leak() {
    // rcp-error-log-allow: applies to the next call only
    tracing::error!("allowed: {error}");
    tracing::error!("not allowed: {error}"); // EXPECT-VIOLATION
}

// a comment INSIDE a multi-line call is not code, and does not hide the call from the rules
fn comment_inside_a_multiline_call() {
    tracing::error!( // EXPECT-VIOLATION
        // an explanatory comment, which the collector drops
        "a long message that overflows the line: {}",
        &error
    );
}

// A TRAILING COMMENT is not part of the call. Prose that names the CORRECT form must not be read as
// the call's own format text — the `{:#}` below became the "last placeholder" and silenced the live
// bare `{}` next to it, which is the exact shape a reviewer reaches for when explaining the rule.
fn a_trailing_comment_does_not_supply_the_format_spec() {
    tracing::error!("failed: {}", error); // correct form: {:#} — EXPECT-VIOLATION
    tracing::warn!("failed: {}", err); // prose mentioning {err:#} must not count — EXPECT-VIOLATION
}

// `//` inside a string literal is not a comment: the format string survives, and its bare `{}` is
// still the error's
fn double_slash_inside_the_format_string() {
    tracing::error!("connecting to ssh://{}: {}", host, &error); // EXPECT-VIOLATION
}

// a match arm whose PATTERN is a string literal: the pattern is not the call's format string. Reading
// the first literal on the line picked up `"error"` and left the real format string unexamined.
fn a_string_match_arm_pattern_is_not_the_format_string() {
    match kind {
        "error" => tracing::error!("failed: {}", error), // EXPECT-VIOLATION
        _ => {}
    }
}

// a nested call inside a block ARGUMENT contains `);`, which used to end collection early — the error
// two lines further down was never collected, so the violation vanished. Collection now ends where the
// call's own parens balance, and the error is the block's tail expression (followed by `}`, not `,`).
fn a_nested_call_inside_an_argument_does_not_end_collection() {
    tracing::error!( // EXPECT-VIOLATION
        "failed: {}",
        {
            observe();
            &error
        }
    );
}

// the opt-out is a justified COMMENT on the line above, and nothing else. A marker appearing in a
// format string, or on an unrelated preceding statement, must not silence a real violation.
fn the_opt_out_marker_only_counts_as_a_comment_above() {
    tracing::error!("rcp-error-log-allow: {}", error); // EXPECT-VIOLATION
    let _unrelated = "rcp-error-log-allow";
    tracing::error!("failed: {}", error); // EXPECT-VIOLATION
}

// A raw string used to desynchronize this lexer for the REST of the file: the quote inside `r#"…"#`
// read as the terminator, the `(` in the body counted as one of the call's own parens, and the
// collector wedged. The violation below then simply vanished — and a vanished violation is
// indistinguishable from a clean file. This case is what proves the lexer RECOVERED, rather than
// merely that it did not crash; the clean-fixture cases only prove it stopped shouting.
fn a_violation_after_a_raw_string_is_still_reported() {
    tracing::debug!(r#"a quote " and an open paren ( in the body: {:#}"#, &error);
    let _multi = r##"a body holding "# -- which does not close a two-hash literal"##;
    tracing::error!("operation failed: {}", &error); // EXPECT-VIOLATION
}

// The same recovery, for a MULTI-LINE raw string — the shape that hid an entire file. Its body quotes
// a `tracing` call, as a usage message naturally would; read as code that armed the collector on
// prose, whose parens never balanced because every following line was correctly dropped as body. The
// violation below is what proves the collector came back, rather than merely that nothing crashed.
fn a_violation_after_a_multi_line_raw_string_is_still_reported() {
    let _usage = r#"Bad: tracing::error!("failed: {}",
    e)
Good: tracing::error!("failed: {:#}", e)
"#;
    tracing::error!("operation failed: {}", &error); // EXPECT-VIOLATION
}

// A marker in a raw string's BODY is prose, not a justification. The opener line strips to blank once
// its body is dropped — the same shape a comment line has — so arming has to require an actual `//`
// comment, which is the only placement AGENTS.md documents, rather than the marker text appearing
// anywhere on a line that happens to strip empty.
fn a_marker_inside_a_raw_string_does_not_arm_the_opt_out() {
    let _usage =
        r#"rcp-error-log-allow: prose inside a literal, not a justification
"#; tracing::error!("operation failed: {}", &error); // EXPECT-VIOLATION
}

// a marker misplaced INSIDE a call neither silences that call nor leaks to the next one. Without the
// second guarantee a stray marker silently disabled the check for whatever followed it.
fn misplaced_marker_neither_applies_nor_leaks() {
    tracing::error!( // EXPECT-VIOLATION
        // rcp-error-log-allow: misplaced - the contract is the line ABOVE the macro
        "still flagged: {}",
        &error
    );
    tracing::error!("also still flagged: {}", &error); // EXPECT-VIOLATION
}
