// Fixture for scripts/test-check-error-logging.sh — NOT part of any crate and never compiled.
//
// Nothing below may be reported by scripts/check-error-logging.sh. These are the false positives a
// widened rule is most likely to introduce; without them, "catches more" and "shouts at everything"
// look identical from the outside.

fn correctly_formatted_errors() {
    tracing::error!("operation failed: {:#}", &error);
    tracing::error!("operation failed: {:?}", &error);
    tracing::warn!("operation failed: {e:#}");
    tracing::debug!("operation failed: {err:?}");
}

// a bare `{}` for some OTHER argument, with the error itself correctly formatted: the last
// placeholder is the error's, and it has a spec
fn a_bare_placeholder_for_a_non_error_argument() {
    tracing::error!("{} failed: {:#}", name, &error);
    tracing::error!("copying {} to {}: {:?}", src, dst, &error);
}

// no error argument at all — a bare `{}` is the normal way to log a value
fn ordinary_logging() {
    tracing::debug!("copied {} bytes", copied);
    tracing::info!("listening on {}", addr);
    tracing::debug!("{} of {} files", done, total);
}

// the error is not the last argument, so the last placeholder is not its
fn error_is_not_the_last_argument() {
    tracing::error!("failed: {:#} while copying {}", &error, dst);
}

// an explicit, justified opt-out on the line above
fn explicitly_allowed() {
    // rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
    tracing::error!("rcpd operation failed: {error}");
}

// macros that merely resemble a tracing call are out of scope
fn other_macros() {
    eprintln!("operation failed: {}", &error);
    println!("operation failed: {}", &error);
    anyhow::bail!("operation failed: {}", &error);
}

// COMMENTED-OUT code is not code. Reporting it made the linter reject valid Rust.
fn commented_out() {
    // tracing::error!("operation failed: {}", &error);
    // tracing::warn!("operation failed: {e}");
    let _ = 1;
}

// `{{` and `}}` are escapes for LITERAL braces, so neither is a placeholder
fn escaped_braces() {
    tracing::debug!("map literal {{}} here", e);
    tracing::debug!("json {{\"k\": 1}} for {}", name);
}

// names that merely CONTAIN "err" are not errors: a boundary is required, so `_err`/`_error` count
// and `stderr` / `errored` / `host_stderr` do not
fn names_that_are_not_errors() {
    tracing::warn!("cleanup failed (non-fatal): {}", stderr);
    tracing::info!("prune swap: pruned={pruned}, errored={errored}");
    tracing::debug!(host = %host_stderr, "rcpd stderr: {}", trimmed);
}

// `?x` records Debug, which preserves the chain; only `%x` (Display) is the mistake
fn debug_sigil_is_fine() {
    tracing::error!(?error, "operation failed");
}

// An error NAMED in a trailing comment is not an argument of the call. Each line below logs
// something else entirely; reading the comment as part of the argument list invented a violation and
// made the linter reject valid Rust.
fn an_error_named_in_a_trailing_comment_is_not_an_argument() {
    tracing::info!("copied {} files", count); // returns Err(error) on failure
    tracing::debug!("retrying {} times", n); // the error itself is logged by the caller
}

// prose INSIDE the format string is not an argument either — the argument scan reads the argument
// list, with string literals blanked out
fn prose_inside_the_format_string_is_not_an_argument() {
    tracing::info!("no error, just {} files", count);
    tracing::warn!("error budget exceeded: {} of {}", used, total);
}

// a match arm's own binding sits to the LEFT of the macro, so it is not one of the call's arguments.
// This is the single most common false positive a whole-line scan produces, because `Err(error) =>`
// introduces so many log lines.
fn a_match_arm_binding_is_not_an_argument() {
    match result {
        Err(error) => tracing::info!("falling back after {} attempts", n),
        Ok(_) => {}
    }
    match result {
        Err(err) => tracing::warn!("using {} of {} retries", used, total),
        Ok(_) => {}
    }
}

// `//` inside a string literal must not be cut as a comment: truncating there would drop the `{:#}`
// and leave a bare `{}` as the last placeholder
fn double_slash_inside_the_format_string() {
    tracing::debug!("connecting to ssh://{}: {:#}", host, &error);
}

// an argument that itself ends in `)` on its own rustfmt-split line does not end the call — the
// error two lines below is still the last argument, and it is correctly formatted
fn a_split_argument_ending_in_a_paren_does_not_end_the_call() {
    tracing::error!(
        "a very long message that pushes this call past the width limit: {} {:#}",
        compute(a),
        &error
    );
}

// `{{` and `}}` are LITERAL braces, so `{{error}}` interpolates nothing — it is the text `{error}`.
// The inline-capture rule has to strip the escapes first, exactly as the placeholder scans do.
fn escaped_braces_are_not_an_inline_capture() {
    tracing::debug!("literal {{error}} here");
    tracing::info!("the {{err}} placeholder is written {{err}}");
}

// a BLOCK comment is not code either, on any of its lines. Only the first line used to be skipped, so
// a commented-out call inside one was reported as a live violation.
fn a_block_commented_call_is_not_code() {
    /*
    tracing::error!("operation failed: {}", &error);
    tracing::warn!("operation failed: {e}");
    */
    let _ = 1;
}

// a structured field recording an error with `?` (Debug) preserves the chain, and a bare `{}` for some
// OTHER value on the same call does not change that
fn a_debug_sigil_field_alongside_a_bare_placeholder() {
    tracing::debug!(err = ?error, "count {}", n);
    tracing::info!(error = ?e, "copied {} of {}", done, total);
}

// The documented gap: a MIXED format string whose error argument is not last. Deciding this needs a
// Rust expression parser, so the rules stay quiet rather than guess — sound, not complete. If a
// future rule can decide it, move this case to the violations fixture.
fn known_gap_mixed_placeholders_with_a_non_final_error() {
    tracing::error!("failed: {:#} while copying {}", &error, dst);
    tracing::error!("{} failed: {} for {:#}", name, count, &error);
}
