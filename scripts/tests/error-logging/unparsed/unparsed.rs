// Fixture for scripts/test-check-error-logging.sh — NOT part of any crate and never compiled.
//
// The `UNPARSED` guard covers one situation: the lexer reaches end of file still collecting a call.
// It stops checking the file at that point, so everything after goes unexamined, and a linter that
// stayed quiet would report "passed" for a file it barely read. That is the one outcome this check
// must never produce, so the guard is pinned here.
//
// This fixture used to be `tracing::debug!(r#"quote " then an open paren ("#);` — a construct the
// lexer did not understand YET. Once it learned raw strings, that line parsed cleanly, the guard
// stopped firing, and the test kept passing while proving nothing. What is below instead is a call
// whose parens GENUINELY never balance: no amount of better lexing finds its terminator, because the
// file ends first.
//
// The trade the replacement makes, deliberately: unlike its predecessor this is not valid Rust, so
// an editor shows a permanent syntax error on it. It has to — an end-of-file wedge is precisely what
// "not valid Rust" means here, and a fixture that is valid Rust is a fixture some future lexer can
// learn its way out of. It also still needs its own file for the blast radius: dropped into
// violations.rs it would swallow every EXPECT-VIOLATION marker below it.

// the call the collector can never terminate: `foo(` is never closed, by this line or any other
fn the_call_that_never_ends() {
    tracing::error!("unterminated", foo(

// unreachable for the linter, which is the point — the guard exists so that this being missed is
// ANNOUNCED rather than silently tolerated
fn a_real_violation_hidden_behind_the_wedge() {
    tracing::error!("operation failed: {}", &error);
}
