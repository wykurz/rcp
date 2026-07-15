//! TOCTOU-safety verdict logic for `--toctou-check` and `--require-toctou-safe`.
//!
//! This module provides the [`toctou_verdict`] function used by `--toctou-check` and
//! `--require-toctou-safe` on every RCP tool. The verdict reflects whether the
//! invocation uses the TOCTOU-hardened walk: `safe = !dereference && linux`.
//!
//! `--require-toctou-safe` additionally enforces the strict operand contract: every
//! operand must be absolute and lexically normal ([`strict_operand_violation`]), and
//! on Proceed the linter arms process-wide strict operand resolution
//! ([`crate::safedir::enable_strict_operand_resolution`]) so every operand
//! root/parent open resolves with `openat2(RESOLVE_NO_SYMLINKS)` — a symlink in any
//! component of an operand path fails closed at the open. What remains the caller's
//! responsibility is keeping the directories along the operand paths out of a
//! less-privileged actor's *write* control (a writer can still rename real
//! directories into place). See the "Scope of TOCTOU safety" section of
//! `docs/tocttou.md` for the authoritative definition of the boundary.

/// Normalized inputs used to compute a TOCTOU-safety verdict.
///
/// Each tool populates this from its parsed CLI flags. Tools without a
/// `--dereference` flag (rchm, rrm) always pass `dereference: false`.
#[derive(Debug, Clone)]
pub struct VerdictInputs {
    /// Whether `--dereference` / `-L` was requested (following symlinks).
    pub dereference: bool,
}

/// The result of a TOCTOU-safety analysis.
#[derive(Debug, Clone)]
pub struct Verdict {
    /// Whether the invocation is considered TOCTOU-safe.
    pub safe: bool,
    /// Human-readable reasons why the invocation is NOT safe (empty when `safe == true`).
    pub reasons: Vec<String>,
    /// Caveats that apply even when `safe == true` (trusted-boundary statements).
    pub caveats: Vec<String>,
}

impl Verdict {
    /// Render a human-readable summary of this verdict.
    pub fn render(&self) -> String {
        let mut out = String::new();
        if self.safe {
            out.push_str("TOCTOU status: SAFE\n");
        } else {
            out.push_str("TOCTOU status: NOT SAFE\n");
            for reason in &self.reasons {
                out.push_str(&format!("  Reason: {}\n", reason));
            }
        }
        for caveat in &self.caveats {
            out.push_str(&format!("  Note: {}\n", caveat));
        }
        out
    }
}

/// Compute the TOCTOU-safety verdict for an invocation described by `inputs`.
///
/// An invocation is NOT safe only when:
/// - `dereference` is true (`--dereference`/`-L` follows symlinks by request), or
/// - the build target is non-Linux (the hardened path is Linux-only).
///
/// All other flags (`--delete`, remote, filtering) are now hardened and do NOT
/// affect the verdict.
///
/// The verdict reflects only whether the hardened walk is in use. It does NOT — and
/// cannot — vouch for the trust of the operand path's prefix; that is the caller's
/// responsibility (see the "Scope of TOCTOU safety" section of `docs/tocttou.md`).
/// The "safe" verdict therefore always carries a caveat stating the trusted-boundary
/// assumption the caller must ensure.
pub fn toctou_verdict(inputs: &VerdictInputs) -> Verdict {
    let linux_build = cfg!(target_os = "linux");
    let safe = !inputs.dereference && linux_build;

    let mut reasons = Vec::new();
    if inputs.dereference {
        reasons.push(
            "--dereference/-L follows symlinks by request, so a swapped link is followed \
            — not hardened under privilege asymmetry"
                .to_string(),
        );
    }
    if !linux_build {
        reasons
            .push("the TOCTOU-hardened path is Linux-only; this build does not use it".to_string());
    }

    let caveats = vec![
        "Hardening assumes the directory named on the command line (and the path components \
        above it) are not modifiable by a less-privileged actor; it protects everything at or \
        below the named root. Also assumes fs.protected_hardlinks=1 (Linux default)."
            .to_string(),
    ];

    Verdict {
        safe,
        reasons,
        caveats,
    }
}

/// Returns why `path` violates the strict operand form required by
/// `--require-toctou-safe`, or `None` when the form is acceptable.
///
/// The strict form is: absolute, and lexically normal — no `.` or `..`
/// components and no empty (`//`) segments; a single trailing slash is allowed
/// (it carries copy-into meaning for destinations). `realpath` output always
/// satisfies it. The check is purely lexical: it guarantees the path *string*
/// can only denote the object literally at that path, so a wrapper or sudo
/// policy that validated the string validated the operand. The matching
/// *resolution* guarantee (no symlink in any component at open time) is
/// enforced separately via `openat2(RESOLVE_NO_SYMLINKS)` — see
/// [`crate::safedir::enable_strict_operand_resolution`].
pub fn strict_operand_violation(path: &std::path::Path) -> Option<String> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    if bytes.is_empty() {
        return Some("operand is empty".to_string());
    }
    if bytes[0] != b'/' {
        return Some(format!(
            "operand {path:?} is not absolute; --require-toctou-safe requires absolute, \
            fully-resolved operand paths (e.g. the output of realpath)"
        ));
    }
    let segments: Vec<&[u8]> = bytes[1..].split(|byte| *byte == b'/').collect();
    for (index, segment) in segments.iter().enumerate() {
        let last = index + 1 == segments.len();
        if segment.is_empty() && !last {
            return Some(format!(
                "operand {path:?} contains an empty path segment (`//`); \
                --require-toctou-safe requires lexically normal operand paths \
                (e.g. the output of realpath)"
            ));
        }
        if *segment == b"." || *segment == b".." {
            return Some(format!(
                "operand {path:?} contains a `{}` component; --require-toctou-safe \
                requires lexically normal operand paths (e.g. the output of realpath)",
                String::from_utf8_lossy(segment)
            ));
        }
    }
    None
}

/// Result of the CLI linter check, indicating whether the caller should proceed.
///
/// When this is `Exit { code }`, the caller must print `output` and exit with
/// `code` WITHOUT starting the operation. When this is `Proceed`, the caller
/// continues normally.
#[derive(Debug)]
pub enum LinterAction {
    /// Caller should print the message and exit with this code.
    Exit { output: String, code: i32 },
    /// Caller should proceed with the normal operation.
    Proceed,
}

/// Run the TOCTOU CLI linter checks.
///
/// Call this in each tool's `main()` after argument parsing, before starting
/// the async runtime / operation. Returns [`LinterAction::Exit`] when either
/// `toctou_check` or `require_toctou_safe` demands early exit, or
/// [`LinterAction::Proceed`] when the tool should run normally.
///
/// The verdict itself operates purely on the flags (`!dereference && linux`).
/// Under `--require-toctou-safe` the linter additionally enforces the strict
/// operand contract on `operands`: each must be absolute and lexically normal
/// ([`strict_operand_violation`]), the kernel must support `openat2(2)`, and on
/// Proceed strict operand resolution is armed process-wide
/// ([`crate::safedir::enable_strict_operand_resolution`]) so every operand
/// root/parent open resolves with `RESOLVE_NO_SYMLINKS`. What remains the
/// caller's responsibility is keeping the directories along the operand paths
/// out of a less-privileged actor's *write* control (a writer can still rename
/// real directories); see the "Scope of TOCTOU safety" section of
/// `docs/tocttou.md`.
///
/// # Parameters
///
/// - `dereference`: whether `--dereference`/`-L` is set (false for rchm/rrm).
/// - `toctou_check`: whether `--toctou-check` was passed.
/// - `require_toctou_safe`: whether `--require-toctou-safe` was passed.
/// - `operands`: every path operand as written on the command line (for remote
///   operands, the path part). rcpd passes `&[]` — its operands arrive from the
///   master, which has already validated them.
pub fn run_linter(
    dereference: bool,
    toctou_check: bool,
    require_toctou_safe: bool,
    operands: &[std::path::PathBuf],
) -> LinterAction {
    if !toctou_check && !require_toctou_safe {
        return LinterAction::Proceed;
    }

    let inputs = VerdictInputs { dereference };
    let verdict = toctou_verdict(&inputs);

    if toctou_check {
        // print verdict and exit — no operation performed. The exit code reflects
        // only the verdict; strict-form notes below are informational, telling the
        // caller whether --require-toctou-safe would accept these operands.
        let code = if verdict.safe { 0 } else { 1 };
        let mut output = verdict.render();
        for operand in operands {
            if let Some(violation) = strict_operand_violation(operand) {
                output.push_str(&format!(
                    "  Note: --require-toctou-safe would refuse this invocation: {}\n",
                    violation
                ));
            }
        }
        if !crate::safedir::openat2_available() {
            output.push_str(
                "  Note: --require-toctou-safe would refuse this invocation: the kernel \
                lacks openat2(2) (Linux 5.6+), so strict operand resolution is unavailable\n",
            );
        }
        return LinterAction::Exit { output, code };
    }

    // --require-toctou-safe mode: refuse when the hardened walk is not in use, when
    // an operand violates the strict form, or when the kernel cannot enforce
    // strict resolution.
    let mut reasons: Vec<String> = verdict.reasons.clone();
    if verdict.safe {
        reasons.extend(operands.iter().filter_map(|p| strict_operand_violation(p)));
        if !crate::safedir::openat2_available() {
            reasons.push(
                "the kernel lacks openat2(2) (Linux 5.6+), so strict operand resolution \
                is unavailable"
                    .to_string(),
            );
        }
    }
    if !reasons.is_empty() {
        let mut msg = "Refusing to run: invocation is not TOCTOU-safe.\n".to_string();
        for reason in &reasons {
            msg.push_str(&format!("  Reason: {}\n", reason));
        }
        return LinterAction::Exit {
            output: msg,
            code: 1,
        };
    }

    // arm strict operand resolution for the rest of the process: every operand
    // root/parent open now resolves with openat2(RESOLVE_NO_SYMLINKS).
    crate::safedir::enable_strict_operand_resolution();
    LinterAction::Proceed
}

/// Run the TOCTOU CLI linter and act on its verdict: print the message and exit the process when
/// the linter demands it ([`LinterAction::Exit`]), or return so the caller proceeds
/// ([`LinterAction::Proceed`]).
///
/// This is the print-and-exit half of [`run_linter`], shared verbatim by every tool's `main()` so
/// the print/exit policy lives in one place (the testable verdict core stays in [`run_linter`] /
/// [`toctou_verdict`]). Call it immediately after argument parsing, before the async runtime / any
/// filesystem operation. `dereference` is `false` for tools without a `--dereference` flag
/// (rchm/rrm/rlink). `operands` is every path operand as written (see [`run_linter`]).
pub fn enforce_or_exit(
    dereference: bool,
    toctou_check: bool,
    require_toctou_safe: bool,
    operands: &[std::path::PathBuf],
) {
    match run_linter(dereference, toctou_check, require_toctou_safe, operands) {
        LinterAction::Exit { output, code } => {
            print!("{}", output);
            std::process::exit(code);
        }
        LinterAction::Proceed => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------------------
    // Verdict tests
    // ---------------------------------------------------------------------------

    #[test]
    fn no_dereference_is_safe() {
        let v = toctou_verdict(&VerdictInputs { dereference: false });
        if cfg!(target_os = "linux") {
            assert!(v.safe, "no-dereference on Linux should be safe");
            assert!(v.reasons.is_empty(), "no reasons expected for safe verdict");
        } else {
            // non-Linux: not safe due to platform
            assert!(!v.safe);
        }
        assert!(
            !v.caveats.is_empty(),
            "caveats must be present even when safe"
        );
    }

    #[test]
    fn dereference_is_not_safe() {
        let v = toctou_verdict(&VerdictInputs { dereference: true });
        assert!(!v.safe, "dereference must make the verdict not-safe");
        assert!(
            !v.reasons.is_empty(),
            "at least one reason must be present when not safe"
        );
        // the dereference reason must be in the list
        assert!(
            v.reasons
                .iter()
                .any(|r| r.contains("dereference") || r.contains("-L")),
            "reason must mention dereference/-L, got: {:?}",
            v.reasons
        );
    }

    #[test]
    fn caveats_always_present() {
        for deref in [false, true] {
            let v = toctou_verdict(&VerdictInputs { dereference: deref });
            assert!(
                !v.caveats.is_empty(),
                "caveats must be present regardless of verdict (deref={})",
                deref
            );
            // the trusted-boundary caveat must be there
            assert!(
                v.caveats
                    .iter()
                    .any(|c| c.contains("named on the command line")),
                "trusted-boundary caveat must be present, got: {:?}",
                v.caveats
            );
        }
    }

    #[test]
    fn render_safe_contains_safe() {
        let v = toctou_verdict(&VerdictInputs { dereference: false });
        let rendered = v.render();
        if cfg!(target_os = "linux") {
            assert!(
                rendered.contains("SAFE"),
                "rendered output must contain SAFE: {rendered}"
            );
        }
    }

    #[test]
    fn render_not_safe_contains_not_safe() {
        let v = toctou_verdict(&VerdictInputs { dereference: true });
        let rendered = v.render();
        assert!(
            rendered.contains("NOT SAFE"),
            "rendered output must contain NOT SAFE: {rendered}"
        );
    }

    // ---------------------------------------------------------------------------
    // Strict operand form tests
    // ---------------------------------------------------------------------------

    #[test]
    fn strict_form_accepts_absolute_normalized_paths() {
        for ok in ["/a/b", "/a/b/", "/", "/a"] {
            assert!(
                strict_operand_violation(std::path::Path::new(ok)).is_none(),
                "expected {ok:?} to be accepted"
            );
        }
    }

    #[test]
    fn strict_form_rejects_relative_paths() {
        for bad in ["a/b", ".", "..", "./a", "../a", ""] {
            let violation = strict_operand_violation(std::path::Path::new(bad));
            assert!(violation.is_some(), "expected {bad:?} to be rejected");
        }
        let msg = strict_operand_violation(std::path::Path::new("a/b")).unwrap();
        assert!(
            msg.contains("absolute"),
            "relative-path message must mention absolute, got: {msg}"
        );
    }

    #[test]
    fn strict_form_rejects_dot_and_dotdot_components() {
        for bad in ["/a/../b", "/a/./b", "/a/..", "/a/.", "/.."] {
            let violation = strict_operand_violation(std::path::Path::new(bad));
            assert!(violation.is_some(), "expected {bad:?} to be rejected");
        }
        let msg = strict_operand_violation(std::path::Path::new("/a/../b")).unwrap();
        assert!(
            msg.contains(".."),
            "dotdot message must name the component, got: {msg}"
        );
    }

    #[test]
    fn strict_form_rejects_empty_segments() {
        for bad in ["//a", "/a//b", "/a/b//"] {
            let violation = strict_operand_violation(std::path::Path::new(bad));
            assert!(violation.is_some(), "expected {bad:?} to be rejected");
        }
    }

    // ---------------------------------------------------------------------------
    // Linter operand-enforcement tests
    // ---------------------------------------------------------------------------

    #[cfg(target_os = "linux")]
    #[test]
    fn require_mode_rejects_bad_operand() {
        let operands = vec![std::path::PathBuf::from("rel/path")];
        match run_linter(false, false, true, &operands) {
            LinterAction::Exit { output, code } => {
                assert_eq!(code, 1, "bad operand must exit 1");
                assert!(
                    output.contains("rel/path") && output.contains("absolute"),
                    "message must name the operand and the requirement, got: {output}"
                );
            }
            LinterAction::Proceed => panic!("bad operand must not proceed"),
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn require_mode_lists_every_bad_operand() {
        let operands = vec![
            std::path::PathBuf::from("rel/src"),
            std::path::PathBuf::from("/ok/dst"),
            std::path::PathBuf::from("/bad/../dst"),
        ];
        match run_linter(false, false, true, &operands) {
            LinterAction::Exit { output, .. } => {
                assert!(
                    output.contains("rel/src") && output.contains("/bad/../dst"),
                    "all violations must be listed, got: {output}"
                );
            }
            LinterAction::Proceed => panic!("bad operands must not proceed"),
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn require_mode_arms_strict_resolution_on_proceed() {
        // one process per test under nextest, so observing the process-global is safe
        assert!(
            !crate::safedir::strict_operand_resolution(),
            "strict mode must be off before the linter proceeds"
        );
        let operands = vec![
            std::path::PathBuf::from("/ok/src"),
            std::path::PathBuf::from("/ok/dst"),
        ];
        match run_linter(false, false, true, &operands) {
            LinterAction::Proceed => {
                assert!(
                    crate::safedir::strict_operand_resolution(),
                    "linter Proceed in require mode must arm strict operand resolution"
                );
            }
            LinterAction::Exit { output, code } => {
                // on kernels without openat2 the refusal is the correct outcome
                assert!(
                    !crate::safedir::openat2_available(),
                    "good operands must proceed on openat2-capable kernels, got: {output}"
                );
                assert_eq!(code, 1);
                assert!(output.contains("openat2"), "got: {output}");
            }
        }
    }

    #[test]
    fn require_mode_flag_refusal_suppresses_operand_reasons() {
        // when the verdict itself is not safe (-L), the refusal lists only the verdict
        // reasons — operand strict-form violations are not appended to the noise
        let operands = vec![std::path::PathBuf::from("rel/path")];
        match run_linter(true, false, true, &operands) {
            LinterAction::Exit { output, code } => {
                assert_eq!(code, 1);
                assert!(
                    output.contains("dereference") || output.contains("-L"),
                    "the -L reason must be present, got: {output}"
                );
                assert!(
                    !output.contains("rel/path"),
                    "operand reasons must be suppressed when the flag verdict already \
                    refuses, got: {output}"
                );
            }
            LinterAction::Proceed => panic!("-L must not proceed under require mode"),
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn require_mode_with_no_operands_proceeds() {
        // rcpd passes no operands (they arrive via the master, already validated)
        match run_linter(false, false, true, &[]) {
            LinterAction::Proceed => {}
            LinterAction::Exit { output, code } => {
                // on kernels without openat2 the refusal is the correct outcome
                assert!(
                    !crate::safedir::openat2_available(),
                    "empty operand list must proceed on openat2-capable kernels, got: {output}"
                );
                assert_eq!(code, 1);
                assert!(output.contains("openat2"), "got: {output}");
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn check_mode_keeps_verdict_but_notes_bad_operand() {
        let operands = vec![std::path::PathBuf::from("rel/path")];
        match run_linter(false, true, false, &operands) {
            LinterAction::Exit { output, code } => {
                assert_eq!(code, 0, "check-mode exit code must stay verdict-based");
                assert!(
                    output.contains("SAFE"),
                    "verdict must be unchanged, got: {output}"
                );
                assert!(
                    output.contains("rel/path") && output.contains("--require-toctou-safe"),
                    "check mode must note the operand strict-form violation, got: {output}"
                );
            }
            LinterAction::Proceed => panic!("check mode always exits"),
        }
    }
}
