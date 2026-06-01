//! TOCTOU-safety verdict logic for `--toctou-check` and `--require-toctou-safe`.
//!
//! This module provides the [`toctou_verdict`] function used by `--toctou-check` and
//! `--require-toctou-safe` on every RCP tool. The verdict reflects whether the
//! invocation uses the TOCTOU-hardened walk: `safe = !dereference && linux`.
//!
//! The tools do NOT verify the trust of the operand path's prefix (the directories
//! above the named root) — that is the caller's responsibility (the sudo policy or a
//! vetted wrapper passes resolved, trusted operands). See the "Scope of TOCTOU safety"
//! section of `docs/tocttou.md` for the authoritative definition of the boundary.

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
/// The linter operates purely on the verdict (`!dereference && linux`). It does NOT
/// inspect the operand paths: the trust of a path's prefix is the caller's
/// responsibility (see the "Scope of TOCTOU safety" section of `docs/tocttou.md`).
/// `--require-toctou-safe` is the tool's half of that contract — it refuses to run
/// unless the hardened walk is in use (rejecting `--dereference`/`-L` and non-Linux
/// builds) — but it does not, and cannot, vouch for prefix trust.
///
/// # Parameters
///
/// - `dereference`: whether `--dereference`/`-L` is set (false for rchm/rrm).
/// - `toctou_check`: whether `--toctou-check` was passed.
/// - `require_toctou_safe`: whether `--require-toctou-safe` was passed.
pub fn run_linter(
    dereference: bool,
    toctou_check: bool,
    require_toctou_safe: bool,
) -> LinterAction {
    if !toctou_check && !require_toctou_safe {
        return LinterAction::Proceed;
    }

    let inputs = VerdictInputs { dereference };
    let verdict = toctou_verdict(&inputs);

    if toctou_check {
        // print verdict and exit — no operation performed
        let code = if verdict.safe { 0 } else { 1 };
        return LinterAction::Exit {
            output: verdict.render(),
            code,
        };
    }

    // --require-toctou-safe mode: refuse only when the hardened walk is not in use.
    if !verdict.safe {
        let mut msg = "Refusing to run: invocation is not TOCTOU-safe.\n".to_string();
        for reason in &verdict.reasons {
            msg.push_str(&format!("  Reason: {}\n", reason));
        }
        return LinterAction::Exit {
            output: msg,
            code: 1,
        };
    }

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
/// (rchm/rrm/rlink).
pub fn enforce_or_exit(dereference: bool, toctou_check: bool, require_toctou_safe: bool) {
    match run_linter(dereference, toctou_check, require_toctou_safe) {
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
}
