#!/bin/bash
# MSRV-aware dependency update
#
# Replaces Dependabot's cargo ecosystem. Dependabot always offers the newest version of every crate
# (dependabot-core#5423), so a crate that raises its `rust-version` above ours reddens the CI `msrv`
# job and blocks every other bump in the batch. Both tools used here read `rust-version` instead:
#
#   cargo upgrade  -- bumps requirements in the manifests; respects `rust-version` by default
#                     (`--ignore-rust-version` is the opt-out). `--incompatible allow` is what makes
#                     it cross semver-major boundaries, matching what Dependabot used to do.
#
#                     `--compatible` is left at its default of `allow`, so a requirement is also
#                     tightened when the new version is semver-compatible: `tokio = "1.52"` becomes
#                     `"1.53"` even though the old range still matched. This is a deliberate choice
#                     to update aggressively, and it is where the tool differs most visibly from
#                     Dependabot, which rewrites a requirement only when forced to. The trade is
#                     that our published crates get a rising minimum-version floor; `--compatible
#                     ignore` is the knob if that ever becomes a problem for a downstream user.
#   cargo update   -- refreshes the lockfile; MSRV-aware because the workspace sets resolver = "3".
#                     This is the half that keeps a *transitive* dependency from raising the floor,
#                     which `cargo upgrade` cannot see.
#
# Neither is a hard guarantee, so the CI `msrv` job remains the backstop, for two distinct reasons:
#
#   1. Neither can see a crate that raises its *real* MSRV without declaring `rust-version`.
#   2. Resolver 3's rust-version handling is a *preference*, not a constraint. Where a requirement
#      admits no MSRV-compatible version at all, the documented `fallback` behaviour is to pick an
#      incompatible one rather than fail -- see the "rust-version" section of the Cargo resolver
#      reference. So the lockfile this produces can still exceed the MSRV; the cargo-update
#      workflow runs `just msrv` before opening a pull request precisely to catch that.
#
# Anything held back for MSRV reasons is reported rather than silently skipped -- see the report
# filter below, whose behaviour is pinned by scripts/test-update-deps.sh.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

DRY_RUN=0
REPORT_OUT=""
REPORT_FROM=""

usage() {
    cat <<'EOF'
Usage: update-deps.sh [OPTIONS]

  -n, --dry-run           Show what would change without touching the tree
      --report-out FILE   Write the "held back by MSRV" report (markdown) to FILE
      --report-from FILE  Only run the report filter over FILE, printing the result.
                          Reads `cargo update --verbose` output; used by the tests.
  -h, --help              Show this help
EOF
}

# Take the value of an option, refusing anything that looks like another option. Without this,
# `--report-out --dry-run` silently consumes the flag as the path and leaves DRY_RUN=0 -- so a
# command that reads as a preview performs a real update instead. Failing is the only safe answer:
# a missing path is a typo, never an intent to write to a file named "--dry-run".
#
# Called as a plain command, never in a command substitution: `exit` inside `$(...)` would only
# leave the subshell and the caller would sail on with an empty value.
require_value() {
    case "${2-}" in
        "" | -*)
            echo -e "${RED}ERROR: $1 needs a path, got '${2-}'${NC}" >&2
            exit 2
            ;;
    esac
}

while [ $# -gt 0 ]; do
    case "$1" in
        -n | --dry-run) DRY_RUN=1 ;;
        --report-out)
            require_value "$1" "${2-}"
            REPORT_OUT="$2"
            shift
            ;;
        --report-from)
            require_value "$1" "${2-}"
            REPORT_FROM="$2"
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo -e "${RED}ERROR: unknown argument '$1'${NC}" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done

# Resolve the report paths against the caller's directory before moving out of it, so a relative
# path still means what the caller meant.
case "$REPORT_OUT" in "" | /*) ;; *) REPORT_OUT="$PWD/$REPORT_OUT" ;; esac
case "$REPORT_FROM" in "" | /*) ;; *) REPORT_FROM="$PWD/$REPORT_FROM" ;; esac

# Anchor every cargo invocation to this script's own workspace. Otherwise the lockfile guarded
# below is *this* repository's while cargo operates on whatever directory the caller happened to be
# in -- so running the script by path from another project would update that project's dependencies
# and then dutifully restore this one's lockfile. `just update-deps` and the workflow always run
# from the root, so this only matters for direct invocation, which is exactly when it is silent.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Extract the MSRV hold-backs from `cargo update --verbose` output. Under resolver 3 cargo reports
# each package it declined to advance, and names the reason only when that reason is the MSRV:
#
#   Unchanged enum-map v2.7.3 (available: v3.1.0, requires Rust 1.95)   <- ours
#   Unchanged matchit v0.8.4 (available: v0.8.6)                        <- held for another reason
#
# Matching on the `requires Rust` suffix is therefore what separates the two. A line format change
# upstream makes a hold-back go unreported -- benign, since nothing lands that shouldn't -- so this
# is a single anchored substitution rather than anything cleverer.
#
# The suffix is not compared against our own `rust-version`, because cargo only emits it when the
# available version actually exceeds it. Confirmed directly: a crate held back by an exact `=` pin,
# whose newer version declares a rust-version BELOW ours, is reported with no suffix at all --
#
#   Unchanged serde v1.0.228 (available: v1.0.229)     # 1.0.229 declares rust-version 1.56
#   Unchanged enum-map v2.7.3 (available: v3.1.0, requires Rust 1.95)
#
# -- so a reported version is by construction greater than the MSRV, and parsing/comparing versions
# here would add nothing but a bespoke version comparator. The fixtures are kept consistent with
# that: every `requires Rust` they contain is above the workspace MSRV, because cargo cannot
# produce one that is not.
msrv_report() {
    sed -nE 's/^[[:space:]]*Unchanged[[:space:]]+([^[:space:]]+)[[:space:]]+v([^[:space:]]+)[[:space:]]+\(available:[[:space:]]*v([^,)]+),[[:space:]]*requires Rust[[:space:]]+([^)]+)\)[[:space:]]*$/- `\1` \2 → \3 (needs Rust \4)/p' "$1"
}

# Render the markdown fragment the workflow drops into the pull request body. Empty (not absent)
# when nothing is held back, so the caller can distinguish "no hold-backs" from "never ran".
write_report_out() {
    [ -n "$REPORT_OUT" ] || return 0
    if [ -n "$1" ]; then
        {
            echo "### Held back by the MSRV"
            echo ""
            echo "$1"
            echo ""
            echo "Each will advance automatically once \`rust-version\` is raised past what it needs."
        } > "$REPORT_OUT"
    else
        : > "$REPORT_OUT"
    fi
}

# Test hook: skip the cargo work entirely and filter a captured `cargo update --verbose` output.
# Only the report goes to stdout, so fixtures can compare it directly; --report-out is still
# honoured so the tests cover the pull-request rendering too, not just the filter.
if [ -n "$REPORT_FROM" ]; then
    [ -f "$REPORT_FROM" ] || { echo -e "${RED}ERROR: no such file: $REPORT_FROM${NC}" >&2; exit 2; }
    report=$(msrv_report "$REPORT_FROM")
    [ -z "$report" ] || echo "$report"
    write_report_out "$report"
    exit 0
fi

for tool in cargo cargo-upgrade; do
    command -v "$tool" > /dev/null 2>&1 || {
        echo -e "${RED}ERROR: $tool not found.${NC}"
        echo "Both are provided by the nix devshell: run 'nix develop' (or 'just update-deps',"
        echo "which is what CI invokes)."
        exit 1
    }
done

if [ "$DRY_RUN" -eq 1 ]; then
    # `--dry-run` is not enough to keep the tree untouched. cargo-edit resolves workspace metadata
    # before it honours the flag, and that resolution rewrites a lockfile which has fallen out of
    # step with the manifests -- observed directly: with one requirement edited and the lock left
    # stale, `update-deps.sh -n` changed Cargo.lock. A preview that edits files is a trap, so snap
    # the lock and put it back on every exit path, including Ctrl-C and any `set -e` abort.
    # Restoring only covers the case where a lockfile already existed. When it does not, cargo
    # *creates* one and the preview would leave that behind, so record which of the two states to
    # return to. One cleanup, on EXIT only: a handler on INT/TERM would run, resume the script,
    # and then run again at EXIT with its backup already deleted.
    lock_file="$REPO_ROOT/Cargo.lock"

    # Refuse anything that is not a plain file. The save/restore below assumes it can copy the
    # lockfile aside and put the bytes back, which is wrong for a symlink in both directions: `-f`
    # follows links, so a DANGLING one reads as absent, cargo then creates the target through it,
    # and the cleanup path deletes the symlink itself while the file it created survives elsewhere.
    # A live symlink is no better -- restoring would write through it, or replace it outright.
    # There is no sensible guess here, so stop rather than damage something deliberate.
    if [ -L "$lock_file" ]; then
        echo -e "${RED}ERROR: $lock_file is a symlink.${NC}" >&2
        echo "The dry run has to save and restore it, which would clobber the link or its target." >&2
        exit 1
    fi
    if [ -e "$lock_file" ] && [ ! -f "$lock_file" ]; then
        echo -e "${RED}ERROR: $lock_file is not a regular file.${NC}" >&2
        exit 1
    fi

    lock_backup="$(mktemp)"
    lock_existed=0
    if [ -f "$lock_file" ]; then
        lock_existed=1
        cp -p "$lock_file" "$lock_backup"
    fi
    restore_lock() {
        if [ "$lock_existed" -eq 1 ]; then
            cp -p "$lock_backup" "$lock_file"
        else
            rm -f "$lock_file"
        fi
        rm -f "$lock_backup"
    }
    trap restore_lock EXIT
    # Route signals through the same EXIT cleanup rather than duplicating it.
    trap 'exit 130' INT
    trap 'exit 143' TERM

    echo "🔍 Dependency updates available (dry run, nothing will be written):"
    echo ""
    cargo upgrade --dry-run --incompatible allow
    echo ""
    # These two previews are independent, not composed: --dry-run leaves the manifests alone, so
    # the lockfile preview below resolves the requirements as they are on disk today rather than
    # the ones the step above proposes. It therefore under-reports the transitive churn that the
    # requirement bumps would cause. Showing the true combined result would mean applying the
    # upgrade and rolling it back, which is a poor trade for a preview -- run the real thing on a
    # branch if you need the exact lockfile diff.
    echo "   (lockfile preview resolves today's requirements, not the upgrades proposed above)"
    cargo update --dry-run
else
    echo "⬆️  Upgrading manifest requirements..."
    cargo upgrade --incompatible allow
    echo ""
    echo "🔒 Refreshing the lockfile..."
    cargo update
fi

# Report last, so that on a real run this describes what is still held back *after* the upgrade
# rather than what was held back before it. Under --dry-run nothing was applied, so it necessarily
# describes the tree as it stands -- which is what a preview should show anyway.
#
# A failure here must not be mistaken for "nothing held back": an empty report is exactly what a
# crates.io outage would produce, and the caller would then open a pull request claiming a clean
# MSRV picture it never actually obtained.
#
# `--color never` is load-bearing, not tidiness: with colour on, cargo wraps the version fields in
# escape sequences and the anchored substitution below matches nothing at all. Measured on this
# workspace -- 2 of 3 hold-backs reported with colour off, 0 of 3 with it on. Colour is off by
# default when stdout is not a terminal, so CI would have been fine and a maintainer running it
# locally, or anyone with CARGO_TERM_COLOR=always, would silently have got an empty report.
echo ""
if ! update_output=$(cargo update --dry-run --verbose --color never 2>&1); then
    echo -e "${RED}ERROR: could not determine what is held back by the MSRV.${NC}" >&2
    echo "$update_output" >&2
    exit 1
fi
report=$(printf '%s\n' "$update_output" | msrv_report /dev/stdin)

# Cross-check the filter against the raw output it just parsed. The static fixtures pin the filter
# against a recorded format; they cannot notice cargo *changing* that format, which would turn every
# hold-back into silence -- the one failure this report exists to prevent. Comparing the two here
# uses whatever cargo actually shipped, so a reflow, a colour leak or a reworded suffix is caught on
# the real run rather than the next time someone happens to look.
#
# `requires Rust` alone is the wrong thing to count. Cargo uses it for two different situations:
#
#   Unchanged sysinfo v0.38.4 (available: v0.39.6, requires Rust 1.95)   held back -- ours
#   Unchanged enum-map v3.1.0 (requires Rust 1.95)                       SELECTED despite the MSRV
#
# The second is the resolver's fallback, where a requirement admitted no compatible version at all,
# and it has no `available:` because nothing better is being declined. Counting it made this check
# fire in exactly the case it must not: the fallback run, where the useful outcome is reaching the
# `just msrv` gate and getting cargo's own precise complaint. Those packages are the gate's job,
# not the report's -- match the pair of markers the report itself keys on.
raw_holdbacks=$(printf '%s\n' "$update_output" | grep "available:" | grep -c "requires Rust" || true)
report_lines=$(printf '%s' "$report" | grep -c . || true)
if [ "$raw_holdbacks" -ne "$report_lines" ]; then
    echo -e "${RED}ERROR: the MSRV report does not match cargo's output.${NC}" >&2
    echo "cargo reported $raw_holdbacks package(s) held back for Rust-version reasons," >&2
    echo "but the filter extracted $report_lines. Its output format has probably changed;" >&2
    echo "update the substitution in msrv_report() and the fixtures in scripts/tests/update-deps/." >&2
    printf '%s\n' "$update_output" | grep "requires Rust" >&2 || true
    exit 1
fi

if [ -n "$report" ]; then
    echo -e "${YELLOW}Held back by the MSRV:${NC}"
    echo "$report"
    echo ""
    echo "These are not failures. They will advance on their own once the MSRV is raised past the"
    echo "version each one needs -- there is no denylist to prune."
else
    echo -e "${GREEN}Nothing held back by the MSRV.${NC}"
fi

write_report_out "$report"
