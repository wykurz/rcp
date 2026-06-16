#!/bin/bash
# Verify every publishable workspace crate has the given version on crates.io.
#
# Release guard: `cargo workspaces publish` can swallow a per-crate failure (e.g. a 403
# when a scoped token can't create a new crate) and still exit 0, so a release can go
# green with a crate silently missing from crates.io (and therefore blank on docs.rs).
# This script is the source of truth for "did everything actually publish".
#
# Usage: scripts/check-crates-published.sh <version>     # e.g. 0.36.0
#
# Env overrides (for fast local testing):
#   CRATES_CHECK_RETRIES   attempts per crate before declaring it missing (default 5)
#   CRATES_CHECK_SLEEP     seconds between attempts (default 5)
#   CRATES_CHECK_THROTTLE  seconds between crates, per crates.io's rate policy (default 1)
set -euo pipefail

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
    echo "usage: $0 <version>" >&2
    exit 2
fi

RETRIES="${CRATES_CHECK_RETRIES:-5}"
SLEEP="${CRATES_CHECK_SLEEP:-5}"
THROTTLE="${CRATES_CHECK_THROTTLE:-1}"
UA="rcp-release-verify (https://github.com/wykurz/rcp)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Enumerate crates.io-publishable workspace crates. In cargo metadata, publish == null means
# "any registry" (publishable); publish == [] means `publish = false` (excluded); a registry
# list like ["crates-io"] is included only when it contains crates-io.
# cargo + python3 only (both present on CI runners and locally) — no jq dependency.
# Capture metadata in its own step first so a `cargo metadata` failure trips errexit here
# (surfacing cargo's own error) instead of silently yielding an empty list from the
# process substitution below.
metadata="$(cargo metadata --no-deps --format-version 1 --manifest-path "$ROOT_DIR/Cargo.toml")"
mapfile -t CRATES < <(
    printf '%s' "$metadata" \
        | python3 -c 'import sys, json; [print(p["name"]) for p in json.load(sys.stdin)["packages"] if p.get("publish") is None or (isinstance(p.get("publish"), list) and "crates-io" in p["publish"])]' \
        | sort
)

if [[ "${#CRATES[@]}" -eq 0 ]]; then
    echo "error: no publishable crates found in workspace metadata" >&2
    exit 1
fi

echo "Verifying ${#CRATES[@]} crate(s) at version ${VERSION} on crates.io..."

missing=()
for idx in "${!CRATES[@]}"; do
    crate="${CRATES[idx]}"
    # Throttle between crates to respect crates.io's ~1 req/s data-access policy. The
    # retry loop's SLEEP already covers the rare 429 path; this prevents the back-to-back
    # burst on the happy path. Skipped before the first crate.
    [[ "$idx" -gt 0 ]] && sleep "$THROTTLE"
    code=000
    for ((attempt = 1; attempt <= RETRIES; attempt++)); do
        code="$(curl -sS -o /dev/null -w '%{http_code}' \
            -A "$UA" \
            "https://crates.io/api/v1/crates/${crate}/${VERSION}" || echo 000)"
        [[ "$code" == "200" ]] && break
        [[ "$attempt" -lt "$RETRIES" ]] && sleep "$SLEEP"
    done
    if [[ "$code" == "200" ]]; then
        echo "OK      ${crate} ${VERSION}"
    else
        echo "MISSING ${crate} ${VERSION} (last HTTP ${code})"
        missing+=("$crate")
    fi
done

if [[ "${#missing[@]}" -gt 0 ]]; then
    echo ""
    echo "ERROR: ${#missing[@]} crate(s) missing at ${VERSION} on crates.io: ${missing[*]}" >&2
    exit 1
fi

echo ""
echo "All ${#CRATES[@]} crate(s) present at ${VERSION} on crates.io."
