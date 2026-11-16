#!/usr/bin/env bash
set -euo pipefail

# actionlint 1.7.12 predates GitHub's workflow/job cache-mode key. Suppress only that
# unknown-key diagnostic; keep all other syntax/context checks and our cache policy tests.
# optional shell/Python analyzers are outside this check's scope, even when installed
exec actionlint -shellcheck= -pyflakes= \
    -ignore '^unexpected key "cache-mode" for "(workflow|job)" section\.' "$@"
