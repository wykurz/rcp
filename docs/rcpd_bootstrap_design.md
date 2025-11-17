# rcpd Binary Discovery and Version Checking

This document describes the current implementation of automatic rcpd binary discovery and version checking for remote copy operations.

## Overview

The rcp remote copy system requires a matching version of rcpd to be installed on remote hosts. The current implementation (as of v0.22.0) provides:

1. **Automatic binary discovery** - Finds rcpd on remote hosts using multiple search strategies
2. **Version verification** - Ensures exact version match between local rcp and remote rcpd
3. **Static binaries** - Provides portable musl-based static binaries that work across all Linux distributions
4. **Clear error messages** - Guides users to fix version mismatches or missing binaries

## Architecture

### Version Information Structure

**Location**: `common/src/version.rs`

```rust
pub struct ProtocolVersion {
    pub semantic: String,              // e.g., "0.22.0"
    pub git_describe: Option<String>,  // e.g., "v0.21.1-7-g644da27"
    pub git_hash: Option<String>,      // Full commit hash
}
```

**Build-time embedding** (`common/build.rs`):
- Semantic version from `CARGO_PKG_VERSION` (always available)
- Git describe from `git describe --tags --long --always --dirty` (best effort)
- Git hash from `git rev-parse HEAD` (best effort)

### Binary Discovery

**Location**: `remote/src/lib.rs::discover_rcpd_path`

**Search strategy** (in order of priority):

1. **Explicit path** - From `--rcpd-path` CLI flag
   - Allows users to override discovery
   - Applies to whichever side is remote (source or destination)
   - Checked via SSH: `test -x /path/to/rcpd`

2. **Deployed cache** - Versioned binary in cache directory (`~/.cache/rcp/bin/rcpd-{version}`)
   - Used for binaries deployed by rcp itself
   - Ensures the exact version is available even if not installed system-wide
   - Checked via SSH on remote host

3. **Same directory** - Next to local rcp binary
   - Maintains backward compatibility with co-located installations
   - Path derived from `std::env::current_exe()`
   - Checked via SSH on remote host

4. **PATH** - Standard Unix locations
   - Uses `which rcpd` on remote host
   - Respects user's PATH configuration

**Graceful handling of HOME not set**:
- If `HOME` is not available on the remote host, the cache directory check is skipped
- Discovery continues with same-directory and PATH checks
- Error message indicates cache was skipped due to missing HOME

**Error handling**:
- If not found in any location, returns clear error with:
  - List of locations searched
  - Suggestion to use automatic deployment: `rcp --auto-deploy-rcpd ...`
  - Manual installation command: `cargo install rcp-tools-rcp --version X.Y.Z`
  - Suggestion to use `--rcpd-path` for custom locations

### Version Checking

**Location**: `remote/src/lib.rs::check_rcpd_version`

**Process**:

1. **Execute version command** on remote host:
   ```bash
   rcpd --protocol-version
   ```
   - Direct binary execution (no shell, prevents injection)
   - Returns JSON with version information

2. **Parse JSON response**:
   ```json
   {
     "semantic": "0.22.0",
     "git_describe": "v0.21.1-7-g644da27",
     "git_hash": "644da27abc..."
   }
   ```

3. **Compare versions**:
   - Current policy: **Exact semantic version match**
   - `rcp 0.22.0` requires `rcpd 0.22.0` exactly
   - Git information used for debugging, not compatibility checking

4. **Error handling**:
   - If version mismatch, returns error with:
     - Local version (with git info)
     - Remote version (with git info)
     - Remote hostname for context
     - Installation command with correct version
     - Shell-escaped hostname in commands for safe copy-paste

### Static Binary Distribution

**Configuration**: `.cargo/config.toml`

```toml
[build]
target = "x86_64-unknown-linux-musl"

[target.x86_64-unknown-linux-musl]
linker = "x86_64-unknown-linux-musl-gcc"
rustflags = ["--cfg", "tokio_unstable", "-C", "target-feature=+crt-static"]
```

**Benefits**:
- ✅ No dynamic library dependencies (except kernel)
- ✅ Works on all Linux distributions (glibc, musl, Alpine, etc.)
- ✅ No libc version conflicts
- ✅ Single binary can be deployed anywhere
- ✅ Verified with `ldd` showing "not a dynamic executable"

**Building**:
- Default: `cargo build` → musl static binary
- Glibc: `cargo build --target x86_64-unknown-linux-gnu`

## CLI Interface

### rcp flags

```bash
# Override rcpd binary path
rcp --rcpd-path=/path/to/rcpd [other options] src dst

# Enable automatic deployment
rcp --auto-deploy-rcpd [other options] src dst
```

- `--rcpd-path` - Override rcpd binary path for remote operations
  - Applies to whichever side is remote (source or destination)
  - Takes precedence over automatic discovery

- `--auto-deploy-rcpd` - Enable automatic deployment of rcpd to remote hosts
  - Deploys static rcpd binary when version mismatch detected
  - Binaries cached at `~/.cache/rcp/bin/rcpd-{version}` on remote hosts
  - Requires local rcpd binary (same directory or PATH)

### Version query

Both `rcp` and `rcpd` support version queries:

```bash
# Human-readable version
$ rcp --version
rcp 0.22.0

# Machine-readable protocol version (JSON)
$ rcp --protocol-version
{"semantic":"0.22.0","git_describe":"v0.21.1-7-g644da27","git_hash":"644da27..."}
```

**Design rationale**:
- Separate flags prevent users from depending on `--version` format
- `--protocol-version` provides stable machine-readable output
- Git information aids debugging without affecting compatibility

**Unix `--` separator support**:
- `rcp --protocol-version` → outputs version JSON
- `rcp -- --protocol-version` → treats `--protocol-version` as filename
- Follows standard Unix convention

## Execution Flow

### Remote Copy Initialization

When starting a remote copy operation:

1. **Setup SSH connection** to remote host
2. **Discover rcpd binary**:
   - Check `--rcpd-path` if provided
   - Check same directory as local rcp
   - Check PATH via `which`
   - Error if not found
3. **Check version**:
   - Run `rcpd --protocol-version` via SSH
   - Parse JSON response
   - Compare semantic versions
   - Error if mismatch
4. **Launch rcpd** with discovered path
5. **Establish QUIC connection** for data transfer

### Performance Considerations

**Version checking overhead**:
- Adds ~9-10 seconds per remote host connection
- Test timeouts increased from 20s to 35s to accommodate
- Future optimization: cache version check results across operations

**Trade-offs**:
- Overhead is acceptable for correctness guarantees
- Prevents cryptic failures from version mismatches
- Clear errors save debugging time

## Security Model

### Shell Injection Prevention

**Path handling**:
- All paths passed to shell commands are escaped using `shell_escape()`
- Prevents command injection via malicious paths
- Unit tests verify handling of paths with: spaces, quotes, semicolons, backticks

**Version checking**:
- Direct binary execution (not through shell)
- No string interpolation of paths in version check
- Command: `session.command(rcpd_path).arg("--protocol-version")`

**Error messages**:
- Hostnames are shell-escaped in suggested commands
- Users can safely copy-paste commands from error messages

### Trust Model

**SSH is the security perimeter**:
- All operations start with SSH authentication
- Version checking happens after SSH auth succeeds
- QUIC connections use certificate pinning for integrity

## Automatic Deployment

### Overview

**Status**: Implemented in v0.22.0

Automatic deployment allows rcp to transfer and install rcpd binaries to remote hosts automatically, eliminating the need for manual installation. This feature is opt-in via the `--auto-deploy-rcpd` flag.

**Location**: `remote/src/deploy.rs`

### Architecture

#### Deployment Workflow

When `--auto-deploy-rcpd` is enabled and a version mismatch is detected:

1. **Find local rcpd binary**:
   - Search same directory as rcp binary first (ensures matching build)
   - Fall back to PATH via `which rcpd`
   - Error if no suitable binary found

2. **Transfer binary to remote**:
   - Read local rcpd binary
   - Compute SHA-256 checksum
   - Base64 encode binary
   - Create target directory: `~/.cache/rcp/bin`
   - Transfer via SSH stdin with atomic rename
   - Verify checksum on remote

3. **Set permissions and cleanup**:
   - Set permissions to 700 (user-only execute)
   - Clean up old versions (keeps last 3)

4. **Use deployed binary**:
   - Update discovery to use cached binary
   - Reuse on subsequent operations (no re-deployment needed)

#### Binary Transfer Mechanism

**Base64 transfer over SSH**:

```bash
# Remote command executed via SSH
mkdir -p ~/.cache/rcp/bin && \
base64 -d > ~/.cache/rcp/bin/.rcpd-{version}.tmp.$$ && \
chmod 700 ~/.cache/rcp/bin/.rcpd-{version}.tmp.$$ && \
mv -f ~/.cache/rcp/bin/.rcpd-{version}.tmp.$$ ~/.cache/rcp/bin/rcpd-{version}
```

**Why base64**:
- Universal availability (POSIX standard)
- No external dependencies (no scp/rsync needed)
- Handles binary data safely through text channels
- Works with restricted shells

#### Atomic Deployment

**Unique temporary files**:
- Each deployment uses `.rcpd-{version}.tmp.$$` where `$$` is the shell PID
- Guarantees uniqueness even with concurrent deployments
- Prevents interference between multiple rcp instances

**Atomic rename**:
- Final step uses `mv -f` which is atomic on POSIX filesystems
- Binary is either fully present or not present at all (no partial writes visible)
- Concurrent renames complete in a well-defined order
- Readers see either old or new inode, never corruption

#### Integrity Verification

**SHA-256 checksum verification**:

1. **Before transfer**: Compute checksum of local binary
2. **After transfer**: Run `sha256sum` on remote to verify
3. **Compare**: Checksums must match exactly
4. **Fail on mismatch**: Indicates corruption or tampering

This ensures the transferred binary is identical to the local binary.

#### Caching and Cleanup

**Deployment location**:
- Binaries stored in `~/.cache/rcp/bin/rcpd-{version}`
- XDG standard cache location (user-specific, no sudo needed)
- Version-specific naming allows multiple versions and rollback

**Caching behavior**:
- Deployed binary is reused for all subsequent operations
- No re-deployment until version changes
- Cache persists across rcp invocations

**Automatic cleanup**:
```bash
# Keeps last 3 versions, removes older
cd ~/.cache/rcp/bin && ls -t rcpd-* | tail -n +4 | xargs -r rm
```

**Benefits**:
- Prevents unbounded disk usage
- Keeps recent versions for potential rollback
- Automatic maintenance, no user intervention needed

### Concurrency and Safety

#### Atomic Operations

1. **Unique temporary files**: Shell PID (`$$`) ensures each deployment has a unique temp file
2. **Atomic rename**: `mv -f` is atomic on POSIX filesystems
3. **Write-then-verify**: Binary is fully written, marked executable, moved atomically, then checksummed

#### Race Condition Scenarios

**Scenario 1: Multiple rcp instances deploying same version concurrently**
- Each uses unique temp file (`.rcpd-0.22.0.tmp.1234`, `.rcpd-0.22.0.tmp.5678`)
- Both write and verify their temp files successfully
- Both attempt `mv -f` to final location
- Filesystem ensures one wins atomically, the other overwrites atomically
- Result: Final binary is valid (both were identical and checksummed)

**Scenario 2: One deployment while another is reading**
- Reader opens `rcpd-0.22.0` and gets a valid file descriptor
- Writer completes deployment and `mv -f` replaces the inode
- Reader continues reading from the original inode (POSIX semantics)
- Result: Reader gets the old version (but it's still valid)

**Scenario 3: Deployment interrupted (network failure, SIGKILL)**
- Temp file may be left in `.cache/rcp/bin/.rcpd-{version}.tmp.*`
- Final file is either: not present (safe to retry) or present and valid (mv completed)
- Temp files are hidden (dotfiles) and don't interfere with discovery
- Result: Safe to retry; old temp files are harmless

#### Assumptions

1. **POSIX filesystem semantics**: Assumes atomic `mv` (rename) operations
   - True for ext4, xfs, btrfs, zfs
   - May not hold for NFSv3 without proper locking

2. **Unique shell PIDs**: `$$` is unique during deployment lifetime
   - Guaranteed by OS
   - Requires PIDs not wrap around extremely rapidly

3. **Checksum integrity**: SHA-256 collision resistance
   - Astronomically unlikely for different binaries to match

4. **No malicious interference**: Assumes remote host is not actively malicious
   - SSH authentication provides trust boundary

### CLI Interface

```bash
# Enable automatic deployment
rcp --auto-deploy-rcpd host1:/source host2:/dest --progress

# Works with other flags
rcp --auto-deploy-rcpd --preserve /local/data remote:/backup
```

**Flag behavior**:
- Only deploys if version mismatch detected
- Reuses cached binary if already deployed
- Fails with clear error if local rcpd not found

### Error Handling

**Local binary not found**:
```
no local rcpd binary found for deployment

Searched in:
- Same directory: /path/to/rcp/../rcpd
- PATH: (via 'which rcpd')

To use auto-deployment, ensure rcpd is available:
- cargo install rcp-tools-rcp (installs to ~/.cargo/bin)
- or add rcpd to PATH
- or build with: cargo build --release --bin rcpd
```

**Deployment failure**:
```
failed to transfer binary to remote host

stderr: Permission denied

This may indicate:
- Insufficient disk space on remote host
- Permission denied creating $HOME/.cache/rcp/bin
- base64 command not available on remote host
```

**Checksum mismatch**:
```
checksum mismatch after transfer

Expected: abc123...
Got:      def456...

The binary transfer may have been corrupted.
Please try again or check network connectivity.
```

### Testing

**Location**: `rcp/tests/remote_tests.rs`

**Integration tests**:
- `test_remote_auto_deploy` - Full deployment workflow
- `test_remote_auto_deploy_reuses_cached_binary` - Caching verification
- `test_auto_deploy_cleanup_old_versions` - Cleanup verification
- `test_auto_deploy_error_explicit_rcpd_not_found` - Local binary not found
- `test_auto_deploy_error_permission_denied` - Permission failure handling
- `test_auto_deploy_error_checksum_mismatch` - Checksum verification presence

**Test coverage**:
- ✅ Successful deployment and execution
- ✅ Binary caching and reuse
- ✅ Cleanup of old versions
- ✅ Error handling for missing binary, permissions, checksum issues

### Security Considerations

**Integrity**:
- SHA-256 checksums verify binary integrity after transfer
- Detects corruption or tampering
- Fails deployment if mismatch detected

**Authentication**:
- All transfers occur over authenticated SSH connections
- Same security model as manual scp/rsync
- No separate authentication required

**Trust model**:
- Local machine must be trusted (binary source)
- Deployed binary is whatever rcpd is found locally
- If local machine is compromised, deployed binary will be compromised
- Equivalent to trust model for master (rcp) binary itself

**File permissions**:
- Deployed binaries are 700 (user-only executable)
- Prevents unauthorized modification by other users
- Follows principle of least privilege

For comprehensive security analysis, see `docs/security.md`.

## Error Messages

### Binary Not Found

```
rcpd binary not found on remote host

Searched in:
- Same directory as local rcp binary
- PATH (via 'which rcpd')

Please install rcpd on the remote host and ensure it's in PATH:
- cargo install rcp-tools-rcp --version 0.22.0
Or specify the path explicitly:
- rcp --rcpd-path=/path/to/rcpd ...
```

With `--rcpd-path`:
```
rcpd binary not found on remote host

Searched in:
- Explicit path: /custom/path/rcpd (not found or not executable)
- Same directory as local rcp binary
- PATH (via 'which rcpd')

Please install rcpd on the remote host...
```

### Version Mismatch

```
rcpd version mismatch

Local:  rcp 0.22.0 (v0.21.1-7-g644da27)
Remote: rcpd 0.21.0 (v0.20.5-12-gf8a1b3c) on host 'prod-server'

The rcpd version on the remote host must exactly match the rcp version.

To fix this, install the matching version on the remote host:
- ssh prod-server 'cargo install rcp-tools-rcp --version 0.22.0'
```

### Old rcpd Without --protocol-version

```
rcpd --protocol-version failed on remote host 'prod-server'

stderr: unrecognized option '--protocol-version'

This may indicate an old version of rcpd that does not support --protocol-version.
Please install a matching version of rcpd on the remote host:
- cargo install rcp-tools-rcp --version 0.22.0
```

## Testing

### Unit Tests

**Location**: `common/src/version.rs`

- `test_current_version` - Verifies version info is available
- `test_exact_version_compatibility` - Tests compatibility logic
- `test_display` - Verifies human-readable output
- `test_json_serialization` - Round-trip JSON serialization
- `test_json_deserialization_without_git` - Handles missing git info

### Integration Tests

**Location**: `rcp/tests/cli_parsing_tests.rs`

- `test_protocol_version_has_git_info` - Verifies rcp outputs valid JSON with git info
- `test_rcpd_protocol_version_has_git_info` - Verifies rcpd outputs git info
- `test_protocol_version_after_separator_is_filename` - Tests `--` separator handling
- `test_protocol_version_before_separator_works` - Tests flag before `--`

**Location**: `rcp/tests/remote_tests.rs`

- Remote copy tests use 35-second timeout to accommodate version checking
- Tests verify end-to-end remote copy with version checking

## Design Decisions

### Exact Version Matching

**Decision**: Require exact semantic version match (`0.22.0` == `0.22.0`)

**Rationale**:
- Strictest policy during active development
- Protocol changes are frequent
- Prevents subtle bugs from version skew
- Future: can relax to minor version tolerance after v1.0

**Alternatives considered**:
- Minor version tolerance (`0.22.5` accepts `0.22.x` where `x >= 5`)
- Protocol version separate from tool version
- Git hash matching (too strict)

### Musl as Default Target

**Decision**: Build static musl binaries by default

**Rationale**:
- Eliminates "works on my machine" issues
- Single binary works everywhere
- No dependency on specific libc version
- Critical for distributed deployments
- Small binary size increase (10-30%) acceptable

**Trade-offs**:
- Slightly larger binaries
- Need musl toolchain in development
- Worth it for deployment simplicity

### Separate --protocol-version Flag

**Decision**: Use `--protocol-version` separate from `--version`

**Rationale**:
- Stable machine-readable format
- Users won't depend on `--version` format for scripts
- `--version` can change freely for humans
- Clear intent when querying compatibility

**Alternative**: Could parse `--version` output, but fragile

### Multi-tier Discovery

**Decision**: Explicit path → same dir → PATH

**Rationale**:
- Respects explicit user configuration (highest priority)
- Maintains backward compatibility (same directory)
- Follows Unix conventions (PATH)
- Clear, predictable search order

**Omitted**: User bin directories (`~/.local/bin`, `~/.cargo/bin`) should be in PATH if desired

## Future Work

### Architecture Support

**Current**: x86_64-linux-musl only

**Future additions**:
- `aarch64-unknown-linux-musl` for ARM servers
- Architecture detection and mismatch errors
- Multi-arch binary embedding

**Estimated effort**: 2-3 days
**Risk**: Medium

### Version Policy Relaxation

**Current**: Exact version match only

**Post-v1.0**: Could implement minor version tolerance
- `rcp 1.2.5` accepts `rcpd 1.2.x` where `x >= 5`
- Requires semantic versioning discipline
- Communicate protocol changes clearly

## References

- **Implementation**:
  - Binary discovery and version checking: `remote/src/lib.rs::discover_rcpd_path`, `remote/src/lib.rs::check_rcpd_version`
  - Automatic deployment: `remote/src/deploy.rs`
  - Version module: `common/src/version.rs`
  - Build script: `common/build.rs`

- **Tests**:
  - CLI and version tests: `rcp/tests/cli_parsing_tests.rs`
  - Remote operations tests: `rcp/tests/remote_tests.rs`
  - Unit tests: `common/src/version.rs`, `remote/src/deploy.rs`

- **Documentation**:
  - Security analysis: `docs/security.md`
  - User-facing documentation: `README.md`
