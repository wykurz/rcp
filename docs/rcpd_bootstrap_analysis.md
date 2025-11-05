# rcpd Bootstrap Analysis

This document contains a comprehensive analysis of implementing automatic rcpd binary discovery, version checking, and deployment for remote copy operations.

**Status**: Phase 1 implementation complete

## Decisions Made

1. **Version matching**: Exact version match (rcp 0.22.0 requires rcpd 0.22.0 exactly) - strictest but safest during active development
2. **Protocol version command**: Use `rcpd --protocol-version` (machine-readable, stable) separate from `rcpd --version` (human-readable, can change)
3. **Binary discovery paths**: Explicit path → same directory as rcp → PATH only. User bin directories like ~/.local/bin and ~/.cargo/bin are omitted as they should be in PATH if desired.
4. **CLI flags** (Phase 1 - Implemented):
   - `--rcpd-path=/path/to/rcpd` - explicit path override (applies to whichever side is remote)
   - `--protocol-version` - outputs JSON with semantic version and git info
5. **CLI flags** (Phase 3 - Planned):
   - `--auto-deploy-rcpd` - enable automatic deployment for src/dst as needed
   - `--rcpd-path-src=/path` - explicit path for source-side rcpd
   - `--rcpd-path-dst=/path` - explicit path for destination-side rcpd
   - `--force-deploy-rcpd` - always deploy, skip discovery and version checking

## Phase 1 Implementation Summary

**What was implemented**:

1. **Build-time version embedding** (`common/build.rs`):
   - Captures semantic version from Cargo.toml
   - Captures git describe and git hash (if available)
   - Sets environment variables for use in code

2. **ProtocolVersion struct** (`common/src/version.rs`):
   - JSON-serializable version information
   - Compatibility checking with exact semantic version match
   - Human-readable display format

3. **Protocol version API** (added to both `rcp` and `rcpd`):
   - `--protocol-version` flag outputs JSON with version info
   - Separate from `--version` for stability

4. **Binary discovery** (`remote/src/lib.rs::discover_rcpd_path`):
   - Searches: explicit path → same dir → PATH
   - Clear error messages when rcpd not found

5. **Version checking** (`remote/src/lib.rs::check_rcpd_version`):
   - Runs `rcpd --protocol-version` on remote via SSH
   - Compares semantic versions
   - Detailed error messages on mismatch with installation instructions

6. **CLI flag**: `--rcpd-path` for explicit binary path override

**Security and UX improvements**:
- Shell escaping applied to all paths used in shell commands to prevent command injection
- Version check calls binary directly without shell to avoid injection vulnerabilities
- Unit tests verify escaping handles malicious inputs correctly
- Error messages include specific version in installation instructions for consistency
- Explicit path shown in "not found" error message when provided
- Remote host properly escaped in command suggestions for safe copy-paste
- `--protocol-version` respects `--` separator to allow files named "--protocol-version" (Unix convention)

**Key files modified**:
- `common/build.rs` - Created for version embedding
- `common/src/version.rs` - Created for ProtocolVersion
- `common/src/lib.rs` - Export version module
- `common/Cargo.toml` - Added serde_json dependency
- `rcp/src/bin/rcp.rs` - Added --protocol-version and --rcpd-path flags
- `rcp/src/bin/rcpd.rs` - Added --protocol-version flag
- `remote/src/lib.rs` - Implemented discovery and version checking

## Problem Statement

The current implementation assumes `rcpd` is installed in the same directory as `rcp` on remote hosts. This causes failures when:
- Remote host has a different version of rcpd
- rcpd is in a different location
- rcpd is not installed at all

## Current Implementation

**Location**: `remote/src/lib.rs:421-448` (function `start_rcpd`)

```rust
let current_exe = std::env::current_exe()?;
let bin_dir = current_exe.parent()?;
let mut cmd = session.arc_command(format!("{}/rcpd", bin_dir.display()));
```

This works for co-located installations but lacks flexibility and version verification.

---

## Proposed Solution Components

### 1. Binary Discovery Strategy

**Multi-tiered search strategy**:

1. Check explicit path from CLI flag (`--rcpd-path=/path/to/rcpd`)
2. Check same directory as `rcp` (current behavior, for co-located installations)
3. Check PATH (`which rcpd` or equivalent)
4. Fall back to auto-deployment (if enabled in Phase 3)

**Rationale**:
- Respects explicit user configuration (highest priority)
- Maintains backward compatibility (same directory check)
- Follows Unix conventions (PATH lookup)
- Standard user bin directories (~/.local/bin, ~/.cargo/bin) are omitted as they should already be in PATH if the user wants them searched

---

### 2. Version Checking Strategy

#### Analysis of Version Schemes

##### Option A: Commit Count (Original Proposal)
```bash
git rev-list --count HEAD  # On main branch
```

**Pros:**
- Monotonically increasing
- Simple number comparison
- Never decreases (if main never rebases)

**Cons:**
- ⚠️ **Critical flaw**: Only works if binary is built from main branch
- Feature branches have different counts
- Forks have incompatible counts
- Not easily reproducible without git repo
- Requires embedding build-time git state

**Verdict**: ❌ **Not recommended** due to branch/fork issues

##### Option B: Semantic Version (Recommended)
Use existing `version = "0.22.0"` from `Cargo.toml`

**Pros:**
- Already exists and is maintained
- Standard practice in Rust ecosystem
- Built into Rust via `env!("CARGO_PKG_VERSION")`
- Communicates compatibility expectations clearly
- Works across all build scenarios

**Cons:**
- Doesn't capture every change (same version between commits during development)
- Requires manual version bumping

**Compatibility strategies:**
1. **Exact match**: `rcp 0.22.0` requires `rcpd 0.22.0`
   - Simplest, most conservative
   - Best for active development phase

2. **Minor version tolerance**: `rcp 0.22.x` accepts `rcpd 0.22.y` where y ≥ x
   - More flexible
   - Better for stable releases post-1.0

3. **Protocol version**: Separate protocol version from tool version
   - Most sophisticated
   - Allows independent evolution of tools and protocol

**Verdict**: ✅ **Recommended**: Start with **exact match**, evolve to minor version tolerance after v1.0

##### Option C: Git Hash
Embed full commit hash: `git rev-parse HEAD`

**Pros:**
- Exact identification of build
- Captures every change
- Works across branches and forks

**Cons:**
- No compatibility information (is commit abc compatible with xyz?)
- Different on every commit (even documentation-only changes)
- Too strict for practical use

**Verdict**: ⚠️ **Use as supplement** for debugging, not primary version check

##### Option D: Git Describe
Use `git describe --tags --long --always`: e.g., `v0.21.1-7-g644da27`

**Pros:**
- Combines tag + commits-since + hash
- Human-readable
- Captures version + development state
- Useful for support/debugging

**Cons:**
- More complex to parse for comparison
- Still has branch/fork ambiguity issues

**Verdict**: ✅ **Good for version display** and debugging, supplement to semantic version

#### Final Version Check Recommendation

**Version Structure**:
```rust
pub struct Version {
    // Primary: semantic version for compatibility check
    semantic: String,  // "0.22.0"

    // Secondary: full info for debugging/display
    git_describe: Option<String>,  // "v0.21.1-7-g644da27"
    git_hash: Option<String>,  // "644da27..."
}
```

**Check Logic**:
1. **Primary check**: Exact semantic version match (can be relaxed to minor version later)
2. **Warning**: If git hashes differ (development builds from different commits)
3. **Display**: Show full `git describe` in `--version` output

**Implementation via build.rs**:
```rust
// build.rs
fn main() {
    // Semantic version is always available from CARGO_PKG_VERSION (set by Cargo)
    // No need to set a custom environment variable

    // Git info (best effort, may fail without git)
    if let Ok(output) = Command::new("git")
        .args(["describe", "--tags", "--long", "--always", "--dirty"])
        .output()
    {
        let describe = String::from_utf8_lossy(&output.stdout).trim().to_string();
        println!("cargo:rustc-env=RCP_GIT_DESCRIBE={}", describe);
    }

    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
    {
        let hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
        println!("cargo:rustc-env=RCP_GIT_HASH={}", hash);
    }

    // Rerun if git state changes
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
}
```

**Version Output Example**:
```bash
$ rcpd --version
rcpd 0.22.0

$ rcpd --protocol-version
{"semantic":"0.22.0","git_describe":"v0.21.1-7-g644da27","git_hash":"644da27abc123..."}
```

**Rationale for separate `--protocol-version`**:
- Separates concerns: `--version` is for human display, `--protocol-version` is for compatibility checking
- Users won't depend on `--version` format for scripts (can change freely)
- More future-proof: stable machine-readable format for automation
- Clear intent: explicitly asking for protocol compatibility information

---

### 3. Static Linking & Portable Binaries

#### Static Linking Approach

**For Linux** (primary target):
```bash
# Build with musl for fully static binary (default target)
cargo build --release
```

**Benefits**:
- Fully static binary (except kernel syscalls)
- Portable across all Linux x64 systems
- No libc version dependencies
- Works on minimal containers (Alpine, distroless)

**For macOS**:
- Typically uses dynamic linking to system frameworks
- Less critical (system libraries are stable across macOS versions)
- Static linking is complex and often unnecessary

#### Architecture Support Matrix

| Priority | Target | Use Case | Complexity |
|----------|--------|----------|-----------|
| **High** | `x86_64-unknown-linux-musl` | Most Linux servers | Low |
| **Medium** | `aarch64-unknown-linux-musl` | ARM servers (cloud) | Low |
| **Low** | `x86_64-apple-darwin` | Intel Mac | Medium |
| **Low** | `aarch64-apple-darwin` | Apple Silicon | Medium |

**Recommendation**:
1. Start with **x86_64-linux-musl only** (covers 90% of server use cases)
2. Add aarch64 after initial implementation is proven
3. Handle architecture mismatch gracefully with clear error messages

#### Binary Size Considerations

Musl-static binaries are typically:
- 10-30% larger than dynamic builds
- Still quite reasonable (5-15 MB for typical Rust CLI)
- Compression helps for transfer (gzip, xz can reduce by 60-70%)

#### Cross-Compilation Setup with Nix

**Add to flake.nix**:
```nix
packages = {
  # ... existing packages ...

  rcpd-static-x86_64 = pkgs.pkgsCross.musl64.rustPlatform.buildRustPackage {
    pname = "rcpd-static";
    version = "0.22.0";
    src = ./.;
    cargoLock = { lockFile = ./Cargo.lock; };
    cargoBuildFlags = [ "-p" "rcp-tools-rcp" "--bin" "rcpd" ];
    # This produces a fully static binary
  };
};
```

**Add to justfile**:
```makefile
# Build static rcpd binary for Linux (default target)
build-static-rcpd:
    cargo build --release --bin rcpd

# Or using nix
build-static-rcpd-nix:
    nix build .#rcpd-static-x86_64
```

---

### 4. Transfer Mechanisms

#### Option A: Embed Binary in rcp

```rust
// At compile time, embed static rcpd binary
const RCPD_BINARY: &[u8] = include_bytes!("../../target/musl-release/rcpd");
```

**Pros:**
- Always available
- No external dependencies
- Single binary to distribute

**Cons:**
- Increases `rcp` binary size by ~5-10 MB
- Need to build rcpd first (complicates build process)
- Architecture mismatch issues (x64 rcp can't embed arm64 rcpd)
- Hard to support multiple architectures simultaneously
- Inflates download size even when not using remote features

**Verdict**: ⚠️ **Possible but has significant drawbacks**

#### Option B: Base64 Transfer over SSH (Recommended)

**Implementation**:
```rust
// Read local rcpd static binary
let binary = std::fs::read(rcpd_static_path)?;
let encoded = base64::encode(&binary);

// Transfer via SSH stdin/stdout
let remote_path = format!("~/.cache/rcp/bin/rcpd-{}", version);
ssh host format!(
    "mkdir -p ~/.cache/rcp/bin && base64 -d > {} && chmod 700 {}",
    remote_path, remote_path
)
// Feed encoded data to stdin
```

**Alternative methods considered**:

1. **SCP** (if available):
   ```bash
   scp local_rcpd remote:~/.cache/rcp/bin/rcpd-{version}
   ```
   - Pro: Efficient, native protocol
   - Con: Requires scp on remote (not always available)

2. **Direct QUIC Transfer**:
   - Pro: Fast, uses existing QUIC infrastructure
   - Con: Chicken-and-egg problem (can't use QUIC to transfer rcpd before rcpd is running)

3. **Cat Heredoc**:
   ```bash
   ssh host "cat > file << 'EOF_BINARY'"
   ```
   - Pro: Simple, no encoding
   - Con: Binary data in shell heredoc is fragile

**Why Base64 over SSH wins**:
- Always available (base64 is POSIX standard, included in all modern systems)
- No external dependencies (no scp/rsync/curl needed)
- Simple implementation with openssh crate
- Handles binary data safely through text channels
- Works even with restricted shells

#### Option C: Hybrid Approach (Final Recommendation)

```rust
// Deployment workflow:
// 1. Try to find existing rcpd in PATH/standard locations
// 2. Check version compatibility
// 3. If version mismatch:
//    a. Look for rcpd-static in ~/.cache/rcp/bin/ (from previous deployment)
//    b. If not found and --auto-deploy-rcpd enabled:
//       - Find local static rcpd binary
//       - Transfer via base64+ssh to remote
//       - Store in ~/.cache/rcp/bin/rcpd-{version}
//       - Verify checksum
//    c. Use the deployed binary
// 4. If no --auto-deploy-rcpd flag:
//    - Show clear error with installation instructions
```

**Benefits**:
- Flexible: works with manual installations and auto-deployment
- User-friendly: clear error messages guide installation
- Efficient: caches deployed binaries for reuse
- Safe: opt-in behavior with verification

**Verdict**: ✅ **Best approach** - balances flexibility and automation

---

### 5. Security Considerations

#### Integrity Verification

**Problem**: How do we ensure transferred binary wasn't corrupted or tampered with?

**Solution**: SHA-256 checksum verification
```rust
// Before transfer: compute checksum
let binary = std::fs::read(rcpd_path)?;
let checksum = sha256(&binary);

// After transfer: verify on remote
let remote_checksum = ssh_exec(host, format!(
    "sha256sum ~/.cache/rcp/bin/rcpd-{}", version
))?;

if checksum != remote_checksum {
    return Err(anyhow!("Checksum mismatch: binary transfer corrupted"));
}
```

#### Authorization

**Question**: Should we auto-deploy without asking?

**Answer**: Make it opt-in with explicit flag

```bash
# Explicit opt-in for auto-deployment
rcp --auto-deploy-rcpd src:path dst:path

# Or pre-deployment command
rcp --deploy-rcpd-to=host1,host2
```

**Rationale**:
- Security-conscious: doesn't transfer binaries without permission
- Transparency: user knows what's happening
- Control: user can choose when/where to deploy

#### File Permissions

Deployed binaries should be user-only executable:
```rust
// After deployment
ssh_exec(host, format!(
    "chmod 700 ~/.cache/rcp/bin/rcpd-{}", version
))?;
```

**Rationale**:
- Prevents unauthorized modification by other users
- Follows principle of least privilege
- Standard practice for user-installed binaries

#### Version Cleanup

**Problem**: Old versions accumulate over time

**Solution**: Keep last N versions (e.g., 3), cleanup older
```rust
// After successful deployment
ssh_exec(host,
    "cd ~/.cache/rcp/bin && ls -t rcpd-* | tail -n +4 | xargs -r rm"
)?;
```

**Benefits**:
- Prevents disk space bloat
- Keeps recent versions for potential rollback
- Automatic maintenance, no user intervention needed

---

### 6. Deployment Location

**Options Considered**:

1. **`~/.cache/rcp/bin/rcpd-{version}`** ✅
   - XDG standard location for cache
   - User-specific, no sudo needed
   - Version-specific (supports multiple versions and rollback)
   - Cache semantics (okay to delete, will be recreated)
   - Clear ownership (managed by rcp)

2. **`~/.local/bin/rcpd`** ❌
   - Would replace existing manual installation
   - No version isolation
   - Conflicts with user's PATH management
   - Could break other tools expecting specific version

3. **`/tmp/rcpd-{version}-{random}`** ❌
   - Might be cleaned up between runs (tmpwatch, systemd-tmpfiles)
   - Wastes bandwidth re-transferring on each run
   - No persistence across sessions
   - Permissions might be restrictive (noexec on /tmp)

**Recommendation**: **`~/.cache/rcp/bin/rcpd-{version}`**

**Directory structure**:
```
~/.cache/rcp/
├── bin/
│   ├── rcpd-0.22.0        # Current version
│   ├── rcpd-0.21.0        # Previous version (kept for rollback)
│   └── rcpd-0.20.0        # Older version (kept temporarily)
└── logs/                  # Future: deployment logs
```

---

## Implementation Complexity Assessment

| Component | Complexity | Estimated Effort | Risk Level |
|-----------|-----------|------------------|-----------|
| Version embedding (build.rs) | Low | 2-4 hours | Low |
| Version checking logic | Low | 2-4 hours | Low |
| Multi-tier binary discovery | Medium | 4-8 hours | Medium |
| Static build (x86_64-musl) | Low | 2-4 hours | Low |
| Base64 transfer over SSH | Medium | 6-10 hours | Medium |
| Checksum verification | Low | 2-3 hours | Low |
| Error handling & cleanup | Medium | 4-6 hours | Medium |
| Testing & integration | High | 8-16 hours | High |
| Documentation | Low | 2-4 hours | Low |
| **Total** | **High** | **32-59 hours** | **Medium-High** |

**Risk factors**:
- SSH interaction edge cases (connection drops, timeouts)
- Remote filesystem constraints (disk space, permissions, noexec)
- Architecture detection false positives
- Version comparison logic bugs
- Race conditions in concurrent deployments

---

## Recommended Implementation Plan

### Phase 1: Foundation (Low Risk, High Value)

**Goal**: Detect version mismatches and provide clear error messages

**Tasks**:

1. **Add build.rs to embed version info**
   - Semantic version (always available)
   - Git describe (if git available)
   - Git hash (if git available)
   - Handle builds without git gracefully

2. **Add `--protocol-version` output to both rcp and rcpd**
   ```bash
   $ rcpd --version
   rcpd 0.22.0

   $ rcpd --protocol-version
   {"semantic":"0.22.0","git_describe":"v0.21.1-7-g644da27","git_hash":"644da27..."}
   ```

   This separates human-readable version (can change format) from machine-readable protocol version (stable format).

3. **Implement version checking in `start_rcpd()`**
   - Before launching rcpd, run: `ssh host "rcpd --protocol-version"`
   - Parse JSON and compare semantic versions
   - If mismatch: **clear error with instructions**

   **Error message example**:
   ```
   Error: rcpd version mismatch

   Local:  rcp 0.22.0 (v0.21.1-7-g644da27)
   Remote: rcpd 0.21.0 on host 'prod-server'

   The rcpd version on the remote host must exactly match the rcp version.

   To fix this, install the matching version on the remote host:
     ssh prod-server 'cargo install rcp-tools-rcp --version 0.22.0'

   Or enable automatic deployment:
     rcp --auto-deploy-rcpd ...
   ```

4. **Multi-tier binary discovery**
   - Add `--rcpd-path` CLI flag for explicit path
   - Try discovery order: explicit path → same dir → PATH → standard locations
   - Report which path was found in debug logs
   - Clear error if not found anywhere

**Deliverables**:
- Version checking works reliably
- Clear, actionable error messages
- No auto-deployment yet (simpler, less risky)

**Timeline**: 1-2 days

**Risk**: Low - mostly data structure and comparison logic

**Testing**:
- Unit tests for version parsing and comparison
- Integration test with mismatched versions
- Test with missing rcpd

**Implementation Notes**:

1. **Version checking performance**: SSH-based version checking adds ~9-10 seconds per remote host connection. Test timeout was increased from 20s to 35s to accommodate version checking overhead (2 connections × 10s + buffer for operations).

2. **Test configuration**: Tests in `rcp/tests/remote_tests.rs` use a 35-second timeout to allow for version checking on both source and destination connections.

3. **Production considerations**: The 9-10 second overhead per host may be acceptable for correctness guarantees, but consider caching version check results across multiple operations to the same host in future iterations

### Phase 2: Static Builds (Medium Risk)

**Goal**: Create portable static binaries that work everywhere

**Tasks**:

1. **Add musl target to build system**
   - Update `flake.nix` / `default.nix` so the dev shell ships the Rust 1.90.0 toolchain with the `x86_64-unknown-linux-musl` target plus cross binutils.
   - Set `target = "x86_64-unknown-linux-musl"` in `.cargo/config.toml` so `cargo build` emits static binaries by default everywhere.
   - Document how to produce musl builds (`cargo build`, `cargo build --release`) and how to explicitly build glibc versions (`cargo build --target x86_64-unknown-linux-gnu`).
   - Test on CI: GitHub Actions with musl target

2. **Test static binary on various distros**
   - Ubuntu (glibc-based)
   - Debian (glibc-based)
   - Alpine (musl-based)
   - RHEL/Rocky (glibc-based)
   - Verify no missing dependencies: `ldd rcpd` should show "not a dynamic executable"

3. **Handle architecture detection**
   - Detect local architecture: `uname -m`
   - Detect remote architecture: `ssh host uname -m`
   - Verify match before deployment
   - Clear error if mismatch:
     ```
     Error: Architecture mismatch
     Local:  x86_64
     Remote: aarch64 on host 'arm-server'

     Cross-architecture deployment is not yet supported.
     Please install rcpd manually on the remote host.
     ```

4. **Find local static binary at runtime**
   - Check for `rcpd` in same directory as rcp (from static build)
   - Check `./target/x86_64-unknown-linux-musl/release/rcpd` (development)
   - Check `/usr/share/rcp/rcpd-static` (system installation)
   - Error if not found when auto-deploy requested

**Deliverables**:
- Reliable static builds that work on all major Linux distros
- Architecture detection prevents invalid deployments
- Clear documentation for building static binaries
  - ✅ Development environments now default to musl static builds; standalone instructions documented in `README.md`

**Timeline**: 1-2 days

**Risk**: Low - nix makes cross-compilation straightforward

**Testing**:
- Test static binary on 5+ different Linux distros
- Verify truly static: no dynamic library dependencies
- Test architecture detection on x86_64 and aarch64

### Phase 3: Auto-Deployment (High Risk, High Value)

**Goal**: Automatically deploy correct version to remote hosts

**Tasks**:

1. **Create deployment module**
   - New file: `remote/src/deploy.rs`
   - Function: `deploy_rcpd(session: &SshSession, version: &str) -> Result<PathBuf>`
   - Implements:
     - Read local static binary
     - Base64 encode
     - Create remote directory: `~/.cache/rcp/bin`
     - Transfer via SSH stdin
     - Set permissions: `chmod 700`
     - Verify checksum
     - Return path to deployed binary

2. **Implement base64 transfer over SSH**
   ```rust
   async fn transfer_binary_base64(
       session: &Session,
       local_path: &Path,
       remote_path: &str,
   ) -> Result<()> {
       let binary = tokio::fs::read(local_path).await?;
       let checksum = sha256(&binary);
       let encoded = base64::encode(&binary);

       // Create directory and transfer
       let cmd = format!(
           "mkdir -p ~/.cache/rcp/bin && base64 -d > {} && chmod 700 {}",
           remote_path, remote_path
       );

       let mut child = session.command("sh")
           .arg("-c")
           .arg(cmd)
           .stdin(Stdio::piped())
           .spawn()
           .await?;

       let mut stdin = child.stdin.take().unwrap();
       stdin.write_all(encoded.as_bytes()).await?;
       stdin.shutdown().await?;

       let output = child.wait_with_output().await?;
       if !output.status.success() {
           return Err(anyhow!("Failed to deploy rcpd: {}",
               String::from_utf8_lossy(&output.stderr)));
       }

       // Verify checksum
       verify_remote_checksum(session, remote_path, &checksum).await?;

       Ok(())
   }
   ```

3. **Add `--auto-deploy-rcpd` flag to rcp**
   ```rust
   #[arg(long, help_heading = "Remote operations")]
   auto_deploy_rcpd: bool,
   ```
   - Only deploy if flag is set (security)
   - Show progress during deployment:
     ```
     Deploying rcpd 0.22.0 to host 'prod-server'...
     Transferring binary (8.2 MB)... Done
     Verifying checksum... OK
     ```
   - Cache deployed binary path for reuse in same session

4. **Implement version cleanup**
   ```rust
   async fn cleanup_old_versions(
       session: &Session,
       keep_count: usize,
   ) -> Result<()> {
       let cmd = format!(
           "cd ~/.cache/rcp/bin && ls -t rcpd-* 2>/dev/null | tail -n +{} | xargs -r rm",
           keep_count + 1
       );
       session.command("sh").arg("-c").arg(cmd).status().await?;
       Ok(())
   }
   ```

5. **Integration with version checking**
   - Modify `start_rcpd()` to:
     1. Check version of existing rcpd (Phase 1)
     2. If mismatch and `--auto-deploy-rcpd`: deploy correct version
     3. If mismatch and no flag: error (Phase 1 behavior)
     4. Use appropriate binary (discovered or deployed)

**Deliverables**:
- Full auto-deployment working end-to-end
- Progress reporting during deployment
- Checksum verification ensures integrity
- Automatic cleanup of old versions

**Timeline**: 3-5 days

**Risk**: Medium-High
- Complex SSH interactions with many failure modes
- Binary transfer can fail (disk full, permissions, network)
- Need robust error handling and recovery

**Testing**:
- Integration test: deploy to localhost
- Test deployment failure scenarios:
  - Disk full on remote
  - Permission denied
  - Network interruption during transfer
  - Invalid checksum
- Test cleanup: verify old versions are removed
- Test concurrent deployments to same host

### Phase 4: Polish & Testing (Medium Risk)

**Goal**: Production-ready feature with comprehensive testing

**Tasks**:

1. **Comprehensive testing**
   - Unit tests:
     - Version parsing and comparison
     - Checksum calculation
     - Path discovery logic
   - Integration tests:
     - Successful deployment
     - Version mismatch detection
     - Architecture mismatch detection
     - Deployment failures and recovery
     - Concurrent access
   - Manual testing on various Linux distros:
     - Ubuntu 20.04, 22.04, 24.04
     - Debian 11, 12
     - RHEL 8, 9 / Rocky Linux
     - Alpine Linux (musl native)
   - Test matrix:
     - Fresh host (no rcpd)
     - Outdated rcpd
     - Correct rcpd already installed
     - Insufficient disk space
     - No write permission to ~/.cache
     - ~/.cache on noexec filesystem

2. **Documentation updates**
   - Update README.md:
     - Remote operations section
     - Auto-deployment feature
     - Version compatibility
   - Create docs/remote_operations.md:
     - Detailed guide
     - Architecture support
     - Troubleshooting
   - Update CHANGELOG.md:
     - Document new feature
     - Breaking changes (if any)
   - Add examples:
     ```bash
     # Auto-deploy rcpd if needed
     rcp --auto-deploy-rcpd local:/data remote:/backup

     # Manual deployment first
     rcp --deploy-rcpd-to=host1,host2,host3

     # Use specific rcpd binary
     rcp --rcpd-path=/custom/path/to/rcpd ...
     ```

3. **Error handling hardening**
   - Handle all failure modes gracefully
   - Rollback on deployment failure (cleanup partial transfer)
   - Clear error messages for every scenario:
     - Network errors
     - Filesystem errors
     - Permission errors
     - Architecture mismatch
     - Version mismatch
   - Add retry logic for transient failures (network hiccups)
   - Timeout handling for slow transfers

4. **Performance optimization**
   - Compress binary before transfer (gzip)
   - Show transfer progress for large binaries
   - Parallel deployment to multiple hosts
   - Cache deployment status across invocations

5. **Security audit**
   - Review deployment code for security issues
   - Ensure no command injection vulnerabilities
   - Verify file permissions are correct
   - Check that checksums can't be bypassed
   - Document security considerations

**Deliverables**:
- Production-ready feature with comprehensive test coverage
- Clear documentation and examples
- Robust error handling for all scenarios
- Security-reviewed implementation

**Timeline**: 2-3 days

**Risk**: Low - mostly testing and documentation

**Success Criteria**:
- All tests pass on CI
- Feature works on 5+ different Linux distros
- Documentation is clear and complete
- No known security issues

---

## Alternative Approach: Simpler Installation Helper

If full auto-deployment proves too complex, consider a simpler approach:

### Phase 1 Only + Installation Helper Command

**Implementation**:

1. **Keep Phase 1** (version checking with clear errors)

2. **Add installation helper command**:
   ```bash
   rcp --install-rcpd-on=host1,host2,host3
   ```

   This command:
   - Detects remote OS and architecture
   - Installs rcpd using appropriate method:
     - `cargo install rcp-tools-rcp` (if cargo available)
     - Downloads from GitHub releases
     - Uses system package manager (if packaged)
   - Verifies installation
   - Reports success/failure

3. **User workflow**:
   ```bash
   # First time setup (once per host)
   $ rcp --install-rcpd-on=prod-server,backup-server
   Installing rcpd 0.22.0 on prod-server... Done!
   Installing rcpd 0.22.0 on backup-server... Done!

   # Then use normally (no special flags needed)
   $ rcp local:file prod-server:/path
   ```

**Pros**:
- Much simpler implementation (50% less code)
- More transparent (user explicitly controls installation)
- Easier to debug (clear separation of install vs. use)
- Can leverage existing package managers
- Familiar workflow (like ssh-copy-id)

**Cons**:
- Requires manual setup step
- Not fully automatic (but is one-time)
- Need to re-run if version changes

**Recommendation**: Consider this if Phase 3 proves too complex or problematic

---

## Key Decisions Required

Before proceeding with implementation, the following decisions need to be made:

### 1. Version Compatibility Strategy

**Options**:
- **Exact match**: rcp 0.22.0 requires rcpd 0.22.0 exactly
- **Minor version tolerance**: rcp 0.22.5 accepts rcpd 0.22.x where x ≥ 5
- **Major version tolerance**: rcp 0.22.x accepts rcpd 0.y.z where y ≥ 22

**Recommendation**: **Exact match initially**, relax to minor version tolerance after v1.0

**Rationale**: During active development, protocol changes are frequent. Exact matching is safest. After stabilization (v1.0+), semantic versioning allows more flexibility.

### 2. Auto-Deployment Strategy

**Options**:
- **Fully automatic**: Always deploy if version mismatch (dangerous)
- **Opt-in with flag**: Require `--auto-deploy-rcpd` (safe, recommended)
- **Installation helper only**: `rcp --install-rcpd-on=hosts` (simpler alternative)

**Recommendation**: **Opt-in with flag** (`--auto-deploy-rcpd`)

**Rationale**: Security-conscious users should explicitly consent to binary transfers. Transparent and predictable behavior.

### 3. Architecture Support

**Options**:
- **x86-64 only**: Simplest, covers most servers
- **x86-64 + ARM64**: Covers modern cloud (AWS Graviton, etc.)
- **Multi-arch**: All common architectures

**Recommendation**: **x86-64 musl only initially**, add ARM64 in Phase 2.5

**Rationale**: Start simple, validate approach, then expand. ARM64 is growing but x86-64 is still dominant in server space.

### 4. Deployment Cache Strategy

**Options**:
- **No caching**: Always re-deploy (wasteful)
- **Cache by version**: Keep N most recent versions (recommended)
- **Cache forever**: Never cleanup (disk space issues)

**Recommendation**: **Cache by version, keep last 3**

**Rationale**: Balances disk space, deployment speed, and rollback capability.

### 5. Error Handling Philosophy

**Options**:
- **Fail fast**: Any deployment issue is fatal error
- **Fallback chain**: Try deployment, fallback to manual instructions
- **Best effort**: Warn but continue if possible

**Recommendation**: **Fail fast for now**, with clear error messages

**Rationale**: Unexpected states are better handled explicitly. Silent fallbacks hide problems.

---

## Timeline Summary

| Phase | Duration | Cumulative | Risk |
|-------|----------|-----------|------|
| Phase 1: Foundation | 1-2 days | 1-2 days | Low |
| Phase 2: Static Builds | 1-2 days | 2-4 days | Low |
| Phase 3: Auto-Deployment | 3-5 days | 5-9 days | Medium-High |
| Phase 4: Polish & Testing | 2-3 days | 7-12 days | Low |

**Total estimated time**: 1-2 weeks of focused development

**Contingency recommendation**: Add 20-30% buffer for unforeseen issues (total: 9-16 days)

---

## Success Metrics

How will we know this feature is successful?

1. **Reliability**: 99%+ successful deployments in typical scenarios
2. **Performance**: Deployment adds < 10 seconds to first remote operation
3. **User Experience**: Clear error messages, no manual intervention needed (with flag)
4. **Security**: No vulnerabilities in deployment or verification
5. **Maintenance**: No increase in support burden or bug reports
6. **Adoption**: Users actually use `--auto-deploy-rcpd` instead of manual installation

---

## Future Enhancements

Features to consider for future iterations:

1. **Multi-architecture support**
   - Detect remote arch, deploy correct binary
   - Embed multiple architectures (x86-64, arm64)

2. **Protocol versioning**
   - Separate protocol version from tool version
   - Allow rcpd 0.23.x with rcp 0.22.y if protocol compatible
   - More flexible upgrades

3. **Deployment server**
   - Central repository of rcpd binaries
   - Faster than transferring from client
   - Reduces client binary size

4. **Smart caching**
   - Check if remote already has correct version before transferring
   - Reuse across multiple rcp invocations
   - Share cache across users (system-wide cache)

5. **Rollback capability**
   - If deployment fails, revert to previous version
   - Keep deployment history
   - Manual rollback command

6. **Monitoring and telemetry**
   - Track deployment success/failure rates
   - Identify problematic hosts
   - Performance metrics

7. **Package manager integration**
   - Use apt/yum/dnf if available
   - Faster and more familiar for users
   - Automatic updates

---

## Conclusion

The proposed rcpd bootstrap feature is technically feasible and would significantly improve the user experience for remote operations. The phased implementation plan balances risk and value:

- **Phase 1** provides immediate value (version checking and clear errors) with low risk
- **Phase 2** ensures portability with static builds
- **Phase 3** delivers the full auto-deployment feature
- **Phase 4** ensures production readiness

**Recommended next step**: Implement Phase 1 to validate the approach and gather feedback, then decide whether to proceed with full auto-deployment or the simpler installation helper alternative.

The key technical decisions are sound:
- Semantic versioning for compatibility (with git info for debugging)
- Static musl builds for portability
- Base64 over SSH for reliable transfer
- Opt-in deployment for security
- XDG cache location for storage

Estimated total effort is 1-2 weeks, with medium-high complexity primarily in Phase 3 (auto-deployment). The simpler installation helper alternative reduces this to 3-5 days with lower risk.
