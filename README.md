<img src="https://raw.githubusercontent.com/wykurz/rcp/main/assets/logo.svg" height="64" alt="RCP Tools Logo">

RCP TOOLS
=========

This repo contains tools to efficiently copy, remove and link large filesets, both locally and across remote hosts.

[![Build status](https://github.com/wykurz/rcp/actions/workflows/rust.yml/badge.svg)](https://github.com/wykurz/rcp/actions)
[![Crates.io](https://img.shields.io/crates/v/rcp-tools-rcp.svg)](https://crates.io/crates/rcp-tools-rcp)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://docs.rs/rcp-tools-rcp/badge.svg)](https://docs.rs/rcp-tools-rcp)
[![Radicle](https://img.shields.io/badge/Radicle-Repository-7B4CDB?logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQiIGhlaWdodD0iMjQiIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPGNpcmNsZSBjeD0iMTIiIGN5PSIxMiIgcj0iMTAiIHN0cm9rZT0id2hpdGUiIHN0cm9rZS13aWR0aD0iMiIvPgo8L3N2Zz4K)](https://app.radicle.xyz/nodes/seed.radicle.garden/rad:z2EHT1VHYdjQfftzpYrmrAVEwL3Lp)

- `rcp` is for copying files; similar to `cp` but generally MUCH faster when dealing with large filesets.

    Supports both local and remote copying using `host:/path` syntax (similar to `scp`).

    Inspired by tools like `dsync`(1) and `pcp`(2).

- `rrm` is for removing large filesets.

- `rlink` allows hard-linking filesets with optional update path; typically used for hard-linking datasets with a delta.

- `rcmp` tool is for comparing filesets.

- `filegen` tool generates sample filesets, useful for testing.

Documentation
=============

API documentation for the command-line tools is available on docs.rs:

- [rcp-tools-rcp](https://docs.rs/rcp-tools-rcp) - File copying tool (rcp & rcpd)
- [rcp-tools-rrm](https://docs.rs/rcp-tools-rrm) - File removal tool
- [rcp-tools-rlink](https://docs.rs/rcp-tools-rlink) - Hard-linking tool
- [rcp-tools-rcmp](https://docs.rs/rcp-tools-rcmp) - File comparison tool
- [rcp-tools-filegen](https://docs.rs/rcp-tools-filegen) - Test file generation utility

**For contributors**: Internal library crates used by the tools above:
- [rcp-tools-common](https://docs.rs/rcp-tools-common) - Shared utilities and types
- [rcp-tools-remote](https://docs.rs/rcp-tools-remote) - Remote operation protocol
- [rcp-tools-throttle](https://docs.rs/rcp-tools-throttle) - Resource throttling

**Design and reference documents** (in the `docs/` directory):
- [Security](docs/security.md) - Threat model and security architecture
- [Remote Copy](docs/remote_copy.md) - rcpd deployment, version checking, troubleshooting
- [Remote Protocol](docs/remote_protocol.md) - Wire protocol specification
- [Testing](docs/testing.md) - Test infrastructure and Docker multi-host testing

Examples
========

Basic local copy with progress-bar and summary at the end:
```fish
> rcp <foo> <bar> --progress --summary
```

Copy while preserving metadata, overwrite/update destination if it already exists:
```fish
> rcp <foo> <bar> --preserve --progress --summary --overwrite
```

Remote copy from one host to another:
```fish
> rcp user@host1:/path/to/source user@host2:/path/to/dest --progress --summary
```
Copies files from `host1` to `host2`. The `rcpd` process is automatically started on both hosts via SSH.

Copy from remote host to local machine:
```fish
> rcp host:/remote/path /local/path --progress --summary
```

Copy from local machine to remote host and preserve metadata:
```fish
> rcp /local/path host:/remote/path --progress --summary --preserve
```

Remote copy with automatic rcpd deployment:
```fish
> rcp /local/data remote-host:/backup --auto-deploy-rcpd --progress
```
Automatically deploys rcpd to the remote host if not already installed. Useful for dynamic infrastructure, development environments, or when rcpd is not pre-installed on remote hosts.

Log tool output to a file while using progress bar:
```fish
> rcp <foo> <bar> --progress --summary > copy.log
```
Progress bar is sent to `stderr` while log messages go to `stdout`. This allows us to pipe `stdout` to a file to preserve the tool output while still viewing the interactive progress bar. This works for all RCP tools.

Path handling (tilde `~` support)
----------------------------------

- Local paths: leading `~` or `~/...` expands to your local `$HOME`.
- Remote paths: leading `~/...` expands to the remote user’s `$HOME` (resolved over SSH). Other `~user` forms are not supported.
- Remote paths may be absolute, start with `~/`, or be relative. Relative remote paths are resolved against the local current working directory before being used remotely.

Remove a path recursively:
```fish
> rrm <bar> --progress --summary
```

Hard-link contents of one path to another:
```fish
> rlink <foo> <bar> --progress --summary
```
Roughly equivalent to: `cp -p --link <foo> <bar>`.

Hard-link contents of `<foo>` to `<baz>` if they are identical to `<bar>`:
```fish
> rlink <foo> --update <bar> <baz> --update-exclusive --progress --summary
```
Using `--update-exclusive` means that if a file is present in `<foo>` but not in `<bar>` it will be ignored.
Roughly equivalent to: `rsync -a --link-dest=<foo> <bar> <baz>`.

Compare `<foo>` vs. `<bar>`:
```fish
> rcmp <foo> <bar> --progress --summary --log compare.log
```

Installation
============

<img src="https://raw.githubusercontent.com/NixOS/nixos-artwork/master/logo/nix-snowflake-colours.svg" height="64" alt="Nix">

nixpkgs
-------

All tools are available via nixpkgs under [rcp](https://github.com/NixOS/nixpkgs/blob/nixos-unstable/pkgs/by-name/rc/rcp/package.nix) package name.

The following command will install all the tools on your system:

```fish
> nix-env -iA nixpkgs.rcp
```

crates.io
---------

All tools are available on [crates.io](https://crates.io/search?q=rcp-tools). Individual tools can be installed using `cargo install`:

```fish
> cargo install rcp-tools-rcp
```

debian / rhel
-------------

Starting with release `v0.10.1`, .deb and .rpm packages are available as part of each release.

Static musl builds
------------------

The repository is configured to build static musl binaries by default via `.cargo/config.toml`. Simply run `cargo build` or `cargo build --release` to produce fully static binaries. To build glibc binaries instead, use `cargo build --target x86_64-unknown-linux-gnu`.

For development, enter the nix environment (`nix develop`) to get all required tools including the musl toolchain. Outside nix shell, install the musl target with `rustup target add x86_64-unknown-linux-musl` and ensure you have musl-tools installed (e.g., `apt-get install musl-tools` on Ubuntu/Debian).

General controls
================

## Copy semantics

The copy semantics for RCP tools differ slightly from how e.g. the `cp` tool works. This is because of the ambiguity in the result of a `cp` operation that we wanted to avoid.

Specifically, the result of `cp foo/x bar/x` depends on `bar/x` being a directory. If so, the resulting path will be `bar/x/x` (which is usually undesired), otherwise it will be `bar/x`.

To avoid this confusion, RCP tools:
- will NOT overwrite data by default (use `--overwrite` to change)
- do assume that a path WITHOUT a trailing slash is the final name of the destination and
- path ending in slash is a directory into which we want to copy the sources (without renaming)

The following examples illustrate this (_those rules apply to both `rcp` and `rlink`_):

- `rcp A/B C/D` - copy `A/B` into `C/` and name it `D`; if `C/D` exists fail immediately
- `rcp A/B C/D/` - copy `B` into `D` WITHOUT renaming i.e., the resulting path will be `C/D/B`; if `C/B/D` exists fail immediately

Using `rcp` it's also possible to copy multiple sources into a single destination, but the destination MUST have a trailing slash (`/`):
- `rcp A B C D/` - copy `A`, `B` and `C` into `D` WITHOUT renaming i.e., the resulting paths will be `D/A`, `D/B` and `D/C`; if any of which exist fail immediately

## Throttling

- set `--ops-throttle` to limit the maximum number of operations per second
  - useful if you want to avoid interfering with other work on the storage / host

- set `--iops-throttle` to limit the maximum number of I/O operations per second
  - MUST be used with `--chunk-size`, which is used to calculate I/O operations per file

- set `--max-open-files` to limit the maximum number of open files
  - RCP tools will automatically adjust the maximum based on the system limits however, this setting can be used if there are additional constraints

## Error handling

- `rcp` tools will log non-terminal errors and continue by default
- to fail immediately on any error use the `--fail-early` flag

## Remote copy configuration

When using remote paths (`host:/path` syntax), `rcp` automatically starts `rcpd` daemons on remote hosts via SSH.

**Requirements:**
- SSH access to remote hosts (uses your SSH config and keys)
- `rcpd` binary available on remote hosts (see **Auto-deployment** below for automatic setup)

**Auto-deployment:**
Starting with v0.22.0, `rcp` can automatically deploy `rcpd` to remote hosts using the `--auto-deploy-rcpd` flag. This eliminates the need to manually install `rcpd` on each remote host.

```fish
# automatic deployment - no manual setup required
> rcp --auto-deploy-rcpd host1:/source host2:/dest --progress
```

When auto-deployment is enabled:
- `rcp` finds the local `rcpd` binary (same directory or PATH)
- Deploys it to `~/.cache/rcp/bin/rcpd-{version}` on remote hosts via SSH
- Verifies integrity using SHA-256 checksums
- Keeps the last 3 versions and cleans up older ones
- Reuses deployed binaries for subsequent operations (cached until version changes)

Manual deployment is still supported and may be preferred for:
- Air-gapped environments where auto-deployment is not feasible
- Production systems with strict change control
- Situations where you want to verify the binary before deployment

**Configuration options:**
- `--port-ranges` - restrict TCP data ports to specific ranges (e.g., "8000-8999")
- `--remote-copy-conn-timeout-sec` - connection timeout in seconds (default: 15)

**Architecture:**
The remote copy uses a three-node architecture:
- Master (`rcp`) orchestrates the copy operation
- Source `rcpd` reads files from source host
- Destination `rcpd` writes files to destination host
- Data flows directly from source to destination (not through master)

For detailed network connectivity and troubleshooting information, see `docs/remote_copy.md`.

## Security

**Remote copy relies on SSH for authentication.**

**Security Model:**
- **SSH Authentication**: All remote operations require SSH authentication first
- **Network Trust**: Data transfers are currently unencrypted (plain TCP)
- **Recommended**: Use on trusted networks or tunnel through VPN/SSH

**What's Protected:**
- ✅ Unauthorized access (SSH authentication required)
- ⚠️ Data encryption: Currently unencrypted (use trusted networks or VPN)

**Best Practices:**
- Use SSH key-based authentication
- Run on trusted network segments (datacenter, VPN)
- For sensitive data over untrusted networks, use SSH tunneling

For detailed security architecture and threat model, see `docs/security.md`.

## Terminal output

**Log messages**
- sent to `stdout`
- by default only errors are logged
- verbosity controlled using `-v`/`-vv`/`-vvv` for INFO/DEBUG/TRACE and `-q`/`--quiet` to disable

**Progress**
- sent to `stderr` (both `ProgressBar` and `TextUpdates`)
- by default disabled
- enabled using `-p`/`--progress` with optional `--progress-type=...` override

**Summary**
- sent to `stdout`
- by default disabled
- enabled using `--summary`

## Overwrite

`rcp` tools will not-overwrite pre-existing data unless used with the `--overwrite` flag.

Performance Tuning
==================

For maximum throughput, especially with remote copies over high-speed networks, consider these optimizations.

## System-Level Tuning (Linux)

### TCP Socket Buffers

`rcp` automatically requests larger TCP socket buffers for high-throughput transfers, but the kernel caps these to system limits. Increase the limits to allow full utilization of high-bandwidth links:

```bash
# Check current limits
sysctl net.core.rmem_max net.core.wmem_max

# Increase to 16 MiB (requires root, temporary until reboot)
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216

# Make permanent (add to /etc/sysctl.d/99-rcp-perf.conf)
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
```

The default is often insufficient for 10+ Gbps links.

### Open File Limits

When copying large filesets with many concurrent operations, you may hit the open file limit:

```bash
# Check current limit
ulimit -n

# Increase for current session
ulimit -n 65536

# Make permanent (add to /etc/security/limits.conf)
* soft nofile 65536
* hard nofile 65536
```

`rcp` automatically queries the system limit and uses `--max-open-files` to self-throttle, but higher limits allow more parallelism.

### Network Backlog (10+ Gbps)

For very high-speed networks, increase the kernel's packet processing capacity:

```bash
sudo sysctl -w net.core.netdev_max_backlog=16384
sudo sysctl -w net.core.netdev_budget=600
```

## Application-Level Tuning

### Worker Threads

Control parallelism with `--max-workers`:

```bash
# Use all CPU cores (default)
rcp --max-workers=0 /source /dest

# Limit to 4 workers (reduce I/O contention)
rcp --max-workers=4 /source /dest
```

### Remote Copy Buffer Size

For remote copies, the `--remote-copy-buffer-size` flag controls the size of data chunks sent over TCP:

```bash
# Larger buffers for high-bandwidth links (default: 16 MiB for datacenter)
rcp --remote-copy-buffer-size=32MiB host1:/data host2:/data

# Smaller buffers for constrained memory
rcp --remote-copy-buffer-size=4MiB host1:/data host2:/data
```

### Network Profile

Use `--network-profile` to optimize for your network type:

```bash
# For datacenter/local networks (aggressive settings)
rcp --network-profile=datacenter host1:/data host2:/data

# For internet transfers (conservative settings)
rcp --network-profile=internet host1:/data host2:/data
```

### Concurrent Connections

Control concurrent TCP connections for file transfers (default: 100):

```bash
# Increase for many small files on high-bandwidth links
rcp --max-connections=200 host1:/many-small-files host2:/dest

# Decrease to reduce resource usage
rcp --max-connections=16 host1:/data host2:/dest
```

## Diagnosing Performance Issues

### Check TCP Buffer Sizes

When `rcp` starts, it logs the actual buffer sizes achieved (visible with `-v`). If the actual sizes are much smaller than requested, increase your system's `rmem_max`/`wmem_max`.

### Check for Network Issues

```bash
# Network interface drops
ip -s link show eth0 | grep -A1 RX | grep dropped

# TCP retransmissions (high values indicate network congestion)
ss -ti | grep retrans
```

## Quick Checklist

For optimal performance on high-speed networks:

1. ☐ Increase `rmem_max`/`wmem_max` to 16+ MiB
2. ☐ Increase `ulimit -n` if copying many files
3. ☐ Use `--network-profile=datacenter` for local/datacenter networks
4. ☐ Use `--progress` to monitor throughput in real-time
5. ☐ Check `-v` output to verify buffer sizes and connection setup

Profiling
=========

`rcp` supports several profiling and debugging options.

## Chrome Tracing

Produces JSON trace files viewable in [Perfetto UI](https://ui.perfetto.dev) or `chrome://tracing`.

```bash
# Profile a local copy
rcp --chrome-trace=/tmp/trace /source /dest

# Profile a remote copy (traces produced on all hosts)
rcp --chrome-trace=/tmp/trace host1:/path host2:/path
```

Output files are named: `{prefix}-{identifier}-{hostname}-{pid}-{timestamp}.json`

Example output:
- `/tmp/trace-rcp-master-myhost-12345-2025-01-15T10:30:45.json`
- `/tmp/trace-rcpd-source-host1-23456-2025-01-15T10:30:46.json`
- `/tmp/trace-rcpd-destination-host2-34567-2025-01-15T10:30:46.json`

View traces by opening https://ui.perfetto.dev and dragging the JSON file into the browser.

## Flamegraph

Produces folded stack files convertible to SVG flamegraphs using [inferno](https://github.com/jonhoo/inferno).

```bash
# Profile and generate flamegraph data
rcp --flamegraph=/tmp/flame /source /dest

# Convert to SVG (requires: cargo install inferno)
cat /tmp/flame-*.folded | inferno-flamegraph > flamegraph.svg

# Or use inferno-flamechart to preserve chronological order
cat /tmp/flame-*.folded | inferno-flamechart > flamechart.svg
```

Output files are named: `{prefix}-{identifier}-{hostname}-{pid}-{timestamp}.folded`

## Profile Level

Control which spans are captured with `--profile-level` (default: `trace`):

```bash
# Capture only info-level and above spans
rcp --chrome-trace=/tmp/trace --profile-level=info /source /dest
```

Only spans from rcp crates are captured (not tokio internals).

## Tokio Console

Enable [tokio-console](https://github.com/tokio-rs/console) for real-time async task inspection:

```bash
# Start rcp with tokio-console enabled
rcp --tokio-console /source /dest

# Or specify a custom port
rcp --tokio-console --tokio-console-port=6670 /source /dest

# Connect with tokio-console CLI
tokio-console http://127.0.0.1:6669
```

Trace events are retained for 60s by default. This can be modified with `RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS=120`.

## Combined profiling

All profiling options can be used together:

```bash
rcp --chrome-trace=/tmp/trace --flamegraph=/tmp/flame --tokio-console /source /dest
```

References
==========
1) https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html
2) https://github.com/wtsi-ssg/pcp
