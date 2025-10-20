<img src="https://raw.githubusercontent.com/wykurz/rcp/main/assets/logo.svg" height="64" alt="RCP Tools Logo">

RCP TOOLS
=========

This repo contains tools to efficiently copy, remove and link large filesets, both locally and across remote hosts.

[![Build status](https://github.com/wykurz/rcp/actions/workflows/rust.yml/badge.svg)](https://github.com/wykurz/rcp/actions)
[![Crates.io](https://img.shields.io/crates/v/rcp-tools-rcp.svg)](https://crates.io/crates/rcp-tools-rcp)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://docs.rs/rcp-tools-rcp/badge.svg)](https://docs.rs/rcp-tools-rcp)

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

Log tool output to a file while using progress bar:
```fish
> rcp <foo> <bar> --progress --summary > copy.log
```
Progress bar is sent to `stderr` while log messages go to `stdout`. This allows us to pipe `stdout` to a file to preserve the tool output while still viewing the interactive progress bar. This works for all RCP tools.

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

debian / rhel
-------------

Starting with release `v0.10.1`, .deb and .rpm packages are available as part of each release.

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
- `rcpd` binary must be available in the same directory as `rcp` on remote hosts

**Configuration options:**
- `--quic-port-ranges` - restrict QUIC to specific port ranges (e.g., "8000-8999")
- `--remote-copy-conn-timeout-sec` - connection timeout in seconds (default: 15)

**Architecture:**
The remote copy uses a three-node architecture with QUIC protocol:
- Master (`rcp`) orchestrates the copy operation
- Source `rcpd` reads files from source host
- Destination `rcpd` writes files to destination host
- Data flows directly from source to destination (not through master)

For detailed network connectivity and troubleshooting information, see `docs/network_connectivity.md`.

## Security

**Remote copy operations are secured against man-in-the-middle (MITM) attacks** using a combination of SSH authentication and certificate pinning.

**Security Model:**
- **SSH Authentication**: All remote operations require SSH authentication first
- **TLS 1.3 Encryption**: Data transfer uses QUIC with TLS 1.3 for encryption
- **Certificate Pinning**: SHA-256 fingerprints prevent endpoint impersonation
- **No Configuration Required**: Security features work automatically

**How It Works:**
1. SSH authenticates and launches `rcpd` on remote hosts
2. Certificate fingerprints are transmitted via the secure SSH channel
3. QUIC connections validate certificates against these fingerprints
4. Connections fail if fingerprints don't match (MITM detected)

**What's Protected:**
- ✅ Man-in-the-middle attacks
- ✅ Eavesdropping (all data encrypted)
- ✅ Data tampering (cryptographic integrity)
- ✅ Connection hijacking
- ✅ Unauthorized access (SSH authentication required)

**Trust Model:**
- SSH is the root of trust (use SSH best practices)
- Certificate fingerprints are ephemeral (generated per session)
- No PKI or long-term certificate management needed

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

tracing and tokio-console
=========================

<img src="https://raw.githubusercontent.com/tokio-rs/tracing/master/assets/logo-type.png" height="64" alt="Tracing">

The `rcp` tools now use the `tracing` crate for logging and support sending data to the `tokio-console` subscriber.

## Enabling

To enable the `console-subscriber` you need to set the environment variable `RCP_TOKIO_TRACING_CONSOLE_ENABLED=1` (or `true` with any case).

## Server port

By default port `6669` is used (`tokio-console` default) but this can be changed by setting `RCP_TOKIO_TRACING_CONSOLE_SERVER_PORT=1234`.

## Retention time

The trace events are retained for 60s. This can be modified by setting `RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS=120`.

references
==========
1) https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html
2) https://github.com/wtsi-ssg/pcp