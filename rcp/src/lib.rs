//! Fast file operations tools - `rcp` (copy) and `rcpd` (remote copy daemon)
//!
//! This crate provides high-performance file copying tools that are significantly faster than
//! traditional tools like `cp` when dealing with large numbers of files.
//!
//! # Tools
//!
//! ## rcp - File Copy Tool
//!
//! The `rcp` tool provides both **local** and **remote** file copying with exceptional performance.
//!
//! ### Local Copying
//!
//! Basic local file copying, similar to `cp` but optimized for large filesets:
//!
//! ```bash
//! # Basic copy
//! rcp /source/path /dest/path --progress --summary
//!
//! # Copy with metadata preservation
//! rcp /source /dest --preserve --progress
//!
//! # Overwrite existing files
//! rcp /source /dest --overwrite --progress
//! ```
//!
//! ### Remote Copying
//!
//! Copy files between remote hosts using `host:/path` syntax (similar to `scp`):
//!
//! ```bash
//! # Copy from remote to local
//! rcp user@host:/remote/path /local/path --progress
//!
//! # Copy from local to remote
//! rcp /local/path user@host:/remote/path --progress --preserve
//!
//! # Copy between two remote hosts
//! rcp user@host1:/path1 user@host2:/path2 --progress --summary
//! ```
//!
//! When using remote paths, `rcp` automatically:
//! - Starts `rcpd` daemons on remote hosts via SSH
//! - Uses QUIC protocol for efficient data transfer
//! - Transfers data directly between source and destination (not through the master)
//!
//! **Requirements for remote copying:**
//! - SSH access to remote hosts (uses your SSH config and keys)
//! - `rcpd` binary available in the same directory as `rcp` on remote hosts
//!
//! ## rcpd - Remote Copy Daemon
//!
//! The `rcpd` daemon is automatically launched by `rcp` when using remote paths.
//! It should generally not be invoked directly by users.
//!
//! The daemon operates in two modes:
//! - **Source mode**: Reads files from the source host
//! - **Destination mode**: Writes files to the destination host
//!
//! # Key Features
//!
//! ## Performance
//!
//! - **Parallel operations**: Uses async I/O and worker threads for maximum throughput
//! - **Optimized for large filesets**: Much faster than `cp` when dealing with many files
//! - **QUIC protocol**: Efficient network transport for remote operations
//!
//! ## Copy Semantics
//!
//! Unlike `cp`, `rcp` uses unambiguous path semantics:
//!
//! - **Without trailing slash**: Path is treated as the final destination name
//!   - `rcp A/B C/D` → creates `C/D` (fails if exists)
//! - **With trailing slash**: Path is treated as a directory to copy into
//!   - `rcp A/B C/D/` → creates `C/D/B` (fails if exists)
//!
//! Use `--overwrite` to allow overwriting existing destinations.
//!
//! ## Metadata Preservation
//!
//! - `--preserve`: Preserve all metadata (owner, group, mode, timestamps)
//! - `--preserve-settings`: Fine-grained control over what to preserve
//!
//! ## Error Handling
//!
//! - By default: Log errors and continue processing
//! - `--fail-early`: Stop on first error
//!
//! ## Progress & Logging
//!
//! - `--progress`: Show progress (auto-detects terminal type)
//! - `--summary`: Print summary statistics at the end
//! - `-v/-vv/-vvv`: Control log verbosity (INFO/DEBUG/TRACE)
//! - Progress goes to stderr, logs to stdout (allows piping logs while viewing progress)
//!
//! ## Throttling & Resource Control
//!
//! - `--ops-throttle`: Limit operations per second
//! - `--iops-throttle`: Limit I/O operations per second
//! - `--max-open-files`: Control maximum open file descriptors
//! - `--max-workers`: Control number of worker threads
//!
//! ## Remote Copy Configuration
//!
//! - `--quic-port-ranges`: Restrict QUIC to specific port ranges (e.g., "8000-8999")
//! - `--remote-copy-conn-timeout-sec`: Connection timeout in seconds (default: 15)
//!
//! # Architecture
//!
//! ## Local Copy Architecture
//!
//! Local copying uses async I/O with multiple worker threads to maximize throughput
//! when dealing with large numbers of files.
//!
//! ## Remote Copy Architecture
//!
//! Remote copying uses a three-node architecture:
//!
//! ```text
//! Master (rcp)
//! ├── SSH → Source Host (rcpd in source mode)
//! │   └── QUIC → Master
//! │   └── QUIC Server (waits for Destination)
//! └── SSH → Destination Host (rcpd in destination mode)
//!     └── QUIC → Master
//!     └── QUIC Client → Source
//! ```
//!
//! **Connection flow:**
//! 1. Master starts `rcpd` processes on both hosts via SSH
//! 2. Both `rcpd` processes connect back to Master via QUIC
//! 3. Source `rcpd` starts a QUIC server and sends its address to Master
//! 4. Master forwards the address to Destination `rcpd`
//! 5. Destination connects directly to Source
//! 6. Data flows directly from Source to Destination (not through Master)
//!
//! This architecture ensures efficient data transfer while allowing the Master to
//! coordinate the operation and monitor progress.
//!
//! For detailed network connectivity and troubleshooting information, see the
//! `docs/network_connectivity.md` file in the repository.
//!
//! # Examples
//!
//! ## Local Copy Examples
//!
//! ```bash
//! # Basic copy with progress
//! rcp /source /dest --progress --summary
//!
//! # Copy preserving all metadata
//! rcp /source /dest --preserve --progress
//!
//! # Copy multiple sources into a directory
//! rcp file1 file2 dir3 /dest/ --progress
//! ```
//!
//! ## Remote Copy Examples
//!
//! ```bash
//! # Copy from remote host to local
//! rcp server:/data/files /local/backup --progress --preserve
//!
//! # Copy to remote host
//! rcp /local/data server:/backup/ --progress --summary
//!
//! # Copy between remote hosts with custom port ranges
//! rcp host1:/path1 host2:/path2 --quic-port-ranges "8000-8999" --progress
//! ```
//!
//! # Library Usage
//!
//! This crate also provides library functions for integrating remote copy functionality
//! into other Rust applications. See the module documentation for details.

// Library for shared code between rcp and rcpd binaries
pub mod destination;
pub mod directory_tracker;
pub mod path;
pub mod source;
