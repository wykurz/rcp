//! Remote copy protocol definitions for source-destination communication.
//!
//! # Protocol Overview
//!
//! The remote copy protocol uses TCP for communication between source and destination.
//! The source listens on two ports: a control port for bidirectional messages and a
//! data port for file transfers. Both sides exchange messages to coordinate directory
//! creation, file transfers, and completion.
//!
//! See `docs/remote_protocol.md` for the full protocol specification.
//!
//! # Message Flow
//!
//! ```text
//! Source                              Destination
//!   |                                      |
//!   |  ---- Directory(root, meta) -------> |  Create root, store metadata
//!   |  ---- Directory(child, meta) ------> |  Create child, store metadata
//!   |  ---- Symlink(...) ----------------> |  Create symlink
//!   |  ---- DirStructureComplete --------> |  Structure complete
//!   |                                      |
//!   |  <--- DirectoryManifestChunk(root,..)|  0+ manifest chunks for reused dirs
//!   |                                      |  under --overwrite/--ignore-existing,
//!   |                                      |  sent BEFORE the trigger (FIFO)
//!   |  <--- DirectoryCreated(root) -------- |  Pass-2 trigger
//!   |  <--- DirectoryCreated(child) ------- |
//!   |                                      |
//!   |  ~~~~ File(f) ~~~~~~~~~~~~~~~~~~~~~> |  Write file (not in manifest / differs)
//!   |  ---- FileUnchanged(g) -----------> |  identical g not transferred
//!   |                                      |  All files done → apply metadata
//!   |                                      |
//!   |  <--- DestinationDone -------------- |  Close send side
//!   |  (close send side)                   |  (detect EOF)
//!   |  (detect EOF)                        |  Close connection
//! ```
//!
//! # Error Communication
//!
//! The protocol uses asymmetric error communication:
//! - **Source → Destination**: Must communicate failures (`FileSkipped`, `SymlinkSkipped`)
//!   so destination can track file counts correctly. `FileUnchanged` is also sent Source →
//!   Destination but is an optimization notification (not a failure): it signals the source
//!   skipped a file whose destination copy is already identical, and is counted as
//!   `files_unchanged` on the destination.
//! - **Destination → Source**: Does NOT communicate failures. Destination handles
//!   errors locally and source continues sending the full structure.
//!
//! # Shutdown Sequence
//!
//! Shutdown is coordinated through TCP connection closure:
//! 1. Destination sends `DestinationDone` and closes its send side
//! 2. Source detects EOF on recv, closes its send side
//! 3. Destination detects EOF on recv, closes connection

use serde::{Deserialize, Serialize};
use std::os::unix::fs::MetadataExt;
use std::os::unix::prelude::PermissionsExt;

/// Default cap on the number of pre-existing destination entries the destination will put in a
/// directory's overwrite/ignore-existing manifest. Above this, the manifest is omitted and that
/// directory falls back to transferring-and-draining files (see `docs/remote_protocol.md`). High
/// by default — rcp typically runs on large hosts; the cap is a backstop, not a normal limit.
pub const DEFAULT_OVERWRITE_MANIFEST_MAX_ENTRIES: usize = 5_000_000;

/// A source entry's ACL payload as captured for the wire — or the fact that it was never captured.
///
/// The distinction between the two non-carrying states is load-bearing (`docs/remote_protocol.md`
/// §2.5). `Captured` with `None` fields is an AUTHORITATIVE "the source has no such ACL", which
/// the destination reproduces by REMOVING the attribute — not by leaving whatever it inherited
/// (see [`common::safedir::apply_acls_fd`]). `Unknown` says the wire carries no ACL information at
/// all: either the master never asked ([`ExtendedMetadataCapture`]), or the source COULD NOT read
/// them (a committed directory that failed to open, whose copy is already recorded as an error) —
/// and the destination must leave the destination entry's ACLs alone. Collapsing the two (the
/// previous shape, a pair of bare `Option`s) turned that source-side read failure into an
/// authoritative clear, permanently stripping a reused destination directory's access and default
/// ACLs on a copy that was already failing.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum WireAcls {
    /// No ACL information: not captured, or unreadable at the source. The destination must not
    /// touch the destination entry's ACLs (for a strict-mode locked reused directory, its
    /// original default ACL is restored by the lockdown guard).
    Unknown,
    /// Read from the source entry (its own fd on the hardened walk; by path under `-L`):
    /// authoritative, including the "has none" case.
    Captured {
        /// `system.posix_acl_access`; `None` = the source has none (an instruction to REMOVE).
        access: Option<Vec<u8>>,
        /// `system.posix_acl_default`; directories only (files have no default ACL), same `None`
        /// semantics.
        default: Option<Vec<u8>>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Metadata {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,
    pub mtime: i64,
    pub atime_nsec: i64,
    pub mtime_nsec: i64,
    /// The source's POSIX ACLs — or [`WireAcls::Unknown`] when they were never read (capture off,
    /// or the source could not read them). Only ever `Captured` when the master asked via
    /// [`ExtendedMetadataCapture`].
    pub acls: WireAcls,
}

impl Metadata {
    /// Attach a source entry's POSIX ACLs, carrying the kernel's bytes VERBATIM.
    ///
    /// The bytes are never rebuilt or reordered. POSIX.1e requires canonical entry order
    /// (`USER_OBJ`, named users by ascending uid, `GROUP_OBJ`, named groups by ascending gid,
    /// `MASK`, `OTHER`) and the kernel rejects anything else with `EINVAL` — passing through what
    /// the source kernel already validated sidesteps that entirely. The format is defined
    /// little-endian (`__le16`/`__le32`), so it is portable across hosts as-is. Any future code
    /// that CONSTRUCTS an ACL (id remapping, a synthesized entry) must sort canonically itself.
    ///
    /// This is a builder rather than a field the [`From`] impls fill because neither of them has an
    /// fd to read from: every site that SHOULD carry ACLs is therefore a visible call, and a site
    /// that does not stays [`WireAcls::Unknown`] — which the destination reads as "leave the
    /// destination's ACLs alone", never as a clear.
    #[must_use]
    pub fn with_acls(mut self, acls: &common::safedir::Acls) -> Self {
        self.acls = WireAcls::Captured {
            access: acls.access.clone(),
            default: acls.default.clone(),
        };
        self
    }

    /// The ACLs this wire metadata carries, in the shape the destination applier takes — or
    /// `None` when they are [`WireAcls::Unknown`] and the destination must not touch the
    /// destination entry's ACLs.
    ///
    /// A `Some` holding all-`None` fields means "the source had no ACL", which is a request to
    /// CLEAR, not to leave the destination alone. That is safe to hand the applier because the
    /// source only reads ACLs when the master asked it to, and the master derives that request
    /// from the very same `preserve` settings it hands the destination — see
    /// [`ExtendedMetadataCapture::for_preserve`].
    pub fn captured_acls(&self) -> Option<common::safedir::Acls> {
        match &self.acls {
            WireAcls::Unknown => None,
            WireAcls::Captured { access, default } => Some(common::safedir::Acls {
                access: access.clone(),
                default: default.clone(),
            }),
        }
    }
}

impl common::preserve::Metadata for Metadata {
    fn uid(&self) -> u32 {
        self.uid
    }
    fn gid(&self) -> u32 {
        self.gid
    }
    fn atime(&self) -> i64 {
        self.atime
    }
    fn atime_nsec(&self) -> i64 {
        self.atime_nsec
    }
    fn mtime(&self) -> i64 {
        self.mtime
    }
    fn mtime_nsec(&self) -> i64 {
        self.mtime_nsec
    }
    fn permissions(&self) -> std::fs::Permissions {
        std::fs::Permissions::from_mode(self.mode)
    }
}

impl common::preserve::Metadata for &Metadata {
    fn uid(&self) -> u32 {
        (*self).uid()
    }
    fn gid(&self) -> u32 {
        (*self).gid()
    }
    fn atime(&self) -> i64 {
        (*self).atime()
    }
    fn atime_nsec(&self) -> i64 {
        (*self).atime_nsec()
    }
    fn mtime(&self) -> i64 {
        (*self).mtime()
    }
    fn mtime_nsec(&self) -> i64 {
        (*self).mtime_nsec()
    }
    fn permissions(&self) -> std::fs::Permissions {
        (*self).permissions()
    }
}

impl From<&std::fs::Metadata> for Metadata {
    /// ACLs stay [`WireAcls::Unknown`]: a `stat` snapshot carries no fd, and reading an ACL by
    /// path would answer from whatever inode the name resolves to now rather than the one this
    /// snapshot describes. Call sites that should carry ACLs add them with
    /// [`Metadata::with_acls`]; a site that cannot (the source failed to read them) correctly
    /// leaves them `Unknown`, which the destination reads as "do not touch", never as a clear.
    fn from(metadata: &std::fs::Metadata) -> Self {
        Metadata {
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            atime: metadata.atime(),
            mtime: metadata.mtime(),
            atime_nsec: metadata.atime_nsec(),
            mtime_nsec: metadata.mtime_nsec(),
            acls: WireAcls::Unknown,
        }
    }
}

impl From<&common::safedir::FileMeta> for Metadata {
    /// Build a wire `Metadata` from an fd-pinned [`common::safedir::FileMeta`]
    /// snapshot (obtained via `fstat`/`fstatat` during a TOCTOU-safe walk),
    /// reading every field through the shared `preserve::Metadata` trait so it
    /// stays in lock-step with the `&std::fs::Metadata` conversion above.
    ///
    /// ACLs stay [`WireAcls::Unknown`] here too: a `FileMeta` is a detached snapshot, not the fd
    /// it came from, so it cannot answer an ACL probe. See [`Metadata::with_acls`].
    fn from(meta: &common::safedir::FileMeta) -> Self {
        use common::preserve::Metadata as _;
        Metadata {
            mode: meta.permissions().mode(),
            uid: meta.uid(),
            gid: meta.gid(),
            atime: meta.atime(),
            mtime: meta.mtime(),
            atime_nsec: meta.atime_nsec(),
            mtime_nsec: meta.mtime_nsec(),
            acls: WireAcls::Unknown,
        }
    }
}

/// One pre-existing destination directory entry, sent in a `DirectoryManifestChunk` so the
/// source can skip transferring identical files. `name` is the child name (serialized as a
/// `PathBuf`, matching the rest of the protocol's path handling). `metadata`/`size` are only
/// meaningful when `is_file`.
///
/// The `metadata`'s ACL fields are always `None` here and deliberately so: the manifest exists to
/// answer `--overwrite-compare` (default `size,mtime`), which has no `acl` term, so an ACL-only
/// difference does not make a file "changed" and a skipped file keeps its old ACL. That is the same
/// shape as `mode`, which the default comparison also ignores; closing it means an `acl` term in
/// the compare DSL as well as the bytes here, and is a follow-up rather than part of ACL transport.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExistingEntry {
    pub name: std::path::PathBuf,
    pub is_file: bool,
    pub metadata: Metadata,
    pub size: u64,
}

/// Conservative byte budget for a single `DirectoryManifestChunk`. Well under the control
/// stream's 8 MiB `LengthDelimitedCodec` frame limit, leaving ample margin for the message
/// envelope and the worst-case final entry (a single ~`PATH_MAX` name).
pub const MANIFEST_CHUNK_BYTE_BUDGET: usize = 4 * 1024 * 1024;

/// Conservative serialized-size estimate for one `ExistingEntry`: the variable-length name plus a
/// fixed allowance covering `is_file` + `Metadata` + `size` + per-entry framing overhead. Used
/// only to bound chunk sizes, so over-estimating (smaller, safer chunks) is fine.
fn estimate_entry_size(entry: &ExistingEntry) -> usize {
    entry.name.as_os_str().len() + 64
}

/// Split a directory manifest into chunks each estimated to stay within `byte_budget`, so every
/// `DirectoryManifestChunk` frame fits under the control stream's frame limit. A single entry
/// larger than the budget still gets its own chunk (it cannot be split further); with the 4 MiB
/// budget versus a worst-case ~4 KiB path, a chunk never approaches the 8 MiB frame limit.
/// `chunk_manifest(v, b).concat() == v` for any `v` (no entries lost or reordered).
pub fn chunk_manifest(entries: Vec<ExistingEntry>, byte_budget: usize) -> Vec<Vec<ExistingEntry>> {
    let mut chunks: Vec<Vec<ExistingEntry>> = Vec::new();
    let mut current: Vec<ExistingEntry> = Vec::new();
    let mut current_bytes = 0usize;
    for entry in entries {
        let size = estimate_entry_size(&entry);
        if !current.is_empty() && current_bytes + size > byte_budget {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += size;
        current.push(entry);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// File header sent on unidirectional streams, followed by raw file data.
#[derive(Debug, Deserialize, Serialize)]
pub struct File {
    pub src: std::path::PathBuf,
    pub dst: std::path::PathBuf,
    pub size: u64,
    pub metadata: Metadata,
    pub is_root: bool,
}

/// Wrapper that includes size for comparison purposes.
#[derive(Debug)]
pub struct FileMetadata<'a> {
    pub metadata: &'a Metadata,
    pub size: u64,
}

impl<'a> common::preserve::Metadata for FileMetadata<'a> {
    fn uid(&self) -> u32 {
        self.metadata.uid()
    }
    fn gid(&self) -> u32 {
        self.metadata.gid()
    }
    fn atime(&self) -> i64 {
        self.metadata.atime()
    }
    fn atime_nsec(&self) -> i64 {
        self.metadata.atime_nsec()
    }
    fn mtime(&self) -> i64 {
        self.metadata.mtime()
    }
    fn mtime_nsec(&self) -> i64 {
        self.metadata.mtime_nsec()
    }
    fn permissions(&self) -> std::fs::Permissions {
        self.metadata.permissions()
    }
    fn size(&self) -> u64 {
        self.size
    }
}

/// Messages sent from source to destination on the control stream.
#[derive(Debug, Deserialize, Serialize)]
pub enum SourceMessage {
    /// Create directory, store metadata, and declare entry counts for completion tracking.
    /// Sent during directory tree traversal in depth-first order. Source pre-reads the
    /// directory children before sending, so counts are known at send time.
    Directory {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        metadata: Metadata,
        is_root: bool,
        /// total child entries (files + directories + symlinks) for completion tracking
        entry_count: usize,
        /// whether to keep this directory if it ends up empty after filtering
        keep_if_empty: bool,
    },
    /// Create symlink with metadata.
    Symlink {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        target: std::path::PathBuf,
        metadata: Metadata,
        is_root: bool,
    },
    /// Signal that all directories and symlinks have been sent.
    /// Required before destination can send `DestinationDone`.
    /// `has_root_item` indicates whether a root file/directory/symlink will be sent.
    /// When false (dry-run or filtered root), destination can mark root as complete.
    DirStructureComplete { has_root_item: bool },
    /// Notify destination that a file failed to send.
    /// Counts as a processed entry for the parent directory's completion tracking.
    FileSkipped {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
    },
    /// Notify destination that the source skipped sending a file because the destination
    /// already holds a matching entry (per the directory manifest). Counts as a processed
    /// entry for the parent directory and as `files_unchanged` (the destination is the
    /// authority for that count). The control-stream sibling of `FileSkipped`.
    FileUnchanged {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
    },
    /// Notify destination that a symlink failed to read.
    /// If `is_root` is true, this signals that root processing is complete (even if failed).
    /// Non-root skipped symlinks count as a processed entry for the parent directory.
    SymlinkSkipped { src_dst: SrcDst, is_root: bool },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SrcDst {
    pub src: std::path::PathBuf,
    pub dst: std::path::PathBuf,
}

/// Messages sent from destination to source on the control stream.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DestinationMessage {
    /// Carry a chunk of the (reused) destination directory's pre-existing-entry manifest, used
    /// by the source to skip transferring identical files. A directory's manifest is split into
    /// one or more chunks (each well under the control stream's frame limit) and ALL of them are
    /// sent BEFORE the directory's `DirectoryCreated`; the control stream is FIFO, so the source
    /// has the complete manifest by the time it sees `DirectoryCreated`. No chunks are sent for a
    /// freshly-created directory, when neither `--overwrite` nor `--ignore-existing` is active, or
    /// when the directory exceeds the manifest cap (see `RcpdConfig::overwrite_manifest_max_entries`).
    DirectoryManifestChunk {
        dst: std::path::PathBuf,
        entries: Vec<ExistingEntry>,
    },
    /// Confirm directory created, request file transfers. This is purely the
    /// Pass-2 trigger: it tells the source the destination created the directory
    /// and is ready to receive its files. The source already retains the
    /// authoritative Pass-1 file count for the directory (in its fd-map entry under hardened
    /// reads, or in a path-keyed Pass-1 entry under `-L`), so no count is echoed back here. Any
    /// `DirectoryManifestChunk`s for this directory precede this message.
    DirectoryCreated {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
    },
    /// Acknowledge a `Directory` message the destination did NOT create (create
    /// failed, ancestor failed, or `--ignore-existing` skipped a non-directory).
    /// No files will be requested for it. The destination sends exactly one of
    /// `DirectoryCreated` / `DirectorySkipped` per `Directory` message so the
    /// source can release the matching Pass-1 entry (a held directory fd under hardened reads, or
    /// an owned pacing credit under `-L`): without this nack a skipped directory's permit would
    /// never be released, hanging large no-ack subtrees. `src` keys the source-side entry to
    /// release (the map is inserted under `src`; see `take_for_skipped`); `dst` is carried for
    /// symmetry/logging.
    DirectorySkipped {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
    },
    /// Signal destination has finished all operations.
    /// Initiates graceful shutdown via stream closure.
    DestinationDone,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RcpdConfig {
    pub verbose: u8,
    pub fail_early: bool,
    pub max_workers: usize,
    pub max_blocking_threads: usize,
    /// Master-resolved file-like work limit, propagated through the version-sensitive rcpd spawn
    /// contract rather than a protocol message.
    pub max_files_in_flight: common::ConcurrencyLimit,
    pub ops_throttle: usize,
    pub iops_throttle: usize,
    pub chunk_size: usize,
    /// Adaptive metadata-ops throttle settings, propagated from the
    /// master's `--auto-meta-*` flags. `None` means the feature is off on
    /// this rcpd instance.
    pub auto_meta: Option<common::AutoMetaThrottleConfig>,
    /// Mirror of master's --auto-meta-histogram flag.
    pub auto_meta_histogram: bool,
    /// Mirror of master's --auto-meta-histogram-log path. Each rcpd
    /// suffixes its own trace identifier so the master and rcpds don't
    /// collide on a localhost run.
    pub auto_meta_histogram_log: Option<String>,
    /// Mirror of master's --auto-meta-histogram-interval.
    pub auto_meta_histogram_interval: std::time::Duration,
    // common::copy::Settings
    pub dereference: bool,
    /// Mirror of master's --require-toctou-safe flag: arms strict operand
    /// resolution (openat2 RESOLVE_NO_SYMLINKS root opens) on the rcpd side.
    pub require_toctou_safe: bool,
    pub overwrite: bool,
    pub overwrite_compare: String,
    /// Cap on pre-existing entries put into a directory's overwrite/ignore-existing manifest.
    pub overwrite_manifest_max_entries: usize,
    pub overwrite_filter: Option<String>,
    pub ignore_existing: bool,
    pub skip_specials: bool,
    pub debug_log_prefix: Option<String>,
    /// Port ranges for TCP connections (e.g., "8000-8999,9000-9999")
    pub port_ranges: Option<String>,
    pub progress: bool,
    pub progress_delay: Option<String>,
    pub remote_copy_conn_timeout_sec: u64,
    /// Mirror of master's --remote-keepalive-sec: the liveness budget for every rcp TCP
    /// connection this rcpd establishes or accepts. Without propagation the master would
    /// recover from a vanished host while both rcpds keep hanging.
    pub remote_keepalive_sec: u64,
    /// Network profile for buffer sizing
    pub network_profile: crate::NetworkProfile,
    /// Buffer size for file transfers (defaults to profile-specific value)
    pub buffer_size: Option<usize>,
    /// Effective concurrent data-stream ceiling resolved by the master
    pub max_connections: usize,
    /// Multiplier for pending file writes (max pending = max_connections × multiplier)
    pub pending_writes_multiplier: usize,
    /// Chrome trace output prefix for profiling
    pub chrome_trace_prefix: Option<String>,
    /// Flamegraph output prefix for profiling
    pub flamegraph_prefix: Option<String>,
    /// Log level for profiling (default: trace when profiling is enabled)
    pub profile_level: Option<String>,
    /// Enable tokio-console
    pub tokio_console: bool,
    /// Port for tokio-console server
    pub tokio_console_port: Option<u16>,
    /// Enable TLS encryption (default: true)
    pub encryption: bool,
    /// Master's certificate fingerprint for client authentication (when encryption enabled)
    pub master_cert_fingerprint: Option<CertFingerprint>,
}

impl RcpdConfig {
    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![
            format!("--max-workers={}", self.max_workers),
            format!("--max-blocking-threads={}", self.max_blocking_threads),
            format!("--ops-throttle={}", self.ops_throttle),
            format!("--iops-throttle={}", self.iops_throttle),
            format!("--chunk-size={}", self.chunk_size),
            format!("--overwrite-compare={}", self.overwrite_compare),
            format!(
                "--overwrite-manifest-max-entries={}",
                self.overwrite_manifest_max_entries
            ),
        ];
        if self.verbose > 0 {
            args.push(format!("-{}", "v".repeat(self.verbose as usize)));
        }
        if self.fail_early {
            args.push("--fail-early".to_string());
        }
        match self.max_files_in_flight {
            common::ConcurrencyLimit::Limited(value) => {
                args.push(format!("--max-files-in-flight={value}"));
            }
            common::ConcurrencyLimit::Unlimited => args.push("--max-open-files=0".to_string()),
        }
        args.push("--files-in-flight-propagated".to_string());
        if self.dereference {
            args.push("--dereference".to_string());
        }
        if self.require_toctou_safe {
            args.push("--require-toctou-safe".to_string());
        }
        if self.overwrite {
            args.push("--overwrite".to_string());
            if let Some(ref filter) = self.overwrite_filter {
                args.push(format!("--overwrite-filter={filter}"));
            }
        }
        if self.ignore_existing {
            args.push("--ignore-existing".to_string());
        }
        if self.skip_specials {
            args.push("--skip-specials".to_string());
        }
        if let Some(ref prefix) = self.debug_log_prefix {
            args.push(format!("--debug-log-prefix={prefix}"));
        }
        if let Some(ref ranges) = self.port_ranges {
            args.push(format!("--port-ranges={ranges}"));
        }
        if self.progress {
            args.push("--progress".to_string());
        }
        if let Some(ref delay) = self.progress_delay {
            args.push(format!("--progress-delay={delay}"));
        }
        args.push(format!(
            "--remote-copy-conn-timeout-sec={}",
            self.remote_copy_conn_timeout_sec
        ));
        args.push(format!(
            "--remote-keepalive-sec={}",
            self.remote_keepalive_sec
        ));
        // network profile
        args.push(format!("--network-profile={}", self.network_profile));
        // tcp tuning (only if set)
        if let Some(v) = self.buffer_size {
            args.push(format!("--buffer-size={v}"));
        }
        args.push(format!("--max-connections={}", self.max_connections));
        args.push(format!(
            "--pending-writes-multiplier={}",
            self.pending_writes_multiplier
        ));
        // profiling options (only add --profile-level when profiling is enabled)
        let profiling_enabled =
            self.chrome_trace_prefix.is_some() || self.flamegraph_prefix.is_some();
        if let Some(ref prefix) = self.chrome_trace_prefix {
            args.push(format!("--chrome-trace={prefix}"));
        }
        if let Some(ref prefix) = self.flamegraph_prefix {
            args.push(format!("--flamegraph={prefix}"));
        }
        if profiling_enabled && let Some(level) = &self.profile_level {
            args.push(format!("--profile-level={level}"));
        }
        if self.tokio_console {
            args.push("--tokio-console".to_string());
        }
        if let Some(port) = self.tokio_console_port {
            args.push(format!("--tokio-console-port={port}"));
        }
        if !self.encryption {
            args.push("--no-encryption".to_string());
        }
        if let Some(fp) = self.master_cert_fingerprint {
            args.push(format!(
                "--master-cert-fp={}",
                crate::tls::fingerprint_to_hex(&fp)
            ));
        }
        // propagate the adaptive metadata-ops throttle settings to rcpd so a
        // remote copy uses the same control law as the master-side tool.
        if let Some(auto) = &self.auto_meta {
            args.push("--auto-meta-throttle".to_string());
            args.push(format!("--auto-meta-initial-cwnd={}", auto.initial_cwnd));
            args.push(format!("--auto-meta-min-cwnd={}", auto.min_cwnd));
            args.push(format!("--auto-meta-max-cwnd={}", auto.max_cwnd));
            args.push(format!("--auto-meta-alpha={}", auto.alpha));
            args.push(format!("--auto-meta-beta={}", auto.beta));
            args.push(format!(
                "--auto-meta-baseline-percentile={}",
                auto.baseline_percentile,
            ));
            args.push(format!(
                "--auto-meta-current-percentile={}",
                auto.current_percentile,
            ));
            args.push(format!("--auto-meta-increase-step={}", auto.increase_step));
            args.push(format!("--auto-meta-decrease-step={}", auto.decrease_step));
            args.push(format!(
                "--auto-meta-long-window={}",
                humantime::format_duration(auto.long_window),
            ));
            args.push(format!(
                "--auto-meta-short-window={}",
                humantime::format_duration(auto.short_window),
            ));
            args.push(format!(
                "--auto-meta-tick-interval={}",
                humantime::format_duration(auto.tick_interval),
            ));
        }
        // Only forward histogram flags when there's a log path: the panel-
        // only flag (--auto-meta-histogram) makes rcpd pay the synchronous
        // accumulator lock cost on every probe, but rcpd's panel never
        // reaches the user (the master's remote-progress renderer doesn't
        // read the rcpd histogram registry). The log path is different —
        // it produces a concrete artifact on the rcpd's host that the user
        // can collect after the run.
        if let Some(path) = &self.auto_meta_histogram_log {
            args.push(format!("--auto-meta-histogram-log={path}"));
            args.push(format!(
                "--auto-meta-histogram-interval={}",
                humantime::format_duration(self.auto_meta_histogram_interval),
            ));
        }
        args
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum RcpdRole {
    Source,
    Destination,
}

impl std::fmt::Display for RcpdRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RcpdRole::Source => write!(f, "source"),
            RcpdRole::Destination => write!(f, "destination"),
        }
    }
}

impl std::str::FromStr for RcpdRole {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "source" => Ok(RcpdRole::Source),
            "destination" | "dest" => Ok(RcpdRole::Destination),
            _ => Err(anyhow::anyhow!("invalid role: {}", s)),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TracingHello {
    pub role: RcpdRole,
    /// true for tracing/progress connection, false for control connection
    pub is_tracing: bool,
}

/// TLS certificate fingerprint (SHA-256 of DER-encoded certificate).
pub type CertFingerprint = [u8; 32];

/// What EXTENDED metadata the SOURCE must read, beyond the `stat` it already does.
///
/// Two of the three fields are per-entry reads whose bytes are sent; the third buys one read on the
/// ROOT whose only product is a log line. All three answer the same question — what may the source
/// spend an xattr syscall on — so with every field false the source issues none, EXCEPT under
/// `--require-toctou-safe`: that flag arms the root notice from the source's own mirrored copy of
/// it rather than from here, so a strict run still pays the one root `listxattr` on an all-false
/// capture. Nothing per-entry is reachable that way — only `file_acl`/`dir_acl` open that door.
///
/// Carried in [`MasterHello::Source`] because the source otherwise has no way to know: only
/// [`MasterHello::Destination`] carries `preserve`, so without this the source would have to probe
/// unconditionally and every remote copy — including every one that does not want ACLs — would pay
/// a syscall per entry that `stat` cannot fold in. That cost is the whole reason `acl` is opt-in.
///
/// Deliberately NOT the whole `preserve` struct, and named for what it is. The source decides only
/// what to READ; the destination remains the sole authority on what is APPLIED. Handing the source
/// a `preserve` would invite a later reader to act on, say, `preserve.file.mode_mask` source-side,
/// which would be a bug. Every field [`MasterHello::Source`] already carries (`filter`, `dry_run`)
/// is of this same "what to read/send" kind. The name leaves room for an `-X`-style xattr option to
/// land here rather than growing another field.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ExtendedMetadataCapture {
    /// Read each source FILE's `system.posix_acl_access`.
    pub file_acl: bool,
    /// Read each source DIRECTORY's access AND default ACLs.
    pub dir_acl: bool,
    /// Probe the source ROOT once for a POSIX ACL, so the source can say that this copy will not
    /// carry it across (see [`common::safedir::RootAclNotice`]).
    ///
    /// Independent of the two above: it is armed by the master's `preserve` asking for metadata
    /// fidelity AT ALL, and disarmed by a copy that asked for none — the reads above are armed by
    /// `preserve` asking for `acl` specifically, which is the case where there is nothing to warn
    /// about. A run at the shipped default sets all three to false and the source touches no xattr.
    pub root_acl_notice: bool,
}

impl ExtendedMetadataCapture {
    /// What a copy preserving `preserve` requires the source to read.
    ///
    /// The ONE derivation, called at the single master site that also sends `preserve` to the
    /// destination, so the two cannot disagree. That matters: the destination treats an all-`None`
    /// [`Metadata::acls`] as "the source had none, so CLEAR", so a capture that said `false` while
    /// the destination's `preserve` said `true` would silently strip every ACL instead of copying
    /// it. Symlinks are absent by construction — the kernel has no symlink ACL, and the settings
    /// parser rejects `l:acl`.
    pub fn for_preserve(preserve: &common::preserve::Settings) -> Self {
        Self {
            file_acl: preserve.file.acl,
            dir_acl: preserve.dir.acl,
            root_acl_notice: preserve.requests_preservation(),
        }
    }
}

impl From<ExtendedMetadataCapture> for common::safedir::RootAclNotice {
    /// The source's view of the notice, reassembled from what crossed the wire.
    ///
    /// `--require-toctou-safe` is absent here on purpose: it reaches the source `rcpd` as its own
    /// mirrored flag and is read from the process-global strict state, so it arms the notice on the
    /// host that actually runs the probe rather than travelling twice.
    fn from(capture: ExtendedMetadataCapture) -> Self {
        Self {
            wanted: capture.root_acl_notice,
            file_acl_preserved: capture.file_acl,
            dir_acl_preserved: capture.dir_acl,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MasterHello {
    Source {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        /// Destination's TLS certificate fingerprint (None if encryption disabled)
        dest_cert_fingerprint: Option<CertFingerprint>,
        /// Filter settings for include/exclude patterns (source-side filtering)
        filter: Option<common::filter::FilterSettings>,
        /// Dry-run mode for previewing operations
        dry_run: Option<common::config::DryRunMode>,
        /// Extended metadata the source must read per entry (currently: POSIX ACLs). When every
        /// field is false the source issues no xattr syscall at all.
        capture: ExtendedMetadataCapture,
    },
    Destination {
        /// TCP address for control connection to source
        source_control_addr: std::net::SocketAddr,
        /// TCP address for data connections to source
        source_data_addr: std::net::SocketAddr,
        server_name: String,
        preserve: common::preserve::Settings,
        /// Source's TLS certificate fingerprint (None if encryption disabled)
        source_cert_fingerprint: Option<CertFingerprint>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceMasterHello {
    /// TCP address for control connection (bidirectional messages)
    pub control_addr: std::net::SocketAddr,
    /// TCP address for data connections (file transfers)
    pub data_addr: std::net::SocketAddr,
    pub server_name: String,
}

// re-export RuntimeStats from common for convenience
pub use common::RuntimeStats;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum RcpdResult {
    Success {
        message: String,
        summary: common::copy::Summary,
        runtime_stats: common::RuntimeStats,
    },
    Failure {
        error: String,
        summary: common::copy::Summary,
        runtime_stats: common::RuntimeStats,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_entry(name: &str) -> ExistingEntry {
        ExistingEntry {
            name: std::path::PathBuf::from(name),
            is_file: true,
            metadata: Metadata {
                mode: 0o644,
                uid: 0,
                gid: 0,
                atime: 0,
                mtime: 0,
                atime_nsec: 0,
                mtime_nsec: 0,
                acls: WireAcls::Unknown,
            },
            size: 0,
        }
    }

    #[test]
    fn chunk_manifest_empty_yields_no_chunks() {
        assert!(chunk_manifest(vec![], MANIFEST_CHUNK_BYTE_BUDGET).is_empty());
    }

    #[test]
    fn chunk_manifest_small_is_single_chunk() {
        let entries: Vec<_> = (0..100).map(|i| mk_entry(&format!("f{i}.txt"))).collect();
        let chunks = chunk_manifest(entries, MANIFEST_CHUNK_BYTE_BUDGET);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 100);
    }

    #[test]
    fn chunk_manifest_splits_and_preserves_all_entries_in_order() {
        let entries: Vec<_> = (0..1000)
            .map(|i| mk_entry(&format!("file_{i:04}.dat")))
            .collect();
        // a tiny budget forces many chunks (each entry estimate is name.len() + 64 ≈ ~78 bytes)
        let chunks = chunk_manifest(entries.clone(), 256);
        assert!(
            chunks.len() > 1,
            "tiny budget should produce multiple chunks"
        );
        // frame-safety property: every multi-entry chunk stays within budget, so a chunk frame
        // never approaches the control stream's frame limit
        for chunk in &chunks {
            if chunk.len() > 1 {
                let total: usize = chunk.iter().map(estimate_entry_size).sum();
                assert!(total <= 256, "multi-entry chunk exceeds budget: {total}");
            }
        }
        // reassembly invariant: concat preserves every entry, in order, with nothing lost
        let flat: Vec<_> = chunks.into_iter().flatten().collect();
        assert_eq!(flat.len(), entries.len());
        for (got, want) in flat.iter().zip(entries.iter()) {
            assert_eq!(got.name, want.name);
        }
    }

    #[test]
    fn chunk_manifest_entry_larger_than_budget_gets_its_own_chunk() {
        // budget smaller than a single entry: each still lands in its own chunk (never dropped).
        let chunks = chunk_manifest(vec![mk_entry("a"), mk_entry("b")], 1);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 1);
        assert_eq!(chunks[1].len(), 1);
    }

    #[test]
    fn chunk_manifest_splits_a_large_manifest_at_the_production_budget() {
        // a realistically large directory manifest (more than two budgets' worth) must split into
        // multiple frames with the PRODUCTION budget — not just an artificially tiny one — so a
        // change to the budget or to `estimate_entry_size` that breaks chunking at real scale is
        // caught. built in memory (no files), so it stays a fast unit test.
        let per_entry = estimate_entry_size(&mk_entry("file_0000000.dat"));
        let n = (MANIFEST_CHUNK_BYTE_BUDGET / per_entry) * 2 + 1000;
        let entries: Vec<_> = (0..n)
            .map(|i| mk_entry(&format!("file_{i:07}.dat")))
            .collect();
        let chunks = chunk_manifest(entries.clone(), MANIFEST_CHUNK_BYTE_BUDGET);
        assert!(
            chunks.len() > 1,
            "a manifest larger than the 4 MiB budget must span multiple chunks, got {}",
            chunks.len()
        );
        // every multi-entry chunk stays within the real budget (frame-safety)
        for chunk in &chunks {
            if chunk.len() > 1 {
                let total: usize = chunk.iter().map(estimate_entry_size).sum();
                assert!(
                    total <= MANIFEST_CHUNK_BYTE_BUDGET,
                    "a chunk exceeds the production budget: {total}"
                );
            }
        }
        // reassembly invariant at production scale: concat preserves every entry, in order
        let flat: Vec<_> = chunks.into_iter().flatten().collect();
        assert_eq!(
            flat.len(),
            entries.len(),
            "reassembly must preserve every entry"
        );
        assert!(
            flat.iter().zip(&entries).all(|(a, b)| a.name == b.name),
            "reassembly must preserve order"
        );
    }

    #[test]
    fn metadata_carries_acl_bytes_verbatim_through_a_wire_round_trip() {
        // the bytes must survive serialization UNCHANGED and in order: POSIX.1e requires canonical
        // entry order and the destination kernel rejects anything else with EINVAL, so any
        // re-encoding on the way through would fail at `fsetxattr` rather than silently.
        let access: Vec<u8> = (0u8..=200).collect();
        let default: Vec<u8> = (0u8..=100).rev().collect();
        let meta = mk_entry("f").metadata.with_acls(&common::safedir::Acls {
            access: Some(access.clone()),
            default: Some(default.clone()),
        });
        // through the same codec the streams use, so this pins the real wire behavior
        let bytes = bitcode::serialize(&meta).unwrap();
        let back: Metadata = bitcode::deserialize(&bytes).unwrap();
        // and the round trip reproduces the applier-side shape exactly
        assert_eq!(
            back.captured_acls(),
            Some(common::safedir::Acls {
                access: Some(access),
                default: Some(default),
            })
        );
    }

    #[test]
    fn metadata_built_from_a_stat_snapshot_carries_unknown_acls() {
        // neither `From` impl has an fd to probe, so both must leave the ACLs Unknown rather than
        // guess — a site that should carry ACLs makes a visible `with_acls` call instead. Unknown
        // tells the destination to leave the destination entry's ACLs alone; only a visible
        // capture may produce the all-`None` CLEAR request.
        let meta = Metadata::from(&std::fs::metadata(".").unwrap());
        assert_eq!(meta.captured_acls(), None);
    }

    #[test]
    fn with_acls_round_trips_the_absent_case_as_a_clear_request() {
        // Captured-with-both-None is distinct from Unknown: it is the authoritative "the source
        // has no ACL", which the destination applies by REMOVING the attributes.
        let meta = mk_entry("f")
            .metadata
            .with_acls(&common::safedir::Acls::default());
        assert_eq!(meta.captured_acls(), Some(common::safedir::Acls::default()));
    }

    #[test]
    fn capture_mirrors_the_preserve_settings_it_is_derived_from() {
        // the one derivation: a capture that said `false` while the destination's `preserve` said
        // `true` would make every entry arrive with all-`None` ACLs, which the destination reads as
        // "clear" — stripping ACLs instead of copying them. Pinned per type, since the two flags are
        // independent.
        assert_eq!(
            ExtendedMetadataCapture::for_preserve(&common::preserve::preserve_all_with_acls()),
            ExtendedMetadataCapture {
                file_acl: true,
                dir_acl: true,
                // armed by the run asking for preservation, but the per-kind flags above then turn
                // the probe away: there is nothing to warn about once both kinds are carried across
                root_acl_notice: true,
            }
        );
        assert_eq!(
            ExtendedMetadataCapture::for_preserve(&common::preserve::preserve_all()),
            ExtendedMetadataCapture {
                file_acl: false,
                dir_acl: false,
                root_acl_notice: true,
            },
            "`all` deliberately excludes ACLs, so it must not make the source pay the per-entry \
             read — but it DID ask for metadata fidelity, so the one root probe behind the notice \
             is exactly the case the notice exists for"
        );
        let mut file_only = common::preserve::preserve_none();
        file_only.file.acl = true;
        assert_eq!(
            ExtendedMetadataCapture::for_preserve(&file_only),
            ExtendedMetadataCapture {
                file_acl: true,
                dir_acl: false,
                root_acl_notice: true,
            }
        );
    }

    #[test]
    fn a_copy_asking_for_no_preservation_makes_the_source_read_nothing() {
        // the whole point of the notice gate: a remote copy left at the shipped default must not
        // make the source touch a single xattr, root probe included
        assert_eq!(
            ExtendedMetadataCapture::for_preserve(&common::preserve::preserve_none()),
            ExtendedMetadataCapture::default(),
        );
    }

    #[test]
    fn the_notice_the_source_reassembles_matches_the_capture_it_came_from() {
        // the source has no `preserve` of its own, so the notice it acts on is rebuilt from the
        // wire; a field crossed over here would silence or misdirect the probe on the far host
        let capture = ExtendedMetadataCapture {
            file_acl: true,
            dir_acl: false,
            root_acl_notice: true,
        };
        assert_eq!(
            common::safedir::RootAclNotice::from(capture),
            common::safedir::RootAclNotice {
                wanted: true,
                file_acl_preserved: true,
                dir_acl_preserved: false,
            }
        );
    }

    fn minimal_rcpd_config() -> RcpdConfig {
        RcpdConfig {
            verbose: 0,
            fail_early: false,
            max_workers: 0,
            max_blocking_threads: 0,
            max_files_in_flight: common::ConcurrencyLimit::Limited(
                std::num::NonZeroUsize::new(4).unwrap(),
            ),
            ops_throttle: 0,
            iops_throttle: 0,
            chunk_size: 0,
            auto_meta: None,
            auto_meta_histogram: false,
            auto_meta_histogram_log: None,
            auto_meta_histogram_interval: std::time::Duration::from_secs(1),
            dereference: false,
            require_toctou_safe: false,
            overwrite: false,
            overwrite_compare: "size,mtime".to_string(),
            overwrite_filter: None,
            ignore_existing: false,
            skip_specials: false,
            debug_log_prefix: None,
            port_ranges: None,
            progress: false,
            progress_delay: None,
            remote_copy_conn_timeout_sec: 30,
            remote_keepalive_sec: crate::DEFAULT_REMOTE_KEEPALIVE_SEC,
            network_profile: crate::NetworkProfile::default(),
            buffer_size: None,
            max_connections: 1,
            pending_writes_multiplier: 1,
            chrome_trace_prefix: None,
            flamegraph_prefix: None,
            profile_level: None,
            tokio_console: false,
            tokio_console_port: None,
            encryption: true,
            master_cert_fingerprint: None,
            overwrite_manifest_max_entries: DEFAULT_OVERWRITE_MANIFEST_MAX_ENTRIES,
        }
    }

    #[test]
    fn to_args_includes_overwrite_manifest_max_entries() {
        let mut config = minimal_rcpd_config();
        config.overwrite_manifest_max_entries = 123_456;
        let args = config.to_args();
        assert!(
            args.iter()
                .any(|a| a == "--overwrite-manifest-max-entries=123456"),
            "expected manifest cap flag in {args:?}"
        );
    }

    #[test]
    fn to_args_propagates_finite_files_in_flight() {
        let mut config = minimal_rcpd_config();
        config.max_files_in_flight =
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(7).unwrap());
        let args = config.to_args();
        assert!(args.iter().any(|arg| arg == "--max-files-in-flight=7"));
        assert!(args.iter().any(|arg| arg == "--files-in-flight-propagated"));
    }

    #[test]
    fn to_args_preserves_legacy_unlimited_files_in_flight() {
        let mut config = minimal_rcpd_config();
        config.max_files_in_flight = common::ConcurrencyLimit::Unlimited;
        let args = config.to_args();
        assert!(args.iter().any(|arg| arg == "--max-open-files=0"));
        assert!(args.iter().any(|arg| arg == "--files-in-flight-propagated"));
    }

    #[test]
    fn to_args_propagates_effective_connection_count() {
        let mut config = minimal_rcpd_config();
        config.max_connections = 7;
        assert!(
            config
                .to_args()
                .iter()
                .any(|arg| arg == "--max-connections=7")
        );
    }

    #[test]
    fn to_args_mirrors_require_toctou_safe() {
        let mut config = minimal_rcpd_config();
        config.require_toctou_safe = true;
        let args = config.to_args();
        assert!(
            args.iter().any(|a| a == "--require-toctou-safe"),
            "expected --require-toctou-safe in {args:?}"
        );
        let config = minimal_rcpd_config();
        assert!(
            !config
                .to_args()
                .iter()
                .any(|a| a == "--require-toctou-safe"),
            "flag must be omitted when off"
        );
    }

    // an rcpd that does not get the budget keeps hanging on a vanished peer while the master
    // recovers — which looks fixed and is worse than today's symmetric hang
    #[test]
    fn to_args_propagates_remote_keepalive_sec() {
        let mut config = minimal_rcpd_config();
        config.remote_keepalive_sec = 45;
        assert!(
            config
                .to_args()
                .iter()
                .any(|a| a == "--remote-keepalive-sec=45"),
            "expected the keepalive budget in {:?}",
            config.to_args()
        );
        // 0 disables keepalive and must travel as faithfully as any other value
        config.remote_keepalive_sec = 0;
        assert!(
            config
                .to_args()
                .iter()
                .any(|a| a == "--remote-keepalive-sec=0"),
            "expected the disabling value to be passed through, not dropped"
        );
        let default_args = minimal_rcpd_config().to_args();
        assert!(
            default_args.iter().any(|a| a
                == &format!(
                    "--remote-keepalive-sec={}",
                    crate::DEFAULT_REMOTE_KEEPALIVE_SEC
                )),
            "the default must be passed explicitly so rcpd cannot drift from the master: {default_args:?}"
        );
    }

    #[test]
    fn to_args_omits_auto_meta_throttle_when_none() {
        let args = minimal_rcpd_config().to_args();
        // throttle-specific flags must be absent when auto_meta is None
        let throttle_flags = [
            "--auto-meta-throttle",
            "--auto-meta-initial-cwnd",
            "--auto-meta-min-cwnd",
            "--auto-meta-max-cwnd",
            "--auto-meta-alpha",
            "--auto-meta-beta",
            "--auto-meta-baseline-percentile",
            "--auto-meta-current-percentile",
            "--auto-meta-increase-step",
            "--auto-meta-decrease-step",
            "--auto-meta-long-window",
            "--auto-meta-short-window",
            "--auto-meta-tick-interval",
        ];
        for flag in throttle_flags {
            assert!(
                !args.iter().any(|a| a.starts_with(flag)),
                "throttle flag {flag} should not be emitted when auto_meta is None: {args:?}",
            );
        }
        // histogram flag, log, and interval must all be absent when histograms are off
        for arg in &args {
            assert!(
                !arg.starts_with("--auto-meta-histogram"),
                "must not emit any histogram flag when histograms are off, found: {arg}",
            );
        }
    }

    #[test]
    fn to_args_propagates_all_auto_meta_fields() {
        let mut config = minimal_rcpd_config();
        config.auto_meta = Some(common::AutoMetaThrottleConfig {
            initial_cwnd: 8,
            min_cwnd: 2,
            max_cwnd: 128,
            alpha: 1.2,
            beta: 1.6,
            increase_step: 2,
            decrease_step: 3,
            baseline_percentile: 0.4,
            current_percentile: 0.6,
            long_window: std::time::Duration::from_secs(20),
            short_window: std::time::Duration::from_secs(2),
            tick_interval: std::time::Duration::from_millis(75),
        });
        let args = config.to_args();
        let has = |needle: &str| args.iter().any(|a| a == needle);
        let has_prefix = |needle: &str| args.iter().any(|a| a.starts_with(needle));
        assert!(has("--auto-meta-throttle"));
        assert!(has("--auto-meta-initial-cwnd=8"));
        assert!(has("--auto-meta-min-cwnd=2"));
        assert!(has("--auto-meta-max-cwnd=128"));
        assert!(has_prefix("--auto-meta-alpha=1.2"));
        assert!(has_prefix("--auto-meta-beta=1.6"));
        assert!(has_prefix("--auto-meta-baseline-percentile=0.4"));
        assert!(has_prefix("--auto-meta-current-percentile=0.6"));
        assert!(has("--auto-meta-increase-step=2"));
        assert!(has("--auto-meta-decrease-step=3"));
        assert!(has_prefix("--auto-meta-long-window="));
        assert!(has_prefix("--auto-meta-short-window="));
        assert!(has_prefix("--auto-meta-tick-interval="));
    }

    #[test]
    fn to_args_omits_histogram_flags_when_disabled() {
        // Critical for backward compatibility: existing rcpd binaries
        // built without histogram support reject --auto-meta-histogram-*
        // flags, so we must not emit them on every remote copy.
        let mut config = minimal_rcpd_config();
        config.auto_meta_histogram = false;
        config.auto_meta_histogram_log = None;
        let args = config.to_args();
        for arg in &args {
            assert!(
                !arg.starts_with("--auto-meta-histogram"),
                "must not emit histogram flag when disabled, found: {arg}",
            );
        }
    }

    #[test]
    fn to_args_omits_panel_only_flag_when_no_log_path() {
        // Panel-only --auto-meta-histogram is intentionally NOT forwarded
        // to rcpd: the panel never reaches the user (no plumbing in remote
        // progress), and forwarding would just add per-probe lock cost.
        let mut config = minimal_rcpd_config();
        config.auto_meta_histogram = true;
        config.auto_meta_histogram_log = None;
        let args = config.to_args();
        for arg in &args {
            assert!(
                !arg.starts_with("--auto-meta-histogram"),
                "panel-only flag must not be forwarded to rcpd, found: {arg}",
            );
        }
    }

    #[test]
    fn to_args_forwards_histogram_log_and_interval_when_log_path_set() {
        let mut config = minimal_rcpd_config();
        config.auto_meta_histogram = false; // panel-only off
        config.auto_meta_histogram_log = Some("/tmp/foo.hdr".into());
        config.auto_meta_histogram_interval = std::time::Duration::from_millis(500);
        let args = config.to_args();
        assert!(
            args.iter()
                .any(|a| a == "--auto-meta-histogram-log=/tmp/foo.hdr")
        );
        assert!(
            args.iter()
                .any(|a| a.starts_with("--auto-meta-histogram-interval="))
        );
        // The bare panel flag is NOT pushed; the log flag at parse time on
        // rcpd already implies the accumulator pipeline.
        assert!(
            !args.iter().any(|a| a == "--auto-meta-histogram"),
            "panel-only flag must not be forwarded; the log flag implies the pipeline",
        );
    }
}
