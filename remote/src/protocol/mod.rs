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
//!   |  <--- DirectoryCreated(root) ------- |
//!   |  <--- DirectoryCreated(child) ------ |
//!   |                                      |
//!   |  ~~~~ File(f, total=N) ~~~~~~~~~~~~> |  Write file, track count
//!   |  ~~~~ File(...) ~~~~~~~~~~~~~~~~~~-> |  ...
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
//! - **Source → Destination**: Must communicate failures (FileSkipped, SymlinkSkipped)
//!   so destination can track file counts correctly
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Metadata {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,
    pub mtime: i64,
    pub atime_nsec: i64,
    pub mtime_nsec: i64,
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
    fn from(metadata: &std::fs::Metadata) -> Self {
        Metadata {
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            atime: metadata.atime(),
            mtime: metadata.mtime(),
            atime_nsec: metadata.atime_nsec(),
            mtime_nsec: metadata.mtime_nsec(),
        }
    }
}

impl From<&common::safedir::FileMeta> for Metadata {
    /// Build a wire `Metadata` from an fd-pinned [`common::safedir::FileMeta`]
    /// snapshot (obtained via `fstat`/`fstatat` during a TOCTOU-safe walk),
    /// reading every field through the shared `preserve::Metadata` trait so it
    /// stays in lock-step with the `&std::fs::Metadata` conversion above.
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
        }
    }
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
    /// Confirm directory created, request file transfers. This is purely the
    /// Pass-2 trigger: it tells the source the destination created the directory
    /// and is ready to receive its files. The source already retains the
    /// authoritative Pass-1 file count for the directory (in its fd-map entry under
    /// hardened reads, or in a path→count map under `-L`), so no count is echoed
    /// back here.
    DirectoryCreated {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
    },
    /// Acknowledge a `Directory` message the destination did NOT create (create
    /// failed, ancestor failed, or `--ignore-existing` skipped a non-directory).
    /// No files will be requested for it. The destination sends exactly one of
    /// `DirectoryCreated` / `DirectorySkipped` per `Directory` message so the
    /// source can release the matching held directory fd (see the source-side
    /// fd-map / dir-fd budget in `rcp::source`): without this nack a skipped
    /// directory's Pass-1 permit would never be released, hanging large no-ack
    /// subtrees. `src` keys the source-side fd-map entry to release (the map is
    /// inserted under `src`; see `take_for_skipped`); `dst` is carried for
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
    pub max_open_files: Option<usize>,
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
    /// Network profile for buffer sizing
    pub network_profile: crate::NetworkProfile,
    /// Buffer size for file transfers (defaults to profile-specific value)
    pub buffer_size: Option<usize>,
    /// Maximum concurrent connections in the pool
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
        if let Some(v) = self.max_open_files {
            args.push(format!("--max-open-files={v}"));
        }
        if self.dereference {
            args.push("--dereference".to_string());
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

    fn minimal_rcpd_config() -> RcpdConfig {
        RcpdConfig {
            verbose: 0,
            fail_early: false,
            max_workers: 0,
            max_blocking_threads: 0,
            max_open_files: None,
            ops_throttle: 0,
            iops_throttle: 0,
            chunk_size: 0,
            auto_meta: None,
            auto_meta_histogram: false,
            auto_meta_histogram_log: None,
            auto_meta_histogram_interval: std::time::Duration::from_secs(1),
            dereference: false,
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
