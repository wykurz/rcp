use anyhow::{Context, anyhow};
use clap::Parser;
use tracing::instrument;

use rcp_tools_rcp::path;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rcp",
    version,
    about = "Copy files efficiently - similar to `cp` but generally MUCH faster when dealing with large filesets",
    long_about = "`rcp` is a tool for copying files similar to `cp` but generally MUCH faster when dealing with a large number of files.

Supports both local and remote copying using `host:/path` syntax (similar to `scp`).

Inspired by tools like `dsync`(1) and `pcp`(2).

EXAMPLES:
    # Basic local copy with progress
    rcp /source /dest --progress --summary

    # Copy with metadata preservation and overwrite
    rcp /source /dest --preserve-settings=all --overwrite --progress

    # Remote copy from one host to another
    rcp user@host1:/path/to/source user@host2:/path/to/dest --progress

    # Copy from remote to local
    rcp host:/remote/path /local/path --progress

    # Copy from local to remote
    rcp /local/path host:/remote/path --preserve-settings=all --progress

1) https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html
2) https://github.com/wtsi-ssg/pcp"
)]
struct Args {
    // Copy options (core behavior + metadata preservation)
    /// Overwrite existing files/directories
    #[arg(short, long, help_heading = "Copy options")]
    overwrite: bool,

    /// File attributes to compare when deciding if files are identical (used with --overwrite)
    ///
    /// Comma-separated list. Available: uid, gid, mode, size, mtime, ctime
    #[arg(
        long,
        default_value = "size,mtime",
        value_name = "OPTIONS",
        help_heading = "Copy options"
    )]
    overwrite_compare: String,

    /// Max pre-existing destination entries per directory eligible for skip-without-transfer
    /// (remote --overwrite/--ignore-existing). Above this, that directory transfers normally.
    #[arg(
        long,
        default_value_t = remote::protocol::DEFAULT_OVERWRITE_MANIFEST_MAX_ENTRIES,
        value_name = "N",
        help_heading = "Copy options"
    )]
    overwrite_manifest_max_entries: usize,

    /// Skip overwriting files that match a condition
    ///
    /// Available filters: "newer" (skip if destination mtime is strictly newer than source).
    /// Requires --overwrite.
    #[arg(long, value_name = "FILTER", help_heading = "Copy options")]
    overwrite_filter: Option<common::copy::OverwriteFilter>,

    /// Do not overwrite existing files
    #[arg(long, conflicts_with = "overwrite", help_heading = "Copy options")]
    ignore_existing: bool,

    /// Skip special files (sockets, FIFOs, devices) without error
    #[arg(long, help_heading = "Copy options")]
    skip_specials: bool,

    /// Delete extraneous files from destination directories (mirror the source)
    ///
    /// Removes entries under the destination that have no counterpart in the
    /// source. Implies --overwrite. Excluded files are protected from deletion
    /// unless --delete-excluded is given. Requires a single source.
    #[arg(
        long,
        conflicts_with_all = ["ignore_existing", "dereference"],
        help_heading = "Copy options"
    )]
    delete: bool,

    /// With --delete, also remove destination entries matching an exclude pattern
    #[arg(long, requires = "delete", help_heading = "Copy options")]
    delete_excluded: bool,

    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Copy options")]
    fail_early: bool,

    /// Always follow symbolic links in source
    #[arg(short = 'L', long, help_heading = "Copy options")]
    dereference: bool,

    /// [DEPRECATED: use --preserve-settings=all] Preserve file metadata: file owner, group, setuid, setgid, mtime, atime and mode.
    /// Does NOT preserve POSIX ACLs - use --preserve-settings=all+acl for those.
    #[arg(short, long, help_heading = "Copy options")]
    preserve: bool,

    /// Specify what attributes to preserve
    ///
    /// Presets: "all" preserves uid, gid, time, and full mode (0o7777);
    /// "none" uses minimal defaults (no uid/gid/time, mode mask 0o0777).
    /// Neither preserves POSIX ACLs: detecting an ACL costs an extra syscall on every
    /// entry, so it is opt-in via the "+acl" modifier, e.g. "all+acl".
    /// Custom format: "`<type1>:<attributes1> <type2>:<attributes2>` ..." where
    /// `<type>` is one of f (file), d (directory), l (symlink), and `<attributes>` is
    /// a comma-separated list of uid, gid, time, acl, or a 4-digit octal mode mask.
    /// "acl" is not valid for symlinks, and cannot be combined with a mode mask that
    /// narrows the rwx bits.
    /// If specified, the --preserve flag is ignored.
    ///
    /// Example: "f:uid,gid,time,0777 d:uid,gid,time,0777 l:uid,gid,time"
    #[arg(long, value_name = "SETTINGS", help_heading = "Copy options")]
    preserve_settings: Option<String>,

    // Filtering options
    /// Glob pattern for files to include (can be specified multiple times)
    ///
    /// Only files matching at least one include pattern will be copied.
    /// Patterns use glob syntax: * matches anything except /, ** matches anything including /,
    /// ? matches single char, [...] for character classes. Leading / anchors to source root,
    /// trailing / matches only directories. Simple patterns (like *.txt) apply to the source
    /// root itself; anchored patterns (like /src/**) match paths inside the source.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append, help_heading = "Filtering")]
    include: Vec<String>,

    /// Glob pattern for files to exclude (can be specified multiple times)
    ///
    /// Files matching any exclude pattern will be skipped. Excludes are checked before includes.
    /// Simple patterns (like *.log) can exclude the source root itself; anchored patterns
    /// (like /build/) only match paths inside the source.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append, help_heading = "Filtering")]
    exclude: Vec<String>,

    /// Read filter patterns from file
    ///
    /// Format: one pattern per line with "--include PATTERN" or "--exclude PATTERN".
    /// Lines starting with # are comments. Mutually exclusive with --include/--exclude.
    #[arg(long, value_name = "PATH", conflicts_with_all = ["include", "exclude"], help_heading = "Filtering")]
    filter_file: Option<std::path::PathBuf>,

    /// Preview mode - show what would be copied without actually copying
    ///
    /// Modes: brief (show only what would be copied), all (also show skipped files),
    /// explain (show skipped files with the pattern that caused the skip).
    ///
    /// Note: dry-run bypasses --overwrite checks and shows all files that would be
    /// attempted, regardless of whether the destination already exists.
    /// --progress and --summary are suppressed in dry-run mode (use -v to
    /// still see summary output).
    #[arg(long, value_name = "MODE", help_heading = "Filtering")]
    dry_run: Option<common::DryRunMode>,

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    /// Chunk size for calculating I/O operations per file
    ///
    /// Required when using --iops-throttle (must be > 0)
    #[arg(
        long,
        default_value = "0",
        value_name = "SIZE",
        help_heading = "Performance & throttling"
    )]
    chunk_size: bytesize::ByteSize,

    #[command(flatten)]
    common: common::cli::CommonArgs,

    // Remote copy options
    /// IP address to bind the master TCP server to
    ///
    /// By default, the best available network interface is automatically selected.
    /// Use this option to explicitly bind to a specific IP address (e.g., "192.168.1.5").
    /// This is useful for multi-homed hosts or when you want to control which network
    /// is used for TCP traffic. Only IPv4 addresses are supported.
    ///
    /// When the source path uses an IP address (e.g., "192.168.1.100:/path"), that IP
    /// is automatically passed to the source rcpd so it binds explicitly to that address.
    #[arg(long, value_name = "IP", help_heading = "Remote copy options")]
    bind_ip: Option<String>,

    /// Restrict TCP to specific port ranges (e.g., "8000-8999,10000-10999")
    ///
    /// Both `-` and `:` are accepted as range separators (e.g., "8000-8999" or "8000:8999").
    /// Defaults to dynamic port allocation if not specified.
    #[arg(long, value_name = "RANGES", help_heading = "Remote copy options")]
    port_ranges: Option<String>,

    /// Connection timeout for remote setup and copy operations in seconds
    ///
    /// Applies to: remote SSH setup, binary discovery, tilde-expansion HOME lookup, rcpd version
    /// probes, deployment readiness/write-idle periods, daemon readiness, rcpd→master connection,
    /// and destination→source connection. It does not cap total deployment transfer time;
    /// post-transfer verification gets at least 60 seconds. Must be at least 1.
    /// Default: 15s normally, 60s when --auto-deploy-rcpd is enabled (to account
    /// for sequential binary deployment to multiple hosts).
    #[arg(
        long,
        value_name = "N",
        value_parser = common::cli::parse_positive_u64,
        default_value = "15",
        default_value_if("auto_deploy_rcpd", "true", "60"),
        help_heading = "Remote copy options"
    )]
    remote_copy_conn_timeout_sec: u64,

    /// Liveness budget for every rcp TCP connection in seconds (0 disables)
    ///
    /// A peer whose host vanishes (power loss, severed link, destroyed VM) sends neither FIN
    /// nor RST, so a copy waiting on it would otherwise hang forever. Every connection is probed
    /// when idle (TCP keepalive); control connections additionally bound unacknowledged data
    /// (TCP_USER_TIMEOUT). The bulk data connections deliberately do not, because a throttled
    /// receiver legitimately stops reading mid-file — so a host that vanishes mid-transfer is
    /// still only detected by the kernel's retransmission limit (~15 min). Widen it on a flaky
    /// WAN; a stall shorter than the budget is survived untouched. Propagated to both rcpds.
    #[arg(
        long,
        value_name = "N",
        default_value_t = remote::DEFAULT_REMOTE_KEEPALIVE_SEC,
        help_heading = "Remote copy options"
    )]
    remote_keepalive_sec: u64,

    /// Network profile for TCP tuning
    ///
    /// 'datacenter' (default): Optimized for datacenter networks (<1ms RTT, 25-100 Gbps).
    /// Uses larger buffer sizes for high-bandwidth links.
    /// 'internet': Conservative settings for internet connections.
    /// Uses smaller buffer sizes suitable for shared networks.
    #[arg(
        long,
        default_value = "datacenter",
        value_name = "PROFILE",
        help_heading = "Remote copy options"
    )]
    network_profile: remote::NetworkProfile,

    /// Buffer size for remote copy file transfer operations.
    ///
    /// Controls the buffer used when copying data between files and network streams.
    /// Larger buffers can improve throughput but use more memory per concurrent transfer.
    /// Accepts byte sizes like "256KiB", "1MiB", or plain numbers in bytes.
    ///
    /// Default: 16 MiB for datacenter, 2 MiB for internet profile.
    #[arg(long, value_name = "SIZE", help_heading = "Remote copy options")]
    remote_copy_buffer_size: Option<bytesize::ByteSize>,

    /// Maximum concurrent data connections (default: 100)
    ///
    /// This separately configurable ceiling defaults to 100. Effective data streams are
    /// min(--max-files-in-flight, --max-connections). Higher values allow more parallel file
    /// transfers but use more resources. Remote automatic limits resolve on the source rcpd and
    /// are adopted by the destination. To raise remote parallelism above the CPU-derived file
    /// default, increase both ceilings.
    #[arg(
        long,
        value_name = "N",
        value_parser = common::cli::parse_positive_usize,
        help_heading = "Remote copy options"
    )]
    max_connections: Option<std::num::NonZeroUsize>,

    /// Multiplier for pending file writes (default: 4)
    ///
    /// Pending capacity is effective data streams × pending-writes-multiplier. Higher values allow
    /// more files to be queued but use more memory when the destination is slow.
    #[arg(
        long,
        default_value_t = std::num::NonZeroUsize::new(
            remote::DEFAULT_PENDING_WRITES_MULTIPLIER
        ).unwrap(),
        value_name = "N",
        value_parser = common::cli::parse_positive_usize,
        help_heading = "Remote copy options"
    )]
    pending_writes_multiplier: std::num::NonZeroUsize,

    /// Enable file-based debug logging for rcpd processes
    ///
    /// Example: /tmp/rcpd-log creates /tmp/rcpd-log-YYYY-MM-DDTHH-MM-SS-RANDOM
    #[arg(long, value_name = "PREFIX", help_heading = "Remote copy options")]
    rcpd_debug_log_prefix: Option<String>,

    /// Disable TLS encryption and authentication for remote copy operations
    ///
    /// By default, remote copy connections use mutual TLS for authentication and encryption.
    /// This flag disables BOTH encryption AND authentication on ALL rcp TCP connections
    /// (master<->rcpd control and tracing, plus source<->destination control and data).
    /// WARNING: all traffic is sent in plaintext and every rcpd listener accepts connections
    /// from anyone who can reach its port. Only use on isolated, trusted networks.
    #[arg(long, help_heading = "Remote copy options")]
    no_encryption: bool,

    // Profiling options
    /// Enable Chrome tracing output for profiling
    ///
    /// Produces JSON files viewable in Perfetto UI (ui.perfetto.dev) or chrome://tracing.
    /// Accepts a path prefix; full filename includes tool name, role, hostname, PID, and timestamp.
    /// For remote operations, tracing is automatically enabled on rcpd processes too.
    /// Example: --chrome-trace=/tmp/trace produces:
    ///   /tmp/trace-rcp-myhost-12345-2025-01-15T10:30:45.json
    ///   /tmp/trace-rcpd-source-host1-23456-2025-01-15T10:30:46.json (remote)
    ///   /tmp/trace-rcpd-destination-host2-34567-2025-01-15T10:30:46.json (remote)
    #[arg(long, value_name = "PREFIX", help_heading = "Profiling")]
    chrome_trace: Option<String>,

    /// Enable flamegraph output for profiling
    ///
    /// Produces folded stack files convertible to SVG with `inferno-flamegraph`.
    /// Accepts a path prefix; full filename includes tool name, role, hostname, PID, and timestamp.
    /// For remote operations, tracing is automatically enabled on rcpd processes too.
    /// Example: --flamegraph=/tmp/flame produces .folded files.
    /// Convert to SVG: cat *.folded | inferno-flamegraph > flamegraph.svg
    #[arg(long, value_name = "PREFIX", help_heading = "Profiling")]
    flamegraph: Option<String>,

    /// Log level for profiling (chrome-trace, flamegraph)
    ///
    /// Controls which spans are captured. Only spans from rcp crates are recorded.
    /// Values: trace, debug, info, warn, error (default: trace)
    #[arg(
        long,
        value_name = "LEVEL",
        default_value = "trace",
        help_heading = "Profiling"
    )]
    profile_level: String,

    /// Enable tokio-console for live async debugging
    ///
    /// Starts a tokio-console server for real-time async task inspection.
    /// Connect with: `tokio-console http://127.0.0.1:PORT`
    #[arg(long, help_heading = "Profiling")]
    tokio_console: bool,

    /// Port for tokio-console server (default: 6669)
    #[arg(long, value_name = "PORT", help_heading = "Profiling")]
    tokio_console_port: Option<u16>,

    /// Print protocol version information as JSON and exit
    ///
    /// Used to verify version compatibility with rcpd
    #[arg(long, help_heading = "Remote copy options")]
    protocol_version: bool,

    // TOCTOU safety
    /// Print TOCTOU-safety verdict for this invocation and exit (0 = safe, 1 = not safe)
    ///
    /// Analyzes whether the invocation is hardened against symlink/path-swap races
    /// and exits without performing the copy operation.
    #[arg(long, help_heading = "Security")]
    toctou_check: bool,

    /// Refuse to run unless the invocation uses the TOCTOU-hardened walk
    ///
    /// Refuses `--dereference`/`-L` (follows symlinks by design), non-Linux builds, kernels
    /// without openat2 (Linux 5.6+), and any operand that is not absolute and lexically
    /// normal (no `.`/`..`/`//`; realpath output qualifies). Operand root opens then resolve
    /// with openat2(RESOLVE_NO_SYMLINKS), so a symlink in any directory component fails closed
    /// (a symlink operand itself is never followed — it is handled as the link object).
    /// Path POLICY stays the caller's: lock paths down in the sudo rule. See "Scope of
    /// TOCTOU safety" in docs/tocttou.md. Intended for sudo rules:
    /// `NOPASSWD: /usr/bin/rcp --require-toctou-safe *`.
    #[arg(long, conflicts_with = "toctou_check", help_heading = "Security")]
    require_toctou_safe: bool,

    /// Path to rcpd binary on remote hosts
    ///
    /// If not specified, rcp will search for rcpd in standard locations
    #[arg(long, value_name = "PATH", help_heading = "Remote copy options")]
    rcpd_path: Option<String>,

    /// Automatically deploy rcpd binary to remote hosts if missing or version mismatch
    ///
    /// When enabled, rcp will transfer the local rcpd binary to remote hosts
    /// at ~/.cache/rcp/bin/rcpd-{version} if not found or if version doesn't match.
    /// The binary is transferred securely via SSH and verified with SHA-256 checksum.
    #[arg(long, help_heading = "Remote copy options")]
    auto_deploy_rcpd: bool,

    /// Force remote copy mode even for local-to-local paths
    ///
    /// Normally, when both source and destination are local paths (including paths
    /// with `localhost:` prefix), rcp performs a local copy. This flag forces the
    /// use of the remote copy protocol (rcpd) instead, which is useful for testing
    /// or when you want consistent behavior across local and remote operations.
    ///
    /// Requires paths to use the `localhost:` prefix (e.g., `localhost:/path/to/file`).
    #[arg(long, help_heading = "Remote copy options")]
    force_remote: bool,

    // ARGUMENTS
    /// Source path(s) and destination path
    #[arg()]
    paths: Vec<String>,
}

/// extract IP from host if it's an IPv4 address (for explicit binding)
fn extract_bind_ip_from_host(host: &str) -> Option<String> {
    // try parsing as IPv4
    if host.parse::<std::net::Ipv4Addr>().is_ok() {
        Some(host.to_string())
    } else {
        None
    }
}

/// The diagnostic for an `rcpd` whose connection FAILED before it said what happened.
///
/// A dying `rcpd` reaches the master two different ways, and which one is not something the master
/// controls: the kernel delivers an RST if the socket's receive queue was non-empty when the process
/// died, and a clean EOF if it was not. Those surface as `Err` and `Ok(None)` respectively — the same
/// event wearing two shapes — and both mean the same thing to the user, so both are reported the same
/// way: as what the master did not receive rather than how the connection ended. This is the `Err`
/// half, which keeps its transport error in the chain underneath. See [`rcpd_closed_quietly`] for the
/// other.
fn rcpd_went_quiet(role: &str, host: &str, expected: &str) -> String {
    format!(
        "{role} rcpd on '{host}' did not {expected} (the process likely died - check the remote \
         host for crashes or OOM kills)"
    )
}

/// The [`rcpd_went_quiet`] situation reached through a clean end-of-stream instead: the peer's
/// control connection closed with nothing left to read, so there is no transport error to report.
///
/// Distinct wording rather than a shared one, for two reasons. It points somewhere different — a
/// connection that closed cleanly but silently means the peer *exited* without sending its message
/// (a crash, an OOM kill, a `SIGKILL`), whereas a failed one can also mean the network went away. And
/// it makes the branch observable: this is the `Ok(None)` that the master used to `.expect()`, which
/// under `panic = "abort"` turned a diagnosable error into `SIGABRT` with its output discarded, so the
/// regression test for it has to be able to tell the two branches apart.
fn rcpd_closed_quietly(role: &str, host: &str, expected: &str) -> String {
    format!(
        "{role} rcpd on '{host}' closed its control connection cleanly but did not {expected} (the \
         process likely died - check the remote host for crashes or OOM kills)"
    )
}

#[derive(Debug)]
struct MasterRemoteConfigs {
    tcp: remote::TcpConfig,
    rcpd: remote::protocol::RcpdConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionCeilingOrigin {
    Default,
    Explicit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConnectionCeiling {
    value: std::num::NonZeroUsize,
    origin: ConnectionCeilingOrigin,
}

impl ConnectionCeiling {
    fn from_arg(value: Option<std::num::NonZeroUsize>) -> Self {
        match value {
            Some(value) => Self {
                value,
                origin: ConnectionCeilingOrigin::Explicit,
            },
            None => Self {
                value: std::num::NonZeroUsize::new(remote::DEFAULT_MAX_CONNECTIONS)
                    .expect("the default connection ceiling is nonzero"),
                origin: ConnectionCeilingOrigin::Default,
            },
        }
    }

    const fn value(self) -> std::num::NonZeroUsize {
        self.value
    }

    const fn is_explicit(self) -> bool {
        matches!(self.origin, ConnectionCeilingOrigin::Explicit)
    }

    fn description(self) -> String {
        match self.origin {
            ConnectionCeilingOrigin::Default => {
                format!("the default connection ceiling of {}", self.value)
            }
            ConnectionCeilingOrigin::Explicit => format!("--max-connections={}", self.value),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum MasterAuthoritativeFilesInFlight {
    Explicit,
    Deprecated,
}

impl MasterAuthoritativeFilesInFlight {
    fn source(self) -> common::FilesInFlightSource {
        match self {
            Self::Explicit => common::FilesInFlightSource::Explicit,
            Self::Deprecated => common::FilesInFlightSource::DeprecatedMaxOpenFiles,
        }
    }

    fn to_rcpd(self, limit: common::ConcurrencyLimit) -> remote::protocol::RcpdFilesInFlight {
        match self {
            Self::Explicit => match limit {
                common::ConcurrencyLimit::Limited(value) => {
                    remote::protocol::RcpdFilesInFlight::Explicit(value)
                }
                common::ConcurrencyLimit::Unlimited => {
                    unreachable!("an explicit master file limit is always finite")
                }
            },
            Self::Deprecated => remote::protocol::RcpdFilesInFlight::DeprecatedMaxOpenFiles(limit),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RemoteConcurrencyPolicy {
    Automatic,
    MasterAuthoritative {
        files_in_flight: MasterAuthoritativeFilesInFlight,
        concurrency: remote::ResolvedRemoteConcurrency,
    },
}

impl RemoteConcurrencyPolicy {
    fn notice_file_limit(
        self,
        source_files_in_flight: common::ConcurrencyLimit,
    ) -> (common::FilesInFlightSource, common::ConcurrencyLimit) {
        match self {
            Self::Automatic => (
                common::FilesInFlightSource::Automatic,
                source_files_in_flight,
            ),
            Self::MasterAuthoritative {
                files_in_flight,
                concurrency,
            } => (files_in_flight.source(), concurrency.files_in_flight()),
        }
    }
}

#[derive(Debug)]
struct MasterRemoteRequest {
    tcp: remote::TcpConfig,
    rcpd: remote::protocol::RcpdConfig,
    filter: Option<common::filter::FilterSettings>,
    connection_ceiling: ConnectionCeiling,
    pending_writes_multiplier: std::num::NonZeroUsize,
    concurrency_policy: RemoteConcurrencyPolicy,
}

fn remote_stream_clamp_notice(
    policy: RemoteConcurrencyPolicy,
    source_files_in_flight: common::ConcurrencyLimit,
    connection_ceiling: ConnectionCeiling,
    effective_connections: std::num::NonZeroUsize,
) -> Option<String> {
    let (source, file_limit) = policy.notice_file_limit(source_files_in_flight);
    let common::ConcurrencyLimit::Limited(file_limit) = file_limit else {
        return None;
    };
    if file_limit > effective_connections {
        let option = match source {
            common::FilesInFlightSource::DeprecatedMaxOpenFiles => "--max-open-files",
            common::FilesInFlightSource::Explicit => "--max-files-in-flight",
            common::FilesInFlightSource::Automatic => {
                if !connection_ceiling.is_explicit() {
                    return None;
                }
                return Some(format!(
                    "The source's automatic file ceiling of {file_limit} was reduced to {effective_connections} remote data streams by {}",
                    connection_ceiling.description()
                ));
            }
        };
        let connection_ceiling = connection_ceiling.description();
        return Some(format!(
            "Requested {option}={file_limit}, but {connection_ceiling} reduced remote data streams to {effective_connections}"
        ));
    }
    if !connection_ceiling.is_explicit() || connection_ceiling.value() <= effective_connections {
        return None;
    }
    let file_ceiling = match source {
        common::FilesInFlightSource::Automatic => {
            format!("the source's automatic file ceiling of {file_limit}")
        }
        common::FilesInFlightSource::Explicit => {
            format!("--max-files-in-flight={file_limit}")
        }
        common::FilesInFlightSource::DeprecatedMaxOpenFiles => {
            format!("--max-open-files={file_limit}")
        }
    };
    let connection_ceiling = connection_ceiling.description();
    Some(format!(
        "Requested {connection_ceiling}, but {file_ceiling} reduced remote data streams to {effective_connections}"
    ))
}

fn build_master_remote_request(
    args: &Args,
    files_in_flight: common::ResolvedFilesInFlight,
    master_cert_fingerprint: Option<remote::protocol::CertFingerprint>,
) -> anyhow::Result<MasterRemoteRequest> {
    let filter = common::filter::FilterSettings::from_args(
        args.filter_file.as_deref(),
        &args.include,
        &args.exclude,
    )
    .context("invalid filter configuration")?;
    // rcpd still receives the version-sensitive CLI spelling, but syntax owned entirely by the
    // initiating host must fail before remote HOME expansion, SSH, discovery, or deployment.
    common::parse_metadata_cmp_settings(&args.overwrite_compare)
        .context("invalid --overwrite-compare configuration")?;
    if let Some(ranges) = args.port_ranges.as_deref() {
        remote::port_ranges::PortRanges::parse(ranges)
            .context("invalid --port-ranges configuration")?;
    }
    let connection_ceiling = ConnectionCeiling::from_arg(args.max_connections);
    let configured_connections = connection_ceiling.value();
    let concurrency_policy = match files_in_flight.source() {
        common::FilesInFlightSource::Automatic => {
            // validate the configured connection upper bound before remote side effects. The source
            // still selects its CPU-based file ceiling, but any capacity it reports at or below this
            // bound is then guaranteed to be representable.
            remote::resolve_remote_concurrency(
                common::ConcurrencyLimit::Unlimited,
                configured_connections,
                args.pending_writes_multiplier,
            )?;
            RemoteConcurrencyPolicy::Automatic
        }
        common::FilesInFlightSource::Explicit => {
            let common::ConcurrencyLimit::Limited(limit) = files_in_flight.limit() else {
                anyhow::bail!("an explicit --max-files-in-flight limit must be finite")
            };
            RemoteConcurrencyPolicy::MasterAuthoritative {
                files_in_flight: MasterAuthoritativeFilesInFlight::Explicit,
                concurrency: remote::resolve_remote_concurrency(
                    common::ConcurrencyLimit::Limited(limit),
                    configured_connections,
                    args.pending_writes_multiplier,
                )?,
            }
        }
        common::FilesInFlightSource::DeprecatedMaxOpenFiles => {
            RemoteConcurrencyPolicy::MasterAuthoritative {
                files_in_flight: MasterAuthoritativeFilesInFlight::Deprecated,
                concurrency: remote::resolve_remote_concurrency(
                    files_in_flight.limit(),
                    configured_connections,
                    args.pending_writes_multiplier,
                )?,
            }
        }
    };
    let tcp = remote::TcpConfig {
        port_ranges: args.port_ranges.clone(),
        conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        network_profile: args.network_profile,
        buffer_size: args.remote_copy_buffer_size.map(|b| b.0 as usize),
        keepalive_sec: args.remote_keepalive_sec,
    };
    let rcpd = remote::protocol::RcpdConfig {
        verbose: args.common.verbose,
        fail_early: args.fail_early,
        max_workers: args.common.max_workers,
        max_blocking_threads: args.common.max_blocking_threads,
        files_in_flight: remote::protocol::RcpdFilesInFlight::Automatic,
        ops_throttle: args.common.ops_throttle,
        iops_throttle: args.common.iops_throttle,
        chunk_size: args.chunk_size.0 as usize,
        // Gate rcpd's auto_meta on explicit throttle or log path.
        //
        // `throttle_config()` always enables auto_meta when any histogram flag
        // is set (including the panel-only `--auto-meta-histogram`), which is
        // correct for the master's local copy path.  But for remote copies we
        // must not propagate the throttle pipeline to rcpd unless the user
        // explicitly asked for it:
        //
        //   - `--auto-meta-throttle`: user explicitly wants throttle → propagate.
        //   - `--auto-meta-histogram-log <PATH>`: user wants per-rcpd log files,
        //     which require the throttle pipeline to fire on rcpd → propagate.
        //   - `--auto-meta-histogram` alone: panel-only flag.  The master's
        //     panel is empty in remote mode (master runs no controllers); rcpd's
        //     panel never reaches the user; rcpd behavior should be unchanged.
        auto_meta: if args.common.auto_meta_throttle
            || args.common.auto_meta_histogram_log.is_some()
        {
            args.common
                .throttle_config(files_in_flight, args.chunk_size.0)
                .auto_meta
        } else {
            None
        },
        auto_meta_histogram: args.common.auto_meta_histogram,
        auto_meta_histogram_log: args
            .common
            .auto_meta_histogram_log
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        auto_meta_histogram_interval: args.common.auto_meta_histogram_interval.into(),
        dereference: args.dereference,
        require_toctou_safe: args.require_toctou_safe,
        overwrite: args.overwrite,
        overwrite_compare: args.overwrite_compare.clone(),
        overwrite_manifest_max_entries: args.overwrite_manifest_max_entries,
        overwrite_filter: args.overwrite_filter.map(|f| f.to_string()),
        ignore_existing: args.ignore_existing,
        skip_specials: args.skip_specials,
        debug_log_prefix: args.rcpd_debug_log_prefix.clone(),
        port_ranges: args.port_ranges.clone(),
        progress: args.common.progress,
        progress_delay: args.common.progress_delay.clone(),
        remote_copy_conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        remote_keepalive_sec: args.remote_keepalive_sec,
        network_profile: args.network_profile,
        buffer_size: args.remote_copy_buffer_size.map(|b| b.0 as usize),
        max_connections: configured_connections.get(),
        pending_writes_multiplier: args.pending_writes_multiplier.get(),
        chrome_trace_prefix: args.chrome_trace.clone(),
        flamegraph_prefix: args.flamegraph.clone(),
        profile_level: Some(args.profile_level.clone()),
        tokio_console: args.tokio_console,
        tokio_console_port: args.tokio_console_port,
        encryption: !args.no_encryption,
        master_cert_fingerprint,
    };
    Ok(MasterRemoteRequest {
        tcp,
        rcpd,
        filter,
        connection_ceiling,
        pending_writes_multiplier: args.pending_writes_multiplier,
        concurrency_policy,
    })
}

fn endpoint_config(
    request: &MasterRemoteRequest,
    files_in_flight: remote::protocol::RcpdFilesInFlight,
    concurrency: Option<remote::ResolvedRemoteConcurrency>,
) -> MasterRemoteConfigs {
    let tcp = request.tcp.clone();
    let mut rcpd = request.rcpd.clone();
    rcpd.files_in_flight = files_in_flight;
    if let Some(concurrency) = concurrency {
        rcpd.max_connections = concurrency.max_connections().get();
    }
    MasterRemoteConfigs { tcp, rcpd }
}

fn build_source_remote_config(request: &MasterRemoteRequest) -> MasterRemoteConfigs {
    match request.concurrency_policy {
        RemoteConcurrencyPolicy::MasterAuthoritative {
            files_in_flight,
            concurrency,
        } => endpoint_config(
            request,
            files_in_flight.to_rcpd(concurrency.files_in_flight()),
            Some(concurrency),
        ),
        RemoteConcurrencyPolicy::Automatic => endpoint_config(
            request,
            remote::protocol::RcpdFilesInFlight::Automatic,
            None,
        ),
    }
}

fn build_destination_remote_config(
    request: &MasterRemoteRequest,
    source: &remote::RcpdConnectionInfo,
) -> anyhow::Result<MasterRemoteConfigs> {
    let (concurrency, files_in_flight) = match request.concurrency_policy {
        RemoteConcurrencyPolicy::Automatic => {
            let concurrency = remote::resolve_remote_concurrency(
                source.files_in_flight,
                request.connection_ceiling.value(),
                request.pending_writes_multiplier,
            )?;
            if concurrency.max_connections() != source.max_connections {
                anyhow::bail!(
                    "source rcpd reported {} effective streams, expected {} from its source-owned file and connection ceilings",
                    source.max_connections,
                    concurrency.max_connections()
                );
            }
            let files_in_flight = match source.files_in_flight {
                common::ConcurrencyLimit::Limited(value) => {
                    remote::protocol::RcpdFilesInFlight::ResolvedAutomatic(value)
                }
                common::ConcurrencyLimit::Unlimited => {
                    anyhow::bail!("an automatic source rcpd reported an unlimited file ceiling")
                }
            };
            (concurrency, files_in_flight)
        }
        RemoteConcurrencyPolicy::MasterAuthoritative {
            files_in_flight,
            concurrency,
        } => {
            if source.files_in_flight != concurrency.files_in_flight() {
                anyhow::bail!(
                    "source rcpd reported file ceiling {}, expected {}",
                    source.files_in_flight,
                    concurrency.files_in_flight()
                );
            }
            if concurrency.max_connections() != source.max_connections {
                anyhow::bail!(
                    "source rcpd reported {} effective streams, expected the master-authoritative value {}",
                    source.max_connections,
                    concurrency.max_connections()
                );
            }
            (
                concurrency,
                files_in_flight.to_rcpd(concurrency.files_in_flight()),
            )
        }
    };
    Ok(endpoint_config(request, files_in_flight, Some(concurrency)))
}

fn validate_destination_readiness(
    source: &remote::RcpdConnectionInfo,
    destination: &remote::RcpdConnectionInfo,
) -> anyhow::Result<()> {
    if destination.files_in_flight != source.files_in_flight
        || destination.max_connections != source.max_connections
    {
        anyhow::bail!(
            "destination rcpd reported file/stream limits {}/{}, but source negotiated {}/{}",
            destination.files_in_flight,
            destination.max_connections,
            source.files_in_flight,
            source.max_connections,
        );
    }
    Ok(())
}

fn spawn_tracing_receiver(
    recv_stream: remote::streams::BoxedRecvStream,
    rcpd_type: remote::tracelog::RcpdType,
) -> remote::AbortOnDropTask<()> {
    remote::AbortOnDropTask::new(tokio::spawn(async move {
        if let Err(error) = remote::tracelog::run_receiver(recv_stream, rcpd_type).await {
            tracing::debug!("{rcpd_type} tracing receiver ended: {error:#}");
        }
    }))
}

const TRACING_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

async fn finish_tracing_receiver(
    task: remote::AbortOnDropTask<()>,
    rcpd_type: remote::tracelog::RcpdType,
    deadline: tokio::time::Instant,
) {
    match tokio::time::timeout_at(deadline, task.join()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::debug!("{rcpd_type} tracing receiver task failed: {error:#}");
        }
        Err(_) => {
            tracing::debug!("{rcpd_type} tracing receiver did not finish during drain grace");
        }
    }
}

async fn finish_tracing_receivers(
    receivers: Vec<(remote::AbortOnDropTask<()>, remote::tracelog::RcpdType)>,
) {
    let deadline = tokio::time::Instant::now() + TRACING_DRAIN_GRACE;
    futures::future::join_all(
        receivers
            .into_iter()
            .map(|(task, rcpd_type)| finish_tracing_receiver(task, rcpd_type, deadline)),
    )
    .await;
}

async fn wait_for_rcpd_processes(rcpd_processes: Vec<remote::RcpdProcess>, report_failures: bool) {
    let results = futures::future::join_all(
        rcpd_processes
            .into_iter()
            .map(remote::wait_for_rcpd_process),
    )
    .await;
    for result in results {
        if let Err(error) = result {
            if report_failures {
                tracing::error!("Failed to wait for rcpd process: {error:#}");
            } else {
                tracing::debug!("rcpd process failed while unwinding startup: {error:#}");
            }
        }
    }
    tracing::info!("All rcpd processes finished");
}

async fn finish_remote_teardown<F>(
    process_wait: F,
    tracing_receivers: Vec<(remote::AbortOnDropTask<()>, remote::tracelog::RcpdType)>,
) where
    F: std::future::Future<Output = ()>,
{
    // rcpd can spend several seconds flushing its tracing sender before exit. Keep receivers live
    // while every daemon finishes, then give all receiver tasks one shared final drain deadline.
    process_wait.await;
    finish_tracing_receivers(tracing_receivers).await;
}

#[derive(Default)]
struct RemoteTeardown {
    processes: Vec<remote::RcpdProcess>,
    tracing_receivers: Vec<(remote::AbortOnDropTask<()>, remote::tracelog::RcpdType)>,
}

impl RemoteTeardown {
    fn retain_process(&mut self, process: remote::RcpdProcess) {
        self.processes.push(process);
    }

    fn retain_tracing_receiver(
        &mut self,
        task: remote::AbortOnDropTask<()>,
        rcpd_type: remote::tracelog::RcpdType,
    ) {
        self.tracing_receivers.push((task, rcpd_type));
    }

    async fn finish(mut self, report_process_failures: bool) {
        finish_remote_teardown(
            wait_for_rcpd_processes(std::mem::take(&mut self.processes), report_process_failures),
            std::mem::take(&mut self.tracing_receivers),
        )
        .await;
    }
}

impl Drop for RemoteTeardown {
    fn drop(&mut self) {
        // graceful exits consume both collections in finish(). Cancellation or panic still aborts
        // receiver tasks; dropping RcpdProcess closes daemon channels and aborts its collectors.
        for (task, _) in self.tracing_receivers.drain(..) {
            drop(task);
        }
    }
}

#[derive(Debug)]
struct RemoteCopyOperands {
    source: path::RemotePath,
    destination: path::RemotePath,
    local_operand_roots: Vec<std::path::PathBuf>,
}

fn collect_local_operand_roots(
    sources: &[path::PathType],
    destination: &path::PathType,
) -> anyhow::Result<Vec<std::path::PathBuf>> {
    sources
        .iter()
        .chain(std::iter::once(destination))
        .filter_map(|operand| match operand {
            path::PathType::Local(path) => Some(path),
            path::PathType::Remote(_) => None,
        })
        .map(|path| path::RemotePath::from_local(path).map(|path| path.path().to_path_buf()))
        .collect()
}

fn remote_copy_operands(
    source: path::PathType,
    destination: path::PathType,
    local_operand_roots: Vec<std::path::PathBuf>,
) -> anyhow::Result<Option<RemoteCopyOperands>> {
    let operands = match (source, destination) {
        (path::PathType::Remote(source), path::PathType::Remote(destination)) => {
            RemoteCopyOperands {
                source,
                destination,
                local_operand_roots,
            }
        }
        (path::PathType::Remote(source), path::PathType::Local(destination)) => {
            let destination = path::RemotePath::from_local(&destination)?;
            RemoteCopyOperands {
                source,
                destination,
                local_operand_roots,
            }
        }
        (path::PathType::Local(source), path::PathType::Remote(destination)) => {
            let source = path::RemotePath::from_local(&source)?;
            RemoteCopyOperands {
                source,
                destination,
                local_operand_roots,
            }
        }
        (path::PathType::Local(_), path::PathType::Local(_)) => return Ok(None),
    };
    Ok(Some(operands))
}

#[instrument(skip(master_cert))]
async fn run_rcpd_master(
    args: &Args,
    preserve: &common::preserve::Settings,
    operands: &RemoteCopyOperands,
    request: MasterRemoteRequest,
    master_cert: Option<remote::tls::CertifiedKey>,
    cleanup: &remote::RemoteCleanup,
) -> anyhow::Result<common::copy::Summary> {
    tracing::debug!("running rcpd src/dst");
    let src = &operands.source;
    let dst = &operands.destination;
    let mut teardown = RemoteTeardown::default();
    let source_config = build_source_remote_config(&request);
    let source_bind_ip = extract_bind_ip_from_host(&src.session().host);
    let same_session = src.session() == dst.session();
    let bootstrap_timeout = std::time::Duration::from_secs(request.tcp.conn_timeout_sec);
    let (prepared_source, prepared_destination) = remote::prepare_rcpd_endpoints_with_timeout(
        src.session(),
        dst.session(),
        args.rcpd_path.as_deref(),
        args.auto_deploy_rcpd,
        cleanup,
        bootstrap_timeout,
        &operands.local_operand_roots,
    )
    .await?;

    let source_rcpd = {
        let _span = tracing::trace_span!(
            "start_rcpd",
            host = %src.session().host,
            role = ?remote::protocol::RcpdRole::Source
        )
        .entered();
        prepared_source
            .spawn(
                &source_config.rcpd,
                source_bind_ip.as_deref(),
                remote::protocol::RcpdRole::Source,
            )
            .await?
    };
    let source_conn_info = source_rcpd.conn_info.clone();
    teardown.retain_process(source_rcpd);
    tracing::info!(
        "Source rcpd at {} (encryption={})",
        source_conn_info.addr,
        source_conn_info.fingerprint.is_some()
    );
    // helper to connect to an rcpd and wrap with TLS if needed
    async fn connect_to_rcpd(
        conn_info: &remote::RcpdConnectionInfo,
        master_cert: Option<&remote::tls::CertifiedKey>,
        tcp_config: &remote::TcpConfig,
        purpose: &str,
    ) -> anyhow::Result<(
        remote::streams::BoxedSendStream,
        remote::streams::BoxedRecvStream,
    )> {
        let timeout = std::time::Duration::from_secs(tcp_config.conn_timeout_sec);
        // this is the connection the master then awaits `RcpdResult` on, with no timeout of its
        // own — the keepalive the connect helper configures is what stops a vanished rcpd host
        // from hanging the master forever
        let stream = remote::connect_tcp_control(conn_info.addr, tcp_config)
            .await
            .with_context(|| format!("connecting to rcpd for {purpose}"))?;
        tracing::debug!("Connected to rcpd at {} for {}", conn_info.addr, purpose);
        match (conn_info.fingerprint, master_cert) {
            (Some(rcpd_fingerprint), Some(cert)) => {
                // mutual TLS: master presents cert, verifies rcpd fingerprint
                let tls_config =
                    remote::tls::create_client_config_with_cert(cert, rcpd_fingerprint)
                        .context("failed to create TLS client config with certificate")?;
                let connector = tokio_rustls::TlsConnector::from(tls_config);
                remote::tls::connect_bounded(
                    Some(&connector),
                    remote::tls::SERVER_NAME_RCPD,
                    stream,
                    timeout,
                    purpose,
                )
                .await
            }
            (Some(_), None) | (None, Some(_)) => {
                anyhow::bail!(
                    "TLS configuration mismatch: rcpd and master must both use encryption or both disable it"
                );
            }
            (None, None) => {
                // plain TCP (no encryption)
                remote::tls::connect_bounded(
                    None,
                    remote::tls::SERVER_NAME_RCPD,
                    stream,
                    timeout,
                    purpose,
                )
                .await
            }
        }
    }
    // connect to source rcpd (control + tracing)
    tracing::info!("Connecting to source rcpd...");
    let source_setup = async {
        let (source_send_stream, source_recv_stream) = connect_to_rcpd(
            &source_conn_info,
            master_cert.as_ref(),
            &source_config.tcp,
            "source control",
        )
        .await?;
        let (source_tracing_send, source_tracing_recv) = connect_to_rcpd(
            &source_conn_info,
            master_cert.as_ref(),
            &source_config.tcp,
            "source tracing",
        )
        .await?;
        drop(source_tracing_send); // we only receive on tracing connection
        anyhow::Ok((source_send_stream, source_recv_stream, source_tracing_recv))
    }
    .await;
    let (mut source_send_stream, mut source_recv_stream, source_tracing_recv) = match source_setup {
        Ok(setup) => setup,
        Err(error) => {
            teardown.finish(false).await;
            return Err(error);
        }
    };
    // start draining immediately. Source startup notices are already queued by this point and must
    // remain visible even if any part of destination bring-up fails.
    teardown.retain_tracing_receiver(
        spawn_tracing_receiver(source_tracing_recv, remote::tracelog::RcpdType::Source),
        remote::tracelog::RcpdType::Source,
    );

    let destination_setup = async {
        let destination_config = build_destination_remote_config(&request, &source_conn_info)?;
        if let Some(notice) = remote_stream_clamp_notice(
            request.concurrency_policy,
            source_conn_info.files_in_flight,
            request.connection_ceiling,
            source_conn_info.max_connections,
        ) {
            tracing::warn!(target: common::NOTICE_TARGET, "{notice}");
        }
        tracing::info!(
            "Effective remote connection count: {}",
            source_conn_info.max_connections
        );
        let destination_bind_ip = same_session.then_some(source_bind_ip.as_deref()).flatten();
        let destination_rcpd = {
            let _span = tracing::trace_span!(
                "start_rcpd",
                host = %dst.session().host,
                role = ?remote::protocol::RcpdRole::Destination
            )
            .entered();
            prepared_destination
                .spawn(
                    &destination_config.rcpd,
                    destination_bind_ip,
                    remote::protocol::RcpdRole::Destination,
                )
                .await?
        };
        let destination_conn_info = destination_rcpd.conn_info.clone();
        // retain process ownership before validation or connection work introduces another exit.
        teardown.retain_process(destination_rcpd);
        validate_destination_readiness(&source_conn_info, &destination_conn_info)?;
        tracing::info!(
            "Destination rcpd at {} (encryption={})",
            destination_conn_info.addr,
            destination_conn_info.fingerprint.is_some()
        );

        // connect to destination rcpd (control + tracing)
        tracing::info!("Connecting to destination rcpd...");
        let (dest_send_stream, dest_recv_stream) = connect_to_rcpd(
            &destination_conn_info,
            master_cert.as_ref(),
            &destination_config.tcp,
            "dest control",
        )
        .await?;
        let (dest_tracing_send, dest_tracing_recv) = connect_to_rcpd(
            &destination_conn_info,
            master_cert.as_ref(),
            &destination_config.tcp,
            "dest tracing",
        )
        .await?;
        drop(dest_tracing_send); // we only receive on tracing connection
        anyhow::Ok((
            destination_conn_info,
            dest_send_stream,
            dest_recv_stream,
            dest_tracing_recv,
        ))
    }
    .await;
    let (destination_conn_info, mut dest_send_stream, mut dest_recv_stream, dest_tracing_recv) =
        match destination_setup {
            Ok(setup) => setup,
            Err(error) => {
                let _ = source_send_stream.close().await;
                drop(source_recv_stream);
                teardown.finish(false).await;
                return Err(error);
            }
        };
    tracing::info!("Connected to all rcpd processes");
    teardown.retain_tracing_receiver(
        spawn_tracing_receiver(dest_tracing_recv, remote::tracelog::RcpdType::Destination),
        remote::tracelog::RcpdType::Destination,
    );
    let operation_result = async {
        let filter = request.filter;
        // send MasterHello to source rcpd (include dest fingerprint for mutual TLS)
        {
            let _span = tracing::trace_span!("send_master_hello_to_source").entered();
            source_send_stream
                .send_control_message(&remote::protocol::MasterHello::Source {
                    src: src.path().to_path_buf(),
                    dst: dst.path().to_path_buf(),
                    dest_cert_fingerprint: destination_conn_info.fingerprint,
                    filter,
                    dry_run: args.dry_run,
                    // derived from the SAME `preserve` the destination is handed below, so what
                    // the source reads and what the destination applies cannot drift apart.
                    capture: remote::protocol::ExtendedMetadataCapture::for_preserve(preserve),
                })
                .await?;
        }
        tracing::debug!("Waiting for source rcpd to send hello");
        let source_hello = {
            let _span = tracing::trace_span!("recv_source_hello").entered();
            // built once and used by BOTH arms, so the two cannot drift into describing the same
            // event differently — which is the whole point of reporting them alike.
            let went_quiet = rcpd_went_quiet("source", &src.session().host, "send its hello");
            let closed_quietly =
                rcpd_closed_quietly("source", &src.session().host, "send its hello");
            source_recv_stream
                .recv_object::<remote::protocol::SourceMasterHello>()
                .await
                .with_context(|| went_quiet)?
                .ok_or_else(|| anyhow!(closed_quietly))?
        };
        // send MasterHello to destination rcpd (include source fingerprint for mutual TLS)
        {
            let _span = tracing::trace_span!("send_master_hello_to_dest").entered();
            dest_send_stream
                .send_control_message(&remote::protocol::MasterHello::Destination {
                    source_control_addr: source_hello.control_addr,
                    source_data_addr: source_hello.data_addr,
                    server_name: source_hello.server_name.clone(),
                    preserve: *preserve,
                    source_cert_fingerprint: source_conn_info.fingerprint,
                })
                .await?;
        }
        tracing::info!("Forwarded source connection info to destination");
        let source_result = {
            let _span = tracing::trace_span!("wait_for_source_result").entered();
            let went_quiet = rcpd_went_quiet("source", &src.session().host, "report a result");
            let closed_quietly =
                rcpd_closed_quietly("source", &src.session().host, "report a result");
            source_recv_stream
                .recv_object::<remote::protocol::RcpdResult>()
                .await
                .with_context(|| went_quiet)?
                .ok_or_else(|| anyhow!(closed_quietly))?
        };
        let dest_result = {
            let _span = tracing::trace_span!("wait_for_dest_result").entered();
            let went_quiet = rcpd_went_quiet("destination", &dst.session().host, "report a result");
            let closed_quietly =
                rcpd_closed_quietly("destination", &dst.session().host, "report a result");
            dest_recv_stream
                .recv_object::<remote::protocol::RcpdResult>()
                .await
                .with_context(|| went_quiet)?
                .ok_or_else(|| anyhow!(closed_quietly))?
        };
        anyhow::Ok((source_result, dest_result))
    }
    .await;

    let _ = source_send_stream.close().await;
    let _ = dest_send_stream.close().await;
    drop(source_recv_stream);
    drop(dest_recv_stream);
    teardown.finish(true).await;

    let (source_result, dest_result) = operation_result?;
    tracing::debug!("Received RcpdResult from both source and destination rcpds");
    // check for failures and collect error details + runtime stats
    let mut errors = Vec::new();
    let (source_summary, source_runtime_stats) = match source_result {
        remote::protocol::RcpdResult::Success {
            message,
            summary,
            runtime_stats,
        } => {
            tracing::info!("Source rcpd completed successfully: {message}");
            (summary, runtime_stats)
        }
        remote::protocol::RcpdResult::Failure {
            error,
            summary,
            runtime_stats,
        } => {
            // rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
            tracing::error!("Source rcpd failed: {error}");
            errors.push(format!("Source: {error}"));
            (summary, runtime_stats)
        }
    };
    let (dest_summary, dest_runtime_stats) = match dest_result {
        remote::protocol::RcpdResult::Success {
            message,
            summary,
            runtime_stats,
        } => {
            tracing::info!("Destination rcpd completed successfully: {message}");
            (summary, runtime_stats)
        }
        remote::protocol::RcpdResult::Failure {
            error,
            summary,
            runtime_stats,
        } => {
            // rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
            tracing::error!("Destination rcpd failed: {error}");
            errors.push(format!("Destination: {error}"));
            (summary, runtime_stats)
        }
    };
    // store remote runtime stats for display at the end
    common::set_remote_runtime_stats(common::RemoteRuntimeStats {
        source_host: src.session().host.clone(),
        source_stats: source_runtime_stats,
        dest_host: dst.session().host.clone(),
        dest_stats: dest_runtime_stats,
    });
    // merge source and destination summaries:
    // - in dry-run mode the source does all the counting (destination is idle)
    // - in normal mode the destination is authoritative for copy/create/unchanged/remove
    //   counts and the source is authoritative for skip counts
    let is_dry_run = args.dry_run.is_some();
    let merge_summaries = |source: common::copy::Summary, dest: common::copy::Summary| {
        // for copy/create/unchanged/remove: destination in normal mode, source in dry-run
        let primary = if is_dry_run { &source } else { &dest };
        common::copy::Summary {
            bytes_copied: primary.bytes_copied,
            files_copied: primary.files_copied,
            symlinks_created: primary.symlinks_created,
            directories_created: primary.directories_created,
            files_unchanged: primary.files_unchanged,
            symlinks_unchanged: primary.symlinks_unchanged,
            directories_unchanged: primary.directories_unchanged,
            // skip counts are always source-only
            files_skipped: source.files_skipped,
            symlinks_skipped: source.symlinks_skipped,
            directories_skipped: source.directories_skipped,
            specials_skipped: source.specials_skipped,
            rm_summary: common::rm::Summary {
                bytes_removed: primary.rm_summary.bytes_removed,
                files_removed: primary.rm_summary.files_removed,
                symlinks_removed: primary.rm_summary.symlinks_removed,
                directories_removed: primary.rm_summary.directories_removed,
                files_skipped: primary.rm_summary.files_skipped,
                symlinks_skipped: primary.rm_summary.symlinks_skipped,
                directories_skipped: primary.rm_summary.directories_skipped,
            },
        }
    };
    // propagate any errors from rcpd processes
    if !errors.is_empty() {
        let combined_error = errors.join("; ");
        // rcp-error-log-allow: already-rendered messages joined into a String, not a chain
        tracing::error!("rcpd operation(s) failed: {combined_error}");
        return Err(common::copy::Error::new(
            anyhow::anyhow!("rcpd operation(s) failed: {combined_error}"),
            merge_summaries(source_summary, dest_summary),
        )
        .into());
    }
    Ok(merge_summaries(source_summary, dest_summary))
}

#[instrument]
async fn async_main(
    args: Args,
    files_in_flight: common::ResolvedFilesInFlight,
    cleanup: Option<remote::RemoteCleanup>,
) -> anyhow::Result<common::copy::Summary> {
    if args.paths.len() < 2 {
        return Err(anyhow!(
            "You must specify at least one source path and one destination path!"
        ));
    }
    let src_strings = &args.paths[0..args.paths.len() - 1];
    // `.`/`..` (and `dir/.`, `dir/..`) source operands are supported: `common::copy` decomposes them
    // via `split_root_operand` (canonicalizing `.`/`..`), so `rcp . dst` copies the current
    // directory just like `cp -r . dst` and like `rrm .`/`rchm .` already work. (`/` is rejected
    // there.) Use a shell glob (`dir/*`) if you want to expand a directory's contents instead.
    if args.delete && src_strings.len() > 1 {
        return Err(anyhow!(
            "--delete requires a single source; mirroring multiple sources into one destination is not supported"
        ));
    }
    if args.overwrite_filter.is_some() && !(args.overwrite || args.delete) {
        return Err(anyhow!(
            "--overwrite-filter requires --overwrite (or --delete, which implies it)"
        ));
    }
    // choose parser based on --force-remote flag
    let parse_fn = if args.force_remote {
        path::parse_path_force_remote
    } else {
        path::parse_path
    };
    let parsed_srcs: Vec<path::PathType> = src_strings
        .iter()
        .map(|src| parse_fn(src))
        .collect::<anyhow::Result<Vec<_>>>()?;
    // pick the path type of the first source in the list and ensure all other sources match
    let first_src_path_type = parsed_srcs[0].clone();
    for path_type in &parsed_srcs[1..] {
        if *path_type != first_src_path_type {
            return Err(anyhow!(
                "Cannot mix different path types in the source list: {:?} and {:?}",
                first_src_path_type,
                path_type
            ));
        }
    }
    let dst_string = args.paths.last().unwrap();
    // validate destination path for problematic patterns (applies to both local and remote)
    path::validate_destination_path(dst_string)?;
    let dst_parsed = parse_fn(dst_string)?;
    // check if we have remote paths
    let has_remote_paths = match first_src_path_type {
        path::PathType::Remote(_) => true,
        path::PathType::Local(_) => matches!(dst_parsed, path::PathType::Remote(_)),
    };
    // for remote paths, we only support single source
    if has_remote_paths && src_strings.len() > 1 {
        return Err(anyhow!(
            "Multiple sources are currently not supported when using remote paths!"
        ));
    }
    if has_remote_paths && args.delete {
        return Err(anyhow!("--delete is not yet supported for remote copies"));
    }
    // if any of the src/dst paths are remote, we'll be using the rcpd
    let remote_src_dst = if has_remote_paths {
        let local_operand_roots = collect_local_operand_roots(&parsed_srcs, &dst_parsed)?;
        // resolve the destination with trailing-slash logic on the already-parsed paths
        let resolved_dst = path::resolve_destination(&parsed_srcs[0], &dst_parsed, dst_string)?;
        remote_copy_operands(
            first_src_path_type.clone(),
            resolved_dst,
            local_operand_roots,
        )?
    } else {
        None
    };
    if args.preserve_settings.is_some() && args.preserve {
        tracing::warn!("The --preserve flag is ignored when --preserve-settings is specified!");
    }
    let preserve = if let Some(preserve_settings) = &args.preserve_settings {
        common::parse_preserve_settings(preserve_settings)
            .map_err(|err| common::copy::Error::new(err, Default::default()))?
    } else if args.preserve {
        eprintln!("WARNING: --preserve is deprecated, use --preserve-settings=all instead");
        common::preserve::preserve_all()
    } else {
        common::preserve::preserve_none()
    };
    tracing::debug!("preserve settings: {:?}", &preserve);
    if let Some(mut operands) = remote_src_dst {
        let cleanup = cleanup
            .as_ref()
            .expect("remote operations start with a cleanup supervisor");
        // install rustls crypto provider (ring) before any TLS operations
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let master_cert = if !args.no_encryption {
            Some(
                remote::tls::generate_self_signed_cert()
                    .context("failed to generate master TLS certificate")?,
            )
        } else {
            None
        };
        // explicit limits can be validated entirely by the master, so reject invalid capacities
        // before a `~` operand can trigger a remote HOME lookup. Automatic selection remains
        // source-owned and is resolved only after source readiness.
        let request = build_master_remote_request(
            &args,
            files_in_flight,
            master_cert.as_ref().map(|cert| cert.fingerprint),
        )?;
        let remote_home_timeout = std::time::Duration::from_secs(request.tcp.conn_timeout_sec);
        {
            let remote_src = &mut operands.source;
            let remote_dst = &mut operands.destination;
            // expand remote '~' using remote HOME if needed
            let same_session = remote_src.session() == remote_dst.session();
            if same_session && (remote_src.needs_remote_home() || remote_dst.needs_remote_home()) {
                let home = remote::get_remote_home_for_session_with_timeout(
                    remote_src.session(),
                    cleanup,
                    remote_home_timeout,
                    &operands.local_operand_roots,
                )
                .await?;
                remote_src.apply_remote_home(&home);
                remote_dst.apply_remote_home(&home);
            } else {
                if remote_src.needs_remote_home() {
                    let home = remote::get_remote_home_for_session_with_timeout(
                        remote_src.session(),
                        cleanup,
                        remote_home_timeout,
                        &operands.local_operand_roots,
                    )
                    .await?;
                    remote_src.apply_remote_home(&home);
                }
                if remote_dst.needs_remote_home() {
                    let home = remote::get_remote_home_for_session_with_timeout(
                        remote_dst.session(),
                        cleanup,
                        remote_home_timeout,
                        &operands.local_operand_roots,
                    )
                    .await?;
                    remote_dst.apply_remote_home(&home);
                }
            }
        }
        if !operands.source.path().is_absolute() || !operands.destination.path().is_absolute() {
            return Err(anyhow!(
                "Remote paths must be absolute after expansion: src={:?}, dst={:?}",
                operands.source.path(),
                operands.destination.path()
            ));
        }
        return match run_rcpd_master(&args, &preserve, &operands, request, master_cert, cleanup)
            .await
        {
            Ok(summary) => Ok(summary),
            Err(error) => {
                if let Some(copy_error) = error.downcast_ref::<common::copy::Error>()
                    && args.summary
                {
                    return Err(anyhow!("{}\n\n{}", copy_error, &copy_error.summary));
                }
                Err(error)
            }
        };
    }
    // warn if paths had localhost: prefix but we're doing a local copy
    // (only check when not using --force-remote, since that's the opt-in for remote behavior)
    if !args.force_remote {
        let any_localhost_prefix = src_strings.iter().any(|s| path::has_localhost_prefix(s))
            || path::has_localhost_prefix(dst_string);
        if any_localhost_prefix {
            tracing::warn!(
                "Paths with 'localhost:' prefix are treated as local. \
                Use --force-remote to force remote copy via SSH."
            );
        }
    }
    // handle multiple sources only when destination ends with '/'
    if src_strings.len() > 1 && !dst_string.ends_with('/') {
        return Err(anyhow!(
            "Multiple sources can only be copied INTO a directory; if this is your intent - follow the \
            destination path with a trailing slash"
        ));
    }
    let mut src_dst: Vec<(std::path::PathBuf, std::path::PathBuf)> =
        Vec::with_capacity(parsed_srcs.len());
    for parsed_src in &parsed_srcs {
        // the resolver preserves the destination's variant and this branch only runs when
        // the destination parsed as local, so a remote result is an internal error
        let dst_path = match path::resolve_destination(parsed_src, &dst_parsed, dst_string)? {
            path::PathType::Local(p) => p,
            path::PathType::Remote(_) => {
                return Err(anyhow!(
                    "Internal error: unexpected remote path in local copy branch"
                ));
            }
        };
        let src_path = match parsed_src {
            path::PathType::Local(p) => p.clone(),
            path::PathType::Remote(_) => {
                return Err(anyhow!(
                    "Internal error: unexpected remote path in local copy branch"
                ));
            }
        };
        // check for existing destination only when not using trailing slash (single source case)
        if src_strings.len() == 1 && !dst_string.ends_with('/') && !(args.overwrite || args.delete)
        {
            // under strict operand resolution, probe the destination fd-relative (parent opened
            // openat2 RESOLVE_NO_SYMLINKS) so a symlinked destination prefix fails closed instead
            // of being followed by a path-based `exists()`. The engine also validates the prefix
            // up front, so this is only the friendly pre-flight message; keeping it fd-relative
            // avoids a pre-engine symlink-following existence probe.
            let dst_exists = if common::safedir::strict_operand_resolution() {
                common::safedir::strict_probe_dst_kind(&dst_path, common::Side::Destination)
                    .await
                    .with_context(|| format!("cannot probe destination {dst_path:?}"))?
                    .is_some()
            } else {
                dst_path.exists()
            };
            if dst_exists {
                return Err(anyhow!(
                    "Destination path {dst_path:?} already exists! \n\
                    If you want to copy INTO it, then follow the destination path with a trailing slash (/). Use \
                    --overwrite if you want to overwrite it"
                ));
            }
        }
        src_dst.push((src_path, dst_path));
    }
    // under --require-toctou-safe, refuse byte-equal duplicate resolved destinations up front — a
    // clear error for the obvious `rcp /a/foo /b/foo /dst/` mistake. This is only the fast, EXACT
    // check: two sources can ALSO alias to the same destination directory via the filesystem (a
    // case-insensitive/casefold or Unicode-normalizing destination, or a bind mount), which a lexical
    // comparison cannot see. The dispatch below therefore ALSO serializes strict multi-source copies,
    // removing the concurrent reused-directory-lockdown race for every such alias without having to
    // detect it. (Outside strict mode, concurrent merge-into-the-same-subtree is a pre-existing race
    // and is left as-is.)
    if common::safedir::strict_operand_resolution() {
        let mut seen = std::collections::HashSet::new();
        for (_, dst) in &src_dst {
            if !seen.insert(dst.as_path()) {
                return Err(anyhow!(
                    "multiple sources resolve to the same destination {:?} under \
                     --require-toctou-safe; run them as separate copies (concurrent copies sharing a \
                     destination directory would race the reused-directory lockdown)",
                    dst
                ));
            }
        }
    }
    // build filter settings from CLI arguments
    let filter = common::filter::FilterSettings::from_args(
        args.filter_file.as_deref(),
        &args.include,
        &args.exclude,
    )
    .map_err(|err| common::copy::Error::new(err, Default::default()))?;
    let delete = if args.delete {
        Some(common::copy::DeleteSettings {
            delete_excluded: args.delete_excluded,
        })
    } else {
        None
    };
    let settings = common::copy::Settings {
        dereference: args.dereference,
        fail_early: args.fail_early,
        overwrite: args.overwrite || args.delete,
        overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)
            .map_err(|err| common::copy::Error::new(err, Default::default()))?,
        overwrite_filter: args.overwrite_filter,
        ignore_existing: args.ignore_existing,
        chunk_size: args.chunk_size.0,
        skip_specials: args.skip_specials,
        // for local copy, buffer size is not used (bypasses user-mode buffering)
        remote_copy_buffer_size: 0,
        filter,
        dry_run: args.dry_run,
        delete,
    };
    tracing::debug!("copy settings: {:?}", &settings);
    let fail_early = settings.fail_early;
    let error_collector = common::error_collector::ErrorCollector::default();
    let mut copy_summary = common::copy::Summary::default();
    // under --require-toctou-safe, run multi-source copies SEQUENTIALLY instead of concurrently, so
    // two sources that alias to the same destination directory (see the duplicate check above)
    // cannot run overlapping reused-directory lockdown/restore lifecycles against it. Strict
    // multi-source is the paranoid, uncommon path, so the sequential cost is acceptable.
    if common::safedir::strict_operand_resolution() && src_dst.len() > 1 {
        for (src_path, dst_path) in src_dst {
            let result = common::copy(&src_path, &dst_path, &settings, &preserve).await;
            if let Some(err) = fold_copy_result(
                result,
                &mut copy_summary,
                &error_collector,
                fail_early,
                args.summary,
            ) {
                return Err(err);
            }
        }
    } else {
        let mut join_set = tokio::task::JoinSet::new();
        for (src_path, dst_path) in src_dst {
            let settings = settings.clone();
            let do_copy =
                || async move { common::copy(&src_path, &dst_path, &settings, &preserve).await };
            join_set.spawn(do_copy());
        }
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(result) => {
                    if let Some(err) = fold_copy_result(
                        result,
                        &mut copy_summary,
                        &error_collector,
                        fail_early,
                        args.summary,
                    ) {
                        return Err(err);
                    }
                }
                Err(error) => {
                    if fail_early {
                        if args.summary {
                            return Err(anyhow!("{}\n\n{}", error, &copy_summary));
                        }
                        return Err(anyhow!("{}", error));
                    }
                    error_collector.push(error.into());
                }
            }
        }
    }
    if let Some(err) = error_collector.into_error() {
        if args.summary {
            return Err(anyhow!("{:#}\n\n{}", err, &copy_summary));
        }
        return Err(err);
    }
    Ok(copy_summary)
}

/// Fold one source→destination copy result into the running summary, applying the shared error
/// policy: on success add the summary; on a copy error log it, add its partial summary, and either
/// signal an immediate `--fail-early` return (`Some(err)`) or push it to `error_collector` and
/// continue (`None`). Used by both the concurrent (default) and serialized (`--require-toctou-safe`
/// multi-source) dispatch paths so both apply identical accounting and fail-early behavior.
fn fold_copy_result(
    result: Result<common::copy::Summary, common::copy::Error>,
    copy_summary: &mut common::copy::Summary,
    error_collector: &common::error_collector::ErrorCollector,
    fail_early: bool,
    want_summary: bool,
) -> Option<anyhow::Error> {
    match result {
        Ok(summary) => {
            *copy_summary = *copy_summary + summary;
            None
        }
        Err(error) => {
            tracing::error!("{:#}", &error);
            *copy_summary = *copy_summary + error.summary;
            if fail_early {
                return Some(if want_summary {
                    anyhow!("{}\n\n{}", error, copy_summary)
                } else {
                    anyhow!("{}", error)
                });
            }
            error_collector.push(error.source);
            None
        }
    }
}

fn has_remote_paths(args: &Args) -> bool {
    // use the same path parser that async_main uses, respecting --force-remote
    let parse_fn = if args.force_remote {
        path::parse_path_force_remote
    } else {
        path::parse_path
    };
    for path in &args.paths {
        if matches!(parse_fn(path), Ok(path::PathType::Remote(_))) {
            return true;
        }
    }
    false
}

fn main() -> Result<(), anyhow::Error> {
    // handle --protocol-version flag before parsing full arguments
    // this allows it to work without required arguments (paths)
    // respect -- separator: only check args before -- to allow files named --protocol-version
    let args: Vec<String> = std::env::args().collect();
    let separator_pos = args.iter().position(|arg| arg == "--");
    let args_to_check = if let Some(pos) = separator_pos {
        &args[..pos]
    } else {
        &args[..]
    };
    if args_to_check.iter().any(|arg| arg == "--protocol-version") {
        let version = common::version::ProtocolVersion::current();
        let json = version.to_json()?;
        println!("{}", json);
        return Ok(());
    }

    let args = Args::parse();
    let files_in_flight = args.common.resolve_files_in_flight();

    // TOCTOU linter: must run before the async runtime starts. The verdict
    // (dereference/Linux) applies to every operation, local or remote. Operands
    // are passed as the filesystem path portion of each argument STRING — the
    // `host:` prefix stripped but tilde NOT expanded (`path::operand_fs_path`),
    // i.e. exactly what a string-level operand policy sees. This makes
    // --require-toctou-safe reject a home-relative `~/x` (or `host:~/x`), whose
    // expansion is environment-dependent and not absolute as written, while the
    // `localhost:/abs` colon escape hatch keeps its absolute `/abs`. So the
    // string a sudo rule / wrapper validated is exactly what the strict contract
    // (absolute + lexically normal, resolved RESOLVE_NO_SYMLINKS) validates;
    // keeping the directories along the path out of a less-privileged actor's
    // write control remains the caller's responsibility (see the "Scope of
    // TOCTOU safety" section of docs/tocttou.md).
    let operand_paths: Vec<std::path::PathBuf> = args
        .paths
        .iter()
        .map(|path| std::path::PathBuf::from(path::operand_fs_path(path)))
        .collect();
    common::toctou_check::enforce_or_exit(
        args.dereference,
        args.toctou_check,
        args.require_toctou_safe,
        &operand_paths,
    );

    let is_remote_operation = has_remote_paths(&args);
    let dry_run_warnings = args.dry_run.map(|_| {
        common::DryRunWarnings::new(
            args.common.progress_requested(),
            args.summary,
            args.common.verbose,
            args.overwrite,
            !args.include.is_empty() || !args.exclude.is_empty() || args.filter_file.is_some(),
            true,
            args.ignore_existing,
        )
    });
    let is_dry_run = dry_run_warnings.is_some();
    let remote_cleanup = is_remote_operation
        .then(remote::RemoteCleanup::new)
        .transpose()
        .context("failed to start remote cleanup supervisor")?;
    let func = {
        let args = args.clone();
        let remote_cleanup = remote_cleanup.clone();
        move || async_main(args, files_in_flight, remote_cleanup)
    };
    let output = args
        .common
        .output_config(args.quiet, !is_dry_run && args.summary);
    let runtime = args.common.runtime_config();
    let mut throttle = args
        .common
        .throttle_config(files_in_flight, args.chunk_size.0);
    if is_remote_operation {
        throttle.apply_files_in_flight = false;
        // in remote mode the master runs no metadata controllers — all probes
        // fire inside rcpd on the remote hosts. Clear both histogram fields so
        // that:
        //   1. The master does not validate --auto-meta-histogram-log locally
        //      against a path that may only exist on the remote hosts.
        //   2. The master does not spawn a logger task that would write an empty
        //      header-only log file at <PATH>.rcp-master.<ext> on the master's
        //      filesystem.
        //   3. The master does not spawn accumulators that will never receive a
        //      sample (master has no controllers in remote mode).
        // the RcpdConfig still carries the original path; each rcpd validates
        // and writes its own log locally.
        throttle.histogram_log_path = None;
        throttle.histogram_enabled = false;
    }
    let tracing = common::TracingConfig {
        remote_layer: None,
        debug_log_file: None,
        chrome_trace_prefix: args.chrome_trace.clone(),
        flamegraph_prefix: args.flamegraph.clone(),
        trace_identifier: "rcp-master".to_string(),
        profile_level: Some(args.profile_level.clone()),
        tokio_console: args.tokio_console,
        tokio_console_port: args.tokio_console_port,
    };
    let progress = if !is_dry_run && args.common.progress_requested() {
        Some(common::ProgressSettings {
            progress_type: if is_remote_operation {
                common::GeneralProgressType::RemoteMaster {
                    progress_type: args.common.progress_type.unwrap_or_default(),
                    get_progress_snapshot: Box::new(remote::tracelog::get_latest_progress_snapshot),
                }
            } else {
                common::GeneralProgressType::User {
                    progress_type: args.common.progress_type.unwrap_or_default(),
                    kind: common::progress::LocalProgressKind::Copy,
                }
            },
            progress_delay: args.common.progress_delay,
        })
    } else {
        None
    };
    let res = common::run(progress, output, runtime, throttle, tracing, func);
    if let Some(remote_cleanup) = remote_cleanup {
        remote_cleanup.finish();
    }
    if let Some(warnings) = dry_run_warnings {
        warnings.print();
    }
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_control_exclusions_follow_local_operand_origin() {
        let remote_source = path::parse_path("host-a:/source").unwrap();
        let remote_destination = path::parse_path("host-b:/destination").unwrap();
        let local_source = path::parse_path("/tmp/local-source").unwrap();
        let local_destination = path::parse_path("/tmp/local-destination").unwrap();
        let default_localhost = path::parse_path("localhost:/tmp/default-localhost").unwrap();
        let forced_localhost =
            path::parse_path_force_remote("localhost:/tmp/forced-localhost").unwrap();

        assert_eq!(
            collect_local_operand_roots(std::slice::from_ref(&local_source), &remote_destination,)
                .unwrap(),
            vec![std::path::PathBuf::from("/tmp/local-source")]
        );
        assert_eq!(
            collect_local_operand_roots(std::slice::from_ref(&remote_source), &local_destination,)
                .unwrap(),
            vec![std::path::PathBuf::from("/tmp/local-destination")]
        );
        assert!(
            collect_local_operand_roots(std::slice::from_ref(&remote_source), &remote_destination,)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            collect_local_operand_roots(
                std::slice::from_ref(&default_localhost),
                &remote_destination,
            )
            .unwrap(),
            vec![std::path::PathBuf::from("/tmp/default-localhost")]
        );
        assert!(
            collect_local_operand_roots(
                std::slice::from_ref(&forced_localhost),
                &remote_destination,
            )
            .unwrap()
            .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tracing_receivers_share_one_teardown_deadline() {
        let receivers = vec![
            (
                remote::AbortOnDropTask::new(tokio::spawn(std::future::pending())),
                remote::tracelog::RcpdType::Source,
            ),
            (
                remote::AbortOnDropTask::new(tokio::spawn(std::future::pending())),
                remote::tracelog::RcpdType::Destination,
            ),
        ];
        let started = tokio::time::Instant::now();

        finish_tracing_receivers(receivers).await;

        assert_eq!(started.elapsed(), TRACING_DRAIN_GRACE);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tracing_receiver_drain_does_not_wait_forever_after_abort() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }));
        started_rx.await.unwrap();

        let drain = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            finish_tracing_receiver(
                receiver,
                remote::tracelog::RcpdType::Source,
                tokio::time::Instant::now() + std::time::Duration::from_millis(20),
            ),
        )
        .await;
        release_tx.send(()).unwrap();

        assert!(
            drain.is_ok(),
            "an uncooperative task must be abandoned after the drain deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tracing_receivers_remain_live_until_processes_finish() {
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(4)).await;
            let _ = completed_tx.send(());
        }));
        let process_wait = async {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        };
        let started = tokio::time::Instant::now();

        finish_remote_teardown(
            process_wait,
            vec![(receiver, remote::tracelog::RcpdType::Source)],
        )
        .await;

        assert_eq!(started.elapsed(), std::time::Duration::from_secs(5));
        completed_rx
            .await
            .expect("receiver must remain live while the daemon is still finishing");
    }

    #[tokio::test]
    async fn dropping_remote_teardown_aborts_retained_receivers() {
        struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for NotifyDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
            let _notify_drop = NotifyDrop(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));
        started_rx.await.unwrap();
        let mut teardown = RemoteTeardown::default();
        teardown.retain_tracing_receiver(receiver, remote::tracelog::RcpdType::Source);

        drop(teardown);

        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("teardown drop must abort retained tracing receivers")
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_remote_teardown_aborts_owned_receivers() {
        struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for NotifyDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
            let _notify_drop = NotifyDrop(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));
        started_rx.await.unwrap();
        let teardown = tokio::spawn(finish_remote_teardown(
            std::future::pending(),
            vec![(receiver, remote::tracelog::RcpdType::Source)],
        ));
        tokio::task::yield_now().await;

        teardown.abort();
        let _ = teardown.await;

        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("cancelling teardown must abort receivers it already took")
            .unwrap();
    }

    fn master_args(extra: &[&str]) -> Args {
        let mut argv = vec!["rcp"];
        argv.extend_from_slice(extra);
        argv.extend_from_slice(&["localhost:/src", "localhost:/dst"]);
        Args::try_parse_from(argv).unwrap()
    }

    fn readiness(
        files_in_flight: common::ConcurrencyLimit,
        max_connections: usize,
    ) -> remote::RcpdConnectionInfo {
        remote::RcpdConnectionInfo {
            addr: "127.0.0.1:1234".parse().unwrap(),
            fingerprint: None,
            files_in_flight,
            max_connections: std::num::NonZeroUsize::new(max_connections).unwrap(),
        }
    }

    #[test]
    fn master_automatic_source_omits_file_limit_overrides() {
        let args = master_args(&[]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap();
        assert_eq!(request.connection_ceiling.value().get(), 100);
        assert!(!request.connection_ceiling.is_explicit());
        let source = build_source_remote_config(&request);
        let spawn_args = source.rcpd.to_args();
        assert!(!spawn_args.iter().any(|arg| arg.contains("files-in-flight")));
        assert!(
            !spawn_args
                .iter()
                .any(|arg| arg.starts_with("--max-open-files"))
        );
        assert!(spawn_args.iter().any(|arg| arg == "--max-connections=100"));
    }

    #[test]
    fn master_automatic_destination_uses_source_selected_concurrency() {
        let args = master_args(&[]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap();
        let source = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(32).unwrap()),
            32,
        );
        let destination = build_destination_remote_config(&request, &source).unwrap();
        let spawn_args = destination.rcpd.to_args();
        assert!(
            spawn_args
                .iter()
                .any(|arg| arg == "--resolved-automatic-files-in-flight=32")
        );
        assert!(spawn_args.iter().any(|arg| arg == "--max-connections=32"));
    }

    #[test]
    fn master_explicit_limit_configures_both_endpoints_identically() {
        let args = master_args(&["--max-connections=100"]);
        let request = build_master_remote_request(
            &args,
            common::ResolvedFilesInFlight::explicit(std::num::NonZeroUsize::new(64).unwrap()),
            None,
        )
        .unwrap();
        assert!(request.connection_ceiling.is_explicit());
        let source = build_source_remote_config(&request);
        let source_readiness = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(64).unwrap()),
            64,
        );
        let destination = build_destination_remote_config(&request, &source_readiness).unwrap();
        for config in [&source.rcpd, &destination.rcpd] {
            let spawn_args = config.to_args();
            assert!(
                spawn_args
                    .iter()
                    .any(|arg| arg == "--max-files-in-flight=64")
            );
            assert!(spawn_args.iter().any(|arg| arg == "--max-connections=64"));
        }
    }

    #[test]
    fn master_reports_master_authoritative_source_file_mismatch() {
        let args = master_args(&["--max-connections=100"]);
        let request = build_master_remote_request(
            &args,
            common::ResolvedFilesInFlight::explicit(std::num::NonZeroUsize::new(64).unwrap()),
            None,
        )
        .unwrap();
        let source_readiness = readiness(common::ConcurrencyLimit::Unlimited, 64);
        let error = build_destination_remote_config(&request, &source_readiness)
            .expect_err("the source must report the master-authoritative file ceiling");
        assert!(
            error
                .to_string()
                .contains("source rcpd reported file ceiling unlimited, expected 64"),
            "unexpected source-readiness error: {error:#}"
        );
    }

    #[test]
    fn master_legacy_finite_preserves_provenance_for_both_daemons() {
        let args = master_args(&["--max-connections=100", "--max-open-files=7"]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::legacy(7), None)
                .unwrap();
        let source = build_source_remote_config(&request);
        let source_readiness = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(7).unwrap()),
            7,
        );
        let destination = build_destination_remote_config(&request, &source_readiness).unwrap();
        for config in [&source.rcpd, &destination.rcpd] {
            let spawn_args = config.to_args();
            assert!(
                spawn_args
                    .iter()
                    .any(|arg| arg == "--forwarded-legacy-files-in-flight=7")
            );
            assert!(
                !spawn_args
                    .iter()
                    .any(|arg| arg.starts_with("--max-open-files"))
            );
        }
    }

    #[test]
    fn master_legacy_unlimited_preserves_provenance_for_both_daemons() {
        let args = master_args(&["--max-connections=100", "--max-open-files=0"]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::legacy(0), None)
                .unwrap();
        let source = build_source_remote_config(&request);
        let source_readiness = readiness(common::ConcurrencyLimit::Unlimited, 100);
        let destination = build_destination_remote_config(&request, &source_readiness).unwrap();
        for config in [&source.rcpd, &destination.rcpd] {
            let spawn_args = config.to_args();
            assert!(
                spawn_args
                    .iter()
                    .any(|arg| arg == "--forwarded-legacy-files-in-flight=0")
            );
            assert!(spawn_args.iter().any(|arg| arg == "--max-connections=100"));
            assert!(
                !spawn_args
                    .iter()
                    .any(|arg| arg.starts_with("--max-open-files"))
            );
        }
    }

    #[test]
    fn master_rejects_destination_readiness_that_differs_from_source() {
        let source = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(32).unwrap()),
            32,
        );
        let wrong_files = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(16).unwrap()),
            32,
        );
        let wrong_streams = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(32).unwrap()),
            16,
        );
        assert!(validate_destination_readiness(&source, &wrong_files).is_err());
        assert!(validate_destination_readiness(&source, &wrong_streams).is_err());
    }

    #[tokio::test]
    async fn master_rejects_invalid_explicit_capacity_before_remote_home_lookup() {
        let streams = tokio::sync::Semaphore::MAX_PERMITS / 2 + 1;
        let streams = streams.to_string();
        let args = Args::try_parse_from([
            "rcp",
            &format!("--max-files-in-flight={streams}"),
            &format!("--max-connections={streams}"),
            "--pending-writes-multiplier=2",
            "unreachable.invalid:~/source",
            "/tmp/destination",
        ])
        .unwrap();
        let files_in_flight = args.common.resolve_files_in_flight();
        let error = async_main(
            args,
            files_in_flight,
            Some(remote::RemoteCleanup::new().unwrap()),
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("pending file capacity"),
            "capacity validation must precede remote HOME lookup: {error:#}"
        );
    }

    #[test]
    fn master_rejects_capacity_invalid_for_every_automatic_source() {
        let multiplier = usize::MAX.to_string();
        let args = master_args(&[&format!("--pending-writes-multiplier={multiplier}")]);
        let error =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .expect_err(
                    "the automatic source floor makes this capacity unconditionally invalid",
                );
        assert!(
            error.to_string().contains("pending file capacity"),
            "unexpected automatic-capacity error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_automatic_capacity_invalid_at_connection_upper_bound() {
        let multiplier = (tokio::sync::Semaphore::MAX_PERMITS / 8 + 1).to_string();
        let args = master_args(&[
            "--max-connections=8",
            &format!("--pending-writes-multiplier={multiplier}"),
        ]);
        let error =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .expect_err(
                    "an unsafe connection upper bound must fail before remote side effects",
                );
        assert!(
            error.to_string().contains("pending file capacity"),
            "unexpected automatic-capacity error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_invalid_filter_before_remote_side_effects() {
        let args = master_args(&["--include=["]);
        let error =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap_err();
        assert!(
            format!("{error:#}").contains("invalid glob pattern"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_unreadable_filter_file_before_remote_side_effects() {
        let filter_directory = tempfile::tempdir().unwrap();
        let filter_arg = format!("--filter-file={}", filter_directory.path().display());
        let args = master_args(&[&filter_arg]);
        let error =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap_err();
        assert!(
            format!("{error:#}").contains("filter file"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_invalid_overwrite_compare_before_remote_side_effects() {
        let args = master_args(&["--overwrite-compare=size,unknown"]);
        let error =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap_err();
        assert!(
            format!("{error:#}").contains("Unknown metadata comparison setting"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_invalid_port_ranges_before_remote_side_effects() {
        let args = master_args(&["--port-ranges=9000-8000"]);
        let error =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap_err();
        assert!(
            format!("{error:#}").contains("start port 9000 > end port 8000"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_pending_capacity_overflow_before_spawn() {
        let max_connections = tokio::sync::Semaphore::MAX_PERMITS.to_string();
        let pending_writes_multiplier = usize::MAX.to_string();
        let args = master_args(&[
            &format!("--max-connections={max_connections}"),
            &format!("--pending-writes-multiplier={pending_writes_multiplier}"),
            "--max-open-files=0",
        ]);
        let error = build_master_remote_request(&args, args.common.resolve_files_in_flight(), None)
            .unwrap_err();
        assert!(
            error.to_string().contains("pending file capacity overflow"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn master_explicit_file_limit_clamp_reports_the_effective_stream_count() {
        let args = master_args(&["--max-connections=100"]);
        let files_in_flight =
            common::ResolvedFilesInFlight::explicit(std::num::NonZeroUsize::new(200).unwrap());
        let request = build_master_remote_request(&args, files_in_flight, None).unwrap();
        let source = build_source_remote_config(&request);
        let effective = std::num::NonZeroUsize::new(source.rcpd.max_connections).unwrap();
        assert_eq!(
            remote_stream_clamp_notice(
                request.concurrency_policy,
                files_in_flight.limit(),
                request.connection_ceiling,
                effective,
            )
            .as_deref(),
            Some(
                "Requested --max-files-in-flight=200, but --max-connections=100 reduced remote data streams to 100"
            )
        );
    }

    #[test]
    fn master_default_connection_clamp_names_the_default_ceiling() {
        let args = master_args(&[]);
        let files_in_flight =
            common::ResolvedFilesInFlight::explicit(std::num::NonZeroUsize::new(200).unwrap());
        let request = build_master_remote_request(&args, files_in_flight, None).unwrap();
        let source = build_source_remote_config(&request);
        let effective = std::num::NonZeroUsize::new(source.rcpd.max_connections).unwrap();
        assert_eq!(
            remote_stream_clamp_notice(
                request.concurrency_policy,
                files_in_flight.limit(),
                request.connection_ceiling,
                effective,
            )
            .as_deref(),
            Some(
                "Requested --max-files-in-flight=200, but the default connection ceiling of 100 reduced remote data streams to 100"
            )
        );
    }

    #[test]
    fn explicit_connection_clamp_names_the_source_automatic_file_ceiling() {
        let args = master_args(&["--max-connections=200"]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap();
        let notice = remote_stream_clamp_notice(
            request.concurrency_policy,
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(4).unwrap()),
            request.connection_ceiling,
            std::num::NonZeroUsize::new(4).unwrap(),
        );
        assert_eq!(
            notice.as_deref(),
            Some(
                "Requested --max-connections=200, but the source's automatic file ceiling of 4 reduced remote data streams to 4"
            )
        );
    }

    #[test]
    fn automatic_default_connection_intersection_stays_quiet() {
        let args = master_args(&[]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap();
        assert_eq!(
            remote_stream_clamp_notice(
                request.concurrency_policy,
                common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(4).unwrap()),
                request.connection_ceiling,
                std::num::NonZeroUsize::new(4).unwrap(),
            ),
            None
        );
    }

    #[test]
    fn explicit_connection_ceiling_reports_automatic_file_limit_clamp() {
        let args = master_args(&["--max-connections=16"]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic(), None)
                .unwrap();
        let notice = remote_stream_clamp_notice(
            request.concurrency_policy,
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(64).unwrap()),
            request.connection_ceiling,
            std::num::NonZeroUsize::new(16).unwrap(),
        );
        assert_eq!(
            notice.as_deref(),
            Some(
                "The source's automatic file ceiling of 64 was reduced to 16 remote data streams by --max-connections=16"
            )
        );
    }

    #[test]
    fn legacy_file_limit_clamp_preserves_the_deprecated_option_name() {
        let args = master_args(&["--max-open-files=200", "--max-connections=100"]);
        let files_in_flight = common::ResolvedFilesInFlight::legacy(200);
        let request = build_master_remote_request(&args, files_in_flight, None).unwrap();
        let notice = remote_stream_clamp_notice(
            request.concurrency_policy,
            files_in_flight.limit(),
            request.connection_ceiling,
            std::num::NonZeroUsize::new(100).unwrap(),
        );
        assert_eq!(
            notice.as_deref(),
            Some(
                "Requested --max-open-files=200, but --max-connections=100 reduced remote data streams to 100"
            )
        );
    }
}
