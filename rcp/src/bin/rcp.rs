use anyhow::{Context, anyhow};
use clap::Parser;
use tracing::instrument;

use rcp_tools_rcp::master_session::{
    self, ConnectionCeiling, DEFAULT_REMOTE_DIRECT_PROBE_TIMEOUT_SEC,
    MasterAuthoritativeFilesInFlight, MasterRemoteRequest, RemoteConcurrencyPolicy,
    RemoteTransport, daemons_need_tls_certs,
};
use rcp_tools_rcp::path;

#[path = "rcp/ssh_plan.rs"]
mod ssh_plan;

use ssh_plan::{
    RemoteCopyOperands, ResolvedMasterSshPlan, collect_local_operand_roots, remote_copy_operands,
};

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

    /// Control reflink-capable acceleration for local file copies
    ///
    /// In auto mode, use filesystem acceleration when available. In never mode, use sparse-aware
    /// read/write copying, bypassing all copy_file_range acceleration (including
    /// non-reflink kernel and server-side copying). Remote transfers already stream
    /// bytes, so both values have the same effect there.
    #[arg(
        long,
        value_enum,
        default_value_t,
        value_name = "WHEN",
        help_heading = "Copy options"
    )]
    reflink: common::copy_data::ReflinkMode,

    /// Control worker reuse when copying local regular files
    #[arg(
        long,
        value_enum,
        default_value_t,
        value_name = "WHEN",
        help_heading = "Copy options"
    )]
    local_copy_handoff: common::copy::LocalCopyHandoff,

    /// Largest read/write buffer each copying thread keeps between local files
    ///
    /// Local copies that cannot use copy_file_range, such as those between different filesystems
    /// or with --reflink=never, go through a buffer of up to 1 MiB. A thread keeps that buffer for
    /// its next file unless it is larger than this limit. Zero keeps nothing: every file then
    /// allocates its own buffer. This bounds idle memory only, not how much is read at a time.
    /// Accepts sizes like "256KiB", "1MiB", or bytes. Remote transfers are unaffected.
    #[arg(
        long,
        value_name = "SIZE",
        default_value_t,
        help_heading = "Copy options"
    )]
    local_copy_buffer_retention_limit: common::copy_data::BufferRetentionLimit,

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

    /// Format of the summary that --summary prints
    ///
    /// "text" is the human-readable summary. "json" prints one JSON object on a single line
    /// instead: the same counters as exact numbers, the wall time in milliseconds and the
    /// statistics of the process (CPU time, peak memory, context switches, page faults).
    /// After a remote copy the object also holds, under "remote", the statistics of the source
    /// and of the destination rcpd with the host each ran on.
    /// A run that fails still reports its error as text.
    #[arg(
        long,
        default_value = "text",
        value_name = "FORMAT",
        help_heading = "Progress & output"
    )]
    output_format: common::OutputFormat,

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
    /// IP address to bind the source TCP server to
    ///
    /// By default, the best available network interface is automatically selected.
    /// Use this option to explicitly bind to a specific IP address (e.g., "192.168.1.5").
    /// This is useful for multi-homed hosts or when you want to control which network
    /// is used for TCP traffic. Only IPv4 addresses are supported.
    ///
    /// When omitted and the source path uses an IP address (e.g., "192.168.1.100:/path"),
    /// that IP is automatically passed to the source rcpd so it binds explicitly to that address.
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
    /// probes, deployment readiness/write-idle periods, daemon readiness, and destination→source
    /// connection. It does not cap total deployment transfer time; post-transfer verification gets
    /// at least 60 seconds. Must be at least 1.
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

    /// Liveness budget for remote connections in seconds (0 disables)
    ///
    /// A peer whose host vanishes (power loss, severed link, destroyed VM) sends neither FIN
    /// nor RST, so a copy waiting on it would otherwise hang forever. Master↔rcpd SSH sessions
    /// enforce this budget via OpenSSH ServerAlive probes and periodic stdin heartbeats.
    /// Source↔Destination TCP connections probe when idle (TCP keepalive); the control connection
    /// additionally bounds unacknowledged data (TCP_USER_TIMEOUT). The bulk data connections
    /// deliberately do not, because a throttled receiver legitimately stops reading mid-file — so
    /// a host that vanishes mid-transfer is still only detected by the kernel's retransmission
    /// limit (~15 min). Widen it on a flaky WAN; a stall shorter than the budget is survived
    /// untouched. Propagated to both rcpds.
    #[arg(
        long,
        value_name = "N",
        default_value_t = remote::DEFAULT_REMOTE_KEEPALIVE_SEC,
        help_heading = "Remote copy options"
    )]
    remote_keepalive_sec: u64,

    /// Network profile for TCP tuning
    ///
    /// A profile selects one buffer size. It is the TCP socket buffer size wanted on data
    /// connections, which each daemon requests from its kernel only where the request yields more
    /// than the kernel's own sizing, and the default for the remote copy buffer and for the
    /// buffer kept between files.
    /// 'datacenter' (default): Optimized for datacenter networks (<1ms RTT, 25-100 Gbps).
    /// Uses 16 MiB buffers for high-bandwidth links.
    /// 'internet': Conservative settings for internet connections.
    /// Uses 2 MiB buffers suitable for shared networks.
    #[arg(
        long,
        default_value = "datacenter",
        value_name = "PROFILE",
        help_heading = "Remote copy options"
    )]
    network_profile: remote::NetworkProfile,

    /// Maximum transfer buffer and file I/O request size for remote copies.
    ///
    /// Sets the size of the source's read buffer and of the destination's receive buffer, which
    /// is also the size of its file writes.
    /// The configured value must fit usize on the master and each daemon; otherwise
    /// argument validation or daemon startup fails. Accepted per-file limits are capped
    /// to the file size and the host's isize::MAX, with a one-byte minimum.
    /// These are representability limits, not available-memory checks; oversized settings
    /// can still exhaust memory and abort a daemon. Individual disk/network operations can be shorter;
    /// this does not set TCP packet or TLS record sizes.
    /// Each active transfer holds one buffer of up to this size on each side, so larger limits
    /// allow more working memory. Keeping buffers between files is controlled separately.
    /// Accepts byte sizes like "256KiB", "1MiB", or plain numbers in bytes.
    ///
    /// Default: the network profile's buffer size, 16 MiB for datacenter and 2 MiB for internet.
    #[arg(long, value_name = "SIZE", help_heading = "Remote copy options")]
    remote_copy_buffer_size: Option<bytesize::ByteSize>,

    /// Maximum transfer buffer each side keeps per remote data connection between files.
    ///
    /// Applies to the source's read buffer and to the destination's receive buffer.
    /// Defaults to the smaller of the remote copy buffer size and the network profile's buffer
    /// size (16 MiB for datacenter, 2 MiB for internet), so a profile's own buffer is kept whole.
    /// Allocated lazily, only as needed. Accepts sizes like "256KiB", "1MiB", or bytes.
    /// Zero disables reuse. Smaller limits reduce idle memory without reducing I/O chunks;
    /// chunks exceeding the limit allocate temporary storage for each file.
    #[arg(long, value_name = "SIZE", help_heading = "Remote copy options")]
    remote_copy_buffer_retention_limit: Option<bytesize::ByteSize>,

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

    /// Disable TLS encryption and authentication for source↔destination TCP connections
    ///
    /// By default, source↔destination TCP connections use mutual TLS for authentication and
    /// encryption, while master↔rcpd control and tracing run over SSH. This flag disables BOTH
    /// encryption AND authentication on source↔destination TCP connections (control and data).
    /// WARNING: file data and control traffic between source and destination are sent in plaintext
    /// and the source listener accepts connections from anyone who can reach its port. Only use on
    /// isolated, trusted networks.
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
    /// use of the remote copy protocol (rcpd) instead, which is useful for testing.
    /// Remote mode does not check whether source and destination trees overlap;
    /// keep them disjoint to avoid copying the destination into itself.
    ///
    /// Requires paths to use the `localhost:` prefix (e.g., `localhost:/path/to/file`).
    #[arg(long, help_heading = "Remote copy options")]
    force_remote: bool,

    /// Transport for source↔destination control and data connections
    ///
    /// - 'tcp' (default): direct TCP between source and destination (highest throughput).
    /// - 'ssh': tunnel source↔destination traffic over SSH without opening extra firewall ports
    ///   (same-host copies still use loopback directly). Ignores --port-ranges and --bind-ip.
    /// - 'auto': try direct TCP first (honoring --port-ranges and --bind-ip) and fall back to
    ///   SSH tunnels if the initial control connection fails within --remote-direct-probe-timeout-sec.
    ///
    /// Master↔rcpd control and tracing always run over SSH regardless of this setting.
    #[arg(
        long,
        value_enum,
        default_value_t = RemoteTransport::Tcp,
        value_name = "MODE",
        help_heading = "Remote copy options"
    )]
    remote_transport: RemoteTransport,

    /// Number of parallel SSH tunnels for source↔destination data transfer when using SSH transport
    ///
    /// When omitted, defaults to the effective remote data stream count capped at 16 (matching the
    /// CPU-derived remote copy default while avoiding excessive concurrent SSH sessions on large
    /// hosts). Pass an explicit value to override that default cap in either direction (still
    /// bounded by effective data streams). Degrades gracefully down to 1 if the remote SSH server
    /// rate-limits extra connections. Ignored for direct TCP and same-host copies.
    #[arg(
        long,
        value_name = "N",
        value_parser = common::cli::parse_positive_usize,
        help_heading = "Remote copy options"
    )]
    ssh_tunnels: Option<std::num::NonZeroUsize>,

    /// Forward the local SSH agent (`ForwardAgent=yes`) to the destination host for 3-host SSH copies
    ///
    /// In 3-host copies (`host1:/src host2:/dst`) over SSH, rcp opens SSH tunnels directly from
    /// host2 to host1 instead of relaying data through the master. Enable this flag (or configure
    /// `ForwardAgent yes` for host2 in `~/.ssh/config`) if host2 needs your local SSH agent to
    /// authenticate to host1. Only applied to the destination host when 3-host SSH transport may
    /// be used.
    #[arg(long, help_heading = "Remote copy options")]
    ssh_forward_agent: bool,

    /// Direct-TCP probe timeout in seconds before falling back to SSH in `--remote-transport=auto`
    ///
    /// Capped at `--remote-copy-conn-timeout-sec`. Must be at least 1.
    #[arg(
        long,
        default_value_t = DEFAULT_REMOTE_DIRECT_PROBE_TIMEOUT_SEC,
        value_name = "N",
        value_parser = common::cli::parse_positive_u64,
        help_heading = "Remote copy options"
    )]
    remote_direct_probe_timeout_sec: u64,

    // ARGUMENTS
    /// Source path(s) and destination path
    #[arg()]
    paths: Vec<String>,
}

fn remote_buffer_size_for_host(size: u64, host_max: usize) -> anyhow::Result<usize> {
    anyhow::ensure!(
        size <= host_max as u64,
        "remote copy buffer size exceeds this platform's addressable size ({host_max} bytes)"
    );
    // the supplied host limit is a usize, so the accepted value can be narrowed losslessly
    Ok(size as usize)
}

fn build_master_remote_request(
    args: &Args,
    files_in_flight: common::ResolvedFilesInFlight,
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
        common::FilesInFlightSource::Explicit => RemoteConcurrencyPolicy::MasterAuthoritative {
            files_in_flight: MasterAuthoritativeFilesInFlight::Explicit,
            concurrency: remote::resolve_remote_concurrency(
                files_in_flight.limit(),
                configured_connections,
                args.pending_writes_multiplier,
            )?,
        },
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
    let buffer_size = args
        .remote_copy_buffer_size
        .map(|size| remote_buffer_size_for_host(size.0, usize::MAX))
        .transpose()?;
    let buffer_retention_limit = args
        .remote_copy_buffer_retention_limit
        .map(|size| usize::try_from(size.0))
        .transpose()
        .context("remote copy buffer retention limit exceeds this platform's addressable size")?;
    let effective_port_ranges = if args.remote_transport == RemoteTransport::Ssh {
        None
    } else {
        args.port_ranges.clone()
    };
    let tcp = remote::TcpConfig {
        port_ranges: effective_port_ranges.clone(),
        conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        network_profile: args.network_profile,
        buffer_size,
        buffer_retention_limit,
        keepalive_sec: args.remote_keepalive_sec,
    };
    let rcpd = remote::protocol::RcpdConfig {
        verbose: args.common.verbose,
        fail_early: args.fail_early,
        preview_only: args.dry_run.is_some(),
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
        port_ranges: effective_port_ranges,
        progress: args.common.progress
            || matches!(
                args.common.progress_type,
                Some(common::ProgressType::JsonLines)
            ),
        progress_delay: args.common.progress_delay.clone(),
        remote_copy_conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        remote_keepalive_sec: args.remote_keepalive_sec,
        network_profile: args.network_profile,
        buffer_size,
        buffer_retention_limit,
        max_connections: configured_connections.get(),
        pending_writes_multiplier: args.pending_writes_multiplier.get(),
        chrome_trace_prefix: args.chrome_trace.clone(),
        timings_prefix: args.common.timings.clone(),
        timings_detail: args.common.timings_detail,
        flamegraph_prefix: args.flamegraph.clone(),
        profile_level: Some(args.profile_level.clone()),
        tokio_console: args.tokio_console,
        tokio_console_port: args.tokio_console_port,
        encryption: daemons_need_tls_certs(args.no_encryption, args.remote_transport),
        ssh_tunnels: 0,
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

struct SshDaemonHandle(remote::RcpdProcess);

impl master_session::DaemonHandle for SshDaemonHandle {
    fn wait(self: Box<Self>) -> futures::future::BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(remote::wait_for_rcpd_process(self.0))
    }
}

struct SshTunnelHandle(remote::ssh_tunnel::SshTunnelSet);

impl master_session::TunnelHandle for SshTunnelHandle {
    fn close(self: Box<Self>) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(self.0.close())
    }
}

struct SshRemoteBackend<'a> {
    prepared_source: remote::PreparedRcpd,
    prepared_destination: remote::PreparedRcpd,
    keepalive_sec: u64,
    cleanup: &'a remote::RemoteCleanup,
    local_operand_roots: &'a [std::path::PathBuf],
}

impl master_session::RemoteBackend for SshRemoteBackend<'_> {
    fn probe_peer_identity(
        &self,
        role: remote::protocol::RcpdRole,
        timeout: std::time::Duration,
    ) -> futures::future::BoxFuture<
        '_,
        anyhow::Result<(remote::protocol::PeerIdentity, Option<std::net::SocketAddr>)>,
    > {
        let endpoint = match role {
            remote::protocol::RcpdRole::Source => &self.prepared_source,
            remote::protocol::RcpdRole::Destination => &self.prepared_destination,
        };
        Box::pin(endpoint.probe_peer_identity(timeout))
    }

    fn disable_destination_agent_forward(&mut self) {
        self.prepared_destination.disable_agent_forward();
    }

    fn spawn_rcpd<'a>(
        &'a self,
        role: remote::protocol::RcpdRole,
        config: &'a remote::protocol::RcpdConfig,
        bind_ip: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<master_session::SpawnedDaemon>> {
        let endpoint = match role {
            remote::protocol::RcpdRole::Source => &self.prepared_source,
            remote::protocol::RcpdRole::Destination => &self.prepared_destination,
        };
        Box::pin(async move {
            let mut process = endpoint.spawn(config, bind_ip, role).await?;
            let readiness = process.readiness.clone();
            let (stdin, stdout) = match process.take_stdio() {
                Ok(stdio) => stdio,
                Err(error) => {
                    let _ = remote::wait_for_rcpd_process(process).await;
                    return Err(error);
                }
            };
            Ok(master_session::SpawnedDaemon {
                readiness,
                stdin,
                stdout,
                handle: Box::new(SshDaemonHandle(process)),
            })
        })
    }

    fn open_tunnels(
        &self,
        request: master_session::TunnelRequest,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<master_session::OpenedTunnels>> {
        let endpoint = match request.direction {
            remote::ssh_tunnel::ForwardDirection::Local => &self.prepared_source,
            remote::ssh_tunnel::ForwardDirection::Remote => &self.prepared_destination,
        };
        Box::pin(async move {
            let tunnels = endpoint
                .open_ssh_tunnels(
                    request.direction,
                    request.target_control,
                    request.target_data,
                    request.requested_tunnels,
                    request.listen_address_family,
                    request.expected_peer.as_ref(),
                    request.node_endpoint,
                    self.keepalive_sec,
                    request.timeout,
                    self.cleanup,
                    self.local_operand_roots,
                )
                .await?;
            let control_addr = tunnels.control_addr();
            let data_addrs = tunnels.data_addrs().to_vec();
            let summary = tunnels.format_tunnel_summary();
            Ok(master_session::OpenedTunnels {
                control_addr,
                data_addrs,
                summary,
                handle: Box::new(SshTunnelHandle(tunnels)),
            })
        })
    }
}

#[instrument(skip(ssh_plan))]
async fn run_rcpd_master(
    args: &Args,
    preserve: &common::preserve::Settings,
    operands: &RemoteCopyOperands,
    request: MasterRemoteRequest,
    ssh_plan: ResolvedMasterSshPlan,
    cleanup: &remote::RemoteCleanup,
) -> anyhow::Result<common::copy::Summary> {
    let bootstrap_timeout = std::time::Duration::from_secs(request.tcp.conn_timeout_sec);
    let keepalive_sec = request.tcp.keepalive_sec;
    let ResolvedMasterSshPlan {
        resolved_src_ssh,
        resolved_dst_ssh,
        dest_agent_forward_mode,
        preauthenticated_destination,
    } = ssh_plan;
    let initial_topology =
        operands.initial_topology(resolved_src_ssh.as_ref(), resolved_dst_ssh.as_ref());
    let (prepared_source, prepared_destination) = remote::prepare_rcpd_endpoints_with_timeout(
        operands.source.session(),
        operands.destination.session(),
        args.rcpd_path.as_deref(),
        args.auto_deploy_rcpd,
        cleanup,
        bootstrap_timeout,
        keepalive_sec,
        &operands.local_operand_roots,
        dest_agent_forward_mode,
        preauthenticated_destination,
    )
    .await?;
    let mut backend = SshRemoteBackend {
        prepared_source,
        prepared_destination,
        keepalive_sec,
        cleanup,
        local_operand_roots: &operands.local_operand_roots,
    };
    let plan = master_session::MasterSessionPlan {
        src_host: operands.source.session().host.clone(),
        src_path: operands.source.path().to_path_buf(),
        dst_host: operands.destination.session().host.clone(),
        dst_path: operands.destination.path().to_path_buf(),
        source_is_local: operands.source_is_local,
        initial_topology,
        transport: args.remote_transport,
        no_encryption: args.no_encryption,
        bind_ip: args.bind_ip.clone(),
        port_ranges_configured: args.port_ranges.is_some(),
        ssh_tunnels: args.ssh_tunnels,
        remote_direct_probe_timeout_sec: args.remote_direct_probe_timeout_sec,
        dry_run: args.dry_run,
        preserve: *preserve,
        resolved_src_ssh,
        resolved_dst_ssh,
        request,
    };
    let outcome = master_session::run_master_session(plan, &mut backend).await;
    if let Some(stats) = outcome.runtime_stats {
        common::set_remote_runtime_stats(stats);
    }
    outcome.result
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
        // explicit limits can be validated entirely by the master, so reject invalid capacities
        // before a `~` operand can trigger a remote HOME lookup. Automatic selection remains
        // source-owned and is resolved only after source readiness.
        let request = build_master_remote_request(&args, files_in_flight)?;
        let remote_home_timeout = std::time::Duration::from_secs(request.tcp.conn_timeout_sec);
        let keepalive_sec = request.tcp.keepalive_sec;
        let ssh_plan = ResolvedMasterSshPlan::prepare(
            &mut operands,
            args.remote_transport,
            args.ssh_forward_agent,
            remote_home_timeout,
            keepalive_sec,
            cleanup,
        )
        .await?;
        return match run_rcpd_master(&args, &preserve, &operands, request, ssh_plan, cleanup).await
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
        // previews report attempted work; only real copies need this existing-destination preflight
        if args.dry_run.is_none()
            && src_strings.len() == 1
            && !dst_string.ends_with('/')
            && !(args.overwrite || args.delete)
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
        reflink: args.reflink,
        local_copy_handoff: args.local_copy_handoff,
        local_copy_buffer_retention_limit: args.local_copy_buffer_retention_limit,
        dereference: args.dereference,
        fail_early: args.fail_early,
        overwrite: args.overwrite || args.delete,
        overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)
            .map_err(|err| common::copy::Error::new(err, Default::default()))?,
        overwrite_filter: args.overwrite_filter,
        ignore_existing: args.ignore_existing,
        chunk_size: args.chunk_size.0,
        skip_specials: args.skip_specials,
        // local copy buffers are managed by copy_data, independently of remote buffering
        remote_copy_buffer_size: 0,
        filter,
        dry_run: args.dry_run,
        delete,
    };
    tracing::debug!("copy settings: {:?}", &settings);
    let fail_early = settings.fail_early;
    let error_collector = common::error_collector::ErrorCollector::default();
    let mut copy_summary = common::copy::Summary::default();
    let session = common::copy::CopySession::default();
    // only explicit roots are inspected here: no directory listing or recursive input inventory
    // retain each tentative claim until that operand finishes admission or fails
    let mut prepared = Vec::with_capacity(src_dst.len());
    for (src, dst) in &src_dst {
        prepared.push(session.prepare(src, dst, &settings).await);
    }

    // under --require-toctou-safe, run multi-source copies SEQUENTIALLY instead of concurrently, so
    // two sources that alias to the same destination directory (see the duplicate check above)
    // cannot run overlapping reused-directory lockdown/restore lifecycles against it. Strict
    // multi-source is the paranoid, uncommon path, so the sequential cost is acceptable.
    if common::safedir::strict_operand_resolution() && src_dst.len() > 1 {
        for operand in prepared {
            let result = match operand {
                Ok(operand) => common::copy_prepared(operand, &preserve).await,
                Err(error) => Err(error),
            };
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
        for operand in prepared {
            let do_copy = || async move {
                match operand {
                    Ok(operand) => common::copy_prepared(operand, &preserve).await,
                    Err(error) => Err(error),
                }
            };
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
        let version = remote::version::ProtocolVersion::current();
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
        move |_admission| async_main(args, files_in_flight, remote_cleanup)
    };
    let output =
        args.common
            .output_config(args.quiet, !is_dry_run && args.summary, args.output_format);
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
        timings_prefix: args.common.timings.clone(),
        timings_detail: args.common.timings_detail,
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
                    take_progress_snapshot: Box::new(remote::tracelog::take_progress_snapshot),
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
    use master_session::{
        build_destination_remote_config, build_source_remote_config, remote_stream_clamp_notice,
    };

    fn master_args(extra: &[&str]) -> Args {
        let mut argv = vec!["rcp"];
        argv.extend_from_slice(extra);
        argv.extend_from_slice(&["localhost:/src", "localhost:/dst"]);
        Args::try_parse_from(argv).unwrap()
    }

    fn readiness(
        files_in_flight: common::ConcurrencyLimit,
        max_connections: usize,
    ) -> remote::RcpdReadiness {
        remote::RcpdReadiness {
            fingerprint: None,
            files_in_flight,
            max_connections: std::num::NonZeroUsize::new(max_connections).unwrap(),
        }
    }

    #[test]
    fn json_progress_requests_periodic_daemon_observations() {
        for (args, expected) in [
            (vec!["--progress-type=json-lines"], "10s"),
            (
                vec!["--progress-type=json-lines", "--progress-delay=80ms"],
                "80ms",
            ),
        ] {
            let args = master_args(&args);
            let request =
                build_master_remote_request(&args, args.common.resolve_files_in_flight()).unwrap();
            assert!(request.rcpd.progress);
            assert_eq!(request.rcpd.progress_delay.as_deref(), Some(expected));
        }
    }

    #[test]
    fn master_forwards_buffer_configuration_to_both_daemon_roles() {
        // options, copy buffer, explicit kept-buffer limit, kept-buffer limit in effect
        for (options, chunk, limit, kept) in [
            (vec![], 16 * 1024 * 1024, None, 16 * 1024 * 1024),
            (
                vec!["--network-profile=internet"],
                2 * 1024 * 1024,
                None,
                2 * 1024 * 1024,
            ),
            (
                vec!["--remote-copy-buffer-size=1MiB"],
                1024 * 1024,
                None,
                1024 * 1024,
            ),
            (
                vec!["--remote-copy-buffer-size=32MiB"],
                32 * 1024 * 1024,
                None,
                16 * 1024 * 1024,
            ),
            (
                vec![
                    "--network-profile=internet",
                    "--remote-copy-buffer-size=16MiB",
                ],
                16 * 1024 * 1024,
                None,
                2 * 1024 * 1024,
            ),
            (
                vec!["--remote-copy-buffer-retention-limit=2MiB"],
                16 * 1024 * 1024,
                Some(2 * 1024 * 1024),
                2 * 1024 * 1024,
            ),
            (
                vec![
                    "--remote-copy-buffer-size=1MiB",
                    "--remote-copy-buffer-retention-limit=256KiB",
                ],
                1024 * 1024,
                Some(256 * 1024),
                256 * 1024,
            ),
            (
                vec!["--remote-copy-buffer-retention-limit=0"],
                16 * 1024 * 1024,
                Some(0),
                0,
            ),
            (
                vec![
                    "--remote-copy-buffer-size=1024",
                    "--remote-copy-buffer-retention-limit=2MiB",
                ],
                1024,
                Some(2 * 1024 * 1024),
                2 * 1024 * 1024,
            ),
        ] {
            let args = master_args(&options);
            let files =
                common::ResolvedFilesInFlight::explicit(std::num::NonZeroUsize::new(4).unwrap());
            let request = build_master_remote_request(&args, files).unwrap();
            let source = build_source_remote_config(&request);
            let destination =
                build_destination_remote_config(&request, &readiness(files.limit(), 4)).unwrap();
            assert_eq!(request.tcp.effective_buffer_size(), chunk);
            assert_eq!(request.tcp.effective_buffer_retention_limit(), kept);
            for config in [&source, &destination] {
                assert_eq!(config.buffer_size, request.tcp.buffer_size);
                let buffer_args: Vec<_> = config
                    .to_args()
                    .into_iter()
                    .filter(|arg| arg.starts_with("--buffer-size="))
                    .collect();
                assert_eq!(
                    buffer_args,
                    request
                        .tcp
                        .buffer_size
                        .map(|value| format!("--buffer-size={value}"))
                        .into_iter()
                        .collect::<Vec<_>>()
                );
                assert_eq!(config.buffer_retention_limit, limit);
                let retained_args: Vec<_> = config
                    .to_args()
                    .into_iter()
                    .filter(|arg| arg.starts_with("--buffer-retention-limit="))
                    .collect();
                assert_eq!(
                    retained_args,
                    limit
                        .map(|value| format!("--buffer-retention-limit={value}"))
                        .into_iter()
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn remote_buffer_size_conversion_checks_the_supplied_host_limit() {
        // exercise the rejection path on 64-bit CI as well as on a 32-bit host
        let host_max = u32::MAX as usize;
        for size in [0, 1, host_max as u64 - 1, host_max as u64] {
            assert_eq!(
                remote_buffer_size_for_host(size, host_max).unwrap(),
                size as usize
            );
        }
        for size in [host_max as u64 + 1, u64::MAX] {
            let error = remote_buffer_size_for_host(size, host_max).unwrap_err();
            assert!(
                format!("{error:#}")
                    .contains("remote copy buffer size exceeds this platform's addressable size")
            );
            assert!(format!("{error:#}").contains(&host_max.to_string()));
        }
    }

    #[test]
    fn master_rejects_invalid_buffer_retention_sizes() {
        for value in ["-1", "nonsense"] {
            let error = Args::try_parse_from([
                "rcp",
                &format!("--remote-copy-buffer-retention-limit={value}"),
                "localhost:/src",
                "/dst",
            ])
            .unwrap_err();
            assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
            assert!(
                error
                    .to_string()
                    .contains("--remote-copy-buffer-retention-limit")
            );
        }
    }

    #[test]
    fn master_forwards_preview_startup_policy_to_both_roles() {
        for options in [vec![], vec!["--dry-run=brief"], vec!["--dry-run=explain"]] {
            let args = master_args(&options);
            let files =
                common::ResolvedFilesInFlight::explicit(std::num::NonZeroUsize::new(200).unwrap());
            let request = build_master_remote_request(&args, files).unwrap();
            let source = build_source_remote_config(&request);
            let source_readiness = readiness(files.limit(), 100);
            let destination = build_destination_remote_config(&request, &source_readiness).unwrap();
            for config in [&source, &destination] {
                assert_eq!(config.preview_only, args.dry_run.is_some());
                assert_eq!(
                    config.to_args().iter().any(|arg| arg == "--preview-only"),
                    args.dry_run.is_some(),
                );
            }
        }
    }

    #[test]
    fn master_automatic_source_omits_file_limit_overrides() {
        let args = master_args(&[]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic()).unwrap();
        assert_eq!(request.connection_ceiling.value().get(), 100);
        assert!(!request.connection_ceiling.is_explicit());
        let source = build_source_remote_config(&request);
        let spawn_args = source.to_args();
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
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic()).unwrap();
        let source = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(32).unwrap()),
            32,
        );
        let destination = build_destination_remote_config(&request, &source).unwrap();
        let spawn_args = destination.to_args();
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
        )
        .unwrap();
        assert!(request.connection_ceiling.is_explicit());
        let source = build_source_remote_config(&request);
        let source_readiness = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(64).unwrap()),
            64,
        );
        let destination = build_destination_remote_config(&request, &source_readiness).unwrap();
        for config in [&source, &destination] {
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
    fn master_explicit_unlimited_configures_both_endpoints_identically() {
        let args = master_args(&["--max-files-in-flight=unlimited", "--max-connections=3"]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::unlimited())
                .expect("explicit unlimited file admission must be valid remotely");
        let source = build_source_remote_config(&request);
        let source_readiness = readiness(common::ConcurrencyLimit::Unlimited, 3);
        let destination = build_destination_remote_config(&request, &source_readiness).unwrap();
        for config in [&source, &destination] {
            let spawn_args = config.to_args();
            assert!(
                spawn_args
                    .iter()
                    .any(|arg| arg == "--max-files-in-flight=unlimited")
            );
            assert!(spawn_args.iter().any(|arg| arg == "--max-connections=3"));
            assert!(
                !spawn_args
                    .iter()
                    .any(|arg| arg.starts_with("--forwarded-legacy-files-in-flight"))
            );
        }
    }

    #[test]
    fn master_reports_master_authoritative_source_file_mismatch() {
        let args = master_args(&["--max-connections=100"]);
        let request = build_master_remote_request(
            &args,
            common::ResolvedFilesInFlight::explicit(std::num::NonZeroUsize::new(64).unwrap()),
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
            build_master_remote_request(&args, common::ResolvedFilesInFlight::legacy(7)).unwrap();
        let source = build_source_remote_config(&request);
        let source_readiness = readiness(
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(7).unwrap()),
            7,
        );
        let destination = build_destination_remote_config(&request, &source_readiness).unwrap();
        for config in [&source, &destination] {
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
            build_master_remote_request(&args, common::ResolvedFilesInFlight::legacy(0)).unwrap();
        let source = build_source_remote_config(&request);
        let source_readiness = readiness(common::ConcurrencyLimit::Unlimited, 100);
        let destination = build_destination_remote_config(&request, &source_readiness).unwrap();
        for config in [&source, &destination] {
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
        let error = build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic())
            .expect_err("the automatic source floor makes this capacity unconditionally invalid");
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
        let error = build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic())
            .expect_err("an unsafe connection upper bound must fail before remote side effects");
        assert!(
            error.to_string().contains("pending file capacity"),
            "unexpected automatic-capacity error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_invalid_filter_before_remote_side_effects() {
        let args = master_args(&["--include=["]);
        let error = build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic())
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
        let error = build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic())
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("filter file"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_invalid_overwrite_compare_before_remote_side_effects() {
        let args = master_args(&["--overwrite-compare=size,unknown"]);
        let error = build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic())
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("Unknown metadata comparison setting"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn master_rejects_invalid_port_ranges_before_remote_side_effects() {
        let args = master_args(&["--port-ranges=9000-8000"]);
        let error = build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic())
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
        let error =
            build_master_remote_request(&args, args.common.resolve_files_in_flight()).unwrap_err();
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
        let request = build_master_remote_request(&args, files_in_flight).unwrap();
        let source = build_source_remote_config(&request);
        let effective = std::num::NonZeroUsize::new(source.max_connections).unwrap();
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
        let request = build_master_remote_request(&args, files_in_flight).unwrap();
        let source = build_source_remote_config(&request);
        let effective = std::num::NonZeroUsize::new(source.max_connections).unwrap();
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
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic()).unwrap();
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
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic()).unwrap();
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
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic()).unwrap();
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
        let request = build_master_remote_request(&args, files_in_flight).unwrap();
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

    #[test]
    fn remote_transport_ssh_clears_port_ranges_after_validating_syntax() {
        let args = master_args(&["--remote-transport=ssh", "--port-ranges=8000-8010"]);
        let request =
            build_master_remote_request(&args, common::ResolvedFilesInFlight::automatic()).unwrap();
        assert_eq!(request.tcp.port_ranges, None);
        assert_eq!(request.rcpd.port_ranges, None);
        // syntax errors still fail fast even when --remote-transport=ssh ignores port_ranges
        let bad = master_args(&["--remote-transport=ssh", "--port-ranges=not-a-range"]);
        assert!(
            build_master_remote_request(&bad, common::ResolvedFilesInFlight::automatic()).is_err()
        );
        // --remote-transport=auto retains port_ranges for the direct-TCP probe
        let auto = master_args(&["--remote-transport=auto", "--port-ranges=8000-8010"]);
        let auto_request =
            build_master_remote_request(&auto, common::ResolvedFilesInFlight::automatic()).unwrap();
        assert_eq!(auto_request.tcp.port_ranges.as_deref(), Some("8000-8010"));
        assert_eq!(auto_request.rcpd.port_ranges.as_deref(), Some("8000-8010"));
    }

    #[test]
    fn remote_transport_flags_default_and_parse_correctly() {
        let defaults = master_args(&[]);
        assert_eq!(defaults.remote_transport, RemoteTransport::Tcp);
        assert_eq!(defaults.ssh_tunnels, None);
        assert!(!defaults.ssh_forward_agent);
        assert_eq!(
            defaults.remote_direct_probe_timeout_sec,
            DEFAULT_REMOTE_DIRECT_PROBE_TIMEOUT_SEC
        );
        let custom = master_args(&[
            "--remote-transport=auto",
            "--ssh-tunnels=8",
            "--ssh-forward-agent",
            "--remote-direct-probe-timeout-sec=5",
        ]);
        assert_eq!(custom.remote_transport, RemoteTransport::Auto);
        assert_eq!(custom.ssh_tunnels.map(std::num::NonZeroUsize::get), Some(8));
        assert!(custom.ssh_forward_agent);
        assert_eq!(custom.remote_direct_probe_timeout_sec, 5);
        // explicit --remote-transport=ssh skips daemon TLS certs while tcp and auto keep them
        let ssh_req = build_master_remote_request(
            &master_args(&["--remote-transport=ssh"]),
            common::ResolvedFilesInFlight::automatic(),
        )
        .unwrap();
        assert!(!ssh_req.rcpd.encryption);
        let auto_req = build_master_remote_request(
            &master_args(&["--remote-transport=auto"]),
            common::ResolvedFilesInFlight::automatic(),
        )
        .unwrap();
        assert!(auto_req.rcpd.encryption);
    }
}
