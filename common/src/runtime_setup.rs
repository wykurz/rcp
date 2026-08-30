//! Runtime, tracing, and throttle bring-up used by [`crate::run`].
//!
//! This module gathers the process-startup plumbing that configures the
//! tokio runtime, installs the tracing subscriber, primes the throttle
//! replenishers, and wires the adaptive metadata-ops control loops. It is
//! kept separate from the crate root purely to keep `lib.rs` focused on the
//! public surface.

use crate::config::{
    AutoMetaThrottleConfig, ConcurrencyLimit, RuntimeConfig, ThrottleConfig, TracingConfig,
};
use crate::{
    PBAR, PROGRESS, REMOTE_RUNTIME_STATS, RuntimeStats, auto_meta, histogram_logger, is_localhost,
    observability, progress, store_logger_cancel, store_logger_handle, walk,
};
use anyhow::Context;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

struct LocalTimeFormatter;

impl tracing_subscriber::fmt::time::FormatTime for LocalTimeFormatter {
    fn format_time(
        &self,
        writer: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        let now = chrono::Local::now();
        writer.write_str(&now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    }
}

struct ProgWriter {}

impl ProgWriter {
    fn new() -> Self {
        Self {}
    }
}

impl std::io::Write for ProgWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        PBAR.suspend(|| std::io::stdout().write(buf))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}

fn get_hostname() -> String {
    nix::unistd::gethostname()
        .ok()
        .and_then(|os_str| os_str.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string())
}

fn read_env_or_default<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(val) => match val.parse() {
            Ok(val) => val,
            Err(_) => default,
        },
        Err(_) => default,
    }
}

/// collects runtime statistics (CPU time, memory) for the current process
#[must_use]
pub fn collect_runtime_stats() -> RuntimeStats {
    collect_runtime_stats_inner(procfs::process::Process::myself().ok())
}

fn collect_runtime_stats_inner(process: Option<procfs::process::Process>) -> RuntimeStats {
    let Some(process) = process else {
        return RuntimeStats::default();
    };
    collect_runtime_stats_for_process(&process).unwrap_or_default()
}

fn collect_runtime_stats_for_process(
    process: &procfs::process::Process,
) -> anyhow::Result<RuntimeStats> {
    let stat = process.stat()?;
    let clock_ticks = procfs::ticks_per_second() as f64;
    // vmhwm from /proc/[pid]/status is in kB, convert to bytes
    let vmhwm_kb = process.status()?.vmhwm.unwrap_or(0);
    Ok(RuntimeStats {
        cpu_time_user_ms: ((stat.utime as f64 / clock_ticks) * 1000.0) as u64,
        cpu_time_kernel_ms: ((stat.stime as f64 / clock_ticks) * 1000.0) as u64,
        peak_rss_bytes: vmhwm_kb * 1024,
    })
}

fn print_runtime_stats_for_role(prefix: &str, stats: &RuntimeStats) {
    let cpu_total =
        std::time::Duration::from_millis(stats.cpu_time_user_ms + stats.cpu_time_kernel_ms);
    let cpu_kernel = std::time::Duration::from_millis(stats.cpu_time_kernel_ms);
    let cpu_user = std::time::Duration::from_millis(stats.cpu_time_user_ms);
    println!(
        "{prefix}cpu time : {:.2?} | k: {:.2?} | u: {:.2?}",
        cpu_total, cpu_kernel, cpu_user
    );
    println!(
        "{prefix}peak RSS : {}",
        bytesize::ByteSize(stats.peak_rss_bytes)
    );
}

#[rustfmt::skip]
pub(crate) fn print_runtime_stats() -> Result<(), anyhow::Error> {
    // check if we have remote runtime stats (from a remote copy operation)
    let remote_stats = REMOTE_RUNTIME_STATS.lock().unwrap().take();
    if let Some(remote) = remote_stats {
        // print global walltime first
        println!("walltime : {:.2?}", &PROGRESS.get_duration());
        println!();
        let source_is_local = is_localhost(&remote.source_host);
        let dest_is_local = is_localhost(&remote.dest_host);
        // collect master stats
        let master_stats = collect_runtime_stats();
        // print non-localhost roles first
        if !source_is_local {
            println!("SOURCE ({}):", remote.source_host);
            print_runtime_stats_for_role("  ", &remote.source_stats);
            println!();
        }
        if !dest_is_local {
            println!("DESTINATION ({}):", remote.dest_host);
            print_runtime_stats_for_role("  ", &remote.dest_stats);
            println!();
        }
        // print combined localhost section
        match (source_is_local, dest_is_local) {
            (true, true) => {
                println!("MASTER + SOURCE + DESTINATION (localhost):");
                print_runtime_stats_for_role("  master ", &master_stats);
                print_runtime_stats_for_role("  source ", &remote.source_stats);
                print_runtime_stats_for_role("  dest   ", &remote.dest_stats);
            }
            (true, false) => {
                println!("MASTER + SOURCE (localhost):");
                print_runtime_stats_for_role("  master ", &master_stats);
                print_runtime_stats_for_role("  source ", &remote.source_stats);
            }
            (false, true) => {
                println!("MASTER + DESTINATION (localhost):");
                print_runtime_stats_for_role("  master ", &master_stats);
                print_runtime_stats_for_role("  dest   ", &remote.dest_stats);
            }
            (false, false) => {
                println!("MASTER (localhost):");
                print_runtime_stats_for_role("  ", &master_stats);
            }
        }
        return Ok(());
    }
    // local operation - print stats for this process only
    let process = procfs::process::Process::myself()?;
    let stat = process.stat()?;
    // The time is in clock ticks, so we need to convert it to seconds
    let clock_ticks_per_second = procfs::ticks_per_second();
    let ticks_to_duration = |ticks: u64| {
        std::time::Duration::from_secs_f64(ticks as f64 / clock_ticks_per_second as f64)
    };
    // vmhwm from /proc/[pid]/status is in kB, convert to bytes
    let vmhwm_kb = process.status()?.vmhwm.unwrap_or(0);
    println!("walltime : {:.2?}", &PROGRESS.get_duration(),);
    println!("cpu time : {:.2?} | k: {:.2?} | u: {:.2?}", ticks_to_duration(stat.utime + stat.stime), ticks_to_duration(stat.stime), ticks_to_duration(stat.utime));
    println!("peak RSS : {:.2?}", bytesize::ByteSize(vmhwm_kb * 1024));
    Ok(())
}

fn get_soft_open_file_limit() -> Result<u64, std::io::Error> {
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // safety: we pass a valid "rlim" pointer and the result is checked
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut rlim) };
    if result == 0 {
        Ok(rlim.rlim_cur)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[must_use]
pub fn generate_debug_log_filename(prefix: &str) -> String {
    let now = chrono::Utc::now();
    let timestamp = now.format("%Y-%m-%dT%H:%M:%S").to_string();
    let process_id = std::process::id();
    format!("{prefix}-{timestamp}-{process_id}")
}

/// Generate a trace filename with identifier, hostname, PID, and timestamp.
///
/// `identifier` should be "rcp", "rcpd-source", or "rcpd-destination"
#[must_use]
pub fn generate_trace_filename(prefix: &str, identifier: &str, extension: &str) -> String {
    let hostname = get_hostname();
    let pid = std::process::id();
    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S");
    format!("{prefix}-{identifier}-{hostname}-{pid}-{timestamp}.{extension}")
}

/// The tracing target for rcp's own user-facing NOTICES: advice about the invocation that a user
/// must see without having to ask for it with `-v`.
///
/// The verbose-level filter (`build_verbose_env_filter`, below) gives this target its own `warn`
/// directive, which is more specific than the global level, so a notice renders at the DEFAULT
/// verbosity while everything else stays at `error`. That targeting is the whole point. Raising the
/// global default to `warn` instead would unmute every other `warn!` in the tools, 14 of which sit
/// in per-entry paths (e.g. "Skipping directory {:?} - ancestor failed to create"), so one failed
/// subtree would print thousands of lines.
///
/// **The bar for putting an event here** is that it is advice about the INVOCATION — constant in
/// volume however large the tree, and actionable by changing the command line. A per-entry event
/// does not qualify, however important it looks. Current notices cover ACL-preservation settings,
/// deprecated options, explicit concurrency clamps, profiling artifacts, and startup safety
/// fallbacks.
///
/// Three things hold, deliberately:
///
/// - `--quiet` suppresses it, along with everything else — no subscriber is installed at all. That
///   is the supported way to turn a notice off.
/// - `RUST_LOG` does NOT suppress it. The directive is added after `EnvFilter::from_default_env`
///   and replaces any directive for the same target, exactly as the `tokio` / `quinn` / `rustls` /
///   `h2` directives and the verbosity level itself already do. `RUST_LOG` still raises verbosity
///   for targets this filter does not name.
/// - It is a `warn!` and not an `error!`: the operation can still succeed and its exit code is
///   unchanged. A notice is advice; some notices also describe heuristics rather than failures.
pub const NOTICE_TARGET: &str = "rcp::notice";

/// Build the verbose-level [`tracing_subscriber::EnvFilter`] used by every
/// non-profile tracing layer (file, fmt, remote). Excludes noisy deps that are
/// rarely useful when debugging rcp.
fn build_verbose_env_filter(verbose: u8) -> tracing_subscriber::EnvFilter {
    let level_directive = match verbose {
        0 => "error".parse().unwrap(),
        1 => "info".parse().unwrap(),
        2 => "debug".parse().unwrap(),
        _ => "trace".parse().unwrap(),
    };
    tracing_subscriber::EnvFilter::from_default_env()
        .add_directive(level_directive)
        // rcp's own notices, visible at every verbosity including the default. Built from the
        // constant rather than spelled out, so the target and the directive cannot drift apart.
        // This layer builder is shared by the master's console/file layers AND by an rcpd's
        // forwarding layer, so one directive covers both ends of a remote copy: without it here,
        // an rcpd at the default verbosity would never even SEND the notice.
        .add_directive(format!("{NOTICE_TARGET}=warn").parse().unwrap())
        .add_directive("tokio=info".parse().unwrap())
        .add_directive("runtime=info".parse().unwrap())
        .add_directive("quinn=warn".parse().unwrap())
        .add_directive("rustls=warn".parse().unwrap())
        .add_directive("h2=warn".parse().unwrap())
}

/// Build the [`tracing_subscriber::EnvFilter`] used by chrome/flame profile
/// layers. Profiling layers don't share the verbose-level filter because they
/// have their own `--profile-level`. Returns the formatted filter string —
/// callers re-parse it per layer because EnvFilter isn't Clone.
fn build_profile_filter_str(profile_level: Option<&str>) -> anyhow::Result<String> {
    let level_str = profile_level.unwrap_or("trace");
    let valid_levels = ["trace", "debug", "info", "warn", "error", "off"];
    if !valid_levels.contains(&level_str.to_lowercase().as_str()) {
        anyhow::bail!(
            "Invalid --profile-level '{level_str}'. Valid values: trace, debug, info, warn, error, off"
        );
    }
    Ok(format!(
        "tokio=off,quinn=off,h2=off,hyper=off,rustls=off,{level_str}"
    ))
}

/// Guards from chrome/flame tracing layers that must outlive the runtime to
/// flush traces on shutdown. Hold the returned struct for the lifetime of the
/// run.
#[allow(dead_code)] // fields are kept alive only for their Drop side-effects
pub(crate) struct TracingGuards {
    chrome: Option<tracing_chrome::FlushGuard>,
    flame: Option<tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>>>,
}

/// Install the global [`tracing_subscriber`] registry from a [`TracingConfig`].
/// Caller must hold the returned [`TracingGuards`] until the run finishes so
/// that chrome/flame traces are flushed before the file handles close.
///
/// In quiet mode this is a no-op (the subscriber is never installed).
pub(crate) fn install_tracing_subscriber(
    quiet: bool,
    verbose: u8,
    tracing_config: TracingConfig,
) -> anyhow::Result<TracingGuards> {
    if quiet {
        assert!(
            verbose == 0,
            "Quiet mode and verbose mode are mutually exclusive"
        );
        return Ok(TracingGuards {
            chrome: None,
            flame: None,
        });
    }
    let TracingConfig {
        remote_layer: remote_tracing_layer,
        debug_log_file,
        chrome_trace_prefix,
        flamegraph_prefix,
        trace_identifier,
        profile_level,
        tokio_console,
        tokio_console_port,
    } = tracing_config;
    let file_layer = debug_log_file
        .map(|log_file_path| {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_file_path)
                .with_context(|| format!("failed to create debug log file at '{log_file_path}'"))?;
            anyhow::Ok(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_line_number(true)
                    .with_thread_ids(true)
                    .with_timer(LocalTimeFormatter)
                    .with_ansi(false)
                    .with_writer(file)
                    .with_filter(build_verbose_env_filter(verbose)),
            )
        })
        .transpose()?;
    // fmt_layer for local console output (when not using remote tracing)
    let fmt_layer = if remote_tracing_layer.is_some() {
        None
    } else {
        Some(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_line_number(true)
                .with_span_events(if verbose > 2 {
                    FmtSpan::NEW | FmtSpan::CLOSE
                } else {
                    FmtSpan::NONE
                })
                .with_timer(LocalTimeFormatter)
                .pretty()
                .with_writer(ProgWriter::new)
                .with_filter(build_verbose_env_filter(verbose)),
        )
    };
    // apply env_filter to remote_tracing_layer so it respects verbose level
    let remote_tracing_layer =
        remote_tracing_layer.map(|layer| layer.with_filter(build_verbose_env_filter(verbose)));
    let mut startup_notices = Vec::new();
    let mut startup_errors = Vec::new();
    let console_layer = tokio_console.then(|| {
        let console_port = tokio_console_port.unwrap_or(6669);
        let retention_seconds: u64 =
            read_env_or_default("RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS", 60);
        startup_notices.push(format!(
            "Tokio console server listening on 127.0.0.1:{console_port}"
        ));
        console_subscriber::ConsoleLayer::builder()
            .retention(std::time::Duration::from_secs(retention_seconds))
            .server_addr(([127, 0, 0, 1], console_port))
            .spawn()
    });
    // chrome/flame share a profile filter; build the string once and re-parse
    // per layer (EnvFilter isn't Clone).
    let profile_filter_str = (chrome_trace_prefix.is_some() || flamegraph_prefix.is_some())
        .then(|| build_profile_filter_str(profile_level.as_deref()))
        .transpose()?;
    let make_profile_filter =
        || tracing_subscriber::EnvFilter::new(profile_filter_str.as_ref().unwrap());
    let mut chrome_guard = None;
    let chrome_layer = chrome_trace_prefix
        .as_ref()
        .map(|prefix| {
            let filename = generate_trace_filename(prefix, &trace_identifier, "json");
            let file = std::fs::File::create(&filename)
                .with_context(|| format!("failed to create Chrome trace file at '{filename}'"))?;
            startup_notices.push(format!("Chrome trace will be written to: {filename}"));
            let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
                .writer(file)
                .include_args(true)
                .build();
            chrome_guard = Some(guard);
            anyhow::Ok(layer.with_filter(make_profile_filter()))
        })
        .transpose()?;
    let mut flame_guard = None;
    let flame_layer = flamegraph_prefix.as_ref().and_then(|prefix| {
        let filename = generate_trace_filename(prefix, &trace_identifier, "folded");
        match tracing_flame::FlameLayer::with_file(&filename) {
            Ok((layer, guard)) => {
                startup_notices.push(format!("Flamegraph data will be written to: {filename}"));
                flame_guard = Some(guard);
                Some(layer.with_filter(make_profile_filter()))
            }
            Err(e) => {
                startup_errors.push(format!("Failed to create flamegraph layer: {e:?}"));
                None
            }
        }
    });
    tracing_subscriber::registry()
        .with(file_layer)
        .with(fmt_layer)
        .with(remote_tracing_layer)
        .with(console_layer)
        .with(chrome_layer)
        .with(flame_layer)
        .init();
    for notice in startup_notices {
        tracing::warn!(target: NOTICE_TARGET, "{notice}");
    }
    for startup_error in startup_errors {
        // rcp-error-log-allow: startup_error is an already-rendered String, not an error chain
        tracing::error!("{startup_error}");
    }
    Ok(TracingGuards {
        chrome: chrome_guard,
        flame: flame_guard,
    })
}

/// Derive a conservative leaf-operation count from the process descriptor limit.
///
/// Local copy/link can overlap three OpenFile descriptors: source classification plus source and
/// destination data during transfer, or source classification plus destination data and its
/// blocking-metadata duplicate after the source data fd has been dropped. Recursive overwrite
/// removal can concurrently use one classification descriptor per PendingMeta permit. Dividing an
/// 80%-of-soft-`RLIMIT_NOFILE` budget by those four overlapping units gives the same admission count
/// to each independent pool. Metadata-only tools can transiently hold two descriptors per
/// PendingMeta operation, but do not use the three-descriptor OpenFile path concurrently. This is a
/// heuristic for shipped tool workflows, not a hard process-wide ceiling: permission-relaxed and
/// recursive directory handles can remain outside leaf admission, as can process support
/// infrastructure. A nonzero soft limit gets at least one operation so very small test/container
/// limits do not silently disable backpressure.
fn descriptor_admission_limit(soft_limit: std::num::NonZeroU64) -> ConcurrencyLimit {
    const OPEN_FILE_DESCRIPTOR_UNITS: u64 = 3;
    const OVERLAPPING_PENDING_META_DESCRIPTOR_UNITS: u64 = 1;
    const DESCRIPTOR_UNITS_PER_OPERATION: u64 =
        OPEN_FILE_DESCRIPTOR_UNITS + OVERLAPPING_PENDING_META_DESCRIPTOR_UNITS;
    const MAX_LEAF_OPERATIONS_PER_POOL: u64 = 4096;
    let descriptor_budget = soft_limit.get().saturating_mul(8) / 10;
    let leaf_operation_limit = std::cmp::min(
        (descriptor_budget / DESCRIPTOR_UNITS_PER_OPERATION).max(1),
        MAX_LEAF_OPERATIONS_PER_POOL,
    );
    ConcurrencyLimit::Limited(
        std::num::NonZeroUsize::new(
            usize::try_from(leaf_operation_limit).expect("the leaf-operation cap fits in usize"),
        )
        .expect("the nonzero soft limit produces a nonzero admission cap"),
    )
}

fn resolve_leaf_capacity(
    files_in_flight: ConcurrencyLimit,
    descriptor_limit: ConcurrencyLimit,
) -> ConcurrencyLimit {
    files_in_flight.meet(descriptor_limit)
}

#[derive(Debug, Eq, PartialEq)]
enum DescriptorClampVisibility {
    Notice,
    Verbose,
}

#[derive(Debug, Eq, PartialEq)]
struct DescriptorClampDiagnostic {
    visibility: DescriptorClampVisibility,
    message: String,
}

fn descriptor_clamp_diagnostic(
    files_in_flight: crate::ResolvedFilesInFlight,
    effective: ConcurrencyLimit,
) -> Option<DescriptorClampDiagnostic> {
    if files_in_flight.limit() == effective {
        return None;
    }
    let ConcurrencyLimit::Limited(effective) = effective else {
        return None;
    };
    let diagnostic = match (files_in_flight.source(), files_in_flight.limit()) {
        (crate::FilesInFlightSource::Automatic, requested) => DescriptorClampDiagnostic {
            visibility: DescriptorClampVisibility::Verbose,
            message: format!(
                "Automatic file admission was reduced by descriptor safety: requested={requested}, effective={}",
                ConcurrencyLimit::Limited(effective),
            ),
        },
        (crate::FilesInFlightSource::Explicit, ConcurrencyLimit::Limited(requested)) => {
            DescriptorClampDiagnostic {
                visibility: DescriptorClampVisibility::Notice,
                message: format!(
                    "Requested --max-files-in-flight={requested}, but descriptor safety reduced endpoint file admission to {effective}"
                ),
            }
        }
        (
            crate::FilesInFlightSource::DeprecatedMaxOpenFiles,
            ConcurrencyLimit::Limited(requested),
        ) => DescriptorClampDiagnostic {
            visibility: DescriptorClampVisibility::Notice,
            message: format!(
                "Requested --max-open-files={requested}, but descriptor safety reduced endpoint file admission to {effective}"
            ),
        },
        (crate::FilesInFlightSource::DeprecatedMaxOpenFiles, ConcurrencyLimit::Unlimited) => {
            DescriptorClampDiagnostic {
                visibility: DescriptorClampVisibility::Notice,
                message: format!(
                    "Requested unlimited file admission with --max-open-files=0, but descriptor safety reduced endpoint file admission to {effective}"
                ),
            }
        }
        (crate::FilesInFlightSource::Explicit, ConcurrencyLimit::Unlimited) => {
            DescriptorClampDiagnostic {
                visibility: DescriptorClampVisibility::Notice,
                message: format!(
                    "Requested unlimited file admission with --max-files-in-flight=unlimited, but descriptor safety reduced endpoint file admission to {effective}"
                ),
            }
        }
    };
    Some(diagnostic)
}

fn configure_leaf_admission_limit(capacity: ConcurrencyLimit) -> anyhow::Result<()> {
    let capacity = match capacity {
        ConcurrencyLimit::Unlimited => None,
        ConcurrencyLimit::Limited(value) => Some(value),
    };
    throttle::try_set_admission_limits(capacity)
        .context("failed to configure runtime file admission")
}

fn configure_file_admission(
    throttle: &ThrottleConfig,
    get_soft_limit: impl FnOnce() -> Result<u64, std::io::Error>,
    configure_limit: impl FnOnce(ConcurrencyLimit) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    if !throttle.apply_files_in_flight {
        return Ok(());
    }
    let descriptor_limit = match get_soft_limit() {
        Ok(soft_limit) => {
            descriptor_admission_limit(std::num::NonZeroU64::new(soft_limit).context(
                "soft RLIMIT_NOFILE is zero; descriptor-safe file admission is impossible",
            )?)
        }
        Err(error) => match (
            throttle.files_in_flight.source(),
            throttle.files_in_flight.limit(),
        ) {
            (
                source @ (crate::FilesInFlightSource::Explicit
                | crate::FilesInFlightSource::DeprecatedMaxOpenFiles),
                ConcurrencyLimit::Limited(limit),
            ) => {
                let option = match source {
                    crate::FilesInFlightSource::Explicit => "--max-files-in-flight",
                    crate::FilesInFlightSource::DeprecatedMaxOpenFiles => "--max-open-files",
                    crate::FilesInFlightSource::Automatic => unreachable!("matched above"),
                };
                tracing::warn!(
                    target: NOTICE_TARGET,
                    "Failed to query RLIMIT_NOFILE; using {option}={limit} as the endpoint file-admission ceiling without an independent descriptor-safety ceiling: {error:#}"
                );
                ConcurrencyLimit::Unlimited
            }
            _ => {
                return Err(error).context(
                    "failed to query rlimit; automatic or unlimited file admission requires descriptor safety",
                );
            }
        },
    };
    let leaf_capacity = resolve_leaf_capacity(throttle.files_in_flight.limit(), descriptor_limit);
    tracing::info!(
        "Resolved file admission: file_ceiling={:?}, source={:?}, descriptor_ceiling={:?}, open_file={:?}, pending_meta={:?}",
        throttle.files_in_flight.limit(),
        throttle.files_in_flight.source(),
        descriptor_limit,
        leaf_capacity,
        leaf_capacity,
    );
    if let Some(diagnostic) = descriptor_clamp_diagnostic(throttle.files_in_flight, leaf_capacity) {
        match diagnostic.visibility {
            DescriptorClampVisibility::Notice => {
                tracing::warn!(target: NOTICE_TARGET, "{}", diagnostic.message);
            }
            DescriptorClampVisibility::Verbose => {
                tracing::info!("{}", diagnostic.message);
            }
        }
    }
    configure_limit(leaf_capacity)
}

/// Build a multi-threaded tokio runtime configured per `runtime`, and apply the
/// file-like work and descriptor-safe admission limits from `throttle`.
pub(crate) fn build_tokio_runtime(
    runtime: &RuntimeConfig,
    throttle: &ThrottleConfig,
) -> anyhow::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if runtime.max_workers > 0 {
        builder.worker_threads(runtime.max_workers);
    }
    if runtime.max_blocking_threads > 0 {
        builder.max_blocking_threads(runtime.max_blocking_threads);
    }
    let runtime = builder.build().context("failed to create Tokio runtime")?;
    configure_file_admission(
        throttle,
        get_soft_open_file_limit,
        configure_leaf_admission_limit,
    )?;
    Ok(runtime)
}

#[cfg(test)]
mod default_leaf_operation_limit_tests {
    use super::*;

    #[derive(Clone, Default)]
    struct CapturedLogs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    struct CapturedLogWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedLogWriter(self.0.clone())
        }
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    #[cfg(target_os = "linux")]
    const RLIMIT_CHILD_MARKER: &str = "RCP_TEST_RUNTIME_SETUP_RLIMIT_CHILD";
    #[cfg(target_os = "linux")]
    const RLIMIT_CHILD_MARKER_VALUE: &str = "preserve-session-soft-limit-v1";
    #[cfg(target_os = "linux")]
    const RLIMIT_CHILD_SUCCESS: &str = "RCP_TEST_RUNTIME_SETUP_RLIMIT_CHILD:success";
    #[cfg(target_os = "linux")]
    const RLIMIT_CHILD_SKIP: &str = "RCP_TEST_RUNTIME_SETUP_RLIMIT_CHILD:skip";
    #[cfg(target_os = "linux")]
    const UNLIMITED_FILE_CEILING_CHILD_MARKER: &str =
        "RCP_TEST_RUNTIME_SETUP_UNLIMITED_FILE_CEILING_CHILD";
    #[cfg(target_os = "linux")]
    const UNLIMITED_FILE_CEILING_CHILD_MARKER_VALUE: &str =
        "uses-descriptor-safety-after-stale-admission-epoch-v1";
    #[cfg(target_os = "linux")]
    const UNLIMITED_FILE_CEILING_CHILD_SUCCESS: &str =
        "RCP_TEST_RUNTIME_SETUP_UNLIMITED_FILE_CEILING_CHILD:success";

    #[cfg(target_os = "linux")]
    fn nofile_limit() -> libc::rlimit {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // safety: `limit` is valid for writes and the return value is checked
        let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) };
        assert_eq!(
            result,
            0,
            "getrlimit failed: {:#}",
            std::io::Error::last_os_error()
        );
        limit
    }

    #[test]
    fn reserves_descriptor_headroom_for_each_leaf_operation() {
        let nonzero = |value| std::num::NonZeroU64::new(value).unwrap();
        assert_eq!(
            descriptor_admission_limit(nonzero(100)),
            ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(20).unwrap())
        );
        assert_eq!(
            descriptor_admission_limit(nonzero(4096)),
            ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(819).unwrap())
        );
        assert_eq!(
            descriptor_admission_limit(nonzero(1_000_000)),
            ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(4096).unwrap())
        );
        assert_eq!(
            descriptor_admission_limit(nonzero(u64::MAX)),
            ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(4096).unwrap())
        );
    }

    #[test]
    fn keeps_a_small_nonzero_limit_live() {
        let one = ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(1).unwrap());
        assert_eq!(
            descriptor_admission_limit(std::num::NonZeroU64::new(1).unwrap()),
            one
        );
        assert_eq!(
            descriptor_admission_limit(std::num::NonZeroU64::new(4).unwrap()),
            one
        );
    }

    #[test]
    fn file_and_descriptor_ceilings_are_intersected() {
        let limited =
            |value| ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(value).unwrap());
        assert_eq!(resolve_leaf_capacity(limited(8), limited(40)), limited(8));
        assert_eq!(resolve_leaf_capacity(limited(80), limited(40)), limited(40));
        assert_eq!(
            resolve_leaf_capacity(ConcurrencyLimit::Unlimited, limited(40)),
            limited(40)
        );
        assert_eq!(
            resolve_leaf_capacity(limited(8), ConcurrencyLimit::Unlimited),
            limited(8)
        );
    }

    #[test]
    fn descriptor_clamp_diagnostics_preserve_file_limit_provenance() {
        let eight = std::num::NonZeroUsize::new(8).unwrap();
        let four = std::num::NonZeroUsize::new(4).unwrap();
        let effective = ConcurrencyLimit::Limited(four);
        assert_eq!(
            descriptor_clamp_diagnostic(
                crate::ResolvedFilesInFlight::legacy(eight.get()),
                effective
            ),
            Some(DescriptorClampDiagnostic {
                visibility: DescriptorClampVisibility::Notice,
                message: "Requested --max-open-files=8, but descriptor safety reduced endpoint file admission to 4".to_string(),
            })
        );
        assert_eq!(
            descriptor_clamp_diagnostic(crate::ResolvedFilesInFlight::legacy(0), effective),
            Some(DescriptorClampDiagnostic {
                visibility: DescriptorClampVisibility::Notice,
                message: "Requested unlimited file admission with --max-open-files=0, but descriptor safety reduced endpoint file admission to 4".to_string(),
            })
        );
        assert_eq!(
            descriptor_clamp_diagnostic(crate::ResolvedFilesInFlight::unlimited(), effective),
            Some(DescriptorClampDiagnostic {
                visibility: DescriptorClampVisibility::Notice,
                message: "Requested unlimited file admission with --max-files-in-flight=unlimited, but descriptor safety reduced endpoint file admission to 4".to_string(),
            })
        );
        assert_eq!(
            descriptor_clamp_diagnostic(
                crate::ResolvedFilesInFlight::automatic_with(eight), effective
            ),
            Some(DescriptorClampDiagnostic {
                visibility: DescriptorClampVisibility::Verbose,
                message: "Automatic file admission was reduced by descriptor safety: requested=8, effective=4".to_string(),
            })
        );
    }

    #[test]
    fn runtime_admission_setup_propagates_oversized_capacity() {
        let oversized = ConcurrencyLimit::Limited(
            std::num::NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS + 1).unwrap(),
        );

        let error = configure_leaf_admission_limit(oversized)
            .expect_err("oversized leaf admission must return an error");

        assert!(
            error
                .downcast_ref::<throttle::AdmissionCapacityError>()
                .is_some(),
            "runtime setup must preserve the typed throttle error: {error:#}"
        );
    }

    #[test]
    fn disabled_file_admission_does_not_query_or_configure_limits() {
        let throttle = ThrottleConfig {
            apply_files_in_flight: false,
            ..ThrottleConfig::default()
        };
        let mut queried = false;
        let mut configured = false;
        configure_file_admission(
            &throttle,
            || {
                queried = true;
                Err(std::io::Error::other(
                    "disabled admission must not query rlimit",
                ))
            },
            |_| {
                configured = true;
                Ok(())
            },
        )
        .expect("disabled file admission must not touch runtime admission state");

        assert!(!queried, "disabled file admission queried rlimit");
        assert!(
            !configured,
            "disabled file admission reconfigured throttle pools"
        );
    }

    #[test]
    fn finite_user_file_limits_recover_from_rlimit_query_failure() {
        let eight = std::num::NonZeroUsize::new(8).unwrap();
        let expected = ConcurrencyLimit::Limited(eight);
        let captured = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_ansi(false)
            .without_time()
            .with_target(true)
            .with_max_level(tracing::Level::TRACE)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            for files_in_flight in [
                crate::ResolvedFilesInFlight::explicit(eight),
                crate::ResolvedFilesInFlight::forwarded_legacy(eight.get()),
            ] {
                let throttle = ThrottleConfig {
                    files_in_flight,
                    ..ThrottleConfig::default()
                };
                let mut configured = None;

                configure_file_admission(
                    &throttle,
                    || Err(std::io::Error::other("sentinel rlimit failure")),
                    |capacity| {
                        configured = Some(capacity);
                        Ok(())
                    },
                )
                .expect("a finite user ceiling must remain usable when getrlimit fails");

                assert_eq!(configured, Some(expected));
            }
        });

        let logs = captured.contents();
        for option in ["--max-files-in-flight=8", "--max-open-files=8"] {
            assert!(
                logs.lines().any(|line| {
                    line.contains(" WARN ")
                        && line.contains("rcp::notice")
                        && line.contains(option)
                        && line.contains("sentinel rlimit failure")
                }),
                "missing default-visible rlimit fallback notice for {option}: {logs}"
            );
        }
    }

    #[test]
    fn automatic_file_limit_does_not_bypass_rlimit_query_failure() {
        let mut configured = false;
        let error = configure_file_admission(
            &ThrottleConfig::default(),
            || Err(std::io::Error::other("sentinel rlimit failure")),
            |_| {
                configured = true;
                Ok(())
            },
        )
        .expect_err("an automatic ceiling must retain descriptor-safety validation");

        assert!(error.to_string().contains("failed to query rlimit"));
        assert!(
            !configured,
            "failed automatic validation configured admission"
        );
    }

    #[test]
    fn unlimited_file_limit_does_not_bypass_rlimit_query_failure() {
        let throttle = ThrottleConfig {
            files_in_flight: crate::ResolvedFilesInFlight::forwarded_legacy(0),
            ..ThrottleConfig::default()
        };
        let mut configured = false;
        let error = configure_file_admission(
            &throttle,
            || Err(std::io::Error::other("sentinel rlimit failure")),
            |_| {
                configured = true;
                Ok(())
            },
        )
        .expect_err("an unlimited ceiling must retain descriptor-safety validation");

        assert!(error.to_string().contains("failed to query rlimit"));
        assert!(
            !configured,
            "failed unlimited validation configured admission"
        );
    }

    #[test]
    fn zero_soft_descriptor_limit_fails_closed_for_every_file_policy() {
        let eight = std::num::NonZeroUsize::new(8).unwrap();
        for files_in_flight in [
            crate::ResolvedFilesInFlight::automatic_with(eight),
            crate::ResolvedFilesInFlight::explicit(eight),
            crate::ResolvedFilesInFlight::forwarded_legacy(0),
        ] {
            let throttle = ThrottleConfig {
                files_in_flight,
                ..ThrottleConfig::default()
            };
            let mut configured = false;
            let error = configure_file_admission(
                &throttle,
                || Ok(0),
                |_| {
                    configured = true;
                    Ok(())
                },
            )
            .expect_err("a zero soft descriptor limit must fail closed");

            assert!(error.to_string().contains("soft RLIMIT_NOFILE is zero"));
            assert!(!configured, "zero descriptor safety configured admission");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn build_tokio_runtime_unlimited_file_ceiling_uses_descriptor_safety_after_stale_epoch() {
        let is_child = std::env::var_os(UNLIMITED_FILE_CEILING_CHILD_MARKER).is_some_and(|value| {
            value == std::ffi::OsStr::new(UNLIMITED_FILE_CEILING_CHILD_MARKER_VALUE)
        });
        if !is_child {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime_setup::default_leaf_operation_limit_tests::build_tokio_runtime_unlimited_file_ceiling_uses_descriptor_safety_after_stale_epoch",
                    "--nocapture",
                ])
                .env(
                    UNLIMITED_FILE_CEILING_CHILD_MARKER,
                    UNLIMITED_FILE_CEILING_CHILD_MARKER_VALUE,
                )
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "child test failed:\n{stdout}\n{stderr}"
            );
            assert!(
                stdout.contains(UNLIMITED_FILE_CEILING_CHILD_SUCCESS)
                    || stderr.contains(UNLIMITED_FILE_CEILING_CHILD_SUCCESS),
                "child branch did not emit its sentinel:\n{stdout}\n{stderr}"
            );
            return;
        }
        let runtime = RuntimeConfig {
            max_workers: 1,
            max_blocking_threads: 1,
        };
        let old_limit = ThrottleConfig {
            files_in_flight: crate::ResolvedFilesInFlight::explicit(
                std::num::NonZeroUsize::new(1).unwrap(),
            ),
            ..ThrottleConfig::default()
        };
        let admission_runtime =
            build_tokio_runtime(&runtime, &old_limit).expect("runtime setup must succeed");
        let (old_open_file, old_pending_meta) = admission_runtime.block_on(async {
            tokio::join!(
                throttle::open_file_permit(),
                throttle::pending_meta_permit()
            )
        });
        let unlimited_file_ceiling = ThrottleConfig {
            files_in_flight: crate::ResolvedFilesInFlight::unlimited(),
            ..ThrottleConfig::default()
        };
        drop(
            build_tokio_runtime(&runtime, &unlimited_file_ceiling)
                .expect("runtime setup must succeed"),
        );
        let (new_open_file, new_pending_meta) = admission_runtime.block_on(async {
            let open_file = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                throttle::open_file_permit(),
            )
            .await
            .expect("unlimited file ceiling must use descriptor safety instead of a stale OpenFile epoch");
            let pending_meta = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                throttle::pending_meta_permit(),
            )
            .await
            .expect("unlimited file ceiling must use descriptor safety instead of a stale PendingMeta epoch");
            (open_file, pending_meta)
        });
        drop((
            new_open_file,
            new_pending_meta,
            old_open_file,
            old_pending_meta,
        ));
        throttle::set_admission_limits(None);
        println!("{UNLIMITED_FILE_CEILING_CHILD_SUCCESS}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn build_tokio_runtime_preserves_session_soft_limit_with_unlimited_file_ceiling() {
        let is_child = std::env::var_os(RLIMIT_CHILD_MARKER)
            .is_some_and(|value| value == std::ffi::OsStr::new(RLIMIT_CHILD_MARKER_VALUE));
        if !is_child {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime_setup::default_leaf_operation_limit_tests::build_tokio_runtime_preserves_session_soft_limit_with_unlimited_file_ceiling",
                    "--nocapture",
                ])
                .env(RLIMIT_CHILD_MARKER, RLIMIT_CHILD_MARKER_VALUE)
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "child test failed:\n{stdout}\n{stderr}"
            );
            assert!(
                stdout.contains(RLIMIT_CHILD_SUCCESS)
                    || stderr.contains(RLIMIT_CHILD_SUCCESS)
                    || stdout.contains(RLIMIT_CHILD_SKIP)
                    || stderr.contains(RLIMIT_CHILD_SKIP),
                "child branch did not emit its sentinel:\n{stdout}\n{stderr}"
            );
            return;
        }
        let original = nofile_limit();
        const TARGET_SOFT_LIMIT: libc::rlim_t = 256;
        if original.rlim_max < TARGET_SOFT_LIMIT {
            eprintln!(
                "{RLIMIT_CHILD_SKIP}: current={} hard={}",
                original.rlim_cur, original.rlim_max
            );
            return;
        }
        let lowered = libc::rlimit {
            rlim_cur: TARGET_SOFT_LIMIT,
            rlim_max: original.rlim_max,
        };
        // safety: `lowered` is a valid read-only limit and affects only this child process
        let result = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const lowered) };
        assert_eq!(
            result,
            0,
            "setrlimit failed: {:#}",
            std::io::Error::last_os_error()
        );
        let before = nofile_limit();
        assert_eq!(before.rlim_cur, TARGET_SOFT_LIMIT);
        assert_eq!(before.rlim_max, original.rlim_max);
        let runtime = RuntimeConfig {
            max_workers: 1,
            max_blocking_threads: 1,
        };
        let throttle = ThrottleConfig {
            files_in_flight: crate::ResolvedFilesInFlight::unlimited(),
            ..ThrottleConfig::default()
        };
        let runtime = build_tokio_runtime(&runtime, &throttle).expect("runtime setup must succeed");
        let after = nofile_limit();
        assert_eq!(
            after.rlim_cur, TARGET_SOFT_LIMIT,
            "runtime setup changed the session soft limit"
        );
        assert_eq!(
            after.rlim_max, original.rlim_max,
            "runtime setup changed the hard limit"
        );
        let (open_files, pending_meta) = runtime
            .block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    const EXPECTED_ADMISSION_LIMIT: usize = 51;
                    let mut open_files = Vec::with_capacity(EXPECTED_ADMISSION_LIMIT);
                    let mut pending_meta = Vec::with_capacity(EXPECTED_ADMISSION_LIMIT);
                    for _ in 0..EXPECTED_ADMISSION_LIMIT {
                        open_files.push(throttle::open_file_permit().await);
                    }
                    for _ in 0..EXPECTED_ADMISSION_LIMIT {
                        pending_meta.push(throttle::pending_meta_permit().await);
                    }
                    assert!(
                        tokio::time::timeout(
                            std::time::Duration::from_secs(1),
                            throttle::open_file_permit(),
                        )
                        .await
                        .is_err(),
                        "the 52nd OpenFile acquisition must wait at the derived limit"
                    );
                    assert!(
                        tokio::time::timeout(
                            std::time::Duration::from_secs(1),
                            throttle::pending_meta_permit(),
                        )
                        .await
                        .is_err(),
                        "the 52nd PendingMeta acquisition must wait at the derived limit"
                    );
                    (open_files, pending_meta)
                })
                .await
            })
            .expect("derived admission boundary must complete within its watchdog");
        drop((open_files, pending_meta));
        throttle::set_admission_limits(None);
        println!("{RLIMIT_CHILD_SUCCESS}");
    }
}

/// Spawn the ops/iops throttle replenisher tasks onto `runtime` if the
/// throttles are enabled.
///
/// When `auto_meta` is set, the ops-throttle is forced to a fixed 100ms
/// replenish interval (matching the constant the auto-meta adapter uses
/// when converting `Decision::rate_per_sec` → tokens-per-interval) *and*
/// is bootstrapped even if `--ops-throttle` was zero. That way:
///
/// 1. a future rate-aware controller's `rate_per_sec: Some(_)` decisions
///    actually gate ops instead of silently no-opping;
/// 2. the adapter's 100ms conversion assumption matches the thread's
///    real interval, regardless of the user's static `--ops-throttle`
///    value.
pub(crate) fn spawn_throttle_replenishers(
    runtime: &tokio::runtime::Runtime,
    throttle: &ThrottleConfig,
    trace_identifier: &str,
    histogram_log_file: Option<std::fs::File>,
) {
    fn get_replenish_interval(replenish: usize) -> (usize, std::time::Duration) {
        let mut replenish = replenish;
        let mut interval = std::time::Duration::from_secs(1);
        while replenish > 100 && interval > std::time::Duration::from_millis(1) {
            replenish /= 10;
            interval /= 10;
        }
        (replenish, interval)
    }
    let auto_meta_on = throttle.auto_meta.is_some();
    if auto_meta_on {
        // Force the fixed 100ms cadence the adapter assumes. Bootstrap
        // with at least 1 token so `setup()` enables the semaphore; if
        // the user didn't pass `--ops-throttle`, immediately disable —
        // the adapter re-enables only when a rate decision arrives.
        let interval = std::time::Duration::from_millis(100);
        let initial_replenish = (throttle.ops_throttle as f64 * 0.1) as usize;
        throttle::init_ops_tokens(initial_replenish.max(1));
        if throttle.ops_throttle == 0 {
            throttle::disable_ops_throttle();
        }
        runtime.spawn(throttle::run_ops_replenish_thread(
            initial_replenish,
            interval,
        ));
    } else if throttle.ops_throttle > 0 {
        let (replenish, interval) = get_replenish_interval(throttle.ops_throttle);
        throttle::init_ops_tokens(replenish);
        runtime.spawn(throttle::run_ops_replenish_thread(replenish, interval));
    }
    if throttle.iops_throttle > 0 {
        let (replenish, interval) = get_replenish_interval(throttle.iops_throttle);
        throttle::init_iops_tokens(replenish);
        runtime.spawn(throttle::run_iops_replenish_thread(replenish, interval));
    }
    if let Some(auto) = throttle.auto_meta {
        spawn_auto_meta_throttle(
            runtime,
            auto,
            throttle.histogram_enabled,
            histogram_log_file,
            throttle.histogram_interval,
            trace_identifier,
        );
    }
}

/// Compute the per-tool resolved log path by inserting `trace_identifier`
/// between the user-supplied stem and extension. Mirrors the
/// chrome_trace_prefix convention so master and rcpds don't collide on
/// localhost runs.
///
/// Handles three edge cases consistently with the startup preparer:
/// - bare filename (`foo.hdr`): parent → `.`
/// - no extension (`foo`): extension → `hdr`
/// - no stem (`.hidden`): stem → `auto-meta`
///
/// Non-UTF-8 stem and extension components (valid on Unix) are preserved
/// unchanged; only genuinely absent components fall back to defaults.
fn resolve_log_path(path: &std::path::Path, trace_identifier: &str) -> std::path::PathBuf {
    let parent = match path.parent() {
        Some(p) if p.as_os_str().is_empty() => std::path::Path::new("."),
        Some(p) => p,
        None => std::path::Path::new("."),
    };
    let mut name: std::ffi::OsString = path
        .file_stem()
        .map(|s| s.to_os_string())
        .unwrap_or_else(|| std::ffi::OsString::from("auto-meta"));
    name.push(".");
    name.push(trace_identifier);
    name.push(".");
    match path.extension() {
        Some(e) => name.push(e),
        None => name.push("hdr"),
    }
    parent.join(name)
}

/// Open the resolved histogram log once and return the file that the logger will write.
///
/// Called before the runtime starts so path and permission failures remain startup errors. The
/// returned descriptor stays bound to this exact file through the logger's lifetime; no later
/// pathname re-open can be redirected to a replacement. `O_NOFOLLOW` rejects a symlink in the
/// final component at the authoritative open.
pub(crate) fn prepare_histogram_log_file(
    throttle: &ThrottleConfig,
    trace_identifier: &str,
) -> Result<Option<std::fs::File>, String> {
    let Some(path) = &throttle.histogram_log_path else {
        return Ok(None);
    };
    if path.file_name().is_none() {
        return Err(format!(
            "--auto-meta-histogram-log {path:?} has no filename component",
        ));
    }
    let resolved = resolve_log_path(path, trace_identifier);
    let mut open_options = std::fs::OpenOptions::new();
    open_options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        open_options.custom_flags(libc::O_NOFOLLOW);
    }
    match open_options.open(&resolved) {
        Ok(file) => Ok(Some(file)),
        Err(err) => {
            // ELOOP from O_NOFOLLOW reads as "too many levels of symbolic links"
            // — make it explicit that this rejection is intentional security.
            #[cfg(unix)]
            let context = if err.raw_os_error() == Some(libc::ELOOP) {
                " (resolved path is a symlink, which would let a local attacker hijack the write)"
            } else {
                ""
            };
            #[cfg(not(unix))]
            let context = "";
            Err(format!(
                "--auto-meta-histogram-log cannot create resolved path {resolved:?}: {err:#}{context}",
            ))
        }
    }
}

/// Stable label for a `(Side, MetadataOp)` controller.
///
/// Naming rule:
/// - **Lookups** (`Stat`, `ReadLink`) happen on either filesystem, so
///   the label always carries an explicit `src-` / `dst-` prefix to
///   disambiguate. Example: `src-stat`, `dst-read-link`.
/// - **Mutations and `open(O_CREAT)`** only ever occur on the
///   destination side (sources are immutable in copy/cmp/link/rm).
///   These labels drop the side prefix entirely. Example: `mkdir`,
///   `unlink`, `rmdir`, `hard-link`, `symlink`, `chmod`, `open-create`.
///
/// The result: single-filesystem tools like `rrm` show clean labels
/// (`src-stat`, `unlink`, `rmdir`) instead of the prior misleading
/// `meta-src` / `meta-dst` framing — there is no second filesystem to
/// distinguish from. Dual-filesystem tools (rcp, rcmp, rlink) still
/// disambiguate the two stat / read-link controllers cleanly.
///
/// Implemented as a `const fn` over a fixed match table so the label
/// set is a compile-time constant — no allocation, no `Box::leak`, and
/// no per-`run()` accumulation when callers invoke the runtime more
/// than once in a single process.
const fn unit_label(side: congestion::Side, op: congestion::MetadataOp) -> &'static str {
    use congestion::MetadataOp::*;
    use congestion::Side::*;
    match (side, op) {
        // Lookups: prefix with side because both sides exercise them.
        (Source, Stat) => "src-stat",
        (Destination, Stat) => "dst-stat",
        (Source, ReadLink) => "src-read-link",
        (Destination, ReadLink) => "dst-read-link",
        // Destination-only ops: no prefix in the active case. The
        // (Source, op) slot is wired but never sees a sample under
        // normal operation; the renderer hides it. The `src-` label is
        // kept so any debugging surface still disambiguates the slot
        // from the active destination one if it ever fires.
        (Destination, MkDir) => "mkdir",
        (Source, MkDir) => "src-mkdir",
        (Destination, RmDir) => "rmdir",
        (Source, RmDir) => "src-rmdir",
        (Destination, Unlink) => "unlink",
        (Source, Unlink) => "src-unlink",
        (Destination, HardLink) => "hard-link",
        (Source, HardLink) => "src-hard-link",
        (Destination, Symlink) => "symlink",
        (Source, Symlink) => "src-symlink",
        (Destination, Chmod) => "chmod",
        (Source, Chmod) => "src-chmod",
        (Destination, OpenCreate) => "open-create",
        (Source, OpenCreate) => "src-open-create",
    }
}

fn build_histogram_header(
    auto: &AutoMetaThrottleConfig,
    tool_name: &str,
    snapshot_interval: std::time::Duration,
) -> congestion::format::LogHeader {
    use congestion::format::{AutoMetaSnapshot, HdrSnapshot, LogHeader, UnitLabel};
    let hostname = get_hostname();
    let mut unit_labels = Vec::with_capacity(congestion::N_META_RESOURCES);
    for &side in &congestion::Side::ALL {
        for &op in &congestion::MetadataOp::ALL {
            unit_labels.push(UnitLabel {
                side: side as u8,
                op: op as u8,
                label: unit_label(side, op).to_string(),
            });
        }
    }
    LogHeader {
        format_version: congestion::format::FORMAT_VERSION,
        tool: tool_name.to_string(),
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        hostname,
        pid: std::process::id(),
        start_unix_micros: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
            .unwrap_or(0),
        snapshot_interval_micros: u64::try_from(snapshot_interval.as_micros()).unwrap_or(u64::MAX),
        auto_meta: AutoMetaSnapshot {
            initial_cwnd: auto.initial_cwnd,
            min_cwnd: auto.min_cwnd,
            max_cwnd: auto.max_cwnd,
            alpha: auto.alpha,
            beta: auto.beta,
            increase_step: auto.increase_step,
            decrease_step: auto.decrease_step,
            baseline_percentile: auto.baseline_percentile,
            current_percentile: auto.current_percentile,
            long_window_micros: u64::try_from(auto.long_window.as_micros()).unwrap_or(u64::MAX),
            short_window_micros: u64::try_from(auto.short_window.as_micros()).unwrap_or(u64::MAX),
            tick_interval_micros: u64::try_from(auto.tick_interval.as_micros()).unwrap_or(u64::MAX),
        },
        hdr: HdrSnapshot {
            lowest_discernible_micros: congestion::HDR_LOWEST_DISCERNIBLE_MICROS,
            highest_trackable_micros: congestion::HDR_HIGHEST_TRACKABLE_MICROS,
            significant_figures: congestion::HDR_SIGNIFICANT_FIGURES,
            unit: "microseconds".into(),
        },
        unit_labels,
    }
}

/// Wire up the adaptive metadata-ops control loops — one per
/// `(Side, MetadataOp)` pair (18 in total):
///
/// 1. Seed every resource's `OPS_IN_FLIGHT_LIMIT_*` semaphore with the
///    controller's initial cwnd so the first probe on any resource
///    finds a permit available.
/// 2. Install one `RoutingSink` that fans metadata samples out to per-
///    `(side, op)` channels, each consumed by its own
///    `ControlUnit<RatioController>`. Each syscall on each side gets
///    an independent latency baseline and an independent cwnd, so a
///    saturated `unlink` path doesn't drag down `stat` (or vice versa).
/// 3. Spawn one combined adapter/monitor task per resource. By
///    convention `(Destination, Stat)` is the rate-driver — the global
///    `OPS_THROTTLE` is shared, so only one adapter may translate rate
///    decisions; all others apply concurrency only. The current
///    `RatioController` doesn't emit rate decisions, so the choice is
///    forward-looking.
/// 4. Each adapter exits cleanly when its control unit stops
///    publishing decisions, so they don't leak as unbounded background
///    loops.
///
/// Only one auto-meta config is supported per process. If a sample sink
/// was already installed, it is silently replaced.
fn spawn_auto_meta_throttle(
    runtime: &tokio::runtime::Runtime,
    auto: AutoMetaThrottleConfig,
    histogram_enabled: bool,
    histogram_log_file: Option<std::fs::File>,
    histogram_interval: std::time::Duration,
    trace_identifier: &str,
) {
    let initial_cwnd = auto
        .initial_cwnd
        .clamp(auto.min_cwnd.max(1), auto.max_cwnd.max(1));
    let histogram_active = histogram_enabled || histogram_log_file.is_some();

    // Build receivers + accumulators in parallel arrays so we can pass
    // each accumulator both to a ControlUnit and to a LoggerUnit.
    let mut builder = congestion::RoutingSinkBuilder::new();
    struct Slot {
        label: &'static str,
        side: congestion::Side,
        op: congestion::MetadataOp,
        sample_rx: tokio::sync::mpsc::Receiver<congestion::Sample>,
        apply_rate: bool,
        accumulator: Option<std::sync::Arc<std::sync::Mutex<congestion::HistogramAccumulator>>>,
    }
    let mut slots: Vec<Slot> = Vec::with_capacity(congestion::N_META_RESOURCES);
    for &side in &congestion::Side::ALL {
        for &op in &congestion::MetadataOp::ALL {
            let resource = walk::meta_resource(side, op);
            throttle::set_max_ops_in_flight(resource, initial_cwnd as usize);
            let rx = builder.metadata_receiver(side, op);
            let apply_rate = matches!(
                (side, op),
                (congestion::Side::Destination, congestion::MetadataOp::Stat),
            );
            let accumulator = if histogram_active {
                let acc = std::sync::Arc::new(std::sync::Mutex::new(
                    congestion::HistogramAccumulator::new(),
                ));
                builder.metadata_histogram(side, op, acc.clone());
                Some(acc)
            } else {
                None
            };
            slots.push(Slot {
                label: unit_label(side, op),
                side,
                op,
                sample_rx: rx,
                apply_rate,
                accumulator,
            });
        }
    }
    let sink = std::sync::Arc::new(builder.build());
    congestion::install_sample_sink(sink.clone());

    // Per-unit watch senders for the live histogram panel; collected into
    // a parallel vec so we can also build the logger's `LoggerUnit` list.
    let mut logger_units: Vec<histogram_logger::LoggerUnit> = Vec::new();
    for slot in slots {
        let controller = congestion::RatioController::new(congestion::RatioConfig {
            initial_cwnd: auto.initial_cwnd,
            min_cwnd: auto.min_cwnd,
            max_cwnd: auto.max_cwnd,
            alpha: auto.alpha,
            beta: auto.beta,
            increase_step: auto.increase_step,
            decrease_step: auto.decrease_step,
            baseline_percentile: auto.baseline_percentile,
            current_percentile: auto.current_percentile,
            long_window: auto.long_window,
            short_window: auto.short_window,
        });
        let (unit, decision_rx, snapshot_rx) = congestion::ControlUnit::new(
            slot.label,
            controller,
            slot.sample_rx,
            auto.tick_interval,
        );
        observability::register_unit(slot.label, snapshot_rx);
        if let Some(acc) = slot.accumulator.as_ref() {
            let (snap_tx, snap_rx) = tokio::sync::watch::channel(
                hdrhistogram::Histogram::<u64>::new_with_bounds(
                    congestion::HDR_LOWEST_DISCERNIBLE_MICROS,
                    congestion::HDR_HIGHEST_TRACKABLE_MICROS,
                    congestion::HDR_SIGNIFICANT_FIGURES,
                )
                .expect("histogram bounds valid"),
            );
            observability::register_histogram(slot.label, snap_rx, histogram_interval);
            logger_units.push(histogram_logger::LoggerUnit {
                label: slot.label,
                side: slot.side,
                op: slot.op,
                accumulator: acc.clone(),
                snapshot_tx: snap_tx,
            });
        }
        runtime.spawn(unit.run());
        runtime.spawn(auto_meta::run_adapter(
            walk::meta_resource(slot.side, slot.op),
            slot.apply_rate,
            decision_rx,
            sink.clone(),
        ));
    }

    if histogram_active {
        let header = build_histogram_header(&auto, trace_identifier, histogram_interval);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        store_logger_cancel(cancel_tx);
        // Snapshot the global PROGRESS counters into JSON each tick so
        // the binary log carries throughput/files-copied alongside the
        // latency distributions — readers can time-align them via the
        // shared unix_micros field. Encoding can't realistically fail
        // for this struct shape, but on the off chance it does we log
        // and return an empty Vec — the logger treats empty as "skip
        // this tick" rather than writing a record readers can't parse.
        let progress_source: histogram_logger::ProgressSource = Box::new(|| {
            let snapshot = progress::SerializableProgress::from(&*PROGRESS);
            serde_json::to_vec(&snapshot).unwrap_or_else(|err| {
                tracing::warn!(
                    "histogram-logger: SerializableProgress JSON encode failed: {err:#}; \
                     dropping this tick's progress record"
                );
                Vec::new()
            })
        });
        let handle = runtime.spawn(histogram_logger::run_logger(
            histogram_logger::LoggerConfig {
                interval: histogram_interval,
                log_file: histogram_log_file,
                header,
                progress_source: Some(progress_source),
            },
            logger_units,
            cancel_rx,
        ));
        store_logger_handle(handle);
    }

    tracing::info!(
        "auto-meta-throttle enabled (per-(side, op) controllers, {} total): \
         initial_cwnd={}, max_cwnd={}, alpha={}, beta={}, \
         baseline_percentile={}, current_percentile={}, \
         long_window={:?}, short_window={:?}, tick={:?}, \
         histograms={}",
        congestion::N_META_RESOURCES,
        auto.initial_cwnd,
        auto.max_cwnd,
        auto.alpha,
        auto.beta,
        auto.baseline_percentile,
        auto.current_percentile,
        auto.long_window,
        auto.short_window,
        auto.tick_interval,
        histogram_active,
    );
}

#[cfg(test)]
mod unit_label_tests {
    use super::unit_label;
    use congestion::{MetadataOp, Side};

    #[test]
    fn lookup_ops_carry_side_prefix() {
        // Stat and ReadLink can be on either side, so disambiguate.
        assert_eq!(unit_label(Side::Source, MetadataOp::Stat), "src-stat");
        assert_eq!(unit_label(Side::Destination, MetadataOp::Stat), "dst-stat");
        assert_eq!(
            unit_label(Side::Source, MetadataOp::ReadLink),
            "src-read-link",
        );
        assert_eq!(
            unit_label(Side::Destination, MetadataOp::ReadLink),
            "dst-read-link",
        );
    }

    #[test]
    fn destination_only_ops_drop_prefix() {
        // Mutations + open-create only fire on the destination, so the
        // active label has no side prefix — single-FS tools like rrm
        // see "unlink", "rmdir" instead of "dst-unlink".
        assert_eq!(unit_label(Side::Destination, MetadataOp::MkDir), "mkdir");
        assert_eq!(unit_label(Side::Destination, MetadataOp::RmDir), "rmdir");
        assert_eq!(unit_label(Side::Destination, MetadataOp::Unlink), "unlink");
        assert_eq!(
            unit_label(Side::Destination, MetadataOp::HardLink),
            "hard-link",
        );
        assert_eq!(
            unit_label(Side::Destination, MetadataOp::Symlink),
            "symlink"
        );
        assert_eq!(unit_label(Side::Destination, MetadataOp::Chmod), "chmod");
        assert_eq!(
            unit_label(Side::Destination, MetadataOp::OpenCreate),
            "open-create",
        );
    }

    #[test]
    fn unused_source_side_mutation_slots_keep_src_prefix() {
        // The wiring registers a controller for every (side, op) pair,
        // including the unused (Source, mutation) slots. Those stay
        // idle and are hidden by the renderer, but if they ever fired
        // a probe (regression / wiring mistake) the label distinguishes
        // them from the active destination-side variant.
        assert_eq!(unit_label(Side::Source, MetadataOp::Unlink), "src-unlink");
        assert_eq!(unit_label(Side::Source, MetadataOp::MkDir), "src-mkdir");
    }

    #[test]
    fn labels_are_unique_across_all_resources() {
        // Sanity: 18 distinct (Side, MetadataOp) pairs must produce 18
        // distinct labels — otherwise observability::register_unit would
        // create ambiguous panel rows.
        let mut seen = std::collections::HashSet::new();
        for &side in &Side::ALL {
            for &op in &MetadataOp::ALL {
                let label = unit_label(side, op);
                assert!(seen.insert(label), "duplicate label: {label}");
            }
        }
        assert_eq!(seen.len(), congestion::N_META_RESOURCES);
    }
}

#[cfg(test)]
mod runtime_stats_tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn collect_runtime_stats_matches_procfs_snapshot() -> Result<()> {
        let process = procfs::process::Process::myself()?;
        let expected = collect_runtime_stats_for_process(&process)?;
        let actual = collect_runtime_stats();
        let cpu_tolerance_ms = 50;
        let rss_tolerance_bytes = 1_000_000;
        assert!(
            expected.cpu_time_user_ms.abs_diff(actual.cpu_time_user_ms) <= cpu_tolerance_ms,
            "user CPU deviated by more than {cpu_tolerance_ms}ms: expected {}, got {}",
            expected.cpu_time_user_ms,
            actual.cpu_time_user_ms
        );
        assert!(
            expected
                .cpu_time_kernel_ms
                .abs_diff(actual.cpu_time_kernel_ms)
                <= cpu_tolerance_ms,
            "kernel CPU deviated by more than {cpu_tolerance_ms}ms: expected {}, got {}",
            expected.cpu_time_kernel_ms,
            actual.cpu_time_kernel_ms
        );
        assert!(
            expected.peak_rss_bytes.abs_diff(actual.peak_rss_bytes) <= rss_tolerance_bytes,
            "peak RSS deviated by more than {rss_tolerance_bytes} bytes: expected {}, got {}",
            expected.peak_rss_bytes,
            actual.peak_rss_bytes
        );
        Ok(())
    }

    #[test]
    fn collect_runtime_stats_returns_default_on_error() {
        let stats = collect_runtime_stats_inner(None);
        assert_eq!(stats, RuntimeStats::default());

        let nonexistent_process = procfs::process::Process::new(i32::MAX).ok();
        let stats = collect_runtime_stats_inner(nonexistent_process);
        assert_eq!(stats, RuntimeStats::default());
    }
}

#[cfg(test)]
mod resolve_log_path_tests {
    use super::*;

    #[test]
    fn full_path_with_extension() {
        let p = std::path::Path::new("/tmp/foo.hdr");
        assert_eq!(
            resolve_log_path(p, "rcp"),
            std::path::PathBuf::from("/tmp/foo.rcp.hdr"),
        );
    }

    #[test]
    fn bare_filename_resolves_to_current_dir() {
        let p = std::path::Path::new("foo.hdr");
        assert_eq!(
            resolve_log_path(p, "rcp"),
            std::path::PathBuf::from("./foo.rcp.hdr"),
        );
    }

    #[test]
    fn no_extension_defaults_to_hdr() {
        let p = std::path::Path::new("/tmp/foo");
        assert_eq!(
            resolve_log_path(p, "rcp"),
            std::path::PathBuf::from("/tmp/foo.rcp.hdr"),
        );
    }

    #[test]
    #[cfg(unix)]
    fn preserves_non_utf8_stem() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        // Build a path with an invalid-UTF-8 stem: /tmp/<0xFF><0xFE>.hdr
        let mut raw_name = vec![b'/', b't', b'm', b'p', b'/'];
        raw_name.extend_from_slice(&[0xFF, 0xFE]);
        raw_name.extend_from_slice(b".hdr");
        let p = std::path::PathBuf::from(std::ffi::OsString::from_vec(raw_name));
        let resolved = resolve_log_path(&p, "rcp");
        // The non-UTF-8 stem must be preserved; the suffix and extension
        // append cleanly.
        let bytes = resolved.as_os_str().as_bytes();
        assert!(
            bytes.windows(2).any(|w| w == [0xFF, 0xFE]),
            "non-UTF-8 bytes must survive resolution; got bytes: {bytes:?}",
        );
        assert!(
            bytes.ends_with(b".rcp.hdr"),
            "expected .rcp.hdr suffix; got bytes: {bytes:?}",
        );
    }
}

#[cfg(test)]
mod prepare_histogram_log_file_tests {
    use super::*;

    fn throttle_with_log_path(path: Option<std::path::PathBuf>) -> ThrottleConfig {
        ThrottleConfig {
            histogram_enabled: path.is_some(),
            histogram_log_path: path,
            ..Default::default()
        }
    }

    #[test]
    fn no_log_path_is_ok() {
        let throttle = throttle_with_log_path(None);
        assert!(
            prepare_histogram_log_file(&throttle, "rcp")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn writable_resolved_path_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let throttle = throttle_with_log_path(Some(dir.path().join("foo.hdr")));
        let file = prepare_histogram_log_file(&throttle, "rcp").unwrap();
        assert!(file.is_some());
        assert!(dir.path().join("foo.rcp.hdr").is_file());
    }

    #[test]
    fn resolved_path_existing_as_directory_is_rejected() {
        // Create a directory at the exact resolved path; OpenOptions::open
        // with create+truncate fails when target is a directory.
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("foo.rcp.hdr");
        std::fs::create_dir(&blocker).unwrap();
        let throttle = throttle_with_log_path(Some(dir.path().join("foo.hdr")));
        let err = prepare_histogram_log_file(&throttle, "rcp").unwrap_err();
        assert!(
            err.contains("histogram-log") && err.contains("foo.rcp.hdr"),
            "got: {err}",
        );
    }

    #[test]
    fn resolved_path_in_missing_parent_is_rejected() {
        let throttle = throttle_with_log_path(Some("/nonexistent-dir-67890/foo.hdr".into()));
        let err = prepare_histogram_log_file(&throttle, "rcp").unwrap_err();
        assert!(err.contains("histogram-log"), "got: {err}");
    }

    #[test]
    fn log_path_with_no_filename_is_rejected() {
        // PathBuf::from("/") has parent() == None and file_name() == None.
        let throttle = throttle_with_log_path(Some(std::path::PathBuf::from("/")));
        let err = prepare_histogram_log_file(&throttle, "rcp").unwrap_err();
        assert!(err.contains("filename"), "got: {err}");
    }

    #[test]
    #[cfg(unix)]
    fn resolved_path_existing_as_symlink_is_rejected() {
        // Defense against symlink-based hijacking: a local attacker who
        // can pre-create the predictable suffixed path as a symlink must
        // not be able to redirect the truncating open to a victim file.
        let dir = tempfile::tempdir().unwrap();
        // The resolved path will be `<dir>/foo.rcp.hdr`. Pre-create it as
        // a symlink pointing somewhere else (in this test, just to a
        // sibling file we don't care about).
        let target = dir.path().join("victim.txt");
        std::fs::write(&target, b"do not clobber").unwrap();
        let resolved_path = dir.path().join("foo.rcp.hdr");
        std::os::unix::fs::symlink(&target, &resolved_path).unwrap();
        let throttle = throttle_with_log_path(Some(dir.path().join("foo.hdr")));
        let err = prepare_histogram_log_file(&throttle, "rcp").unwrap_err();
        assert!(
            err.contains("symlink") || err.contains("ELOOP") || err.contains("Too many levels"),
            "got: {err}",
        );
        // Victim file content is preserved (the truncating open never reached it).
        let preserved = std::fs::read(&target).unwrap();
        assert_eq!(preserved, b"do not clobber");
    }

    #[tokio::test]
    async fn logger_writes_through_the_prepared_file_without_reopening_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let requested = dir.path().join("foo.hdr");
        let throttle = throttle_with_log_path(Some(requested.clone()));
        let log_file = prepare_histogram_log_file(&throttle, "rcp")
            .unwrap()
            .expect("configured log path must return its opened file");

        let resolved = dir.path().join("foo.rcp.hdr");
        let prepared = dir.path().join("prepared.hdr");
        std::fs::rename(&resolved, &prepared).unwrap();
        std::fs::write(&resolved, b"replacement must stay untouched").unwrap();

        let auto = AutoMetaThrottleConfig {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 4,
            alpha: 1.3,
            beta: 1.8,
            increase_step: 1,
            decrease_step: 1,
            baseline_percentile: 0.1,
            current_percentile: 0.5,
            long_window: std::time::Duration::from_secs(10),
            short_window: std::time::Duration::from_secs(1),
            tick_interval: std::time::Duration::from_millis(50),
        };
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let config = histogram_logger::LoggerConfig {
            interval: std::time::Duration::from_millis(10),
            log_file: Some(log_file),
            header: build_histogram_header(&auto, "rcp", std::time::Duration::from_millis(10)),
            progress_source: None,
        };
        let handle = tokio::spawn(histogram_logger::run_logger(config, Vec::new(), cancel_rx));
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        cancel_tx.send(true).unwrap();
        handle.await.unwrap();

        assert_eq!(
            std::fs::read(&resolved).unwrap(),
            b"replacement must stay untouched",
            "the logger reopened and truncated the replaced pathname"
        );
        let prepared_bytes = std::fs::read(&prepared).unwrap();
        assert!(
            prepared_bytes.starts_with(b"RCP-AUTOMETA-HIST-V2\n"),
            "the file prepared at startup did not receive the log header"
        );
    }
}
