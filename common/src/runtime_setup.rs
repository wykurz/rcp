//! Runtime, tracing, and throttle bring-up used by [`crate::run`].
//!
//! This module gathers the process-startup plumbing that configures the
//! tokio runtime, installs the tracing subscriber, primes the throttle
//! replenishers, and wires the adaptive metadata-ops control loops. It is
//! kept separate from the crate root purely to keep `lib.rs` focused on the
//! public surface.

use crate::config::{AutoMetaThrottleConfig, RuntimeConfig, ThrottleConfig, TracingConfig};
use crate::{
    PBAR, PROGRESS, REMOTE_RUNTIME_STATS, RuntimeStats, auto_meta, histogram_logger, is_localhost,
    observability, progress, store_logger_cancel, store_logger_handle, walk,
};
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

fn get_max_open_files() -> Result<u64, std::io::Error> {
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // Safety: we pass a valid "rlim" pointer and the result is checked
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
fn build_profile_filter_str(profile_level: Option<&str>) -> String {
    let level_str = profile_level.unwrap_or("trace");
    let valid_levels = ["trace", "debug", "info", "warn", "error", "off"];
    if !valid_levels.contains(&level_str.to_lowercase().as_str()) {
        eprintln!(
            "Invalid --profile-level '{level_str}'. Valid values: trace, debug, info, warn, error, off"
        );
        std::process::exit(1);
    }
    format!("tokio=off,quinn=off,h2=off,hyper=off,rustls=off,{level_str}")
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
) -> TracingGuards {
    if quiet {
        assert!(
            verbose == 0,
            "Quiet mode and verbose mode are mutually exclusive"
        );
        return TracingGuards {
            chrome: None,
            flame: None,
        };
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
    let file_layer = debug_log_file.map(|log_file_path| {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
            .unwrap_or_else(|e| {
                panic!("Failed to create debug log file at '{log_file_path}': {e}")
            });
        tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_timer(LocalTimeFormatter)
            .with_ansi(false)
            .with_writer(file)
            .with_filter(build_verbose_env_filter(verbose))
    });
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
    let console_layer = tokio_console.then(|| {
        let console_port = tokio_console_port.unwrap_or(6669);
        let retention_seconds: u64 =
            read_env_or_default("RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS", 60);
        eprintln!("Tokio console server listening on 127.0.0.1:{console_port}");
        console_subscriber::ConsoleLayer::builder()
            .retention(std::time::Duration::from_secs(retention_seconds))
            .server_addr(([127, 0, 0, 1], console_port))
            .spawn()
    });
    // chrome/flame share a profile filter; build the string once and re-parse
    // per layer (EnvFilter isn't Clone).
    let profile_filter_str = (chrome_trace_prefix.is_some() || flamegraph_prefix.is_some())
        .then(|| build_profile_filter_str(profile_level.as_deref()));
    let make_profile_filter =
        || tracing_subscriber::EnvFilter::new(profile_filter_str.as_ref().unwrap());
    let mut chrome_guard = None;
    let chrome_layer = chrome_trace_prefix.as_ref().map(|prefix| {
        let filename = generate_trace_filename(prefix, &trace_identifier, "json");
        eprintln!("Chrome trace will be written to: {filename}");
        let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .file(&filename)
            .include_args(true)
            .build();
        chrome_guard = Some(guard);
        layer.with_filter(make_profile_filter())
    });
    let mut flame_guard = None;
    let flame_layer = flamegraph_prefix.as_ref().and_then(|prefix| {
        let filename = generate_trace_filename(prefix, &trace_identifier, "folded");
        eprintln!("Flamegraph data will be written to: {filename}");
        match tracing_flame::FlameLayer::with_file(&filename) {
            Ok((layer, guard)) => {
                flame_guard = Some(guard);
                Some(layer.with_filter(make_profile_filter()))
            }
            Err(e) => {
                eprintln!("Failed to create flamegraph layer: {e}");
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
    TracingGuards {
        chrome: chrome_guard,
        flame: flame_guard,
    }
}

/// Build a multi-threaded tokio runtime configured per `runtime`, and apply
/// the `max_open_files` limit from `throttle`. Falls back to ~80% of the
/// system rlimit (capped at 4096) when `max_open_files` is unset.
pub(crate) fn build_tokio_runtime(
    runtime: &RuntimeConfig,
    throttle: &ThrottleConfig,
) -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if runtime.max_workers > 0 {
        builder.worker_threads(runtime.max_workers);
    }
    if runtime.max_blocking_threads > 0 {
        builder.max_blocking_threads(runtime.max_blocking_threads);
    }
    if !sysinfo::set_open_files_limit(usize::MAX) {
        tracing::info!("Failed to update the open files limit (expected on non-linux targets)");
    }
    let set_max_open_files = throttle.max_open_files.unwrap_or_else(|| {
        let limit = get_max_open_files().expect(
            "We failed to query rlimit, if this is expected try specifying --max-open-files",
        ) as usize;
        // use ~80% of the system limit, but cap at 4096 to avoid overwhelming
        // distributed filesystems
        std::cmp::min(limit / 10 * 8, 4096)
    });
    if set_max_open_files > 0 {
        tracing::info!("Setting max open files to: {}", set_max_open_files);
        throttle::set_max_open_files(set_max_open_files);
    } else {
        tracing::info!("Not applying any limit to max open files!");
    }
    builder.build().expect("Failed to create runtime")
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
            throttle.histogram_log_path.clone(),
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
/// Handles three edge cases consistently with the validator:
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

/// Verify the resolved histogram log path can actually be opened for
/// writing. Called from [`run`] before any work begins so a typo or
/// permission issue surfaces as a configuration error rather than a
/// silent warning at runtime.
///
/// The check creates+truncates the file (matching what the logger
/// task does later) so it catches "exists as a directory", "exists
/// as an unwritable file", and most permission issues. The logger's
/// own open later in startup will reuse the same path; the double-
/// open is harmless (the logger truncates again).
pub(crate) fn validate_histogram_log_target(
    throttle: &ThrottleConfig,
    trace_identifier: &str,
) -> Result<(), String> {
    let Some(path) = &throttle.histogram_log_path else {
        return Ok(());
    };
    if path.is_dir() {
        return Err(format!(
            "--auto-meta-histogram-log {path:?} is a directory; expected a file path",
        ));
    }
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
        Ok(_) => Ok(()),
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
    histogram_log_path: Option<std::path::PathBuf>,
    histogram_interval: std::time::Duration,
    trace_identifier: &str,
) {
    let initial_cwnd = auto
        .initial_cwnd
        .clamp(auto.min_cwnd.max(1), auto.max_cwnd.max(1));
    let histogram_active = histogram_enabled || histogram_log_path.is_some();
    // Per-tool log path: each rcpd / master writes to its own file by
    // suffixing trace_identifier on the user-supplied path, mirroring
    // chrome_trace_prefix's convention.
    let resolved_log_path = histogram_log_path
        .as_ref()
        .map(|p| resolve_log_path(p, trace_identifier));

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
                log_path: resolved_log_path,
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
mod validate_histogram_log_target_tests {
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
        assert!(validate_histogram_log_target(&throttle, "rcp").is_ok());
    }

    #[test]
    fn writable_resolved_path_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let throttle = throttle_with_log_path(Some(dir.path().join("foo.hdr")));
        assert!(validate_histogram_log_target(&throttle, "rcp").is_ok());
    }

    #[test]
    fn resolved_path_existing_as_directory_is_rejected() {
        // Create a directory at the exact resolved path; OpenOptions::open
        // with create+truncate fails when target is a directory.
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("foo.rcp.hdr");
        std::fs::create_dir(&blocker).unwrap();
        let throttle = throttle_with_log_path(Some(dir.path().join("foo.hdr")));
        let err = validate_histogram_log_target(&throttle, "rcp").unwrap_err();
        assert!(
            err.contains("histogram-log") && err.contains("foo.rcp.hdr"),
            "got: {err}",
        );
    }

    #[test]
    fn resolved_path_in_missing_parent_is_rejected() {
        let throttle = throttle_with_log_path(Some("/nonexistent-dir-67890/foo.hdr".into()));
        let err = validate_histogram_log_target(&throttle, "rcp").unwrap_err();
        assert!(err.contains("histogram-log"), "got: {err}");
    }

    #[test]
    fn log_path_pointing_at_existing_directory_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let throttle = throttle_with_log_path(Some(dir.path().to_path_buf()));
        let err = validate_histogram_log_target(&throttle, "rcp").unwrap_err();
        assert!(err.contains("directory"), "got: {err}");
    }

    #[test]
    fn log_path_with_no_filename_is_rejected() {
        // PathBuf::from("/") has parent() == None and file_name() == None.
        let throttle = throttle_with_log_path(Some(std::path::PathBuf::from("/")));
        let err = validate_histogram_log_target(&throttle, "rcp").unwrap_err();
        assert!(
            err.contains("filename") || err.contains("directory"),
            "got: {err}",
        );
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
        let err = validate_histogram_log_target(&throttle, "rcp").unwrap_err();
        assert!(
            err.contains("symlink") || err.contains("ELOOP") || err.contains("Too many levels"),
            "got: {err}",
        );
        // Victim file content is preserved (the truncating open never reached it).
        let preserved = std::fs::read(&target).unwrap();
        assert_eq!(preserved, b"do not clobber");
    }
}
