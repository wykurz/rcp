//! Common utilities and types for RCP file operation tools
//!
//! This crate provides shared functionality used across all RCP tools (`rcp`, `rrm`, `rlink`, `rcmp`).
//! It includes core operations (copy, remove, link, compare), progress reporting, metadata preservation, and runtime configuration.
//!
//! # Core Modules
//!
//! - [`mod@copy`] - File copying operations with metadata preservation and error handling
//! - [`mod@rm`] - File removal operations
//! - [`mod@link`] - Hard-linking operations
//! - [`mod@cmp`] - File comparison operations (metadata-based)
//! - [`mod@preserve`] - Metadata preservation settings and operations
//! - [`mod@progress`] - Progress tracking and reporting
//! - [`mod@filecmp`] - File metadata comparison utilities
//! - [`mod@remote_tracing`] - Remote tracing support for distributed operations
//!
//! # Key Types
//!
//! ## `RcpdType`
//!
//! Identifies the role of a remote copy daemon:
//! - `Source` - reads files from source host
//! - `Destination` - writes files to destination host
//!
//! ## `ProgressType`
//!
//! Controls progress reporting display:
//! - `Auto` - automatically choose based on terminal type
//! - `ProgressBar` - animated progress bar (for interactive terminals)
//! - `TextUpdates` - periodic text updates (for logging/non-interactive)
//!
//! # Progress Reporting
//!
//! The crate provides a global progress tracking system accessible via [`get_progress()`].
//! Progress can be displayed in different formats depending on the execution context.
//!
//! Progress output goes to stderr, while logs go to stdout, allowing users to redirect logs to a file while still viewing interactive progress.
//!
//! # Runtime Configuration
//!
//! The [`run`] function provides a unified entry point for all RCP tools with support for:
//! - Progress tracking and reporting
//! - Logging configuration (quiet/verbose modes)
//! - Resource limits (max workers, open files, throttling)
//! - Tokio runtime setup
//! - Remote tracing integration
//!
//! # Examples
//!
//! ## Basic Copy Operation
//!
//! ```rust,no_run
//! use std::path::Path;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let src = Path::new("/source");
//! let dst = Path::new("/destination");
//!
//! let settings = common::copy::Settings {
//!     dereference: false,
//!     fail_early: false,
//!     overwrite: false,
//!     overwrite_compare: Default::default(),
//!     overwrite_filter: None,
//!     ignore_existing: false,
//!     chunk_size: 0,
//!     skip_specials: false,
//!     remote_copy_buffer_size: 0,
//!     filter: None,
//!     dry_run: None,
//!     delete: None,
//! };
//! let preserve = common::preserve::preserve_none();
//!
//! let summary = common::copy(src, dst, &settings, &preserve).await?;
//! println!("Copied {} files", summary.files_copied);
//! # Ok(())
//! # }
//! ```
//!
//! ## Metadata Comparison
//!
//! ```rust,no_run
//! use std::path::Path;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let src = Path::new("/path1");
//! let dst = Path::new("/path2");
//!
//! // output differences to stdout (use false for quiet mode)
//! let log = common::cmp::LogWriter::new(None, true, common::cmp::OutputFormat::default()).await?;
//! let settings = common::cmp::Settings {
//!     fail_early: false,
//!     exit_early: false,
//!     expand_missing: false,
//!     compare: Default::default(),
//!     filter: None,
//! };
//!
//! let summary = common::cmp(src, dst, &log, &settings).await?;
//! println!("Comparison complete: {}", summary);
//! # Ok(())
//! # }
//! ```

use anyhow::Context;
use std::io::IsTerminal;
use tracing::instrument;

mod auto_meta;
pub mod chmod;
pub mod cli;
pub mod cmp;
pub mod config;
pub mod copy;
pub mod copy_data;
pub mod delete;
pub mod dry_run;
pub mod error;
pub mod error_collector;
pub mod filegen;
pub mod filter;
pub mod histogram_logger;
pub mod histogram_panel;
pub mod link;
pub mod observability;
pub mod preserve;
pub mod remote_tracing;
pub mod rm;
mod runtime_setup;
pub mod safedir;
mod settings_parse;
pub mod version;

pub mod filecmp;
pub mod progress;
mod testutils;
pub mod toctou_check;
pub mod walk;
pub mod walk_driver;

pub use config::{
    AutoMetaThrottleConfig, DryRunMode, DryRunWarnings, OutputConfig, RuntimeConfig,
    ThrottleConfig, TracingConfig,
};
// Re-export `Side` from the congestion crate so downstream binaries
// (rcp, rrm, …) and integration tests can pass `common::Side::Source` /
// `common::Side::Destination` to `walk::next_entry_probed` and friends
// without taking a direct dependency on `congestion`.
pub use congestion::{MetadataOp, Side};
pub use progress::{RcpdProgressPrinter, SerializableProgress};
// Re-export the runtime-stat / trace-filename helpers that moved into
// `runtime_setup` so downstream binaries keep reaching them as
// `common::collect_runtime_stats`, etc.
pub use runtime_setup::{
    collect_runtime_stats, generate_debug_log_filename, generate_trace_filename,
};
pub use settings_parse::{
    parse_compare_settings, parse_metadata_cmp_settings, parse_preserve_settings,
    validate_update_compare_vs_preserve,
};

// Define RcpdType in common since remote depends on common
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, enum_map::Enum)]
pub enum RcpdType {
    Source,
    Destination,
}

impl std::fmt::Display for RcpdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RcpdType::Source => write!(f, "source"),
            RcpdType::Destination => write!(f, "destination"),
        }
    }
}

// Type alias for progress snapshots
pub type ProgressSnapshot<T> = enum_map::EnumMap<RcpdType, T>;

/// runtime statistics collected from a process (CPU time, memory usage)
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct RuntimeStats {
    /// user-mode CPU time in milliseconds
    pub cpu_time_user_ms: u64,
    /// kernel-mode CPU time in milliseconds
    pub cpu_time_kernel_ms: u64,
    /// peak resident set size in bytes
    pub peak_rss_bytes: u64,
}

/// runtime stats collected from remote rcpd processes for display at the end of a remote copy
#[derive(Debug, Default)]
pub struct RemoteRuntimeStats {
    pub source_host: String,
    pub source_stats: RuntimeStats,
    pub dest_host: String,
    pub dest_stats: RuntimeStats,
}

/// checks if a host string refers to the local machine.
/// returns true for `localhost`, `127.0.0.1`, `::1`, `[::1]`, or the actual hostname
#[must_use]
pub fn is_localhost(host: &str) -> bool {
    if host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]" {
        return true;
    }
    // check against actual hostname using gethostname
    let mut buf = [0u8; 256];
    // Safety: gethostname writes to buf and returns 0 on success
    let result = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if result == 0
        && let Ok(hostname_cstr) = std::ffi::CStr::from_bytes_until_nul(&buf)
        && let Ok(hostname) = hostname_cstr.to_str()
        && host == hostname
    {
        return true;
    }
    false
}

pub(crate) static PROGRESS: std::sync::LazyLock<progress::Progress> =
    std::sync::LazyLock::new(progress::Progress::new);
pub(crate) static PBAR: std::sync::LazyLock<indicatif::ProgressBar> =
    std::sync::LazyLock::new(indicatif::ProgressBar::new_spinner);
pub(crate) static REMOTE_RUNTIME_STATS: std::sync::LazyLock<
    std::sync::Mutex<Option<RemoteRuntimeStats>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(None));
static HISTOGRAM_LOGGER_CANCEL: std::sync::Mutex<Option<tokio::sync::watch::Sender<bool>>> =
    std::sync::Mutex::new(None);
static HISTOGRAM_LOGGER_HANDLE: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>> =
    std::sync::Mutex::new(None);

pub(crate) fn store_logger_cancel(tx: tokio::sync::watch::Sender<bool>) {
    *HISTOGRAM_LOGGER_CANCEL
        .lock()
        .expect("histogram logger cancel mutex poisoned") = Some(tx);
}

pub(crate) fn store_logger_handle(handle: tokio::task::JoinHandle<()>) {
    *HISTOGRAM_LOGGER_HANDLE
        .lock()
        .expect("histogram logger handle mutex poisoned") = Some(handle);
}

fn take_logger_handle() -> Option<tokio::task::JoinHandle<()>> {
    HISTOGRAM_LOGGER_HANDLE
        .lock()
        .expect("histogram logger handle mutex poisoned")
        .take()
}

fn signal_logger_cancel() {
    if let Some(tx) = HISTOGRAM_LOGGER_CANCEL
        .lock()
        .expect("histogram logger cancel mutex poisoned")
        .take()
        && let Err(err) = tx.send(true)
    {
        tracing::debug!("histogram-logger cancel send failed (already gone): {err:#}");
    }
}

#[must_use]
pub fn get_progress() -> &'static progress::Progress {
    &PROGRESS
}

/// stores remote runtime stats for display at the end of a remote copy operation
pub fn set_remote_runtime_stats(stats: RemoteRuntimeStats) {
    *REMOTE_RUNTIME_STATS.lock().unwrap() = Some(stats);
}

struct ProgressTracker {
    lock_cvar: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    pbar_thread: Option<std::thread::JoinHandle<()>>,
}

#[derive(Copy, Clone, Debug, Default, clap::ValueEnum)]
pub enum ProgressType {
    #[default]
    #[value(name = "auto", alias = "Auto")]
    Auto,
    #[value(name = "ProgressBar", alias = "progress-bar")]
    ProgressBar,
    #[value(name = "TextUpdates", alias = "text-updates")]
    TextUpdates,
}

pub enum GeneralProgressType {
    User {
        progress_type: ProgressType,
        kind: progress::LocalProgressKind,
    },
    Remote(tokio::sync::mpsc::UnboundedSender<remote_tracing::TracingMessage>),
    RemoteMaster {
        progress_type: ProgressType,
        get_progress_snapshot:
            Box<dyn Fn() -> ProgressSnapshot<SerializableProgress> + Send + 'static>,
    },
}

impl std::fmt::Debug for GeneralProgressType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneralProgressType::User {
                progress_type,
                kind,
            } => write!(f, "User(progress_type: {progress_type:?}, kind: {kind:?})"),
            GeneralProgressType::Remote(_) => write!(f, "Remote(<sender>)"),
            GeneralProgressType::RemoteMaster { progress_type, .. } => {
                write!(
                    f,
                    "RemoteMaster(progress_type: {progress_type:?}, <function>)"
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct ProgressSettings {
    pub progress_type: GeneralProgressType,
    pub progress_delay: Option<String>,
}

fn progress_bar(
    lock: &std::sync::Mutex<bool>,
    cvar: &std::sync::Condvar,
    delay_opt: &Option<std::time::Duration>,
    kind: progress::LocalProgressKind,
) {
    let delay = delay_opt.unwrap_or(std::time::Duration::from_millis(200));
    PBAR.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    let mut prog_printer = progress::make_local_printer(kind, &PROGRESS);
    let mut is_done = lock.lock().unwrap();
    loop {
        PBAR.set_position(PBAR.position() + 1); // do we need to update?
        let mut msg = prog_printer.print().unwrap();
        msg.push_str(&observability::render_lines());
        msg.push_str(&render_panel_from_registry());
        PBAR.set_message(msg);
        let result = cvar.wait_timeout(is_done, delay).unwrap();
        is_done = result.0;
        if *is_done {
            break;
        }
    }
    PBAR.finish_and_clear();
}

fn get_datetime_prefix() -> String {
    chrono::Local::now()
        .format("%Y-%m-%dT%H:%M:%S%.3f%:z")
        .to_string()
}

fn text_updates(
    lock: &std::sync::Mutex<bool>,
    cvar: &std::sync::Condvar,
    delay_opt: &Option<std::time::Duration>,
    kind: progress::LocalProgressKind,
) {
    let delay = delay_opt.unwrap_or(std::time::Duration::from_secs(10));
    let mut prog_printer = progress::make_local_printer(kind, &PROGRESS);
    let mut is_done = lock.lock().unwrap();
    loop {
        eprintln!("=======================");
        eprintln!(
            "{}\n--{}{}{}",
            get_datetime_prefix(),
            prog_printer.print().unwrap(),
            observability::render_lines(),
            render_panel_from_registry(),
        );
        let result = cvar.wait_timeout(is_done, delay).unwrap();
        is_done = result.0;
        if *is_done {
            break;
        }
    }
}

fn rcpd_updates(
    lock: &std::sync::Mutex<bool>,
    cvar: &std::sync::Condvar,
    delay_opt: &Option<std::time::Duration>,
    sender: tokio::sync::mpsc::UnboundedSender<remote_tracing::TracingMessage>,
) {
    tracing::debug!("Starting rcpd progress updates");
    let delay = delay_opt.unwrap_or(std::time::Duration::from_millis(200));
    let mut is_done = lock.lock().unwrap();
    loop {
        if remote_tracing::send_progress_update(&sender, &PROGRESS).is_err() {
            // channel closed, receiver is done
            tracing::debug!("Progress update channel closed, stopping progress updates");
            break;
        }
        let result = cvar.wait_timeout(is_done, delay).unwrap();
        is_done = result.0;
        if *is_done {
            break;
        }
    }
}

fn remote_master_updates<F>(
    lock: &std::sync::Mutex<bool>,
    cvar: &std::sync::Condvar,
    delay_opt: &Option<std::time::Duration>,
    get_progress_snapshot: F,
    progress_type: ProgressType,
) where
    F: Fn() -> ProgressSnapshot<SerializableProgress> + Send + 'static,
{
    let interactive = match progress_type {
        ProgressType::Auto => std::io::stderr().is_terminal(),
        ProgressType::ProgressBar => true,
        ProgressType::TextUpdates => false,
    };
    if interactive {
        PBAR.set_style(
            indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        let delay = delay_opt.unwrap_or(std::time::Duration::from_millis(200));
        let mut printer = RcpdProgressPrinter::new();
        let mut is_done = lock.lock().unwrap();
        loop {
            let progress_map = get_progress_snapshot();
            let source_progress = &progress_map[RcpdType::Source];
            let destination_progress = &progress_map[RcpdType::Destination];
            PBAR.set_position(PBAR.position() + 1); // do we need to update?
            let mut msg = printer
                .print(source_progress, destination_progress)
                .unwrap();
            msg.push_str(&render_panel_from_registry());
            PBAR.set_message(msg);
            let result = cvar.wait_timeout(is_done, delay).unwrap();
            is_done = result.0;
            if *is_done {
                break;
            }
        }
        PBAR.finish_and_clear();
    } else {
        let delay = delay_opt.unwrap_or(std::time::Duration::from_secs(10));
        let mut printer = RcpdProgressPrinter::new();
        let mut is_done = lock.lock().unwrap();
        loop {
            let progress_map = get_progress_snapshot();
            let source_progress = &progress_map[RcpdType::Source];
            let destination_progress = &progress_map[RcpdType::Destination];
            eprintln!("=======================");
            eprintln!(
                "{}\n--{}{}",
                get_datetime_prefix(),
                printer
                    .print(source_progress, destination_progress)
                    .unwrap(),
                render_panel_from_registry(),
            );
            let result = cvar.wait_timeout(is_done, delay).unwrap();
            is_done = result.0;
            if *is_done {
                break;
            }
        }
    }
}

impl ProgressTracker {
    pub fn new(progress_type: GeneralProgressType, delay_opt: Option<std::time::Duration>) -> Self {
        let lock_cvar =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let lock_cvar_clone = lock_cvar.clone();
        let pbar_thread = std::thread::spawn(move || {
            let (lock, cvar) = &*lock_cvar_clone;
            match progress_type {
                GeneralProgressType::Remote(sender) => {
                    rcpd_updates(lock, cvar, &delay_opt, sender);
                }
                GeneralProgressType::RemoteMaster {
                    progress_type,
                    get_progress_snapshot,
                } => {
                    remote_master_updates(
                        lock,
                        cvar,
                        &delay_opt,
                        get_progress_snapshot,
                        progress_type,
                    );
                }
                GeneralProgressType::User {
                    progress_type,
                    kind,
                } => {
                    let interactive = match progress_type {
                        ProgressType::Auto => std::io::stderr().is_terminal(),
                        ProgressType::ProgressBar => true,
                        ProgressType::TextUpdates => false,
                    };
                    if interactive {
                        progress_bar(lock, cvar, &delay_opt, kind);
                    } else {
                        text_updates(lock, cvar, &delay_opt, kind);
                    }
                }
            }
        });
        Self {
            lock_cvar,
            pbar_thread: Some(pbar_thread),
        }
    }
}

impl Drop for ProgressTracker {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.lock_cvar;
        let mut is_done = lock.lock().unwrap();
        *is_done = true;
        cvar.notify_one();
        drop(is_done);
        if let Some(pbar_thread) = self.pbar_thread.take() {
            pbar_thread.join().unwrap();
        }
    }
}

pub async fn cmp(
    src: &std::path::Path,
    dst: &std::path::Path,
    log: &cmp::LogWriter,
    settings: &cmp::Settings,
) -> Result<cmp::Summary, anyhow::Error> {
    cmp::cmp(&PROGRESS, src, dst, log, settings).await
}

pub async fn copy(
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &copy::Settings,
    preserve: &preserve::Settings,
) -> Result<copy::Summary, copy::Error> {
    copy::copy(&PROGRESS, src, dst, settings, preserve, false).await
}

pub async fn rm(path: &std::path::Path, settings: &rm::Settings) -> Result<rm::Summary, rm::Error> {
    rm::rm(&PROGRESS, path, settings).await
}

pub async fn chmod(
    path: &std::path::Path,
    settings: &chmod::Settings,
) -> Result<chmod::Summary, chmod::Error> {
    chmod::chmod(&PROGRESS, path, settings).await
}

pub async fn link(
    src: &std::path::Path,
    dst: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &link::Settings,
) -> Result<link::Summary, link::Error> {
    let cwd = std::env::current_dir()
        .with_context(|| "failed to get current working directory")
        .map_err(|err| link::Error::new(err, link::Summary::default()))?;
    link::link(&PROGRESS, &cwd, src, dst, update, settings, false).await
}

fn render_panel_from_registry() -> String {
    let entries = observability::registered_histograms();
    if entries.is_empty() {
        return String::new();
    }
    let snapshots: Vec<hdrhistogram::Histogram<u64>> = entries
        .iter()
        .map(|e| (*e.snapshot_rx.borrow()).clone())
        .collect();
    let units: Vec<histogram_panel::PanelUnit> = entries
        .iter()
        .zip(snapshots.iter())
        .map(|(e, snap)| histogram_panel::PanelUnit {
            label: e.label,
            histogram: snap,
            interval: e.interval,
        })
        .collect();
    histogram_panel::render_histogram_panel(&units)
}

#[instrument(skip(func))] // "func" is not Debug printable
pub fn run<Fut, Summary, Error>(
    progress: Option<ProgressSettings>,
    output: OutputConfig,
    runtime_config: RuntimeConfig,
    throttle_config: ThrottleConfig,
    tracing_config: TracingConfig,
    func: impl FnOnce() -> Fut,
) -> Option<Summary>
// we return an Option rather than a Result to indicate that callers of this function should NOT print the error
where
    Summary: std::fmt::Display,
    Error: std::fmt::Display + std::fmt::Debug,
    Fut: std::future::Future<Output = Result<Summary, Error>>,
{
    // force initialization of PROGRESS to set start_time at the beginning of the run
    // (for remote master operations, PROGRESS is otherwise only accessed at the end in
    // print_runtime_stats(), leading to near-zero walltime)
    let _ = get_progress();
    if let Err(e) = throttle_config.validate() {
        eprintln!("Configuration error: {e}");
        return None;
    }
    let OutputConfig {
        quiet,
        verbose,
        print_summary,
        suppress_runtime_stats,
    } = output;
    // tracing guards must outlive the runtime so chrome/flame traces flush
    // extract trace_identifier before install_tracing_subscriber consumes tracing_config
    let trace_identifier = tracing_config.trace_identifier.clone();
    if let Err(e) =
        runtime_setup::validate_histogram_log_target(&throttle_config, &trace_identifier)
    {
        eprintln!("Configuration error: {e}");
        return None;
    }
    let _tracing_guards = runtime_setup::install_tracing_subscriber(quiet, verbose, tracing_config);
    let res = {
        let runtime = runtime_setup::build_tokio_runtime(&runtime_config, &throttle_config);
        runtime_setup::spawn_throttle_replenishers(&runtime, &throttle_config, &trace_identifier);
        let res = {
            let _progress_tracker = progress.map(|settings| {
                tracing::debug!("Requesting progress updates {settings:?}");
                let delay = settings.progress_delay.map(|delay_str| {
                    humantime::parse_duration(&delay_str)
                        .expect("Couldn't parse duration out of --progress-delay")
                });
                ProgressTracker::new(settings.progress_type, delay)
            });
            runtime.block_on(func())
        };
        match &res {
            Ok(summary) => {
                if print_summary || verbose > 0 {
                    println!("{summary}");
                }
            }
            Err(err) => {
                if !quiet {
                    println!("{err:?}");
                }
            }
        }
        if (print_summary || verbose > 0)
            && !suppress_runtime_stats
            && let Err(err) = runtime_setup::print_runtime_stats()
        {
            println!("Failed to print runtime stats: {err:?}");
        }
        // Signal the histogram logger to exit cleanly so its final
        // snapshot is written before the runtime drops and aborts it.
        // No-op when histograms are disabled.
        signal_logger_cancel();
        if let Some(handle) = take_logger_handle() {
            // Bound the wait so a stuck logger can't hang shutdown — 1s is
            // generous: the logger only does a snapshot+flush on cancel.
            let _ = runtime.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(1), handle).await
            });
        }
        res
        // runtime drops here, cancelling all spawned tasks (control
        // loop, adapter, replenishers) and releasing their permits.
    };
    // Clear process-wide state so a second `run()` in the same process
    // starts on a clean slate. Without this, a later run inherits the
    // previous auto-meta sample sink and ops-in-flight cap, and its
    // probes acquire against stale limits even when auto_meta is off.
    reset_process_throttle_state();
    res.ok()
}

/// Reset process-wide throttle + congestion state to its pre-`run()`
/// defaults. Called by [`run`] on exit so callers that invoke it more
/// than once in a single process (library users, integration tests
/// outside this crate) aren't affected by the previous invocation's
/// decisions.
fn reset_process_throttle_state() {
    congestion::clear_sample_sink();
    observability::clear();
    for &side in &throttle::Side::ALL {
        for &op in &throttle::MetadataOp::ALL {
            throttle::set_max_ops_in_flight(throttle::Resource::meta(side, op), 0);
        }
    }
    throttle::disable_ops_throttle();
    // Without these resets, a second run() in the same process inherits
    // the previous run's open-files cap and iops-throttle even when the
    // caller passes 0 ("no limit"): `set_max_open_files` / `init_iops_tokens`
    // are skipped on 0, leaving the prior `setup(N)` in force. setup(0)
    // disables the semaphore, so the next run sees a clean slate and can
    // either re-init with a fresh value or stay disabled.
    throttle::set_max_open_files(0);
    throttle::init_iops_tokens(0);
}
