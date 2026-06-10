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

use crate::cmp::ObjType;
use anyhow::Context;
use anyhow::anyhow;
use std::io::IsTerminal;
use tracing::instrument;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

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
pub mod safedir;
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

static PROGRESS: std::sync::LazyLock<progress::Progress> =
    std::sync::LazyLock::new(progress::Progress::new);
static PBAR: std::sync::LazyLock<indicatif::ProgressBar> =
    std::sync::LazyLock::new(indicatif::ProgressBar::new_spinner);
static REMOTE_RUNTIME_STATS: std::sync::LazyLock<std::sync::Mutex<Option<RemoteRuntimeStats>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(None));
static HISTOGRAM_LOGGER_CANCEL: std::sync::Mutex<Option<tokio::sync::watch::Sender<bool>>> =
    std::sync::Mutex::new(None);
static HISTOGRAM_LOGGER_HANDLE: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>> =
    std::sync::Mutex::new(None);

fn store_logger_cancel(tx: tokio::sync::watch::Sender<bool>) {
    *HISTOGRAM_LOGGER_CANCEL
        .lock()
        .expect("histogram logger cancel mutex poisoned") = Some(tx);
}

fn store_logger_handle(handle: tokio::task::JoinHandle<()>) {
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

pub fn parse_metadata_cmp_settings(
    settings: &str,
) -> Result<filecmp::MetadataCmpSettings, anyhow::Error> {
    let mut metadata_cmp_settings = filecmp::MetadataCmpSettings::default();
    for setting in settings.split(',') {
        match setting {
            "uid" => metadata_cmp_settings.uid = true,
            "gid" => metadata_cmp_settings.gid = true,
            "mode" => metadata_cmp_settings.mode = true,
            "size" => metadata_cmp_settings.size = true,
            "mtime" => metadata_cmp_settings.mtime = true,
            "ctime" => metadata_cmp_settings.ctime = true,
            _ => {
                return Err(anyhow!("Unknown metadata comparison setting: {}", setting));
            }
        }
    }
    Ok(metadata_cmp_settings)
}

fn parse_type_settings(
    settings: &str,
) -> Result<(preserve::UserAndTimeSettings, Option<preserve::ModeMask>), anyhow::Error> {
    let mut user_and_time = preserve::UserAndTimeSettings::default();
    let mut mode_mask = None;
    for setting in settings.split(',') {
        match setting {
            "uid" => user_and_time.uid = true,
            "gid" => user_and_time.gid = true,
            "time" => user_and_time.time = true,
            _ => {
                if let Ok(mask) = u32::from_str_radix(setting, 8) {
                    mode_mask = Some(mask);
                } else {
                    return Err(anyhow!("Unknown preserve attribute specified: {}", setting));
                }
            }
        }
    }
    Ok((user_and_time, mode_mask))
}

pub fn parse_preserve_settings(settings: &str) -> Result<preserve::Settings, anyhow::Error> {
    // handle presets
    match settings {
        "all" => return Ok(preserve::preserve_all()),
        "none" => return Ok(preserve::preserve_none()),
        _ => {}
    }
    let mut preserve_settings = preserve::Settings::default();
    for type_settings in settings.split_whitespace() {
        if let Some((obj_type, obj_settings)) = type_settings.split_once(':') {
            let (user_and_time_settings, mode_opt) = parse_type_settings(obj_settings).context(
                format!("parsing preserve settings: {obj_settings}, type: {obj_type}"),
            )?;
            match obj_type {
                "f" | "file" => {
                    preserve_settings.file = preserve::FileSettings::default();
                    preserve_settings.file.user_and_time = user_and_time_settings;
                    if let Some(mode) = mode_opt {
                        preserve_settings.file.mode_mask = mode;
                    }
                }
                "d" | "dir" | "directory" => {
                    preserve_settings.dir = preserve::DirSettings::default();
                    preserve_settings.dir.user_and_time = user_and_time_settings;
                    if let Some(mode) = mode_opt {
                        preserve_settings.dir.mode_mask = mode;
                    }
                }
                "l" | "link" | "symlink" => {
                    preserve_settings.symlink = preserve::SymlinkSettings::default();
                    preserve_settings.symlink.user_and_time = user_and_time_settings;
                }
                _ => {
                    return Err(anyhow!("Unknown object type: {}", obj_type));
                }
            }
        } else {
            return Err(anyhow!("Invalid preserve settings: {}", settings));
        }
    }
    Ok(preserve_settings)
}

/// Validates that every attribute checked by --update's comparison is actually being preserved.
/// Skips size (always preserved via content copy) and ctime (kernel-managed, cannot be set).
pub fn validate_update_compare_vs_preserve(
    update_compare: &filecmp::MetadataCmpSettings,
    preserve: &preserve::Settings,
) -> Result<(), String> {
    let mut missing = Vec::new();
    if update_compare.mtime && !preserve.file.user_and_time.time {
        missing.push("mtime");
    }
    if update_compare.uid && !preserve.file.user_and_time.uid {
        missing.push("uid");
    }
    if update_compare.gid && !preserve.file.user_and_time.gid {
        missing.push("gid");
    }
    // metadata_equal compares full mode (0o7777), so a partial mask is lossy
    if update_compare.mode && preserve.file.mode_mask != 0o7777 {
        missing.push("mode");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "--update compares [{}] but --preserve-settings does not preserve them. \
             Use --allow-lossy-update to override or adjust --preserve-settings.",
            missing.join(", ")
        ))
    }
}

pub fn parse_compare_settings(settings: &str) -> Result<cmp::ObjSettings, anyhow::Error> {
    let mut cmp_settings = cmp::ObjSettings::default();
    for type_settings in settings.split_whitespace() {
        if let Some((obj_type, obj_settings)) = type_settings.split_once(':') {
            let obj_cmp_settings = parse_metadata_cmp_settings(obj_settings).context(format!(
                "parsing preserve settings: {obj_settings}, type: {obj_type}"
            ))?;
            let obj_type = match obj_type {
                "f" | "file" => ObjType::File,
                "d" | "dir" | "directory" => ObjType::Dir,
                "l" | "link" | "symlink" => ObjType::Symlink,
                "o" | "other" => ObjType::Other,
                _ => {
                    return Err(anyhow!("Unknown obj type: {}", obj_type));
                }
            };
            cmp_settings[obj_type] = obj_cmp_settings;
        } else {
            return Err(anyhow!("Invalid preserve settings: {}", settings));
        }
    }
    Ok(cmp_settings)
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
fn print_runtime_stats() -> Result<(), anyhow::Error> {
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
struct TracingGuards {
    chrome: Option<tracing_chrome::FlushGuard>,
    flame: Option<tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>>>,
}

/// Install the global [`tracing_subscriber`] registry from a [`TracingConfig`].
/// Caller must hold the returned [`TracingGuards`] until the run finishes so
/// that chrome/flame traces are flushed before the file handles close.
///
/// In quiet mode this is a no-op (the subscriber is never installed).
fn install_tracing_subscriber(
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
fn build_tokio_runtime(
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
fn spawn_throttle_replenishers(
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
fn validate_histogram_log_target(
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
    if let Err(e) = validate_histogram_log_target(&throttle_config, &trace_identifier) {
        eprintln!("Configuration error: {e}");
        return None;
    }
    let _tracing_guards = install_tracing_subscriber(quiet, verbose, tracing_config);
    let res = {
        let runtime = build_tokio_runtime(&runtime_config, &throttle_config);
        spawn_throttle_replenishers(&runtime, &throttle_config, &trace_identifier);
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
            && let Err(err) = print_runtime_stats()
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
mod parse_preserve_settings_tests {
    use super::*;
    #[test]
    fn preset_all_returns_preserve_all() {
        let settings = parse_preserve_settings("all").unwrap();
        let expected = preserve::preserve_all();
        assert_eq!(settings.file.mode_mask, expected.file.mode_mask);
        assert!(settings.file.user_and_time.uid);
        assert!(settings.file.user_and_time.gid);
        assert!(settings.file.user_and_time.time);
        assert_eq!(settings.dir.mode_mask, expected.dir.mode_mask);
        assert!(settings.dir.user_and_time.uid);
        assert!(settings.dir.user_and_time.gid);
        assert!(settings.dir.user_and_time.time);
        assert!(settings.symlink.user_and_time.uid);
        assert!(settings.symlink.user_and_time.gid);
        assert!(settings.symlink.user_and_time.time);
    }
    #[test]
    fn preset_none_returns_preserve_none() {
        let settings = parse_preserve_settings("none").unwrap();
        let expected = preserve::preserve_none();
        assert_eq!(settings.file.mode_mask, expected.file.mode_mask);
        assert!(!settings.file.user_and_time.uid);
        assert!(!settings.file.user_and_time.gid);
        assert!(!settings.file.user_and_time.time);
        assert_eq!(settings.dir.mode_mask, expected.dir.mode_mask);
        assert!(!settings.dir.user_and_time.uid);
        assert!(!settings.dir.user_and_time.gid);
        assert!(!settings.dir.user_and_time.time);
        assert!(!settings.symlink.user_and_time.uid);
        assert!(!settings.symlink.user_and_time.gid);
        assert!(!settings.symlink.user_and_time.time);
    }
    #[test]
    fn per_type_settings_still_work() {
        let settings = parse_preserve_settings("f:uid,time,0777 d:gid").unwrap();
        assert!(settings.file.user_and_time.uid);
        assert!(settings.file.user_and_time.time);
        assert!(!settings.file.user_and_time.gid);
        assert_eq!(settings.file.mode_mask, 0o777);
        assert!(!settings.dir.user_and_time.uid);
        assert!(settings.dir.user_and_time.gid);
        assert!(!settings.dir.user_and_time.time);
    }
    #[test]
    fn invalid_settings_returns_error() {
        assert!(parse_preserve_settings("invalid").is_err());
        assert!(parse_preserve_settings("f:unknown_attr").is_err());
    }
}

#[cfg(test)]
mod validate_update_compare_vs_preserve_tests {
    use super::*;
    #[test]
    fn detects_mtime_mismatch() {
        let compare = filecmp::MetadataCmpSettings {
            mtime: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mtime"));
    }
    #[test]
    fn detects_uid_mismatch() {
        let compare = filecmp::MetadataCmpSettings {
            uid: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("uid"));
    }
    #[test]
    fn detects_gid_mismatch() {
        let compare = filecmp::MetadataCmpSettings {
            gid: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("gid"));
    }
    #[test]
    fn detects_mode_mismatch() {
        let compare = filecmp::MetadataCmpSettings {
            mode: true,
            ..Default::default()
        };
        let mut preserve = preserve::preserve_none();
        preserve.file.mode_mask = 0;
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mode"));
    }
    #[test]
    fn detects_multiple_mismatches() {
        let compare = filecmp::MetadataCmpSettings {
            mtime: true,
            uid: true,
            gid: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("mtime"));
        assert!(err.contains("uid"));
        assert!(err.contains("gid"));
    }
    #[test]
    fn passes_when_preserve_covers_all_compared_attrs() {
        let compare = filecmp::MetadataCmpSettings {
            mtime: true,
            uid: true,
            gid: true,
            mode: true,
            size: true,  // always preserved, should not cause error
            ctime: true, // kernel-managed, should not cause error
        };
        let preserve = preserve::preserve_all();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_ok());
    }
    #[test]
    fn fails_with_partial_mode_mask_when_mode_compared() {
        // default mode_mask is 0o0777 which drops setuid/setgid/sticky bits,
        // but metadata_equal compares full mode (0o7777) — so this is lossy
        let compare = filecmp::MetadataCmpSettings {
            mode: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mode"));
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
