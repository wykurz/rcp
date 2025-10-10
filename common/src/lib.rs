//! Common utilities and types for RCP file operation tools
//!
//! This crate provides shared functionality used across all RCP tools (`rcp`, `rrm`, `rlink`, `rcmp`).
//! It includes core operations (copy, remove, link, compare), progress reporting, metadata preservation,
//! and runtime configuration.
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
//! ## RcpdType
//!
//! Identifies the role of a remote copy daemon:
//! - `Source` - reads files from source host
//! - `Destination` - writes files to destination host
//!
//! ## ProgressType
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
//! Progress output goes to stderr, while logs go to stdout, allowing users to redirect
//! logs to a file while still viewing interactive progress.
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
//! let settings = common::copy::Settings::default();
//! let preserve = common::preserve::preserve_default();
//!
//! let summary = common::copy(src, dst, &settings, &preserve).await?;
//! println!("Copied {} files", summary.ok_cnt);
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
//! let log = common::cmp::LogWriter::new(None)?;
//! let settings = common::cmp::Settings::default();
//!
//! let summary = common::cmp(src, dst, &log, &settings).await?;
//! println!("Differences found: {}", summary.ne_cnt);
//! # Ok(())
//! # }
//! ```

#[macro_use]
extern crate lazy_static;

use crate::cmp::ObjType;
use anyhow::anyhow;
use anyhow::Context;
use std::io::IsTerminal;
use tracing::instrument;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

pub mod cmp;
pub mod copy;
pub mod link;
pub mod preserve;
pub mod remote_tracing;
pub mod rm;

pub mod filecmp;
pub mod progress;
mod testutils;

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

lazy_static! {
    static ref PROGRESS: progress::Progress = progress::Progress::new();
    static ref PBAR: indicatif::ProgressBar = indicatif::ProgressBar::new_spinner();
}

pub fn get_progress() -> &'static progress::Progress {
    &PROGRESS
}

struct ProgressTracker {
    lock_cvar: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    pbar_thread: Option<std::thread::JoinHandle<()>>,
}

#[derive(Copy, Clone, Debug, Default, clap::ValueEnum)]
pub enum ProgressType {
    #[default]
    Auto,
    ProgressBar,
    TextUpdates,
}

pub enum GeneralProgressType {
    User(ProgressType),
    Remote(tokio::sync::mpsc::UnboundedSender<remote_tracing::TracingMessage>),
    RemoteMaster(Box<dyn Fn() -> ProgressSnapshot<SerializableProgress> + Send + 'static>),
}

impl std::fmt::Debug for GeneralProgressType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneralProgressType::User(pt) => write!(f, "User({pt:?})"),
            GeneralProgressType::Remote(_) => write!(f, "Remote(<sender>)"),
            GeneralProgressType::RemoteMaster(_) => write!(f, "RemoteMaster(<function>)"),
        }
    }
}

// Keep FromStr for backwards compatibility, but ValueEnum will be used by clap
impl std::str::FromStr for ProgressType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" | "Auto" => Ok(ProgressType::Auto),
            "ProgressBar" => Ok(ProgressType::ProgressBar),
            "TextUpdates" => Ok(ProgressType::TextUpdates),
            _ => Err(anyhow!("Invalid progress type: {}", s)),
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
) {
    let delay = delay_opt.unwrap_or(std::time::Duration::from_millis(200));
    PBAR.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    let mut prog_printer = progress::ProgressPrinter::new(&PROGRESS);
    let mut is_done = lock.lock().unwrap();
    loop {
        PBAR.set_position(PBAR.position() + 1); // do we need to update?
        PBAR.set_message(prog_printer.print().unwrap());
        let result = cvar.wait_timeout(is_done, delay).unwrap();
        is_done = result.0;
        if *is_done {
            break;
        }
    }
    PBAR.finish_and_clear();
}

fn text_updates(
    lock: &std::sync::Mutex<bool>,
    cvar: &std::sync::Condvar,
    delay_opt: &Option<std::time::Duration>,
) {
    let delay = delay_opt.unwrap_or(std::time::Duration::from_secs(10));
    let mut prog_printer = progress::ProgressPrinter::new(&PROGRESS);
    let mut is_done = lock.lock().unwrap();
    loop {
        eprintln!("--{}", prog_printer.print().unwrap());
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
) where
    F: Fn() -> ProgressSnapshot<SerializableProgress> + Send + 'static,
{
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
        PBAR.set_message(
            printer
                .print(source_progress, destination_progress)
                .unwrap(),
        );
        let result = cvar.wait_timeout(is_done, delay).unwrap();
        is_done = result.0;
        if *is_done {
            break;
        }
    }
    PBAR.finish_and_clear();
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
                GeneralProgressType::RemoteMaster(get_progress_snapshot) => {
                    remote_master_updates(lock, cvar, &delay_opt, get_progress_snapshot);
                }
                _ => {
                    let interactive = match progress_type {
                        GeneralProgressType::User(ProgressType::Auto) => {
                            std::io::stderr().is_terminal()
                        }
                        GeneralProgressType::User(ProgressType::ProgressBar) => true,
                        GeneralProgressType::User(ProgressType::TextUpdates) => false,
                        GeneralProgressType::Remote(_) | GeneralProgressType::RemoteMaster(_) => {
                            unreachable!("Invalid progress type: {progress_type:?}")
                        }
                    };
                    if interactive {
                        progress_bar(lock, cvar, &delay_opt);
                    } else {
                        text_updates(lock, cvar, &delay_opt);
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
    let mut preserve_settings = preserve::Settings::default();
    for type_settings in settings.split(' ') {
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
                    };
                }
                "d" | "dir" | "directory" => {
                    preserve_settings.dir = preserve::DirSettings::default();
                    preserve_settings.dir.user_and_time = user_and_time_settings;
                    if let Some(mode) = mode_opt {
                        preserve_settings.dir.mode_mask = mode;
                    };
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

pub fn parse_compare_settings(settings: &str) -> Result<cmp::ObjSettings, anyhow::Error> {
    let mut cmp_settings = cmp::ObjSettings::default();
    for type_settings in settings.split(' ') {
        if let Some((obj_type, obj_settings)) = type_settings.split_once(':') {
            let obj_cmp_settings = parse_metadata_cmp_settings(obj_settings).context(format!(
                "parsing preserve settings: {obj_settings}, type: {obj_type}"
            ))?;
            let obj_type = match obj_type {
                "f" | "file" => ObjType::File,
                "d" | "dir" | "directory" => ObjType::Dir,
                "l" | "link" | "symlink" => ObjType::Symlink,
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

pub async fn link(
    src: &std::path::Path,
    dst: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &link::Settings,
) -> Result<link::Summary, link::Error> {
    let cwd = std::env::current_dir()
        .map_err(|err| link::Error::new(anyhow::Error::msg(err), link::Summary::default()))?;
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

#[rustfmt::skip]
fn print_runtime_stats() -> Result<(), anyhow::Error> {
    let process = procfs::process::Process::myself()?;
    let stat = process.stat()?;
    // The time is in clock ticks, so we need to convert it to seconds
    let clock_ticks_per_second = procfs::ticks_per_second();
    let ticks_to_duration = |ticks: u64| {
        std::time::Duration::from_secs_f64(ticks as f64 / clock_ticks_per_second as f64)
    };
    let vmhwm = process.status()?.vmhwm.unwrap_or(0);
    println!("walltime : {:.2?}", &PROGRESS.get_duration(),);
    println!("cpu time : {:.2?} | k: {:.2?} | u: {:.2?}", ticks_to_duration(stat.utime + stat.stime), ticks_to_duration(stat.stime), ticks_to_duration(stat.utime));
    println!("peak RSS : {:.2?}", bytesize::ByteSize(vmhwm));
    Ok(())
}

fn get_max_open_files() -> Result<u64, std::io::Error> {
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // Safety: we pass a valid "rlim" pointer and the result is checked
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
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

pub fn generate_debug_log_filename(prefix: &str) -> String {
    let now = chrono::Utc::now();
    let timestamp = now.format("%Y-%m-%dT%H:%M:%S").to_string();
    let process_id = std::process::id();
    format!("{prefix}-{timestamp}-{process_id}")
}

#[instrument(skip(func))] // "func" is not Debug printable
#[allow(clippy::too_many_arguments)]
pub fn run<Fut, Summary, Error>(
    progress: Option<ProgressSettings>,
    quiet: bool,
    verbose: u8,
    print_summary: bool,
    max_workers: usize,
    max_blocking_threads: usize,
    max_open_files: Option<usize>,
    ops_throttle: usize,
    iops_throttle: usize,
    chunk_size: u64,
    remote_tracing_layer: Option<remote_tracing::RemoteTracingLayer>,
    debug_log_file: Option<String>,
    func: impl FnOnce() -> Fut,
) -> Option<Summary>
// we return an Option rather than a Result to indicate that callers of this function should NOT print the error
where
    Summary: std::fmt::Display,
    Error: std::fmt::Display + std::fmt::Debug,
    Fut: std::future::Future<Output = Result<Summary, Error>>,
{
    if !quiet {
        let env_filter =
            tracing_subscriber::EnvFilter::from_default_env().add_directive(match verbose {
                0 => "error".parse().unwrap(),
                1 => "info".parse().unwrap(),
                2 => "debug".parse().unwrap(),
                _ => "trace".parse().unwrap(),
            });
        let file_layer = if let Some(ref log_file_path) = debug_log_file {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_file_path)
                .unwrap_or_else(|e| {
                    panic!("Failed to create debug log file at '{log_file_path}': {e}")
                });
            let file_layer = tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_ansi(false)
                .with_writer(file)
                .with_filter(
                    tracing_subscriber::EnvFilter::from_default_env().add_directive(
                        match verbose {
                            0 => "error".parse().unwrap(),
                            1 => "info".parse().unwrap(),
                            2 => "debug".parse().unwrap(),
                            _ => "trace".parse().unwrap(),
                        },
                    ),
                );
            Some(file_layer)
        } else {
            None
        };
        let fmt_layer = if remote_tracing_layer.is_some() {
            None
        } else {
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_line_number(true)
                .with_span_events(if verbose > 2 {
                    FmtSpan::NEW | FmtSpan::CLOSE
                } else {
                    FmtSpan::NONE
                })
                .pretty()
                .with_writer(ProgWriter::new);
            Some(fmt_layer)
        };
        let is_console_enabled = match std::env::var("RCP_TOKIO_TRACING_CONSOLE_ENABLED") {
            Ok(val) => matches!(val.to_lowercase().as_str(), "true" | "1"),
            Err(_) => false,
        };
        let console_layer = if is_console_enabled {
            let console_port: u16 =
                read_env_or_default("RCP_TOKIO_TRACING_CONSOLE_SERVER_PORT", 6669);
            let retention_seconds: u64 =
                read_env_or_default("RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS", 60);
            let console_layer = console_subscriber::ConsoleLayer::builder()
                .retention(std::time::Duration::from_secs(retention_seconds))
                .server_addr(([127, 0, 0, 1], console_port))
                .spawn();
            Some(console_layer)
        } else {
            None
        };
        tracing_subscriber::registry()
            .with(env_filter)
            .with(file_layer)
            .with(fmt_layer)
            .with(remote_tracing_layer)
            .with(console_layer)
            .init();
    } else {
        assert!(
            verbose == 0,
            "Quiet mode and verbose mode are mutually exclusive"
        );
    }
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if max_workers > 0 {
        builder.worker_threads(max_workers);
    }
    if max_blocking_threads > 0 {
        builder.max_blocking_threads(max_blocking_threads);
    }
    if !sysinfo::set_open_files_limit(isize::MAX) {
        tracing::info!("Failed to update the open files limit (expected on non-linux targets)");
    }
    let set_max_open_files = max_open_files.unwrap_or_else(|| {
        let limit = get_max_open_files().expect(
            "We failed to query rlimit, if this is expected try specifying --max-open-files",
        ) as usize;
        80 * limit / 100 // ~80% of the max open files limit
    });
    if set_max_open_files > 0 {
        tracing::info!("Setting max open files to: {}", set_max_open_files);
        throttle::set_max_open_files(set_max_open_files);
    } else {
        tracing::info!("Not applying any limit to max open files!");
    }
    let runtime = builder.build().expect("Failed to create runtime");
    fn get_replenish_interval(replenish: usize) -> (usize, std::time::Duration) {
        let mut replenish = replenish;
        let mut interval = std::time::Duration::from_secs(1);
        while replenish > 100 && interval > std::time::Duration::from_millis(1) {
            replenish /= 10;
            interval /= 10;
        }
        (replenish, interval)
    }
    if ops_throttle > 0 {
        let (replenish, interval) = get_replenish_interval(ops_throttle);
        throttle::init_ops_tokens(replenish);
        runtime.spawn(throttle::run_ops_replenish_thread(replenish, interval));
    }
    if iops_throttle > 0 {
        if chunk_size == 0 {
            tracing::error!("Chunk size must be specified when using --iops-throttle");
            return None;
        }
        let (replenish, interval) = get_replenish_interval(iops_throttle);
        throttle::init_iops_tokens(replenish);
        runtime.spawn(throttle::run_iops_replenish_thread(replenish, interval));
    } else if chunk_size > 0 {
        tracing::error!(
            "--chunk-size > 0 but --iops-throttle is 0 -- did you intend to use --iops-throttle?"
        );
        return None;
    }
    let res = {
        let _progress = progress.map(|settings| {
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
    if print_summary || verbose > 0 {
        if let Err(err) = print_runtime_stats() {
            println!("Failed to print runtime stats: {err:?}");
        }
    }
    res.ok()
}
