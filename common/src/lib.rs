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
//!     chunk_size: 0,
//!     remote_copy_buffer_size: 0,
//! };
//! let preserve = common::preserve::preserve_default();
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
//! let log = common::cmp::LogWriter::new(None).await?;
//! let settings = common::cmp::Settings {
//!     fail_early: false,
//!     exit_early: false,
//!     compare: Default::default(),
//! };
//!
//! let summary = common::cmp(src, dst, &log, &settings).await?;
//! println!("Comparison complete: {}", summary);
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
pub mod config;
pub mod copy;
pub mod filegen;
pub mod link;
pub mod preserve;
pub mod remote_tracing;
pub mod rm;
pub mod version;

pub mod filecmp;
pub mod progress;
mod testutils;

pub use config::{OutputConfig, RuntimeConfig, ThrottleConfig, TracingConfig};
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
    if result == 0 {
        if let Ok(hostname_cstr) = std::ffi::CStr::from_bytes_until_nul(&buf) {
            if let Ok(hostname) = hostname_cstr.to_str() {
                if host == hostname {
                    return true;
                }
            }
        }
    }
    false
}

lazy_static! {
    static ref PROGRESS: progress::Progress = progress::Progress::new();
    static ref PBAR: indicatif::ProgressBar = indicatif::ProgressBar::new_spinner();
    static ref REMOTE_RUNTIME_STATS: std::sync::Mutex<Option<RemoteRuntimeStats>> =
        std::sync::Mutex::new(None);
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
    User(ProgressType),
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
            GeneralProgressType::User(pt) => write!(f, "User({pt:?})"),
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

fn get_datetime_prefix() -> String {
    chrono::Local::now()
        .format("%Y-%m-%dT%H:%M:%S%.3f%:z")
        .to_string()
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
        eprintln!("=======================");
        eprintln!(
            "{}\n--{}",
            get_datetime_prefix(),
            prog_printer.print().unwrap()
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
                "{}\n--{}",
                get_datetime_prefix(),
                printer
                    .print(source_progress, destination_progress)
                    .unwrap()
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
                _ => {
                    let interactive = match progress_type {
                        GeneralProgressType::User(ProgressType::Auto) => {
                            std::io::stderr().is_terminal()
                        }
                        GeneralProgressType::User(ProgressType::ProgressBar) => true,
                        GeneralProgressType::User(ProgressType::TextUpdates) => false,
                        GeneralProgressType::Remote(_)
                        | GeneralProgressType::RemoteMaster { .. } => {
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

#[instrument(skip(func))] // "func" is not Debug printable
pub fn run<Fut, Summary, Error>(
    progress: Option<ProgressSettings>,
    output: OutputConfig,
    runtime: RuntimeConfig,
    throttle: ThrottleConfig,
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
    // validate configuration
    if let Err(e) = throttle.validate() {
        eprintln!("Configuration error: {e}");
        return None;
    }
    // unpack configs for internal use
    let OutputConfig {
        quiet,
        verbose,
        print_summary,
    } = output;
    let RuntimeConfig {
        max_workers,
        max_blocking_threads,
    } = runtime;
    let ThrottleConfig {
        max_open_files,
        ops_throttle,
        iops_throttle,
        chunk_size: _,
    } = throttle;
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
    // guards must be kept alive for the duration of the run to ensure traces are flushed
    let mut _chrome_guard: Option<tracing_chrome::FlushGuard> = None;
    let mut _flame_guard: Option<tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>>> =
        None;
    if quiet {
        assert!(
            verbose == 0,
            "Quiet mode and verbose mode are mutually exclusive"
        );
    } else {
        // helper to create the verbose-level filter consistently
        let make_env_filter = || {
            let level_directive = match verbose {
                0 => "error".parse().unwrap(),
                1 => "info".parse().unwrap(),
                2 => "debug".parse().unwrap(),
                _ => "trace".parse().unwrap(),
            };
            // filter out noisy dependencies - they're extremely verbose at DEBUG/TRACE level
            // and not useful for debugging rcp
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(level_directive)
                .add_directive("tokio=info".parse().unwrap())
                .add_directive("runtime=info".parse().unwrap())
                .add_directive("quinn=warn".parse().unwrap())
                .add_directive("rustls=warn".parse().unwrap())
                .add_directive("h2=warn".parse().unwrap())
        };
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
                .with_timer(LocalTimeFormatter)
                .with_ansi(false)
                .with_writer(file)
                .with_filter(make_env_filter());
            Some(file_layer)
        } else {
            None
        };
        // fmt_layer for local console output (when not using remote tracing)
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
                .with_timer(LocalTimeFormatter)
                .pretty()
                .with_writer(ProgWriter::new)
                .with_filter(make_env_filter());
            Some(fmt_layer)
        };
        // apply env_filter to remote_tracing_layer so it respects verbose level
        let remote_tracing_layer =
            remote_tracing_layer.map(|layer| layer.with_filter(make_env_filter()));
        let console_layer = if tokio_console {
            let console_port = tokio_console_port.unwrap_or(6669);
            let retention_seconds: u64 =
                read_env_or_default("RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS", 60);
            eprintln!("Tokio console server listening on 127.0.0.1:{console_port}");
            let console_layer = console_subscriber::ConsoleLayer::builder()
                .retention(std::time::Duration::from_secs(retention_seconds))
                .server_addr(([127, 0, 0, 1], console_port))
                .spawn();
            Some(console_layer)
        } else {
            None
        };
        // build profile filter for chrome/flame layers
        // uses EnvFilter to capture spans from our crates at the specified level
        // while excluding noisy dependencies like tokio, quinn, h2, etc.
        let profiling_enabled = chrome_trace_prefix.is_some() || flamegraph_prefix.is_some();
        let profile_filter_str = if profiling_enabled {
            let level_str = profile_level.as_deref().unwrap_or("trace");
            // validate level is a known tracing level
            let valid_levels = ["trace", "debug", "info", "warn", "error", "off"];
            if !valid_levels.contains(&level_str.to_lowercase().as_str()) {
                eprintln!(
                    "Invalid --profile-level '{}'. Valid values: trace, debug, info, warn, error, off",
                    level_str
                );
                std::process::exit(1);
            }
            // exclude noisy deps, include everything else at the profile level
            Some(format!(
                "tokio=off,quinn=off,h2=off,hyper=off,rustls=off,{}",
                level_str
            ))
        } else {
            None
        };
        // helper to create profile filter (already validated above)
        let make_profile_filter =
            || tracing_subscriber::EnvFilter::new(profile_filter_str.as_ref().unwrap());
        // chrome tracing layer (produces JSON viewable in Perfetto UI)
        let chrome_layer = if let Some(ref prefix) = chrome_trace_prefix {
            let filename = generate_trace_filename(prefix, &trace_identifier, "json");
            eprintln!("Chrome trace will be written to: {filename}");
            let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
                .file(&filename)
                .include_args(true)
                .build();
            _chrome_guard = Some(guard);
            Some(layer.with_filter(make_profile_filter()))
        } else {
            None
        };
        // flamegraph layer (produces folded stacks for inferno)
        let flame_layer = if let Some(ref prefix) = flamegraph_prefix {
            let filename = generate_trace_filename(prefix, &trace_identifier, "folded");
            eprintln!("Flamegraph data will be written to: {filename}");
            match tracing_flame::FlameLayer::with_file(&filename) {
                Ok((layer, guard)) => {
                    _flame_guard = Some(guard);
                    Some(layer.with_filter(make_profile_filter()))
                }
                Err(e) => {
                    eprintln!("Failed to create flamegraph layer: {e}");
                    None
                }
            }
        } else {
            None
        };
        tracing_subscriber::registry()
            .with(file_layer)
            .with(fmt_layer)
            .with(remote_tracing_layer)
            .with(console_layer)
            .with(chrome_layer)
            .with(flame_layer)
            .init();
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
        let (replenish, interval) = get_replenish_interval(iops_throttle);
        throttle::init_iops_tokens(replenish);
        runtime.spawn(throttle::run_iops_replenish_thread(replenish, interval));
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
