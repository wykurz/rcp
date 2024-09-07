#[macro_use]
extern crate lazy_static;

use anyhow::anyhow;
use anyhow::Context;
use cmp::ObjType;
use std::fmt;
use std::io::IsTerminal;
use tracing::{event, instrument, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

mod cmp;
mod copy;
mod filecmp;
mod link;
mod preserve;
mod progress;
mod rm;
mod testutils;
mod throttle;

pub use cmp::CmpResult;
pub use cmp::CmpSettings;
pub use cmp::CmpSummary;
pub use cmp::LogWriter;
pub use cmp::ObjCmpSettings;
pub use copy::CopyError;
pub use copy::CopySettings;
pub use copy::CopySummary;
pub use link::LinkError;
pub use link::LinkSettings;
pub use link::LinkSummary;
pub use preserve::{preserve_all, preserve_default, PreserveSettings};
pub use rm::RmError;
pub use rm::RmSettings;
pub use rm::RmSummary;

lazy_static! {
    static ref PROGRESS: progress::TlsProgress = progress::TlsProgress::new();
}

struct ProgressTracker {
    lock_cvar: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    pbar_thread: Option<std::thread::JoinHandle<()>>,
}

#[derive(Copy, Clone, Default)]
pub enum ProgressType {
    #[default]
    Auto,
    ProgressBar,
    TextUpdates,
}

impl std::fmt::Debug for ProgressType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProgressType::Auto => write!(f, "Auto"),
            ProgressType::ProgressBar => write!(f, "ProgressBar"),
            ProgressType::TextUpdates => write!(f, "TextUpdates"),
        }
    }
}

impl fmt::Display for ProgressType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProgressType::Auto => write!(f, "Auto"),
            ProgressType::ProgressBar => write!(f, "ProgressBar"),
            ProgressType::TextUpdates => write!(f, "TextUpdates"),
        }
    }
}

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

fn progress_bar(lock: &std::sync::Mutex<bool>, cvar: &std::sync::Condvar, op_name: &str) {
    let pbar = indicatif::ProgressBar::new_spinner();
    pbar.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    let time_started = std::time::Instant::now();
    let mut last_update = time_started;
    let mut is_done = lock.lock().unwrap();
    loop {
        let progress_status = PROGRESS.get();
        let time_now = std::time::Instant::now();
        let finished = progress_status.finished;
        let in_progress = progress_status.started - progress_status.finished;
        let avarage_rate = finished as f64 / time_started.elapsed().as_secs_f64();
        let current_rate =
            (finished - pbar.position()) as f64 / (time_now - last_update).as_secs_f64();
        pbar.set_position(finished);
        pbar.set_message(format!(
            "done: {} | {}: {} | average: {:.2} items/s | current: {:.2} items/s",
            finished, op_name, in_progress, avarage_rate, current_rate
        ));
        last_update = time_now;
        let result = cvar
            .wait_timeout(is_done, std::time::Duration::from_millis(200))
            .unwrap();
        is_done = result.0;
        if *is_done {
            break;
        }
    }
    pbar.finish_and_clear();
}

fn text_updates(lock: &std::sync::Mutex<bool>, cvar: &std::sync::Condvar, op_name: &str) {
    let time_started = std::time::Instant::now();
    let mut last_update = time_started;
    let mut prev_finished = 0;
    let mut is_done = lock.lock().unwrap();
    loop {
        let progress_status = PROGRESS.get();
        let time_now = std::time::Instant::now();
        let finished = progress_status.finished;
        let in_progress = progress_status.started - progress_status.finished;
        let avarage_rate = finished as f64 / time_started.elapsed().as_secs_f64();
        let current_rate =
            (finished - prev_finished) as f64 / (time_now - last_update).as_secs_f64();
        prev_finished = finished;
        eprintln!(
            "{} :: done: {} | {}: {} | average: {:.2} items/s | current: {:.2} items/s",
            chrono::Local::now(),
            finished,
            op_name,
            in_progress,
            avarage_rate,
            current_rate
        );
        last_update = time_now;
        let result = cvar
            .wait_timeout(is_done, std::time::Duration::from_secs(10))
            .unwrap();
        is_done = result.0;
        if *is_done {
            break;
        }
    }
}

impl ProgressTracker {
    pub fn new(progress_type: ProgressType, op_name: &str) -> Self {
        let op_name = op_name.to_string();
        let lock_cvar =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let lock_cvar_clone = lock_cvar.clone();
        let pbar_thread = std::thread::spawn(move || {
            let (lock, cvar) = &*lock_cvar_clone;
            let interactive = match progress_type {
                ProgressType::Auto => std::io::stderr().is_terminal(),
                ProgressType::ProgressBar => true,
                ProgressType::TextUpdates => false,
            };
            if interactive {
                progress_bar(lock, cvar, &op_name);
            } else {
                text_updates(lock, cvar, &op_name);
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

pub fn parse_preserve_settings(
    settings: &str,
) -> Result<preserve::PreserveSettings, anyhow::Error> {
    let mut preserve_settings = preserve::PreserveSettings::default();
    for type_settings in settings.split(' ') {
        if let Some((obj_type, obj_settings)) = type_settings.split_once(':') {
            let (user_and_time_settings, mode_opt) =
                parse_type_settings(obj_settings).context(format!(
                    "parsing preserve settings: {}, type: {}",
                    obj_settings, obj_type
                ))?;
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

pub fn parse_compare_settings(settings: &str) -> Result<ObjCmpSettings, anyhow::Error> {
    let mut cmp_settings = ObjCmpSettings::default();
    for type_settings in settings.split(' ') {
        if let Some((obj_type, obj_settings)) = type_settings.split_once(':') {
            let obj_cmp_settings = parse_metadata_cmp_settings(obj_settings).context(format!(
                "parsing preserve settings: {}, type: {}",
                obj_settings, obj_type
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
    settings: &cmp::CmpSettings,
) -> Result<CmpSummary, anyhow::Error> {
    cmp::cmp(&PROGRESS, src, dst, log, settings).await
}

pub async fn copy(
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &copy::CopySettings,
    preserve: &preserve::PreserveSettings,
) -> Result<CopySummary, CopyError> {
    let cwd = std::env::current_dir()
        .map_err(|err| CopyError::new(anyhow::Error::msg(err), CopySummary::default()))?;
    copy::copy(&PROGRESS, &cwd, src, dst, settings, preserve, false).await
}

pub async fn rm(path: &std::path::Path, settings: &rm::RmSettings) -> Result<RmSummary, RmError> {
    rm::rm(&PROGRESS, path, settings).await
}

pub async fn link(
    src: &std::path::Path,
    dst: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &link::LinkSettings,
) -> Result<LinkSummary, LinkError> {
    let cwd = std::env::current_dir()
        .map_err(|err| LinkError::new(anyhow::Error::msg(err), LinkSummary::default()))?;
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

#[instrument(skip(func))] // "func" is not Debug printable
#[allow(clippy::too_many_arguments)]
pub fn run<Fut, Summary, Error>(
    progress: Option<(&str, ProgressType)>,
    quiet: bool,
    verbose: u8,
    print_summary: bool,
    max_workers: usize,
    max_blocking_threads: usize,
    max_open_files: Option<usize>,
    ops_throttle: usize,
    func: impl FnOnce() -> Fut,
) -> Result<Summary, anyhow::Error>
where
    Summary: std::fmt::Display,
    Error: std::fmt::Display + std::fmt::Debug,
    Fut: std::future::Future<Output = Result<Summary, Error>>,
{
    if !quiet {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_line_number(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_filter(
                tracing_subscriber::EnvFilter::try_new(match verbose {
                    0 => "error",
                    1 => "info",
                    2 => "debug",
                    _ => "trace",
                })
                .unwrap(),
            );

        let is_console_enabled = match std::env::var("RCP_TOKIO_TRACING_CONSOLE_ENABLED") {
            Ok(val) => matches!(val.to_lowercase().as_str(), "true" | "1"),
            Err(_) => false,
        };

        let subscriber = tracing_subscriber::registry().with(fmt_layer);

        if is_console_enabled {
            let console_port: u16 =
                read_env_or_default("RCP_TOKIO_TRACING_CONSOLE_SERVER_PORT", 6669);
            let retention_seconds: u64 =
                read_env_or_default("RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS", 60);
            let console_layer = console_subscriber::ConsoleLayer::builder()
                .retention(std::time::Duration::from_secs(retention_seconds))
                .server_addr(([127, 0, 0, 1], console_port))
                .spawn();
            subscriber.with(console_layer).init();
        } else {
            subscriber.init();
        };
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
        event!(
            Level::INFO,
            "Failed to update the open files limit (expeted on non-linux targets)"
        );
    }
    let set_max_open_files = max_open_files.unwrap_or_else(|| {
        let limit = get_max_open_files().expect(
            "We failed to query rlimit, if this is expected try specifying --max-open-files",
        ) as usize;
        80 * limit / 100 // ~80% of the max open files limit
    });
    if set_max_open_files > 0 {
        event!(
            Level::INFO,
            "Setting max open files to: {}",
            set_max_open_files
        );
        throttle::set_max_open_files(set_max_open_files);
    } else {
        event!(Level::INFO, "Not applying any limit to max open files!",);
    }
    let runtime = builder.build()?;
    if ops_throttle > 0 {
        let mut replenish = ops_throttle;
        let mut interval = std::time::Duration::from_secs(1);
        while replenish > 100 && interval > std::time::Duration::from_millis(1) {
            replenish /= 10;
            interval /= 10;
        }
        throttle::set_init_tokens(replenish);
        runtime.spawn(throttle::start_replenish_thread(replenish, interval));
    }
    let res = {
        let _progress =
            progress.map(|(op_name, progress_type)| ProgressTracker::new(progress_type, op_name));
        runtime.block_on(func())
    };
    if let Err(error) = res {
        if !quiet {
            println!("{:#}", error);
            print_runtime_stats()?;
        }
        return Err(anyhow!("{}", error));
    }
    let summary = res.unwrap();
    if print_summary || verbose > 0 {
        println!("{}", &summary);
    }
    if !quiet {
        print_runtime_stats()?;
    }
    Ok(summary)
}
