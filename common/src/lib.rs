#[macro_use]
extern crate lazy_static;

use anyhow::Context;
use anyhow::{anyhow, Result};
use cmp::ObjType;
use std::future::Future;
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

pub use cmp::CmpSettings;
pub use cmp::CmpSummary;
pub use cmp::LogWriter;
pub use cmp::ObjCmpSettings;
pub use copy::CopySettings;
pub use copy::CopySummary;
pub use link::LinkSettings;
pub use link::LinkSummary;
pub use preserve::{preserve_all, preserve_default, PreserveSettings};
pub use rm::RmSummary;
pub use rm::Settings as RmSettings;

lazy_static! {
    static ref PROGRESS: progress::TlsProgress = progress::TlsProgress::new();
}

struct ProgressTracker {
    done: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pbar_thread: Option<std::thread::JoinHandle<()>>,
}

impl ProgressTracker {
    pub fn new(op_name: &str) -> Self {
        let op_name = op_name.to_string();
        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_clone = done.clone();
        let pbar_thread = std::thread::spawn(move || {
            let pbar = indicatif::ProgressBar::new_spinner();
            pbar.set_style(
                indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            let time_started = std::time::Instant::now();
            let mut last_update = time_started;
            loop {
                if done_clone.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
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
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
        });
        Self {
            done,
            pbar_thread: Some(pbar_thread),
        }
    }
}

impl Drop for ProgressTracker {
    fn drop(&mut self) {
        self.done.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(pbar_thread) = self.pbar_thread.take() {
            pbar_thread.join().unwrap();
        }
    }
}

pub fn parse_metadata_cmp_settings(settings: &str) -> Result<filecmp::MetadataCmpSettings> {
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
) -> Result<(preserve::UserAndTimeSettings, Option<preserve::ModeMask>)> {
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

pub fn parse_preserve_settings(settings: &str) -> Result<preserve::PreserveSettings> {
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

pub fn parse_compare_settings(settings: &str) -> Result<ObjCmpSettings> {
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
) -> Result<CmpSummary> {
    cmp::cmp(&PROGRESS, src, dst, log, settings).await
}

pub async fn copy(
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &copy::CopySettings,
    preserve: &preserve::PreserveSettings,
) -> Result<CopySummary> {
    let cwd = std::env::current_dir()?;
    copy::copy(&PROGRESS, &cwd, src, dst, settings, preserve, false).await
}

pub async fn rm(path: &std::path::Path, settings: &rm::Settings) -> Result<RmSummary> {
    rm::rm(&PROGRESS, path, settings).await
}

pub async fn link(
    src: &std::path::Path,
    dst: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &link::LinkSettings,
) -> Result<LinkSummary> {
    let cwd = std::env::current_dir()?;
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

#[instrument(skip(func))] // "func" is not Debug printable
pub fn run<Fut, Summary>(
    progress_op_name: Option<&str>,
    quiet: bool,
    verbose: u8,
    summary: bool,
    max_workers: usize,
    max_blocking_threads: usize,
    func: impl FnOnce() -> Fut,
) -> Result<Summary>
where
    Summary: std::fmt::Display,
    Fut: Future<Output = Result<Summary>>,
{
    let _progress = progress_op_name.map(ProgressTracker::new);
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
    let runtime = builder.build()?;
    let res = runtime.block_on(func());
    if let Err(error) = res {
        if !quiet {
            eprintln!("{:#}", error);
        }
        std::process::exit(1);
    }
    if summary || verbose > 0 {
        println!("{}", res.unwrap());
    }
    std::process::exit(0);
}
