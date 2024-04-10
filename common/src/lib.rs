#[macro_use]
extern crate lazy_static;

use anyhow::{anyhow, Result};
use std::future::Future;
use tracing::{event, instrument, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

mod copy;
mod filecmp;
mod link;
mod preserve;
mod progress;
mod rm;
mod testutils;

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

pub async fn copy(
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &copy::CopySettings,
    preserve: &preserve::PreserveSettings,
) -> Result<CopySummary> {
    let cwd = std::env::current_dir()?;
    copy::copy(&PROGRESS, &cwd, src, dst, settings, preserve).await
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
    link::link(&PROGRESS, &cwd, src, dst, update, settings).await
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
            eprintln!("{}", error);
        }
        std::process::exit(1);
    }
    if summary || verbose > 0 {
        println!("{}", res.unwrap());
    }
    std::process::exit(0);
}
