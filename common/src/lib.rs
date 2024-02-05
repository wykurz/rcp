#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

use anyhow::Result;
use std::future::Future;

mod copy;
mod progress;
mod rm;
mod testutils;

pub use copy::CopySummary;
pub use copy::LinkSummary;
pub use copy::Settings as CopySettings;
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

pub async fn copy(
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &copy::Settings,
) -> Result<CopySummary> {
    copy::copy(&PROGRESS, src, dst, settings).await
}

pub async fn rm(path: &std::path::Path, settings: &rm::Settings) -> Result<RmSummary> {
    rm::rm(&PROGRESS, path, settings).await
}

pub async fn link(
    src: &std::path::Path,
    dst: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &copy::Settings,
) -> Result<LinkSummary> {
    copy::link(&PROGRESS, src, dst, update, settings).await
}

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
        env_logger::Builder::new()
            .target(env_logger::Target::Stdout)
            .filter_level(match verbose {
                0 => log::LevelFilter::Error,
                1 => log::LevelFilter::Info,
                2 => log::LevelFilter::Debug,
                _ => log::LevelFilter::Trace,
            })
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
        info!("Failed to update the open files limit (expeted on non-linux targets)");
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
