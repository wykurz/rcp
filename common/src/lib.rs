#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

use anyhow::Result;

mod copy;
mod progress;
mod rm;

pub use copy::Settings as CopySettings;
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
    show_progress: bool,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &copy::Settings,
) -> Result<()> {
    let _progress = match show_progress {
        true => Some(ProgressTracker::new("copy")),
        false => None,
    };
    copy::copy(&PROGRESS, src, dst, settings).await?;
    Ok(())
}

pub async fn rm(
    show_progress: bool,
    path: &std::path::Path,
    settings: &rm::Settings,
) -> Result<()> {
    let _progress = match show_progress {
        true => Some(ProgressTracker::new("remove")),
        false => None,
    };
    rm::rm(&PROGRESS, path, settings).await?;
    Ok(())
}
