#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

use anyhow::Result;

mod copy;
mod progress;

lazy_static! {
    static ref PROGRESS: progress::TlsProgress = progress::TlsProgress::new();
}

pub async fn copy(
    show_progress: bool,
    src: &std::path::Path,
    dst: &std::path::Path,
    preserve: bool,
    read_buffer: usize,
) -> Result<()> {
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_clone = done.clone();
    let pbar_thread = std::thread::spawn(move || {
        if !show_progress {
            return;
        }
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
            let copying = progress_status.started - progress_status.finished;
            let avarage_rate = finished as f64 / time_started.elapsed().as_secs_f64();
            let current_rate =
                (finished - pbar.position()) as f64 / (time_now - last_update).as_secs_f64();
            pbar.set_position(finished);
            pbar.set_message(format!(
                "done: {} | copying: {} | average: {:.2} items/s | current: {:.2} items/s",
                finished, copying, avarage_rate, current_rate
            ));
            last_update = time_now;
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    });
    copy::copy(&PROGRESS, src, dst, preserve, read_buffer).await?;
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    pbar_thread.join().unwrap();
    Ok(())
}
