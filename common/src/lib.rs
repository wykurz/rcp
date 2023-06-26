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
    max_width: usize,
) -> Result<()> {
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_clone = done.clone();
    let pbar_thread = std::thread::spawn(move || {
        if !show_progress {
            return;
        }
        let pbar = indicatif::ProgressBar::new(0);
        loop {
            if done_clone.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            let progress_status = PROGRESS.get();
            pbar.set_length(progress_status.started);
            pbar.set_position(progress_status.finished);
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    });
    copy::copy(&PROGRESS, src, dst, max_width).await?;
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    pbar_thread.join().unwrap();
    Ok(())
}
