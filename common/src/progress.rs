use tracing::{event, instrument, Level};

#[derive(Debug)]
pub struct TlsCounter {
    // mutex is used primarily from one thread, so it's not a bottleneck
    count: thread_local::ThreadLocal<std::sync::Mutex<u64>>,
}

impl TlsCounter {
    pub fn new() -> Self {
        Self {
            count: thread_local::ThreadLocal::new(),
        }
    }

    pub fn inc(&self) {
        let mutex = self.count.get_or(|| std::sync::Mutex::new(0));
        let mut guard = mutex.lock().unwrap();
        *guard += 1;
    }

    pub fn get(&self) -> u64 {
        self.count.iter().fold(0, |x, y| x + *y.lock().unwrap())
    }
}

impl Default for TlsCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ProgressCounter {
    started: TlsCounter,
    finished: TlsCounter,
}

impl Default for ProgressCounter {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ProgressGuard<'a> {
    progress: &'a ProgressCounter,
}

impl<'a> ProgressGuard<'a> {
    pub fn new(progress: &'a ProgressCounter) -> Self {
        progress.started.inc();
        Self { progress }
    }
}

impl Drop for ProgressGuard<'_> {
    fn drop(&mut self) {
        self.progress.finished.inc();
    }
}

pub struct Status {
    pub started: u64,
    pub finished: u64,
}

impl ProgressCounter {
    pub fn new() -> Self {
        Self {
            started: TlsCounter::new(),
            finished: TlsCounter::new(),
        }
    }

    pub fn guard(&self) -> ProgressGuard {
        ProgressGuard::new(self)
    }

    #[instrument]
    pub fn get(&self) -> Status {
        let mut status = Status {
            started: self.started.get(),
            finished: self.finished.get(),
        };
        if status.finished > status.started {
            event!(
                Level::DEBUG,
                "Progress inversion - started: {}, finished {}",
                status.started,
                status.finished
            );
            status.started = status.finished;
        }
        status
    }
}

pub struct Progress {
    ops: ProgressCounter,
    bytes: ProgressCounter,
    hard_links_created: ProgressCounter,
    directories_created: ProgressCounter,
    symlinks_created: ProgressCounter,
    files_copied: ProgressCounter,
    symlinks_removed: ProgressCounter,
    files_removed: ProgressCounter,
    directories_removed: ProgressCounter,
    unchanged: ProgressCounter,
    start_time: std::time::Instant,
}

impl Progress {
    pub fn new() -> Self {
        Self {
            ops: Default::default(),
            bytes: Default::default(),
            hard_links_created: Default::default(),
            directories_created: Default::default(),
            symlinks_created: Default::default(),
            files_copied: Default::default(),
            symlinks_removed: Default::default(),
            files_removed: Default::default(),
            directories_removed: Default::default(),
            unchanged: Default::default(),
            start_time: std::time::Instant::now(),
        }
    }

    // TODO: move to a CopyPrinter object (that implements Display) and store things like the last query time, last ops, etc.
    pub fn print_copy_progress(&self, since: &std::time::Instant) -> String {

        // let time_now = std::time::Instant::now();
        // let finished = progress_status.finished;
        // let in_progress = progress_status.started - progress_status.finished;
        // let avarage_rate = finished as f64 / time_started.elapsed().as_secs_f64();
        // let current_rate =
        //     (finished - pbar.position()) as f64 / (time_now - last_update).as_secs_f64();

        let time_now = std::time::Instant::now();
        let ops = self.ops.get();
        let avarage_rate = ops.finished as f64 / since.elapsed().as_secs_f64();
        let current_rate =
            (ops.finished - pbar.position()) as f64 / (time_now - last_update).as_secs_f64();

        format!("pending: {} | average: {:.2} items/s | current: {:.2} items/s | copied: {} | hard_links_created: {} | directories_created: {} | symlinks_created: {} | files_copied: {} | symlinks_removed: {} | files_removed: {} | directories_removed: {} | unchanged: {} | duration: {:?}",
        ops.started - ops.finished, // pending
        avarage_rate, // average
        self.bytes.get().started,
        self.hard_links_created.get().started,
        self.directories_created.get().started,
        self.symlinks_created.get().started,
        self.files_copied.get().started,
        self.symlinks_removed.get().started,
        self.files_removed.get().started,
        self.directories_removed.get().started,
        self.unchanged.get().started,
        self.get_duration())
    }

    pub fn get_duration(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn basic_counting() -> Result<()> {
        let tls_counter = TlsCounter::new();
        for _ in 0..10 {
            tls_counter.inc();
        }
        assert!(tls_counter.get() == 10);
        Ok(())
    }

    #[test]
    fn threaded_counting() -> Result<()> {
        let tls_counter = TlsCounter::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..10 {
                handles.push(scope.spawn(|| {
                    for _ in 0..100 {
                        tls_counter.inc();
                    }
                }));
            }
        });
        assert!(tls_counter.get() == 1000);
        Ok(())
    }

    #[test]
    fn basic_guard() -> Result<()> {
        let tls_progress = ProgressCounter::new();
        let _guard = tls_progress.guard();
        Ok(())
    }
}
