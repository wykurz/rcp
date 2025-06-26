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

    pub fn add(&self, value: u64) {
        let mutex = self.count.get_or(|| std::sync::Mutex::new(0));
        let mut guard = mutex.lock().unwrap();
        *guard += value;
    }

    pub fn inc(&self) {
        self.add(1);
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
    pub ops: ProgressCounter,
    pub bytes_copied: TlsCounter,
    pub hard_links_created: TlsCounter,
    pub files_copied: TlsCounter,
    pub symlinks_created: TlsCounter,
    pub directories_created: TlsCounter,
    pub files_unchanged: TlsCounter,
    pub symlinks_unchanged: TlsCounter,
    pub directories_unchanged: TlsCounter,
    pub hard_links_unchanged: TlsCounter,
    pub files_removed: TlsCounter,
    pub symlinks_removed: TlsCounter,
    pub directories_removed: TlsCounter,
    start_time: std::time::Instant,
}

impl Progress {
    pub fn new() -> Self {
        Self {
            ops: Default::default(),
            bytes_copied: Default::default(),
            hard_links_created: Default::default(),
            files_copied: Default::default(),
            symlinks_created: Default::default(),
            directories_created: Default::default(),
            files_unchanged: Default::default(),
            symlinks_unchanged: Default::default(),
            directories_unchanged: Default::default(),
            hard_links_unchanged: Default::default(),
            files_removed: Default::default(),
            symlinks_removed: Default::default(),
            directories_removed: Default::default(),
            start_time: std::time::Instant::now(),
        }
    }

    pub fn get_duration(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }
}

impl Default for Progress {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ProgressPrinter<'a> {
    progress: &'a Progress,
    last_ops: u64,
    last_bytes: u64,
    last_update: std::time::Instant,
}

impl<'a> ProgressPrinter<'a> {
    pub fn new(progress: &'a Progress) -> Self {
        Self {
            progress,
            last_ops: progress.ops.get().finished,
            last_bytes: progress.bytes_copied.get(),
            last_update: std::time::Instant::now(),
        }
    }

    pub fn print(&mut self) -> anyhow::Result<String> {
        let time_now = std::time::Instant::now();
        let ops = self.progress.ops.get();
        let total_duration_secs = self.progress.get_duration().as_secs_f64();
        let curr_duration_secs = (time_now - self.last_update).as_secs_f64();
        let avarage_ops_rate = ops.finished as f64 / total_duration_secs;
        let current_ops_rate = (ops.finished - self.last_ops) as f64 / curr_duration_secs;
        let bytes = self.progress.bytes_copied.get();
        let avarage_bytes_rate = bytes as f64 / total_duration_secs;
        let current_bytes_rate = (bytes - self.last_bytes) as f64 / curr_duration_secs;
        // update self
        self.last_ops = ops.finished;
        self.last_bytes = bytes;
        self.last_update = time_now;
        // nice to have: convert to a table
        Ok(format!(
            "---------------------\n\
            OPS:\n\
            pending: {:>10}\n\
            average: {:>10.2} items/s\n\
            current: {:>10.2} items/s\n\
            -----------------------\n\
            COPIED:\n\
            average: {:>10}/s\n\
            current: {:>10}/s\n\
            total:   {:>10}\n\
            \n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}\n\
            hard-links:  {:>10}\n\
            -----------------------\n\
            UNCHANGED:\n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}\n\
            hard-links:  {:>10}\n\
            -----------------------\n\
            REMOVED:\n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}",
            ops.started - ops.finished, // pending
            avarage_ops_rate,
            current_ops_rate,
            // copy
            bytesize::ByteSize(avarage_bytes_rate as u64),
            bytesize::ByteSize(current_bytes_rate as u64),
            bytesize::ByteSize(self.progress.bytes_copied.get()),
            self.progress.files_copied.get(),
            self.progress.symlinks_created.get(),
            self.progress.directories_created.get(),
            self.progress.hard_links_created.get(),
            // unchanged
            self.progress.files_unchanged.get(),
            self.progress.symlinks_unchanged.get(),
            self.progress.directories_unchanged.get(),
            self.progress.hard_links_unchanged.get(),
            // remove
            self.progress.files_removed.get(),
            self.progress.symlinks_removed.get(),
            self.progress.directories_removed.get(),
        ))
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
