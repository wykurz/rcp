pub struct TlsCounter {
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

pub struct TlsProgress {
    started: TlsCounter,
    finished: TlsCounter,
}

pub struct ProgressGuard<'a> {
    progress: &'a TlsProgress,
}

impl<'a> ProgressGuard<'a> {
    pub fn new(progress: &'a TlsProgress) -> Self {
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

impl TlsProgress {
    pub fn new() -> Self {
        Self {
            started: TlsCounter::new(),
            finished: TlsCounter::new(),
        }
    }

    pub fn guard(&self) -> ProgressGuard {
        ProgressGuard::new(self)
    }

    pub fn get(&self) -> Status {
        let mut status = Status {
            started: self.started.get(),
            finished: self.finished.get(),
        };
        if status.finished > status.started {
            debug!(
                "Progress inversion - started: {}, finished {}",
                status.started, status.finished
            );
            status.started = status.finished;
        }
        status
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
        let tls_progress = TlsProgress::new();
        let _guard = tls_progress.guard();
        Ok(())
    }
}
