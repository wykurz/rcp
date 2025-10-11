use std::sync::atomic::{AtomicBool, Ordering};

pub struct Semaphore {
    flag: std::sync::Arc<AtomicBool>,
    sem: tokio::sync::Semaphore,
}

impl Semaphore {
    pub fn new() -> Self {
        let flag = std::sync::Arc::new(AtomicBool::new(false));
        let sem = tokio::sync::Semaphore::const_new(tokio::sync::Semaphore::MAX_PERMITS);
        Self { flag, sem }
    }

    pub fn setup(&self, value: usize) {
        self.flag.store(value > 0, Ordering::Release);
        if value == 0 {
            return;
        }
        self.sem.forget_permits(self.sem.available_permits());
        self.sem.add_permits(value);
    }

    pub async fn acquire(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        if self.flag.load(Ordering::Acquire) {
            Some(self.sem.acquire().await.unwrap())
        } else {
            None
        }
    }

    pub async fn consume(&self) {
        if self.flag.load(Ordering::Acquire) {
            self.sem.acquire().await.unwrap().forget();
        }
    }

    pub async fn consume_many(&self, value: u32) {
        if self.flag.load(Ordering::Acquire) {
            self.sem.acquire_many(value).await.unwrap().forget();
        }
    }

    pub async fn run_replenish_thread(&self, replenish: usize, interval: std::time::Duration) {
        if !self.flag.load(Ordering::Acquire) {
            return;
        }
        loop {
            tokio::time::sleep(interval).await;
            let curr_permits = self.sem.available_permits();
            if curr_permits >= replenish {
                continue;
            }
            self.sem.add_permits(replenish - curr_permits);
        }
    }
}
