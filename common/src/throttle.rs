use std::sync::atomic::{AtomicBool, Ordering};

lazy_static! {
    static ref ENABLE_OPEN_FILES_LIMIT: std::sync::Arc<AtomicBool> =
        std::sync::Arc::new(AtomicBool::new(false));
    static ref OPEN_FILES_SEM: tokio::sync::Semaphore =
        tokio::sync::Semaphore::const_new(tokio::sync::Semaphore::MAX_PERMITS);
    static ref ENABLE_THROTTLE: std::sync::Arc<AtomicBool> =
        std::sync::Arc::new(AtomicBool::new(false));
    static ref THROTTLE_SEM: tokio::sync::Semaphore =
        tokio::sync::Semaphore::const_new(tokio::sync::Semaphore::MAX_PERMITS);
}

pub fn init_semaphore(
    value: usize,
    flag: &'static std::sync::Arc<AtomicBool>,
    sem: &tokio::sync::Semaphore,
) {
    flag.store(value > 0, Ordering::Release);
    if value == 0 {
        return;
    }
    sem.forget_permits(sem.available_permits());
    sem.add_permits(value);
}

pub fn set_max_open_files(max_open_files: usize) {
    init_semaphore(max_open_files, &ENABLE_OPEN_FILES_LIMIT, &OPEN_FILES_SEM);
}

pub struct OpenFileGuard<'a> {
    _permit: Option<tokio::sync::SemaphorePermit<'a>>,
}

impl OpenFileGuard<'_> {
    async fn new() -> Self {
        if !ENABLE_OPEN_FILES_LIMIT.load(Ordering::Acquire) {
            return Self { _permit: None };
        }
        Self {
            _permit: Some(OPEN_FILES_SEM.acquire().await.unwrap()),
        }
    }
}

pub async fn open_file_permit() -> OpenFileGuard<'static> {
    OpenFileGuard::new().await
}

pub fn set_init_tokens(init_iops_tokens: usize) {
    init_semaphore(init_iops_tokens, &ENABLE_THROTTLE, &THROTTLE_SEM);
}

pub async fn get_token() {
    if !ENABLE_THROTTLE.load(Ordering::Acquire) {
        return;
    }
    THROTTLE_SEM.acquire().await.unwrap().forget();
}

pub async fn start_replenish_thread(replenish: usize, interval: std::time::Duration) {
    if !ENABLE_THROTTLE.load(Ordering::Acquire) {
        return;
    }
    loop {
        tokio::time::sleep(interval).await;
        let curr_permits = THROTTLE_SEM.available_permits();
        if curr_permits >= replenish {
            continue;
        }
        THROTTLE_SEM.add_permits(replenish - curr_permits);
    }
}
