use std::sync::atomic::{AtomicBool, Ordering};

lazy_static! {
    static ref ENABLE_THROTTLE: std::sync::Arc<AtomicBool> =
        std::sync::Arc::new(AtomicBool::new(false));
    static ref OPEN_FILES: tokio::sync::Semaphore =
        tokio::sync::Semaphore::const_new(tokio::sync::Semaphore::MAX_PERMITS);
}

pub fn set_max_open_files(max_open_files: usize) {
    ENABLE_THROTTLE.store(max_open_files > 0, Ordering::Release);
    if max_open_files == 0 {
        return;
    }
    OPEN_FILES.forget_permits(OPEN_FILES.available_permits());
    OPEN_FILES.add_permits(max_open_files);
}

pub struct OpeFileGuard<'a> {
    _permit: Option<tokio::sync::SemaphorePermit<'a>>,
}

impl<'a> OpeFileGuard<'a> {
    async fn new() -> Self {
        if !ENABLE_THROTTLE.load(Ordering::Acquire) {
            return Self { _permit: None };
        }
        Self {
            _permit: Some(OPEN_FILES.acquire().await.unwrap()),
        }
    }
}

pub async fn open_file_permit() -> OpeFileGuard<'static> {
    OpeFileGuard::new().await
}
