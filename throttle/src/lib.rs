#[macro_use]
extern crate lazy_static;

mod semaphore;

lazy_static! {
    static ref OPEN_FILES_LIMIT: semaphore::Semaphore = semaphore::Semaphore::new();
    static ref OPS_THROTTLE: semaphore::Semaphore = semaphore::Semaphore::new();
    static ref IOPS_THROTTLE: semaphore::Semaphore = semaphore::Semaphore::new();
}

pub fn set_max_open_files(max_open_files: usize) {
    OPEN_FILES_LIMIT.setup(max_open_files);
}

pub struct OpenFileGuard {
    _permit: Option<tokio::sync::SemaphorePermit<'static>>,
}

pub async fn open_file_permit() -> OpenFileGuard {
    OpenFileGuard {
        _permit: OPEN_FILES_LIMIT.acquire().await,
    }
}

pub fn init_ops_tokens(ops_tokens: usize) {
    OPS_THROTTLE.setup(ops_tokens);
}

pub fn init_iops_tokens(ops_tokens: usize) {
    IOPS_THROTTLE.setup(ops_tokens);
}

pub async fn get_ops_token() {
    OPS_THROTTLE.consume().await;
}

async fn get_iops_tokens(tokens: u32) {
    IOPS_THROTTLE.consume_many(tokens).await;
}

pub async fn get_file_iops_tokens(chunk_size: u64, file_size: u64) {
    if chunk_size > 0 {
        let tokens = 1 + (std::cmp::max(1, file_size) - 1) / chunk_size;
        if tokens > u32::MAX as u64 {
            tracing::error!(
                "chunk size: {} is too small to limit throughput for files this big, size: {}",
                chunk_size,
                file_size,
            );
        } else {
            get_iops_tokens(tokens as u32).await;
        }
    }
}

pub async fn run_ops_replenish_thread(replenish: usize, interval: std::time::Duration) {
    OPS_THROTTLE.run_replenish_thread(replenish, interval).await;
}

pub async fn run_iops_replenish_thread(replenish: usize, interval: std::time::Duration) {
    IOPS_THROTTLE
        .run_replenish_thread(replenish, interval)
        .await;
}
