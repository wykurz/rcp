//! Resource throttling and rate limiting for file operations
//!
//! This crate provides throttling mechanisms to control resource usage during file operations.
//! It helps prevent system overload and allows for controlled resource consumption when working
//! with large filesets or in resource-constrained environments.
//!
//! # Overview
//!
//! The throttle system provides three types of rate limiting:
//!
//! 1. **Open Files Limit** - Controls the maximum number of simultaneously open files
//! 2. **Operations Throttle** - Limits the number of operations per second
//! 3. **I/O Operations Throttle** - Limits the number of I/O operations per second based on chunk size
//!
//! All throttling is implemented using token-bucket semaphores that are automatically replenished
//! at configured intervals.
//!
//! # Usage Patterns
//!
//! ## Open Files Limit
//!
//! Prevents exceeding system file descriptor limits by controlling concurrent file operations:
//!
//! ```rust,no_run
//! use throttle::{set_max_open_files, open_file_permit};
//!
//! # async fn example() {
//! // Configure max open files (typically 80% of system limit)
//! set_max_open_files(8000);
//!
//! // Acquire permit before opening file
//! let _guard = open_file_permit().await;
//! // Open file here - permit is automatically released when guard is dropped
//! # }
//! ```
//!
//! ## Operations Throttling
//!
//! Limits general operations per second to reduce system load:
//!
//! ```rust,no_run
//! use throttle::{init_ops_tokens, run_ops_replenish_thread, get_ops_token};
//! use std::time::Duration;
//!
//! # async fn example() {
//! // Initialize with 100 operations per second
//! let ops_per_interval = 10;
//! let interval = Duration::from_millis(100); // 10 tokens / 100ms = 100/sec
//!
//! init_ops_tokens(ops_per_interval);
//!
//! // Start replenishment in background
//! tokio::spawn(run_ops_replenish_thread(ops_per_interval, interval));
//!
//! // Acquire token before each operation
//! get_ops_token().await;
//! // Perform operation here
//! # }
//! ```
//!
//! ## I/O Operations Throttling
//!
//! Limits I/O operations based on file size and chunk size, useful for bandwidth control:
//!
//! ```rust,no_run
//! use throttle::{init_iops_tokens, run_iops_replenish_thread, get_file_iops_tokens};
//! use std::time::Duration;
//!
//! # async fn example() {
//! // Initialize with desired IOPS limit
//! let iops_per_interval = 100;
//! let interval = Duration::from_millis(100);
//! let chunk_size = 64 * 1024; // 64 KB chunks
//!
//! init_iops_tokens(iops_per_interval);
//! tokio::spawn(run_iops_replenish_thread(iops_per_interval, interval));
//!
//! // For a 1 MB file with 64 KB chunks: requires 16 tokens
//! let file_size = 1024 * 1024;
//! get_file_iops_tokens(chunk_size, file_size).await;
//! // Copy file here
//! # }
//! ```
//!
//! # Token Calculation
//!
//! For I/O throttling, the number of tokens required for a file is calculated as:
//!
//! ```text
//! tokens = ⌈file_size / chunk_size⌉
//! ```
//!
//! This allows throttling to be proportional to the amount of data transferred.
//!
//! # Replenishment Strategy
//!
//! Tokens are replenished using a background task that periodically adds tokens to the semaphore.
//! The replenishment rate can be tuned by adjusting:
//!
//! - **`tokens_per_interval`**: Number of tokens added each interval
//! - **interval**: Time between replenishments
//!
//! For example, to achieve 1000 ops/sec:
//! - Option 1: 100 tokens every 100ms
//! - Option 2: 10 tokens every 10ms
//!
//! The implementation automatically scales down to prevent excessive granularity while
//! maintaining the target rate.
//!
//! # Thread Safety
//!
//! All throttling mechanisms are thread-safe and can be used across multiple async tasks
//! and threads. The semaphores use efficient `parking_lot` mutexes internally.
//!
//! # Performance Considerations
//!
//! - **Open Files Limit**: No replenishment needed, permits released automatically
//! - **Ops/IOPS Throttle**: Background task overhead is minimal (~1 task per throttle type)
//! - **Token Acquisition**: Async operation that parks task when no tokens available
//!
//! # Examples
//!
//! ## Complete Throttled Copy Operation
//!
//! ```rust,no_run
//! use throttle::*;
//! use std::time::Duration;
//!
//! async fn setup_throttling() {
//!     // Limit to 80% of 10000 max files
//!     set_max_open_files(8000);
//!
//!     // 500 operations per second
//!     init_ops_tokens(50);
//!     tokio::spawn(run_ops_replenish_thread(50, Duration::from_millis(100)));
//!
//!     // 1000 IOPS (with 64KB chunks ≈ 64 MB/s)
//!     init_iops_tokens(100);
//!     tokio::spawn(run_iops_replenish_thread(100, Duration::from_millis(100)));
//! }
//!
//! async fn copy_file_throttled(size: u64) {
//!     let chunk_size = 64 * 1024;
//!
//!     // Acquire all required permits
//!     get_ops_token().await;
//!     get_file_iops_tokens(chunk_size, size).await;
//!     let _file_guard = open_file_permit().await;
//!
//!     // Perform copy operation
//!     // ...
//! }
//! ```

mod semaphore;

static OPEN_FILES_LIMIT: std::sync::LazyLock<semaphore::Semaphore> =
    std::sync::LazyLock::new(semaphore::Semaphore::new);
static OPS_THROTTLE: std::sync::LazyLock<semaphore::Semaphore> =
    std::sync::LazyLock::new(semaphore::Semaphore::new);
static IOPS_THROTTLE: std::sync::LazyLock<semaphore::Semaphore> =
    std::sync::LazyLock::new(semaphore::Semaphore::new);

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
        if tokens > u64::from(u32::MAX) {
            tracing::error!(
                "chunk size: {} is too small to limit throughput for files this big, size: {}",
                chunk_size,
                file_size,
            );
        } else {
            // tokens is guaranteed to be <= u32::MAX by check above
            let tokens_u32 =
                u32::try_from(tokens).expect("tokens should fit in u32 after bounds check");
            get_iops_tokens(tokens_u32).await;
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
