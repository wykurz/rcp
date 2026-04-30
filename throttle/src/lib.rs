//! Resource throttling and rate limiting for file operations
//!
//! This crate provides throttling mechanisms to control resource usage during file operations.
//! It helps prevent system overload and allows for controlled resource consumption when working with large filesets or in resource-constrained environments.
//!
//! # Overview
//!
//! The throttle system provides three types of rate limiting:
//!
//! 1. **Open Files Limit** - Controls the maximum number of simultaneously open files
//! 2. **Operations Throttle** - Limits the number of operations per second
//! 3. **I/O Operations Throttle** - Limits the number of I/O operations per second based on chunk size
//!
//! All throttling is implemented using token-bucket semaphores that are automatically replenished at configured intervals.
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
//! The implementation automatically scales down to prevent excessive granularity while maintaining the target rate.
//!
//! # Thread Safety
//!
//! All throttling mechanisms are thread-safe and can be used across multiple async tasks and threads. The semaphores use efficient `parking_lot` mutexes internally.
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

/// Which filesystem side a metadata syscall touches.
///
/// Mirrors the congestion crate's `Side`; this enum is intentionally
/// independent so `throttle` has no dependency on `congestion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Side {
    Source = 0,
    Destination = 1,
}

/// Which metadata syscall a permit / cap belongs to.
///
/// Mirrors `congestion::MetadataOp` variant-for-variant. Each crate
/// indexes its own flat array using its own enum, so the per-crate
/// discriminants drive routing inside that crate; the cross-crate
/// translation goes through name-based bridge functions in
/// `common::walk`. Adding a variant here without a matching one in
/// `congestion` (or vice versa) is caught at compile time by the
/// exhaustive matches in those bridges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MetadataOp {
    Stat = 0,
    ReadLink = 1,
    MkDir = 2,
    RmDir = 3,
    Unlink = 4,
    HardLink = 5,
    Symlink = 6,
    Chmod = 7,
    OpenCreate = 8,
}

/// Number of [`MetadataOp`] variants. Keep in sync when adding variants.
pub const N_META_OPS: usize = 9;
/// Number of [`Side`] variants.
pub const N_SIDES: usize = 2;
/// Total number of distinct (Side, MetadataOp) controllers.
pub const N_META_RESOURCES: usize = N_META_OPS * N_SIDES;

impl MetadataOp {
    /// All op variants, in discriminant order.
    pub const ALL: [Self; N_META_OPS] = [
        Self::Stat,
        Self::ReadLink,
        Self::MkDir,
        Self::RmDir,
        Self::Unlink,
        Self::HardLink,
        Self::Symlink,
        Self::Chmod,
        Self::OpenCreate,
    ];
}

impl Side {
    /// All side variants, in discriminant order.
    pub const ALL: [Self; N_SIDES] = [Self::Source, Self::Destination];
}

/// Which throttled metadata resource a permit / cap belongs to.
///
/// Each `(Side, MetadataOp)` pair gets its own independent concurrency
/// cap so the controller for one syscall on one filesystem can adjust
/// without dragging others along — for example, `(Source, Stat)` and
/// `(Destination, Unlink)` are completely independent enforcement gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Resource {
    pub side: Side,
    pub op: MetadataOp,
}

impl Resource {
    /// Construct a metadata resource for the given side + op.
    pub const fn meta(side: Side, op: MetadataOp) -> Self {
        Self { side, op }
    }
    /// Map the resource to its slot in the per-resource semaphore array.
    /// Side is the major axis, op the minor — matches the corresponding
    /// fan-out layout in `congestion::RoutingSink`.
    fn index(self) -> usize {
        (self.side as usize) * N_META_OPS + (self.op as usize)
    }
}

static OPEN_FILES_LIMIT: std::sync::LazyLock<semaphore::Semaphore> =
    std::sync::LazyLock::new(semaphore::Semaphore::new);
// Spawn-time backpressure for operations that don't actually hold open
// file descriptors but still benefit from a bound on in-flight tasks
// (rm, cmp). Kept separate from OPEN_FILES_LIMIT so that an inner rm
// triggered by a copy/link path that already holds an OPEN_FILES_LIMIT
// permit cannot deadlock against itself when the outer permit pool is
// saturated. Both semaphores are sized to the same configured limit by
// `set_max_open_files`.
static PENDING_META_LIMIT: std::sync::LazyLock<semaphore::Semaphore> =
    std::sync::LazyLock::new(semaphore::Semaphore::new);
static OPS_THROTTLE: std::sync::LazyLock<semaphore::Semaphore> =
    std::sync::LazyLock::new(semaphore::Semaphore::new);
static IOPS_THROTTLE: std::sync::LazyLock<semaphore::Semaphore> =
    std::sync::LazyLock::new(semaphore::Semaphore::new);
// Per-(Side, MetadataOp) concurrency caps driven by the congestion
// controller. One semaphore per [`Resource`] so each syscall on each
// side is throttled independently. Distinct from OPS_THROTTLE (rate)
// and OPEN_FILES_LIMIT (FDs).
//
// `[const { ... }; N]` initializes each slot independently — without
// the inline-const block the array repeat syntax would require `Copy`,
// which `LazyLock` is not.
static OPS_IN_FLIGHT_LIMITS: [std::sync::LazyLock<semaphore::Semaphore>; N_META_RESOURCES] =
    [const { std::sync::LazyLock::new(semaphore::Semaphore::new) }; N_META_RESOURCES];

fn ops_in_flight_limit(resource: Resource) -> &'static semaphore::Semaphore {
    &OPS_IN_FLIGHT_LIMITS[resource.index()]
}

/// Configure the spawn-time concurrency caps from a single knob.
///
/// Despite the name, this sizes **two** independent semaphores:
///
/// * [`open_file_permit`] — actual file-descriptor backpressure for
///   paths that hold open fds (copy/link).
/// * [`pending_meta_permit`] — task-spawn backpressure for recursive
///   metadata-only walks (rm/cmp) that don't hold fds. Sized
///   separately so paths that compose these operations
///   (e.g. `copy_file → rm` for an overwrite of a directory
///   destination) cannot deadlock against the open-files pool.
///
/// Pass `0` to disable both caps; `setup` is idempotent and intended
/// for startup or test reset.
pub fn set_max_open_files(max_open_files: usize) {
    OPEN_FILES_LIMIT.setup(max_open_files);
    PENDING_META_LIMIT.setup(max_open_files);
}

pub struct OpenFileGuard {
    _permit: Option<semaphore::Permit<'static>>,
}

pub async fn open_file_permit() -> OpenFileGuard {
    OpenFileGuard {
        _permit: OPEN_FILES_LIMIT.acquire().await,
    }
}

/// Backpressure guard for in-flight metadata-only operations (rm, cmp).
///
/// Held across a spawned task to bound the live task count under
/// recursive walk operations that don't hold open file descriptors.
/// Kept distinct from [`OpenFileGuard`] so that paths which compose
/// these operations (e.g. `copy_file → rm` for an overwrite of a
/// directory destination) don't deadlock against a saturated
/// `OPEN_FILES_LIMIT`.
pub struct PendingMetaGuard {
    _permit: Option<semaphore::Permit<'static>>,
}

pub async fn pending_meta_permit() -> PendingMetaGuard {
    PendingMetaGuard {
        _permit: PENDING_META_LIMIT.acquire().await,
    }
}

/// Dynamically set the maximum number of concurrent operations in flight
/// for the given [`Resource`].
///
/// Increasing the cap is instant; decreasing it can temporarily overshoot
/// if permits are already held — they return naturally on drop. This is
/// the enforcement knob the adaptive controller drives.
///
/// Setting to 0 disables the cap for *subsequent* acquires (they return
/// immediately without a permit), but does not wake acquirers already
/// blocked inside the semaphore's wait queue — they remain parked until
/// permits become available. Callers that need a cancellable disable
/// should use a higher-level shutdown signal.
pub fn set_max_ops_in_flight(resource: Resource, max_in_flight: usize) {
    ops_in_flight_limit(resource).set_max(max_in_flight);
}

pub struct OpsInFlightGuard {
    _permit: Option<semaphore::Permit<'static>>,
}

/// Acquire a permit from the ops-in-flight cap for the given [`Resource`].
/// No-op (returns immediately) when that resource's cap is not configured.
pub async fn ops_in_flight_permit(resource: Resource) -> OpsInFlightGuard {
    OpsInFlightGuard {
        _permit: ops_in_flight_limit(resource).acquire().await,
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

/// Dynamically update the ops-throttle replenish count.
///
/// Takes effect on the next iteration of the replenish loop started by
/// [`run_ops_replenish_thread`] — no loop restart, no permits forcibly
/// drained. Setting `value = 0` pauses replenishment (tokens already in
/// the bucket will be consumed but no new ones will be added).
///
/// Intended for congestion-control layers that translate a Controller's
/// decisions into dynamic rate targets. For the update to visibly gate
/// ops, two things must be true:
///
/// 1. A replenish task must be running (spawned via
///    [`run_ops_replenish_thread`]); otherwise the new value is stored
///    but no token refills happen.
/// 2. The ops-throttle must be enabled (via [`init_ops_tokens`] with a
///    non-zero value, or [`enable_ops_throttle`] after a prior
///    [`disable_ops_throttle`]); otherwise [`get_ops_token`] is a no-op
///    regardless of the replenish count.
pub fn set_ops_replenish(value: usize) {
    OPS_THROTTLE.set_replenish(value);
}

/// Dynamically update the iops-throttle replenish count. See
/// [`set_ops_replenish`] for the semantics.
pub fn set_iops_replenish(value: usize) {
    IOPS_THROTTLE.set_replenish(value);
}

/// Disable the ops-throttle, making [`get_ops_token`] a no-op. Mirrors
/// the "unlimited on this dimension" semantics of `Decision` so an
/// adaptive controller can transition a previously-set rate back to "no
/// limit" by sending `rate_per_sec: None`.
///
/// The replenish loop keeps running (it has no mid-loop flag check) but
/// its token additions become inert until the flag is flipped back on
/// via [`enable_ops_throttle`].
pub fn disable_ops_throttle() {
    OPS_THROTTLE.disable();
}

/// Re-enable the ops-throttle after [`disable_ops_throttle`] — the
/// counterpart that allows a controller to toggle rate capping on and
/// off via `Decision::rate_per_sec`. Returns `true` if enablement took
/// effect, `false` if the throttle was never initialized (i.e.
/// `--ops-throttle` was not set at startup) so there is nothing to
/// enable.
pub fn enable_ops_throttle() -> bool {
    OPS_THROTTLE.enable()
}

/// Current in-flight concurrency cap for the given [`Resource`], for
/// metrics and integration tests. Returns `0` when the cap has been set
/// to zero (disabled) or has never been configured.
#[must_use]
pub fn current_ops_in_flight_limit(resource: Resource) -> usize {
    ops_in_flight_limit(resource).current_limit()
}
