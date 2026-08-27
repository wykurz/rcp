#![allow(dead_code)]

use anyhow::{Context, Result};
use async_recursion::async_recursion;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::fs::MetadataExt;

static ADMISSION_LIMIT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Exclusively configures process-global admission limits for one test.
pub struct AdmissionLimit {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl AdmissionLimit {
    /// Acquires exclusive access and starts from unlimited admission state.
    pub async fn new() -> Self {
        let guard = ADMISSION_LIMIT_LOCK.lock().await;
        reset_admission_limits();
        Self { _guard: guard }
    }

    /// Sets the shared file-in-flight limits.
    pub fn set_files_in_flight(&self, files_in_flight: usize) {
        let files_in_flight = std::num::NonZeroUsize::new(files_in_flight);
        throttle::set_admission_limits(files_in_flight);
    }

    /// Sets one metadata operation's in-flight limit.
    pub fn set_max_ops_in_flight(&self, resource: throttle::Resource, max_in_flight: usize) {
        throttle::set_max_ops_in_flight(resource, max_in_flight);
    }

    /// Runs admission-sensitive work with a timeout and quiesces it before reporting expiry.
    pub async fn run_with_timeout<F>(
        &self,
        duration: std::time::Duration,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: std::future::Future,
    {
        tokio::pin!(future);
        match tokio::time::timeout(duration, future.as_mut()).await {
            Ok(output) => Ok(output),
            Err(error) => {
                self.quiesce(future).await;
                Err(error)
            }
        }
    }

    /// Removes configured limits and waits for admission-sensitive work to finish.
    pub async fn quiesce<F>(&self, future: F)
    where
        F: std::future::Future,
    {
        reset_admission_limits();
        let _ = future.await;
    }
}

impl Drop for AdmissionLimit {
    fn drop(&mut self) {
        reset_admission_limits();
    }
}

fn reset_admission_limits() {
    throttle::set_admission_limits(None);
    for side in throttle::Side::ALL {
        for op in throttle::MetadataOp::ALL {
            throttle::set_max_ops_in_flight(throttle::Resource::meta(side, op), 0);
        }
    }
    throttle::disable_ops_throttle();
    congestion::clear_sample_sink();
}

/// Signals when a blocking test owner has been dropped.
pub struct CompletionSignal(Option<tokio::sync::oneshot::Sender<()>>);

impl CompletionSignal {
    /// Creates a completion signal and its receiver.
    pub fn new() -> (Self, tokio::sync::oneshot::Receiver<()>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (Self(Some(sender)), receiver)
    }
}

impl Drop for CompletionSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

/// Waits for blocking work and then for its abandoned output to release capacity.
pub async fn await_completion_and_capacity<C, F>(
    completion: C,
    capacity: F,
) -> (C::Output, F::Output)
where
    C: std::future::Future,
    F: std::future::Future,
{
    let completion_result = completion.await;
    let capacity = capacity.await;
    (completion_result, capacity)
}

/// Captures the identity of a live file descriptor for later closure checks.
#[derive(Clone, Copy)]
pub struct FdIdentityProbe {
    raw_fd: RawFd,
    device: libc::dev_t,
    inode: libc::ino_t,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FdIdentityProbeOperation {
    Capture,
    OriginalIsClosed,
}

#[cfg(test)]
// process-global injectors require nextest isolation or libtest `--test-threads=1`; parallel
// in-process libtest is unsupported.
static FD_IDENTITY_PROBE_FAILURE: std::sync::LazyLock<
    std::sync::Mutex<Option<(FdIdentityProbeOperation, usize)>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) struct FdIdentityProbeFailureGuard;

#[cfg(test)]
pub(crate) fn inject_fd_identity_probe_failure(
    operation: FdIdentityProbeOperation,
    occurrence: usize,
) -> FdIdentityProbeFailureGuard {
    assert!(occurrence != 0, "failure occurrence must be nonzero");
    let mut failure = FD_IDENTITY_PROBE_FAILURE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        failure.is_none(),
        "an fd identity probe failure is already armed"
    );
    *failure = Some((operation, occurrence));
    FdIdentityProbeFailureGuard
}

#[cfg(test)]
impl Drop for FdIdentityProbeFailureGuard {
    fn drop(&mut self) {
        *FD_IDENTITY_PROBE_FAILURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

#[cfg(test)]
fn take_injected_fd_identity_probe_failure(
    operation: FdIdentityProbeOperation,
) -> Option<std::io::Error> {
    let mut failure = FD_IDENTITY_PROBE_FAILURE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (armed_operation, occurrence) = failure.as_mut()?;
    if *armed_operation != operation {
        return None;
    }
    if *occurrence > 1 {
        *occurrence -= 1;
        return None;
    }
    *failure = None;
    Some(std::io::Error::other("injected fd identity probe failure"))
}

impl FdIdentityProbe {
    /// Duplicates `raw_fd` with close-on-exec and records the duplicate's identity.
    pub fn capture(raw_fd: RawFd) -> std::io::Result<Self> {
        #[cfg(test)]
        if let Some(error) =
            take_injected_fd_identity_probe_failure(FdIdentityProbeOperation::Capture)
        {
            return Err(error);
        }
        // SAFETY: `F_DUPFD_CLOEXEC` reads only the descriptor integer and returns a new descriptor;
        // the kernel validates `raw_fd` and sets close-on-exec before exposing the duplicate.
        let duplicate_raw = unsafe { libc::fcntl(raw_fd, libc::F_DUPFD_CLOEXEC, 0) };
        if duplicate_raw == -1 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `duplicate_raw` was just returned by `F_DUPFD_CLOEXEC` and is owned exclusively
        // by this function until `duplicate` is dropped.
        let duplicate = unsafe { OwnedFd::from_raw_fd(duplicate_raw) };
        let identity = fd_identity(duplicate.as_raw_fd());
        drop(duplicate);
        let (device, inode) = identity?;
        Ok(Self {
            raw_fd,
            device,
            inode,
        })
    }

    /// Returns whether the captured owner is gone, including when its numeric slot was reused.
    pub fn original_is_closed(&self) -> std::io::Result<bool> {
        #[cfg(test)]
        if let Some(error) =
            take_injected_fd_identity_probe_failure(FdIdentityProbeOperation::OriginalIsClosed)
        {
            return Err(error);
        }
        match fd_identity(self.raw_fd) {
            Ok((device, inode)) => Ok((device, inode) != (self.device, self.inode)),
            Err(error) if error.raw_os_error() == Some(libc::EBADF) => Ok(true),
            Err(error) => Err(error),
        }
    }
}

fn fd_identity(raw_fd: RawFd) -> std::io::Result<(libc::dev_t, libc::ino_t)> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `stat` points to enough writable memory for `libc::stat`; the kernel validates the
    // descriptor and initializes it fully before success is reported.
    let result = unsafe { libc::fstat(raw_fd, stat.as_mut_ptr()) };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a successful `fstat` initialized the complete `libc::stat` above.
    let stat = unsafe { stat.assume_init() };
    Ok((stat.st_dev, stat.st_ino))
}

/// Records the destruction order of test-only ownership probes.
#[cfg(test)]
#[derive(Clone, Default)]
pub struct DropEvents(std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>);

/// Records one event when dropped.
#[cfg(test)]
pub struct DropEvent {
    name: &'static str,
    events: DropEvents,
}

#[cfg(test)]
impl DropEvents {
    /// Creates a probe that appends `name` when dropped.
    pub fn probe(&self, name: &'static str) -> DropEvent {
        DropEvent {
            name,
            events: self.clone(),
        }
    }

    /// Returns the events observed so far.
    pub fn snapshot(&self) -> Vec<&'static str> {
        lock_unpoisoned(&self.0).clone()
    }
}

#[cfg(test)]
impl Drop for DropEvent {
    fn drop(&mut self) {
        lock_unpoisoned(&self.events.0).push(self.name);
    }
}

#[cfg(test)]
struct BlockingPathDropBarrier {
    released: std::sync::Mutex<bool>,
    released_cv: std::sync::Condvar,
}

#[cfg(test)]
struct BlockingPathGateState {
    started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<std::os::fd::RawFd>>>,
    released: std::sync::Mutex<bool>,
    released_cv: std::sync::Condvar,
    hit_count: std::sync::atomic::AtomicUsize,
    allocation_count: std::sync::atomic::AtomicUsize,
    allocation_thread: std::sync::Mutex<Option<std::thread::ThreadId>>,
    visit: std::sync::Mutex<Option<BlockingPathGateVisit>>,
    output_drop_barrier: std::sync::Arc<BlockingPathDropBarrier>,
}

#[cfg(test)]
static BLOCKING_PATH_GATES: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<std::path::PathBuf, std::sync::Weak<BlockingPathGateState>>,
    >,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
fn lock_unpoisoned<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Installs a failure-safe blocking gate for one exact test path.
#[cfg(test)]
pub struct BlockingPathGate {
    path: std::path::PathBuf,
    state: std::sync::Arc<BlockingPathGateState>,
    started: Option<tokio::sync::oneshot::Receiver<std::os::fd::RawFd>>,
    output_drop_started: Option<tokio::sync::oneshot::Receiver<()>>,
    completed: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// Holds an abandoned output when its call site places this token after the file owner.
#[cfg(test)]
pub struct BlockingPathGateVisit {
    output_drop_started: Option<tokio::sync::oneshot::Sender<()>>,
    output_drop_barrier: std::sync::Arc<BlockingPathDropBarrier>,
    _completion: CompletionSignal,
}

#[cfg(test)]
impl Drop for BlockingPathGateVisit {
    fn drop(&mut self) {
        if let Some(started) = self.output_drop_started.take() {
            let _ = started.send(());
        }
        let mut released = lock_unpoisoned(&self.output_drop_barrier.released);
        while !*released {
            released = self
                .output_drop_barrier
                .released_cv
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(test)]
impl BlockingPathGate {
    /// Installs a gate that unrelated filesystem paths cannot enter.
    pub fn install(path: impl Into<std::path::PathBuf>) -> Self {
        let path = path.into();
        let mut gates = lock_unpoisoned(&BLOCKING_PATH_GATES);
        let occupied = gates.get(&path).and_then(std::sync::Weak::upgrade);
        if occupied.is_some() {
            drop(gates);
            panic!("a blocking path gate is already installed for {path:?}");
        }
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (output_drop_started_tx, output_drop_started_rx) = tokio::sync::oneshot::channel();
        let (completion, completion_rx) = CompletionSignal::new();
        let output_drop_barrier = std::sync::Arc::new(BlockingPathDropBarrier {
            released: std::sync::Mutex::new(false),
            released_cv: std::sync::Condvar::new(),
        });
        let state = std::sync::Arc::new(BlockingPathGateState {
            started: std::sync::Mutex::new(Some(started_tx)),
            released: std::sync::Mutex::new(false),
            released_cv: std::sync::Condvar::new(),
            hit_count: std::sync::atomic::AtomicUsize::new(0),
            allocation_count: std::sync::atomic::AtomicUsize::new(0),
            allocation_thread: std::sync::Mutex::new(None),
            visit: std::sync::Mutex::new(Some(BlockingPathGateVisit {
                output_drop_started: Some(output_drop_started_tx),
                output_drop_barrier: output_drop_barrier.clone(),
                _completion: completion,
            })),
            output_drop_barrier,
        });
        gates.insert(path.clone(), std::sync::Arc::downgrade(&state));
        drop(gates);
        Self {
            path,
            state,
            started: Some(started_rx),
            output_drop_started: Some(output_drop_started_rx),
            completed: Some(completion_rx),
        }
    }

    /// Waits until production blocking work enters this path's gate.
    pub async fn wait_started(&mut self) -> Result<std::os::fd::RawFd> {
        self.started
            .take()
            .context("the blocking path gate can only be awaited once")?
            .await
            .context("blocking path work ended before entering its gate")
    }

    /// Waits until an abandoned output begins dropping its visit token.
    pub async fn wait_output_drop_started(&mut self) -> Result<()> {
        self.output_drop_started
            .take()
            .context("the blocking path output-drop start can only be awaited once")?
            .await
            .context("blocking path output ended without entering its drop barrier")
    }

    /// Waits until an abandoned output passes its drop barrier and releases its visit token.
    pub async fn wait_completed(&mut self) -> Result<()> {
        self.completed
            .take()
            .context("the blocking path completion can only be awaited once")?
            .await
            .context("blocking path output ended without its completion token")
    }

    /// Releases blocked production work. Calling this more than once is harmless.
    pub fn release(&self) {
        let mut released = lock_unpoisoned(&self.state.released);
        *released = true;
        self.state.released_cv.notify_all();
    }

    /// Releases the abandoned-output drop barrier. Calling this more than once is harmless.
    pub fn release_output_drop(&self) {
        let mut released = lock_unpoisoned(&self.state.output_drop_barrier.released);
        *released = true;
        self.state.output_drop_barrier.released_cv.notify_all();
    }

    /// Releases both blocking phases. Calling this more than once is harmless.
    pub fn release_all(&self) {
        self.release();
        self.release_output_drop();
    }

    /// Returns how many blocking jobs entered this exact path's gate.
    pub fn hit_count(&self) -> usize {
        self.state
            .hit_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns how many filegen buffers were allocated for this exact path.
    pub fn allocation_count(&self) -> usize {
        self.state
            .allocation_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns the thread that performed the first recorded filegen buffer allocation.
    pub fn allocation_thread(&self) -> Option<std::thread::ThreadId> {
        *lock_unpoisoned(&self.state.allocation_thread)
    }
}

#[cfg(test)]
impl Drop for BlockingPathGate {
    fn drop(&mut self) {
        self.release_all();
        let mut gates = lock_unpoisoned(&BLOCKING_PATH_GATES);
        if gates
            .get(&self.path)
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|state| std::sync::Arc::ptr_eq(&state, &self.state))
        {
            gates.remove(&self.path);
        }
    }
}

/// Records one filegen buffer allocation for an installed exact-path gate.
#[cfg(test)]
pub fn record_blocking_path_allocation(path: &std::path::Path) {
    if let Some(state) = lock_unpoisoned(&BLOCKING_PATH_GATES)
        .get(path)
        .and_then(std::sync::Weak::upgrade)
    {
        let mut allocation_thread = lock_unpoisoned(&state.allocation_thread);
        allocation_thread.get_or_insert_with(|| std::thread::current().id());
        drop(allocation_thread);
        state
            .allocation_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Blocks a production test job and returns its output-lifetime visit token.
#[cfg(test)]
#[must_use]
pub fn wait_on_blocking_path_gate(
    path: &std::path::Path,
    raw_fd: std::os::fd::RawFd,
) -> Option<BlockingPathGateVisit> {
    let state = lock_unpoisoned(&BLOCKING_PATH_GATES)
        .get(path)
        .and_then(std::sync::Weak::upgrade);
    let state = state?;
    state
        .hit_count
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    if let Some(started) = lock_unpoisoned(&state.started).take() {
        let _ = started.send(raw_fd);
    }
    let mut released = lock_unpoisoned(&state.released);
    while !*released {
        released = state
            .released_cv
            .wait(released)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    drop(released);
    lock_unpoisoned(&state.visit).take()
}

/// Observations from cancelling one production task at its exact-path blocking boundary.
#[cfg(test)]
pub struct CancelledBlockingPathObservations {
    pub waiter_was_cancelled: bool,
    pub admission_was_retained_while_work_gated: bool,
    pub fd_was_open_while_work_gated: bool,
    pub hit_count_before_release: usize,
    pub allocation_count_before_release: usize,
    pub fd_was_closed_at_output_drop_start: bool,
    pub admission_was_retained_at_output_drop_start: bool,
    pub final_hit_count: usize,
    pub final_allocation_count: usize,
    pub allocation_thread: Option<std::thread::ThreadId>,
}

#[cfg(test)]
async fn abort_and_quiesce_hit_blocking_path<T>(
    admission: &AdmissionLimit,
    gate: &mut BlockingPathGate,
    task: &mut tokio::task::JoinHandle<T>,
    timeout: std::time::Duration,
) -> Result<()> {
    task.abort();
    let _ = task.await;
    gate.release_all();
    admission
        .run_with_timeout(timeout, async {
            let output =
                await_completion_and_capacity(gate.wait_completed(), throttle::open_file_permit());
            let (completion, permit) = output.await;
            drop(permit);
            completion
        })
        .await
        .context("blocking task did not quiesce after both gates released")??;
    Ok(())
}

#[cfg(test)]
async fn quiesce_unentered_blocking_path<T>(
    admission: &AdmissionLimit,
    gate: &mut BlockingPathGate,
    task: &mut tokio::task::JoinHandle<T>,
) -> Result<()> {
    gate.release_all();
    admission.quiesce(task).await;
    if gate.hit_count() != 0 {
        gate.wait_completed().await?;
    }
    Ok(())
}

#[cfg(test)]
fn record_fd_identity_probe_result(
    probe_error: &mut Option<anyhow::Error>,
    result: std::io::Result<bool>,
) -> bool {
    match result {
        Ok(value) => value,
        Err(error) => {
            if probe_error.is_none() {
                *probe_error = Some(error.into());
            }
            false
        }
    }
}

/// Cancels a task inside an exact-path blocking job and quiesces every owned resource.
///
/// The supplied admission fixture must already have `max_files_in_flight` set to one; the pending
/// second permit is the old-epoch witness that the gated task still owns the only slot.
#[cfg(test)]
pub async fn cancel_at_blocking_path<T, O, F>(
    admission: AdmissionLimit,
    mut gate: BlockingPathGate,
    mut task: tokio::task::JoinHandle<T>,
    timeout: std::time::Duration,
    observe_while_gated: F,
) -> Result<(CancelledBlockingPathObservations, O)>
where
    T: Send + 'static,
    F: FnOnce(std::os::fd::RawFd) -> O,
{
    enum StartOutcome {
        Entered(Result<std::os::fd::RawFd>),
        OwnerFinished,
    }

    let start_outcome = tokio::time::timeout(timeout, async {
        tokio::select! {
            started = gate.wait_started() => StartOutcome::Entered(started),
            result = &mut task => {
                let _ = result;
                StartOutcome::OwnerFinished
            }
        }
    })
    .await;
    let raw_fd = match start_outcome {
        Ok(StartOutcome::Entered(Ok(raw_fd))) => raw_fd,
        Ok(StartOutcome::Entered(Err(error))) => {
            let cleanup_result = if gate.hit_count() != 0 {
                abort_and_quiesce_hit_blocking_path(&admission, &mut gate, &mut task, timeout).await
            } else {
                quiesce_unentered_blocking_path(&admission, &mut gate, &mut task).await
            };
            drop(gate);
            drop(admission);
            cleanup_result.context("failed blocking gate did not quiesce its output")?;
            return Err(error);
        }
        Ok(StartOutcome::OwnerFinished) => {
            gate.release_all();
            drop(gate);
            drop(admission);
            anyhow::bail!("task completed without entering its production blocking path gate");
        }
        Err(error) => {
            let gate_was_hit = gate.hit_count() != 0;
            let cleanup_result = if gate_was_hit {
                abort_and_quiesce_hit_blocking_path(&admission, &mut gate, &mut task, timeout).await
            } else {
                quiesce_unentered_blocking_path(&admission, &mut gate, &mut task).await
            };
            drop(gate);
            drop(admission);
            cleanup_result.context("timed-out blocking task did not quiesce its output")?;
            return Err(error)
                .context("task did not enter its production blocking path gate in time");
        }
    };
    // keep probe failures until the same release/join/completion/capacity cleanup used for normal
    // cancellation has finished. Dropping a gate only opens its barriers; it does not await the
    // blocking output that those barriers release.
    let mut probe_error: Option<anyhow::Error> = None;
    let fd_probe = match FdIdentityProbe::capture(raw_fd) {
        Ok(probe) => Some(probe),
        Err(error) => {
            probe_error = Some(error.into());
            None
        }
    };

    task.abort();
    let cancellation = tokio::time::timeout(timeout, &mut task).await;
    if let Err(error) = cancellation {
        let cleanup_result =
            abort_and_quiesce_hit_blocking_path(&admission, &mut gate, &mut task, timeout).await;
        drop(gate);
        drop(admission);
        let cancellation_error =
            anyhow::Error::new(error).context("blocking-path task did not cancel in time");
        let cleanup_error = cleanup_result
            .err()
            .map(|error| error.context("timed-out task cancellation did not quiesce its output"));
        if let Some(probe_error) = probe_error {
            return Err(match cleanup_error {
                Some(cleanup_error) => probe_error.context(format!(
                    "blocking-path cancellation also failed after the probe error: {cancellation_error:#}; \
                     cleanup failed: {cleanup_error:#}"
                )),
                None => probe_error.context(format!(
                    "blocking-path cancellation also timed out after the probe error: {cancellation_error:#}"
                )),
            });
        }
        if let Some(cleanup_error) = cleanup_error {
            return Err(cleanup_error);
        }
        return Err(cancellation_error);
    }
    let waiter_was_cancelled = matches!(cancellation, Ok(Err(error)) if error.is_cancelled());
    let mut second_permit = Box::pin(throttle::open_file_permit());
    let (admission_was_retained_while_work_gated, mut acquired_permit) =
        match futures::poll!(second_permit.as_mut()) {
            std::task::Poll::Pending => (true, None),
            std::task::Poll::Ready(permit) => (false, Some(permit)),
        };
    let fd_was_open_while_work_gated = match fd_probe.as_ref() {
        Some(probe) => {
            !record_fd_identity_probe_result(&mut probe_error, probe.original_is_closed())
        }
        None => false,
    };
    let hit_count_before_release = gate.hit_count();
    let allocation_count_before_release = gate.allocation_count();
    let caller_observation =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observe_while_gated(raw_fd)));

    gate.release();
    let output_drop_started = tokio::time::timeout(timeout, gate.wait_output_drop_started()).await;
    let (
        fd_was_closed_at_output_drop_start,
        admission_was_retained_at_output_drop_start,
        output_drop_start_error,
    ) = match output_drop_started {
        Ok(Ok(())) => {
            let fd_was_closed = match fd_probe.as_ref() {
                Some(probe) => {
                    record_fd_identity_probe_result(&mut probe_error, probe.original_is_closed())
                }
                None => false,
            };
            let admission_was_retained = if acquired_permit.is_some() {
                false
            } else {
                match futures::poll!(second_permit.as_mut()) {
                    std::task::Poll::Pending => true,
                    std::task::Poll::Ready(permit) => {
                        acquired_permit = Some(permit);
                        false
                    }
                }
            };
            (fd_was_closed, admission_was_retained, None)
        }
        Ok(Err(error)) => (
            false,
            false,
            Some(error.context("blocking output ended before its drop barrier")),
        ),
        Err(error) => (
            false,
            false,
            Some(
                anyhow::Error::new(error)
                    .context("blocking output did not enter its drop barrier in time"),
            ),
        ),
    };

    gate.release_output_drop();
    let capacity = async move {
        match acquired_permit {
            Some(permit) => permit,
            None => second_permit.await,
        }
    };
    let completion_and_capacity = admission
        .run_with_timeout(
            timeout,
            await_completion_and_capacity(gate.wait_completed(), capacity),
        )
        .await;
    let (permit, output_completion_error) = match completion_and_capacity {
        Ok((Ok(()), permit)) => (Some(permit), None),
        Ok((Err(error), permit)) => {
            drop(permit);
            (
                None,
                Some(error.context("blocking output lost its completion witness")),
            )
        }
        Err(error) => (
            None,
            Some(
                anyhow::Error::new(error)
                    .context("blocking output did not quiesce after both gates released"),
            ),
        ),
    };
    let final_hit_count = gate.hit_count();
    let final_allocation_count = gate.allocation_count();
    let allocation_thread = gate.allocation_thread();
    drop(permit);
    drop(gate);
    drop(admission);
    let observations = CancelledBlockingPathObservations {
        waiter_was_cancelled,
        admission_was_retained_while_work_gated,
        fd_was_open_while_work_gated,
        hit_count_before_release,
        allocation_count_before_release,
        fd_was_closed_at_output_drop_start,
        admission_was_retained_at_output_drop_start,
        final_hit_count,
        final_allocation_count,
        allocation_thread,
    };
    let caller_observation = match caller_observation {
        Ok(caller_observation) => caller_observation,
        Err(payload) => std::panic::resume_unwind(payload),
    };
    if let Some(error) = probe_error {
        return Err(error);
    }
    if let Some(error) = output_drop_start_error {
        return Err(error);
    }
    if let Some(error) = output_completion_error {
        return Err(error);
    }
    Ok((observations, caller_observation))
}

pub async fn create_temp_dir() -> Result<std::path::PathBuf> {
    let mut idx = 0;
    loop {
        let tmp_dir = std::env::temp_dir().join(format!("rcp_test{}", &idx));
        if let Err(error) = tokio::fs::create_dir(&tmp_dir).await {
            match error.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    idx += 1;
                }
                _ => return Err(error.into()),
            }
        } else {
            return Ok(tmp_dir);
        }
    }
}

pub async fn setup_test_dir() -> Result<std::path::PathBuf> {
    // create a temporary directory
    let tmp_dir = create_temp_dir().await?;
    // foo
    // |- 0.txt
    // |- bar
    //    |- 1.txt
    //    |- 2.txt
    //    |- 3.txt
    // |- baz
    //    |- 4.txt
    //    |- 5.txt -> ../bar/2.txt
    //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
    let foo_path = tmp_dir.join("foo");
    tokio::fs::create_dir(&foo_path).await.unwrap();
    tokio::fs::write(foo_path.join("0.txt"), "0").await.unwrap();
    let bar_path = foo_path.join("bar");
    tokio::fs::create_dir(&bar_path).await.unwrap();
    tokio::fs::write(bar_path.join("1.txt"), "1").await.unwrap();
    tokio::fs::write(bar_path.join("2.txt"), "2").await.unwrap();
    tokio::fs::write(bar_path.join("3.txt"), "3").await.unwrap();
    let baz_path = foo_path.join("baz");
    tokio::fs::create_dir(&baz_path).await.unwrap();
    tokio::fs::write(baz_path.join("4.txt"), "4").await.unwrap();
    tokio::fs::symlink("../bar/2.txt", baz_path.join("5.txt"))
        .await
        .unwrap();
    tokio::fs::symlink(bar_path.join("3.txt"), baz_path.join("6.txt"))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    Ok(tmp_dir)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileEqualityCheck {
    Basic,
    Timestamp,
    HardLink,
}

#[async_recursion]
pub async fn check_dirs_identical(
    src: &std::path::Path,
    dst: &std::path::Path,
    file_equality_check: FileEqualityCheck,
) -> Result<()> {
    let mut src_entries = tokio::fs::read_dir(src).await?;
    while let Some(src_entry) = src_entries.next_entry().await? {
        let src_entry_path = src_entry.path();
        let src_entry_name = src_entry_path.file_name().unwrap();
        let dst_entry_path = dst.join(src_entry_name);
        let src_md = tokio::fs::symlink_metadata(&src_entry_path)
            .await
            .context(format!("Source file {:?} is missing!", &src_entry_path))?;
        let dst_md = tokio::fs::symlink_metadata(&dst_entry_path)
            .await
            .context(format!(
                "Destination file {:?} is missing!",
                &dst_entry_path
            ))?;
        // compare file type and content
        assert_eq!(src_md.file_type(), dst_md.file_type());
        if src_md.is_file() {
            if file_equality_check == FileEqualityCheck::HardLink {
                assert_eq!(src_md.ino(), dst_md.ino());
            } else {
                let src_contents = tokio::fs::read_to_string(&src_entry_path).await?;
                let dst_contents = tokio::fs::read_to_string(&dst_entry_path).await?;
                assert_eq!(src_contents, dst_contents);
            }
        } else if src_md.file_type().is_symlink() {
            let src_link = tokio::fs::read_link(&src_entry_path).await?;
            let dst_link = tokio::fs::read_link(&dst_entry_path).await?;
            assert_eq!(src_link, dst_link);
        } else {
            check_dirs_identical(&src_entry_path, &dst_entry_path, file_equality_check).await?;
        }
        // compare permissions
        assert_eq!(src_md.permissions(), dst_md.permissions());
        if file_equality_check != FileEqualityCheck::Timestamp {
            continue;
        }
        // compare timestamps
        // NOTE: skip comparing "atime" - we read the file few times when comparing agaisnt "cp"
        assert_eq!(
            src_md.mtime_nsec(),
            dst_md.mtime_nsec(),
            "mtime doesn't match for {src_entry_path:?} {dst_entry_path:?}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::time::Duration;

    struct DelayedDrop(Duration);

    impl Drop for DelayedDrop {
        fn drop(&mut self) {
            std::thread::sleep(self.0);
        }
    }

    #[test]
    fn fd_identity_probe_detects_a_closed_descriptor_slot_reused_for_another_file()
    -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let first_path = dir.path().join("first");
        let second_path = dir.path().join("second");
        std::fs::write(&first_path, b"first")?;
        std::fs::write(&second_path, b"second")?;
        let first = std::fs::File::open(first_path)?;
        let second = std::fs::File::open(second_path)?;
        let first_fd = first.as_raw_fd();
        let probe = FdIdentityProbe::capture(first_fd)?;

        // SAFETY: both descriptors are live files, and `dup2` atomically replaces only `first_fd`.
        let result = unsafe { libc::dup2(second.as_raw_fd(), first_fd) };
        assert_ne!(result, -1, "failed to reuse the first descriptor slot");
        // SAFETY: `F_GETFD` reads only the descriptor integer and writes through no userspace
        // pointer; the kernel validates whether the reused numeric slot remains open.
        let slot_flags = unsafe { libc::fcntl(first_fd, libc::F_GETFD) };
        assert!(
            slot_flags != -1,
            "the reused descriptor slot must remain numerically open"
        );
        assert!(
            probe.original_is_closed()?,
            "a descriptor reused for another file must not appear to retain the original owner"
        );
        Ok(())
    }

    #[test]
    fn fd_identity_probe_distinguishes_a_live_original_from_a_closed_one() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("file");
        std::fs::write(&path, b"contents")?;
        let file = std::fs::File::open(path)?;
        let probe = FdIdentityProbe::capture(file.as_raw_fd())?;

        assert!(!probe.original_is_closed()?);
        drop(file);
        assert!(probe.original_is_closed()?);
        Ok(())
    }

    #[test]
    fn duplicate_blocking_gate_registration_does_not_poison_the_registry() {
        let path = std::path::PathBuf::from("blocking-path-gate-duplicate-registration-test");
        let first = BlockingPathGate::install(path.clone());

        let duplicate = catch_unwind(AssertUnwindSafe(|| {
            drop(BlockingPathGate::install(path.clone()));
        }));
        assert!(duplicate.is_err());
        drop(first);

        drop(BlockingPathGate::install(path));
    }

    async fn cancellation_controller_quiesces_before_returning_injected_probe_error(
        failure_point: FdIdentityProbeOperation,
        occurrence: usize,
    ) -> anyhow::Result<()> {
        let root = create_temp_dir().await?;
        let path = root.join("controller-injected-probe-error");
        tokio::fs::write(&path, b"x").await?;
        let gate = BlockingPathGate::install(path.clone());
        let admission = AdmissionLimit::new().await;
        admission.set_files_in_flight(1);
        let held_permit = throttle::open_file_permit().await;
        let release_after_gate = std::sync::Arc::new(tokio::sync::Notify::new());
        let task_release = std::sync::Arc::clone(&release_after_gate);
        let (owner_dropped, mut owner_dropped_rx) = CompletionSignal::new();
        let (task_finished_tx, task_finished_rx) = tokio::sync::oneshot::channel();
        let task_path = path.clone();
        let task = tokio::spawn(async move {
            let output = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                let file = std::fs::File::open(&task_path)?;
                let visit = wait_on_blocking_path_gate(&task_path, file.as_raw_fd());
                std::thread::sleep(Duration::from_millis(100));
                Ok((
                    (
                        file,
                        visit,
                        DelayedDrop(Duration::from_millis(100)),
                        owner_dropped,
                    ),
                    held_permit,
                ))
            })
            .await??;
            let _output = output;
            task_release.notified().await;
            let _ = task_finished_tx.send(());
            Ok::<(), anyhow::Error>(())
        });
        let _failure = inject_fd_identity_probe_failure(failure_point, occurrence);

        let result =
            cancel_at_blocking_path(admission, gate, task, Duration::from_secs(5), |_| ()).await;
        let owner_dropped_before_error = owner_dropped_rx.try_recv().is_ok();
        release_after_gate.notify_waiters();
        let task_finished = tokio::time::timeout(Duration::from_secs(5), task_finished_rx).await;
        let cleanup_result = tokio::fs::remove_dir_all(root).await;

        let error = match result {
            Ok(_) => anyhow::bail!("the injected identity probe error was not returned"),
            Err(error) => error,
        };
        cleanup_result?;
        assert!(
            format!("{error:#}").contains("injected fd identity probe failure"),
            "the controller returned an unexpected error: {error:#}"
        );
        assert!(
            owner_dropped_before_error,
            "the controller returned its probe error before the blocked output dropped"
        );
        assert!(
            task_finished.is_ok(),
            "the detached task did not finish after its test-only release"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_controller_quiesces_before_returning_an_injected_capture_error()
    -> anyhow::Result<()> {
        cancellation_controller_quiesces_before_returning_injected_probe_error(
            FdIdentityProbeOperation::Capture,
            1,
        )
        .await
    }

    #[tokio::test]
    async fn cancellation_controller_quiesces_before_returning_an_injected_gated_probe_error()
    -> anyhow::Result<()> {
        cancellation_controller_quiesces_before_returning_injected_probe_error(
            FdIdentityProbeOperation::OriginalIsClosed,
            1,
        )
        .await
    }

    #[tokio::test]
    async fn cancellation_controller_quiesces_before_returning_an_injected_output_drop_probe_error()
    -> anyhow::Result<()> {
        cancellation_controller_quiesces_before_returning_injected_probe_error(
            FdIdentityProbeOperation::OriginalIsClosed,
            2,
        )
        .await
    }

    #[tokio::test]
    async fn cancellation_controller_quiesces_before_resuming_observer_panic() -> anyhow::Result<()>
    {
        static PROGRESS: std::sync::LazyLock<crate::progress::Progress> =
            std::sync::LazyLock::new(crate::progress::Progress::new);

        let root = create_temp_dir().await?;
        let path = root.join("controller-observer-panic");
        let gate = BlockingPathGate::install(path.clone());
        let admission = AdmissionLimit::new().await;
        admission.set_files_in_flight(1);
        let timeout = Duration::from_secs(20);
        let task = tokio::spawn(crate::filegen::write_file(
            &PROGRESS,
            path.clone(),
            4096,
            4096,
            0,
        ));
        let observed_probe = std::sync::Arc::new(std::sync::Mutex::new(None));
        let panic_probe = observed_probe.clone();
        let controller = cancel_at_blocking_path(admission, gate, task, timeout, move |raw_fd| {
            *panic_probe.lock().expect("observer fd probe lock poisoned") =
                Some(FdIdentityProbe::capture(raw_fd).expect("gated fd must be capturable"));
            panic!("observer panic after the production gate was entered");
        });
        let panic_result = AssertUnwindSafe(controller).catch_unwind().await;
        let fd_was_closed = observed_probe
            .lock()
            .expect("observer fd probe lock poisoned")
            .take()
            .expect("observer did not capture the gated fd")
            .original_is_closed();
        let admission_reacquired = tokio::time::timeout(timeout, AdmissionLimit::new()).await;
        let admission_was_reacquired = admission_reacquired.is_ok();
        drop(admission_reacquired);
        let gate_reinstalled = catch_unwind(AssertUnwindSafe(|| {
            drop(BlockingPathGate::install(path));
        }))
        .is_ok();
        let cleanup_result = tokio::fs::remove_dir_all(root).await;

        let fd_was_closed = fd_was_closed?;
        cleanup_result?;
        assert!(panic_result.is_err(), "the observer panic was not resumed");
        assert!(
            fd_was_closed,
            "the controller resumed its observer panic before the fd owner quiesced"
        );
        assert!(
            admission_was_reacquired,
            "the controller resumed its observer panic before unlocking admission state"
        );
        assert!(
            gate_reinstalled,
            "the controller resumed its observer panic before unregistering the path gate"
        );
        Ok(())
    }

    #[tokio::test]
    async fn admission_limits_reset_when_a_guarded_section_panics() {
        let limit = AdmissionLimit::new().await;
        let resource = throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);

        let panic = catch_unwind(AssertUnwindSafe(move || {
            limit.set_files_in_flight(1);
            limit.set_max_ops_in_flight(resource, 1);
            panic!("leave the guarded section early");
        }));
        assert!(panic.is_err());

        let _later_section = ADMISSION_LIMIT_LOCK.lock().await;
        let first_open = throttle::open_file_permit().await;
        let second_open = tokio::time::timeout(Duration::from_millis(100), async {
            throttle::open_file_permit().await
        })
        .await
        .expect("OpenFile admission was not reset after panic");
        let first_pending = throttle::pending_meta_permit().await;
        let second_pending = tokio::time::timeout(Duration::from_millis(100), async {
            throttle::pending_meta_permit().await
        })
        .await
        .expect("PendingMeta admission was not reset after panic");
        assert_eq!(throttle::current_ops_in_flight_limit(resource), 0);
        drop((first_open, second_open, first_pending, second_pending));
    }

    #[tokio::test]
    async fn timed_out_admission_work_finishes_after_limits_are_removed() {
        let limit = AdmissionLimit::new().await;
        limit.set_files_in_flight(1);
        let held_open = throttle::open_file_permit().await;
        let held_pending = throttle::pending_meta_permit().await;
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_completed = std::sync::Arc::clone(&completed);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            limit.run_with_timeout(Duration::from_millis(10), async move {
                let (_open, _pending) = tokio::join!(
                    throttle::open_file_permit(),
                    throttle::pending_meta_permit(),
                );
                task_completed.store(true, std::sync::atomic::Ordering::SeqCst);
            }),
        )
        .await
        .expect("timed-out work did not quiesce after admission was removed");

        assert!(result.is_err(), "the inner timeout must still be reported");
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
        drop((held_open, held_pending));
    }

    #[tokio::test]
    async fn completion_witness_also_waits_for_capacity() {
        let limit = AdmissionLimit::new().await;
        limit.set_files_in_flight(1);
        let held = throttle::open_file_permit().await;
        let (completion, completion_rx) = CompletionSignal::new();
        let owner_dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_owner_dropped = std::sync::Arc::clone(&owner_dropped);
        let owner = tokio::spawn(async move {
            drop(completion);
            tokio::task::yield_now().await;
            drop(held);
            task_owner_dropped.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        let helper_result = tokio::time::timeout(
            Duration::from_secs(1),
            await_completion_and_capacity(completion_rx, throttle::open_file_permit()),
        )
        .await;
        let returned_before_owner_dropped =
            helper_result.is_ok() && !owner_dropped.load(std::sync::atomic::Ordering::SeqCst);
        let owner_result = owner.await;

        owner_result.expect("capacity owner failed");
        let (completion_result, permit) = helper_result.expect("capacity cleanup did not finish");
        completion_result.expect("completion sender was dropped");
        assert!(
            !returned_before_owner_dropped,
            "completion witness returned before the capacity owner dropped"
        );
        drop(permit);
    }
}
