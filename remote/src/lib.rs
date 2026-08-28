//! Remote copy protocol and networking for distributed file operations
//!
//! This crate provides the networking layer and protocol definitions for remote file copying in the RCP tools suite.
//! It enables efficient distributed copying between remote hosts using SSH for orchestration and TCP for high-throughput data transfer.
//!
//! # Overview
//!
//! The remote copy system uses a three-node architecture:
//!
//! ```text
//! Master (rcp)
//! ├── SSH → Source Host (rcpd)
//! │   ├── TCP Server ← Master (control + tracing)
//! │   └── TCP Server ← Destination (control + data)
//! └── SSH → Destination Host (rcpd)
//!     ├── TCP Server ← Master (control + tracing)
//!     └── TCP Client → Source (control + data)
//! ```
//!
//! ## Connection Flow
//!
//! 1. **Initialization**: Master starts `rcpd` processes on source and destination via SSH
//! 2. **Control Connections**: Master connects to each `rcpd`'s TCP listener (address read
//!    from the rcpd's stderr over SSH)
//! 3. **Address Exchange**: Source starts TCP listeners and sends addresses to Master
//! 4. **Direct Connection**: Master forwards addresses to Destination, which connects to Source
//! 5. **Data Transfer**: Files flow directly from Source to Destination (not through Master)
//!
//! This design ensures efficient data transfer while allowing the Master to coordinate operations and monitor progress.
//!
//! # Key Components
//!
//! ## SSH Session Management
//!
//! The [`SshSession`] type represents an SSH connection to a remote host and is used to:
//! - Launch `rcpd` daemons on remote hosts
//! - Configure connection parameters (user, host, port)
//!
//! ## TCP Networking
//!
//! TCP provides high-throughput bulk data transfer with:
//! - Connection pooling for parallel file transfers
//! - Configurable buffer sizes for different network profiles
//! - Length-delimited message framing for control messages
//!
//! Key functions:
//! - [`create_tcp_control_listener`] - Create TCP listener for control connections
//! - [`create_tcp_data_listener`] - Create TCP listener for data connections
//! - [`connect_tcp_control`] - Connect to a TCP control server
//! - [`get_tcp_listener_addr`] - Get externally-routable address of a listener
//! - [`configure_tcp_socket`] - Apply the standard socket options to every established connection
//!
//! ## Port Range Configuration
//!
//! The [`port_ranges`] module allows restricting TCP to specific port ranges, useful for firewall-restricted environments:
//!
//! ```rust,no_run
//! # async fn example() -> anyhow::Result<()> {
//! let config = remote::TcpConfig {
//!     port_ranges: Some("8000-8999".to_string()),
//!     ..remote::TcpConfig::default()
//! };
//! let listener = remote::create_tcp_control_listener(&config, None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Protocol Messages
//!
//! The [`protocol`] module defines the message types exchanged between nodes:
//! - `MasterHello` - Master → rcpd configuration
//! - `SourceMasterHello` - Source → Master address information
//! - `RcpdResult` - rcpd → Master operation results
//!
//! ## Stream Communication
//!
//! The [`streams`] module provides high-level abstractions over TCP streams:
//! - [`streams::SendStream`] / [`streams::RecvStream`] for framed message passing
//! - [`streams::ControlConnection`] for bidirectional control channels
//! - Object serialization/deserialization using bitcode
//!
//! ## Remote Tracing
//!
//! The [`tracelog`] module enables distributed logging and progress tracking:
//! - Forward tracing events from remote `rcpd` processes to Master
//! - Aggregate progress information across multiple remote operations
//! - Display unified progress for distributed operations
//!
//! # Security Model
//!
//! The remote copy system provides multiple security layers:
//!
//! - **SSH Authentication**: All remote operations require SSH authentication to spawn rcpd
//! - **TLS Encryption**: All TCP connections encrypted with TLS 1.3 by default
//! - **Certificate Pinning**: Self-signed certificates with fingerprint verification
//! - **Mutual Authentication**: Source↔Destination connections use mutual TLS
//!
//! **How it works**:
//! 1. Each rcpd generates an ephemeral self-signed certificate
//! 2. rcpd outputs its certificate fingerprint to stderr (read by master over the trusted SSH channel)
//! 3. Master connects to rcpd as TLS client, verifies fingerprint
//! 4. Master distributes fingerprints to enable Source↔Destination mutual auth
//!
//! Use `--no-encryption` to disable TLS for trusted networks where performance is critical.
//!
//! # Network Troubleshooting
//!
//! Common failure scenarios:
//!
//! - **SSH Connection Fails**: Host unreachable or authentication failure
//! - **Master Cannot Connect to rcpd**: Firewall blocks TCP ports
//! - **Destination Cannot Connect to Source**: Use `--port-ranges` to specify allowed ports
//!
//! # Module Organization
//!
//! - [`port_ranges`] - Port range parsing and socket binding
//! - [`protocol`] - Protocol message definitions and serialization
//! - [`streams`] - TCP stream wrappers with typed message passing
//! - [`tls`] - TLS certificate generation and configuration
//! - [`tracelog`] - Remote tracing and progress aggregation

#[cfg(not(tokio_unstable))]
compile_error!("tokio_unstable cfg must be enabled; see .cargo/config.toml");

use anyhow::{Context, anyhow};
use tracing::instrument;

#[cfg(test)]
const DEFAULT_REMOTE_BOOTSTRAP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
const FAILED_RCPD_REAP_GRACE: std::time::Duration = std::time::Duration::from_secs(2);
const RCPD_OUTPUT_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);
const REMOTE_CLEANUP_GRACE: std::time::Duration = std::time::Duration::from_secs(3);
const CLEANUP_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
const DIAGNOSTIC_CAPTURE_LIMIT: usize = 64 * 1024;
const RCPD_STARTUP_RECORD_LIMIT: usize = 64 * 1024;
pub(crate) const RCPD_PATH_DISCOVERY_SCRIPT: &str = "command -v \"$1\"";

std::thread_local! {
    static ACTIVE_REMOTE_CLEANUP_SCOPES: std::cell::RefCell<Vec<ActiveCleanupScope>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

#[derive(Clone)]
struct ActiveCleanupScope {
    scope_id: usize,
    budget: CleanupBudget,
}

struct CleanupWorkers {
    owners: usize,
    pending: usize,
    threads: Vec<std::thread::JoinHandle<()>>,
    next_job_id: u64,
    finalizations: Vec<CleanupFinalization>,
}

impl Default for CleanupWorkers {
    fn default() -> Self {
        Self {
            owners: 1,
            pending: 0,
            threads: Vec::new(),
            next_job_id: 0,
            finalizations: Vec::new(),
        }
    }
}

impl CleanupWorkers {
    fn allocate_job_id(&mut self) -> u64 {
        let job_id = self.next_job_id;
        self.next_job_id = self
            .next_job_id
            .checked_add(1)
            .expect("cleanup job id overflow");
        job_id
    }
}

struct CleanupFinalization {
    cutoff_job_id: u64,
    deadline: std::time::Instant,
}

#[derive(Default)]
struct CleanupState {
    workers: std::sync::Mutex<CleanupWorkers>,
    changed: std::sync::Condvar,
    supervisor: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

struct CleanupJob {
    thread_name: &'static str,
    deadline: std::time::Instant,
    operation: CleanupOperation,
}

struct ScheduledCleanupJob {
    id: u64,
    job: CleanupJob,
}

enum CleanupOperation {
    Bounded(Box<dyn FnOnce(CleanupBudget) + Send + 'static>),
    Disposable(Box<dyn FnOnce() + Send + 'static>),
}

/// Mandatory liveness budget supplied by the cleanup scope to cooperative waits.
#[derive(Clone)]
pub(crate) struct CleanupBudget {
    state: std::sync::Arc<CleanupState>,
    job_id: u64,
    job_deadline: std::time::Instant,
}

impl CleanupBudget {
    #[cfg(test)]
    fn for_job(state: std::sync::Arc<CleanupState>, grace: std::time::Duration) -> Self {
        let job_id = state
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .allocate_job_id();
        Self {
            state,
            job_id,
            job_deadline: std::time::Instant::now() + grace,
        }
    }

    fn effective_deadline(&self, workers: &CleanupWorkers) -> std::time::Instant {
        workers
            .finalizations
            .iter()
            .filter(|finalization| self.job_id < finalization.cutoff_job_id)
            .fold(self.job_deadline, |deadline, finalization| {
                std::cmp::min(deadline, finalization.deadline)
            })
    }

    fn wait_for_next_poll(&self) -> bool {
        let workers = self
            .state
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = std::time::Instant::now();
        let deadline = self.effective_deadline(&workers);
        if now >= deadline {
            return false;
        }
        let wait = std::cmp::min(CLEANUP_POLL_INTERVAL, deadline.duration_since(now));
        drop(
            self.state
                .changed
                .wait_timeout(workers, wait)
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        true
    }
}

/// Owns all blocking cleanup work started by one rcp invocation.
pub struct RemoteCleanup {
    state: std::sync::Arc<CleanupState>,
    sender: Option<std::sync::mpsc::Sender<ScheduledCleanupJob>>,
}

impl Clone for RemoteCleanup {
    fn clone(&self) -> Self {
        let mut workers = self
            .state
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        workers.owners = workers
            .owners
            .checked_add(1)
            .expect("remote cleanup owner count overflow");
        drop(workers);
        Self {
            state: self.state.clone(),
            sender: Some(
                self.sender
                    .as_ref()
                    .expect("a live cleanup owner retains its supervisor sender")
                    .clone(),
            ),
        }
    }
}

impl Drop for RemoteCleanup {
    fn drop(&mut self) {
        let mut workers = self
            .state
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        workers.owners = workers
            .owners
            .checked_sub(1)
            .expect("remote cleanup owner drop matches its construction");
        self.state.changed.notify_all();
    }
}

impl std::fmt::Debug for RemoteCleanup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteCleanup")
            .finish_non_exhaustive()
    }
}

struct CleanupWorkerContext {
    scope_id: usize,
}

impl CleanupWorkerContext {
    fn enter(state: &std::sync::Arc<CleanupState>, budget: CleanupBudget) -> Self {
        let scope_id = std::sync::Arc::as_ptr(state) as usize;
        ACTIVE_REMOTE_CLEANUP_SCOPES.with(|scopes| {
            scopes
                .borrow_mut()
                .push(ActiveCleanupScope { scope_id, budget });
        });
        Self { scope_id }
    }
}

impl Drop for CleanupWorkerContext {
    fn drop(&mut self) {
        ACTIVE_REMOTE_CLEANUP_SCOPES.with(|scopes| {
            let removed = scopes.borrow_mut().pop();
            debug_assert_eq!(removed.map(|scope| scope.scope_id), Some(self.scope_id));
        });
    }
}

struct CleanupWorkerCompletion(std::sync::Arc<CleanupState>);

impl Drop for CleanupWorkerCompletion {
    fn drop(&mut self) {
        let mut workers = self
            .0
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        workers.pending = workers
            .pending
            .checked_sub(1)
            .expect("cleanup worker completion matches a pending worker");
        self.0.changed.notify_all();
    }
}

impl RemoteCleanup {
    /// Create a cleanup scope before any remote-resource ownership is accepted.
    pub fn new() -> std::io::Result<Self> {
        Self::try_new_with(|operation| {
            std::thread::Builder::new()
                .name("rcp-cleanup-supervisor".to_string())
                .spawn(operation)
        })
    }

    fn try_new_with<F>(start_supervisor: F) -> std::io::Result<Self>
    where
        F: FnOnce(
            Box<dyn FnOnce() + Send + 'static>,
        ) -> std::io::Result<std::thread::JoinHandle<()>>,
    {
        let state = std::sync::Arc::new(CleanupState {
            workers: std::sync::Mutex::new(CleanupWorkers::default()),
            changed: std::sync::Condvar::new(),
            supervisor: std::sync::Mutex::new(None),
        });
        let (sender, receiver) = std::sync::mpsc::channel();
        // retain the scope until every sender is gone so unwinding still drains queued cleanup
        let supervisor_state = state.clone();
        let supervisor = start_supervisor(Box::new(move || {
            while let Ok(job) = receiver.recv() {
                supervisor_state
                    .start_cleanup_job(job, WorkerSpawnFallback::RunInSupervisor)
                    .expect("the cleanup supervisor owns its worker-spawn fallback");
            }
        }))?;
        *state
            .supervisor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(supervisor);
        Ok(Self {
            state,
            sender: Some(sender),
        })
    }

    fn scope_id(&self) -> usize {
        std::sync::Arc::as_ptr(&self.state) as usize
    }

    fn current_budget(&self) -> Option<CleanupBudget> {
        let scope_id = self.scope_id();
        ACTIVE_REMOTE_CLEANUP_SCOPES.with(|scopes| {
            scopes
                .borrow()
                .iter()
                .rev()
                .find(|scope| scope.scope_id == scope_id)
                .map(|scope| scope.budget.clone())
        })
    }

    fn submit(&self, job: CleanupJob) -> std::io::Result<()> {
        let mut workers = self
            .state
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        workers.pending = workers
            .pending
            .checked_add(1)
            .expect("cleanup worker count overflow");
        let job_id = workers.allocate_job_id();
        drop(workers);
        let job = ScheduledCleanupJob { id: job_id, job };
        match self
            .sender
            .as_ref()
            .expect("a live cleanup owner retains its supervisor sender")
            .send(job)
        {
            Ok(()) => Ok(()),
            Err(error) => {
                tracing::error!(
                    "remote cleanup supervisor stopped unexpectedly; starting an isolated fallback worker"
                );
                self.state
                    .start_cleanup_job(error.0, WorkerSpawnFallback::Abandon)
            }
        }
    }

    fn submit_disposable<F>(&self, thread_name: &'static str, operation: F) -> std::io::Result<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.submit(CleanupJob {
            thread_name,
            deadline: std::time::Instant::now() + REMOTE_CLEANUP_GRACE,
            operation: CleanupOperation::Disposable(Box::new(operation)),
        })
    }

    fn submit_bounded<F>(&self, thread_name: &'static str, operation: F) -> std::io::Result<()>
    where
        F: FnOnce(CleanupBudget) + Send + 'static,
    {
        self.submit(CleanupJob {
            thread_name,
            deadline: std::time::Instant::now() + REMOTE_CLEANUP_GRACE,
            operation: CleanupOperation::Bounded(Box::new(operation)),
        })
    }

    fn defer_drop<T>(&self, value: T, thread_name: &'static str)
    where
        T: Send + 'static,
    {
        if self.current_budget().is_some() {
            drop(value);
            return;
        }
        if let Err(error) = self.submit_disposable(thread_name, move || drop(value)) {
            tracing::error!("failed to start deferred cleanup worker: {error:#}");
        }
    }

    fn defer_bounded<F>(&self, thread_name: &'static str, operation: F)
    where
        F: FnOnce(CleanupBudget) + Send + 'static,
    {
        if let Some(budget) = self.current_budget() {
            operation(budget);
            return;
        }
        if let Err(error) = self.submit_bounded(thread_name, operation) {
            tracing::error!("failed to start bounded cleanup worker: {error:#}");
        }
    }

    /// Wait for this invocation's remaining owners and cleanup jobs under one shared time budget.
    pub fn finish(self) {
        let _supervisor_joined = self.finish_with_grace(REMOTE_CLEANUP_GRACE);
    }

    fn finish_with_grace(mut self, grace: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + grace;
        let state = self.state.clone();
        let mut workers = state
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cutoff_job_id = workers.next_job_id;
        workers.finalizations.push(CleanupFinalization {
            cutoff_job_id,
            deadline,
        });
        state.changed.notify_all();
        drop(workers);
        // this owner cannot submit more work. Closing its sender before waiting lets an otherwise
        // idle supervisor retire concurrently with workers instead of starting after grace expires.
        drop(self.sender.take());
        let mut workers = state
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while workers.owners > 1
            || workers.pending > 0
            || workers.threads.iter().any(|worker| !worker.is_finished())
            || state
                .supervisor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_some_and(|supervisor| !supervisor.is_finished())
        {
            let now = std::time::Instant::now();
            if now >= deadline {
                break;
            }
            // pending completion is signaled just before the OS marks its JoinHandle finished.
            // poll that final transition under the same deadline rather than performing an
            // unbounded join or detaching an almost-finished worker.
            let wait = std::cmp::min(
                CLEANUP_POLL_INTERVAL,
                deadline.saturating_duration_since(now),
            );
            let (next, _) = state
                .changed
                .wait_timeout(workers, wait)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            workers = next;
        }
        let remaining_owners = workers.owners.saturating_sub(1);
        let pending = workers.pending;
        let mut completed = Vec::new();
        let mut still_running = Vec::new();
        for worker in std::mem::take(&mut workers.threads) {
            if worker.is_finished() {
                completed.push(worker);
            } else {
                still_running.push(worker);
            }
        }
        let unfinished_workers = still_running.len();
        workers.threads = still_running;
        drop(workers);
        for worker in completed {
            if let Err(error) = worker.join() {
                tracing::debug!("deferred remote cleanup worker panicked: {error:?}");
            }
        }
        if pending > 0 || unfinished_workers > 0 {
            tracing::debug!(
                "{pending} deferred remote cleanup job(s) and {unfinished_workers} worker thread(s) did not finish within {}; abandoning them at process exit",
                humantime::format_duration(grace)
            );
        }
        if remaining_owners > 0 {
            tracing::debug!(
                "{remaining_owners} remote cleanup owner(s) remained live after {}; abandoning their cleanup at process exit",
                humantime::format_duration(grace)
            );
        }
        let supervisor = state
            .supervisor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        match supervisor {
            Some(supervisor) if supervisor.is_finished() => {
                if let Err(error) = supervisor.join() {
                    tracing::debug!("remote cleanup supervisor panicked: {error:?}");
                }
                true
            }
            Some(_) => {
                tracing::debug!(
                    "remote cleanup supervisor did not finish within its shared cleanup grace"
                );
                false
            }
            None => true,
        }
    }
}

#[derive(Clone, Copy)]
enum WorkerSpawnFallback {
    RunInSupervisor,
    Abandon,
}

impl CleanupState {
    fn start_cleanup_job(
        self: &std::sync::Arc<Self>,
        job: ScheduledCleanupJob,
        fallback: WorkerSpawnFallback,
    ) -> std::io::Result<()> {
        let thread_name = job.job.thread_name;
        // retain the job until the OS confirms worker creation. The start gate lets the handle enter
        // shared state before the worker can finish, without holding that mutex across thread spawn.
        let job = std::sync::Arc::new(std::sync::Mutex::new(Some(job)));
        let worker_job = job.clone();
        let state = self.clone();
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        match std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(move || {
                if start_rx.recv().is_err() {
                    return;
                }
                if let Some(job) = worker_job
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    state.run_cleanup_job(job);
                }
            }) {
            Ok(worker) => {
                self.workers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .threads
                    .push(worker);
                if start_tx.send(()).is_err() {
                    let job = job
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                        .expect("a failed worker start signal retains its job");
                    return self.handle_worker_start_failure(
                        job,
                        fallback,
                        std::io::Error::other("cleanup worker exited before its start signal"),
                    );
                }
                Ok(())
            }
            Err(error) => {
                let job = job
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .expect("a failed cleanup worker start retains its job");
                self.handle_worker_start_failure(job, fallback, error)
            }
        }
    }

    fn handle_worker_start_failure(
        self: &std::sync::Arc<Self>,
        job: ScheduledCleanupJob,
        fallback: WorkerSpawnFallback,
        error: std::io::Error,
    ) -> std::io::Result<()> {
        match fallback {
            WorkerSpawnFallback::RunInSupervisor => {
                tracing::error!(
                    "failed to start deferred cleanup worker; running cleanup in its supervisor: {error:#}"
                );
                self.run_cleanup_job(job);
                Ok(())
            }
            WorkerSpawnFallback::Abandon => {
                tracing::error!(
                    "failed to start an isolated fallback cleanup worker; leaking the cleanup job for process-exit reclamation: {error:#}"
                );
                std::mem::forget(job);
                let mut workers = self
                    .workers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                workers.pending = workers
                    .pending
                    .checked_sub(1)
                    .expect("abandoned cleanup matches a pending job");
                self.changed.notify_all();
                Err(error)
            }
        }
    }

    fn run_cleanup_job(self: &std::sync::Arc<Self>, scheduled: ScheduledCleanupJob) {
        let _completion = CleanupWorkerCompletion(self.clone());
        let budget = CleanupBudget {
            state: self.clone(),
            job_id: scheduled.id,
            job_deadline: scheduled.job.deadline,
        };
        let _context = CleanupWorkerContext::enter(self, budget.clone());
        let operation = move || match scheduled.job.operation {
            CleanupOperation::Bounded(operation) => operation(budget),
            CleanupOperation::Disposable(operation) => operation(),
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)).is_err() {
            tracing::debug!("deferred remote cleanup job panicked");
        }
    }
}

/// Owns a Tokio task and aborts it if its supervising future is cancelled.
pub struct AbortOnDropTask<T>(tokio::task::JoinHandle<T>);

impl<T> AbortOnDropTask<T> {
    /// Retain a task under cancellation-safe ownership.
    pub fn new(task: tokio::task::JoinHandle<T>) -> Self {
        Self(task)
    }

    /// Await and consume the retained task.
    pub async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.0).await
    }

    async fn wait(&mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.0).await
    }

    fn abort(&self) {
        self.0.abort();
    }

    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
pub(crate) struct CapturedOutput {
    pub(crate) bytes: Vec<u8>,
    pub(crate) truncated: bool,
    pub(crate) read_error: Option<std::io::Error>,
}

impl CapturedOutput {
    pub(crate) fn rendered(&self) -> String {
        let body = String::from_utf8_lossy(&self.bytes);
        let truncation = self.truncated.then(|| {
            format!(
                "[output truncated to the last {} bytes]\n",
                self.bytes.len()
            )
        });
        let read_error = self
            .read_error
            .as_ref()
            .map(|error| format!("\n[output read failed: {error:#}]"));
        format!(
            "{}{}{}",
            truncation.as_deref().unwrap_or_default(),
            body,
            read_error.as_deref().unwrap_or_default()
        )
    }
}

fn retain_bounded_tail(output: &mut Vec<u8>, input: &[u8], limit: usize) -> bool {
    if input.is_empty() {
        return false;
    }
    if limit == 0 {
        return true;
    }
    if input.len() >= limit {
        let truncated = !output.is_empty() || input.len() > limit;
        output.clear();
        output.extend_from_slice(&input[input.len() - limit..]);
        return truncated;
    }
    let overflow = output
        .len()
        .saturating_add(input.len())
        .saturating_sub(limit);
    if overflow > 0 {
        output.drain(..overflow);
    }
    output.extend_from_slice(input);
    overflow > 0
}

async fn drain_bounded_output_forwarding<R, F>(
    mut reader: R,
    limit: usize,
    mut forward: F,
) -> CapturedOutput
where
    R: tokio::io::AsyncRead + Unpin,
    F: FnMut(&[u8]),
{
    use tokio::io::AsyncReadExt;

    let mut output = CapturedOutput::default();
    let mut chunk = [0_u8; 8192];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => break,
            Ok(bytes_read) => {
                forward(&chunk[..bytes_read]);
                output.truncated |=
                    retain_bounded_tail(&mut output.bytes, &chunk[..bytes_read], limit);
            }
            Err(error) => {
                output.read_error = Some(error);
                break;
            }
        }
    }
    output
}

pub(crate) async fn drain_bounded_output<R>(reader: R, limit: usize) -> CapturedOutput
where
    R: tokio::io::AsyncRead + Unpin,
{
    drain_bounded_output_forwarding(reader, limit, |_| {}).await
}

struct Utf8ChunkForwarder<F> {
    pending: Vec<u8>,
    forward: F,
}

impl<F> Utf8ChunkForwarder<F>
where
    F: FnMut(&str),
{
    fn new(forward: F) -> Self {
        Self {
            pending: Vec::new(),
            forward,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        self.pending.extend_from_slice(chunk);
        let complete = complete_utf8_prefix_len(&self.pending);
        if complete == 0 {
            return;
        }
        let incomplete = self.pending.split_off(complete);
        let output = String::from_utf8_lossy(&self.pending);
        (self.forward)(&output);
        self.pending = incomplete;
    }

    fn finish(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let output = String::from_utf8_lossy(&self.pending);
        (self.forward)(&output);
        self.pending.clear();
    }
}

fn complete_utf8_prefix_len(bytes: &[u8]) -> usize {
    let mut consumed = 0;
    while consumed < bytes.len() {
        match std::str::from_utf8(&bytes[consumed..]) {
            Ok(_) => return bytes.len(),
            Err(error) => {
                consumed += error.valid_up_to();
                let Some(invalid) = error.error_len() else {
                    return consumed;
                };
                consumed += invalid;
            }
        }
    }
    bytes.len()
}

async fn drain_rcpd_output<R>(reader: R, host: String, stream: &'static str) -> CapturedOutput
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut forwarder = Utf8ChunkForwarder::new(move |output: &str| {
        let output = output.trim_end_matches(['\r', '\n']);
        if !output.is_empty() {
            tracing::debug!(host, stream, output, "rcpd output");
        }
    });
    let captured = drain_bounded_output_forwarding(reader, DIAGNOSTIC_CAPTURE_LIMIT, |chunk| {
        forwarder.push(chunk);
    })
    .await;
    forwarder.finish();
    captured
}

/// Prefix for an intentional daemon refusal in place of a readiness record.
pub const RCPD_STARTUP_ERROR_PREFIX: &str = "RCP_ERROR ";

#[derive(Debug)]
struct PeerPreparationCancelled;

impl std::fmt::Display for PeerPreparationCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("rcpd preparation cancelled because peer preparation failed")
    }
}

impl std::error::Error for PeerPreparationCancelled {}

/// Cancellation and owned-work completion policy for one endpoint preparation.
#[derive(Clone)]
pub(crate) struct PreparationContext {
    cancellation: tokio_util::sync::CancellationToken,
    cleanup: RemoteCleanup,
    control_directory_exclusions: std::sync::Arc<[std::path::PathBuf]>,
}

struct SshMasterReaper {
    launcher: Option<tokio::process::Child>,
    control_directory: Option<std::path::PathBuf>,
}

pub(crate) fn poll_process_exit_until_deadline(
    budget: &CleanupBudget,
    mut try_wait: impl FnMut() -> std::io::Result<bool>,
) -> std::io::Result<bool> {
    loop {
        if try_wait()? {
            return Ok(true);
        }
        if !budget.wait_for_next_poll() {
            return Ok(false);
        }
    }
}

fn reap_process_and_control_directory(
    budget: &CleanupBudget,
    control_directory: Option<&std::path::Path>,
    try_wait: impl FnMut() -> std::io::Result<bool>,
) {
    let master_exited = match poll_process_exit_until_deadline(budget, try_wait) {
        Ok(exited) => exited,
        Err(error) => {
            tracing::debug!("failed to reap SSH multiplex master: {error:#}");
            false
        }
    };
    if !master_exited {
        tracing::debug!(
            "SSH multiplex master did not exit within its cleanup budget; preserving its control directory"
        );
    }
    if master_exited
        && let Some(control_directory) = control_directory
        && let Err(error) = std::fs::remove_dir_all(control_directory)
    {
        tracing::debug!(
            "failed to remove SSH control directory {}: {error:#}",
            control_directory.display()
        );
    }
}

impl SshMasterReaper {
    fn reap(mut self, budget: CleanupBudget) {
        if let Some(launcher) = self.launcher.as_mut() {
            reap_process_and_control_directory(&budget, self.control_directory.as_deref(), || {
                launcher.try_wait().map(|status| status.is_some())
            });
        } else {
            reap_process_and_control_directory(&budget, self.control_directory.as_deref(), || {
                Ok(true)
            });
        }
    }
}

struct PreparingSshMaster {
    launcher: Option<tokio::process::Child>,
    control_directory: Option<std::path::PathBuf>,
    cleanup: RemoteCleanup,
}

impl PreparingSshMaster {
    fn new(control_directory: std::path::PathBuf, cleanup: RemoteCleanup) -> Self {
        Self {
            launcher: None,
            control_directory: Some(control_directory),
            cleanup,
        }
    }

    fn control_directory(&self) -> &std::path::Path {
        self.control_directory
            .as_deref()
            .expect("preparing SSH control directory remains owned")
    }

    fn retain_launcher(&mut self, launcher: tokio::process::Child) {
        debug_assert!(self.launcher.is_none());
        self.launcher = Some(launcher);
    }

    fn launcher_mut(&mut self) -> &mut tokio::process::Child {
        self.launcher
            .as_mut()
            .expect("preparing SSH master retains its launcher")
    }

    fn into_managed(mut self, session: openssh::Session) -> ManagedSshSession {
        let launcher = self
            .launcher
            .take()
            .expect("prepared SSH session takes its launcher once");
        let control_directory = self
            .control_directory
            .take()
            .expect("prepared SSH session takes its control directory once");
        ManagedSshSession::new(session, launcher, control_directory, self.cleanup.clone())
    }
}

impl Drop for PreparingSshMaster {
    fn drop(&mut self) {
        if let Some(launcher) = self.launcher.as_mut()
            && let Err(error) = launcher.start_kill()
        {
            tracing::debug!("failed to terminate preparing SSH multiplex master: {error:#}");
        }
        if self.launcher.is_none() && self.control_directory.is_none() {
            return;
        }
        let reaper = SshMasterReaper {
            launcher: self.launcher.take(),
            control_directory: self.control_directory.take(),
        };
        self.cleanup
            .defer_bounded("rcp-ssh-cleanup", move |budget| reaper.reap(budget));
    }
}

struct ManagedSshSessionInner {
    session: Option<openssh::Session>,
    launcher: Option<tokio::process::Child>,
    control_directory: Option<std::path::PathBuf>,
    cleanup: RemoteCleanup,
}

impl Drop for ManagedSshSessionInner {
    fn drop(&mut self) {
        // release the resumed native-mux handle without asking it to shut down the externally owned
        // master. Signal that retained process before returning from Drop, then move reaping and
        // directory removal to an executor that remains available after Tokio shutdown.
        let _detached_paths = self.session.take().map(openssh::Session::detach);
        if let Some(launcher) = self.launcher.as_mut()
            && let Err(error) = launcher.start_kill()
        {
            tracing::debug!("failed to terminate SSH multiplex master: {error:#}");
        }
        let reaper = SshMasterReaper {
            launcher: self.launcher.take(),
            control_directory: self.control_directory.take(),
        };
        self.cleanup
            .defer_bounded("rcp-ssh-cleanup", move |budget| reaper.reap(budget));
    }
}

/// Cloneable session ownership whose final drop cannot block or depend on Tokio still running.
#[derive(Clone)]
pub(crate) struct ManagedSshSession(std::sync::Arc<ManagedSshSessionInner>);

impl std::fmt::Debug for ManagedSshSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagedSshSession")
            .finish_non_exhaustive()
    }
}

impl ManagedSshSession {
    fn new(
        session: openssh::Session,
        launcher: tokio::process::Child,
        control_directory: std::path::PathBuf,
        cleanup: RemoteCleanup,
    ) -> Self {
        Self(std::sync::Arc::new(ManagedSshSessionInner {
            session: Some(session),
            launcher: Some(launcher),
            control_directory: Some(control_directory),
            cleanup,
        }))
    }

    fn cleanup(&self) -> RemoteCleanup {
        self.0.cleanup.clone()
    }
}

impl std::ops::Deref for ManagedSshSession {
    type Target = openssh::Session;

    fn deref(&self) -> &Self::Target {
        self.0
            .session
            .as_ref()
            .expect("managed SSH session is present before final drop")
    }
}

pub(crate) trait SshSessionOwner:
    Clone + std::ops::Deref<Target = openssh::Session> + Send + Sync + 'static
{
}

impl<T> SshSessionOwner for T where
    T: Clone + std::ops::Deref<Target = openssh::Session> + Send + Sync + 'static
{
}

impl PreparationContext {
    fn new(cancellation: tokio_util::sync::CancellationToken, cleanup: RemoteCleanup) -> Self {
        Self {
            cancellation,
            cleanup,
            control_directory_exclusions: Vec::new().into(),
        }
    }

    fn uncancelled(cleanup: RemoteCleanup) -> Self {
        Self::new(tokio_util::sync::CancellationToken::new(), cleanup)
    }

    fn with_control_directory_exclusions(
        mut self,
        exclusions: std::sync::Arc<[std::path::PathBuf]>,
    ) -> Self {
        self.control_directory_exclusions = exclusions;
        self
    }

    fn cancellation_error() -> anyhow::Error {
        anyhow::Error::new(PeerPreparationCancelled)
    }

    fn ensure_active(&self) -> anyhow::Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(Self::cancellation_error());
        }
        Ok(())
    }

    fn record_owned_completion<T>(
        cleanup: &RemoteCleanup,
        result: Result<anyhow::Result<T>, tokio::task::JoinError>,
        operation: &str,
        timing: &str,
    ) where
        T: Send + 'static,
    {
        match result {
            Ok(Ok(value)) => {
                tracing::debug!("{operation} completed {timing}");
                cleanup.defer_drop(value, "rcp-owned-result-dispose");
            }
            Ok(Err(error)) => {
                tracing::debug!("{operation} failed {timing}: {error:#}");
            }
            Err(error) => {
                tracing::debug!("{operation} owner task failed {timing}: {error:#}");
            }
        }
    }

    async fn finish_or_abort_owned_during_grace<T>(
        &self,
        mut task: AbortOnDropTask<anyhow::Result<T>>,
        grace: std::time::Duration,
        operation: &'static str,
    ) where
        T: Send + 'static,
    {
        match tokio::time::timeout(grace, task.wait()).await {
            Ok(result) => {
                Self::record_owned_completion(
                    &self.cleanup,
                    result,
                    operation,
                    "during cancellation grace",
                );
            }
            Err(_) => {
                self.abort_owned(task, operation, "cancellation grace");
            }
        }
    }

    /// Await cancellation-safe work that owns no process or session after its future is dropped.
    pub(crate) async fn run<T>(
        &self,
        operation: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(Self::cancellation_error()),
            result = operation => result,
        }
    }

    /// Await a transaction with no wall-clock deadline, closing its local owner after cancellation.
    pub(crate) async fn run_cancellation_owned_transaction<T>(
        &self,
        task: tokio::task::JoinHandle<anyhow::Result<T>>,
        grace: std::time::Duration,
        operation: &'static str,
    ) -> anyhow::Result<T>
    where
        T: Send + 'static,
    {
        let mut task = AbortOnDropTask::new(task);
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                self.finish_or_abort_owned_during_grace(task, grace, operation).await;
                Err(Self::cancellation_error())
            }
            result = task.wait() => result
                .with_context(|| format!("{operation} owner task failed"))?,
        }
    }

    fn abort_owned<T>(
        &self,
        task: AbortOnDropTask<anyhow::Result<T>>,
        operation: &str,
        reason: &str,
    ) where
        T: Send + 'static,
    {
        task.abort();
        let cleanup = self.cleanup.clone();
        let result_cleanup = cleanup.clone();
        let operation = operation.to_string();
        let reason = reason.to_string();
        let completion_timing = format!("as {reason} arrived");
        cleanup.defer_bounded("rcp-owned-task-reap", move |budget| {
            while !task.is_finished() {
                if !budget.wait_for_next_poll() {
                    tracing::debug!(
                        "{operation} owner task did not finish after {reason} within its cleanup budget"
                    );
                    return;
                }
            }
            let result = futures::executor::block_on(task.join());
            Self::record_owned_completion(
                &result_cleanup,
                result,
                &operation,
                &completion_timing,
            );
        });
    }

    async fn run_abortable_with_deadline<T>(
        &self,
        task: tokio::task::JoinHandle<anyhow::Result<T>>,
        operation: &str,
        deadline: BootstrapDeadline,
    ) -> anyhow::Result<T>
    where
        T: Send + 'static,
    {
        let mut task = AbortOnDropTask::new(task);
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                self.abort_owned(task, operation, "peer cancellation");
                Err(Self::cancellation_error())
            }
            result = deadline.wait(operation, task.wait()) => {
                match result {
                    Ok(result) => result
                        .with_context(|| format!("{operation} owner task failed"))?,
                    Err(error) => {
                        self.abort_owned(task, operation, "its bootstrap deadline");
                        Err(error)
                    }
                }
            }
        }
    }

    /// Run a remote command in an owned task so cancellation cannot orphan its local SSH child.
    fn remote_output_task<S: SshSessionOwner>(
        mut command: openssh::OwningCommand<S>,
        operation: String,
    ) -> tokio::task::JoinHandle<anyhow::Result<std::process::Output>> {
        tokio::spawn(async move {
            command
                .stdin(openssh::Stdio::null())
                .stdout(openssh::Stdio::piped())
                .stderr(openssh::Stdio::piped());
            let mut child = command
                .spawn()
                .await
                .with_context(|| format!("{operation} failed to start"))?;
            let stdout = child
                .stdout()
                .take()
                .with_context(|| format!("{operation} stdout was not piped"))?;
            let stderr = child
                .stderr()
                .take()
                .with_context(|| format!("{operation} stderr was not piped"))?;
            let (status, stdout, stderr) = tokio::join!(
                child.wait(),
                drain_bounded_output(stdout, DIAGNOSTIC_CAPTURE_LIMIT),
                drain_bounded_output(stderr, DIAGNOSTIC_CAPTURE_LIMIT),
            );
            let status = status.with_context(|| format!("{operation} failed to wait"))?;
            let stdout = finish_remote_output_capture(&operation, "stdout", stdout, true)?;
            let stderr = finish_remote_output_capture(&operation, "stderr", stderr, false)?;
            Ok(std::process::Output {
                status,
                stdout,
                stderr,
            })
        })
    }

    /// Run a remote probe with its required hard deadline.
    ///
    /// On deadline the local SSH channel is explicitly aborted. The remote command may outlive
    /// that channel, which is why this is reserved for probes whose hard deadline is itself part
    /// of the protocol contract, never for staging ownership.
    async fn remote_output<S: SshSessionOwner>(
        &self,
        command: openssh::OwningCommand<S>,
        operation: &str,
        deadline: BootstrapDeadline,
    ) -> anyhow::Result<std::process::Output> {
        self.run_abortable_with_deadline(
            Self::remote_output_task(command, operation.to_string()),
            operation,
            deadline,
        )
        .await
    }
}

fn finish_remote_output_capture(
    operation: &str,
    stream: &str,
    mut output: CapturedOutput,
    reject_truncation: bool,
) -> anyhow::Result<Vec<u8>> {
    if let Some(error) = output.read_error.take() {
        return Err(error).with_context(|| format!("failed to read {operation} {stream}"));
    }
    if output.truncated && reject_truncation {
        anyhow::bail!(
            "{operation} {stream} exceeded the {}-byte capture limit",
            DIAGNOSTIC_CAPTURE_LIMIT
        );
    }
    if output.truncated {
        Ok(output.rendered().into_bytes())
    } else {
        Ok(output.bytes)
    }
}

#[derive(Clone, Copy)]
struct BootstrapDeadline(std::time::Duration);

impl BootstrapDeadline {
    const fn new(duration: std::time::Duration) -> Self {
        Self(duration)
    }

    fn at_least(self, minimum: std::time::Duration) -> Self {
        Self(std::cmp::max(self.0, minimum))
    }

    async fn run<T>(
        self,
        operation: &str,
        future: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        self.wait(operation, future).await?
    }

    async fn wait<T>(
        self,
        operation: &str,
        future: impl std::future::Future<Output = T>,
    ) -> anyhow::Result<T> {
        tokio::time::timeout(self.0, future).await.with_context(|| {
            format!(
                "{operation} timed out after {}",
                humantime::format_duration(self.0)
            )
        })
    }
}

mod deploy;
pub mod port_ranges;
pub mod protocol;
pub mod streams;
pub mod tls;
pub mod tracelog;

/// Network profile for TCP configuration tuning
///
/// Profiles provide pre-configured settings optimized for different network environments.
/// The Datacenter profile is optimized for high-bandwidth, low-latency datacenter networks,
/// while the Internet profile uses more conservative settings suitable for internet connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum NetworkProfile {
    /// Optimized for datacenter networks: <1ms RTT, 25-100 Gbps
    /// Uses aggressive buffer sizes
    #[default]
    Datacenter,
    /// Conservative settings for internet connections
    /// Uses standard buffer sizes
    Internet,
}

impl std::fmt::Display for NetworkProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Datacenter => write!(f, "datacenter"),
            Self::Internet => write!(f, "internet"),
        }
    }
}

impl std::str::FromStr for NetworkProfile {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "datacenter" => Ok(Self::Datacenter),
            "internet" => Ok(Self::Internet),
            _ => Err(format!(
                "invalid network profile '{}', expected 'datacenter' or 'internet'",
                s
            )),
        }
    }
}

/// Datacenter profile: buffer size for remote copy operations (16 MiB)
pub const DATACENTER_REMOTE_COPY_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// Internet profile: buffer size for remote copy operations (2 MiB)
pub const INTERNET_REMOTE_COPY_BUFFER_SIZE: usize = 2 * 1024 * 1024;

impl NetworkProfile {
    /// Returns the default buffer size for remote copy operations for this profile
    ///
    /// Datacenter profile uses a large buffer (16 MiB) matching the per-stream receive window
    /// to maximize throughput on high-bandwidth networks.
    /// Internet profile uses a smaller buffer (2 MiB) suitable for internet connections.
    pub fn default_remote_copy_buffer_size(&self) -> usize {
        match self {
            Self::Datacenter => DATACENTER_REMOTE_COPY_BUFFER_SIZE,
            Self::Internet => INTERNET_REMOTE_COPY_BUFFER_SIZE,
        }
    }
}

/// Configuration for TCP connections
///
/// Used to configure TCP listeners and clients for file transfers.
#[derive(Debug, Clone)]
pub struct TcpConfig {
    /// Port ranges to use for TCP connections (e.g., "8000-8999,9000-9999")
    pub port_ranges: Option<String>,
    /// Connection timeout for remote operations (seconds)
    pub conn_timeout_sec: u64,
    /// Network profile for tuning (default: Datacenter)
    pub network_profile: NetworkProfile,
    /// Buffer size for file transfers (defaults to profile-specific value)
    pub buffer_size: Option<usize>,
    /// Liveness budget for every rcp TCP connection (seconds), 0 disables it.
    /// See [`configure_tcp_socket`].
    pub keepalive_sec: u64,
}

/// Default ceiling for concurrent remote data connections.
pub const DEFAULT_MAX_CONNECTIONS: usize = 100;

/// Default multiplier for pending writes (4× max_connections).
pub const DEFAULT_PENDING_WRITES_MULTIPLIER: usize = 4;

/// Default liveness budget for an rcp TCP connection (seconds).
///
/// Detects a vanished peer host in about two minutes while surviving any stall shorter than that.
/// Exposed as `--remote-keepalive-sec`; see [`configure_tcp_socket`].
pub const DEFAULT_REMOTE_KEEPALIVE_SEC: u64 = 120;

/// Validated capacities used by the remote file-transfer pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedRemoteConcurrency {
    files_in_flight: common::ConcurrencyLimit,
    max_connections: std::num::NonZeroUsize,
    max_pending_files: std::num::NonZeroUsize,
}

impl ResolvedRemoteConcurrency {
    #[must_use]
    pub const fn files_in_flight(self) -> common::ConcurrencyLimit {
        self.files_in_flight
    }

    #[must_use]
    pub const fn max_connections(self) -> std::num::NonZeroUsize {
        self.max_connections
    }

    #[must_use]
    pub const fn max_pending_files(self) -> std::num::NonZeroUsize {
        self.max_pending_files
    }
}

/// Resolve and validate all capacities that become Tokio semaphores in remote copy.
pub fn resolve_remote_concurrency(
    files_in_flight: common::ConcurrencyLimit,
    max_connections: std::num::NonZeroUsize,
    pending_writes_multiplier: std::num::NonZeroUsize,
) -> anyhow::Result<ResolvedRemoteConcurrency> {
    let max_connections = effective_max_connections(files_in_flight, max_connections);
    if max_connections.get() > tokio::sync::Semaphore::MAX_PERMITS {
        anyhow::bail!(
            "effective stream capacity {} exceeds the Tokio semaphore maximum {}",
            max_connections,
            tokio::sync::Semaphore::MAX_PERMITS,
        );
    }
    let max_pending_files = max_connections
        .get()
        .checked_mul(pending_writes_multiplier.get())
        .context("pending file capacity overflow")?;
    if max_pending_files > tokio::sync::Semaphore::MAX_PERMITS {
        anyhow::bail!(
            "pending file capacity {} exceeds the Tokio semaphore maximum {}",
            max_pending_files,
            tokio::sync::Semaphore::MAX_PERMITS,
        );
    }
    Ok(ResolvedRemoteConcurrency {
        files_in_flight,
        max_connections,
        max_pending_files: std::num::NonZeroUsize::new(max_pending_files)
            .expect("nonzero factors have a nonzero product"),
    })
}

/// Intersect the configured data-stream ceiling with the file-work ceiling.
#[must_use]
fn effective_max_connections(
    files_in_flight: common::ConcurrencyLimit,
    configured: std::num::NonZeroUsize,
) -> std::num::NonZeroUsize {
    match files_in_flight.meet(common::ConcurrencyLimit::Limited(configured)) {
        common::ConcurrencyLimit::Limited(value) => value,
        common::ConcurrencyLimit::Unlimited => {
            unreachable!("a finite connection ceiling always makes the intersection finite")
        }
    }
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            port_ranges: None,
            conn_timeout_sec: 15,
            network_profile: NetworkProfile::default(),
            buffer_size: None,
            keepalive_sec: DEFAULT_REMOTE_KEEPALIVE_SEC,
        }
    }
}

impl TcpConfig {
    /// Get the effective buffer size (explicit or profile default)
    pub fn effective_buffer_size(&self) -> usize {
        self.buffer_size
            .unwrap_or_else(|| self.network_profile.default_remote_copy_buffer_size())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct SshSession {
    pub user: Option<String>,
    pub host: String,
    pub port: Option<u16>,
}

impl SshSession {
    pub fn local() -> Self {
        Self {
            user: None,
            host: "localhost".to_string(),
            port: None,
        }
    }

    /// Return the host spelling accepted by OpenSSH's direct argv interface.
    fn openssh_host(&self) -> &str {
        let Some(host) = self
            .host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
        else {
            return &self.host;
        };
        let address = match host.split_once('%') {
            Some((_, "")) => return &self.host,
            Some((address, _scope_id)) => address,
            None => host,
        };
        address
            .parse::<std::net::Ipv6Addr>()
            .map_or(&self.host, |_| host)
    }
}

// re-export is_localhost from common for convenience
pub use common::is_localhost;

async fn setup_ssh_session(
    session: &SshSession,
    preparation: &PreparationContext,
    deadline: BootstrapDeadline,
) -> anyhow::Result<ManagedSshSession> {
    setup_ssh_session_with_program(session, preparation, std::path::Path::new("ssh"), deadline)
        .await
}

async fn setup_ssh_session_with_program(
    session: &SshSession,
    preparation: &PreparationContext,
    ssh_program: &std::path::Path,
    deadline: BootstrapDeadline,
) -> anyhow::Result<ManagedSshSession> {
    let session = session.clone();
    let ssh_program = ssh_program.to_path_buf();
    let cleanup = preparation.cleanup.clone();
    let control_directory_exclusions = preparation.control_directory_exclusions.clone();
    let connect = tokio::spawn(async move {
        let control_dir =
            run_disposable_blocking(cleanup.clone(), "rcp-ssh-control-select", move || {
                ssh_control_directory(&control_directory_exclusions)
                    .context("no usable SSH control directory found outside local copy operands")
            })
            .await?;
        launch_ssh_master(&session, &control_dir, &ssh_program, cleanup)
            .await
            .context("Failed to establish SSH connection")
    });
    preparation
        .run_abortable_with_deadline(connect, "SSH session setup", deadline)
        .await
}

/// Run filesystem preflight on a disposable OS thread.
///
/// The caller may abandon the receiver at a bootstrap deadline without waiting for an uninterruptible
/// filesystem syscall. These probes launch no process, and a value produced after abandonment is
/// dropped on the worker thread.
async fn run_disposable_blocking<T, F>(
    cleanup: RemoteCleanup,
    thread_name: &'static str,
    operation: F,
) -> anyhow::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    cleanup
        .submit_disposable(thread_name, move || {
            let _ = result_tx.send(operation());
        })
        .with_context(|| format!("failed to start {thread_name} worker"))?;
    result_rx
        .await
        .with_context(|| format!("{thread_name} worker exited without a result"))?
}

struct StopSocketProbe(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for StopSocketProbe {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

async fn wait_for_ssh_control_socket<F>(
    cleanup: &RemoteCleanup,
    mut inspect: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> anyhow::Result<bool> + Send + 'static,
{
    let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _stop_on_exit = StopSocketProbe(stopped.clone());
    run_disposable_blocking(cleanup.clone(), "rcp-ssh-control-inspect", move || {
        while !stopped.load(std::sync::atomic::Ordering::Acquire) {
            if inspect()? {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(())
    })
    .await
}

/// Launch and retain the foreground OpenSSH multiplex master.
fn ssh_master_launcher_command(ssh_program: &std::path::Path) -> tokio::process::Command {
    // launch a known-local shell first so resolving the configured SSH program happens inside a
    // child that the bootstrap deadline can kill and reap. Calling Command::new(ssh_program)
    // directly can block the Tokio worker in the parent's synchronous spawn handshake.
    let mut launcher = tokio::process::Command::new("/bin/sh");
    launcher
        .args(["-c", "exec \"$@\"", "rcp-ssh-master"])
        .arg(ssh_program);
    launcher
}

async fn launch_ssh_master(
    session: &SshSession,
    control_dir: &std::path::Path,
    ssh_program: &std::path::Path,
    cleanup: RemoteCleanup,
) -> anyhow::Result<ManagedSshSession> {
    tracing::debug!("Connecting to SSH destination: {}", session.host);
    tracing::debug!("Using SSH control directory: {}", control_dir.display());
    let control_dir = control_dir.to_path_buf();
    let directory = run_disposable_blocking(cleanup.clone(), "rcp-ssh-control-create", move || {
        let mut temp = tempfile::Builder::new();
        temp.prefix(".ssh-connection");
        temp.tempdir_in(control_dir)
            .context("failed to create SSH control directory")
    })
    .await?;
    // persist the TempDir only after the disposable worker returned it. If that wait is abandoned,
    // tempfile cleanup stays on the worker; after persistence every exit is guarded below.
    let mut preparing = PreparingSshMaster::new(directory.keep(), cleanup.clone());
    let log = preparing.control_directory().join("log");
    let control_path = preparing.control_directory().join("master");
    let mut launcher = ssh_master_launcher_command(ssh_program);
    launcher
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .arg("-E")
        .arg(&log)
        .arg("-S")
        .arg(&control_path)
        .arg("-M")
        .arg("-N")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("StrictHostKeyChecking=no")
        .arg("-o")
        .arg("ForkAfterAuthentication=no")
        .arg("-o")
        .arg("ControlPersist=no");
    if let Some(port) = session.port {
        launcher.arg("-p").arg(port.to_string());
    }
    if let Some(user) = session.user.as_deref() {
        launcher.arg("-l").arg(user);
    }
    let launcher = launcher
        .arg(session.openssh_host())
        .spawn()
        .context("failed to launch SSH multiplex master")?;
    preparing.retain_launcher(launcher);
    {
        let socket_path = control_path.clone();
        let socket_ready = wait_for_ssh_control_socket(&cleanup, move || {
            socket_path
                .try_exists()
                .context("failed to inspect SSH multiplex control socket")
        });
        tokio::pin!(socket_ready);
        tokio::select! {
            biased;
            status = preparing.launcher_mut().wait() => {
                let status = status.context("failed to wait for SSH multiplex master")?;
                let diagnostic_path = log.clone();
                let diagnostic = run_disposable_blocking(
                    cleanup.clone(),
                    "rcp-ssh-log-read",
                    move || Ok(std::fs::read_to_string(diagnostic_path).unwrap_or_default()),
                )
                .await?;
                anyhow::bail!(
                    "SSH multiplex master exited with {status}: {}",
                    diagnostic.trim()
                );
            }
            result = &mut socket_ready => {
                result?;
            }
        }
    }
    // use the native multiplex client so every remote command opens the retained control socket
    // directly. The process backend synchronously spawns a fresh local `ssh` executable for each
    // command, which would create an uninterruptible pre-deadline PATH/exec window.
    let session =
        openssh::Session::resume_mux(control_path.into_boxed_path(), Some(log.into_boxed_path()));
    Ok(preparing.into_managed(session))
}

/// Where to put the SSH connection-multiplexing socket.
///
/// `openssh` defaults this to `$XDG_STATE_HOME`, else `$HOME/.local/state`, and the resulting
/// socket path is subject to `sockaddr_un`'s 108-byte `sun_path` limit -- a limit that counts the
/// terminating NUL, the `/.ssh-connectionXXXXXX/master` the library appends, AND the further
/// `.XXXXXXXXXXXXXXXX` that ssh(1) itself appends while creating the socket before renaming it.
/// That leaves only about 48 bytes for `$HOME`, and exceeding it fails with ssh's rather opaque
/// `unix_listener: path "..." too long for Unix domain socket`, which names neither `$HOME` nor
/// rcp. (Kept as an inline span rather than an indented block: rustdoc reads an indented block as
/// a doctest, and a doctest on a private item is denied by the workspace rustdoc lints.)
///
/// 48 bytes is not a comfortable margin: container workspaces, network homes and CI checkout paths
/// routinely run longer.
///
/// Candidates are tried in order and the first genuinely usable one wins:
///
/// 1. `$XDG_RUNTIME_DIR` -- typically `/run/user/<uid>`, ~14 bytes, and semantically the right
///    home for a runtime socket: per-user, mode 0700, cleared when the session ends.
/// 2. `std::env::temp_dir()` -- honours `TMPDIR`, so it follows a sandbox that redirects it.
/// 3. `/tmp` -- for when `TMPDIR` itself points somewhere long or unusable.
/// 4. `$XDG_STATE_HOME` -- the first location used by `openssh`'s own default.
/// 5. `$HOME` -- a shorter final fallback when the state directory is absent or unusable.
///
/// Falling through the list is the point rather than a nicety: the environments named above as
/// motivation -- containers, CI runners, `su` sessions -- are exactly the ones that tend NOT to set
/// `$XDG_RUNTIME_DIR`, so stopping at step 1 would have left the intended beneficiaries on the very
/// `$HOME`-derived path this exists to avoid. The state candidate preserves the library's first
/// fallback, while the shorter home alternative remains available when its `.local/state` path
/// would be unusable. The launcher creates the socket directory through `tempfile`, which makes it
/// mode 0700, so a shared parent is still private. Candidates inside a canonical local operand are
/// deliberately excluded: an unreapable SSH master requires its private directory to survive until
/// process exit, so that directory must not appear inside a copied tree. Remote-only operands impose
/// no such exclusion, regardless of the master's working directory. The filesystem root is also
/// omitted because no absolute socket candidate could otherwise be selected.
///
/// Returns `None` only when none of the candidates can safely hold a control socket; callers then
/// fail before launching SSH rather than selecting a path known to be broken.
fn ssh_control_directory(excluded_roots: &[std::path::PathBuf]) -> Option<std::path::PathBuf> {
    select_ssh_control_directory_from_environment(
        std::env::var_os("XDG_RUNTIME_DIR").map(Into::into),
        std::env::temp_dir(),
        std::path::PathBuf::from("/tmp"),
        std::env::var_os("XDG_STATE_HOME").map(Into::into),
        std::env::var_os("HOME").map(Into::into),
        excluded_roots,
    )
}

fn select_ssh_control_directory_from_environment(
    runtime_dir: Option<std::path::PathBuf>,
    temp_dir: std::path::PathBuf,
    system_temp_dir: std::path::PathBuf,
    state_dir: Option<std::path::PathBuf>,
    home: Option<std::path::PathBuf>,
    excluded_roots: &[std::path::PathBuf],
) -> Option<std::path::PathBuf> {
    let mut candidates = Vec::with_capacity(5);
    candidates.extend(runtime_dir);
    candidates.push(temp_dir);
    candidates.push(system_temp_dir);
    candidates.extend(state_dir);
    candidates.extend(home);
    let excluded_roots: Vec<_> = excluded_roots
        .iter()
        .filter_map(|path| {
            std::fs::canonicalize(path)
                .ok()
                .or_else(|| std::path::absolute(path).ok())
        })
        .filter(|path| path.parent().is_some())
        .collect();
    select_ssh_control_directory(candidates.into_iter().filter(|candidate| {
        excluded_roots.is_empty()
            || std::fs::canonicalize(candidate).is_ok_and(|candidate| {
                excluded_roots
                    .iter()
                    .all(|excluded_root| !candidate.starts_with(excluded_root))
            })
    }))
}

fn select_ssh_control_directory(
    candidates: impl IntoIterator<Item = std::path::PathBuf>,
) -> Option<std::path::PathBuf> {
    candidates
        .into_iter()
        .find(|dir| control_dir_is_usable(dir))
}

/// Longest socket path `sockaddr_un` can hold, counting the terminating NUL.
const SUN_PATH_MAX: usize = 108;

/// What gets appended to the control directory before the socket finally exists:
///
/// - `/.ssh-connectionXXXXXX` (22) -- the private dir our launcher makes via `tempfile`
/// - `/master` (7) -- the `ControlPath` itself
/// - `.XXXXXXXXXXXXXXXX` (17) -- the temporary name ssh(1) creates it under before renaming
const CONTROL_PATH_SUFFIX: usize = 22 + 7 + 17;

/// Whether `dir` can actually host the control socket: short enough to leave room for everything
/// appended to it, and writable by us.
///
/// Both halves matter. Length is the original bug. Writability is what makes the candidate list
/// above mean anything -- a directory that merely *exists* is not enough, and `$XDG_RUNTIME_DIR`
/// surviving into a `su`/`sudo -u` session while pointing at the original user's `/run/user/<uid>`
/// is a common way to get one that is present but unusable. Without this check we would select it
/// confidently and fail, instead of moving on to `/tmp`.
fn control_dir_is_usable(dir: &std::path::Path) -> bool {
    if dir.as_os_str().is_empty() || !dir.is_dir() {
        return false;
    }
    if dir.as_os_str().len() + CONTROL_PATH_SUFFIX >= SUN_PATH_MAX {
        tracing::debug!(
            "skipping SSH control directory {}: too long to hold the socket path",
            dir.display()
        );
        return false;
    }
    // probe rather than inspect the mode: ownership, ACLs and read-only mounts all decide this,
    // and creating a directory is exactly what `openssh` is about to do anyway.
    match tempfile::Builder::new()
        .prefix(".rcp-control-probe-")
        .tempdir_in(dir)
    {
        Ok(probe) => {
            drop(probe);
            true
        }
        Err(error) => {
            tracing::debug!(
                "skipping SSH control directory {}: not writable: {:#}",
                dir.display(),
                &error
            );
            false
        }
    }
}

/// Retrieve a remote home directory with a caller-selected bootstrap timeout.
#[instrument(skip(cleanup, local_operand_roots))]
pub async fn get_remote_home_for_session_with_timeout(
    session: &SshSession,
    cleanup: &RemoteCleanup,
    bootstrap_timeout: std::time::Duration,
    local_operand_roots: &[std::path::PathBuf],
) -> anyhow::Result<std::path::PathBuf> {
    let preparation = PreparationContext::uncancelled(cleanup.clone())
        .with_control_directory_exclusions(local_operand_roots.to_vec().into());
    let deadline = BootstrapDeadline::new(bootstrap_timeout);
    let ssh_session = setup_ssh_session(session, &preparation, deadline).await?;
    let home = get_remote_home_with_context(&ssh_session, &preparation, deadline).await?;
    Ok(std::path::PathBuf::from(home))
}

#[instrument(skip(process))]
pub async fn wait_for_rcpd_process(mut process: RcpdProcess) -> anyhow::Result<()> {
    let child = process
        .child
        .take()
        .expect("an owned rcpd process retains its child until wait");
    let stderr_drain = process
        .stderr_drain
        .take()
        .expect("an owned rcpd process retains its stderr collector until wait");
    let stdout_drain = process.stdout_drain.take();
    tracing::info!("Waiting on rcpd server on: {:?}", child);
    // closing the child's stdin is the daemon watchdog signal. Always join the output collectors,
    // even when waiting fails, so teardown neither detaches tasks nor loses bounded diagnostics.
    let wait = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait()).await;
    let (stderr, stdout) = tokio::join!(
        finish_rcpd_output_drain(stderr_drain, "stderr"),
        finish_optional_rcpd_output_drain(stdout_drain, "stdout"),
    );
    let status = wait
        .context("Timeout waiting for rcpd process to exit")?
        .context("Failed to wait for rcpd process")?;
    if !status.success() {
        return Err(anyhow!(
            "rcpd command failed on remote host, status code: {:?}\nstdout:\n{}\nstderr:\n{}",
            status.code(),
            stdout.rendered(),
            stderr.rendered()
        ));
    }
    for (stream, output) in [("stdout", stdout), ("stderr", stderr)] {
        if let Some(error) = output.read_error {
            tracing::debug!("rcpd {stream} collector failed after startup: {error:#}");
        }
        if output.truncated {
            tracing::debug!("rcpd {stream} diagnostic tail was truncated after live forwarding");
        }
    }
    Ok(())
}

async fn finish_rcpd_output_drain(
    task: AbortOnDropTask<CapturedOutput>,
    stream: &'static str,
) -> CapturedOutput {
    match tokio::time::timeout(RCPD_OUTPUT_DRAIN_GRACE, task.join()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            tracing::debug!("rcpd {stream} collector task failed: {error:#}");
            CapturedOutput::default()
        }
        Err(_) => {
            tracing::debug!("rcpd {stream} collector did not finish during drain grace");
            CapturedOutput::default()
        }
    }
}

async fn finish_optional_rcpd_output_drain(
    task: Option<AbortOnDropTask<CapturedOutput>>,
    stream: &'static str,
) -> CapturedOutput {
    match task {
        Some(task) => finish_rcpd_output_drain(task, stream).await,
        None => CapturedOutput::default(),
    }
}

/// Escape a string for safe use in POSIX shell single quotes
///
/// Wraps the string in single quotes and escapes any single quotes within
pub(crate) fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// Marker error used when a remote host has no usable HOME directory.
#[derive(Debug)]
struct RemoteHomeUnavailable(String);

impl std::fmt::Display for RemoteHomeUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RemoteHomeUnavailable {}

/// Validate and retrieve the HOME directory on a remote host.
///
/// The lookup rejects an unset or empty HOME so callers cannot accidentally construct paths such
/// as `/.cache/rcp/bin/rcpd-{version}`.
///
/// # Errors
///
/// Returns an error when the lookup fails or HOME is unset or empty.
async fn get_remote_home_with_context<S: SshSessionOwner>(
    session: &S,
    preparation: &PreparationContext,
    deadline: BootstrapDeadline,
) -> anyhow::Result<String> {
    if let Ok(home_override) = std::env::var("RCP_REMOTE_HOME_OVERRIDE")
        && !home_override.is_empty()
    {
        return Ok(home_override);
    }
    let mut command = openssh::Session::to_command(session.clone(), "sh");
    command.arg("-c").arg("echo \"${HOME:?HOME not set}\"");
    run_remote_home_probe(command, preparation, deadline).await
}

async fn run_remote_home_probe<S: SshSessionOwner>(
    command: openssh::OwningCommand<S>,
    preparation: &PreparationContext,
    deadline: BootstrapDeadline,
) -> anyhow::Result<String> {
    let output = preparation
        .remote_output(command, "remote HOME lookup", deadline)
        .await
        .context("failed to check HOME environment variable on remote host")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::Error::new(RemoteHomeUnavailable(format!(
            "HOME environment variable is not set on remote host\n\
            \n\
            stderr: {}\n\
            \n\
            The HOME environment variable is required for remote ~ expansion and rcpd deployment.\n\
            Please ensure your SSH configuration preserves environment variables.",
            stderr
        ))));
    }

    let home = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if home.is_empty() {
        return Err(anyhow::Error::new(RemoteHomeUnavailable(
            "HOME environment variable is empty on remote host\n\
            \n\
            The HOME environment variable is required for remote ~ expansion and rcpd deployment.\n\
            Please ensure your SSH configuration sets HOME correctly."
                .to_string(),
        )));
    }

    Ok(home)
}

#[cfg(test)]
mod shell_escape_tests {
    use super::*;

    #[test]
    fn test_shell_escape_simple() {
        assert_eq!(shell_escape("simple"), "'simple'");
    }

    #[test]
    fn test_shell_escape_with_spaces() {
        assert_eq!(shell_escape("path with spaces"), "'path with spaces'");
    }

    #[test]
    fn test_shell_escape_with_single_quote() {
        // single quote becomes: close quote, escaped quote, open quote
        assert_eq!(
            shell_escape("path'with'quotes"),
            r"'path'\''with'\''quotes'"
        );
    }

    #[test]
    fn test_shell_escape_injection_attempt() {
        // attempt to inject command
        assert_eq!(shell_escape("foo; rm -rf /"), "'foo; rm -rf /'");
        // the semicolon is now safely quoted and won't execute
    }

    #[test]
    fn test_shell_escape_special_chars() {
        assert_eq!(shell_escape("$PATH && echo pwned"), "'$PATH && echo pwned'");
        // special chars are safely quoted
    }
}

trait DiscoverySession {
    fn test_executable<'a>(
        &'a self,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'a>>;
    fn find_in_path<'a>(
        &'a self,
        binary: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'a>,
    >;
    fn remote_home<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'a>,
    >;
}

struct RealDiscoverySession<'a, S> {
    session: &'a S,
    preparation: &'a PreparationContext,
    deadline: BootstrapDeadline,
}

impl<S: SshSessionOwner> DiscoverySession for RealDiscoverySession<'_, S> {
    fn test_executable<'b>(
        &'b self,
        path: &'b str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'b>>
    {
        Box::pin(async move {
            let mut command = openssh::Session::to_command(self.session.clone(), "sh");
            command
                .arg("-c")
                .arg(format!("test -x {}", shell_escape(path)));
            let output = self
                .preparation
                .remote_output(command, "remote executable discovery", self.deadline)
                .await?;
            Ok(output.status.success())
        })
    }
    fn find_in_path<'b>(
        &'b self,
        binary: &'b str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let mut command = openssh::Session::to_command(self.session.clone(), "sh");
            command
                .arg("-c")
                .arg(RCPD_PATH_DISCOVERY_SCRIPT)
                .arg("rcp-path-discovery")
                .arg(binary);
            let output = self
                .preparation
                .remote_output(command, "remote PATH discovery", self.deadline)
                .await?;
            if output.status.success() {
                let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !path.is_empty() {
                    return Ok(Some(path));
                }
            }
            Ok(None)
        })
    }
    fn remote_home<'b>(
        &'b self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'b>,
    > {
        Box::pin(async move {
            match get_remote_home_with_context(self.session, self.preparation, self.deadline).await
            {
                Ok(home) => Ok(Some(home)),
                Err(error) if error.downcast_ref::<RemoteHomeUnavailable>().is_some() => Ok(None),
                Err(error) => Err(error),
            }
        })
    }
}

/// Discover rcpd binary on remote host
///
/// Searches in the following order:
/// 1. Explicit path (if provided)
/// 2. Same directory as local rcp binary
/// 3. PATH (via the shell's `command -v rcpd`)
/// 4. Deployed cache directory (~/.cache/rcp/bin/rcpd-{version})
///
/// The cache is checked last as it contains auto-deployed binaries and should
/// only be used as a fallback after checking user-installed locations.
///
/// Returns the path to rcpd if found, otherwise an error
async fn discover_rcpd_path<S: SshSessionOwner>(
    session: &S,
    explicit_path: Option<&str>,
    preparation: &PreparationContext,
    deadline: BootstrapDeadline,
) -> anyhow::Result<String> {
    let real_session = RealDiscoverySession {
        session,
        preparation,
        deadline,
    };
    discover_rcpd_path_internal(&real_session, explicit_path, None).await
}

async fn discover_rcpd_path_internal<S: DiscoverySession + ?Sized>(
    session: &S,
    explicit_path: Option<&str>,
    current_exe_override: Option<std::path::PathBuf>,
) -> anyhow::Result<String> {
    let local_version = common::version::ProtocolVersion::current();
    // try explicit path first
    if let Some(path) = explicit_path {
        tracing::debug!("Trying explicit rcpd path: {}", path);
        if session.test_executable(path).await? {
            tracing::info!("Found rcpd at explicit path: {}", path);
            return Ok(path.to_string());
        }
        // explicit path was provided but not found - return error immediately
        // don't fall back to other discovery methods
        return Err(anyhow::anyhow!(
            "rcpd binary not found or not executable at explicit path: {}",
            path
        ));
    }
    // try same directory as local rcp binary
    if let Ok(current_exe) = current_exe_override
        .map(Ok)
        .unwrap_or_else(std::env::current_exe)
        && let Some(bin_dir) = current_exe.parent()
    {
        let path = bin_dir.join("rcpd").display().to_string();
        tracing::debug!("Trying same directory as rcp: {}", path);
        if session.test_executable(&path).await? {
            tracing::info!("Found rcpd in same directory as rcp: {}", path);
            return Ok(path);
        }
    }
    // try PATH
    tracing::debug!("Trying to find rcpd in PATH");
    if let Some(path) = session.find_in_path("rcpd").await? {
        tracing::info!("Found rcpd in PATH: {}", path);
        return Ok(path);
    }
    // try deployed cache directory as last resort (reuse already-deployed binaries)
    // if HOME is not set, skip cache check
    let cache_path = match session.remote_home().await? {
        Some(home) => {
            let path = format!("{}/.cache/rcp/bin/rcpd-{}", home, local_version.cache_tag());
            tracing::debug!("Trying deployed cache path: {}", path);
            if session.test_executable(&path).await? {
                tracing::info!("Found rcpd in deployed cache: {}", path);
                return Ok(path);
            }
            Some(path)
        }
        None => {
            tracing::debug!("HOME not set on remote host, skipping cache directory check");
            None
        }
    };
    // build error message with what we searched
    let mut searched = vec![];
    searched.push("- Same directory as local rcp binary".to_string());
    searched.push("- PATH (via 'command -v rcpd')".to_string());
    if let Some(path) = cache_path.as_ref() {
        searched.push(format!("- Deployed cache: {}", path));
    } else {
        searched.push("- Deployed cache: (skipped, HOME not set)".to_string());
    }
    Err(anyhow::anyhow!(
        "rcpd binary not found on remote host\n\
        \n\
        Searched in:\n\
        {}\n\
        \n\
        Options:\n\
        - Use automatic deployment: rcp --auto-deploy-rcpd ...\n\
        - Install rcpd manually: cargo install rcp-tools-rcp --version {}\n\
        - Specify explicit path: rcp --rcpd-path=/path/to/rcpd ...",
        searched.join("\n"),
        local_version.crate_version()
    ))
}

/// Try to discover rcpd and check version compatibility
///
/// Combines discovery and version checking into one function for cleaner error handling.
/// Returns the path to a compatible rcpd if found, or an error describing the problem.
async fn try_discover_and_check_version<S: SshSessionOwner>(
    session: &S,
    explicit_path: Option<&str>,
    remote_host: &str,
    preparation: &PreparationContext,
    version_probe_timeout: std::time::Duration,
) -> anyhow::Result<String> {
    // discover rcpd binary on remote host
    let rcpd_path = discover_rcpd_path(
        session,
        explicit_path,
        preparation,
        BootstrapDeadline::new(version_probe_timeout),
    )
    .await?;
    // check version compatibility
    check_rcpd_version(
        session,
        &rcpd_path,
        remote_host,
        preparation,
        version_probe_timeout,
    )
    .await?;
    Ok(rcpd_path)
}

/// Check version compatibility between local rcp and remote rcpd
///
/// Returns Ok if versions are compatible, Err with detailed message if not
async fn check_rcpd_version<S: SshSessionOwner>(
    session: &S,
    rcpd_path: &str,
    remote_host: &str,
    preparation: &PreparationContext,
    version_probe_timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let local_version = common::version::ProtocolVersion::current();

    tracing::debug!("Checking rcpd version on remote host: {}", remote_host);

    // run rcpd --protocol-version on remote (call binary directly, no shell)
    let mut command = openssh::Session::to_command(session.clone(), rcpd_path.to_string());
    command.arg("--protocol-version");
    let operation = format!("rcpd --protocol-version probe on remote host '{remote_host}'");
    let output = preparation
        .remote_output(
            command,
            &operation,
            BootstrapDeadline::new(version_probe_timeout),
        )
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "rcpd --protocol-version failed on remote host '{}'\n\
            \n\
            stderr: {}\n\
            \n\
            This may indicate an old version of rcpd that does not support --protocol-version.\n\
            Please install a matching version of rcpd on the remote host:\n\
            - cargo install rcp-tools-rcp --version {}",
            remote_host,
            stderr,
            local_version.crate_version()
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let remote_version = common::version::ProtocolVersion::from_json(stdout.trim())
        .context("Failed to parse rcpd version JSON from remote host")?;

    tracing::info!(
        "Local version: {}, Remote version: {}",
        local_version,
        remote_version
    );

    if !local_version.is_compatible_with(&remote_version) {
        return Err(anyhow::anyhow!(
            "rcpd version mismatch\n\
            \n\
            Local:  rcp {}\n\
            Remote: rcpd {} on host '{}'\n\
            \n\
            The rcpd version on the remote host must exactly match the rcp version,\n\
            including the +w compatibility revision — two builds of the same release can\n\
            differ when the development remote protocol or rcpd spawn contract moved\n\
            between them (then\n\
            reinstall/redeploy the remote build rather than pinning a version).\n\
            \n\
            To fix this, install the matching version on the remote host:\n\
            - ssh {} 'cargo install rcp-tools-rcp --version {}'",
            local_version,
            remote_version,
            remote_host,
            shell_escape(remote_host),
            local_version.crate_version()
        ));
    }

    Ok(())
}

/// Connection info received from rcpd after it starts listening.
#[derive(Debug, Clone)]
pub struct RcpdConnectionInfo {
    /// Address rcpd is listening on
    pub addr: std::net::SocketAddr,
    /// TLS certificate fingerprint (None if encryption disabled)
    pub fingerprint: Option<tls::Fingerprint>,
    /// Logical file-work ceiling resolved by this daemon.
    pub files_in_flight: common::ConcurrencyLimit,
    /// Effective data-stream count resolved by this daemon.
    pub max_connections: std::num::NonZeroUsize,
}

/// Format the first stderr record emitted by a successfully started rcpd.
#[must_use]
pub fn format_rcpd_readiness(
    addr: std::net::SocketAddr,
    fingerprint: Option<&tls::Fingerprint>,
    concurrency: ResolvedRemoteConcurrency,
) -> String {
    let files_in_flight = match concurrency.files_in_flight() {
        common::ConcurrencyLimit::Unlimited => "unlimited".to_string(),
        common::ConcurrencyLimit::Limited(value) => value.to_string(),
    };
    match fingerprint {
        Some(fingerprint) => format!(
            "RCP_TLS {} {} {} {}",
            addr,
            tls::fingerprint_to_hex(fingerprint),
            files_in_flight,
            concurrency.max_connections(),
        ),
        None => format!(
            "RCP_TCP {} {} {}",
            addr,
            files_in_flight,
            concurrency.max_connections(),
        ),
    }
}

/// Parse the version-sensitive first stderr record emitted by rcpd.
fn parse_rcpd_readiness(line: &str) -> anyhow::Result<RcpdConnectionInfo> {
    fn parse_files_in_flight(token: &str) -> anyhow::Result<common::ConcurrencyLimit> {
        if token == "unlimited" {
            return Ok(common::ConcurrencyLimit::Unlimited);
        }
        let value = token
            .parse::<usize>()
            .with_context(|| format!("invalid file limit in rcpd readiness record: {token}"))?;
        let value = std::num::NonZeroUsize::new(value).with_context(|| {
            format!("invalid zero file limit in rcpd readiness record: {token}")
        })?;
        Ok(common::ConcurrencyLimit::Limited(value))
    }

    let (kind, rest) = line
        .split_once(' ')
        .context("rcpd readiness record is missing its fields")?;
    let parts: Vec<&str> = rest.split_whitespace().collect();
    let (addr, fingerprint, files_token, connections_token) =
        match (kind, parts.as_slice()) {
            ("RCP_TLS", [addr, fingerprint, files, connections]) => (
                addr,
                Some(tls::fingerprint_from_hex(fingerprint).with_context(|| {
                    format!("invalid fingerprint in RCP_TLS line: {fingerprint}")
                })?),
                files,
                connections,
            ),
            ("RCP_TCP", [addr, files, connections]) => (addr, None, files, connections),
            ("RCP_TLS", _) => anyhow::bail!("invalid RCP_TLS line from rcpd: {line}"),
            ("RCP_TCP", _) => anyhow::bail!("invalid RCP_TCP line from rcpd: {line}"),
            _ => anyhow::bail!("unexpected output from rcpd (expected RCP_TLS or RCP_TCP): {line}"),
        };
    let addr = addr
        .parse()
        .with_context(|| format!("invalid address in rcpd readiness record: {addr}"))?;
    let files_in_flight = parse_files_in_flight(files_token)?;
    let max_connections = connections_token.parse::<usize>().with_context(|| {
        format!("invalid effective stream count in rcpd readiness record: {connections_token}")
    })?;
    let max_connections = std::num::NonZeroUsize::new(max_connections).with_context(|| {
        format!("invalid zero stream count in rcpd readiness record: {connections_token}")
    })?;
    Ok(RcpdConnectionInfo {
        addr,
        fingerprint,
        files_in_flight,
        max_connections,
    })
}

#[derive(Debug)]
struct RcpdStartupRefusal(String);

impl std::fmt::Display for RcpdStartupRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RcpdStartupRefusal {}

fn parse_rcpd_startup_record(line: &str) -> anyhow::Result<RcpdConnectionInfo> {
    let refusal_token = RCPD_STARTUP_ERROR_PREFIX.trim_end();
    let diagnostic = if line == refusal_token {
        Some("")
    } else {
        line.strip_prefix(RCPD_STARTUP_ERROR_PREFIX)
    };
    if let Some(diagnostic) = diagnostic {
        let diagnostic = diagnostic.trim();
        if diagnostic.is_empty() {
            anyhow::bail!("rcpd refused startup without a diagnostic");
        }
        return Err(anyhow::Error::new(RcpdStartupRefusal(
            diagnostic.to_string(),
        )))
        .context("rcpd refused startup");
    }
    parse_rcpd_readiness(line)
}

async fn read_rcpd_startup_record<R>(reader: &mut R) -> anyhow::Result<RcpdConnectionInfo>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;

    let mut bytes = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .context("failed to read connection info from rcpd")?;
        if available.is_empty() {
            if bytes.is_empty() {
                anyhow::bail!("rcpd exited before writing a readiness record");
            }
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        if bytes.len().saturating_add(take) > RCPD_STARTUP_RECORD_LIMIT {
            anyhow::bail!(
                "rcpd readiness record exceeds the {}-byte limit",
                RCPD_STARTUP_RECORD_LIMIT
            );
        }
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            break;
        }
    }
    let line = String::from_utf8(bytes).context("rcpd readiness record is not valid UTF-8")?;
    let line = line.trim();
    tracing::debug!("rcpd connection line: {line}");
    parse_rcpd_startup_record(line)
}

/// Result of starting an rcpd process.
///
/// Dropping it closes the SSH child channel and aborts both owned output collectors.
pub struct RcpdProcess {
    /// SSH child process handle
    child: Option<openssh::Child<ManagedSshSession>>,
    /// Connection info (address and optional fingerprint)
    pub conn_info: RcpdConnectionInfo,
    /// Handle for the bounded stderr collector.
    stderr_drain: Option<AbortOnDropTask<CapturedOutput>>,
    /// Handle for the bounded stdout collector.
    stdout_drain: Option<AbortOnDropTask<CapturedOutput>>,
}

#[derive(Clone)]
pub struct PreparedRcpd {
    session: ManagedSshSession,
    rcpd_path: String,
    remote: SshSession,
}

/// Prepare two endpoints with a caller-selected remote bootstrap timeout.
pub async fn prepare_rcpd_endpoints_with_timeout(
    source: &SshSession,
    destination: &SshSession,
    explicit_rcpd_path: Option<&str>,
    auto_deploy_rcpd: bool,
    cleanup: &RemoteCleanup,
    bootstrap_timeout: std::time::Duration,
    local_operand_roots: &[std::path::PathBuf],
) -> anyhow::Result<(PreparedRcpd, PreparedRcpd)> {
    let control_directory_exclusions: std::sync::Arc<[std::path::PathBuf]> =
        local_operand_roots.to_vec().into();
    if source == destination {
        let prepared = prepare_rcpd_with_context(
            source,
            explicit_rcpd_path,
            auto_deploy_rcpd,
            PreparationContext::uncancelled(cleanup.clone())
                .with_control_directory_exclusions(control_directory_exclusions),
            bootstrap_timeout,
        )
        .await?;
        return Ok((prepared.clone(), prepared));
    }
    let source_control_directory_exclusions = control_directory_exclusions.clone();
    join_remote_preparations(
        cleanup,
        move |preparation| {
            prepare_rcpd_with_context(
                source,
                explicit_rcpd_path,
                auto_deploy_rcpd,
                preparation.with_control_directory_exclusions(source_control_directory_exclusions),
                bootstrap_timeout,
            )
        },
        move |preparation| {
            prepare_rcpd_with_context(
                destination,
                explicit_rcpd_path,
                auto_deploy_rcpd,
                preparation.with_control_directory_exclusions(control_directory_exclusions),
                bootstrap_timeout,
            )
        },
    )
    .await
}

/// Await both preparations after the first intrinsic error cancels its peer.
///
/// Every production endpoint path receives a [`PreparationContext`], so awaiting both preserves
/// cleanup ownership without allowing an uncancellable peer to hide the first error indefinitely.
pub(crate) async fn join_remote_preparations<
    Source,
    Destination,
    SourceFuture,
    DestinationFuture,
    S,
    D,
>(
    cleanup: &RemoteCleanup,
    source: Source,
    destination: Destination,
) -> anyhow::Result<(S, D)>
where
    Source: FnOnce(PreparationContext) -> SourceFuture,
    Destination: FnOnce(PreparationContext) -> DestinationFuture,
    SourceFuture: std::future::Future<Output = anyhow::Result<S>>,
    DestinationFuture: std::future::Future<Output = anyhow::Result<D>>,
    S: Send + 'static,
    D: Send + 'static,
{
    const SOURCE: u8 = 1;
    const DESTINATION: u8 = 2;

    fn take_error_and_defer_success<T: Send + 'static>(
        cleanup: &RemoteCleanup,
        result: anyhow::Result<T>,
    ) -> Option<anyhow::Error> {
        match result {
            Ok(value) => {
                // successful preparation values are generic and may own blocking destructors; keep
                // their disposal off the endpoint coordinator and independent of Tokio shutdown
                cleanup.defer_drop(value, "rcp-preparation-dispose");
                None
            }
            Err(error) => Some(error),
        }
    }

    async fn prepare<Prepare, PrepareFuture, Prepared>(
        prepare: Prepare,
        preparation: PreparationContext,
        first_failure: std::sync::Arc<std::sync::atomic::AtomicU8>,
        endpoint: u8,
    ) -> anyhow::Result<Prepared>
    where
        Prepare: FnOnce(PreparationContext) -> PrepareFuture,
        PrepareFuture: std::future::Future<Output = anyhow::Result<Prepared>>,
    {
        let cancellation = preparation.cancellation.clone();
        let result = prepare(preparation).await;
        if result.is_err()
            && first_failure
                .compare_exchange(
                    0,
                    endpoint,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
        {
            cancellation.cancel();
        }
        result
    }

    let cancellation = tokio_util::sync::CancellationToken::new();
    let first_failure = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    let source_preparation = prepare(
        source,
        PreparationContext::new(cancellation.clone(), cleanup.clone()),
        first_failure.clone(),
        SOURCE,
    );
    let destination_preparation = prepare(
        destination,
        PreparationContext::new(cancellation, cleanup.clone()),
        first_failure.clone(),
        DESTINATION,
    );
    let (source, destination) = tokio::join!(source_preparation, destination_preparation);
    match (source, destination) {
        (Ok(source), Ok(destination)) => Ok((source, destination)),
        (source, destination) => {
            let source_error = take_error_and_defer_success(cleanup, source);
            let destination_error = take_error_and_defer_success(cleanup, destination);
            match (
                first_failure.load(std::sync::atomic::Ordering::SeqCst),
                source_error,
                destination_error,
            ) {
                (SOURCE, Some(error), _) | (DESTINATION, _, Some(error)) => Err(error),
                (_, Some(error), _) | (_, _, Some(error)) => Err(error),
                (_, None, None) => unreachable!("an error outcome must retain at least one error"),
            }
        }
    }
}

#[instrument(skip(preparation))]
async fn prepare_rcpd_with_context(
    session: &SshSession,
    explicit_rcpd_path: Option<&str>,
    auto_deploy_rcpd: bool,
    preparation: PreparationContext,
    bootstrap_timeout: std::time::Duration,
) -> anyhow::Result<PreparedRcpd> {
    tracing::info!("Preparing rcpd server on: {:?}", session);
    let remote_host = &session.host;
    let ssh_session = setup_ssh_session(
        session,
        &preparation,
        BootstrapDeadline::new(bootstrap_timeout),
    )
    .await?;
    let rcpd_path = match try_discover_and_check_version(
        &ssh_session,
        explicit_rcpd_path,
        remote_host,
        &preparation,
        bootstrap_timeout,
    )
    .await
    {
        Ok(path) => path,
        Err(error) => {
            preparation.ensure_active()?;
            if !auto_deploy_rcpd {
                return Err(error);
            }
            tracing::info!("rcpd unavailable or incompatible, attempting auto-deployment");
            let local_rcpd = deploy::find_local_rcpd_binary_with_context(&preparation)
                .await
                .context("failed to find local rcpd binary for deployment")?;
            tracing::info!("Found local rcpd binary at {}", local_rcpd.display());
            let local_version = common::version::ProtocolVersion::current();
            let deployed_path = deploy::deploy_rcpd_with_context(
                &ssh_session,
                &local_rcpd,
                &local_version.cache_tag(),
                remote_host,
                &preparation,
                bootstrap_timeout,
            )
            .await
            .context("failed to deploy rcpd to remote host")?;
            check_rcpd_version(
                &ssh_session,
                &deployed_path,
                remote_host,
                &preparation,
                bootstrap_timeout,
            )
            .await
            .with_context(|| {
                format!("deployed rcpd at {deployed_path} failed compatibility verification")
            })?;
            tracing::info!("Successfully deployed rcpd to {deployed_path}");
            match deploy::cleanup_old_versions_with_context(
                &ssh_session,
                3,
                &preparation,
                bootstrap_timeout,
            )
            .await
            {
                Ok(()) => {}
                Err(_) if preparation.cancellation.is_cancelled() => {
                    return Err(PreparationContext::cancellation_error());
                }
                Err(error) => {
                    tracing::warn!("failed to cleanup old versions (non-fatal): {error:#}");
                }
            }
            deployed_path
        }
    };
    preparation.ensure_active()?;
    Ok(PreparedRcpd {
        session: ssh_session,
        rcpd_path,
        remote: session.clone(),
    })
}

impl PreparedRcpd {
    pub async fn spawn(
        &self,
        rcpd_config: &protocol::RcpdConfig,
        bind_ip: Option<&str>,
        role: protocol::RcpdRole,
    ) -> anyhow::Result<RcpdProcess> {
        tracing::info!("Starting prepared rcpd server on: {}", self.remote.host);
        let session = &self.remote;
        // build the exact remote argv once so execution and the stable diagnostic cannot drift
        let mut rcpd_args = vec!["--role".to_string(), role.to_string()];
        rcpd_args.extend(rcpd_config.to_args());
        if let Some(ip) = bind_ip {
            rcpd_args.push("--bind-ip".to_string());
            rcpd_args.push(ip.to_string());
        }
        tracing::info!(
            "Will run remote rcpd: path={:?} role={} args={:?}",
            self.rcpd_path,
            role,
            rcpd_args
        );
        let mut cmd = openssh::Session::to_command(self.session.clone(), &self.rcpd_path);
        cmd.args(rcpd_args);
        // configure stdin/stdout/stderr
        // stdin must be piped so rcpd can monitor it for master disconnection (stdin watchdog)
        cmd.stdin(openssh::Stdio::piped());
        cmd.stdout(openssh::Stdio::piped());
        cmd.stderr(openssh::Stdio::piped());
        let startup_deadline = BootstrapDeadline::new(std::time::Duration::from_secs(
            rcpd_config.remote_copy_conn_timeout_sec,
        ));
        let spawn =
            tokio::spawn(async move { cmd.spawn().await.context("Failed to spawn rcpd command") });
        let mut child = PreparationContext::uncancelled(self.session.cleanup())
            .run_abortable_with_deadline(spawn, "spawning rcpd command", startup_deadline)
            .await?;
        // read connection info from rcpd's stderr
        // (rcpd uses stderr for the protocol line because stdout is reserved for logs per convention;
        // rcpd doesn't display progress bars locally - it sends progress data over the network)
        // format: "RCP_TLS <addr> <fingerprint> <F> <E>" or "RCP_TCP <addr> <F> <E>"
        let Some(stderr) = child.stderr().take() else {
            let error = anyhow::anyhow!("rcpd stderr not available");
            let diagnostic = reap_failed_rcpd(child, &session.host, None).await;
            return Err(attach_startup_diagnostic(error, diagnostic));
        };
        let mut stderr_reader = tokio::io::BufReader::new(stderr);
        let startup = startup_deadline
            .run(
                "waiting for rcpd readiness record",
                read_rcpd_startup_record(&mut stderr_reader),
            )
            .await;
        let conn_info = match startup {
            Ok(conn_info) => conn_info,
            Err(error) => {
                let diagnostic = reap_failed_rcpd(child, &session.host, Some(stderr_reader)).await;
                return Err(attach_startup_diagnostic(error, diagnostic));
            }
        };
        // drain both streams concurrently so the daemon cannot block on a full pipe; retain only a
        // bounded tail for completion diagnostics
        let stderr_drain = AbortOnDropTask::new(tokio::spawn(drain_rcpd_output(
            stderr_reader,
            session.host.clone(),
            "stderr",
        )));
        let stdout_drain = child.stdout().take().map(|stdout| {
            AbortOnDropTask::new(tokio::spawn(drain_rcpd_output(
                stdout,
                session.host.clone(),
                "stdout",
            )))
        });
        tracing::info!(
            "rcpd listening on {} (encryption={})",
            conn_info.addr,
            conn_info.fingerprint.is_some()
        );
        Ok(RcpdProcess {
            child: Some(child),
            conn_info,
            stderr_drain: Some(stderr_drain),
            stdout_drain,
        })
    }
}

fn attach_startup_diagnostic(error: anyhow::Error, diagnostic: Option<String>) -> anyhow::Error {
    match diagnostic {
        Some(diagnostic) => error.context(format!("rcpd startup output: {diagnostic}")),
        None => error,
    }
}

async fn reap_failed_rcpd(
    mut child: openssh::Child<ManagedSshSession>,
    host: &str,
    stderr_reader: Option<tokio::io::BufReader<openssh::ChildStderr>>,
) -> Option<String> {
    let stdout_reader = child.stdout().take();
    let reaper = async move {
        let stdout = async move {
            match stdout_reader {
                Some(stdout) => drain_bounded_output(stdout, DIAGNOSTIC_CAPTURE_LIMIT).await,
                None => CapturedOutput::default(),
            }
        };
        let stderr = async move {
            match stderr_reader {
                Some(stderr) => drain_bounded_output(stderr, DIAGNOSTIC_CAPTURE_LIMIT).await,
                None => CapturedOutput::default(),
            }
        };
        let (status, stdout, stderr) = tokio::join!(child.wait(), stdout, stderr);
        anyhow::Ok((status?, stdout, stderr))
    };
    match tokio::time::timeout(FAILED_RCPD_REAP_GRACE, reaper).await {
        Ok(Ok((status, stdout, stderr))) => {
            tracing::debug!(
                host,
                status = ?status.code(),
                "reaped rcpd after failed startup"
            );
            format_failed_rcpd_output(stdout.rendered().as_bytes(), &stderr.rendered())
        }
        Ok(Err(error)) => {
            tracing::debug!(host, "failed to reap rcpd after startup error: {error:#}");
            None
        }
        Err(_) => {
            tracing::debug!(
                host,
                "rcpd did not exit within the startup cleanup grace; released its SSH child and output pipes"
            );
            None
        }
    }
}

fn format_failed_rcpd_output(stdout: &[u8], stderr: &str) -> Option<String> {
    let stdout = String::from_utf8_lossy(stdout);
    let stdout = common::format_startup_diagnostic(None, stdout.trim());
    let stderr = common::format_startup_diagnostic(None, stderr.trim());
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => None,
        (false, true) => Some(format!("stdout: {stdout}")),
        (true, false) => Some(format!("stderr: {stderr}")),
        (false, false) => Some(format!("stdout: {stdout}; stderr: {stderr}")),
    }
}

// ============================================================================
// ip address detection
// ============================================================================

fn get_local_ip(explicit_bind_ip: Option<&str>) -> anyhow::Result<std::net::IpAddr> {
    // if explicit IP provided, validate and use it
    if let Some(ip_str) = explicit_bind_ip {
        let ip = ip_str
            .parse::<std::net::IpAddr>()
            .with_context(|| format!("invalid IP address: {}", ip_str))?;
        match ip {
            std::net::IpAddr::V4(ipv4) => {
                tracing::debug!("using explicit bind IP: {}", ipv4);
                return Ok(std::net::IpAddr::V4(ipv4));
            }
            std::net::IpAddr::V6(_) => {
                anyhow::bail!(
                    "IPv6 address not supported for binding (got {}). \
                     TCP endpoints bind to 0.0.0.0 (IPv4 only)",
                    ip
                );
            }
        }
    }
    // auto-detection: try kernel routing first
    if let Some(ipv4) = try_ipv4_via_kernel_routing()? {
        return Ok(std::net::IpAddr::V4(ipv4));
    }
    // fallback to interface enumeration
    tracing::debug!("routing-based detection failed, falling back to interface enumeration");
    let interfaces = collect_ipv4_interfaces().context("Failed to enumerate network interfaces")?;
    if let Some(ipv4) = choose_best_ipv4(&interfaces) {
        tracing::debug!("using IPv4 address from interface scan: {}", ipv4);
        return Ok(std::net::IpAddr::V4(ipv4));
    }
    anyhow::bail!("No IPv4 interfaces found (TCP endpoints require IPv4 as they bind to 0.0.0.0)")
}

fn try_ipv4_via_kernel_routing() -> anyhow::Result<Option<std::net::Ipv4Addr>> {
    // strategy: ask the kernel which interface it would use by connecting to RFC1918 targets.
    // these addresses never leave the local network but still exercise the routing table.
    let private_ips = ["10.0.0.1:80", "172.16.0.1:80", "192.168.1.1:80"];
    for addr_str in &private_ips {
        let addr = addr_str
            .parse::<std::net::SocketAddr>()
            .expect("hardcoded socket addresses are valid");
        let socket = match std::net::UdpSocket::bind("0.0.0.0:0") {
            Ok(socket) => socket,
            Err(err) => {
                tracing::debug!(?err, "failed to bind UDP socket for routing detection");
                continue;
            }
        };
        if let Err(err) = socket.connect(addr) {
            tracing::debug!(?err, "connect() failed for routing target {}", addr);
            continue;
        }
        match socket.local_addr() {
            Ok(std::net::SocketAddr::V4(local_addr)) => {
                let ipv4 = *local_addr.ip();
                if !ipv4.is_loopback() && !ipv4.is_unspecified() {
                    tracing::debug!(
                        "using IPv4 address from kernel routing (via {}): {}",
                        addr,
                        ipv4
                    );
                    return Ok(Some(ipv4));
                }
            }
            Ok(_) => {
                tracing::debug!("kernel routing returned IPv6 despite IPv4 bind, ignoring");
            }
            Err(err) => {
                tracing::debug!(?err, "local_addr() failed for routing-based detection");
            }
        }
    }
    Ok(None)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InterfaceIpv4 {
    name: String,
    addr: std::net::Ipv4Addr,
}

fn collect_ipv4_interfaces() -> anyhow::Result<Vec<InterfaceIpv4>> {
    use if_addrs::get_if_addrs;
    let mut interfaces = Vec::new();
    for iface in get_if_addrs()? {
        if let std::net::IpAddr::V4(ipv4) = iface.addr.ip() {
            interfaces.push(InterfaceIpv4 {
                name: iface.name,
                addr: ipv4,
            });
        }
    }
    Ok(interfaces)
}

fn choose_best_ipv4(interfaces: &[InterfaceIpv4]) -> Option<std::net::Ipv4Addr> {
    interfaces
        .iter()
        .filter(|iface| !iface.addr.is_unspecified())
        .min_by_key(|iface| interface_priority(&iface.name, &iface.addr))
        .map(|iface| iface.addr)
}

fn interface_priority(
    name: &str,
    addr: &std::net::Ipv4Addr,
) -> (InterfaceCategory, u8, u8, std::net::Ipv4Addr) {
    (
        classify_interface(name, addr),
        if addr.is_link_local() { 1 } else { 0 },
        if addr.is_private() { 1 } else { 0 },
        *addr,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum InterfaceCategory {
    Preferred = 0,
    Normal = 1,
    Virtual = 2,
    Loopback = 3,
}

fn classify_interface(name: &str, addr: &std::net::Ipv4Addr) -> InterfaceCategory {
    if addr.is_loopback() {
        return InterfaceCategory::Loopback;
    }
    let normalized = normalize_interface_name(name);
    if is_virtual_interface(&normalized) {
        return InterfaceCategory::Virtual;
    }
    if is_preferred_physical_interface(&normalized) {
        return InterfaceCategory::Preferred;
    }
    InterfaceCategory::Normal
}

fn normalize_interface_name(original: &str) -> String {
    let mut normalized = String::with_capacity(original.len());
    for ch in original.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        }
    }
    normalized
}

fn is_virtual_interface(name: &str) -> bool {
    const VIRTUAL_PREFIXES: &[&str] = &[
        "br",
        "docker",
        "veth",
        "virbr",
        "vmnet",
        "wg",
        "tailscale",
        "zt",
        "zerotier",
        "tap",
        "tun",
        "utun",
        "ham",
        "vpn",
        "lo",
        "lxc",
    ];
    VIRTUAL_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
        || name.contains("virtual")
}

fn is_preferred_physical_interface(name: &str) -> bool {
    const PHYSICAL_PREFIXES: &[&str] = &[
        "en", "eth", "em", "eno", "ens", "enp", "wl", "ww", "wlan", "ethernet", "lan", "wifi",
    ];
    PHYSICAL_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

/// Generate a random server name for identifying connections
#[instrument]
pub fn get_random_server_name() -> String {
    rand::random_iter::<u8>()
        .filter(|b| b.is_ascii_alphanumeric())
        .take(20)
        .map(char::from)
        .collect()
}

// ============================================================================
// tcp server and client functions
// ============================================================================

/// Create a TCP listener for control connections
///
/// Returns a listener bound to the specified port range (or any available port).
#[instrument(skip(config))]
pub async fn create_tcp_control_listener(
    config: &TcpConfig,
    bind_ip: Option<&str>,
) -> anyhow::Result<tokio::net::TcpListener> {
    let bind_addr = if let Some(ip_str) = bind_ip {
        let ip = ip_str
            .parse::<std::net::IpAddr>()
            .with_context(|| format!("invalid IP address: {}", ip_str))?;
        std::net::SocketAddr::new(ip, 0)
    } else {
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
    };
    let listener = if let Some(ranges_str) = config.port_ranges.as_deref() {
        let ranges = port_ranges::PortRanges::parse(ranges_str)?;
        ranges.bind_tcp_listener(bind_addr.ip()).await?
    } else {
        tokio::net::TcpListener::bind(bind_addr).await?
    };
    let local_addr = listener.local_addr()?;
    tracing::info!("TCP control listener bound to {}", local_addr);
    Ok(listener)
}

/// Create a TCP listener for data connections (file transfers)
///
/// Returns a listener bound to the specified port range (or any available port).
#[instrument(skip(config))]
pub async fn create_tcp_data_listener(
    config: &TcpConfig,
    bind_ip: Option<&str>,
) -> anyhow::Result<tokio::net::TcpListener> {
    let bind_addr = if let Some(ip_str) = bind_ip {
        let ip = ip_str
            .parse::<std::net::IpAddr>()
            .with_context(|| format!("invalid IP address: {}", ip_str))?;
        std::net::SocketAddr::new(ip, 0)
    } else {
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
    };
    let listener = if let Some(ranges_str) = config.port_ranges.as_deref() {
        let ranges = port_ranges::PortRanges::parse(ranges_str)?;
        ranges.bind_tcp_listener(bind_addr.ip()).await?
    } else {
        tokio::net::TcpListener::bind(bind_addr).await?
    };
    let local_addr = listener.local_addr()?;
    tracing::info!("TCP data listener bound to {}", local_addr);
    Ok(listener)
}

/// Get the external address of a TCP listener
///
/// Similar to `get_endpoint_addr_with_bind_ip`, replaces 0.0.0.0 with the local IP.
pub fn get_tcp_listener_addr(
    listener: &tokio::net::TcpListener,
    bind_ip: Option<&str>,
) -> anyhow::Result<std::net::SocketAddr> {
    let local_addr = listener.local_addr()?;
    if local_addr.ip().is_unspecified() {
        let local_ip = get_local_ip(bind_ip).context("failed to get local IP address")?;
        Ok(std::net::SocketAddr::new(local_ip, local_addr.port()))
    } else {
        Ok(local_addr)
    }
}

/// Connect to a TCP control server, applying the connect timeout and the standard socket options
#[instrument(skip(config))]
pub async fn connect_tcp_control(
    addr: std::net::SocketAddr,
    config: &TcpConfig,
) -> anyhow::Result<tokio::net::TcpStream> {
    let timeout_sec = config.conn_timeout_sec;
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_sec),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .with_context(|| format!("connection to {} timed out after {}s", addr, timeout_sec))?
    .with_context(|| format!("failed to connect to {}", addr))?;
    configure_tcp_socket(
        &stream,
        config.network_profile,
        config.keepalive_sec,
        ConnectionKind::Control,
    );
    tracing::debug!("connected to TCP control server at {}", addr);
    Ok(stream)
}

/// Connect a TCP data connection, applying the standard socket options.
///
/// Deliberately NOT bounded here: the data pool bounds TCP connect + TLS handshake under ONE
/// caller-side deadline, and an inner timeout would double-count it. Takes the profile and
/// keepalive budget rather than a whole [`TcpConfig`] because that is what the pool carries.
///
/// These helpers exist so no connection can be established without its socket options:
/// `scripts/check-tcp-socket-config.sh` forbids raw `TcpStream::connect`/`.accept()` outside this
/// file, and inside it requires every connecting/accepting function to configure what it opened.
pub async fn connect_tcp_data(
    addr: std::net::SocketAddr,
    profile: NetworkProfile,
    keepalive_sec: u64,
) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    configure_tcp_socket(&stream, profile, keepalive_sec, ConnectionKind::Data);
    Ok(stream)
}

/// Accept one TCP control connection, applying the standard socket options before returning it.
///
/// The wait is bounded by the CALLER (each control accept awaits one specific peer under its own
/// timeout and error wording); the configuration lives here so no accepted control connection can
/// miss it. Cancel-safe exactly as `TcpListener::accept` is: the accept is the only await, and the
/// configuration runs synchronously in the same poll that completes it.
pub async fn accept_tcp_control(
    listener: &tokio::net::TcpListener,
    config: &TcpConfig,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    let (stream, addr) = listener.accept().await?;
    configure_tcp_socket(
        &stream,
        config.network_profile,
        config.keepalive_sec,
        ConnectionKind::Control,
    );
    Ok((stream, addr))
}

/// Accept one TCP data connection, applying the standard socket options before returning it.
///
/// Data connections get no `TCP_USER_TIMEOUT` — a throttled receiver legitimately stops reading
/// mid-file, and aborting that sender would fail a copy that used to just run slow (see
/// [`configure_tcp_socket`]). Unbounded like [`connect_tcp_data`]: the source's accept loop runs
/// for the life of the pool. Cancel-safe as `TcpListener::accept` is (see
/// [`accept_tcp_control`]), so it can sit in a `select!` arm.
pub async fn accept_tcp_data(
    listener: &tokio::net::TcpListener,
    profile: NetworkProfile,
    keepalive_sec: u64,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    let (stream, addr) = listener.accept().await?;
    configure_tcp_socket(&stream, profile, keepalive_sec, ConnectionKind::Data);
    Ok((stream, addr))
}

/// Number of unacknowledged keepalive probes tolerated before a connection is declared dead.
///
/// Decides the outcome only where `TCP_USER_TIMEOUT` is NOT set — on Linux the user timeout
/// overrides the probe count, so this is inert on [`ConnectionKind::Control`] and is what actually
/// ends a dead [`ConnectionKind::Data`] connection (after idle + retries × interval).
pub const TCP_KEEPALIVE_RETRIES: u32 = 6;

/// What an rcp TCP connection carries, which decides whether `TCP_USER_TIMEOUT` applies.
///
/// The user timeout aborts a connection whose data stays unacknowledged for the budget — and a
/// peer that has simply STOPPED READING is indistinguishable from a dead one at that level. It is
/// therefore only safe on a connection that application backpressure never legitimately blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionKind {
    /// Protocol messages: master↔rcpd (control and tracing) and source↔destination control.
    /// Reads are driven by a dispatch loop that does not block on transfer-sized work.
    Control,
    /// Pooled source→destination bulk file transfer, where the receiver legitimately stops
    /// reading for as long as its throttles make it wait.
    Data,
}

impl std::fmt::Display for ConnectionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control => write!(f, "control"),
            Self::Data => write!(f, "data"),
        }
    }
}

/// Configure an rcp TCP connection: no-delay, buffer sizes, and dead-peer detection.
///
/// This is the ONE place these options are set — call it at every site that establishes or accepts
/// an rcp TCP connection. They used to be hand-paired per connection (`set_nodelay` at 7 sites,
/// buffer sizing at 4, so two sites got no buffer sizing at all), which is the missed-exit-path
/// smell: a new option has to be copied into every site and eventually is not.
/// `scripts/check-tcp-socket-config.sh` enforces that no other site sets any of these options
/// itself, and that every file opening or accepting a TCP connection routes through here.
///
/// `keepalive_sec` is the budget for noticing that the peer's HOST has vanished (power loss,
/// severed link, destroyed VM). Such a peer sends neither FIN nor RST, so an await on it never
/// completes — the master waiting for `RcpdResult` hangs forever, and so do the control reads on
/// both rcpds. Two options cover different halves of that, and `kind` decides which apply:
///
/// - `SO_KEEPALIVE` (`TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT`), on EVERY connection, probes an
///   IDLE one — the awaiting-`RcpdResult` case, the control streams generally, and a data
///   connection between transfers.
/// - `TCP_USER_TIMEOUT`, on [`ConnectionKind::Control`] ONLY, bounds how long UNACKED data may
///   stay outstanding. Keepalive never fires while data is in flight, so without it the kernel
///   retransmits for roughly 15 minutes (`tcp_retries2`) before giving up.
///
/// **Why data connections do not get the user timeout.** It cannot tell a dead peer from a live
/// one that has stopped reading: with the receiver's window at zero and every zero-window probe
/// ACKed, the sender is still aborted once the budget expires. The destination does exactly that —
/// it awaits its per-file iops reservation after reading a file header and before reading any of
/// its bytes, so `--iops-throttle 50` on a 10 GiB file at 1 MiB chunks leaves that socket unread
/// for minutes. Under a shared budget the copy would FAIL where it used to merely run slow. The
/// price is stated plainly: a host that vanishes MID-TRANSFER is detected only by the kernel's
/// retransmission limit (~15 minutes), exactly as before this option existed — an idle data
/// connection is still caught by keepalive after idle + retries × interval.
///
/// Control connections are not backpressure-free in the strict sense — the destination's control
/// dispatch loop takes a single ops token per message, and directory COMPLETIONS run their
/// congestion-gated finalize syscalls (chown/chmod/ACL/utimens, chained bottom-up) on that same
/// loop — so a pathological `--ops-throttle` or badly stalled destination storage can stop the
/// reads. The budget only starts once the multi-megabyte receive buffer has filled and closed the
/// window on top of that. Known residual, not a claim of immunity; if it ever bites, the
/// structural fix is decoupling finalization from the receive loop, exactly as the manifest build
/// already was.
///
/// The keepalive sub-values are derived from the single budget rather than exposed individually, so
/// their relationship stays correct by construction: idle at half the budget, probes every twelfth
/// of it, [`TCP_KEEPALIVE_RETRIES`] of them. `keepalive_sec == 0` disables both mechanisms and
/// leaves only no-delay and buffer sizing.
///
/// Every option is best effort: a failure is logged and tolerated, because a socket option that a
/// platform or a container policy refuses is not a reason to fail a copy.
pub fn configure_tcp_socket(
    stream: &tokio::net::TcpStream,
    profile: NetworkProfile,
    keepalive_sec: u64,
    kind: ConnectionKind,
) {
    if let Err(err) = stream.set_nodelay(true) {
        tracing::warn!("failed to set TCP_NODELAY: {err:#}");
    }
    let (send_buf, recv_buf) = match profile {
        NetworkProfile::Datacenter => (16 * 1024 * 1024, 16 * 1024 * 1024),
        NetworkProfile::Internet => (2 * 1024 * 1024, 2 * 1024 * 1024),
    };
    let sock_ref = socket2::SockRef::from(stream);
    if let Err(err) = sock_ref.set_send_buffer_size(send_buf) {
        tracing::warn!("failed to set TCP send buffer size: {err:#}");
    }
    if let Err(err) = sock_ref.set_recv_buffer_size(recv_buf) {
        tracing::warn!("failed to set TCP receive buffer size: {err:#}");
    }
    if let (Ok(send), Ok(recv)) = (sock_ref.send_buffer_size(), sock_ref.recv_buffer_size()) {
        tracing::debug!(
            "TCP socket buffer sizes: send={} recv={}",
            bytesize::ByteSize(send as u64),
            bytesize::ByteSize(recv as u64),
        );
    }
    if keepalive_sec == 0 {
        tracing::debug!("TCP keepalive and user timeout disabled on {kind} connection");
        return;
    }
    // clamped to 1s: the kernel rejects a zero idle/interval, which a budget under 12s would
    // otherwise produce
    let idle_sec = std::cmp::max(keepalive_sec / 2, 1);
    let interval_sec = std::cmp::max(keepalive_sec / 12, 1);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(idle_sec))
        .with_interval(std::time::Duration::from_secs(interval_sec))
        .with_retries(TCP_KEEPALIVE_RETRIES);
    // each log reports what LANDED, not what was attempted: `set_tcp_keepalive` turns SO_KEEPALIVE
    // on and then bails on the first sub-option that fails, so a partial failure leaves keepalive
    // enabled at the system defaults (7200s idle) — a line claiming the configured values would be
    // actively misleading about how long a dead peer goes unnoticed
    match sock_ref.set_tcp_keepalive(&keepalive) {
        Ok(()) => tracing::debug!(
            "TCP keepalive on {kind} connection: idle {idle_sec}s, interval {interval_sec}s, retries {TCP_KEEPALIVE_RETRIES}"
        ),
        Err(err) => tracing::warn!(
            "failed to configure TCP keepalive on {kind} connection (it may be enabled at system defaults): {err:#}"
        ),
    }
    // tcp_user_timeout is Linux-only; elsewhere keepalive alone covers the idle case
    #[cfg(target_os = "linux")]
    if kind == ConnectionKind::Control {
        let user_timeout = std::time::Duration::from_secs(keepalive_sec);
        match sock_ref.set_tcp_user_timeout(Some(user_timeout)) {
            Ok(()) => {
                tracing::debug!("TCP_USER_TIMEOUT on control connection: {keepalive_sec}s")
            }
            Err(err) => tracing::warn!("failed to set TCP_USER_TIMEOUT: {err:#}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Condvar, Mutex};

    fn nonzero(value: usize) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(value).unwrap()
    }

    fn test_rcpd_config() -> protocol::RcpdConfig {
        protocol::RcpdConfig {
            verbose: 0,
            fail_early: false,
            max_workers: 0,
            max_blocking_threads: 0,
            files_in_flight: protocol::RcpdFilesInFlight::Explicit(
                common::ConcurrencyLimit::Limited(nonzero(4)),
            ),
            ops_throttle: 0,
            iops_throttle: 0,
            chunk_size: 0,
            auto_meta: None,
            auto_meta_histogram: false,
            auto_meta_histogram_log: None,
            auto_meta_histogram_interval: std::time::Duration::from_secs(1),
            dereference: false,
            require_toctou_safe: false,
            overwrite: false,
            overwrite_compare: "size,mtime".to_string(),
            overwrite_manifest_max_entries: protocol::DEFAULT_OVERWRITE_MANIFEST_MAX_ENTRIES,
            overwrite_filter: None,
            ignore_existing: false,
            skip_specials: false,
            debug_log_prefix: None,
            port_ranges: None,
            progress: false,
            progress_delay: None,
            remote_copy_conn_timeout_sec: 1,
            remote_keepalive_sec: DEFAULT_REMOTE_KEEPALIVE_SEC,
            network_profile: NetworkProfile::default(),
            buffer_size: None,
            max_connections: 4,
            pending_writes_multiplier: 1,
            chrome_trace_prefix: None,
            flamegraph_prefix: None,
            profile_level: None,
            tokio_console: false,
            tokio_console_port: None,
            encryption: false,
            master_cert_fingerprint: None,
        }
    }

    async fn prepare_test_rcpd(
        session: &SshSession,
        explicit_rcpd_path: Option<&str>,
        auto_deploy_rcpd: bool,
        cleanup: &RemoteCleanup,
    ) -> anyhow::Result<PreparedRcpd> {
        prepare_rcpd_with_context(
            session,
            explicit_rcpd_path,
            auto_deploy_rcpd,
            PreparationContext::uncancelled(cleanup.clone()),
            DEFAULT_REMOTE_BOOTSTRAP_TIMEOUT,
        )
        .await
    }

    #[cfg(target_os = "linux")]
    struct KillTestProcessOnDrop {
        pid: String,
        process: std::path::PathBuf,
        start_time: String,
    }

    #[cfg(target_os = "linux")]
    impl KillTestProcessOnDrop {
        fn new(pid: String) -> Self {
            let process = std::path::PathBuf::from(format!("/proc/{pid}"));
            let start_time = linux_process_start_time(&process)
                .expect("failed-rcpd test process must expose a stable identity");
            Self {
                pid,
                process,
                start_time,
            }
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for KillTestProcessOnDrop {
        fn drop(&mut self) {
            if linux_process_start_time(&self.process).as_ref() == Some(&self.start_time) {
                let _ = std::process::Command::new("kill")
                    .args(["-KILL", self.pid.as_str()])
                    .status();
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn spawn_failed_rcpd_that_never_exits(
        session: &ManagedSshSession,
        directory: &std::path::Path,
    ) -> (
        openssh::Child<ManagedSshSession>,
        tokio::io::BufReader<openssh::ChildStderr>,
        KillTestProcessOnDrop,
    ) {
        use std::os::unix::fs::PermissionsExt;
        use tokio::io::AsyncBufReadExt;

        let script = directory.join("failed-rcpd");
        let process_pid = directory.join("process-pid");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$$\" > {}\nprintf '%s\\n' 'not a readiness record' >&2\nexec sleep 30\n",
                shell_escape(process_pid.to_str().unwrap()),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut command = openssh::Session::to_command(session.clone(), script.to_str().unwrap());
        command
            .stdin(openssh::Stdio::piped())
            .stdout(openssh::Stdio::piped())
            .stderr(openssh::Stdio::piped());
        let mut child = command.spawn().await.unwrap();
        let stderr = child.stderr().take().unwrap();
        let mut stderr_reader = tokio::io::BufReader::new(stderr);
        let mut readiness = String::new();
        stderr_reader.read_line(&mut readiness).await.unwrap();
        assert_eq!(readiness, "not a readiness record\n");
        let pid = std::fs::read_to_string(process_pid).unwrap();
        let guard = KillTestProcessOnDrop::new(pid.trim().to_string());
        (child, stderr_reader, guard)
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_remote_failed_rcpd_reap_timeout_releases_child_session() {
        let directory = tempfile::tempdir().unwrap();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let session = setup_ssh_session(
            &SshSession::local(),
            &preparation,
            BootstrapDeadline::new(std::time::Duration::from_secs(5)),
        )
        .await
        .expect("localhost SSH must be available");
        let weak_session = std::sync::Arc::downgrade(&session.0);
        let (child, stderr_reader, _process) =
            spawn_failed_rcpd_that_never_exits(&session, directory.path()).await;

        let diagnostic = reap_failed_rcpd(child, "localhost", Some(stderr_reader)).await;
        assert!(diagnostic.is_none());
        drop(session);
        drop(preparation);

        assert!(
            weak_session.upgrade().is_none(),
            "an expired failed-rcpd reap must release its child and managed SSH session"
        );
        cleanup.finish();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_remote_cancelled_failed_rcpd_reap_releases_child_session() {
        let directory = tempfile::tempdir().unwrap();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let session = setup_ssh_session(
            &SshSession::local(),
            &preparation,
            BootstrapDeadline::new(std::time::Duration::from_secs(5)),
        )
        .await
        .expect("localhost SSH must be available");
        let weak_session = std::sync::Arc::downgrade(&session.0);
        let (child, stderr_reader, _process) =
            spawn_failed_rcpd_that_never_exits(&session, directory.path()).await;
        let (first_poll_tx, first_poll_rx) = tokio::sync::oneshot::channel();
        let reaper = tokio::spawn(async move {
            let mut reaper = Box::pin(reap_failed_rcpd(child, "localhost", Some(stderr_reader)));
            let mut first_poll_tx = Some(first_poll_tx);
            std::future::poll_fn(move |context| {
                let result = std::future::Future::poll(reaper.as_mut(), context);
                if let Some(first_poll_tx) = first_poll_tx.take() {
                    let _ = first_poll_tx.send(());
                }
                result
            })
            .await
        });
        first_poll_rx.await.unwrap();

        reaper.abort();
        let error = reaper.await.unwrap_err();
        assert!(error.is_cancelled());
        drop(session);
        drop(preparation);

        assert!(
            weak_session.upgrade().is_none(),
            "cancelling a failed-rcpd reap must release its child and managed SSH session"
        );
        cleanup.finish();
    }

    #[test]
    fn rejects_effective_streams_above_tokio_semaphore_capacity() {
        let too_many = tokio::sync::Semaphore::MAX_PERMITS.checked_add(1).unwrap();
        let error = resolve_remote_concurrency(
            common::ConcurrencyLimit::Unlimited,
            std::num::NonZeroUsize::new(too_many).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("Tokio semaphore maximum"));
    }

    #[test]
    fn rejects_nonoverflowing_pending_capacity_above_tokio_maximum() {
        let half_plus_one = tokio::sync::Semaphore::MAX_PERMITS / 2 + 1;
        let error = resolve_remote_concurrency(
            common::ConcurrencyLimit::Unlimited,
            std::num::NonZeroUsize::new(half_plus_one).unwrap(),
            std::num::NonZeroUsize::new(2).unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("pending file capacity"));
    }

    #[test]
    fn readiness_records_report_negotiated_file_and_stream_limits() {
        let fingerprint_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let cases = [
            (
                "RCP_TCP 127.0.0.1:1234 8 8".to_string(),
                common::ConcurrencyLimit::Limited(nonzero(8)),
                nonzero(8),
                None,
            ),
            (
                "RCP_TCP 127.0.0.1:1234 unlimited 100".to_string(),
                common::ConcurrencyLimit::Unlimited,
                nonzero(100),
                None,
            ),
            (
                format!("RCP_TLS 127.0.0.1:1234 {fingerprint_hex} 32 16"),
                common::ConcurrencyLimit::Limited(nonzero(32)),
                nonzero(16),
                Some(tls::fingerprint_from_hex(fingerprint_hex).unwrap()),
            ),
        ];
        for (record, expected_files, expected_streams, expected_fingerprint) in cases {
            let parsed = parse_rcpd_readiness(&record).unwrap();
            assert_eq!(parsed.addr, "127.0.0.1:1234".parse().unwrap());
            assert_eq!(parsed.fingerprint, expected_fingerprint);
            assert_eq!(parsed.files_in_flight, expected_files);
            assert_eq!(parsed.max_connections, expected_streams);
        }
    }

    #[test]
    fn readiness_records_reject_invalid_limit_tokens() {
        for record in [
            "RCP_TCP 127.0.0.1:1234 0 1",
            "RCP_TCP 127.0.0.1:1234 automatic 1",
            "RCP_TCP 127.0.0.1:1234 1 0",
        ] {
            assert!(
                parse_rcpd_readiness(record).is_err(),
                "invalid readiness record was accepted: {record}"
            );
        }
    }

    #[tokio::test]
    async fn empty_startup_refusal_reports_missing_diagnostic() {
        for record in ["RCP_ERROR\n", "RCP_ERROR \n"] {
            let mut reader = tokio::io::BufReader::new(record.as_bytes());
            let error = read_rcpd_startup_record(&mut reader)
                .await
                .expect_err("an empty startup refusal must fail");
            assert!(
                format!("{error:#}").contains("rcpd refused startup without a diagnostic"),
                "empty refusal was misclassified for {record:?}: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn startup_error_prefix_requires_a_token_boundary() {
        let mut reader = tokio::io::BufReader::new("RCP_ERRORfoo\n".as_bytes());
        let error = read_rcpd_startup_record(&mut reader)
            .await
            .expect_err("an unknown readiness token must fail");
        assert!(
            !format!("{error:#}").contains("rcpd refused startup"),
            "a prefix collision was treated as a typed refusal: {error:#}"
        );
    }

    #[tokio::test]
    async fn test_remote_command_output_uses_required_bootstrap_deadline() {
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let session = setup_ssh_session(
            &SshSession::local(),
            &preparation,
            BootstrapDeadline::new(std::time::Duration::from_secs(5)),
        )
        .await
        .expect("localhost SSH must be available");
        let mut command = openssh::Session::to_command(session, "sh");
        command.arg("-c").arg("sleep 30");
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            preparation.remote_output(
                command,
                "remote discovery probe",
                BootstrapDeadline::new(std::time::Duration::from_millis(100)),
            ),
        )
        .await
        .expect("the shared remote-command helper did not enforce its deadline");
        let error = result.expect_err("the hanging remote command unexpectedly completed");
        assert!(
            format!("{error:#}").contains("remote discovery probe timed out after 100ms"),
            "configured deadline missing from remote-command error: {error:#}"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[tokio::test]
    async fn test_remote_home_lookup_uses_required_bootstrap_deadline() {
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let session = setup_ssh_session(
            &SshSession::local(),
            &preparation,
            BootstrapDeadline::new(std::time::Duration::from_secs(5)),
        )
        .await
        .expect("localhost SSH must be available");
        let mut command = openssh::Session::to_command(session, "sh");
        command.arg("-c").arg("sleep 30");
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run_remote_home_probe(
                command,
                &preparation,
                BootstrapDeadline::new(std::time::Duration::from_millis(100)),
            ),
        )
        .await
        .expect("the HOME lookup ignored its configured deadline");
        let error = result.expect_err("the hanging HOME lookup unexpectedly completed");
        assert!(
            format!("{error:#}").contains("remote HOME lookup timed out after 100ms"),
            "configured deadline missing from HOME lookup error: {error:#}"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[tokio::test]
    async fn test_remote_cancelled_command_output_is_bounded() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::new(cancellation.clone(), cleanup.clone());
        let session = setup_ssh_session(
            &SshSession::local(),
            &preparation,
            BootstrapDeadline::new(std::time::Duration::from_secs(5)),
        )
        .await
        .expect("localhost SSH must be available");
        let mut command = openssh::Session::to_command(session, "sh");
        command.arg("-c").arg("sleep 30");
        let cancel = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            cancellation.cancel();
        });

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            preparation.remote_output(
                command,
                "remote cancellation probe",
                BootstrapDeadline::new(std::time::Duration::from_secs(30)),
            ),
        )
        .await
        .expect("peer cancellation did not bound the remote command");
        cancel.await.unwrap();
        drop(preparation);
        cleanup.finish();
        let error = result.expect_err("the cancelled remote command unexpectedly completed");
        assert!(
            format!("{error:#}")
                .contains("rcpd preparation cancelled because peer preparation failed"),
            "peer cancellation missing from remote-command error: {error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_remote_start_rcpd_uses_configured_version_probe_timeout() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!(
            "rcp-configured-probe-timeout-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&directory).unwrap();
        let script = directory.join("rcpd");
        std::fs::write(
            &script,
            "#!/bin/sh\nif [ \"$1\" = \"--protocol-version\" ]; then\n  exec sleep 30\nfi\nexit 99\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cleanup = RemoteCleanup::new().unwrap();

        let config = test_rcpd_config();
        let result = tokio::time::timeout(std::time::Duration::from_secs(8), async {
            prepare_rcpd_with_context(
                &SshSession::local(),
                Some(script.to_str().unwrap()),
                false,
                PreparationContext::uncancelled(cleanup.clone()),
                std::time::Duration::from_secs(config.remote_copy_conn_timeout_sec),
            )
            .await?
            .spawn(&config, None, protocol::RcpdRole::Source)
            .await
        })
        .await
        .expect("the configured one-second probe deadline was not honored");
        let error = match result {
            Ok(_) => panic!("the hanging version probe unexpectedly succeeded"),
            Err(error) => error,
        };
        let error = format!("{error:#}");
        assert!(
            error.contains("timed out after 1s"),
            "configured deadline missing from probe error: {error}"
        );

        cleanup.finish();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_remote_prepared_rcpd_readiness_uses_configured_timeout() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join("rcpd");
        let daemon_exit = directory.path().join("daemon-exit");
        let version = common::version::ProtocolVersion::current()
            .to_json()
            .unwrap();
        let contents = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--protocol-version\" ]; then\n  printf '%s\\n' {}\n  exit 0\nfi\ncat >/dev/null\nprintf 'exited\\n' > {}\n",
            shell_escape(&version),
            shell_escape(daemon_exit.to_str().unwrap()),
        );
        std::fs::write(&script, contents).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cleanup = RemoteCleanup::new().unwrap();
        let prepared = prepare_test_rcpd(
            &SshSession::local(),
            Some(script.to_str().unwrap()),
            false,
            &cleanup,
        )
        .await
        .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            prepared.spawn(&test_rcpd_config(), None, protocol::RcpdRole::Source),
        )
        .await
        .expect("the configured one-second readiness deadline was not honored");
        let error = match result {
            Ok(_) => panic!("rcpd without a readiness record unexpectedly started"),
            Err(error) => format!("{error:#}"),
        };
        assert!(
            error.contains("timed out after 1s"),
            "configured deadline missing from readiness error: {error}"
        );
        assert_eq!(std::fs::read_to_string(daemon_exit).unwrap(), "exited\n");
        drop(prepared);
        cleanup.finish();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_remote_prepared_rcpd_rejects_an_oversized_readiness_record() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join("rcpd");
        let version = common::version::ProtocolVersion::current()
            .to_json()
            .unwrap();
        let contents = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--protocol-version\" ]; then\n  printf '%s\\n' {}\n  exit 0\nfi\ndd if=/dev/zero bs=71680 count=1 2>/dev/null | tr '\\000' x >&2\ncat >/dev/null\n",
            shell_escape(&version),
        );
        std::fs::write(&script, contents).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cleanup = RemoteCleanup::new().unwrap();
        let prepared = prepare_test_rcpd(
            &SshSession::local(),
            Some(script.to_str().unwrap()),
            false,
            &cleanup,
        )
        .await
        .unwrap();
        let mut config = test_rcpd_config();
        config.remote_copy_conn_timeout_sec = 5;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            prepared.spawn(&config, None, protocol::RcpdRole::Source),
        )
        .await
        .expect("an oversized readiness record must fail before the connection deadline");
        let error = match result {
            Ok(_) => panic!("an oversized readiness record unexpectedly started rcpd"),
            Err(error) => format!("{error:#}"),
        };
        assert!(
            error.contains("readiness record exceeds"),
            "oversized readiness error omitted the size limit: {error}"
        );
        drop(prepared);
        cleanup.finish();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_remote_prepared_rcpd_spawns_both_roles_after_one_preparation() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!(
            "rcp-prepared-rcpd-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&directory).unwrap();
        let script = directory.join("rcpd");
        let marker = directory.join("version-probes");
        let version = common::version::ProtocolVersion::current()
            .to_json()
            .unwrap();
        let contents = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--protocol-version\" ]; then\n  printf 'probe\\n' >> {}\n  printf '%s\\n' {}\n  exit 0\nfi\nprintf '%s\\n' 'RCP_TCP 127.0.0.1:1234 4 4' >&2\ncat >/dev/null\n",
            shell_escape(marker.to_str().unwrap()),
            shell_escape(&version),
        );
        std::fs::write(&script, contents).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = test_rcpd_config();
        let cleanup = RemoteCleanup::new().unwrap();
        let prepared = prepare_test_rcpd(
            &SshSession::local(),
            Some(script.to_str().unwrap()),
            false,
            &cleanup,
        )
        .await
        .unwrap();
        let source = prepared
            .spawn(&config, None, protocol::RcpdRole::Source)
            .await
            .unwrap();
        let destination = prepared
            .spawn(&config, None, protocol::RcpdRole::Destination)
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "probe\n");
        wait_for_rcpd_process(source).await.unwrap();
        wait_for_rcpd_process(destination).await.unwrap();
        drop(prepared);
        cleanup.finish();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_remote_wait_for_rcpd_process_retains_daemon_diagnostics() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!(
            "rcp-rcpd-diagnostics-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&directory).unwrap();
        let script = directory.join("rcpd");
        let version = common::version::ProtocolVersion::current()
            .to_json()
            .unwrap();
        let contents = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--protocol-version\" ]; then\n  printf '%s\\n' {}\n  exit 0\nfi\nprintf '%s\\n' 'RCP_TCP 127.0.0.1:1234 4 4' >&2\nprintf '%s\\n' 'daemon stdout detail'\nprintf '%s\\n' 'daemon stderr detail' >&2\nexit 42\n",
            shell_escape(&version),
        );
        std::fs::write(&script, contents).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cleanup = RemoteCleanup::new().unwrap();
        let prepared = prepare_test_rcpd(
            &SshSession::local(),
            Some(script.to_str().unwrap()),
            false,
            &cleanup,
        )
        .await
        .unwrap();
        let process = prepared
            .spawn(&test_rcpd_config(), None, protocol::RcpdRole::Source)
            .await
            .unwrap();
        let error = wait_for_rcpd_process(process).await.unwrap_err();
        let error = format!("{error:#}");
        assert!(
            error.contains("daemon stdout detail"),
            "stdout diagnostic missing: {error}"
        );
        assert!(
            error.contains("daemon stderr detail"),
            "stderr diagnostic missing: {error}"
        );
        drop(prepared);
        cleanup.finish();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_remote_failed_rcpd_bootstrap_reaps_child_before_returning() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!(
            "rcp-failed-bootstrap-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&directory).unwrap();
        let script = directory.join("rcpd");
        let source_exit = directory.join("source-exit");
        let destination_exit = directory.join("destination-exit");
        let version = common::version::ProtocolVersion::current()
            .to_json()
            .unwrap();
        let contents = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--protocol-version\" ]; then\n  printf '%s\\n' {}\n  exit 0\nfi\nif [ \"$2\" = source ]; then\n  printf '%s\\n' 'RCP_ERROR pending file capacity test refusal' >&2\n  marker={}\nelse\n  printf '%s\\n' 'not a readiness record' >&2\n  printf '%s\\n' 'destination listener bind failed'\n  marker={}\nfi\ncat >/dev/null\nprintf 'exited\\n' > \"$marker\"\n",
            shell_escape(&version),
            shell_escape(source_exit.to_str().unwrap()),
            shell_escape(destination_exit.to_str().unwrap()),
        );
        std::fs::write(&script, contents).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cleanup = RemoteCleanup::new().unwrap();
        let prepared = prepare_test_rcpd(
            &SshSession::local(),
            Some(script.to_str().unwrap()),
            false,
            &cleanup,
        )
        .await
        .unwrap();
        let config = test_rcpd_config();

        let refusal = match prepared
            .spawn(&config, None, protocol::RcpdRole::Source)
            .await
        {
            Ok(_) => panic!("startup refusal unexpectedly produced an rcpd process"),
            Err(error) => error,
        };
        assert!(refusal.to_string().contains("rcpd refused startup"));
        assert!(refusal.chain().any(|cause| {
            cause
                .to_string()
                .contains("pending file capacity test refusal")
        }));
        assert_eq!(std::fs::read_to_string(&source_exit).unwrap(), "exited\n");

        let malformed = match prepared
            .spawn(&config, None, protocol::RcpdRole::Destination)
            .await
        {
            Ok(_) => panic!("malformed readiness unexpectedly produced an rcpd process"),
            Err(error) => error,
        };
        assert!(
            format!("{malformed:#}").contains("unexpected output from rcpd"),
            "unexpected startup error: {malformed:#}"
        );
        assert!(
            format!("{malformed:#}").contains("destination listener bind failed"),
            "startup output must retain the daemon's failure: {malformed:#}"
        );
        assert_eq!(
            std::fs::read_to_string(&destination_exit).unwrap(),
            "exited\n"
        );
        drop(prepared);
        cleanup.finish();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_remote_cancelled_ssh_setup_terminates_its_launcher() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!(
            "rcp-ssh-launcher-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&directory).unwrap();
        let launcher = directory.join("ssh");
        let pid_file = directory.join("pid");
        let args_file = directory.join("args");
        std::fs::write(
            &launcher,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > {}\ncase \" $* \" in *\" ForkAfterAuthentication=no \"*) no_fork=yes ;; esac\ncase \" $* \" in *\" ControlPersist=no \"*) no_persist=yes ;; esac\nif [ \"$no_fork\" != yes ] || [ \"$no_persist\" != yes ]; then\n  setsid sh -c 'trap \"\" HUP TERM; printf \"%s\\n\" \"$$\" > \"$1\"; exec sleep 30' sh {} &\n  while [ ! -s {} ]; do sleep 0.01; done\n  sleep 0.2\n  exit 0\nfi\nprintf '%s\\n' \"$$\" > {}\nexec sleep 30\n",
                shell_escape(args_file.to_str().unwrap()),
                shell_escape(pid_file.to_str().unwrap()),
                shell_escape(pid_file.to_str().unwrap()),
                shell_escape(pid_file.to_str().unwrap()),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
        let session = SshSession::local();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::new(cancellation.clone(), cleanup.clone());
        let setup = tokio::spawn(async move {
            setup_ssh_session_with_program(
                &session,
                &preparation,
                &launcher,
                BootstrapDeadline::new(DEFAULT_REMOTE_BOOTSTRAP_TIMEOUT),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while std::fs::read_to_string(&pid_file)
                .map(|pid| pid.trim().is_empty())
                .unwrap_or(true)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fake SSH launcher must start");
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        let process = std::path::PathBuf::from(format!("/proc/{}", pid.trim()));
        assert!(process.exists(), "fake SSH launcher must still be running");
        let start_time = linux_process_start_time(&process)
            .expect("fake SSH launcher must expose a process identity");
        let arguments = std::fs::read_to_string(&args_file).unwrap();
        let control_directory = ssh_control_directory_from_arguments(&arguments);
        assert!(control_directory.exists());
        cancellation.cancel();
        let error = match setup.await.unwrap() {
            Ok(_) => panic!("cancelled SSH setup unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("cancelled because peer preparation failed")
        );
        assert!(
            arguments.contains("ForkAfterAuthentication=no")
                && arguments.contains("ControlPersist=no"),
            "launcher must override SSH config that could background the owned master: {arguments}"
        );
        cleanup.finish();
        if !wait_for_process_identity_to_disappear(&process, &start_time).await {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", pid.trim()])
                .status();
            panic!("cancelling SSH setup left its launcher unreaped");
        }
        assert!(
            !control_directory.exists(),
            "cancelled SSH setup must remove its private control directory"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn ssh_master_launcher_resolves_configured_program_in_owned_shell_child() {
        let launcher = ssh_master_launcher_command(std::path::Path::new("/stalled/path/ssh"));
        let command = launcher.as_std();
        assert_eq!(command.get_program(), std::ffi::OsStr::new("/bin/sh"));
        let arguments = command.get_args().collect::<Vec<_>>();
        assert_eq!(
            arguments,
            [
                std::ffi::OsStr::new("-c"),
                std::ffi::OsStr::new("exec \"$@\""),
                std::ffi::OsStr::new("rcp-ssh-master"),
                std::ffi::OsStr::new("/stalled/path/ssh"),
            ]
        );
    }

    #[test]
    fn openssh_host_strips_only_ipv6_operand_brackets() {
        for (host, expected) in [
            ("[2001:db8::1]", "2001:db8::1"),
            ("[fe80::1%eth0]", "fe80::1%eth0"),
            ("host.example", "host.example"),
            ("[host.example]", "[host.example]"),
            ("[host:name]", "[host:name]"),
            ("[fe80::1%]", "[fe80::1%]"),
            ("[2001:db8::1", "[2001:db8::1"),
        ] {
            let session = SshSession {
                user: None,
                host: host.to_string(),
                port: None,
            };
            assert_eq!(session.openssh_host(), expected, "unexpected host: {host}");
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_remote_ssh_setup_passes_bare_ipv6_host_to_launcher() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let launcher = directory.path().join("ssh");
        let args_file = directory.path().join("args");
        std::fs::write(
            &launcher,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > {}\nsocket=\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = -S ]; then\n    shift\n    socket=$1\n  fi\n  shift\ndone\n: > \"$socket\"\nexec sleep 30\n",
                shell_escape(args_file.to_str().unwrap()),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let managed = setup_ssh_session_with_program(
            &SshSession {
                user: Some("remote-user".to_string()),
                host: "[2001:db8::1]".to_string(),
                port: Some(2222),
            },
            &preparation,
            &launcher,
            BootstrapDeadline::new(DEFAULT_REMOTE_BOOTSTRAP_TIMEOUT),
        )
        .await
        .unwrap();

        let arguments = std::fs::read_to_string(&args_file).unwrap();
        let arguments = arguments.split_whitespace().collect::<Vec<_>>();
        assert_eq!(
            arguments.last().copied(),
            Some("2001:db8::1"),
            "the direct OpenSSH argv must not retain operand-syntax brackets: {arguments:?}"
        );
        assert!(
            arguments.windows(2).any(|pair| pair == ["-p", "2222"]),
            "the IPv6 normalization must retain the SSH port: {arguments:?}"
        );
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["-l", "remote-user"]),
            "the IPv6 normalization must retain the SSH user: {arguments:?}"
        );

        drop(managed);
        drop(preparation);
        cleanup.finish();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_remote_ssh_setup_deadline_terminates_its_launcher() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let launcher = directory.path().join("ssh");
        let pid_file = directory.path().join("pid");
        let args_file = directory.path().join("args");
        std::fs::write(
            &launcher,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > {}\nprintf '%s\\n' \"$$\" > {}\nexec sleep 30\n",
                shell_escape(args_file.to_str().unwrap()),
                shell_escape(pid_file.to_str().unwrap()),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
        let session = SshSession::local();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let mut setup = tokio::spawn(async move {
            setup_ssh_session_with_program(
                &session,
                &preparation,
                &launcher,
                BootstrapDeadline::new(std::time::Duration::from_millis(500)),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while std::fs::read_to_string(&pid_file)
                .map(|pid| pid.trim().is_empty())
                .unwrap_or(true)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fake SSH launcher must start");
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        let process = std::path::PathBuf::from(format!("/proc/{}", pid.trim()));
        let start_time = linux_process_start_time(&process)
            .expect("fake SSH launcher must expose a process identity");
        let arguments = std::fs::read_to_string(&args_file).unwrap();
        let control_directory = ssh_control_directory_from_arguments(&arguments);
        assert!(control_directory.exists());
        let result = match tokio::time::timeout(std::time::Duration::from_secs(2), &mut setup).await
        {
            Ok(result) => result.unwrap(),
            Err(_) => {
                setup.abort();
                let _ = setup.await;
                let _ = std::process::Command::new("kill")
                    .args(["-KILL", pid.trim()])
                    .status();
                panic!("the configured SSH setup deadline was not honored");
            }
        };
        let error = match result {
            Ok(_) => panic!("SSH setup without a control socket unexpectedly succeeded"),
            Err(error) => format!("{error:#}"),
        };
        assert!(
            error.contains("SSH session setup timed out after 500ms"),
            "configured deadline missing from SSH setup error: {error}"
        );
        cleanup.finish();
        if !wait_for_process_identity_to_disappear(&process, &start_time).await {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", pid.trim()])
                .status();
            panic!("timed-out SSH setup must reap its launcher");
        }
        assert!(
            !control_directory.exists(),
            "timed-out SSH setup must remove its private control directory"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_remote_managed_ssh_drop_terminates_master_without_a_runtime() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!(
            "rcp-managed-ssh-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&directory).unwrap();
        let launcher = directory.join("ssh");
        let pid_file = directory.join("pid");
        let socket_file = directory.join("socket");
        std::fs::write(
            &launcher,
            format!(
                "#!/bin/sh\nsocket=\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = -S ]; then\n    shift\n    socket=$1\n  fi\n  shift\ndone\nprintf '%s\\n' \"$$\" > {}\nprintf '%s\\n' \"$socket\" > {}\n: > \"$socket\"\nexec sleep 30\n",
                shell_escape(pid_file.to_str().unwrap()),
                shell_escape(socket_file.to_str().unwrap()),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let managed = runtime
            .block_on(setup_ssh_session_with_program(
                &SshSession::local(),
                &preparation,
                &launcher,
                BootstrapDeadline::new(DEFAULT_REMOTE_BOOTSTRAP_TIMEOUT),
            ))
            .unwrap();
        let pid = std::fs::read_to_string(&pid_file).unwrap();
        let process = std::path::PathBuf::from(format!("/proc/{}", pid.trim()));
        assert!(process_is_running(&process));
        let control_path =
            std::path::PathBuf::from(std::fs::read_to_string(&socket_file).unwrap().trim());
        let control_directory = control_path.parent().unwrap().to_path_buf();
        assert!(control_directory.exists());
        runtime.shutdown_timeout(std::time::Duration::from_millis(100));

        drop(managed);
        drop(preparation);
        cleanup.finish();
        if process_is_running(&process) {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", pid.trim()])
                .status();
            panic!("dropping the managed session did not terminate its SSH master");
        }
        assert!(
            !control_directory.exists(),
            "off-runtime SSH cleanup must remove its private control directory"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(target_os = "linux")]
    fn process_is_running(process: &std::path::Path) -> bool {
        let Ok(status) = std::fs::read_to_string(process.join("status")) else {
            return false;
        };
        !status.lines().any(|line| line.starts_with("State:\tZ"))
    }

    #[cfg(target_os = "linux")]
    fn linux_process_start_time(process: &std::path::Path) -> Option<String> {
        let stat = std::fs::read_to_string(process.join("stat")).ok()?;
        let (_, fields) = stat.rsplit_once(") ")?;
        fields.split_whitespace().nth(19).map(str::to_string)
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_process_identity_to_disappear(
        process: &std::path::Path,
        start_time: &str,
    ) -> bool {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while linux_process_start_time(process).as_deref() == Some(start_time) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok()
    }

    #[cfg(target_os = "linux")]
    fn ssh_control_directory_from_arguments(arguments: &str) -> std::path::PathBuf {
        let mut arguments = arguments.split_whitespace();
        while let Some(argument) = arguments.next() {
            if argument == "-S" {
                let socket = arguments
                    .next()
                    .expect("SSH -S must be followed by its socket path");
                return std::path::Path::new(socket)
                    .parent()
                    .expect("SSH control socket must have a parent directory")
                    .to_path_buf();
            }
        }
        panic!("SSH launcher arguments are missing -S: {arguments:?}");
    }

    #[test]
    fn failed_rcpd_output_keeps_both_captured_streams() {
        assert_eq!(
            format_failed_rcpd_output(b"destination listener bind failed\n", "daemon detail\n"),
            Some("stdout: destination listener bind failed; stderr: daemon detail".to_string())
        );
    }

    #[test]
    fn bounded_diagnostic_capture_retains_only_the_tail() {
        let mut output = b"012345".to_vec();
        assert!(retain_bounded_tail(&mut output, b"6789", 6));
        assert_eq!(output, b"456789");
        assert!(retain_bounded_tail(&mut output, b"abcdefgh", 6));
        assert_eq!(output, b"cdefgh");
    }

    #[tokio::test]
    async fn cancelling_output_drain_aborts_its_collector() {
        struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for NotifyDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let collector = tokio::spawn(async move {
            let _notify_drop = NotifyDrop(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<CapturedOutput>().await
        });
        started_rx.await.unwrap();
        let drain = tokio::spawn(finish_rcpd_output_drain(
            AbortOnDropTask::new(collector),
            "test",
        ));
        tokio::task::yield_now().await;

        drain.abort();
        let _ = drain.await;

        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("cancelling output drain must abort its collector")
            .unwrap();
    }

    #[tokio::test]
    async fn daemon_output_is_forwarded_before_eof_and_capture_remains_bounded() {
        use tokio::io::AsyncWriteExt;

        let (mut writer, reader) = tokio::io::duplex(64);
        let (forwarded_tx, mut forwarded_rx) = tokio::sync::mpsc::unbounded_channel();
        let collector = tokio::spawn(drain_bounded_output_forwarding(reader, 8, move |chunk| {
            let _ = forwarded_tx.send(chunk.to_vec());
        }));
        writer.write_all(b"sentinel\n").await.unwrap();

        let forwarded =
            tokio::time::timeout(std::time::Duration::from_secs(1), forwarded_rx.recv())
                .await
                .expect("daemon output must be forwarded while its stream is open")
                .expect("daemon output collector must remain connected");

        assert_eq!(forwarded, b"sentinel\n");
        assert!(
            !collector.is_finished(),
            "forwarding output must not require EOF"
        );
        writer.write_all(b"0123456789").await.unwrap();
        drop(writer);
        let captured = collector.await.unwrap();
        assert_eq!(captured.bytes, b"23456789");
        assert!(captured.truncated);
    }

    #[test]
    fn daemon_output_forwarding_preserves_utf8_split_across_reads() {
        let mut forwarded = Vec::new();
        {
            let mut forwarder = Utf8ChunkForwarder::new(|text| forwarded.push(text.to_string()));
            let output = "prefix € suffix".as_bytes();
            forwarder.push(&output[..8]);
            forwarder.push(&output[8..9]);
            forwarder.push(&output[9..]);
            forwarder.finish();
        }

        assert_eq!(forwarded.concat(), "prefix € suffix");
    }

    #[test]
    fn control_directory_selection_reaches_a_usable_home_fallback() {
        let home = tempfile::tempdir().unwrap();
        let missing = home.path().join("missing");
        assert_eq!(
            select_ssh_control_directory([missing.clone(), home.path().to_path_buf()]),
            Some(home.path().to_path_buf())
        );
        assert_eq!(select_ssh_control_directory([missing]), None);
    }

    #[test]
    fn control_directory_selection_stays_outside_a_local_operand_tree() {
        let local_operand = tempfile::tempdir().unwrap();
        let nested_temp = local_operand.path().join("tmp");
        std::fs::create_dir(&nested_temp).unwrap();
        let missing_system_temp = local_operand.path().join("missing-system-temp");

        assert_eq!(
            select_ssh_control_directory_from_environment(
                None,
                nested_temp,
                missing_system_temp,
                None,
                None,
                &[local_operand.path().to_path_buf()],
            ),
            None,
            "a usable directory inside a local operand is not an SSH artifact fallback"
        );
    }

    #[test]
    fn control_directory_selection_without_local_operands_accepts_an_absolute_candidate() {
        let temp = tempfile::tempdir().unwrap();

        assert_eq!(
            select_ssh_control_directory_from_environment(
                None,
                temp.path().to_path_buf(),
                temp.path().to_path_buf(),
                None,
                None,
                &[],
            ),
            Some(temp.path().to_path_buf()),
            "remote-to-remote copies have no local operand tree to exclude"
        );
    }

    #[test]
    fn control_directory_selection_accepts_an_ancestor_of_a_local_operand() {
        let candidate = tempfile::tempdir().unwrap();
        let local_operand = candidate.path().join("copy-source");
        std::fs::create_dir(&local_operand).unwrap();

        assert_eq!(
            select_ssh_control_directory_from_environment(
                None,
                candidate.path().to_path_buf(),
                candidate.path().to_path_buf(),
                None,
                None,
                &[local_operand],
            ),
            Some(candidate.path().to_path_buf()),
            "a private sibling in the candidate directory is outside the copied subtree"
        );
    }

    #[test]
    fn control_directory_selection_does_not_exclude_the_filesystem_root() {
        let temp = tempfile::tempdir().unwrap();

        assert_eq!(
            select_ssh_control_directory_from_environment(
                None,
                temp.path().to_path_buf(),
                temp.path().to_path_buf(),
                None,
                None,
                &[std::path::PathBuf::from("/")],
            ),
            Some(temp.path().to_path_buf()),
            "the root operand cannot exclude every absolute socket candidate"
        );
    }

    #[cfg(unix)]
    #[test]
    fn control_directory_selection_resolves_aliases_into_a_local_operand_tree() {
        use std::os::unix::fs::symlink;

        let local_operand = tempfile::tempdir().unwrap();
        let nested_temp = local_operand.path().join("tmp");
        std::fs::create_dir(&nested_temp).unwrap();
        let aliases = tempfile::tempdir().unwrap();
        let nested_alias = aliases.path().join("tmp-alias");
        symlink(&nested_temp, &nested_alias).unwrap();
        let missing_system_temp = aliases.path().join("missing-system-temp");

        assert_eq!(
            select_ssh_control_directory_from_environment(
                None,
                nested_alias,
                missing_system_temp,
                None,
                None,
                &[local_operand.path().to_path_buf()],
            ),
            None,
            "a path alias into a local operand is not an SSH artifact fallback"
        );
    }

    #[test]
    fn tcp_config_effective_connections_meet_finite_file_ceiling() {
        let configured = nonzero(8);
        for (files_in_flight, expected) in [(3, 3), (8, 8), (12, 8)] {
            assert_eq!(
                effective_max_connections(
                    common::ConcurrencyLimit::Limited(nonzero(files_in_flight)),
                    configured,
                ),
                nonzero(expected),
            );
        }
    }

    #[test]
    fn tcp_config_effective_connections_keep_configured_ceiling_when_files_are_unlimited() {
        assert_eq!(
            effective_max_connections(common::ConcurrencyLimit::Unlimited, nonzero(8)),
            nonzero(8),
        );
    }

    #[test]
    fn remote_concurrency_pending_capacity_uses_connections_and_multiplier() {
        let concurrency =
            resolve_remote_concurrency(common::ConcurrencyLimit::Unlimited, nonzero(3), nonzero(4))
                .unwrap();
        assert_eq!(concurrency.max_pending_files().get(), 12);
    }

    #[test]
    fn remote_concurrency_pending_capacity_rejects_overflow() {
        let valid_streams = tokio::sync::Semaphore::MAX_PERMITS;
        let overflowing_multiplier = usize::MAX / valid_streams + 1;
        let error = resolve_remote_concurrency(
            common::ConcurrencyLimit::Unlimited,
            nonzero(valid_streams),
            nonzero(overflowing_multiplier),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("pending file capacity overflow"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn remote_concurrency_rejects_stream_limit_before_pending_overflow() {
        let error = resolve_remote_concurrency(
            common::ConcurrencyLimit::Unlimited,
            nonzero(tokio::sync::Semaphore::MAX_PERMITS + 1),
            nonzero(usize::MAX),
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("effective stream capacity"),
            "unexpected capacity error: {error:#}"
        );
    }

    struct MockDiscoverySession {
        test_responses: HashMap<String, bool>,
        path_response: Option<String>,
        home_response: Result<Option<String>, String>,
        calls: Mutex<Vec<String>>,
    }

    impl Default for MockDiscoverySession {
        fn default() -> Self {
            Self {
                test_responses: HashMap::new(),
                path_response: None,
                home_response: Ok(None),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl MockDiscoverySession {
        fn new() -> Self {
            Self::default()
        }

        fn with_home(mut self, home: Option<&str>) -> Self {
            self.home_response = Ok(home.map(str::to_string));
            self
        }
        fn with_home_error(mut self, error: &str) -> Self {
            self.home_response = Err(error.to_string());
            self
        }
        fn with_path(mut self, path: Option<&str>) -> Self {
            self.path_response = path.map(|p| p.to_string());
            self
        }
        fn set_test_response(&mut self, path: &str, exists: bool) {
            self.test_responses.insert(path.to_string(), exists);
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl DiscoverySession for MockDiscoverySession {
        fn test_executable<'a>(
            &'a self,
            path: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'a>>
        {
            self.calls.lock().unwrap().push(format!("test:{}", path));
            let exists = self.test_responses.get(path).copied().unwrap_or(false);
            Box::pin(async move { Ok(exists) })
        }
        fn find_in_path<'a>(
            &'a self,
            binary: &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'a>,
        > {
            self.calls.lock().unwrap().push(format!("path:{}", binary));
            let result = self.path_response.clone();
            Box::pin(async move { Ok(result) })
        }
        fn remote_home<'a>(
            &'a self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'a>,
        > {
            self.calls.lock().unwrap().push("home".to_string());
            let result = self.home_response.clone();
            Box::pin(async move {
                match result {
                    Ok(home) => Ok(home),
                    Err(e) => Err(anyhow::anyhow!(e)),
                }
            })
        }
    }

    struct BlockingDrop {
        started: Option<std::sync::mpsc::Sender<()>>,
        release: std::sync::Arc<(Mutex<bool>, Condvar)>,
        finished: Option<std::sync::mpsc::Sender<()>>,
    }

    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            let _ = self.started.take().unwrap().send(());
            let (released, release_changed) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = release_changed.wait(released).unwrap();
            }
            let _ = self.finished.take().unwrap().send(());
        }
    }

    struct BlockingDropRelease(std::sync::Arc<(Mutex<bool>, Condvar)>);

    impl BlockingDropRelease {
        fn release(&self) {
            let (released, release_changed) = &*self.0;
            *released.lock().unwrap() = true;
            release_changed.notify_all();
        }
    }

    impl Drop for BlockingDropRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    #[test]
    fn deferred_drop_is_nonblocking_without_a_tokio_runtime() {
        let cleanup = RemoteCleanup::new().unwrap();
        let watchdog = std::time::Duration::from_secs(1);
        let (drop_started, wait_for_drop) = std::sync::mpsc::channel();
        let (drop_finished, wait_for_drop_finish) = std::sync::mpsc::channel();
        let (drop_returned, wait_for_return) = std::sync::mpsc::channel();
        let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let release_guard = BlockingDropRelease(release.clone());
        let blocking_drop = BlockingDrop {
            started: Some(drop_started),
            release,
            finished: Some(drop_finished),
        };
        let cleanup_for_drop = cleanup.clone();

        let coordinator = std::thread::spawn(move || {
            cleanup_for_drop.defer_drop(blocking_drop, "rcp-test-cleanup");
            drop_returned.send(()).unwrap();
        });
        wait_for_drop
            .recv_timeout(watchdog)
            .expect("deferred destructor must start");
        let returned_before_destructor = wait_for_return.recv_timeout(watchdog).is_ok();
        release_guard.release();
        coordinator.join().unwrap();
        wait_for_drop_finish
            .recv_timeout(watchdog)
            .expect("deferred destructor must finish after release");

        assert!(
            returned_before_destructor,
            "dropping the owner must not wait for its blocking destructor"
        );
        cleanup.finish();
    }

    #[test]
    fn cleanup_construction_failure_accepts_no_work() {
        let error = RemoteCleanup::try_new_with(|_| {
            Err(std::io::Error::other("cleanup supervisor unavailable"))
        })
        .expect_err("a scope without its supervisor must not be created");

        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(error.to_string(), "cleanup supervisor unavailable");
    }

    #[test]
    fn stopped_supervisor_never_runs_cleanup_on_the_submitter() {
        let (supervisor_stopped_tx, supervisor_stopped_rx) = std::sync::mpsc::channel();
        let cleanup = RemoteCleanup::try_new_with(move |_operation| {
            std::thread::Builder::new()
                .name("rcp-test-stopped-cleanup-supervisor".to_string())
                .spawn(move || {
                    supervisor_stopped_tx.send(()).unwrap();
                })
        })
        .unwrap();
        supervisor_stopped_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("test supervisor must stop before submission");
        let stopped_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !cleanup
            .state
            .supervisor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            assert!(
                std::time::Instant::now() < stopped_deadline,
                "test supervisor did not finish after reporting shutdown"
            );
            std::thread::yield_now();
        }
        let submitter = std::thread::current().id();
        let (executed_tx, executed_rx) = std::sync::mpsc::channel();

        cleanup.defer_bounded("rcp-test-supervisor-fallback", move |_budget| {
            executed_tx.send(std::thread::current().id()).unwrap();
        });

        let executor = executed_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("fallback cleanup must execute");
        assert_ne!(
            executor, submitter,
            "a stopped supervisor must not move blocking cleanup onto its submitter"
        );
        cleanup.finish();
    }

    #[test]
    fn cleanup_grace_also_retires_the_supervisor() {
        let cleanup = RemoteCleanup::new().unwrap();
        let state = cleanup.state.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let worker_release = release.clone();
        cleanup
            .submit_disposable("rcp-test-blocked-cleanup", move || {
                started_tx.send(()).unwrap();
                let (released, release_changed) = &*worker_release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = release_changed.wait(released).unwrap();
                }
            })
            .unwrap();
        started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("cleanup worker must start");

        let supervisor_joined = cleanup.finish_with_grace(std::time::Duration::from_millis(250));

        assert!(
            state
                .supervisor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none(),
            "the supervisor handle must leave shared state within the same cleanup grace"
        );
        assert!(
            supervisor_joined,
            "the cleanup supervisor itself must be joined within the shared cleanup grace"
        );
        let (released, release_changed) = &*release;
        *released.lock().unwrap() = true;
        release_changed.notify_all();
    }

    #[test]
    fn cleanup_grace_never_joins_a_still_running_supervisor() {
        let (operation_finished_tx, operation_finished_rx) = std::sync::mpsc::channel();
        let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let supervisor_release = release.clone();
        let cleanup = RemoteCleanup::try_new_with(move |operation| {
            std::thread::Builder::new()
                .name("rcp-test-blocked-supervisor-exit".to_string())
                .spawn(move || {
                    operation();
                    operation_finished_tx.send(()).unwrap();
                    let (released, release_changed) = &*supervisor_release;
                    let mut released = released.lock().unwrap();
                    while !*released {
                        released = release_changed.wait(released).unwrap();
                    }
                })
        })
        .unwrap();

        let supervisor_joined = cleanup.finish_with_grace(std::time::Duration::from_millis(20));

        operation_finished_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("the supervisor operation must finish before the wrapper remains blocked");
        assert!(
            !supervisor_joined,
            "a supervisor wrapper still running after the deadline must be detached"
        );
        let (released, release_changed) = &*release;
        *released.lock().unwrap() = true;
        release_changed.notify_all();
    }

    #[test]
    fn cleanup_finish_deadline_shortens_an_active_job_budget() {
        let cleanup = RemoteCleanup::new().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        cleanup.defer_bounded("rcp-test-shared-cleanup-deadline", move |budget| {
            started_tx.send(()).unwrap();
            let started = std::time::Instant::now();
            let exited = poll_process_exit_until_deadline(&budget, || Ok(false)).unwrap();
            finished_tx.send((exited, started.elapsed())).unwrap();
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("bounded cleanup must start before publishing the finish deadline");

        cleanup.finish_with_grace(std::time::Duration::from_millis(20));

        let (exited, elapsed) = finished_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("the shared finish deadline must wake and stop the cleanup worker");
        assert!(!exited, "a process that never exited must remain unreaped");
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "the per-job cleanup deadline ignored the shorter shared finish deadline"
        );
    }

    #[test]
    fn cleanup_submitted_after_finish_gets_a_fresh_job_budget() {
        let cleanup = RemoteCleanup::new().unwrap();
        let later_owner = cleanup.clone();

        cleanup.finish_with_grace(std::time::Duration::from_millis(20));

        let (waited_tx, waited_rx) = std::sync::mpsc::channel();
        later_owner.defer_bounded("rcp-test-late-cleanup-budget", move |budget| {
            waited_tx.send(budget.wait_for_next_poll()).unwrap();
        });
        assert!(
            waited_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("cleanup submitted by a remaining owner must run"),
            "an earlier owner's expired finish deadline must not poison a later cleanup job"
        );
        later_owner.finish();
    }

    #[test]
    fn process_polling_stops_at_its_cleanup_budget() {
        let cleanup = RemoteCleanup::new().unwrap();
        let budget =
            CleanupBudget::for_job(cleanup.state.clone(), std::time::Duration::from_millis(20));
        let polls = std::sync::atomic::AtomicUsize::new(0);
        let started = std::time::Instant::now();

        let reaped = poll_process_exit_until_deadline(&budget, || {
            polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(false)
        })
        .unwrap();

        assert!(
            !reaped,
            "a process still running at the deadline is unreaped"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "cleanup process polling exceeded its mandatory budget"
        );
        assert!(polls.load(std::sync::atomic::Ordering::Relaxed) >= 1);
        cleanup.finish();
    }

    #[test]
    fn dropping_last_cleanup_owner_still_drains_queued_work() {
        let start_supervisor = std::sync::Arc::new(std::sync::Barrier::new(2));
        let supervisor_gate = start_supervisor.clone();
        let cleanup = RemoteCleanup::try_new_with(move |operation| {
            std::thread::Builder::new()
                .name("rcp-test-gated-cleanup-supervisor".to_string())
                .spawn(move || {
                    supervisor_gate.wait();
                    operation();
                })
        })
        .unwrap();
        let (executed_tx, executed_rx) = std::sync::mpsc::channel();
        cleanup.defer_bounded("rcp-test-drop-cleanup", move |_budget| {
            executed_tx.send(()).unwrap();
        });

        drop(cleanup);
        start_supervisor.wait();

        executed_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("the supervisor must retain queued cleanup after its final owner drops");
    }

    #[test]
    fn ssh_control_directory_waits_for_master_exit() {
        let root = tempfile::tempdir().unwrap();
        let control_directory = root.path().join("control");
        std::fs::create_dir(&control_directory).unwrap();
        let cleanup = RemoteCleanup::new().unwrap();
        let budget = CleanupBudget::for_job(cleanup.state.clone(), REMOTE_CLEANUP_GRACE);
        let (polled_tx, polled_rx) = std::sync::mpsc::channel();
        let (exit_tx, exit_rx) = std::sync::mpsc::channel();
        let worker_control_directory = control_directory.clone();
        let worker = std::thread::spawn(move || {
            reap_process_and_control_directory(
                &budget,
                Some(&worker_control_directory),
                move || {
                    polled_tx.send(()).unwrap();
                    exit_rx.recv().unwrap();
                    Ok(true)
                },
            );
        });

        polled_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("the reaper must inspect the process before changing the directory");
        assert!(
            control_directory.exists(),
            "the control directory must outlive a still-running SSH master"
        );

        exit_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(
            !control_directory.exists(),
            "the control directory must be removed after the SSH master exits"
        );
        cleanup.finish();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ssh_control_directory_survives_reap_budget_expiry() {
        let root = tempfile::tempdir().unwrap();
        let control_directory = root.path().join("control");
        std::fs::create_dir(&control_directory).unwrap();
        let mut command = tokio::process::Command::new("sh");
        command.args(["-c", "sleep 2"]).kill_on_drop(true);
        let launcher = command.spawn().unwrap();
        let reaper = SshMasterReaper {
            launcher: Some(launcher),
            control_directory: Some(control_directory.clone()),
        };
        let cleanup = RemoteCleanup::new().unwrap();
        let budget =
            CleanupBudget::for_job(cleanup.state.clone(), std::time::Duration::from_millis(20));

        reaper.reap(budget);

        assert!(
            control_directory.exists(),
            "an unconfirmed process exit must not remove its live control directory"
        );
        cleanup.finish();
    }

    #[test]
    fn cleanup_scope_drains_nested_cleanup_in_its_parent_worker() {
        struct EnqueueSshCleanup {
            cleanup: RemoteCleanup,
            control_directory: std::path::PathBuf,
        }

        impl Drop for EnqueueSshCleanup {
            fn drop(&mut self) {
                let reaper = SshMasterReaper {
                    launcher: None,
                    control_directory: Some(self.control_directory.clone()),
                };
                self.cleanup
                    .defer_bounded("rcp-test-nested-ssh-cleanup", move |budget| {
                        reaper.reap(budget);
                    });
            }
        }

        let cleanup = RemoteCleanup::new().unwrap();
        let root = tempfile::tempdir().unwrap();
        let control_directory = root.path().join("control");
        std::fs::create_dir(&control_directory).unwrap();

        cleanup.defer_drop(
            EnqueueSshCleanup {
                cleanup: cleanup.clone(),
                control_directory: control_directory.clone(),
            },
            "rcp-test-nested-cleanup",
        );
        cleanup.finish();

        assert!(
            !control_directory.exists(),
            "the scope must finish nested SSH cleanup before its parent worker completes"
        );
    }

    #[test]
    fn cleanup_scopes_do_not_drain_each_others_workers() {
        fn blocked_cleanup(
            cleanup: &RemoteCleanup,
        ) -> (
            std::sync::mpsc::Receiver<()>,
            std::sync::Arc<(Mutex<bool>, Condvar)>,
        ) {
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
            let worker_release = release.clone();
            cleanup
                .submit_disposable("rcp-test-isolated-cleanup", move || {
                    started_tx.send(()).unwrap();
                    let (released, release_changed) = &*worker_release;
                    let mut released = released.lock().unwrap();
                    while !*released {
                        released = release_changed.wait(released).unwrap();
                    }
                })
                .unwrap();
            (started_rx, release)
        }

        let cleanup_a = RemoteCleanup::new().unwrap();
        let cleanup_b = RemoteCleanup::new().unwrap();
        let (started_a, release_a) = blocked_cleanup(&cleanup_a);
        let (started_b, release_b) = blocked_cleanup(&cleanup_b);
        started_a
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        started_b
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();

        let (finished_a_tx, finished_a_rx) = std::sync::mpsc::channel();
        let finish_a = std::thread::spawn(move || {
            cleanup_a.finish();
            finished_a_tx.send(()).unwrap();
        });
        let (released, release_changed) = &*release_a;
        *released.lock().unwrap() = true;
        release_changed.notify_all();
        finished_a_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("finishing one scope must not wait for another scope's worker");
        finish_a.join().unwrap();

        let (released, release_changed) = &*release_b;
        *released.lock().unwrap() = true;
        release_changed.notify_all();
        cleanup_b.finish();
    }

    #[test]
    fn cleanup_finish_waits_for_work_queued_by_a_last_owner() {
        let cleanup = RemoteCleanup::new().unwrap();
        let last_owner = cleanup.clone();
        let (finish_started_tx, finish_started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let finisher = std::thread::spawn(move || {
            finish_started_tx.send(()).unwrap();
            cleanup.finish();
            finished_tx.send(()).unwrap();
        });
        finish_started_rx.recv().unwrap();
        assert!(
            finished_rx
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err(),
            "cleanup scope finished while another owner was still live"
        );

        let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let release_guard = BlockingDropRelease(release.clone());
        let worker_release = release.clone();
        let (job_started_tx, job_started_rx) = std::sync::mpsc::channel();
        last_owner
            .submit_disposable("rcp-test-last-owner-cleanup", move || {
                job_started_tx.send(()).unwrap();
                let (released, release_changed) = &*worker_release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = release_changed.wait(released).unwrap();
                }
            })
            .unwrap();
        job_started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("last-owner cleanup must start");
        drop(last_owner);
        assert!(
            finished_rx
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err(),
            "cleanup scope finished before the last owner's worker"
        );

        release_guard.release();
        finished_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("cleanup scope must finish after the last owner's worker");
        finisher.join().unwrap();
    }

    #[test]
    fn panicking_cleanup_worker_still_completes_its_scope() {
        let cleanup = RemoteCleanup::new().unwrap();
        let state = cleanup.state.clone();
        cleanup
            .submit_disposable("rcp-test-panicking-cleanup", || panic!("cleanup panic"))
            .unwrap();

        cleanup.finish_with_grace(REMOTE_CLEANUP_GRACE);

        let workers = state
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(workers.pending, 0);
        assert!(workers.threads.is_empty());
    }

    #[tokio::test]
    async fn ssh_socket_readiness_reuses_one_worker() {
        let cleanup = RemoteCleanup::new().unwrap();
        let thread_ids = std::sync::Arc::new(Mutex::new(Vec::new()));
        let observed_ids = thread_ids.clone();
        let mut probes = 0;
        wait_for_ssh_control_socket(&cleanup, move || {
            observed_ids
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            probes += 1;
            Ok(probes == 4)
        })
        .await
        .unwrap();
        cleanup.finish();

        let thread_ids = thread_ids.lock().unwrap();
        assert_eq!(thread_ids.len(), 4);
        assert!(
            thread_ids
                .iter()
                .all(|thread_id| *thread_id == thread_ids[0])
        );
    }

    #[tokio::test]
    async fn cleanup_barrier_joins_an_abandoned_disposable_worker() {
        let cleanup = RemoteCleanup::new().unwrap();
        let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let worker_release = release.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let operation = tokio::spawn(run_disposable_blocking(
            cleanup.clone(),
            "rcp-test-disposable-cleanup",
            move || {
                let _ = started_tx.send(());
                let (released, release_changed) = &*worker_release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = release_changed.wait(released).unwrap();
                }
                anyhow::Ok(())
            },
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), started_rx)
            .await
            .expect("disposable worker must start before the timeout")
            .expect("disposable worker must report that it started");
        operation.abort();
        let _ = operation.await;

        let (barrier_started_tx, barrier_started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let barrier = std::thread::spawn(move || {
            barrier_started_tx.send(()).unwrap();
            cleanup.finish();
            finished_tx.send(()).unwrap();
        });
        barrier_started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("cleanup barrier thread must start");
        assert!(
            finished_rx
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err(),
            "cleanup barrier returned while its abandoned filesystem worker was still running"
        );
        let (released, release_changed) = &*release;
        *released.lock().unwrap() = true;
        release_changed.notify_all();
        finished_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("cleanup barrier must return after its disposable worker exits");
        barrier.join().unwrap();
    }

    #[test]
    fn paired_preparation_returns_error_while_successful_peer_drop_is_blocked() {
        let watchdog = std::time::Duration::from_secs(5);
        let (drop_started, wait_for_drop) = std::sync::mpsc::channel();
        let (drop_finished, wait_for_drop_finish) = std::sync::mpsc::channel();
        let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let release_guard = BlockingDropRelease(release.clone());
        let successful_peer = BlockingDrop {
            started: Some(drop_started),
            release,
            finished: Some(drop_finished),
        };
        let (success_ready, wait_for_success) = tokio::sync::oneshot::channel();
        let (result_ready, wait_for_result) = std::sync::mpsc::channel();

        let coordinator = std::thread::spawn(move || {
            let cleanup = RemoteCleanup::new().unwrap();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(join_remote_preparations(
                &cleanup,
                move |_preparation| async move {
                    success_ready.send(()).unwrap();
                    anyhow::Ok(successful_peer)
                },
                move |_preparation| async move {
                    wait_for_success.await.unwrap();
                    Err::<(), _>(anyhow::anyhow!("destination preparation failed"))
                },
            ));
            let _ = result_ready.send(result);
            runtime.shutdown_timeout(watchdog);
            cleanup.finish();
        });

        let drop_started_result = wait_for_drop.recv_timeout(watchdog);
        let result_before_release = wait_for_result.recv_timeout(watchdog);
        let returned_while_drop_blocked = result_before_release.is_ok();
        release_guard.release();
        let drop_finished_result = wait_for_drop_finish.recv_timeout(watchdog);
        let coordinator_result = match result_before_release {
            Ok(result) => Some(result),
            Err(_) => wait_for_result.recv_timeout(watchdog).ok(),
        };
        let coordinator_joined = coordinator.join();

        assert!(
            drop_started_result.is_ok(),
            "successful peer disposal must start before the watchdog"
        );
        assert!(
            drop_finished_result.is_ok(),
            "successful peer disposal must finish after release"
        );
        assert!(
            coordinator_joined.is_ok(),
            "coordinator thread must finish after disposal is released"
        );
        assert!(
            returned_while_drop_blocked,
            "the intrinsic endpoint error must return without synchronously waiting for successful peer Drop"
        );
        let error = match coordinator_result.expect("coordinator must return a result") {
            Ok(_) => panic!("paired preparation unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "destination preparation failed");
    }

    #[tokio::test]
    async fn cancelled_transaction_reaps_a_cooperative_owner() {
        struct ActiveOwner {
            active: std::sync::Arc<std::sync::atomic::AtomicBool>,
            dropped: Option<tokio::sync::oneshot::Sender<()>>,
        }

        impl Drop for ActiveOwner {
            fn drop(&mut self) {
                self.active
                    .store(false, std::sync::atomic::Ordering::SeqCst);
                if let Some(dropped) = self.dropped.take() {
                    let _ = dropped.send(());
                }
            }
        }

        let cancellation = tokio_util::sync::CancellationToken::new();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::new(cancellation.clone(), cleanup.clone());
        let active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let active_in_owner = active.clone();
        let (started, owner_started) = tokio::sync::oneshot::channel();
        let (dropped, owner_dropped) = tokio::sync::oneshot::channel();
        let owner = tokio::spawn(async move {
            active_in_owner.store(true, std::sync::atomic::Ordering::SeqCst);
            let _active_owner = ActiveOwner {
                active: active_in_owner,
                dropped: Some(dropped),
            };
            started.send(()).unwrap();
            std::future::pending::<anyhow::Result<()>>().await
        });
        owner_started.await.unwrap();
        cancellation.cancel();

        let error = preparation
            .run_cancellation_owned_transaction(
                owner,
                std::time::Duration::from_millis(20),
                "test owned transaction",
            )
            .await
            .expect_err("a pending owned transaction must observe cancellation");

        assert!(
            format!("{error:#}")
                .contains("rcpd preparation cancelled because peer preparation failed"),
            "peer cancellation missing from owned-transaction error: {error:#}"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), owner_dropped)
            .await
            .expect("the cleanup funnel must reap a cooperative cancelled owner")
            .unwrap();
        assert!(
            !active.load(std::sync::atomic::Ordering::SeqCst),
            "the cleanup funnel must drop the transaction owner"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_transaction_returns_after_grace_for_an_uncooperative_owner() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::new(cancellation.clone(), cleanup.clone());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let owner = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            anyhow::Ok(())
        });
        started_rx.await.unwrap();
        cancellation.cancel();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            preparation.run_cancellation_owned_transaction(
                owner,
                std::time::Duration::from_millis(20),
                "test uncooperative transaction",
            ),
        )
        .await;
        release_tx.send(()).unwrap();

        let error = result
            .expect("cancellation must not await an uncooperative owner after its grace")
            .expect_err("the cancelled transaction must fail");
        assert!(error.downcast_ref::<PeerPreparationCancelled>().is_some());
        drop(preparation);
        cleanup.finish();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bootstrap_deadline_reaps_a_raced_result_off_the_runtime_worker() {
        #[derive(Debug)]
        struct DropThreadReporter(Option<tokio::sync::oneshot::Sender<String>>);

        impl Drop for DropThreadReporter {
            fn drop(&mut self) {
                if let Some(dropped) = self.0.take() {
                    let thread_name = std::thread::current()
                        .name()
                        .unwrap_or("unnamed")
                        .to_string();
                    let _ = dropped.send(thread_name);
                }
            }
        }

        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let owner = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            anyhow::Ok(DropThreadReporter(Some(dropped_tx)))
        });
        started_rx.await.unwrap();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            preparation.run_abortable_with_deadline(
                owner,
                "test uncooperative bootstrap",
                BootstrapDeadline::new(std::time::Duration::from_millis(20)),
            ),
        )
        .await;
        release_tx.send(()).unwrap();

        let error = result
            .expect("a bootstrap deadline must not await an uncooperative owner")
            .expect_err("the expired bootstrap operation must fail");
        assert!(
            format!("{error:#}").contains("test uncooperative bootstrap timed out"),
            "bootstrap timeout missing from error: {error:#}"
        );
        let drop_thread = tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("the cleanup funnel must retain and dispose the raced owner result")
            .unwrap();
        assert_eq!(drop_thread, "rcp-owned-task-reap");
        drop(preparation);
        cleanup.finish();
    }

    #[tokio::test]
    async fn owned_transaction_error_propagates_without_cancellation_grace() {
        let cleanup = RemoteCleanup::new().unwrap();
        let preparation = PreparationContext::uncancelled(cleanup.clone());
        let owner = tokio::spawn(async { Err::<(), _>(anyhow::anyhow!("owned failure")) });
        let started = std::time::Instant::now();

        let error = preparation
            .run_cancellation_owned_transaction(
                owner,
                std::time::Duration::from_secs(1),
                "test failing transaction",
            )
            .await
            .expect_err("the owner failure must propagate");

        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "a completed owner failure must not enter deadline grace"
        );
        assert!(
            format!("{error:#}").contains("owned failure"),
            "owner error missing from chain: {error:#}"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[tokio::test]
    async fn discover_rcpd_prefers_explicit_path() {
        let mut session = MockDiscoverySession::new();
        session.set_test_response("/opt/rcpd", true);
        let path = discover_rcpd_path_internal(&session, Some("/opt/rcpd"), None)
            .await
            .expect("should return explicit path");
        assert_eq!(path, "/opt/rcpd");
        assert_eq!(session.calls(), vec!["test:/opt/rcpd"]);
    }

    #[tokio::test]
    async fn discover_rcpd_explicit_path_errors_without_fallbacks() {
        let session = MockDiscoverySession::new();
        let err = discover_rcpd_path_internal(&session, Some("/missing/rcpd"), None)
            .await
            .expect_err("should fail when explicit path is missing");
        assert!(
            err.to_string()
                .contains("rcpd binary not found or not executable"),
            "unexpected error: {err}"
        );
        assert_eq!(session.calls(), vec!["test:/missing/rcpd"]);
    }

    #[tokio::test]
    async fn discover_rcpd_uses_same_dir_first() {
        let mut session = MockDiscoverySession::new();
        session.set_test_response("/custom/bin/rcpd", true);
        let path =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect("should find in same directory");
        assert_eq!(path, "/custom/bin/rcpd");
        assert_eq!(session.calls(), vec!["test:/custom/bin/rcpd"]);
    }

    #[tokio::test]
    async fn discover_rcpd_falls_back_to_path_after_same_dir() {
        let mut session = MockDiscoverySession::new().with_path(Some("/usr/bin/rcpd"));
        session.set_test_response("/custom/bin/rcpd", false);
        let path =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect("should find in PATH after same dir miss");
        assert_eq!(path, "/usr/bin/rcpd");
        assert_eq!(session.calls(), vec!["test:/custom/bin/rcpd", "path:rcpd"]);
    }

    #[cfg(unix)]
    #[test]
    fn remote_path_discovery_script_does_not_require_external_which() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let candidate = directory.path().join("rcpd");
        std::fs::write(&candidate, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o755)).unwrap();
        let output = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                RCPD_PATH_DISCOVERY_SCRIPT,
                "rcp-path-discovery",
                "rcpd",
            ])
            .env("PATH", directory.path())
            .output()
            .unwrap();

        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            candidate.to_str().unwrap()
        );
    }

    #[tokio::test]
    async fn discover_rcpd_uses_cache_last() {
        let mut session = MockDiscoverySession::new()
            .with_home(Some("/home/rcp"))
            .with_path(None);
        session.set_test_response("/custom/bin/rcpd", false);
        let local_version = common::version::ProtocolVersion::current();
        let cache_path = format!(
            "/home/rcp/.cache/rcp/bin/rcpd-{}",
            local_version.cache_tag()
        );
        session.set_test_response(&cache_path, true);
        let path =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect("should fall back to cache");
        assert_eq!(path, cache_path);
        assert_eq!(
            session.calls(),
            vec![
                "test:/custom/bin/rcpd".to_string(),
                "path:rcpd".to_string(),
                "home".to_string(),
                format!("test:{cache_path}")
            ]
        );
    }

    #[tokio::test]
    async fn discover_rcpd_reports_home_missing_in_error() {
        let mut session = MockDiscoverySession::new().with_path(None);
        session.set_test_response("/custom/bin/rcpd", false);
        let err =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect_err("should fail when nothing is found");
        let msg = err.to_string();
        assert!(
            msg.contains("Deployed cache: (skipped, HOME not set)"),
            "expected searched list to mention skipped cache, got: {msg}"
        );
        assert_eq!(
            session.calls(),
            vec!["test:/custom/bin/rcpd", "path:rcpd", "home"]
        );
    }

    #[tokio::test]
    async fn discover_rcpd_propagates_home_lookup_failure() {
        let mut session = MockDiscoverySession::new()
            .with_path(None)
            .with_home_error("sentinel HOME lookup timeout");
        session.set_test_response("/custom/bin/rcpd", false);

        let error =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect_err("a failed HOME probe must not look like an absent HOME");

        assert!(
            format!("{error:#}").contains("sentinel HOME lookup timeout"),
            "HOME lookup failure missing from discovery error: {error:#}"
        );
        assert_eq!(
            session.calls(),
            vec!["test:/custom/bin/rcpd", "path:rcpd", "home"]
        );
    }

    /// verify that tokio_unstable is enabled
    ///
    /// this test ensures that the tokio_unstable cfg flag is properly set, which is required
    /// for console-subscriber (used in common/src/lib.rs) to function correctly.
    ///
    /// the compile_error! at the top of this file prevents compilation without tokio_unstable,
    /// but this test provides additional verification that the cfg flag is properly configured
    /// and catches cases where someone might remove the compile_error! macro.
    #[test]
    fn test_tokio_unstable_enabled() {
        // compile-time check: this will cause a test failure if tokio_unstable is not set
        #[cfg(not(tokio_unstable))]
        {
            panic!(
                "tokio_unstable cfg flag is not enabled! \
                 This is required for console-subscriber support. \
                 Check .cargo/config.toml"
            );
        }

        // runtime verification: if we get here, tokio_unstable is enabled
        #[cfg(tokio_unstable)]
        {
            // test passes - verify we can access tokio unstable features
            // tokio::task::JoinSet is an example of a type that uses unstable features
            let _join_set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        }
    }

    fn iface(name: &str, addr: [u8; 4]) -> InterfaceIpv4 {
        InterfaceIpv4 {
            name: name.to_string(),
            addr: std::net::Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3]),
        }
    }

    #[test]
    fn choose_best_ipv4_prefers_physical_interfaces() {
        let interfaces = vec![
            iface("docker0", [172, 17, 0, 1]),
            iface("enp3s0", [192, 168, 1, 44]),
            iface("tailscale0", [100, 115, 92, 5]),
        ];
        assert_eq!(
            choose_best_ipv4(&interfaces),
            Some(std::net::Ipv4Addr::new(192, 168, 1, 44))
        );
    }

    #[test]
    fn choose_best_ipv4_deprioritizes_link_local() {
        let interfaces = vec![
            iface("enp0s8", [169, 254, 10, 2]),
            iface("wlan0", [10, 0, 0, 23]),
        ];
        assert_eq!(
            choose_best_ipv4(&interfaces),
            Some(std::net::Ipv4Addr::new(10, 0, 0, 23))
        );
    }

    #[test]
    fn choose_best_ipv4_falls_back_to_loopback() {
        let interfaces = vec![iface("lo", [127, 0, 0, 1]), iface("docker0", [0, 0, 0, 0])];
        assert_eq!(
            choose_best_ipv4(&interfaces),
            Some(std::net::Ipv4Addr::new(127, 0, 0, 1))
        );
    }

    #[test]
    fn test_get_local_ip_with_explicit_ipv4() {
        // test that providing a valid IPv4 address works
        let result = get_local_ip(Some("192.168.1.100"));
        assert!(result.is_ok(), "should accept valid IPv4 address");
        let ip = result.unwrap();
        assert_eq!(
            ip,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 1, 100))
        );
    }

    #[test]
    fn test_get_local_ip_with_explicit_loopback() {
        // test that providing loopback address works
        let result = get_local_ip(Some("127.0.0.1"));
        assert!(result.is_ok(), "should accept loopback address");
        let ip = result.unwrap();
        assert_eq!(
            ip,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
        );
    }

    #[test]
    fn test_get_local_ip_rejects_ipv6() {
        // test that providing an IPv6 address fails with a good error message
        let result = get_local_ip(Some("::1"));
        assert!(result.is_err(), "should reject IPv6 address");
        let err = result.unwrap_err();
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("IPv6 address not supported"),
            "error should mention IPv6 not supported, got: {err_msg}"
        );
        assert!(
            err_msg.contains("0.0.0.0"),
            "error should mention IPv4-only binding, got: {err_msg}"
        );
    }

    #[test]
    fn test_get_local_ip_rejects_ipv6_full() {
        // test that providing a full IPv6 address fails
        let result = get_local_ip(Some("2001:db8::1"));
        assert!(result.is_err(), "should reject IPv6 address");
        let err = result.unwrap_err();
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("IPv6 address not supported"),
            "error should mention IPv6 not supported, got: {err_msg}"
        );
    }

    #[test]
    fn test_get_local_ip_rejects_invalid_ip() {
        // test that providing an invalid IP format fails with a good error message
        let result = get_local_ip(Some("not-an-ip"));
        assert!(result.is_err(), "should reject invalid IP format");
        let err = result.unwrap_err();
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("invalid IP address"),
            "error should mention invalid IP address, got: {err_msg}"
        );
    }

    #[test]
    fn test_get_local_ip_rejects_invalid_ipv4() {
        // test that providing an invalid IPv4 format fails
        let result = get_local_ip(Some("999.999.999.999"));
        assert!(result.is_err(), "should reject invalid IPv4 address");
        let err = result.unwrap_err();
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("invalid IP address"),
            "error should mention invalid IP address, got: {err_msg}"
        );
    }

    /// A real connected pair on loopback. The accepted half is returned so the caller keeps it
    /// alive — dropping it would reset the connection under the socket being inspected.
    async fn connected_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
        (client.unwrap(), accepted.unwrap().0)
    }

    // the options are read back with getsockopt rather than trusted: a wrong constant, a value the
    // kernel rejects, or a call that a socket2 feature gate silently compiled out would otherwise
    // leave the connection with no liveness detection at all and nothing to show for it.
    #[tokio::test]
    async fn configure_tcp_socket_applies_every_option() {
        let (stream, _accepted) = connected_pair().await;
        configure_tcp_socket(
            &stream,
            NetworkProfile::Datacenter,
            120,
            ConnectionKind::Control,
        );
        let sock = socket2::SockRef::from(&stream);
        assert!(stream.nodelay().unwrap(), "TCP_NODELAY must be set");
        assert!(sock.keepalive().unwrap(), "SO_KEEPALIVE must be on");
        assert_eq!(
            sock.tcp_keepalive_time().unwrap(),
            std::time::Duration::from_secs(60),
            "TCP_KEEPIDLE must be half the budget"
        );
        assert_eq!(
            sock.tcp_keepalive_interval().unwrap(),
            std::time::Duration::from_secs(10),
            "TCP_KEEPINTVL must be a twelfth of the budget"
        );
        assert_eq!(
            sock.tcp_keepalive_retries().unwrap(),
            TCP_KEEPALIVE_RETRIES,
            "TCP_KEEPCNT must be the configured retry count"
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            sock.tcp_user_timeout().unwrap(),
            Some(std::time::Duration::from_secs(120)),
            "TCP_USER_TIMEOUT must be the budget itself"
        );
    }

    #[tokio::test]
    async fn configure_tcp_socket_derives_keepalive_from_the_budget() {
        let (stream, _accepted) = connected_pair().await;
        configure_tcp_socket(
            &stream,
            NetworkProfile::Internet,
            60,
            ConnectionKind::Control,
        );
        let sock = socket2::SockRef::from(&stream);
        assert_eq!(
            sock.tcp_keepalive_time().unwrap(),
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            sock.tcp_keepalive_interval().unwrap(),
            std::time::Duration::from_secs(5)
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            sock.tcp_user_timeout().unwrap(),
            Some(std::time::Duration::from_secs(60))
        );
    }

    // a budget under 12s would derive a zero interval, which the kernel rejects outright — the
    // whole keepalive call would then fail and leave the connection unprotected
    #[tokio::test]
    async fn configure_tcp_socket_clamps_sub_second_derivations() {
        let (stream, _accepted) = connected_pair().await;
        configure_tcp_socket(
            &stream,
            NetworkProfile::Datacenter,
            1,
            ConnectionKind::Control,
        );
        let sock = socket2::SockRef::from(&stream);
        assert!(sock.keepalive().unwrap(), "SO_KEEPALIVE must still be on");
        assert_eq!(
            sock.tcp_keepalive_time().unwrap(),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            sock.tcp_keepalive_interval().unwrap(),
            std::time::Duration::from_secs(1)
        );
    }

    #[tokio::test]
    async fn configure_tcp_socket_leaves_keepalive_off_when_disabled() {
        let (stream, _accepted) = connected_pair().await;
        configure_tcp_socket(
            &stream,
            NetworkProfile::Datacenter,
            0,
            ConnectionKind::Control,
        );
        let sock = socket2::SockRef::from(&stream);
        assert!(
            !sock.keepalive().unwrap(),
            "SO_KEEPALIVE must stay off when the budget is 0"
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            sock.tcp_user_timeout().unwrap(),
            None,
            "TCP_USER_TIMEOUT must stay at the system default when the budget is 0"
        );
        assert!(
            stream.nodelay().unwrap(),
            "no-delay is not part of liveness detection and must still be set"
        );
    }

    // a data connection must NOT get TCP_USER_TIMEOUT: the destination stops reading for the whole
    // of its per-file iops reservation, and the user timeout would abort that live-but-silent peer
    // — failing a copy that used to merely run slow. Keepalive stays on, so an IDLE data connection
    // to a vanished host is still caught.
    #[tokio::test]
    async fn configure_tcp_socket_omits_user_timeout_on_data_connections() {
        let (stream, _accepted) = connected_pair().await;
        configure_tcp_socket(
            &stream,
            NetworkProfile::Datacenter,
            120,
            ConnectionKind::Data,
        );
        let sock = socket2::SockRef::from(&stream);
        #[cfg(target_os = "linux")]
        assert_eq!(
            sock.tcp_user_timeout().unwrap(),
            None,
            "a data connection must be left at the system retransmission limit"
        );
        assert!(
            sock.keepalive().unwrap(),
            "keepalive still covers an idle data connection"
        );
        assert_eq!(
            sock.tcp_keepalive_time().unwrap(),
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            sock.tcp_keepalive_retries().unwrap(),
            TCP_KEEPALIVE_RETRIES,
            "with no user timeout to override it, TCP_KEEPCNT is what ends a dead data connection"
        );
        assert!(stream.nodelay().unwrap());
    }

    // absolute sizes are not assertable — the kernel doubles the request and clamps it to
    // net.core.{r,w}mem_max — but a THIRD, untouched socket from the same host pins the baseline,
    // so the comparison fails if the sizing calls are removed. Comparing the two profiles to each
    // other cannot: they collapse to equality whenever both clamp to the same maximum.
    #[tokio::test]
    async fn configure_tcp_socket_sizes_buffers_by_profile() {
        let (datacenter, _a) = connected_pair().await;
        let (internet, _b) = connected_pair().await;
        let (untouched, _c) = connected_pair().await;
        configure_tcp_socket(
            &datacenter,
            NetworkProfile::Datacenter,
            0,
            ConnectionKind::Data,
        );
        configure_tcp_socket(&internet, NetworkProfile::Internet, 0, ConnectionKind::Data);
        let (dc, net, base) = (
            socket2::SockRef::from(&datacenter),
            socket2::SockRef::from(&internet),
            socket2::SockRef::from(&untouched),
        );
        // what is (and is not) assertable across kernels: an explicit SO_SNDBUF/SO_RCVBUF is
        // clamped to `wmem_max`/`rmem_max`, and on hosts whose sysctls leave those at the default
        // (~208 KiB) while TCP auto-tuning grows untouched sockets into megabytes — GitHub's
        // runners measure 425984 configured vs 2626560 untouched — the configured size sits BELOW
        // the untouched default no matter what this code does. So "configured > default" is a
        // property of the host, not of configure_tcp_socket, and is deliberately NOT asserted.
        // what the code does guarantee: both profiles issue a set (asserted as ordering below —
        // datacenter requests 8x Internet, so wherever the clamp permits any distinction the
        // ordering holds, and equality is exactly the fully-clamped case), and the sizes are sane.
        for (name, sock) in [("datacenter", &dc), ("internet", &net)] {
            assert!(
                sock.send_buffer_size().unwrap() > 0 && sock.recv_buffer_size().unwrap() > 0,
                "{name} buffer sizes must be readable and non-zero"
            );
        }
        // the untouched socket is read (not asserted against) so a debugging run shows all three
        let _ = (base.send_buffer_size(), base.recv_buffer_size());
        assert!(dc.send_buffer_size().unwrap() >= net.send_buffer_size().unwrap());
        assert!(dc.recv_buffer_size().unwrap() >= net.recv_buffer_size().unwrap());
    }
}
