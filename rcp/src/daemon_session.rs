//! Post-readiness `rcpd` session lifecycle over the master's framed `stdin`/`stdout` control
//! channel.
//!
//! Used directly by the `rcpd` binary and by deterministic multi-host Turmoil simulations so
//! heartbeat watchdogs, `SshFallbackEndpoints` forwarding, result delivery under backpressure,
//! and stdout draining execute the exact same state machine in production and tests.

use anyhow::Context as _;

use crate::{destination, source};

#[derive(Debug)]
struct KeepaliveIdleTimeout(std::time::Duration);

impl std::fmt::Display for KeepaliveIdleTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "master control channel idle timeout after {}s",
            self.0.as_secs()
        )
    }
}

impl std::error::Error for KeepaliveIdleTimeout {}

fn find_keepalive_idle_timeout(error: &anyhow::Error) -> Option<std::time::Duration> {
    for cause in error.chain() {
        if let Some(timeout) = cause.downcast_ref::<KeepaliveIdleTimeout>() {
            return Some(timeout.0);
        }
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>()
            && let Some(timeout) = io_err
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<KeepaliveIdleTimeout>())
        {
            return Some(timeout.0);
        }
    }
    None
}

/// Async reader adapter that bounds continuous read idleness by `budget`, refreshing the idle
/// deadline whenever new bytes arrive so a slowly-streaming large frame (such as `MasterHello`
/// carrying a large filter file) stays alive as long as read progress continues.
struct IdleTimeoutReader<R> {
    inner: R,
    budget: Option<std::time::Duration>,
    deadline_armed: bool,
    sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
}

impl<R> IdleTimeoutReader<R> {
    fn new(inner: R, keepalive_sec: u64) -> Self {
        Self {
            inner,
            budget: (keepalive_sec > 0).then(|| std::time::Duration::from_secs(keepalive_sec)),
            deadline_armed: false,
            sleep: Box::pin(tokio::time::sleep(std::time::Duration::ZERO)),
        }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for IdleTimeoutReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before_len = buf.filled().len();
        match std::pin::Pin::new(&mut this.inner).poll_read(cx, buf) {
            std::task::Poll::Ready(Ok(())) => {
                if buf.filled().len() > before_len {
                    // forward progress arrived; the next wait gets a fresh idle budget
                    this.deadline_armed = false;
                }
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(Err(error)) => {
                this.deadline_armed = false;
                std::task::Poll::Ready(Err(error))
            }
            std::task::Poll::Pending => {
                if let Some(budget) = this.budget {
                    if !this.deadline_armed
                        && let Some(deadline) = tokio::time::Instant::now().checked_add(budget)
                    {
                        this.sleep.as_mut().reset(deadline);
                        this.deadline_armed = true;
                    }
                    if this.deadline_armed
                        && std::future::Future::poll(this.sleep.as_mut(), cx).is_ready()
                    {
                        this.deadline_armed = false;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            KeepaliveIdleTimeout(budget),
                        )));
                    }
                }
                std::task::Poll::Pending
            }
        }
    }
}

enum MasterReadOutcome {
    Message(Box<remote::protocol::MasterToRcpd>),
    Eof,
    KeepaliveTimedOut(std::time::Duration),
    Error(anyhow::Error),
}

struct MasterStdinStream<R> {
    framed: remote::streams::RecvStream<IdleTimeoutReader<R>>,
}

impl<R: tokio::io::AsyncRead + Unpin> MasterStdinStream<R> {
    fn new(reader: R, keepalive_sec: u64) -> Self {
        Self {
            framed: remote::streams::RecvStream::new(IdleTimeoutReader::new(reader, keepalive_sec)),
        }
    }

    async fn recv_message(&mut self) -> MasterReadOutcome {
        match self
            .framed
            .recv_object::<remote::protocol::MasterToRcpd>()
            .await
        {
            Ok(Some(message)) => MasterReadOutcome::Message(Box::new(message)),
            Ok(None) => MasterReadOutcome::Eof,
            Err(error) => {
                if let Some(budget) = find_keepalive_idle_timeout(&error) {
                    MasterReadOutcome::KeepaliveTimedOut(budget)
                } else {
                    MasterReadOutcome::Error(error)
                }
            }
        }
    }
}

/// Receive the initial `MasterHello` before starting the operation.
///
/// Pre-hello `Eof` (for example when destination startup fails and the master closes source
/// stdin while draining source startup notices) and decode/I/O errors return `drain_stdout = true`
/// because no `RcpdResult` has been queued yet. A keepalive timeout returns `drain_stdout = false`
/// because the master host vanished.
async fn receive_master_hello<R>(
    master_stdin: &mut MasterStdinStream<R>,
) -> Result<remote::protocol::MasterHello, OperationOutcome>
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        match master_stdin.recv_message().await {
            MasterReadOutcome::Message(message) => match *message {
                remote::protocol::MasterToRcpd::Hello(hello) => return Ok(*hello),
                remote::protocol::MasterToRcpd::Heartbeat => {
                    // master sends periodic heartbeats from spawn while bringing up the peer daemon
                    continue;
                }
                remote::protocol::MasterToRcpd::SshFallbackEndpoints { .. } => {
                    tracing::warn!("ignoring unexpected SshFallbackEndpoints before MasterHello");
                    continue;
                }
            },
            MasterReadOutcome::Eof => {
                return Err(OperationOutcome {
                    result: remote::protocol::RcpdResult::Failure {
                        error: "master closed its stdin control channel before sending its hello"
                            .to_string(),
                        summary: common::copy::Summary::default(),
                        runtime_stats: common::collect_runtime_stats(),
                        peer_connected: false,
                    },
                    drain_stdout: true,
                });
            }
            MasterReadOutcome::KeepaliveTimedOut(budget) => {
                return Err(OperationOutcome {
                    result: remote::protocol::RcpdResult::Failure {
                        error: format!(
                            "master control channel timed out after {}s waiting for hello",
                            budget.as_secs()
                        ),
                        summary: common::copy::Summary::default(),
                        runtime_stats: common::collect_runtime_stats(),
                        peer_connected: false,
                    },
                    drain_stdout: false,
                });
            }
            MasterReadOutcome::Error(error) => {
                return Err(OperationOutcome {
                    result: remote::protocol::RcpdResult::Failure {
                        error: format!("Failed to receive hello message from master: {error:#}"),
                        summary: common::copy::Summary::default(),
                        runtime_stats: common::collect_runtime_stats(),
                        peer_connected: false,
                    },
                    drain_stdout: true,
                });
            }
        }
    }
}

struct MasterLoss {
    cause: String,
    /// True when the master may still be draining stdout diagnostics (for example after an
    /// explicit stdin EOF); false after a keepalive timeout where the master host vanished.
    drain_stdout: bool,
}

/// Monitor the master's SSH stdin control stream for disconnect, missed heartbeats, or
/// master-initiated fallback endpoints.
///
/// The master sends periodic [`remote::protocol::MasterToRcpd::Heartbeat`] frames while alive
/// (whenever `keepalive_sec > 0`), optionally sends one
/// [`remote::protocol::MasterToRcpd::SshFallbackEndpoints`] frame when destination `auto` mode
/// falls back to SSH, and closes stdin when tearing down. Returning ends the active operation.
async fn master_stdin_watchdog<R>(
    master_stdin: &mut MasterStdinStream<R>,
    mut fallback_tx: Option<tokio::sync::oneshot::Sender<destination::SshFallbackEndpoints>>,
) -> MasterLoss
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        match master_stdin.recv_message().await {
            MasterReadOutcome::Message(message) => match *message {
                remote::protocol::MasterToRcpd::Heartbeat => continue,
                remote::protocol::MasterToRcpd::SshFallbackEndpoints {
                    control_addr,
                    data_addrs,
                } => {
                    if let Some(tx) = fallback_tx.take() {
                        let _ = tx.send(destination::SshFallbackEndpoints {
                            control_addr,
                            data_addrs,
                        });
                    } else {
                        tracing::warn!(
                            "ignoring unexpected SshFallbackEndpoints from master (control={control_addr})"
                        );
                    }
                }
                remote::protocol::MasterToRcpd::Hello(unexpected) => {
                    tracing::warn!(
                        "ignoring an unexpected hello message from the master after startup: {unexpected:?}"
                    );
                }
            },
            MasterReadOutcome::Eof => {
                return MasterLoss {
                    cause: "master closed its stdin control channel".to_string(),
                    drain_stdout: true,
                };
            }
            MasterReadOutcome::KeepaliveTimedOut(budget) => {
                return MasterLoss {
                    cause: format!(
                        "master control channel timed out after {}s without a heartbeat",
                        budget.as_secs()
                    ),
                    drain_stdout: false,
                };
            }
            MasterReadOutcome::Error(error) => {
                return MasterLoss {
                    cause: format!("master control channel failed: {error:#}"),
                    drain_stdout: true,
                };
            }
        }
    }
}

/// Outcome of running an `rcpd` operation and attempting to deliver its final `RcpdResult`.
#[derive(Debug)]
struct OperationOutcome {
    result: remote::protocol::RcpdResult,
    /// True when the daemon should drain queued tracing notices for `TRACING_DRAIN_GRACE`
    /// (after `RcpdResult` delivery succeeded, or on non-timeout pre-result exits where no
    /// `RcpdResult` was queued). False after a keepalive timeout or failed `RcpdResult` delivery
    /// so the stdout writer is aborted immediately.
    drain_stdout: bool,
}

/// Map an operation's outcome to the `RcpdResult` sent to the master, extracting the real
/// summary from a `common::copy::Error` when one is carried.
fn rcpd_result_from(
    result: anyhow::Result<(String, common::copy::Summary)>,
    peer_connected: bool,
) -> remote::protocol::RcpdResult {
    let runtime_stats = common::collect_runtime_stats();
    match result {
        Ok((message, summary)) => remote::protocol::RcpdResult::Success {
            message,
            summary,
            runtime_stats,
        },
        Err(error) => {
            // try to extract the real summary from common::copy::Error
            let (error_msg, summary) = match error.downcast::<common::copy::Error>() {
                Ok(copy_error) => (format!("{:#}", copy_error.source), copy_error.summary),
                Err(other_error) => (format!("{other_error:#}"), common::copy::Summary::default()),
            };
            remote::protocol::RcpdResult::Failure {
                error: error_msg,
                summary,
                runtime_stats,
                peer_connected,
            }
        }
    }
}

/// Verify that `MasterHello`'s preview setting matches the daemon's startup admission mode.
fn validate_preview_mode(
    preview_only: bool,
    hello: &remote::protocol::MasterHello,
) -> anyhow::Result<()> {
    let preview = match hello {
        remote::protocol::MasterHello::Source { dry_run, .. } => dry_run.is_some(),
        remote::protocol::MasterHello::Destination { dry_run, .. } => *dry_run,
    };
    anyhow::ensure!(
        preview == preview_only,
        "master hello preview mode does not match daemon startup admission"
    );
    Ok(())
}

enum WatchdogOutcome<T> {
    Completed(T),
    MasterLost(MasterLoss),
}

async fn run_with_master_watchdog<Operation, Watchdog, T>(
    operation: Operation,
    watchdog: Watchdog,
) -> WatchdogOutcome<T>
where
    Operation: std::future::Future<Output = T>,
    Watchdog: std::future::Future<Output = MasterLoss>,
{
    // watchdog cancellation drops the losing operation inside this surviving scope, so aborted
    // tracked descendants release their captures before daemon teardown can finish
    common::task_scope::scope_tasks(async move {
        tokio::pin!(operation);
        tokio::select! {
            // biased: the operation branch is polled first, so whenever both branches are ready
            // in the same poll the finished operation wins
            biased;
            result = &mut operation => WatchdogOutcome::Completed(result),
            loss = watchdog => WatchdogOutcome::MasterLost(loss),
        }
    })
    .await
}

/// Brief grace after `stdin` EOF for an already-written `RcpdResult` acknowledgement to arrive.
///
/// On normal completion the master closes daemon `stdin` right after decoding `RcpdResult`, so the
/// writer task has already pushed the frame into stdout and its ack resolves immediately. On a real
/// disconnect stdout breaks with `EPIPE` immediately as well. This grace only bounds a half-closed
/// peer that closes `stdin` while leaving `stdout` open and unread.
const STDIN_EOF_RESULT_ACK_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Deliver the authoritative `RcpdResult` while monitoring `master_stdin` under the configured
/// liveness policy instead of capping delivery to [`TRACING_DRAIN_GRACE`].
///
/// While the master remains alive (heartbeats keep arriving, or `keepalive_sec == 0`), this waits
/// for stdout backpressure (for example an in-flight trace frame on a slow SSH link) to clear. On
/// disconnect or keepalive timeout it returns `drain_stdout = false` so the caller aborts the
/// stdout writer instead of later flushing an unacknowledged result.
async fn deliver_result<R>(
    master_stdin: &mut MasterStdinStream<R>,
    master_control: &remote::stdio::DaemonControlSender,
    result: remote::protocol::RcpdResult,
) -> OperationOutcome
where
    R: tokio::io::AsyncRead + Unpin,
{
    let send = master_control.send_result(result.clone());
    tokio::pin!(send);
    loop {
        tokio::select! {
            // biased: if the writer already flushed RcpdResult and sent its ack, always report
            // that consistent delivery even if master stdin EOF arrived in the same poll
            biased;
            send_res = &mut send => {
                return match send_res {
                    Ok(()) => OperationOutcome {
                        result,
                        drain_stdout: true,
                    },
                    Err(error) => {
                        tracing::error!("failed to send result to master: {error:#}");
                        OperationOutcome {
                            result: remote::protocol::RcpdResult::Failure {
                                error: format!("failed to send result to master: {error:#}"),
                                summary: common::copy::Summary::default(),
                                runtime_stats: common::collect_runtime_stats(),
                                peer_connected: false,
                            },
                            drain_stdout: false,
                        }
                    }
                };
            }
            outcome = master_stdin.recv_message() => {
                match outcome {
                    MasterReadOutcome::Message(message) => match *message {
                        remote::protocol::MasterToRcpd::Heartbeat => continue,
                        remote::protocol::MasterToRcpd::SshFallbackEndpoints { .. } => {
                            tracing::warn!(
                                "ignoring unexpected SshFallbackEndpoints while sending result"
                            );
                        }
                        remote::protocol::MasterToRcpd::Hello(unexpected) => {
                            tracing::warn!(
                                "ignoring an unexpected hello message from the master while sending result: {unexpected:?}"
                            );
                        }
                    },
                    MasterReadOutcome::Eof => {
                        return match tokio::time::timeout(STDIN_EOF_RESULT_ACK_GRACE, &mut send).await {
                            Ok(Ok(())) => OperationOutcome {
                                result,
                                drain_stdout: true,
                            },
                            Ok(Err(error)) => {
                                tracing::error!("failed to send result to master: {error:#}");
                                OperationOutcome {
                                    result: remote::protocol::RcpdResult::Failure {
                                        error: format!("failed to send result to master: {error:#}"),
                                        summary: common::copy::Summary::default(),
                                        runtime_stats: common::collect_runtime_stats(),
                                        peer_connected: false,
                                    },
                                    drain_stdout: false,
                                }
                            }
                            Err(_) => {
                                tracing::error!(
                                    "master closed stdin before result delivery completed"
                                );
                                OperationOutcome {
                                    result: remote::protocol::RcpdResult::Failure {
                                        error: "master closed stdin before result delivery completed"
                                            .to_string(),
                                        summary: common::copy::Summary::default(),
                                        runtime_stats: common::collect_runtime_stats(),
                                        peer_connected: false,
                                    },
                                    drain_stdout: false,
                                }
                            }
                        };
                    }
                    MasterReadOutcome::KeepaliveTimedOut(budget) => {
                        tracing::error!(
                            "master control channel timed out after {}s while sending result",
                            budget.as_secs()
                        );
                        return OperationOutcome {
                            result: remote::protocol::RcpdResult::Failure {
                                error: format!(
                                    "master control channel timed out after {}s while sending result",
                                    budget.as_secs()
                                ),
                                summary: common::copy::Summary::default(),
                                runtime_stats: common::collect_runtime_stats(),
                                peer_connected: false,
                            },
                            drain_stdout: false,
                        };
                    }
                    MasterReadOutcome::Error(error) => {
                        tracing::error!(
                            "master control channel failed while sending result: {error:#}"
                        );
                        return OperationOutcome {
                            result: remote::protocol::RcpdResult::Failure {
                                error: format!(
                                    "master control channel failed while sending result: {error:#}"
                                ),
                                summary: common::copy::Summary::default(),
                                runtime_stats: common::collect_runtime_stats(),
                                peer_connected: false,
                            },
                            drain_stdout: false,
                        };
                    }
                }
            }
        }
    }
}

/// Run an operation under the master stdin watchdog, then deliver `RcpdResult` tied to master
/// disconnect or the configured keepalive policy.
async fn run_guarded_operation_and_send_result<Operation, R>(
    operation: Operation,
    master_stdin: &mut MasterStdinStream<R>,
    master_control: &remote::stdio::DaemonControlSender,
    fallback_tx: Option<tokio::sync::oneshot::Sender<destination::SshFallbackEndpoints>>,
    peer_connected: &std::sync::atomic::AtomicBool,
) -> OperationOutcome
where
    Operation: std::future::Future<Output = anyhow::Result<(String, common::copy::Summary)>>,
    R: tokio::io::AsyncRead + Unpin,
{
    let outcome =
        match run_with_master_watchdog(operation, master_stdin_watchdog(master_stdin, fallback_tx))
            .await
        {
            WatchdogOutcome::Completed(outcome) => outcome,
            WatchdogOutcome::MasterLost(loss) => {
                tracing::error!(
                    "Master (rcp) control channel lost mid-operation - {}. This usually \
                     means the master process was killed or its host became unreachable. \
                     Shutting down.",
                    loss.cause
                );
                return OperationOutcome {
                    result: remote::protocol::RcpdResult::Failure {
                        error: format!("master control channel lost: {}", loss.cause),
                        summary: common::copy::Summary::default(),
                        runtime_stats: common::collect_runtime_stats(),
                        peer_connected: peer_connected.load(std::sync::atomic::Ordering::Acquire),
                    },
                    drain_stdout: loss.drain_stdout,
                };
            }
        };
    let result = rcpd_result_from(
        outcome,
        peer_connected.load(std::sync::atomic::Ordering::Acquire),
    );
    deliver_result(master_stdin, master_control, result).await
}

/// Maximum grace given to drain queued daemon tracing frames on stdout after result delivery.
const TRACING_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

fn write_stderr_best_effort(args: std::fmt::Arguments<'_>) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{args}");
}

async fn finish_stdout_writer(
    task: remote::AbortOnDropTask<()>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    // the same grace bounds an in-flight frame and the remaining queue. The task guard aborts
    // on expiry or caller cancellation instead of detaching a writer blocked on the master
    cancellation_token.cancel();
    match tokio::time::timeout(TRACING_DRAIN_GRACE, task.join()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            write_stderr_best_effort(format_args!("Master stdout writer task failed: {error:#}"));
        }
        Err(_) => {
            write_stderr_best_effort(format_args!(
                "Timed out draining remote master stdout stream"
            ));
        }
    }
}

/// Static configuration for one post-readiness `rcpd` session.
pub struct DaemonSessionConfig {
    pub preview_only: bool,
    pub overwrite_manifest_max_entries: usize,
    pub bind_ip: Option<String>,
    pub tcp_config: remote::TcpConfig,
    pub concurrency: remote::ResolvedRemoteConcurrency,
    pub admission: common::EndpointAdmission,
    pub files_source: common::FilesInFlightSource,
    pub cert_key: Option<remote::tls::CertifiedKey>,
    /// Base copy settings before applying `MasterHello::Source`'s `filter` and `dry_run`.
    pub copy_settings_base: common::copy::Settings,
}

async fn run_operation<R>(
    config: DaemonSessionConfig,
    master_control: remote::stdio::DaemonControlSender,
    mut master_stdin: MasterStdinStream<R>,
    remote_cleanup: Option<remote::RemoteCleanup>,
    transport: std::sync::Arc<dyn remote::transport::Transport>,
) -> OperationOutcome
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let master_hello = match receive_master_hello(&mut master_stdin).await {
        Ok(hello) => hello,
        Err(outcome) => return outcome,
    };
    tracing::info!("Received side: {:?}", master_hello);
    let (fallback_tx, fallback_rx) = match &master_hello {
        remote::protocol::MasterHello::Destination { transport, .. }
            if matches!(
                transport.as_ref(),
                remote::protocol::DestinationTransport::AutoMasterSsh { .. }
            ) =>
        {
            let (tx, rx) = tokio::sync::oneshot::channel();
            (Some(tx), Some(rx))
        }
        _ => (None, None),
    };
    let control_for_op = master_control.clone();
    let peer_connected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let peer_connected_for_op = peer_connected.clone();
    // box the operation future so debug callers do not stack-nest both role state machines inside
    // run_with_master_watchdog
    let operation = Box::pin(async move {
        validate_preview_mode(config.preview_only, &master_hello)?;
        match master_hello {
            remote::protocol::MasterHello::Source {
                src,
                dst,
                peer_auth,
                filter,
                dry_run,
                capture,
                accept_timeout_sec,
            } => {
                let mut settings = config.copy_settings_base;
                settings.filter = filter;
                settings.dry_run = dry_run;
                let accept_timeout = std::time::Duration::from_secs(accept_timeout_sec.max(1));
                tracing::info!("Starting source");
                source::run_source_with_transport(
                    &control_for_op,
                    &src,
                    &dst,
                    &settings,
                    capture,
                    &config.tcp_config,
                    config.concurrency,
                    config.admission,
                    config.files_source,
                    config.bind_ip.as_deref(),
                    accept_timeout,
                    config.cert_key.as_ref(),
                    &peer_auth,
                    &peer_connected_for_op,
                    transport,
                )
                .await
            }
            remote::protocol::MasterHello::Destination {
                source_control_addr,
                source_data_addrs,
                server_name,
                preserve,
                peer_auth,
                dry_run,
                transport: dest_transport,
            } => {
                let cleanup = remote_cleanup.as_ref().context(
                    "destination rcpd was invoked without a RemoteCleanup supervisor (check --role)",
                )?;
                // destination doesn't use filter (filtering happens at source). Empty directory
                // cleanup decisions are communicated per-directory via keep_if_empty in the
                // Directory message
                let mut settings = config.copy_settings_base;
                settings.filter = None;
                settings.dry_run = None;
                tracing::info!("Starting destination");
                destination::run_destination_with_transport(
                    &source_control_addr,
                    &source_data_addrs,
                    &server_name,
                    &settings,
                    dry_run,
                    config.overwrite_manifest_max_entries,
                    &preserve,
                    &config.tcp_config,
                    config.concurrency,
                    config.admission,
                    config.cert_key.as_ref(),
                    &peer_auth,
                    &dest_transport,
                    &control_for_op,
                    fallback_rx,
                    cleanup,
                    &peer_connected_for_op,
                    transport,
                )
                .await
            }
        }
    });
    run_guarded_operation_and_send_result(
        operation,
        &mut master_stdin,
        &master_control,
        fallback_tx,
        &peer_connected,
    )
    .await
}

/// Run one complete post-readiness `rcpd` session over the supplied master `stdin`/`stdout`
/// streams and source↔destination `transport`.
///
/// Spawns the stdout multiplexer (prioritizing control frames ahead of tracing frames), receives
/// `MasterHello`, runs the source or destination pipeline under the master stdin heartbeat
/// watchdog, delivers `RcpdResult`, and either drains trailing tracing frames within the tracing
/// drain grace or aborts stdout immediately when master liveness failed.
pub async fn run_daemon_session<R, W>(
    config: DaemonSessionConfig,
    stdin_reader: R,
    stdout_writer: W,
    tracing_receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
    remote_cleanup: Option<remote::RemoteCleanup>,
    transport: std::sync::Arc<dyn remote::transport::Transport>,
) -> remote::protocol::RcpdResult
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let master_stdin = MasterStdinStream::new(stdin_reader, config.tcp_config.keepalive_sec);
    let master_send_stream = remote::streams::SendStream::new(stdout_writer);
    let (control_sender, control_receiver) = remote::stdio::daemon_control_channel();
    let stdout_cancel = tokio_util::sync::CancellationToken::new();
    let stdout_writer_task = {
        let cancel = stdout_cancel.clone();
        remote::AbortOnDropTask::new(tokio::spawn(async move {
            if let Err(error) = remote::stdio::run_daemon_stdout_writer(
                control_receiver,
                tracing_receiver,
                master_send_stream,
                cancel,
            )
            .await
            {
                // its receiver is gone, so reporting through tracing would lose this diagnostic
                write_stderr_best_effort(format_args!("Master stdout writer failed: {error:#}"));
            }
        }))
    };
    let outcome = run_operation(
        config,
        control_sender,
        master_stdin,
        remote_cleanup,
        transport,
    )
    .await;
    if outcome.drain_stdout {
        // control_sender was dropped when run_operation returned and RcpdResult is already
        // delivered; drain queued trailing traces within TRACING_DRAIN_GRACE
        finish_stdout_writer(stdout_writer_task, stdout_cancel).await;
    } else {
        // abort immediately when master was lost or result delivery failed so we never hang
        // waiting on a dead pipe or flush an unacknowledged frame after returning failure
        drop(stdout_writer_task);
    }
    outcome.result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn hello_preview_mode_must_match_startup_admission() {
        for preview in [false, true] {
            for dry_run in [false, true] {
                let source = remote::protocol::MasterHello::Source {
                    src: "/source".into(),
                    dst: "/destination".into(),
                    peer_auth: remote::protocol::PeerAuth::None,
                    filter: None,
                    dry_run: dry_run.then_some(common::DryRunMode::Brief),
                    capture: Default::default(),
                    accept_timeout_sec: 15,
                };
                let destination = remote::protocol::MasterHello::Destination {
                    source_control_addr: "127.0.0.1:10001".parse().unwrap(),
                    source_data_addrs: vec!["127.0.0.1:10002".parse().unwrap()],
                    server_name: "source".to_owned(),
                    preserve: Default::default(),
                    peer_auth: remote::protocol::PeerAuth::None,
                    dry_run,
                    transport: Box::new(remote::protocol::DestinationTransport::Direct),
                };
                for hello in [&source, &destination] {
                    assert_eq!(
                        validate_preview_mode(preview, hello).is_ok(),
                        preview == dry_run
                    );
                }
            }
        }
    }

    fn blocked_stdout_writer() -> (
        remote::AbortOnDropTask<()>,
        tokio_util::sync::CancellationToken,
        tokio::io::DuplexStream,
    ) {
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        drop(control_tx);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        sender
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: "rcp::source".to_owned(),
                message: "queued preview".to_owned(),
            })
            .unwrap();
        let (writer, reader) = tokio::io::duplex(1);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let token = shutdown.clone();
        let task = remote::AbortOnDropTask::new(tokio::spawn(async move {
            remote::stdio::run_daemon_stdout_writer(
                control_rx,
                receiver,
                remote::streams::SendStream::new(writer),
                token,
            )
            .await
            .unwrap();
        }));
        (task, shutdown, reader)
    }

    async fn assert_stdout_writer_was_aborted(mut reader: tokio::io::DuplexStream) {
        let mut remaining = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reader.read_to_end(&mut remaining),
        )
        .await
        .expect("the writer must release its stream after shutdown ends")
        .unwrap();
        // only the byte already buffered may remain. A detached writer resumes once we read
        // and sends the rest of the frame, which would make an EOF-only assertion pass
        assert!(
            remaining.len() <= 1,
            "writer continued writing after shutdown: {} bytes",
            remaining.len()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stdout_writer_timeout_aborts_a_blocked_write() {
        let (task, shutdown, mut reader) = blocked_stdout_writer();
        reader.read_u8().await.unwrap();
        let started = tokio::time::Instant::now();
        finish_stdout_writer(task, shutdown).await;
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(5));
        assert_stdout_writer_was_aborted(reader).await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_stdout_writer_shutdown_aborts_a_blocked_write() {
        let (task, shutdown, mut reader) = blocked_stdout_writer();
        reader.read_u8().await.unwrap();
        let mut finish = Box::pin(finish_stdout_writer(task, shutdown));
        assert!(futures::poll!(&mut finish).is_pending());
        drop(finish);
        assert_stdout_writer_was_aborted(reader).await;
    }

    #[tokio::test(start_paused = true)]
    async fn stdout_writer_shutdown_waits_for_queued_output() -> anyhow::Result<()> {
        let (task, shutdown, reader) = blocked_stdout_writer();
        let mut stream = remote::streams::RecvStream::new(reader);
        let receive = async {
            let message = stream
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await?;
            let Some(remote::protocol::RcpdToMaster::Tracing(
                common::remote_tracing::TracingMessage::Log { message, .. },
            )) = message
            else {
                panic!("shutdown must forward the queued preview");
            };
            assert_eq!(message, "queued preview");
            assert!(
                stream
                    .recv_object::<remote::protocol::RcpdToMaster>()
                    .await?
                    .is_none()
            );
            Ok::<_, anyhow::Error>(())
        };
        let ((), received) = tokio::join!(finish_stdout_writer(task, shutdown), receive);
        received
    }

    struct HeldCapture(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for HeldCapture {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    async fn operation_holding_children(
        dropped: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        cancel: tokio::sync::oneshot::Sender<()>,
    ) -> anyhow::Result<(String, common::copy::Summary)> {
        let mut children = tokio::task::JoinSet::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let started_capture = HeldCapture(dropped.clone());
        common::task_scope::spawn_tracked(&mut children, async move {
            let _capture = started_capture;
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();
        let unpolled_capture = HeldCapture(dropped);
        common::task_scope::spawn_tracked(&mut children, async move {
            let _capture = unpolled_capture;
            std::future::pending::<()>().await;
        });
        let _ = cancel.send(());
        std::future::pending().await
    }

    #[tokio::test]
    async fn stdin_eof_waits_for_started_and_unpolled_captures() {
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 0);
        let closer = tokio::spawn(async move {
            let _ = cancelled.await;
            drop(master_stdin_tx);
        });
        let (control_tx, _control_rx) = remote::stdio::daemon_control_channel();
        let operation =
            common::task_scope::scope_tasks(operation_holding_children(dropped.clone(), cancel));
        let peer_connected = std::sync::atomic::AtomicBool::new(false);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_guarded_operation_and_send_result(
                operation,
                &mut master_stdin,
                &control_tx,
                None,
                &peer_connected,
            ),
        )
        .await
        .expect("stdin cancellation must drain child captures");
        closer.await.unwrap();
        assert!(outcome.drain_stdout);
        assert!(
            matches!(outcome.result, remote::protocol::RcpdResult::Failure { error, .. } if error.contains("master closed its stdin control channel"))
        );
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn completed_operation_delivers_result_even_when_master_closes_stdin_immediately() {
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(4096);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (_tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await
                .unwrap();
            }))
        };
        let master = tokio::spawn(async move {
            let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
            let frame = stdout
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await
                .unwrap()
                .expect("master must receive RcpdResult");
            // close daemon stdin the instant RcpdResult is decoded
            drop(master_stdin_tx);
            frame
        });
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 5);
        let peer_connected = std::sync::atomic::AtomicBool::new(false);
        let outcome = run_guarded_operation_and_send_result(
            async {
                Ok((
                    "copy complete".to_string(),
                    common::copy::Summary::default(),
                ))
            },
            &mut master_stdin,
            &control_tx,
            None,
            &peer_connected,
        )
        .await;
        drop(control_tx);
        assert!(outcome.drain_stdout);
        finish_stdout_writer(writer_task, stdout_cancel).await;
        assert!(matches!(
            outcome.result,
            remote::protocol::RcpdResult::Success { ref message, .. } if message == "copy complete"
        ));
        assert!(matches!(
            master.await.unwrap(),
            remote::protocol::RcpdToMaster::Result(remote::protocol::RcpdResult::Success {
                message,
                ..
            }) if message == "copy complete"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn deliver_result_waits_past_trace_grace_while_master_heartbeats_continue() {
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(64);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        // enqueue an in-flight trace larger than the 64-byte duplex buffer so stdout blocks
        tracing_tx
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: "rcpd".to_owned(),
                message: "b".repeat(512),
            })
            .unwrap();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await
                .unwrap();
            }))
        };
        // let the writer start writing the in-flight trace and park on stdout backpressure
        tokio::task::yield_now().await;
        let master = tokio::spawn(async move {
            let mut stdin = remote::streams::SendStream::new(master_stdin_tx);
            // keep heartbeating across more than 2x TRACING_DRAIN_GRACE (12s > 5s) before reading
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                stdin
                    .send_control_message(&remote::protocol::MasterToRcpd::Heartbeat)
                    .await
                    .unwrap();
            }
            let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
            let mut final_result = None;
            while let Some(frame) = stdout
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await
                .unwrap()
            {
                if let remote::protocol::RcpdToMaster::Result(res) = frame {
                    final_result = Some(res);
                    drop(stdin);
                    break;
                }
            }
            final_result.expect("master must receive RcpdResult after resuming reads")
        });
        let started = tokio::time::Instant::now();
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 30);
        let peer_connected = std::sync::atomic::AtomicBool::new(false);
        let outcome = run_guarded_operation_and_send_result(
            async {
                Ok((
                    "copy complete".to_string(),
                    common::copy::Summary::default(),
                ))
            },
            &mut master_stdin,
            &control_tx,
            None,
            &peer_connected,
        )
        .await;
        assert!(started.elapsed() >= std::time::Duration::from_secs(12));
        assert!(outcome.drain_stdout);
        drop(control_tx);
        finish_stdout_writer(writer_task, stdout_cancel).await;
        assert!(matches!(
            outcome.result,
            remote::protocol::RcpdResult::Success { ref message, .. } if message == "copy complete"
        ));
        assert!(matches!(
            master.await.unwrap(),
            remote::protocol::RcpdResult::Success { message, .. } if message == "copy complete"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn deliver_result_with_zero_keepalive_waits_past_trace_grace_until_reader_resumes() {
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(64);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        tracing_tx
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: "rcpd".to_owned(),
                message: "z".repeat(512),
            })
            .unwrap();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await
                .unwrap();
            }))
        };
        tokio::task::yield_now().await;
        let master = tokio::spawn(async move {
            let _keep_stdin_open = master_stdin_tx;
            // with keepalive disabled (0), no heartbeats are sent and reads pause for 60s
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
            while let Some(frame) = stdout
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await
                .unwrap()
            {
                if let remote::protocol::RcpdToMaster::Result(res) = frame {
                    return res;
                }
            }
            panic!("master must receive RcpdResult when keepalive is disabled");
        });
        let started = tokio::time::Instant::now();
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 0);
        let peer_connected = std::sync::atomic::AtomicBool::new(false);
        let outcome = run_guarded_operation_and_send_result(
            async {
                Ok((
                    "copy complete".to_string(),
                    common::copy::Summary::default(),
                ))
            },
            &mut master_stdin,
            &control_tx,
            None,
            &peer_connected,
        )
        .await;
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(60));
        assert!(outcome.drain_stdout);
        drop(control_tx);
        finish_stdout_writer(writer_task, stdout_cancel).await;
        assert!(matches!(
            outcome.result,
            remote::protocol::RcpdResult::Success { ref message, .. } if message == "copy complete"
        ));
        assert!(matches!(
            master.await.unwrap(),
            remote::protocol::RcpdResult::Success { message, .. } if message == "copy complete"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn deliver_result_honors_keepalive_timeout_and_aborts_without_leaking_success() {
        let (_master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(64);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        tracing_tx
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: "rcpd".to_owned(),
                message: "s".repeat(512),
            })
            .unwrap();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                let _ = remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await;
            }))
        };
        tokio::task::yield_now().await;
        let started = tokio::time::Instant::now();
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 30);
        let peer_connected = std::sync::atomic::AtomicBool::new(false);
        let outcome = run_guarded_operation_and_send_result(
            async {
                Ok((
                    "copy complete".to_string(),
                    common::copy::Summary::default(),
                ))
            },
            &mut master_stdin,
            &control_tx,
            None,
            &peer_connected,
        )
        .await;
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(30));
        assert!(!outcome.drain_stdout);
        assert!(
            matches!(outcome.result, remote::protocol::RcpdResult::Failure { ref error, .. } if error.contains("timed out after 30s while sending result")),
            "unexpected outcome: {:?}",
            outcome.result
        );
        // match run_daemon_session: abort the writer immediately when drain_stdout is false
        drop(control_tx);
        drop(writer_task);
        // draining whatever was in the pipe must never yield Success
        let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
        while let Ok(Some(frame)) = stdout.recv_object::<remote::protocol::RcpdToMaster>().await {
            assert!(
                !matches!(
                    frame,
                    remote::protocol::RcpdToMaster::Result(
                        remote::protocol::RcpdResult::Success { .. }
                    )
                ),
                "aborted delivery must never leak RcpdResult::Success"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pre_hello_eof_drains_queued_startup_notice_when_reader_resumes_during_grace() {
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(64);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        let expected_notice = format!(
            "Chrome trace will be written to: /tmp/trace-{}.json",
            "x".repeat(128)
        );
        // queue a startup notice larger than the 64-byte duplex buffer so stdout is backpressured
        tracing_tx
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: common::NOTICE_TARGET.to_owned(),
                message: expected_notice.clone(),
            })
            .unwrap();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await
                .unwrap();
            }))
        };
        // park the stdout writer on the backpressured pipe, then close stdin before MasterHello
        tokio::task::yield_now().await;
        drop(master_stdin_tx);
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 5);
        let outcome = receive_master_hello(&mut master_stdin)
            .await
            .expect_err("EOF before MasterHello must fail startup");
        assert!(
            outcome.drain_stdout,
            "pre-hello EOF must drain queued startup notices within TRACING_DRAIN_GRACE"
        );
        assert!(
            matches!(
                outcome.result,
                remote::protocol::RcpdResult::Failure { ref error, .. }
                    if error.contains("closed its stdin control channel before sending its hello")
            ),
            "unexpected pre-hello EOF result: {:?}",
            outcome.result
        );
        // drop control_tx (as run_operation returning does) and start draining while the reader
        // stays stalled for 100ms before resuming within the 5-second drain grace
        drop(control_tx);
        let reader = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
            let mut notices = Vec::new();
            while let Some(frame) = stdout
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await
                .unwrap()
            {
                if let remote::protocol::RcpdToMaster::Tracing(
                    common::remote_tracing::TracingMessage::Log { message, .. },
                ) = frame
                {
                    notices.push(message);
                }
            }
            notices
        });
        finish_stdout_writer(writer_task, stdout_cancel).await;
        let notices = reader.await.unwrap();
        assert_eq!(notices, vec![expected_notice]);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_keepalive_never_times_out_and_exits_on_eof() {
        let (writer, reader) = tokio::io::duplex(4096);
        let mut send_stream = remote::streams::SendStream::new(writer);
        let mut master_stdin = MasterStdinStream::new(reader, 0);
        let sender = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::Hello(Box::new(
                    remote::protocol::MasterHello::Source {
                        src: "/src".into(),
                        dst: "/dst".into(),
                        peer_auth: remote::protocol::PeerAuth::None,
                        filter: None,
                        dry_run: None,
                        capture: Default::default(),
                        accept_timeout_sec: 15,
                    },
                )))
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            drop(send_stream);
        });
        let started = tokio::time::Instant::now();
        let _hello = receive_master_hello(&mut master_stdin).await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(600));
        let loss = master_stdin_watchdog(&mut master_stdin, None).await;
        sender.await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(1200));
        assert_eq!(loss.cause, "master closed its stdin control channel");
        assert!(loss.drain_stdout);
    }

    #[tokio::test(start_paused = true)]
    async fn recv_master_hello_keeps_advancing_large_frame_alive_and_times_out_if_stalled() {
        use tokio::io::AsyncWriteExt as _;
        let mut filter = common::filter::FilterSettings::default();
        for i in 0..200 {
            filter.add_include(&format!("dir_{i}/**/*.txt")).unwrap();
            filter.add_exclude(&format!("tmp_{i}/**/*.log")).unwrap();
        }
        let expected_hello = remote::protocol::MasterHello::Source {
            src: "/src".into(),
            dst: "/dst".into(),
            peer_auth: remote::protocol::PeerAuth::None,
            filter: Some(filter),
            dry_run: None,
            capture: Default::default(),
            accept_timeout_sec: 15,
        };
        let mut encoded = Vec::new();
        remote::streams::SendStream::new(&mut encoded)
            .send_control_message(&remote::protocol::MasterToRcpd::Hello(Box::new(
                expected_hello.clone(),
            )))
            .await
            .unwrap();
        assert!(encoded.len() > 400);
        // drip the single MasterHello frame in 4 chunks spaced 3s apart under a 5s keepalive
        // budget (total 9s > 5s): read progress must refresh the idle deadline
        let (mut writer, reader) = tokio::io::duplex(4096);
        let mut master_stdin = MasterStdinStream::new(reader, 5);
        let encoded_chunks = encoded.clone();
        let sender = tokio::spawn(async move {
            let chunk_size = encoded_chunks.len().div_ceil(4);
            for (idx, chunk) in encoded_chunks.chunks(chunk_size).enumerate() {
                if idx > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                }
                writer.write_all(chunk).await.unwrap();
                writer.flush().await.unwrap();
            }
            writer
        });
        let started = tokio::time::Instant::now();
        let received = receive_master_hello(&mut master_stdin).await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(9));
        assert_eq!(format!("{received:?}"), format!("{expected_hello:?}"));
        let _keep_open = sender.await.unwrap();
        // a peer that stalls mid-frame for the full keepalive budget must time out
        let (mut stalled_writer, stalled_reader) = tokio::io::duplex(4096);
        let mut stalled_stdin = MasterStdinStream::new(stalled_reader, 5);
        let stalled_sender = tokio::spawn(async move {
            stalled_writer.write_all(&encoded[..16]).await.unwrap();
            stalled_writer.flush().await.unwrap();
            stalled_writer
        });
        let stalled_start = tokio::time::Instant::now();
        let outcome = receive_master_hello(&mut stalled_stdin)
            .await
            .expect_err("mid-frame stall must time out");
        assert_eq!(stalled_start.elapsed(), std::time::Duration::from_secs(5));
        assert!(!outcome.drain_stdout);
        assert!(
            matches!(
                outcome.result,
                remote::protocol::RcpdResult::Failure { ref error, .. }
                    if error.contains("timed out after 5s waiting for hello")
            ),
            "unexpected stall outcome: {:?}",
            outcome.result
        );
        let _keep_stalled = stalled_sender.await.unwrap();
        // a completely silent peer must also time out after one budget
        let (_silent_writer, silent_reader) = tokio::io::duplex(4096);
        let mut silent_stdin = MasterStdinStream::new(silent_reader, 5);
        let silent_start = tokio::time::Instant::now();
        let outcome = receive_master_hello(&mut silent_stdin)
            .await
            .expect_err("silent peer must time out");
        assert_eq!(silent_start.elapsed(), std::time::Duration::from_secs(5));
        assert!(!outcome.drain_stdout);
        assert!(
            matches!(
                outcome.result,
                remote::protocol::RcpdResult::Failure { ref error, .. }
                    if error.contains("timed out after 5s waiting for hello")
            ),
            "unexpected silent outcome: {:?}",
            outcome.result
        );
    }

    #[tokio::test(start_paused = true)]
    async fn recv_master_hello_ignores_heartbeats_and_honors_keepalive_timeout() {
        let (writer, reader) = tokio::io::duplex(4096);
        let mut send_stream = remote::streams::SendStream::new(writer);
        let mut master_stdin = MasterStdinStream::new(reader, 5);
        let expected_hello = remote::protocol::MasterHello::Source {
            src: "/src".into(),
            dst: "/dst".into(),
            peer_auth: remote::protocol::PeerAuth::None,
            filter: None,
            dry_run: None,
            capture: Default::default(),
            accept_timeout_sec: 15,
        };
        let hello_to_send = expected_hello.clone();
        let sender = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::Heartbeat)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::Hello(Box::new(
                    hello_to_send,
                )))
                .await
                .unwrap();
            send_stream
        });
        let received = receive_master_hello(&mut master_stdin).await.unwrap();
        assert_eq!(format!("{received:?}"), format!("{expected_hello:?}"));
        let _keep_open = sender.await.unwrap();
        // silence longer than keepalive_sec triggers timeout in the post-hello watchdog
        let started = tokio::time::Instant::now();
        let loss = master_stdin_watchdog(&mut master_stdin, None).await;
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(5));
        assert!(!loss.drain_stdout);
        assert!(
            loss.cause
                .contains("timed out after 5s without a heartbeat"),
            "unexpected watchdog cause: {}",
            loss.cause
        );
    }

    #[tokio::test(start_paused = true)]
    async fn master_stdin_watchdog_refreshes_on_heartbeats_and_exits_on_eof() {
        let (writer, reader) = tokio::io::duplex(4096);
        let mut send_stream = remote::streams::SendStream::new(writer);
        let mut master_stdin = MasterStdinStream::new(reader, 3);
        let sender = tokio::spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                send_stream
                    .send_control_message(&remote::protocol::MasterToRcpd::Heartbeat)
                    .await
                    .unwrap();
            }
            drop(send_stream);
        });
        let started = tokio::time::Instant::now();
        let loss = master_stdin_watchdog(&mut master_stdin, None).await;
        sender.await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(6));
        assert_eq!(loss.cause, "master closed its stdin control channel");
        assert!(loss.drain_stdout);
    }

    #[tokio::test(start_paused = true)]
    async fn master_stdin_watchdog_routes_ssh_fallback_endpoints_alongside_heartbeats() {
        let (writer, reader) = tokio::io::duplex(4096);
        let mut send_stream = remote::streams::SendStream::new(writer);
        let mut master_stdin = MasterStdinStream::new(reader, 5);
        let expected_control: std::net::SocketAddr = "127.0.0.1:46001".parse().unwrap();
        let expected_data: Vec<std::net::SocketAddr> = vec![
            "127.0.0.1:46002".parse().unwrap(),
            "127.0.0.1:46003".parse().unwrap(),
        ];
        let data_to_send = expected_data.clone();
        let sender = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::Heartbeat)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::SshFallbackEndpoints {
                    control_addr: expected_control,
                    data_addrs: data_to_send,
                })
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::Heartbeat)
                .await
                .unwrap();
            drop(send_stream);
        });
        let (fallback_tx, fallback_rx) = tokio::sync::oneshot::channel();
        let (endpoints, loss) = tokio::join!(
            fallback_rx,
            master_stdin_watchdog(&mut master_stdin, Some(fallback_tx)),
        );
        sender.await.unwrap();
        let endpoints = endpoints.expect("watchdog must deliver fallback endpoints");
        assert_eq!(endpoints.control_addr, expected_control);
        assert_eq!(endpoints.data_addrs, expected_data);
        assert_eq!(loss.cause, "master closed its stdin control channel");
        assert!(loss.drain_stdout);
    }
}
