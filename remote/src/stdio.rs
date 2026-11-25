//! Framed SSH stdio transport and liveness helpers for master↔`rcpd` communication.
//!
//! Master (`rcp`) and each `rcpd` communicate exclusively over the SSH exec channel's
//! stdin and stdout pipes. `stderr` remains reserved for the initial readiness/refusal
//! record (`RCP_READY` or `RCP_ERROR`) and subsequent diagnostic output.

use anyhow::Context as _;
use std::os::fd::{AsRawFd, FromRawFd};

/// Maximum number of `ServerAliveInterval` periods before OpenSSH declares a silent host dead.
///
/// OpenSSH increments its unanswered-probe counter before checking `> ServerAliveCountMax`, so
/// `(interval, count)` disconnects after `interval * (count + 1)` silent seconds. Capping the
/// total periods at 6 keeps `ServerAliveCountMax <= 5` and matches standard budgets exactly
/// (for example `120s -> (20, 5) -> 20 * 6 = 120s`, `5s -> (1, 4) -> 1 * 5 = 5s`).
const SSH_SERVER_ALIVE_MAX_INTERVALS: u64 = 6;

/// Minimum number of `ServerAliveInterval` periods (`ServerAliveCountMax >= 1`) so OpenSSH
/// always sends at least one probe before closing an idle healthy connection.
const SSH_SERVER_ALIVE_MIN_INTERVALS: u64 = 2;

/// Upper bound OpenSSH accepts for `ServerAliveInterval` (`INT_MAX` seconds).
const MAX_SSH_SERVER_ALIVE_INTERVAL: u64 = i32::MAX as u64;

/// Compute OpenSSH `(ServerAliveInterval, ServerAliveCountMax)` from `--remote-keepalive-sec`.
///
/// Returns `None` when `keepalive_sec == 0` (dead-peer detection disabled). For nonzero budgets,
/// OpenSSH disconnects after `interval * (count + 1)` silent seconds with `1 <= count <= 5` and
/// `interval <= i32::MAX` (OpenSSH's upper bound for `ServerAliveInterval`). Because OpenSSH
/// requires `ServerAliveInterval >= 1` and `ServerAliveCountMax >= 1` to probe before
/// disconnecting, `keepalive_sec == 1` maps to `(1, 1)` (an effective 2-second OpenSSH bound).
#[must_use]
pub(crate) fn ssh_server_alive_params(keepalive_sec: u64) -> Option<(u64, u64)> {
    if keepalive_sec == 0 {
        return None;
    }
    let interval = keepalive_sec
        .div_ceil(SSH_SERVER_ALIVE_MAX_INTERVALS)
        .min(MAX_SSH_SERVER_ALIVE_INTERVAL);
    let total_intervals = keepalive_sec.div_ceil(interval).clamp(
        SSH_SERVER_ALIVE_MIN_INTERVALS,
        SSH_SERVER_ALIVE_MAX_INTERVALS,
    );
    Some((interval, total_intervals - 1))
}

/// Compute the interval between master→`rcpd` stdin heartbeat frames from `--remote-keepalive-sec`.
///
/// Returns `None` when `keepalive_sec == 0`. Otherwise fires every `keepalive_sec / 3` so a
/// healthy session refreshes the daemon watchdog three times per budget.
#[must_use]
fn master_heartbeat_interval(keepalive_sec: u64) -> Option<std::time::Duration> {
    if keepalive_sec == 0 {
        return None;
    }
    Some(std::time::Duration::from_secs(keepalive_sec) / 3)
}

struct NonBlockingFd(std::os::fd::OwnedFd);

impl AsRawFd for NonBlockingFd {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.0.as_raw_fd()
    }
}

fn dup_cloexec(fd: std::os::fd::RawFd) -> std::io::Result<std::os::fd::OwnedFd> {
    loop {
        // SAFETY: `F_DUPFD_CLOEXEC` duplicates an existing descriptor atomically with `FD_CLOEXEC`.
        // Minimum fd 3 keeps private protocol descriptors out of the 0/1/2 stdio slots even if a
        // standard descriptor was closed before invocation.
        let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, libc::STDERR_FILENO + 1) };
        if duplicated >= 0 {
            // SAFETY: `duplicated` is a newly created valid file descriptor owned by this call.
            return Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(duplicated) });
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn dup2_retry(source: std::os::fd::RawFd, target: std::os::fd::RawFd) -> std::io::Result<()> {
    loop {
        // SAFETY: `source` and `target` are valid descriptors in this process.
        let result = unsafe { libc::dup2(source, target) };
        if result >= 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn open_dev_null_readonly() -> std::io::Result<std::os::fd::OwnedFd> {
    loop {
        // SAFETY: `c"/dev/null"` is a valid NUL-terminated static C string.
        let fd = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
        if fd >= 0 {
            // SAFETY: `fd` is a newly opened valid file descriptor owned by this call.
            return Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) });
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

enum FdKind {
    Pipe,
    Socket,
    Other,
}

fn inspect_fd_kind(fd: std::os::fd::RawFd) -> std::io::Result<FdKind> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `stat.as_mut_ptr()` points to valid storage for `libc::stat`.
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `fstat` succeeded and initialized `stat`.
    let mode = unsafe { stat.assume_init().st_mode } & libc::S_IFMT;
    Ok(match mode {
        libc::S_IFIFO => FdKind::Pipe,
        libc::S_IFSOCK => FdKind::Socket,
        _ => FdKind::Other,
    })
}

fn set_nonblocking(fd: std::os::fd::RawFd) -> std::io::Result<()> {
    // SAFETY: `F_GETFL` reads status flags of a live descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `F_SETFL` sets `O_NONBLOCK` on a dedicated pipe/socketpair descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

enum StdioReaderInner {
    NonBlocking(tokio::io::unix::AsyncFd<NonBlockingFd>),
    File(tokio::fs::File),
}

/// Async reader for `rcpd`'s protocol stdin descriptor.
///
/// Uses non-blocking `AsyncFd` on pipes and socketpairs (the SSH exec channel path) so
/// cancelled reads never leave a blocking-pool thread parked in `read(2)`. Falls back to
/// `tokio::fs::File` for non-pollable descriptors such as `/dev/null` or regular files.
pub struct StdioReader(StdioReaderInner);

impl StdioReader {
    fn from_owned_fd(fd: std::os::fd::OwnedFd) -> std::io::Result<Self> {
        match inspect_fd_kind(fd.as_raw_fd())? {
            FdKind::Pipe | FdKind::Socket => {
                set_nonblocking(fd.as_raw_fd())?;
                Ok(Self(StdioReaderInner::NonBlocking(
                    tokio::io::unix::AsyncFd::with_interest(
                        NonBlockingFd(fd),
                        tokio::io::Interest::READABLE,
                    )?,
                )))
            }
            FdKind::Other => Ok(Self(StdioReaderInner::File(tokio::fs::File::from_std(
                std::fs::File::from(fd),
            )))),
        }
    }
}

impl tokio::io::AsyncRead for StdioReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return std::task::Poll::Ready(Ok(()));
        }
        match &mut self.0 {
            StdioReaderInner::NonBlocking(async_fd) => loop {
                let mut guard = std::task::ready!(async_fd.poll_read_ready(cx))?;
                match guard.try_io(|inner| {
                    let slice = buf.initialize_unfilled();
                    loop {
                        // SAFETY: `inner` holds a live descriptor and `slice` is valid writable
                        // memory for `slice.len()` bytes.
                        let read = unsafe {
                            libc::read(
                                inner.get_ref().as_raw_fd(),
                                slice.as_mut_ptr().cast(),
                                slice.len(),
                            )
                        };
                        if read >= 0 {
                            return Ok(read as usize);
                        }
                        let error = std::io::Error::last_os_error();
                        if error.kind() != std::io::ErrorKind::Interrupted {
                            return Err(error);
                        }
                    }
                }) {
                    Ok(Ok(count)) => {
                        buf.advance(count);
                        return std::task::Poll::Ready(Ok(()));
                    }
                    Ok(Err(error)) => return std::task::Poll::Ready(Err(error)),
                    Err(_would_block) => continue,
                }
            },
            StdioReaderInner::File(file) => std::pin::Pin::new(file).poll_read(cx, buf),
        }
    }
}

enum StdioWriterInner {
    NonBlocking {
        async_fd: Option<tokio::io::unix::AsyncFd<NonBlockingFd>>,
        is_socket: bool,
    },
    File(Option<tokio::fs::File>),
}

/// Async writer for `rcpd`'s protocol stdout descriptor.
///
/// Uses non-blocking `AsyncFd` on pipes and socketpairs so timed-out shutdown drains abort
/// immediately without leaving a blocking-pool thread stuck in `write(2)`. Closing via
/// `AsyncWriteExt::shutdown` shuts down socket write halves and drops the owned descriptor,
/// delivering immediate EOF to the peer.
pub struct StdioWriter(StdioWriterInner);

impl StdioWriter {
    fn from_owned_fd(fd: std::os::fd::OwnedFd) -> std::io::Result<Self> {
        match inspect_fd_kind(fd.as_raw_fd())? {
            kind @ (FdKind::Pipe | FdKind::Socket) => {
                set_nonblocking(fd.as_raw_fd())?;
                Ok(Self(StdioWriterInner::NonBlocking {
                    async_fd: Some(tokio::io::unix::AsyncFd::with_interest(
                        NonBlockingFd(fd),
                        tokio::io::Interest::WRITABLE,
                    )?),
                    is_socket: matches!(kind, FdKind::Socket),
                }))
            }
            FdKind::Other => Ok(Self(StdioWriterInner::File(Some(
                tokio::fs::File::from_std(std::fs::File::from(fd)),
            )))),
        }
    }
}

impl tokio::io::AsyncWrite for StdioWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            return std::task::Poll::Ready(Ok(0));
        }
        match &mut self.0 {
            StdioWriterInner::NonBlocking { async_fd, .. } => {
                let Some(async_fd) = async_fd.as_ref() else {
                    return std::task::Poll::Ready(Err(std::io::Error::from(
                        std::io::ErrorKind::BrokenPipe,
                    )));
                };
                loop {
                    let mut guard = std::task::ready!(async_fd.poll_write_ready(cx))?;
                    match guard.try_io(|inner| {
                        loop {
                            // SAFETY: `inner` holds a live descriptor and `buf` is valid readable
                            // memory for `buf.len()` bytes.
                            let written = unsafe {
                                libc::write(
                                    inner.get_ref().as_raw_fd(),
                                    buf.as_ptr().cast(),
                                    buf.len(),
                                )
                            };
                            if written >= 0 {
                                return Ok(written as usize);
                            }
                            let error = std::io::Error::last_os_error();
                            if error.kind() != std::io::ErrorKind::Interrupted {
                                return Err(error);
                            }
                        }
                    }) {
                        Ok(Ok(count)) => return std::task::Poll::Ready(Ok(count)),
                        Ok(Err(error)) => return std::task::Poll::Ready(Err(error)),
                        Err(_would_block) => continue,
                    }
                }
            }
            StdioWriterInner::File(slot) => {
                let Some(file) = slot.as_mut() else {
                    return std::task::Poll::Ready(Err(std::io::Error::from(
                        std::io::ErrorKind::BrokenPipe,
                    )));
                };
                std::pin::Pin::new(file).poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.0 {
            StdioWriterInner::NonBlocking { .. } => std::task::Poll::Ready(Ok(())),
            StdioWriterInner::File(Some(file)) => std::pin::Pin::new(file).poll_flush(cx),
            StdioWriterInner::File(None) => std::task::Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.0 {
            StdioWriterInner::NonBlocking {
                async_fd,
                is_socket,
            } => {
                if let Some(fd) = async_fd.take() {
                    if *is_socket {
                        // shut down the write half explicitly so even a shared bidirectional
                        // socketpair delivers EOF immediately while stdin stays open
                        // SAFETY: `fd` holds a live socket descriptor.
                        let _ = unsafe { libc::shutdown(fd.get_ref().as_raw_fd(), libc::SHUT_WR) };
                    }
                    drop(fd);
                }
                std::task::Poll::Ready(Ok(()))
            }
            StdioWriterInner::File(slot) => {
                if let Some(file) = slot.as_mut() {
                    std::task::ready!(std::pin::Pin::new(file).poll_shutdown(cx))?;
                }
                *slot = None;
                std::task::Poll::Ready(Ok(()))
            }
        }
    }
}

/// Claim exclusive ownership of `rcpd`'s stdin and stdout for binary protocol framing.
///
/// Duplicates fd 0 and fd 1 into private `CLOEXEC` descriptors (>= 3), redirects fd 1 to `stderr`
/// (flushing any stray buffered stdout bytes there instead of into the binary protocol stream),
/// and redirects fd 0 to `/dev/null` so dropping [`StdioReader`] and [`StdioWriter`] releases the
/// sole references to the SSH channel pipes.
///
/// Call this only after emitting the `RCP_READY` line on `stderr`.
///
/// # Errors
///
/// Returns an error if duplicating descriptors, redirecting fd 0/fd 1, or registering non-blocking
/// stdio fails.
pub fn take_protocol_stdio() -> anyhow::Result<(StdioReader, StdioWriter)> {
    use std::io::Write as _;
    let stdin_fd = dup_cloexec(libc::STDIN_FILENO).context("failed to duplicate stdin")?;
    let stdout_fd = dup_cloexec(libc::STDOUT_FILENO).context("failed to duplicate stdout")?;
    dup2_retry(libc::STDERR_FILENO, libc::STDOUT_FILENO)
        .context("failed to redirect process stdout to stderr")?;
    // flush any stray buffered Rust stdout after redirecting fd 1 so it lands on stderr rather
    // than corrupting the first binary frame; ignore BrokenPipe if stderr is already closed
    if let Err(error) = std::io::stdout().flush()
        && error.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(error).context("failed to flush redirected stdout");
    }
    let dev_null = open_dev_null_readonly().context("failed to open /dev/null for stdin")?;
    dup2_retry(dev_null.as_raw_fd(), libc::STDIN_FILENO)
        .context("failed to redirect process stdin to /dev/null")?;
    drop(dev_null);
    let reader = StdioReader::from_owned_fd(stdin_fd).context("failed to initialize stdin")?;
    let writer = StdioWriter::from_owned_fd(stdout_fd).context("failed to initialize stdout")?;
    Ok((reader, writer))
}

struct ControlOutbound {
    message: crate::protocol::RcpdToMaster,
    ack: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
}

/// Cloneable sender for high-priority `rcpd`→master control frames (`SourceHello`,
/// `DirectConnectFailed`, `RcpdResult`).
#[derive(Clone)]
pub struct DaemonControlSender {
    tx: tokio::sync::mpsc::UnboundedSender<ControlOutbound>,
}

/// Receiving half paired with [`DaemonControlSender`], consumed by [`run_daemon_stdout_writer`].
pub struct DaemonControlReceiver {
    rx: tokio::sync::mpsc::UnboundedReceiver<ControlOutbound>,
}

/// Create the daemon-side control channel feeding [`run_daemon_stdout_writer`].
#[must_use]
pub fn daemon_control_channel() -> (DaemonControlSender, DaemonControlReceiver) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (DaemonControlSender { tx }, DaemonControlReceiver { rx })
}

impl DaemonControlSender {
    /// Send the source daemon's TCP listener addresses to the master and wait until flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if the stdout writer stopped or failed while flushing the frame.
    pub async fn send_source_hello(
        &self,
        hello: crate::protocol::SourceMasterHello,
    ) -> anyhow::Result<()> {
        self.send(crate::protocol::RcpdToMaster::SourceHello(hello))
            .await
    }

    /// Notify the master that the destination's `auto` direct-TCP probe failed and wait until
    /// the frame is flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if the stdout writer stopped or failed while flushing the frame.
    pub async fn send_direct_connect_failed(&self, reason: String) -> anyhow::Result<()> {
        self.send(crate::protocol::RcpdToMaster::DirectConnectFailed { reason })
            .await
    }

    /// Send the final [`crate::protocol::RcpdResult`] to the master and wait until flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if the stdout writer stopped or failed while flushing the frame.
    pub async fn send_result(&self, result: crate::protocol::RcpdResult) -> anyhow::Result<()> {
        self.send(crate::protocol::RcpdToMaster::Result(result))
            .await
    }

    async fn send(&self, message: crate::protocol::RcpdToMaster) -> anyhow::Result<()> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(ControlOutbound {
                message,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("master stdout writer is no longer running"))?;
        ack_rx
            .await
            .context("master stdout writer exited before confirming frame flush")?
    }
}

enum NextOutbound {
    Control(ControlOutbound),
    Tracing(common::remote_tracing::TracingMessage),
}

/// Multiplex daemon control and tracing messages onto `rcpd`'s framed SSH stdout stream.
///
/// Control messages (`SourceHello`, `DirectConnectFailed`, `RcpdResult`) take priority over tracing
/// events. When `shutdown` is cancelled, admission closes on both channels (even if producers still
/// retain senders), any in-flight frame finishes intact, already-queued messages drain in order,
/// and the stream closes cleanly.
///
/// # Errors
///
/// Returns an error if writing a frame or closing the stream fails.
pub async fn run_daemon_stdout_writer<W: tokio::io::AsyncWrite + Unpin + Send>(
    mut control: DaemonControlReceiver,
    mut tracing_rx: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
    mut send_stream: crate::streams::SendStream<W>,
    shutdown: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let mut control_open = true;
    let mut tracing_open = true;
    let mut closing = false;
    while control_open || tracing_open {
        let next = tokio::select! {
            biased;
            _ = shutdown.cancelled(), if !closing => {
                control.rx.close();
                tracing_rx.close();
                closing = true;
                continue;
            }
            msg = control.rx.recv(), if control_open => match msg {
                Some(outbound) => NextOutbound::Control(outbound),
                None => {
                    control_open = false;
                    continue;
                }
            },
            msg = tracing_rx.recv(), if tracing_open => match msg {
                Some(tracing_msg) => NextOutbound::Tracing(tracing_msg),
                None => {
                    tracing_open = false;
                    continue;
                }
            },
        };
        match next {
            NextOutbound::Control(outbound) => {
                if outbound.ack.is_closed() {
                    // caller already abandoned this control message; do not leak a stale frame
                    continue;
                }
                match send_stream.send_control_message(&outbound.message).await {
                    Ok(()) => {
                        let _ = outbound.ack.send(Ok(()));
                    }
                    Err(error) => {
                        // preserve the full error chain for the caller awaiting the flush ack;
                        // if the caller already left, surface it from the writer task instead
                        if let Err(Err(error)) = outbound.ack.send(Err(error)) {
                            return Err(error.context("failed to send control message to master"));
                        }
                        return Ok(());
                    }
                }
            }
            NextOutbound::Tracing(tracing_msg) => {
                let frame = crate::protocol::RcpdToMaster::Tracing(tracing_msg);
                let bytes = match bitcode::serialize(&frame) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        // discard unserializable optional traces before touching the shared sink;
                        // never emit through tracing here because we own the tracing receiver
                        let _ = std::io::Write::write_fmt(
                            &mut std::io::stderr(),
                            format_args!("Dropping unserializable remote trace frame: {error:#}\n"),
                        );
                        continue;
                    }
                };
                let max_len = send_stream.max_frame_length();
                if bytes.len() > max_len {
                    let _ = std::io::Write::write_fmt(
                        &mut std::io::stderr(),
                        format_args!(
                            "Dropping oversized remote trace frame ({} bytes > {} byte frame limit)\n",
                            bytes.len(),
                            max_len,
                        ),
                    );
                    continue;
                }
                send_stream
                    .send_serialized_frame(bytes::Bytes::from(bytes))
                    .await
                    .context("failed to forward tracing message")?;
            }
        }
    }
    send_stream
        .close()
        .await
        .context("failed to close master stdout stream")?;
    Ok(())
}

/// Control messages demuxed from `rcpd`'s framed SSH stdout stream.
#[derive(Debug)]
enum MasterControlMessage {
    SourceHello(crate::protocol::SourceMasterHello),
    DirectConnectFailed(String),
    Result(crate::protocol::RcpdResult),
}

/// First control frame received from the source daemon after `MasterHello::Source`.
#[derive(Debug)]
pub enum SourceHelloOutcome {
    /// Normal handshake: source created its listeners and sent their addresses.
    Hello(crate::protocol::SourceMasterHello),
    /// Source failed (or finished) before sending `SourceMasterHello` and reported its result.
    Result(crate::protocol::RcpdResult),
}

/// Control frame received from the destination daemon after `MasterHello::Destination`.
#[derive(Debug)]
pub enum DestinationControlEvent {
    /// Destination failed its `auto` direct-TCP probe and requests master-managed SSH tunnels.
    DirectConnectFailed { reason: String },
    /// Final destination daemon result.
    Result(Box<crate::protocol::RcpdResult>),
}

/// Master-side receiver for control frames demuxed from one `rcpd`'s stdout stream.
pub struct MasterControlReceiver {
    rx: tokio::sync::mpsc::UnboundedReceiver<anyhow::Result<MasterControlMessage>>,
}

impl MasterControlReceiver {
    /// Receive the source daemon's [`crate::protocol::SourceMasterHello`], or an early
    /// [`crate::protocol::RcpdResult`] if the source failed before binding its listeners.
    ///
    /// Returns `Ok(None)` when the daemon closed stdout cleanly without sending a control frame.
    ///
    /// # Errors
    ///
    /// Returns an error on transport/decode failure or an unexpected control variant.
    pub async fn recv_source_hello(&mut self) -> anyhow::Result<Option<SourceHelloOutcome>> {
        match self.rx.recv().await {
            Some(Ok(MasterControlMessage::SourceHello(hello))) => {
                Ok(Some(SourceHelloOutcome::Hello(hello)))
            }
            Some(Ok(MasterControlMessage::Result(result))) => {
                Ok(Some(SourceHelloOutcome::Result(result)))
            }
            Some(Ok(MasterControlMessage::DirectConnectFailed(reason))) => {
                anyhow::bail!(
                    "expected SourceMasterHello from source rcpd, got unexpected DirectConnectFailed: {reason}"
                )
            }
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    /// Receive the next destination daemon control event (`DirectConnectFailed` or `RcpdResult`).
    ///
    /// Returns `Ok(None)` when the daemon closed stdout cleanly without sending another control frame.
    ///
    /// # Errors
    ///
    /// Returns an error on transport/decode failure or an unexpected `SourceHello` variant.
    pub async fn recv_destination_event(
        &mut self,
    ) -> anyhow::Result<Option<DestinationControlEvent>> {
        match self.rx.recv().await {
            Some(Ok(MasterControlMessage::DirectConnectFailed(reason))) => {
                Ok(Some(DestinationControlEvent::DirectConnectFailed {
                    reason,
                }))
            }
            Some(Ok(MasterControlMessage::Result(result))) => {
                Ok(Some(DestinationControlEvent::Result(Box::new(result))))
            }
            Some(Ok(MasterControlMessage::SourceHello(hello))) => {
                anyhow::bail!(
                    "expected destination control frame from rcpd, got unexpected {hello:?}"
                )
            }
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    /// Receive a daemon's final [`crate::protocol::RcpdResult`].
    ///
    /// Returns `Ok(None)` when the daemon closed stdout cleanly without reporting a result.
    ///
    /// # Errors
    ///
    /// Returns an error on transport/decode failure or an out-of-order control variant.
    pub async fn recv_result(&mut self) -> anyhow::Result<Option<crate::protocol::RcpdResult>> {
        match self.rx.recv().await {
            Some(Ok(MasterControlMessage::Result(result))) => Ok(Some(result)),
            Some(Ok(MasterControlMessage::SourceHello(hello))) => {
                anyhow::bail!("expected RcpdResult from rcpd, got unexpected {hello:?}")
            }
            Some(Ok(MasterControlMessage::DirectConnectFailed(reason))) => {
                anyhow::bail!(
                    "expected RcpdResult from rcpd, got unexpected DirectConnectFailed: {reason}"
                )
            }
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
}

/// Spawn the master-side stdout demuxer for one `rcpd`.
///
/// Tracing and progress frames are dispatched immediately via `crate::tracelog::dispatch_message`;
/// control frames (`SourceHello`, `DirectConnectFailed`, `RcpdResult`) are forwarded to the
/// returned [`MasterControlReceiver`]. The demuxer continues draining trailing tracing frames
/// until EOF even after all control frames have been consumed or dropped.
#[must_use]
pub fn spawn_master_stdout_demux<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    mut recv_stream: crate::streams::RecvStream<R>,
    rcpd_type: crate::tracelog::RcpdType,
) -> (MasterControlReceiver, crate::AbortOnDropTask<()>) {
    let (control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();
    let task = crate::AbortOnDropTask::new(tokio::spawn(async move {
        loop {
            match recv_stream
                .recv_object::<crate::protocol::RcpdToMaster>()
                .await
            {
                Ok(Some(crate::protocol::RcpdToMaster::Tracing(message))) => {
                    crate::tracelog::dispatch_message(message, rcpd_type);
                }
                Ok(Some(crate::protocol::RcpdToMaster::SourceHello(hello))) => {
                    let _ = control_tx.send(Ok(MasterControlMessage::SourceHello(hello)));
                }
                Ok(Some(crate::protocol::RcpdToMaster::DirectConnectFailed { reason })) => {
                    let _ = control_tx.send(Ok(MasterControlMessage::DirectConnectFailed(reason)));
                }
                Ok(Some(crate::protocol::RcpdToMaster::Result(result))) => {
                    let _ = control_tx.send(Ok(MasterControlMessage::Result(result)));
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::debug!("{rcpd_type} stdout demux ended: {error:#}");
                    let _ = control_tx.send(Err(error));
                    break;
                }
            }
        }
    }));
    (MasterControlReceiver { rx: control_rx }, task)
}

/// Master-side framed stdin sender with optional periodic keepalive heartbeats.
pub struct MasterStdinSender<W = crate::streams::BoxedWrite> {
    heartbeat: Option<crate::AbortOnDropTask<()>>,
    stream: std::sync::Arc<tokio::sync::Mutex<Option<crate::streams::SendStream<W>>>>,
}

impl<W: tokio::io::AsyncWrite + Unpin + Send + 'static> MasterStdinSender<W> {
    /// Wrap a daemon's SSH stdin stream and start periodic heartbeats when `keepalive_sec > 0`.
    #[must_use]
    pub fn new(stdin: W, keepalive_sec: u64) -> Self {
        let stream = std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            crate::streams::SendStream::new(stdin),
        )));
        let heartbeat = master_heartbeat_interval(keepalive_sec)
            .and_then(|period| {
                tokio::time::Instant::now()
                    .checked_add(period)
                    .map(|first_tick| (first_tick, period))
            })
            .map(|(first_tick, period)| {
                let stream = stream.clone();
                crate::AbortOnDropTask::new(tokio::spawn(async move {
                    let mut ticker = tokio::time::interval_at(first_tick, period);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        ticker.tick().await;
                        let mut guard = stream.lock().await;
                        let Some(send_stream) = guard.as_mut() else {
                            break;
                        };
                        if let Err(error) = send_stream
                            .send_control_message(&crate::protocol::MasterToRcpd::Heartbeat)
                            .await
                        {
                            tracing::debug!("stopping master stdin heartbeat: {error:#}");
                            break;
                        }
                    }
                }))
            });
        Self { heartbeat, stream }
    }

    /// Send the initial [`crate::protocol::MasterHello`] frame to `rcpd`.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream was already closed or writing/flushing fails.
    pub async fn send_hello(&self, hello: crate::protocol::MasterHello) -> anyhow::Result<()> {
        self.send_frame(&crate::protocol::MasterToRcpd::Hello(Box::new(hello)))
            .await
    }

    /// Send master-opened SSH fallback endpoints to destination `rcpd`.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream was already closed or writing/flushing fails.
    pub async fn send_ssh_fallback_endpoints(
        &self,
        control_addr: std::net::SocketAddr,
        data_addrs: Vec<std::net::SocketAddr>,
    ) -> anyhow::Result<()> {
        self.send_frame(&crate::protocol::MasterToRcpd::SshFallbackEndpoints {
            control_addr,
            data_addrs,
        })
        .await
    }

    async fn send_frame(&self, message: &crate::protocol::MasterToRcpd) -> anyhow::Result<()> {
        let mut guard = self.stream.lock().await;
        let send_stream = guard
            .as_mut()
            .context("master stdin stream is already closed")?;
        send_stream.send_control_message(message).await
    }

    /// Stop the heartbeat task and drop the underlying stdin pipe to signal immediate EOF to
    /// `rcpd` without blocking on a full pipe.
    pub async fn close(mut self) {
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
            let _ = heartbeat.join().await;
        }
        drop(self.stream.lock().await.take());
    }
}

impl<W> Drop for MasterStdinSender<W> {
    fn drop(&mut self) {
        // abort the heartbeat task first, then release the underlying stdin pipe synchronously
        // whenever heartbeat is not actively holding the lock so cancellation delivers EOF
        // without waiting for Tokio to poll the aborted task
        drop(self.heartbeat.take());
        if let Ok(mut guard) = self.stream.try_lock() {
            drop(guard.take());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn os_pipe() -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
        let mut fds = [0 as std::os::fd::RawFd; 2];
        // SAFETY: `fds` is a valid 2-element array for `pipe2`.
        assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        // SAFETY: both descriptors were freshly created by `pipe2`.
        unsafe {
            (
                std::os::fd::OwnedFd::from_raw_fd(fds[0]),
                std::os::fd::OwnedFd::from_raw_fd(fds[1]),
            )
        }
    }

    fn os_socketpair() -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
        let mut fds = [0 as std::os::fd::RawFd; 2];
        // SAFETY: `fds` is a valid 2-element array for `socketpair`.
        assert_eq!(
            unsafe {
                libc::socketpair(
                    libc::AF_UNIX,
                    libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                    0,
                    fds.as_mut_ptr(),
                )
            },
            0
        );
        // SAFETY: both descriptors were freshly created by `socketpair`.
        unsafe {
            (
                std::os::fd::OwnedFd::from_raw_fd(fds[0]),
                std::os::fd::OwnedFd::from_raw_fd(fds[1]),
            )
        }
    }

    #[test]
    fn ssh_server_alive_params_respect_keepalive_budget() {
        assert_eq!(ssh_server_alive_params(0), None);
        // keepalive_sec == 1 hits OpenSSH's 2-second minimum effective bound (interval=1, count=1)
        // so at least one probe is sent before disconnecting an idle healthy session
        assert_eq!(ssh_server_alive_params(1), Some((1, 1)));
        let max_count = SSH_SERVER_ALIVE_MAX_INTERVALS - 1;
        for (budget, expected) in [
            (2, (1, 1)),
            (3, (1, 2)),
            (5, (1, 4)),
            (6, (1, 5)),
            (7, (2, 3)),
            (10, (2, 4)),
            (15, (3, 4)),
            (30, (5, 5)),
            (60, (10, 5)),
            (120, (20, 5)),
        ] {
            let actual = ssh_server_alive_params(budget).unwrap();
            assert_eq!(actual, expected, "budget={budget}");
            let (interval, count) = actual;
            let effective_timeout = interval * (count + 1);
            assert!(interval >= 1);
            assert!((1..=max_count).contains(&count));
            assert!(
                effective_timeout >= budget,
                "budget={budget}, effective={effective_timeout}"
            );
            assert!(
                effective_timeout < budget + interval,
                "budget={budget}, effective={effective_timeout}"
            );
        }
        // huge budgets must stay within OpenSSH's i32::MAX ServerAliveInterval limit
        let (interval, count) = ssh_server_alive_params(u64::MAX).unwrap();
        assert_eq!(interval, MAX_SSH_SERVER_ALIVE_INTERVAL);
        assert_eq!(count, max_count);
    }

    #[test]
    fn master_heartbeat_interval_respects_keepalive_budget() {
        assert_eq!(master_heartbeat_interval(0), None);
        assert_eq!(
            master_heartbeat_interval(1),
            Some(std::time::Duration::from_secs(1) / 3)
        );
        assert_eq!(
            master_heartbeat_interval(5),
            Some(std::time::Duration::from_secs(5) / 3)
        );
        assert_eq!(
            master_heartbeat_interval(120),
            Some(std::time::Duration::from_secs(40))
        );
        assert_eq!(
            master_heartbeat_interval(u64::MAX),
            Some(std::time::Duration::from_secs(u64::MAX) / 3)
        );
    }

    #[tokio::test]
    async fn stdio_reader_and_writer_round_trip_over_pipe_and_socketpair() {
        for (read_fd, write_fd) in [os_pipe(), os_socketpair()] {
            let mut reader = StdioReader::from_owned_fd(read_fd).unwrap();
            let mut writer = StdioWriter::from_owned_fd(write_fd).unwrap();
            assert!(matches!(reader.0, StdioReaderInner::NonBlocking(_)));
            assert!(matches!(writer.0, StdioWriterInner::NonBlocking { .. }));
            writer.write_all(b"framed-ssh-stdio").await.unwrap();
            writer.shutdown().await.unwrap();
            let mut received = Vec::new();
            reader.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, b"framed-ssh-stdio");
        }
    }

    #[tokio::test]
    async fn stdio_writer_shutdown_delivers_eof_on_shared_socketpair() {
        let (peer_fd, shared_fd) = os_socketpair();
        // simulate a shared bidirectional socketpair where stdin keeps the same socket open
        let stdin_dup = dup_cloexec(shared_fd.as_raw_fd()).unwrap();
        let _reader = StdioReader::from_owned_fd(stdin_dup).unwrap();
        let mut writer = StdioWriter::from_owned_fd(shared_fd).unwrap();
        let mut peer = StdioReader::from_owned_fd(peer_fd).unwrap();
        writer.write_all(b"shared-socket").await.unwrap();
        writer.shutdown().await.unwrap();
        let mut received = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            peer.read_to_end(&mut received),
        )
        .await
        .expect("SHUT_WR must deliver EOF even while stdin holds a dup of the same socket")
        .unwrap();
        assert_eq!(received, b"shared-socket");
    }

    #[tokio::test]
    async fn stdio_reader_and_writer_fall_back_for_regular_files() {
        let file = tempfile::tempfile().unwrap();
        let read_fd = dup_cloexec(file.as_raw_fd()).unwrap();
        let write_fd = dup_cloexec(file.as_raw_fd()).unwrap();
        let mut reader = StdioReader::from_owned_fd(read_fd).unwrap();
        let mut writer = StdioWriter::from_owned_fd(write_fd).unwrap();
        assert!(matches!(reader.0, StdioReaderInner::File(_)));
        assert!(matches!(writer.0, StdioWriterInner::File(_)));
        writer.write_all(b"regular-file").await.unwrap();
        writer.flush().await.unwrap();
        drop(writer);
        // rewind shared file description to read back from offset 0
        // SAFETY: `file` is live and `SEEK_SET` to 0 is valid on a regular file.
        assert_eq!(
            unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_SET) },
            0
        );
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"regular-file");
    }

    fn preview(message: &str) -> common::remote_tracing::TracingMessage {
        common::remote_tracing::TracingMessage::Log {
            timestamp: std::time::UNIX_EPOCH,
            level: "INFO".to_owned(),
            target: "rcp::source".to_owned(),
            message: message.to_owned(),
        }
    }

    async fn receive_messages(
        reader: tokio::io::DuplexStream,
    ) -> anyhow::Result<Vec<crate::protocol::RcpdToMaster>> {
        let mut stream = crate::streams::RecvStream::new(reader);
        let mut messages = Vec::new();
        while let Some(message) = stream
            .recv_object::<crate::protocol::RcpdToMaster>()
            .await?
        {
            messages.push(message);
        }
        Ok(messages)
    }

    #[tokio::test]
    async fn daemon_stdout_writer_drains_queued_tracing_with_live_producers() -> anyhow::Result<()>
    {
        let (_control_tx, control_rx) = daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        for name in ["first.txt", "second.txt", "last.txt"] {
            tracing_tx.send(preview(name))?;
        }
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        let (writer, reader) = tokio::io::duplex(1);
        let send = run_daemon_stdout_writer(
            control_rx,
            tracing_rx,
            crate::streams::SendStream::new(writer),
            shutdown,
        );
        let receive = async {
            tracing_tx.closed().await;
            assert!(tracing_tx.send(preview("too-late.txt")).is_err());
            receive_messages(reader).await
        };
        let (_, messages) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::try_join!(send, receive)
        })
        .await
        .expect("shutdown must close admission before draining queued messages")?;
        let names: Vec<_> = messages
            .into_iter()
            .map(|msg| match msg {
                crate::protocol::RcpdToMaster::Tracing(
                    common::remote_tracing::TracingMessage::Log { message, .. },
                ) => message,
                other => panic!("unexpected frame: {other:?}"),
            })
            .collect();
        assert_eq!(names, ["first.txt", "second.txt", "last.txt"]);
        Ok(())
    }

    #[tokio::test]
    async fn daemon_stdout_writer_finishes_in_flight_frame_before_draining() -> anyhow::Result<()> {
        let (_control_tx, control_rx) = daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        tracing_tx.send(preview("in-flight.txt"))?;
        tracing_tx.send(preview("queued.txt"))?;
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (writer, reader) = tokio::io::duplex(1);
        let send = run_daemon_stdout_writer(
            control_rx,
            tracing_rx,
            crate::streams::SendStream::new(writer),
            shutdown.clone(),
        );
        tokio::pin!(send);
        assert!(futures::poll!(&mut send).is_pending());
        shutdown.cancel();
        let (_, messages) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::try_join!(send, receive_messages(reader))
        })
        .await
        .expect("an in-flight frame and its queued successor must finish")?;
        let names: Vec<_> = messages
            .into_iter()
            .map(|msg| match msg {
                crate::protocol::RcpdToMaster::Tracing(
                    common::remote_tracing::TracingMessage::Log { message, .. },
                ) => message,
                other => panic!("unexpected frame: {other:?}"),
            })
            .collect();
        assert_eq!(names, ["in-flight.txt", "queued.txt"]);
        assert!(tracing_tx.is_closed());
        Ok(())
    }

    #[tokio::test]
    async fn daemon_stdout_writer_prioritizes_control_over_tracing() -> anyhow::Result<()> {
        let (control_tx, control_rx) = daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        tracing_tx.send(preview("queued-log"))?;
        let hello = crate::protocol::SourceMasterHello {
            control_addr: "127.0.0.1:4001".parse().unwrap(),
            data_addr: "127.0.0.1:4002".parse().unwrap(),
            tunnel_target_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            server_name: "source".to_owned(),
            peer_identity: crate::protocol::PeerIdentity::current().0,
            ssh_node_endpoint: None,
        };
        let send_hello = control_tx.send_source_hello(hello);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (writer, reader) = tokio::io::duplex(4096);
        let writer_task = tokio::spawn(run_daemon_stdout_writer(
            control_rx,
            tracing_rx,
            crate::streams::SendStream::new(writer),
            shutdown.clone(),
        ));
        send_hello.await?;
        drop(control_tx);
        shutdown.cancel();
        let messages = receive_messages(reader).await?;
        writer_task.await??;
        assert!(matches!(
            messages.first(),
            Some(crate::protocol::RcpdToMaster::SourceHello(_))
        ));
        assert!(matches!(
            messages.get(1),
            Some(crate::protocol::RcpdToMaster::Tracing(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn daemon_stdout_writer_preserves_broken_pipe_error() {
        let (_control_tx, control_rx) = daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        tracing_tx.send(preview("first.txt")).unwrap();
        drop(tracing_tx);
        let (writer, reader) = tokio::io::duplex(1);
        drop(reader);
        let error = run_daemon_stdout_writer(
            control_rx,
            tracing_rx,
            crate::streams::SendStream::new(writer),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("a broken pipe must return its transport error");
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn master_stdin_sender_emits_hello_heartbeats_and_eof() -> anyhow::Result<()> {
        let (client_write, daemon_read) = tokio::io::duplex(4096);
        let sender = MasterStdinSender::new(client_write, 1);
        sender
            .send_hello(crate::protocol::MasterHello::Source {
                src: "/src".into(),
                dst: "/dst".into(),
                dest_cert_fingerprint: None,
                filter: None,
                dry_run: None,
                capture: Default::default(),
                accept_timeout_sec: 15,
            })
            .await?;
        let mut recv = crate::streams::RecvStream::new(daemon_read);
        assert!(matches!(
            recv.recv_object::<crate::protocol::MasterToRcpd>().await?,
            Some(crate::protocol::MasterToRcpd::Hello(hello))
                if matches!(*hello, crate::protocol::MasterHello::Source { .. })
        ));
        let heartbeat = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            recv.recv_object::<crate::protocol::MasterToRcpd>(),
        )
        .await
        .context("timed out waiting for stdin heartbeat")??;
        assert!(matches!(
            heartbeat,
            Some(crate::protocol::MasterToRcpd::Heartbeat)
        ));
        sender.close().await;
        assert!(
            recv.recv_object::<crate::protocol::MasterToRcpd>()
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn master_stdout_demux_routes_control_and_eof() -> anyhow::Result<()> {
        let (daemon_write, master_read) = tokio::io::duplex(4096);
        let mut send = crate::streams::SendStream::new(daemon_write);
        let (mut control_rx, demux_task) = spawn_master_stdout_demux(
            crate::streams::RecvStream::new(master_read),
            crate::tracelog::RcpdType::Source,
        );
        send.send_control_message(&crate::protocol::RcpdToMaster::Tracing(preview(
            "startup-notice",
        )))
        .await?;
        send.send_control_message(&crate::protocol::RcpdToMaster::SourceHello(
            crate::protocol::SourceMasterHello {
                control_addr: "127.0.0.1:4100".parse().unwrap(),
                data_addr: "127.0.0.1:4101".parse().unwrap(),
                tunnel_target_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_name: "src".to_owned(),
                peer_identity: crate::protocol::PeerIdentity::current().0,
                ssh_node_endpoint: None,
            },
        ))
        .await?;
        let Some(SourceHelloOutcome::Hello(hello)) = control_rx.recv_source_hello().await? else {
            panic!("source hello must arrive");
        };
        assert_eq!(hello.control_addr, "127.0.0.1:4100".parse().unwrap());
        send.close().await?;
        assert!(control_rx.recv_result().await?.is_none());
        demux_task.join().await?;
        Ok(())
    }

    #[tokio::test]
    async fn master_stdout_demux_surfaces_early_source_result_before_hello() -> anyhow::Result<()> {
        let (daemon_write, master_read) = tokio::io::duplex(4096);
        let mut send = crate::streams::SendStream::new(daemon_write);
        let (mut control_rx, demux_task) = spawn_master_stdout_demux(
            crate::streams::RecvStream::new(master_read),
            crate::tracelog::RcpdType::Source,
        );
        send.send_control_message(&crate::protocol::RcpdToMaster::Result(
            crate::protocol::RcpdResult::Failure {
                error: "port range exhausted".to_owned(),
                summary: Default::default(),
                runtime_stats: Default::default(),
                peer_connected: false,
            },
        ))
        .await?;
        send.close().await?;
        match control_rx.recv_source_hello().await? {
            Some(SourceHelloOutcome::Result(crate::protocol::RcpdResult::Failure {
                error,
                ..
            })) => assert_eq!(error, "port range exhausted"),
            other => panic!("expected early Failure result, got {other:?}"),
        }
        demux_task.join().await?;
        Ok(())
    }

    #[tokio::test]
    async fn master_stdin_sender_drop_closes_stream_and_handles_max_keepalive() {
        let (client_write, daemon_read) = tokio::io::duplex(4096);
        // huge keepalive_sec must not panic in Instant::checked_add
        let sender = MasterStdinSender::new(client_write, u64::MAX);
        drop(sender);
        let mut recv = crate::streams::RecvStream::new(daemon_read);
        assert!(
            recv.recv_object::<crate::protocol::MasterToRcpd>()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn daemon_stdout_writer_drops_oversized_trace_and_still_delivers_control_result()
    -> anyhow::Result<()> {
        let (daemon_write, master_read) = tokio::io::duplex(64 * 1024);
        let (control_tx, control_rx) = daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let send_stream = crate::streams::SendStream::new(daemon_write);
        let max_len = send_stream.max_frame_length();
        // enqueue an oversized trace frame before a normal trace and control result so the
        // writer encounters it first
        tracing_tx.send(common::remote_tracing::TracingMessage::Log {
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            level: "INFO".to_owned(),
            target: "rcpd".to_owned(),
            message: "x".repeat(max_len + 1024),
        })?;
        tracing_tx.send(preview("after-oversized"))?;
        let writer_task = tokio::spawn(run_daemon_stdout_writer(
            control_rx,
            tracing_rx,
            send_stream,
            shutdown.clone(),
        ));
        let reader_task = tokio::spawn(async move {
            let mut recv = crate::streams::RecvStream::new(master_read);
            let mut frames = Vec::new();
            while let Some(frame) = recv.recv_object::<crate::protocol::RcpdToMaster>().await? {
                frames.push(frame);
            }
            Ok::<_, anyhow::Error>(frames)
        });
        // yield so the writer processes the oversized trace before we send the control result
        tokio::task::yield_now().await;
        control_tx
            .send_result(crate::protocol::RcpdResult::Success {
                message: "copy ok".to_owned(),
                summary: Default::default(),
                runtime_stats: Default::default(),
            })
            .await?;
        drop(control_tx);
        shutdown.cancel();
        writer_task.await??;
        let frames = reader_task.await??;
        assert!(
            frames.iter().any(|frame| matches!(
                frame,
                crate::protocol::RcpdToMaster::Tracing(
                    common::remote_tracing::TracingMessage::Log { message, .. }
                ) if message == "after-oversized"
            )),
            "normal trace after oversized trace must still arrive: {frames:?}"
        );
        assert!(
            frames.iter().any(|frame| matches!(
                frame,
                crate::protocol::RcpdToMaster::Result(crate::protocol::RcpdResult::Success {
                    message,
                    ..
                }) if message == "copy ok"
            )),
            "control result after oversized trace must still arrive: {frames:?}"
        );
        assert!(
            !frames.iter().any(|frame| matches!(
                frame,
                crate::protocol::RcpdToMaster::Tracing(
                    common::remote_tracing::TracingMessage::Log { message, .. }
                ) if message.len() > max_len
            )),
            "oversized trace must be discarded"
        );
        Ok(())
    }

    #[tokio::test]
    async fn daemon_stdout_writer_fails_on_real_transport_error_for_trace() -> anyhow::Result<()> {
        let (daemon_write, master_read) = tokio::io::duplex(4096);
        // close the peer reader so writing a valid trace hits a real transport error
        drop(master_read);
        let (_control_tx, control_rx) = daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = tokio_util::sync::CancellationToken::new();
        tracing_tx.send(preview("normal-trace"))?;
        let result = run_daemon_stdout_writer(
            control_rx,
            tracing_rx,
            crate::streams::SendStream::new(daemon_write),
            shutdown,
        )
        .await;
        assert!(
            result.is_err(),
            "real transport errors on tracing frames must remain fatal"
        );
        Ok(())
    }

    #[tokio::test]
    async fn daemon_stdout_writer_skips_abandoned_control_frame() -> anyhow::Result<()> {
        let (daemon_write, master_read) = tokio::io::duplex(4096);
        let (control_tx, control_rx) = daemon_control_channel();
        let (_tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = tokio_util::sync::CancellationToken::new();
        // enqueue a control frame and immediately drop its ack receiver before the writer runs
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        drop(ack_rx);
        control_tx
            .tx
            .send(ControlOutbound {
                message: crate::protocol::RcpdToMaster::Result(
                    crate::protocol::RcpdResult::Success {
                        message: "stale".to_owned(),
                        summary: Default::default(),
                        runtime_stats: Default::default(),
                    },
                ),
                ack: ack_tx,
            })
            .unwrap();
        drop(control_tx);
        shutdown.cancel();
        let writer_task = tokio::spawn(run_daemon_stdout_writer(
            control_rx,
            tracing_rx,
            crate::streams::SendStream::new(daemon_write),
            shutdown,
        ));
        let mut recv = crate::streams::RecvStream::new(master_read);
        assert!(
            recv.recv_object::<crate::protocol::RcpdToMaster>()
                .await?
                .is_none(),
            "abandoned control frame must not be written to master stdout"
        );
        writer_task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn master_stdout_demux_routes_destination_fallback_then_result() -> anyhow::Result<()> {
        let (daemon_write, master_read) = tokio::io::duplex(4096);
        let (control_tx, control_rx) = daemon_control_channel();
        let (_tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let writer_task = tokio::spawn(run_daemon_stdout_writer(
            control_rx,
            tracing_rx,
            crate::streams::SendStream::new(daemon_write),
            shutdown.clone(),
        ));
        let (mut dest_control_rx, demux_task) = spawn_master_stdout_demux(
            crate::streams::RecvStream::new(master_read),
            crate::tracelog::RcpdType::Destination,
        );
        control_tx
            .send_direct_connect_failed("timed out after 3s".to_owned())
            .await?;
        match dest_control_rx.recv_destination_event().await? {
            Some(DestinationControlEvent::DirectConnectFailed { reason }) => {
                assert_eq!(reason, "timed out after 3s");
            }
            other => panic!("expected DirectConnectFailed, got {other:?}"),
        }
        control_tx
            .send_result(crate::protocol::RcpdResult::Success {
                message: "copied via ssh".to_owned(),
                summary: Default::default(),
                runtime_stats: Default::default(),
            })
            .await?;
        drop(control_tx);
        shutdown.cancel();
        match dest_control_rx.recv_destination_event().await? {
            Some(DestinationControlEvent::Result(boxed)) => match *boxed {
                crate::protocol::RcpdResult::Success { message, .. } => {
                    assert_eq!(message, "copied via ssh");
                }
                other => panic!("expected Success result, got {other:?}"),
            },
            other => panic!("expected Result event, got {other:?}"),
        }
        assert!(dest_control_rx.recv_destination_event().await?.is_none());
        writer_task.await??;
        demux_task.join().await?;
        Ok(())
    }

    #[tokio::test]
    async fn master_control_receiver_rejects_out_of_order_fallback_and_hello_frames()
    -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut receiver = MasterControlReceiver { rx };
        tx.send(Ok(MasterControlMessage::DirectConnectFailed(
            "unexpected".to_owned(),
        )))
        .unwrap();
        let err = receiver
            .recv_source_hello()
            .await
            .expect_err("recv_source_hello must reject DirectConnectFailed");
        assert!(format!("{err:#}").contains("DirectConnectFailed"));
        tx.send(Ok(MasterControlMessage::DirectConnectFailed(
            "unexpected".to_owned(),
        )))
        .unwrap();
        let err = receiver
            .recv_result()
            .await
            .expect_err("recv_result must reject DirectConnectFailed");
        assert!(format!("{err:#}").contains("DirectConnectFailed"));
        tx.send(Ok(MasterControlMessage::SourceHello(
            crate::protocol::SourceMasterHello {
                control_addr: "127.0.0.1:41001".parse().unwrap(),
                data_addr: "127.0.0.1:41002".parse().unwrap(),
                tunnel_target_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_name: "src".to_owned(),
                peer_identity: crate::protocol::PeerIdentity::current().0,
                ssh_node_endpoint: None,
            },
        )))
        .unwrap();
        let err = receiver
            .recv_destination_event()
            .await
            .expect_err("recv_destination_event must reject SourceHello");
        assert!(format!("{err:#}").contains("expected destination control frame"));
        Ok(())
    }

    #[tokio::test]
    async fn master_stdin_sender_emits_ssh_fallback_endpoints() -> anyhow::Result<()> {
        let (client_write, daemon_read) = tokio::io::duplex(4096);
        let sender = MasterStdinSender::new(client_write, 0);
        let control_addr: std::net::SocketAddr = "127.0.0.1:45001".parse().unwrap();
        let data_addrs: Vec<std::net::SocketAddr> = vec![
            "127.0.0.1:45002".parse().unwrap(),
            "127.0.0.1:45003".parse().unwrap(),
        ];
        sender
            .send_ssh_fallback_endpoints(control_addr, data_addrs.clone())
            .await?;
        let mut recv = crate::streams::RecvStream::new(daemon_read);
        match recv.recv_object::<crate::protocol::MasterToRcpd>().await? {
            Some(crate::protocol::MasterToRcpd::SshFallbackEndpoints {
                control_addr: got_control,
                data_addrs: got_data,
            }) => {
                assert_eq!(got_control, control_addr);
                assert_eq!(got_data, data_addrs);
            }
            other => panic!("expected SshFallbackEndpoints, got {other:?}"),
        }
        sender.close().await;
        Ok(())
    }
}
