use super::*;
use std::io::{ErrorKind, Read};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

#[derive(Default)]
struct Observations {
    reads: Vec<(usize, usize)>,
    // what each read found in the storage it was given, before filling it
    found: Vec<Vec<u8>>,
    buffers: Vec<(usize, usize, usize)>,
    writes: Vec<(usize, Vec<u8>)>,
    flushes: usize,
    dropped: bool,
}

tokio::task_local! {
    static BUFFERS: Arc<Mutex<Observations>>;
}

pub(super) fn record_buffer(buffer: &Vec<u8>) {
    let _ = BUFFERS.try_with(|observed| {
        observed.lock().unwrap().buffers.push((
            buffer.as_ptr() as usize,
            buffer.len(),
            buffer.capacity(),
        ));
    });
}

struct Reader {
    observations: Arc<Mutex<Observations>>,
    bytes: std::io::Cursor<Vec<u8>>,
    short: usize,
    interrupted: bool,
    fail_at: Option<u64>,
    panic_on_read: bool,
    gate: Option<(
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
}

impl Read for Reader {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        {
            let mut observations = self.observations.lock().unwrap();
            observations
                .reads
                .push((bytes.as_ptr() as usize, bytes.len()));
            observations.found.push(bytes.to_vec());
        }
        if let Some((started, release)) = self.gate.take() {
            let _ = started.send(());
            release
                .recv_timeout(std::time::Duration::from_secs(10))
                .map_err(std::io::Error::other)?;
        }
        if std::mem::take(&mut self.interrupted) {
            return Err(ErrorKind::Interrupted.into());
        }
        if self.fail_at == Some(self.bytes.position()) {
            return Err(ErrorKind::PermissionDenied.into());
        }
        assert!(!self.panic_on_read, "injected source read panic");
        let size = bytes.len().min(self.short);
        Read::read(&mut self.bytes, &mut bytes[..size])
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.observations.lock().unwrap().dropped = true;
    }
}

struct Writer {
    observations: Arc<Mutex<Observations>>,
    partial: usize,
    pending: bool,
    pending_bytes: Option<(usize, Vec<u8>)>,
    failure: Option<ErrorKind>,
    flush_failure: bool,
    header_failure: bool,
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.observations.lock().unwrap().flushes == 0 {
            return Poll::Ready(if self.header_failure {
                Err(ErrorKind::BrokenPipe.into())
            } else {
                Ok(bytes.len())
            });
        }
        if let Some(kind) = self.failure {
            return if kind == ErrorKind::WriteZero {
                Poll::Ready(Ok(0))
            } else {
                Poll::Ready(Err(kind.into()))
            };
        }
        if self.pending && self.pending_bytes.is_none() {
            self.pending_bytes = Some((bytes.as_ptr() as usize, bytes.to_vec()));
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if let Some((pointer, expected)) = self.pending_bytes.take() {
            assert_eq!(bytes.as_ptr() as usize, pointer);
            assert_eq!(bytes, expected, "a pending payload was overwritten");
        }
        let size = bytes.len().min(self.partial);
        self.observations
            .lock()
            .unwrap()
            .writes
            .push((bytes.as_ptr() as usize, bytes[..size].to_vec()));
        Poll::Ready(Ok(size))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut observations = self.observations.lock().unwrap();
        observations.flushes += 1;
        if self.flush_failure && observations.flushes == 2 {
            Poll::Ready(Err(ErrorKind::ConnectionReset.into()))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A further file for a stream whose observations already exist.
fn reader(observations: &Arc<Mutex<Observations>>, bytes: &[u8]) -> Reader {
    Reader {
        observations: observations.clone(),
        bytes: std::io::Cursor::new(bytes.to_vec()),
        short: usize::MAX,
        interrupted: false,
        fail_at: None,
        panic_on_read: false,
        gate: None,
    }
}

fn fixture(bytes: &[u8]) -> (Reader, SendStream<Writer>, Arc<Mutex<Observations>>) {
    let observations = Arc::new(Mutex::new(Observations::default()));
    let reader = reader(&observations, bytes);
    let writer = Writer {
        observations: observations.clone(),
        partial: usize::MAX,
        pending: false,
        pending_bytes: None,
        failure: None,
        flush_failure: false,
        header_failure: false,
    };
    (reader, SendStream::new(writer), observations)
}

#[tokio::test]
async fn one_allocation_reaches_partial_pending_writes_without_overwrite() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let bytes: Vec<_> = (0..29).collect();
    let (reader, mut stream, observed) = fixture(&bytes);
    stream.framed.get_mut().partial = 2;
    stream.framed.get_mut().pending = true;
    assert_eq!(
        BUFFERS
            .scope(
                observed.clone(),
                stream.send_message_with_file(&17_u64, reader, bytes.len() as u64, 8, &admission)
            )
            .await?,
        29
    );
    let observed = observed.lock().unwrap();
    let pointer = observed.reads[0].0;
    assert_eq!(observed.buffers, vec![(pointer, 8, 8); 4]);
    assert_eq!(
        observed.reads,
        [(pointer, 8), (pointer, 8), (pointer, 8), (pointer, 5)]
    );
    let mut total = 0;
    let mut actual = Vec::new();
    for (write_pointer, bytes) in &observed.writes {
        assert_eq!(*write_pointer, pointer + total % 8);
        actual.extend_from_slice(bytes);
        total += bytes.len();
    }
    assert_eq!(actual, bytes);
    assert_eq!(observed.flushes, 2);
    assert!(observed.dropped);
    Ok(())
}

#[tokio::test]
async fn short_reads_are_forwarded_immediately_and_interrupted_reads_retry() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let (mut reader, mut stream, observed) = fixture(b"abcdefghij");
    reader.short = 3;
    reader.interrupted = true;
    assert_eq!(
        stream
            .send_message_with_file(&1_u64, reader, 8, 5, &admission)
            .await?,
        8
    );
    let observed = observed.lock().unwrap();
    let pointer = observed.reads[0].0;
    assert_eq!(
        observed.reads,
        [(pointer, 5), (pointer, 5), (pointer, 5), (pointer, 2)]
    );
    assert_eq!(
        observed.writes,
        [
            (pointer, b"abc".to_vec()),
            (pointer, b"def".to_vec()),
            (pointer, b"gh".to_vec())
        ]
    );
    assert_eq!(observed.flushes, 2);
    Ok(())
}

#[tokio::test]
async fn empty_and_short_payloads_flush_and_zero_configuration_normalizes() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    for (size, configured, expected) in [(0, usize::MAX, 0), (3, 0, 3), (12, 4, 6)] {
        let (reader, mut stream, observed) = fixture(b"abcdef");
        assert_eq!(
            stream
                .send_message_with_file(&1_u64, reader, size, configured, &admission)
                .await?,
            expected
        );
        let observed = observed.lock().unwrap();
        assert_eq!(observed.flushes, 2);
        assert!(observed.dropped);
        if size == 0 {
            assert!(observed.reads.is_empty());
            assert!(observed.writes.is_empty());
        } else {
            assert!(
                observed
                    .reads
                    .iter()
                    .all(|(_, size)| *size <= configured.max(1))
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn read_write_and_final_flush_errors_preserve_their_causes() {
    let admission = throttle::open_file_permit().await;
    for expected in [
        ErrorKind::PermissionDenied,
        ErrorKind::BrokenPipe,
        ErrorKind::WriteZero,
        ErrorKind::ConnectionReset,
    ] {
        let (mut reader, mut stream, observed) = fixture(b"abcdef");
        match expected {
            ErrorKind::PermissionDenied => reader.fail_at = Some(0),
            ErrorKind::ConnectionReset => stream.framed.get_mut().flush_failure = true,
            kind => stream.framed.get_mut().failure = Some(kind),
        }
        let error = stream
            .send_message_with_file(&1_u64, reader, 6, 4, &admission)
            .await
            .unwrap_err();
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            expected
        );
        assert_eq!(
            error.to_string(),
            match expected {
                ErrorKind::PermissionDenied => "failed to read source payload",
                ErrorKind::ConnectionReset => "failed to flush source payload",
                _ => "failed to write source payload",
            }
        );
        let observed = observed.lock().unwrap();
        assert_eq!(
            observed.flushes,
            if expected == ErrorKind::ConnectionReset {
                2
            } else {
                1
            }
        );
        assert!(observed.dropped);
    }
    let (reader, mut stream, observed) = fixture(b"abc");
    stream.framed.get_mut().header_failure = true;
    let error = BUFFERS
        .scope(
            observed.clone(),
            stream.send_message_with_file(&1_u64, reader, 3, 3, &admission),
        )
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "failed to send source header");
    assert_eq!(
        error
            .root_cause()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        ErrorKind::BrokenPipe
    );
    {
        let observed = observed.lock().unwrap();
        assert!(observed.dropped && observed.reads.is_empty() && observed.buffers.is_empty());
        assert_eq!(observed.flushes, 0);
    }
    let (mut reader, mut stream, observed) = fixture(b"abc");
    reader.panic_on_read = true;
    let error = stream
        .send_message_with_file(&1_u64, reader, 3, 3, &admission)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "failed to read source payload");
    assert!(
        error
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_panic()
    );
    {
        let observed = observed.lock().unwrap();
        assert!(observed.dropped && observed.writes.is_empty());
        assert_eq!(observed.flushes, 1);
    }
    // EOF does not hide an error flushing the bytes that were sent before it.
    let (reader, mut stream, _) = fixture(b"abc");
    stream.framed.get_mut().flush_failure = true;
    let error = stream
        .send_message_with_file(&1_u64, reader, 8, 4, &admission)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .root_cause()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        ErrorKind::ConnectionReset
    );
}

// these tests change process-wide admission; keep the parent test process untouched.
#[tokio::test]
async fn a_stream_reuses_its_payload_buffer_for_files_within_the_retention_limit()
-> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let (first, stream, observed) = fixture(&[0xaa; 8]);
    let mut stream = stream.with_payload_buffer_retention_limit(8);
    assert!(stream.payload_buffer.is_empty());
    assert_eq!(
        stream
            .send_message_with_file(&1_u64, first, 8, 8, &admission)
            .await?,
        8
    );
    let pointer = stream.payload_buffer.as_ptr() as usize;
    assert_eq!(stream.payload_buffer.len(), 8);
    assert_eq!(stream.payload_buffer.capacity(), 8);
    for (bytes, configured) in [(&b"xyz"[..], 8), (&b"pqrstuvw"[..], 64), (&b"k"[..], 1)] {
        let before = observed.lock().unwrap().reads.len();
        assert_eq!(
            stream
                .send_message_with_file(
                    &2_u64,
                    reader(&observed, bytes),
                    bytes.len() as u64,
                    configured,
                    &admission
                )
                .await?,
            bytes.len() as u64
        );
        let observed = observed.lock().unwrap();
        // a read at the same address proves nothing: an allocator hands a freed block out again.
        // the bytes of the file before, still in the storage, prove it was neither replaced nor
        // initialized again
        assert_eq!(observed.reads[before..], [(pointer, bytes.len())]);
        assert!(observed.found[before].iter().all(|byte| *byte != 0));
        assert_eq!(stream.payload_buffer.as_ptr() as usize, pointer);
        assert_eq!(stream.payload_buffer.len(), 8);
    }
    Ok(())
}

#[tokio::test]
async fn a_default_configuration_keeps_a_whole_profile_sized_payload_buffer() -> anyhow::Result<()>
{
    let admission = throttle::open_file_permit().await;
    for profile in [
        crate::NetworkProfile::Internet,
        crate::NetworkProfile::Datacenter,
    ] {
        let config = crate::TcpConfig {
            network_profile: profile,
            ..Default::default()
        };
        let chunk = config.effective_buffer_size();
        let payload = vec![0xaa; chunk];
        let (first, stream, observed) = fixture(&payload);
        let mut stream =
            stream.with_payload_buffer_retention_limit(config.effective_buffer_retention_limit());
        assert_eq!(
            stream
                .send_message_with_file(&1_u64, first, chunk as u64, chunk, &admission)
                .await?,
            chunk as u64
        );
        assert_eq!(stream.payload_buffer.len(), chunk, "{profile:?}");
        let before = observed.lock().unwrap().reads.len();
        assert_eq!(
            stream
                .send_message_with_file(
                    &2_u64,
                    reader(&observed, &payload),
                    chunk as u64,
                    chunk,
                    &admission
                )
                .await?,
            chunk as u64
        );
        let observed = observed.lock().unwrap();
        // the file before, still in the storage, proves the buffer was kept and not replaced
        assert_eq!(observed.reads[before].1, chunk);
        assert!(observed.found[before].iter().all(|byte| *byte == 0xaa));
        assert_eq!(stream.payload_buffer.len(), chunk);
    }
    Ok(())
}

#[tokio::test]
async fn the_payload_buffer_grows_only_to_the_chunk_a_file_needs() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let (_, stream, observed) = fixture(b"");
    let mut stream = stream.with_payload_buffer_retention_limit(16);
    for (size, kept) in [(0, 0), (1, 1), (8, 8), (4, 8), (16, 16), (2, 16)] {
        let bytes = vec![0x7b; size];
        stream
            .send_message_with_file(
                &1_u64,
                reader(&observed, &bytes),
                size as u64,
                64,
                &admission,
            )
            .await?;
        // no spare capacity: the retention bound covers the whole allocation
        assert_eq!(stream.payload_buffer.len(), kept);
        assert_eq!(stream.payload_buffer.capacity(), kept);
    }
    Ok(())
}

#[tokio::test]
async fn a_chunk_above_the_limit_uses_temporary_storage_without_shrinking_reads()
-> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let (first, stream, observed) = fixture(&[0xaa; 8]);
    let mut stream = stream.with_payload_buffer_retention_limit(8);
    stream
        .send_message_with_file(&1_u64, first, 8, 8, &admission)
        .await?;
    let pointer = stream.payload_buffer.as_ptr() as usize;
    stream
        .send_message_with_file(&2_u64, reader(&observed, &[0x59; 32]), 32, 32, &admission)
        .await?;
    {
        let observed = observed.lock().unwrap();
        // one read of the whole chunk, into storage that was initialized for this file alone
        assert_eq!(observed.reads[1].1, 32);
        assert_eq!(observed.found[1], [0; 32]);
    }
    // the kept storage is neither replaced by the larger one nor given up for it
    assert_eq!(stream.payload_buffer.as_ptr() as usize, pointer);
    assert_eq!(stream.payload_buffer.len(), 8);
    stream
        .send_message_with_file(&3_u64, reader(&observed, b"abc"), 3, 8, &admission)
        .await?;
    let observed = observed.lock().unwrap();
    assert_eq!(observed.reads[2], (pointer, 3));
    assert_eq!(observed.found[2], [0xaa; 3]);
    Ok(())
}

#[tokio::test]
async fn without_a_retention_limit_every_file_reads_into_fresh_storage() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let (first, mut stream, observed) = fixture(&[0xaa; 8]);
    assert_eq!(stream.payload_buffer_retention_limit(), 0);
    stream
        .send_message_with_file(&1_u64, first, 8, 8, &admission)
        .await?;
    assert_eq!(stream.payload_buffer.capacity(), 0);
    stream
        .send_message_with_file(&2_u64, reader(&observed, b"abc"), 3, 8, &admission)
        .await?;
    assert_eq!(stream.payload_buffer.capacity(), 0);
    assert_eq!(observed.lock().unwrap().found[1], [0; 3]);
    Ok(())
}

#[tokio::test]
async fn lowering_or_disabling_the_limit_releases_kept_payload_storage() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let (first, stream, observed) = fixture(&[0xaa; 16]);
    let mut stream = stream.with_payload_buffer_retention_limit(16);
    stream
        .send_message_with_file(&1_u64, first, 16, 16, &admission)
        .await?;
    assert_eq!(stream.payload_buffer.len(), 16);
    // a limit the kept storage still fits leaves it alone
    stream = stream.with_payload_buffer_retention_limit(16);
    assert_eq!(stream.payload_buffer.len(), 16);
    stream = stream.with_payload_buffer_retention_limit(8);
    assert_eq!(stream.payload_buffer.capacity(), 0);
    stream
        .send_message_with_file(&2_u64, reader(&observed, &[0x59; 8]), 8, 16, &admission)
        .await?;
    assert_eq!(stream.payload_buffer.len(), 8);
    stream = stream.with_payload_buffer_retention_limit(0);
    assert_eq!(stream.payload_buffer.capacity(), 0);
    Ok(())
}

#[tokio::test]
async fn debug_formats_metadata_without_writer_or_payload_contents() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let secret = b"private-source-payload";
    // the writer deliberately has no Debug implementation; its contents must not be formatted
    let (first, stream, _) = fixture(secret);
    let mut stream = stream.with_payload_buffer_retention_limit(64);
    stream
        .send_message_with_file(&1_u64, first, secret.len() as u64, 64, &admission)
        .await?;
    let debug = format!("{stream:?}");
    assert!(debug.contains(&format!("payload_buffer_len: {}", secret.len())));
    assert!(debug.contains("payload_buffer_retention_limit: 64"));
    assert!(!debug.contains(std::str::from_utf8(secret)?));
    assert!(!debug.contains(&format!("{secret:?}")));
    assert!(debug.len() < 128);
    Ok(())
}

pub(super) fn run_in_child(test: &str) -> anyhow::Result<bool> {
    const ENV: &str = "RCP_REMOTE_SOURCE_ADMISSION_TEST";
    if std::env::var(ENV).as_deref() == Ok(test) {
        return Ok(false);
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", test, "--nocapture"])
        .env(ENV, test)
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    let done = format!("completed: {test}");
    assert!(
        stdout.contains(&done) || stderr.contains(&done),
        "child did not complete {test}: {stdout}\n{stderr}"
    );
    Ok(true)
}

pub(super) fn completed(test: &str) {
    println!("completed: {test}");
}

#[test]
fn cancelling_a_queued_read_drops_its_owner_without_reading() -> anyhow::Result<()> {
    const TEST: &str =
        "streams::owned_source_tests::cancelling_a_queued_read_drops_its_owner_without_reading";
    if run_in_child(TEST)? {
        return Ok(());
    }
    throttle::set_admission_limits(std::num::NonZeroUsize::new(1));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let admission = throttle::open_file_permit().await;
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            let _ = wait.recv_timeout(std::time::Duration::from_secs(10));
        });
        start.await?;
        let (reader, mut stream, observed) = fixture(b"abc");
        let mut send = Box::pin(stream.send_message_with_file(&1_u64, reader, 3, 3, &admission));
        assert!(futures::poll!(send.as_mut()).is_pending());
        let mut next = Box::pin(throttle::open_file_permit());
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(send);
        let (dropped, unread) = {
            let observed = observed.lock().unwrap();
            (observed.dropped, observed.reads.is_empty())
        };
        drop(admission);
        // capacity must return while the only blocking worker is still occupied
        let replacement = tokio::time::timeout(std::time::Duration::from_secs(5), next).await;
        drop(release);
        blocker.await?;
        assert!(dropped && unread);
        let _replacement = replacement?;
        completed(TEST);
        anyhow::Ok(())
    })
}

#[tokio::test]
async fn cancelling_a_started_read_leaves_its_owner_with_the_worker() -> anyhow::Result<()> {
    const TEST: &str =
        "streams::owned_source_tests::cancelling_a_started_read_leaves_its_owner_with_the_worker";
    if run_in_child(TEST)? {
        return Ok(());
    }
    throttle::set_admission_limits(std::num::NonZeroUsize::new(1));
    let admission = throttle::open_file_permit().await;
    let (mut reader, mut stream, observed) = fixture(b"abc");
    let (started, start) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    reader.gate = Some((started, wait));
    // no ambient admission scope: each read must use the explicit guard
    let mut send = Box::pin(stream.send_message_with_file(&1_u64, reader, 3, 3, &admission));
    assert!(futures::poll!(send.as_mut()).is_pending());
    tokio::time::timeout(std::time::Duration::from_secs(5), start).await??;
    drop(send);
    drop(admission);
    let held = !observed.lock().unwrap().dropped;
    let mut next = Box::pin(throttle::open_file_permit());
    let admitted_early = futures::poll!(next.as_mut()).is_ready();
    release.send(())?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !observed.lock().unwrap().dropped {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(held);
    assert!(
        !admitted_early,
        "cancelled read returned admission before its owner dropped"
    );
    let _next = tokio::time::timeout(std::time::Duration::from_secs(5), next).await?;
    assert!(observed.lock().unwrap().writes.is_empty());
    completed(TEST);
    Ok(())
}

struct CompletionWake(Mutex<Option<tokio::sync::oneshot::Sender<()>>>);

impl futures::task::ArcWake for CompletionWake {
    fn wake_by_ref(this: &Arc<Self>) {
        if let Some(sender) = this.0.lock().unwrap().take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test]
async fn cancelling_completed_unobserved_output_drops_its_owner() -> anyhow::Result<()> {
    const TEST: &str =
        "streams::owned_source_tests::cancelling_completed_unobserved_output_drops_its_owner";
    if run_in_child(TEST)? {
        return Ok(());
    }
    throttle::set_admission_limits(std::num::NonZeroUsize::new(1));
    let admission = throttle::open_file_permit().await;
    let (mut reader, mut stream, observed) = fixture(b"abc");
    let (started, start) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    reader.gate = Some((started, wait));
    let (ready, completion) = tokio::sync::oneshot::channel();
    let waker = futures::task::waker(Arc::new(CompletionWake(Mutex::new(Some(ready)))));
    let mut context = Context::from_waker(&waker);
    let mut send = Box::pin(stream.send_message_with_file(&1_u64, reader, 3, 3, &admission));
    assert!(std::future::Future::poll(send.as_mut(), &mut context).is_pending());
    tokio::time::timeout(std::time::Duration::from_secs(5), start).await??;
    release.send(())?;
    tokio::time::timeout(std::time::Duration::from_secs(5), completion).await??;
    assert!(!observed.lock().unwrap().dropped);
    let mut next = Box::pin(throttle::open_file_permit());
    assert!(futures::poll!(next.as_mut()).is_pending());
    drop(send);
    {
        let observed = observed.lock().unwrap();
        assert!(observed.dropped);
        assert_eq!(observed.reads.len(), 1);
        assert!(observed.writes.is_empty());
    }
    drop(admission);
    let _replacement = tokio::time::timeout(std::time::Duration::from_secs(5), next).await?;
    completed(TEST);
    Ok(())
}
