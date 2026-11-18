use super::copy_buffer_tests::{Reader, TEST_RETENTION_LIMIT, append_frame, recv_stream};
use super::file_buffer_size;
use super::owned_source_tests::{completed, run_in_child};
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};

/// The byte slices a blocking writer was given, one entry per write call.
#[derive(Clone, Default)]
struct Writes(Arc<Mutex<Vec<Vec<u8>>>>);

impl Writes {
    fn sizes(&self) -> Vec<usize> {
        self.0.lock().unwrap().iter().map(Vec::len).collect()
    }
    fn bytes(&self) -> Vec<u8> {
        self.0.lock().unwrap().concat()
    }
}

struct Writer {
    writes: Writes,
    // fail the write that would start at this many bytes already written
    fail_at: Option<usize>,
    panic_on_write: bool,
}

impl Writer {
    fn new(writes: &Writes) -> Self {
        Self {
            writes: writes.clone(),
            fail_at: None,
            panic_on_write: false,
        }
    }
}

impl std::io::Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        assert!(!self.panic_on_write, "injected destination write panic");
        if self.fail_at == Some(self.writes.bytes().len()) {
            return Err(ErrorKind::StorageFull.into());
        }
        self.writes.0.lock().unwrap().push(bytes.to_vec());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn payloads_reach_the_writer_whole_and_frame_boundaries_hold() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let payloads: Vec<Vec<u8>> = [0, 3, 127, 8193, 1, 32769, 0, 19]
        .into_iter()
        .enumerate()
        .map(|(index, size)| {
            (0..size)
                .map(|offset| ((offset + index * 17) % 251) as u8)
                .collect()
        })
        .collect();
    let mut wire = Vec::new();
    for payload in &payloads {
        append_frame(&mut wire, payload.len() as u64)?;
        wire.extend_from_slice(payload);
    }
    for max_read in [1, 7, 8192, usize::MAX] {
        let mut recv = recv_stream(Reader::new(wire.clone(), max_read));
        for payload in &payloads {
            let size = recv.recv_object::<u64>().await?.unwrap();
            let writes = Writes::default();
            recv.copy_exact_to_blocking_writer(Writer::new(&writes), size, 1024, &admission)
                .await?;
            assert_eq!(&writes.bytes(), payload);
        }
        assert!(recv.recv_object::<u64>().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn every_write_carries_a_full_chunk_however_short_the_reads() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let payload: Vec<u8> = (0..2500).map(|offset| (offset % 251) as u8).collect();
    for max_read in [1, 7, 1000, usize::MAX] {
        let mut recv = recv_stream(Reader::new(payload.clone(), max_read));
        let writes = Writes::default();
        recv.copy_exact_to_blocking_writer(Writer::new(&writes), 2500, 1024, &admission)
            .await?;
        // a blocking write is a thread handoff: one per chunk, not one per read
        assert_eq!(writes.sizes(), [1024, 1024, 452]);
        assert_eq!(writes.bytes(), payload);
    }
    Ok(())
}

#[tokio::test]
async fn bytes_already_in_the_frame_buffer_are_written_first() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    // part of the payload arrived with its header, the rest is still on the wire
    let mut recv = recv_stream(Reader::new(b"defg".to_vec(), usize::MAX));
    recv.framed.read_buffer_mut().extend_from_slice(b"abc");
    let writes = Writes::default();
    recv.copy_exact_to_blocking_writer(Writer::new(&writes), 7, 16, &admission)
        .await?;
    assert_eq!(*writes.0.lock().unwrap(), [b"abcdefg".to_vec()]);
    assert!(recv.framed.read_buffer().is_empty());
    assert_eq!(recv.framed.get_ref().requests, [4]);
    // the whole payload and the next header arrived together: nothing is read, the header stays
    let mut wire = Vec::new();
    append_frame(&mut wire, 2)?;
    wire.extend_from_slice(b"xy");
    append_frame(&mut wire, 42)?;
    let mut recv = recv_stream(wire.as_slice());
    assert_eq!(recv.recv_object::<u64>().await?, Some(2));
    let writes = Writes::default();
    recv.copy_exact_to_blocking_writer(Writer::new(&writes), 2, 8192, &admission)
        .await?;
    assert_eq!(*writes.0.lock().unwrap(), [b"xy".to_vec()]);
    assert_eq!(recv.recv_object::<u64>().await?, Some(42));
    Ok(())
}

#[tokio::test]
async fn an_empty_payload_touches_neither_the_writer_nor_the_scratch() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let mut recv = recv_stream(Reader::new(vec![1; 8], usize::MAX));
    let writes = Writes::default();
    recv.copy_exact_to_blocking_writer(Writer::new(&writes), 0, 8192, &admission)
        .await?;
    assert!(writes.sizes().is_empty());
    assert!(recv.framed.get_ref().requests.is_empty());
    assert!(recv.copy_buffer.is_empty());
    Ok(())
}

#[tokio::test]
async fn file_copies_reuse_the_scratch_of_the_connection() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let mut recv = recv_stream(tokio::io::repeat(0xa5));
    let writes = Writes::default();
    recv.copy_exact_to_blocking_writer(Writer::new(&writes), 16384, 16384, &admission)
        .await?;
    let pointer = recv.copy_buffer.as_ptr();
    assert_eq!(recv.copy_buffer.len(), 16384);
    for size in [8192, 1024, 16384, 0, 4096] {
        recv.copy_exact_to_blocking_writer(Writer::new(&writes), size, 8192, &admission)
            .await?;
        assert_eq!(recv.copy_buffer.as_ptr(), pointer);
        assert_eq!(recv.copy_buffer.len(), 16384);
        // the untouched tail proves smaller copies neither truncate nor reinitialize storage
        assert!(recv.copy_buffer.iter().all(|byte| *byte == 0xa5));
    }
    // a drain on the same connection uses the same storage
    recv.copy_exact_to_buffered(&mut tokio::io::sink(), 4096, 8192)
        .await?;
    assert_eq!(recv.copy_buffer.as_ptr(), pointer);
    Ok(())
}

#[tokio::test]
async fn the_scratch_grows_only_to_the_chunk_a_file_needs() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let mut recv = recv_stream(tokio::io::repeat(0x7b));
    for size in [1, 8192, TEST_RETENTION_LIMIT] {
        recv.copy_exact_to_blocking_writer(
            Writer::new(&Writes::default()),
            size as u64,
            16 * 1024 * 1024,
            &admission,
        )
        .await?;
        assert_eq!(recv.copy_buffer.len(), size);
        assert!(recv.copy_buffer.iter().all(|byte| *byte == 0x7b));
    }
    Ok(())
}

#[tokio::test]
async fn a_chunk_above_the_limit_stays_temporary_without_shrinking_reads() -> anyhow::Result<()> {
    let admission = throttle::open_file_permit().await;
    let size = TEST_RETENTION_LIMIT + 1;
    let mut recv = recv_stream(Reader::new(vec![0x59; size + 16], usize::MAX));
    recv.copy_exact_to_blocking_writer(Writer::new(&Writes::default()), 8, 8, &admission)
        .await?;
    let pointer = recv.copy_buffer.as_ptr();
    let writes = Writes::default();
    recv.copy_exact_to_blocking_writer(Writer::new(&writes), size as u64, size, &admission)
        .await?;
    assert_eq!(recv.framed.get_ref().requests, [8, size]);
    assert_eq!(writes.sizes(), [size]);
    assert_eq!(recv.copy_buffer.as_ptr(), pointer);
    assert_eq!(recv.copy_buffer.len(), 8);
    // with retention disabled nothing is kept at all
    let mut recv = recv_stream(tokio::io::repeat(0)).with_copy_buffer_retention_limit(0);
    for _ in 0..2 {
        recv.copy_exact_to_blocking_writer(Writer::new(&Writes::default()), 16, 16, &admission)
            .await?;
        assert!(recv.copy_buffer.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn a_truncated_payload_reports_the_shortfall_and_no_partial_chunk_is_written() {
    let admission = throttle::open_file_permit().await;
    let mut recv = recv_stream(Reader::new(b"abcdef".to_vec(), 1));
    let writes = Writes::default();
    let error = recv
        .copy_exact_to_blocking_writer(Writer::new(&writes), 7, 4, &admission)
        .await
        .err()
        .unwrap();
    assert_eq!(error.to_string(), "unexpected EOF: expected 7 bytes, got 6");
    // the chunk that never filled is not written: the copy has failed either way
    assert_eq!(*writes.0.lock().unwrap(), [b"abcd".to_vec()]);
}

#[tokio::test]
async fn read_and_write_errors_keep_their_io_cause() {
    let admission = throttle::open_file_permit().await;
    for fail_read in [false, true] {
        let mut reader = Reader::new(vec![1; 20], 3);
        let writes = Writes::default();
        let mut writer = Writer::new(&writes);
        let expected = if fail_read {
            reader.fail_at = Some(12);
            ErrorKind::ConnectionReset
        } else {
            writer.fail_at = Some(8);
            ErrorKind::StorageFull
        };
        let mut recv = recv_stream(reader);
        let error = recv
            .copy_exact_to_blocking_writer(writer, 20, 8, &admission)
            .await
            .err()
            .unwrap();
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            expected
        );
        // one whole chunk reached the writer before either failure
        assert_eq!(writes.sizes(), [8]);
    }
}

#[tokio::test]
async fn a_panicking_writer_fails_the_copy_with_its_cause() {
    let admission = throttle::open_file_permit().await;
    let mut recv = recv_stream(tokio::io::repeat(1));
    let mut writer = Writer::new(&Writes::default());
    writer.panic_on_write = true;
    let error = recv
        .copy_exact_to_blocking_writer(writer, 8, 8, &admission)
        .await
        .err()
        .unwrap();
    assert_eq!(error.to_string(), "failed to write destination payload");
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
}

/// A writer whose first write waits to be released, and which says when it is dropped.
struct GatedWriter {
    started: Option<tokio::sync::oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}

impl std::io::Write for GatedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
            self.release
                .recv_timeout(std::time::Duration::from_secs(10))
                .map_err(std::io::Error::other)?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for GatedWriter {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[tokio::test]
async fn cancelling_a_started_write_leaves_its_writer_with_the_worker() -> anyhow::Result<()> {
    const TEST: &str =
        "streams::file_copy_tests::cancelling_a_started_write_leaves_its_writer_with_the_worker";
    if run_in_child(TEST)? {
        return Ok(());
    }
    throttle::set_admission_limits(std::num::NonZeroUsize::new(1));
    let admission = throttle::open_file_permit().await;
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (started, start) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let writer = GatedWriter {
        started: Some(started),
        release: wait,
        dropped: dropped.clone(),
    };
    let mut recv = recv_stream(tokio::io::repeat(1));
    // no ambient admission scope: each write must use the explicit guard
    let mut copy = Box::pin(recv.copy_exact_to_blocking_writer(writer, 8, 8, &admission));
    assert!(futures::poll!(copy.as_mut()).is_pending());
    tokio::time::timeout(std::time::Duration::from_secs(5), start).await??;
    drop(copy);
    drop(admission);
    let held = !dropped.load(std::sync::atomic::Ordering::Relaxed);
    let mut next = Box::pin(throttle::open_file_permit());
    let admitted_early = futures::poll!(next.as_mut()).is_ready();
    release.send(())?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !dropped.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    // the write that had started owns the descriptor until it returns, and the slot with it
    assert!(held);
    assert!(
        !admitted_early,
        "cancelled write returned admission before its writer dropped"
    );
    let _next = tokio::time::timeout(std::time::Duration::from_secs(5), next).await?;
    // a cancelled copy keeps no scratch: its connection is discarded
    assert!(recv.copy_buffer.is_empty());
    completed(TEST);
    Ok(())
}

#[test]
fn the_chunk_bound_follows_configuration_and_file_size() {
    let default = crate::TcpConfig::default().effective_buffer_size();
    let larger = 32 * 1024 * 1024;
    for (size, configured, expected) in [
        (0, default, 1),
        (1, default, 1),
        (4095, 4096, 4095),
        (4096, 4096, 4096),
        (4097, 4096, 4096),
        (16384, 0, 1),
        (16384, 1, 1),
        (0, 0, 1),
        (default as u64 - 1, default, default - 1),
        (default as u64, default, default),
        (default as u64 + 1, default, default),
        (larger as u64 + 1, larger, larger),
        (u64::MAX, default, default),
        (u64::MAX, larger, larger),
    ] {
        assert_eq!(file_buffer_size(size, configured), expected);
    }
}

#[test]
fn the_chunk_bound_caps_sizes_before_narrowing() {
    let maximum = isize::MAX as usize;
    for (size, configured, expected) in [
        (maximum as u64 - 1, usize::MAX, maximum - 1),
        (maximum as u64, usize::MAX, maximum),
        (maximum as u64 + 1, usize::MAX, maximum),
        (u64::MAX, usize::MAX, maximum),
        (u64::MAX, 7, 7),
        (7, usize::MAX, 7),
    ] {
        assert_eq!(file_buffer_size(size, configured), expected);
    }
}
