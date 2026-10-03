use super::*;

const TEST_RETENTION_LIMIT: usize = 256 * 1024;

fn recv_stream<R: AsyncRead + Unpin>(reader: R) -> RecvStream<R> {
    RecvStream::new(reader).with_copy_buffer_retention_limit(TEST_RETENTION_LIMIT)
}

struct Reader {
    bytes: Vec<u8>,
    offset: usize,
    max_read: usize,
    requests: Vec<usize>,
    fail_at: Option<usize>,
}

impl Reader {
    fn new(bytes: Vec<u8>, max_read: usize) -> Self {
        Self {
            bytes,
            offset: 0,
            max_read,
            requests: Vec::new(),
            fail_at: None,
        }
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.requests.push(buf.remaining());
        let end = self.fail_at.unwrap_or(self.bytes.len());
        if self.fail_at == Some(self.offset) {
            return std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()));
        }
        let count = buf.remaining().min(self.max_read).min(end - self.offset);
        buf.put_slice(&self.bytes[self.offset..self.offset + count]);
        self.offset += count;
        std::task::Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
    fail_at: Option<usize>,
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.fail_at == Some(self.bytes.len()) {
            return std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
        let count = bytes.len().min(3).min(
            self.fail_at
                .map_or(usize::MAX, |limit| limit - self.bytes.len()),
        );
        self.bytes.extend_from_slice(&bytes[..count]);
        std::task::Poll::Ready(Ok(count))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

fn append_frame(wire: &mut Vec<u8>, value: u64) -> anyhow::Result<()> {
    let bytes = bitcode::serialize(&value)?;
    wire.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    wire.extend_from_slice(&bytes);
    Ok(())
}

#[tokio::test]
async fn empty_and_fully_buffered_payloads_need_no_scratch() -> anyhow::Result<()> {
    let mut wire = Vec::new();
    append_frame(&mut wire, 0)?;
    append_frame(&mut wire, 3)?;
    wire.extend_from_slice(b"abc");
    append_frame(&mut wire, 42)?;
    let mut recv = recv_stream(wire.as_slice());
    let mut written = Vec::new();
    assert!(recv.copy_buffer.is_empty());
    assert_eq!(recv.recv_object::<u64>().await?, Some(0));
    let buffered = recv.framed.read_buffer().clone();
    assert_eq!(recv.copy_exact_to_buffered(&mut written, 0, 0).await?, 0);
    assert_eq!(recv.framed.read_buffer(), &buffered);
    assert_eq!(recv.recv_object::<u64>().await?, Some(3));
    assert!(recv.framed.read_buffer().len() > 3);
    assert_eq!(recv.copy_exact_to_buffered(&mut written, 3, 0).await?, 3);
    assert_eq!(written, b"abc");
    assert!(recv.copy_buffer.is_empty());
    assert_eq!(recv.recv_object::<u64>().await?, Some(42));
    Ok(())
}

#[tokio::test]
async fn alternating_headers_and_payloads_preserve_boundaries() -> anyhow::Result<()> {
    let payloads: Vec<Vec<u8>> = [0, 3, 127, 8193, 1, 32769, 0, 19]
        .into_iter()
        .enumerate()
        .map(|(index, size)| vec![index as u8; size])
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
            let mut writer = Writer::default();
            assert_eq!(
                recv.copy_exact_to_buffered(&mut writer, size, 1024).await?,
                size
            );
            assert_eq!(&writer.bytes, payload);
        }
        assert!(recv.recv_object::<u64>().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn scratch_reuses_initialized_storage_across_copies_and_drains() -> anyhow::Result<()> {
    let mut recv = recv_stream(tokio::io::repeat(0xa5));
    let mut sink = tokio::io::sink();
    recv.copy_exact_to_buffered(&mut sink, 16384, 16384).await?;
    let pointer = recv.copy_buffer.as_ptr();
    for size in [8192, 1024, 16384, 0, 4096] {
        recv.copy_exact_to_buffered(&mut sink, size, 8192).await?;
        assert_eq!(recv.copy_buffer.as_ptr(), pointer);
        assert_eq!(recv.copy_buffer.len(), 16384);
        // the untouched tail proves smaller copies neither truncate nor reinitialize storage
        assert!(recv.copy_buffer.iter().all(|byte| *byte == 0xa5));
    }
    Ok(())
}

#[tokio::test]
async fn scratch_grows_only_to_needed_payload_and_retention_limit() -> anyhow::Result<()> {
    let mut recv = recv_stream(tokio::io::repeat(0x7b));
    let mut sink = tokio::io::sink();
    for size in [1, 8192, TEST_RETENTION_LIMIT] {
        recv.copy_exact_to_buffered(&mut sink, size as u64, 16 * 1024 * 1024)
            .await?;
        // boxed slices have no uninitialized or spare capacity beyond their length
        assert_eq!(recv.copy_buffer.len(), size);
        assert!(recv.copy_buffer.iter().all(|byte| *byte == 0x7b));
    }
    Ok(())
}

#[tokio::test]
async fn profile_sized_chunks_reuse_initialized_storage() -> anyhow::Result<()> {
    for profile in [
        crate::NetworkProfile::Internet,
        crate::NetworkProfile::Datacenter,
    ] {
        let config = crate::TcpConfig {
            network_profile: profile,
            ..Default::default()
        };
        let chunk = config.effective_buffer_size();
        let mut recv = RecvStream::new(tokio::io::repeat(0x59))
            .with_copy_buffer_retention_limit(config.effective_buffer_retention_limit());
        assert!(recv.copy_buffer.is_empty());
        let mut sink = tokio::io::sink();
        recv.copy_exact_to_buffered(&mut sink, chunk as u64 * 2, chunk)
            .await?;
        let pointer = recv.copy_buffer.as_ptr();
        for size in [chunk, 8192, chunk] {
            recv.copy_exact_to_buffered(&mut sink, size as u64, size)
                .await?;
            assert_eq!(recv.copy_buffer.as_ptr(), pointer);
            assert_eq!(recv.copy_buffer.len(), chunk);
            assert!(recv.copy_buffer.iter().all(|byte| *byte == 0x59));
        }
    }
    Ok(())
}

#[tokio::test]
async fn lowering_or_disabling_retention_releases_existing_storage() -> anyhow::Result<()> {
    let mut recv = recv_stream(Reader::new(vec![0x59; 4 * 16384], usize::MAX));
    let mut sink = tokio::io::sink();
    recv.copy_exact_to_buffered(&mut sink, 16384, 16384).await?;
    recv = recv.with_copy_buffer_retention_limit(8192);
    assert!(recv.copy_buffer.is_empty());
    recv.copy_exact_to_buffered(&mut sink, 8192, 16384).await?;
    assert_eq!(recv.copy_buffer.len(), 8192);
    recv = recv.with_copy_buffer_retention_limit(0);
    for _ in 0..2 {
        recv.copy_exact_to_buffered(&mut sink, 16384, 16384).await?;
        assert!(recv.copy_buffer.is_empty());
    }
    assert_eq!(recv.framed.get_ref().requests, [16384, 8192, 16384, 16384]);
    Ok(())
}

#[tokio::test]
async fn oversized_chunks_stay_temporary_without_shrinking_reads() -> anyhow::Result<()> {
    for size in [
        TEST_RETENTION_LIMIT + 1,
        crate::INTERNET_REMOTE_COPY_BUFFER_SIZE,
        crate::DATACENTER_REMOTE_COPY_BUFFER_SIZE,
    ] {
        let mut recv = recv_stream(Reader::new(vec![0x59; size + 16], usize::MAX));
        let mut sink = tokio::io::sink();
        recv.copy_exact_to_buffered(&mut sink, 8, 8).await?;
        let pointer = recv.copy_buffer.as_ptr();
        recv.copy_exact_to_buffered(&mut sink, size as u64, size)
            .await?;
        assert_eq!(recv.framed.get_ref().requests, [8, size]);
        assert_eq!(recv.copy_buffer.as_ptr(), pointer);
        assert_eq!(recv.copy_buffer.len(), 8);
        recv.copy_exact_to_buffered(&mut sink, 8, 8).await?;
        assert_eq!(recv.copy_buffer.as_ptr(), pointer);
    }
    let mut recv = recv_stream(tokio::io::repeat(0));
    recv.copy_exact_to_buffered(
        &mut tokio::io::sink(),
        TEST_RETENTION_LIMIT as u64 + 1,
        TEST_RETENTION_LIMIT + 1,
    )
    .await?;
    assert!(recv.copy_buffer.is_empty());
    Ok(())
}

#[tokio::test]
async fn drain_uses_requested_chunk_after_larger_copy_and_leaves_next_frame() -> anyhow::Result<()>
{
    let mut wire = vec![0x31; 16384 + 17000];
    append_frame(&mut wire, 91)?;
    let mut recv = recv_stream(Reader::new(wire, usize::MAX));
    recv.copy_exact_to_buffered(&mut tokio::io::sink(), 16384, 16384)
        .await?;
    let pointer = recv.copy_buffer.as_ptr();
    recv.copy_exact_to_buffered(&mut tokio::io::sink(), 17000, 8192)
        .await?;
    assert_eq!(recv.framed.get_ref().requests, [16384, 8192, 8192, 616]);
    assert_eq!(recv.copy_buffer.as_ptr(), pointer);
    assert_eq!(recv.recv_object::<u64>().await?, Some(91));
    Ok(())
}

#[tokio::test]
async fn partial_framed_payload_and_short_reads_report_truncation() {
    let mut recv = recv_stream(Reader::new(b"def".to_vec(), 1));
    recv.framed.read_buffer_mut().extend_from_slice(b"abc");
    let mut written = Vec::new();
    let error = recv
        .copy_exact_to_buffered(&mut written, 7, 4)
        .await
        .unwrap_err();
    assert_eq!(written, b"abcdef");
    assert_eq!(error.to_string(), "unexpected EOF: expected 7 bytes, got 6");
    assert!(recv.framed.read_buffer().is_empty());
    assert_eq!(recv.copy_buffer.len(), 4);
}

#[tokio::test]
async fn zero_chunk_with_unbuffered_payload_still_fails() {
    let mut recv = recv_stream(Reader::new(vec![1], 1));
    let error = recv
        .copy_exact_to_buffered(&mut tokio::io::sink(), 1, 0)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "unexpected EOF: expected 1 bytes, got 0");
    assert!(recv.framed.get_ref().requests.is_empty());
    assert!(recv.copy_buffer.is_empty());
}

#[tokio::test]
async fn read_and_write_errors_keep_their_io_cause() {
    for fail_read in [false, true] {
        let mut reader = Reader::new(vec![1; 20], 3);
        let mut writer = Writer::default();
        let expected = if fail_read {
            reader.fail_at = Some(5);
            std::io::ErrorKind::ConnectionReset
        } else {
            writer.fail_at = Some(5);
            std::io::ErrorKind::BrokenPipe
        };
        let mut recv = recv_stream(reader);
        let error = recv
            .copy_exact_to_buffered(&mut writer, 20, 8)
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            expected
        );
        assert_eq!(writer.bytes, vec![1; 5]);
    }
}

#[tokio::test]
async fn cancelled_pending_read_keeps_only_bounded_connection_scratch() -> anyhow::Result<()> {
    for size in [8192, TEST_RETENTION_LIMIT + 1] {
        let (reader, mut sender) = tokio::io::duplex(16);
        sender.write_all(&[1; 8]).await?;
        let mut recv = recv_stream(reader);
        let mut sink = tokio::io::sink();
        recv.copy_exact_to_buffered(&mut sink, 8, 8).await?;
        {
            let copy = recv.copy_exact_to_buffered(&mut sink, size as u64, size);
            tokio::pin!(copy);
            assert!(futures::poll!(&mut copy).is_pending());
        }
        assert_eq!(recv.copy_buffer.len(), if size == 8192 { 8192 } else { 8 });
        // cancellation corrupts a transfer; discard the connection instead of resuming it
        drop(recv);
        assert_eq!(sender.read(&mut [0]).await?, 0);
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_pending_write_does_not_retain_oversized_scratch() {
    let size = TEST_RETENTION_LIMIT + 1;
    let mut recv = recv_stream(tokio::io::repeat(1));
    let (mut writer, _reader) = tokio::io::duplex(1);
    {
        let copy = recv.copy_exact_to_buffered(&mut writer, size as u64, size);
        tokio::pin!(copy);
        assert!(futures::poll!(&mut copy).is_pending());
    }
    assert!(recv.copy_buffer.is_empty());
}
