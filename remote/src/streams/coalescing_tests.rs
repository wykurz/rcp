use super::*;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Written {
    bytes: Vec<u8>,
    flushes: usize,
    max_write: usize,
    blocked: bool,
    wake: Option<std::task::Waker>,
}

struct Writer(Arc<Mutex<Written>>);

impl AsyncWrite for Writer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut state = self.0.lock().unwrap();
        state.max_write = state.max_write.max(bytes.len());
        if state.blocked {
            state.wake = Some(cx.waker().clone());
            return std::task::Poll::Pending;
        }
        state.bytes.extend_from_slice(bytes);
        std::task::Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.0.lock().unwrap().flushes += 1;
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn grouped_control_frames_share_one_flush() -> anyhow::Result<()> {
    for count in [1, 7, 64] {
        let messages: Vec<_> = (0..count)
            .map(|i| crate::protocol::SourceMessage::FileUnchanged {
                src: format!("s/{i}").into(),
                dst: format!("d/{i}").into(),
            })
            .collect();
        let grouped = Arc::new(Mutex::new(Written::default()));
        let separate = Arc::new(Mutex::new(Written::default()));
        SendStream::new(Writer(grouped.clone()))
            .send_control_messages(&messages)
            .await?;
        let mut individual = SendStream::new(Writer(separate.clone()));
        for message in &messages {
            individual.send_control_message(message).await?;
        }
        let bytes = grouped.lock().unwrap().bytes.clone();
        assert_eq!(bytes, separate.lock().unwrap().bytes);
        assert_eq!(grouped.lock().unwrap().flushes, 1);
        assert!(separate.lock().unwrap().flushes >= count);
        let mut recv = RecvStream::new(bytes.as_slice());
        for expected in &messages {
            let actual = recv
                .recv_object::<crate::protocol::SourceMessage>()
                .await?
                .unwrap();
            assert_eq!(bitcode::serialize(&actual)?, bitcode::serialize(expected)?);
        }
        assert!(
            recv.recv_object::<crate::protocol::SourceMessage>()
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn grouped_control_frames_retain_writer_backpressure() -> anyhow::Result<()> {
    let messages: Vec<_> = (0..64).map(|_| "x".repeat(1024)).collect();
    let state = Arc::new(Mutex::new(Written {
        blocked: true,
        ..Default::default()
    }));
    let mut sender = SendStream::new(Writer(state.clone()));
    let boundary = sender.framed.backpressure_boundary();
    let frame_size = bitcode::serialize(&messages[0])?.len() + 4;
    {
        let future = sender.send_control_messages(&messages);
        tokio::pin!(future);
        assert!(futures::poll!(&mut future).is_pending());
        {
            let mut written = state.lock().unwrap();
            assert!(written.bytes.is_empty());
            assert!(written.max_write >= boundary);
            assert!(written.max_write < boundary + frame_size);
            written.blocked = false;
            written.wake.take().unwrap().wake();
        }
        future.await?;
    }
    let bytes = {
        let written = state.lock().unwrap();
        assert!(
            written.flushes > 1,
            "feed must honor the existing backpressure bound"
        );
        assert!(written.max_write < boundary + frame_size);
        written.bytes.clone()
    };
    let mut recv = RecvStream::new(bytes.as_slice());
    for message in messages {
        assert_eq!(recv.recv_object::<String>().await?, Some(message));
    }
    assert!(recv.recv_object::<String>().await?.is_none());
    Ok(())
}
