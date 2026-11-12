use super::*;

#[test]
fn test_rcpd_type_display() {
    assert_eq!(format!("{}", RcpdType::Source), "source");
    assert_eq!(format!("{}", RcpdType::Destination), "destination");
}

#[test]
fn test_rcpd_type_debug() {
    assert_eq!(format!("{:?}", RcpdType::Source), "Source");
    assert_eq!(format!("{:?}", RcpdType::Destination), "Destination");
}

fn preview(message: &str) -> common::remote_tracing::TracingMessage {
    common::remote_tracing::TracingMessage::Log {
        timestamp: std::time::UNIX_EPOCH,
        level: "INFO".to_owned(),
        target: "rcp::source".to_owned(),
        message: message.to_owned(),
    }
}

async fn receive_previews(reader: tokio::io::DuplexStream) -> anyhow::Result<Vec<String>> {
    let mut stream = crate::streams::RecvStream::new(reader);
    let mut messages = Vec::new();
    while let Some(message) = stream.recv_object().await? {
        match message {
            common::remote_tracing::TracingMessage::Log { message, .. } => messages.push(message),
            common::remote_tracing::TracingMessage::Progress(_) => panic!("unexpected progress"),
        }
    }
    Ok(messages)
}

#[tokio::test]
async fn shutdown_drains_queued_messages_with_live_producers() -> anyhow::Result<()> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    for name in ["first.txt", "second.txt", "last.txt"] {
        sender.send(preview(name))?;
    }
    let shutdown = tokio_util::sync::CancellationToken::new();
    shutdown.cancel();
    let (writer, reader) = tokio::io::duplex(1);
    let send = run_sender(receiver, crate::streams::SendStream::new(writer), shutdown);
    let receive = async {
        // keep the producer alive and gate reads on closed admission. The old sender either
        // discards the queue or blocks mid-frame; neither selection order can pass this test
        sender.closed().await;
        assert!(sender.send(preview("too-late.txt")).is_err());
        receive_previews(reader).await
    };
    let (_, messages) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::try_join!(send, receive)
    })
    .await
    .expect("shutdown must close admission before draining queued messages")?;
    assert_eq!(messages, ["first.txt", "second.txt", "last.txt"]);
    Ok(())
}

#[tokio::test]
async fn shutdown_finishes_an_in_flight_frame_before_draining() -> anyhow::Result<()> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    sender.send(preview("in-flight.txt"))?;
    sender.send(preview("queued.txt"))?;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let (writer, reader) = tokio::io::duplex(1);
    let send = run_sender(
        receiver,
        crate::streams::SendStream::new(writer),
        shutdown.clone(),
    );
    tokio::pin!(send);
    // one byte fits, so the first frame is suspended before cancellation arrives
    assert!(futures::poll!(&mut send).is_pending());
    shutdown.cancel();
    let (_, messages) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::try_join!(send, receive_previews(reader))
    })
    .await
    .expect("an in-flight frame and its queued successor must finish")?;
    assert_eq!(messages, ["in-flight.txt", "queued.txt"]);
    assert!(sender.is_closed());
    Ok(())
}

#[tokio::test]
async fn shutdown_closes_an_empty_queue_with_live_producers() -> anyhow::Result<()> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let (writer, reader) = tokio::io::duplex(1);
    let send = run_sender(
        receiver,
        crate::streams::SendStream::new(writer),
        shutdown.clone(),
    );
    tokio::pin!(send);
    assert!(futures::poll!(&mut send).is_pending());
    shutdown.cancel();
    let (_, messages) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::try_join!(send, receive_previews(reader))
    })
    .await
    .expect("empty shutdown must not wait for live producers")?;
    assert!(messages.is_empty());
    assert!(sender.is_closed());
    Ok(())
}

#[tokio::test]
async fn producer_closure_drains_messages_without_cancellation() -> anyhow::Result<()> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    sender.send(preview("first.txt"))?;
    sender.send(preview("last.txt"))?;
    drop(sender);
    let (writer, reader) = tokio::io::duplex(1);
    let send = run_sender(
        receiver,
        crate::streams::SendStream::new(writer),
        tokio_util::sync::CancellationToken::new(),
    );
    let (_, messages) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::try_join!(send, receive_previews(reader))
    })
    .await
    .expect("producer closure must drain queued messages")?;
    assert_eq!(messages, ["first.txt", "last.txt"]);
    Ok(())
}

#[tokio::test]
async fn disconnected_peer_preserves_the_transport_error() -> anyhow::Result<()> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    sender.send(preview("first.txt"))?;
    sender.send(preview("last.txt"))?;
    drop(sender);
    let (writer, reader) = tokio::io::duplex(1);
    drop(reader);
    let error = run_sender(
        receiver,
        crate::streams::SendStream::new(writer),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect_err("a failed frame must stop forwarding and retain its cause");
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    Ok(())
}
