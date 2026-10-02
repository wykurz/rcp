use super::*;
use remote::protocol::{DestinationMessage, DirectoryClass, DirectoryLimits, SourceMessage};
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

const TIMEOUT: Duration = Duration::from_secs(3);

struct ObservedRead {
    inner: tokio::io::DuplexStream,
    consumed: tokio::sync::watch::Sender<usize>,
}

impl AsyncRead for ObservedRead {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        let result = std::pin::Pin::new(&mut this.inner).poll_read(cx, buffer);
        let read = buffer.filled().len() - before;
        if read != 0 {
            this.consumed.send_modify(|count| *count += read);
        }
        result
    }
}

struct LifetimeReceiver {
    source: tokio::io::DuplexStream,
    consumed: tokio::sync::watch::Receiver<usize>,
    replies: remote::streams::RecvStream<tokio::io::DuplexStream>,
    send: remote::streams::BoxedSharedSendStream,
    tracker: directory_tracker::SharedDirectoryTracker,
    errors: Arc<common::error_collector::ErrorCollector>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl LifetimeReceiver {
    fn new() -> Self {
        let (source, input) = tokio::io::duplex(4096);
        let (consumed, observations) = tokio::sync::watch::channel(0);
        let (output, replies) = tokio::io::duplex(4096);
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(output) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let (job_send, job_tracker, job_errors) = (send.clone(), tracker.clone(), errors.clone());
        let task = tokio::spawn(async move {
            process_control_stream(
                Some(DirectoryLimits {
                    normal: NonZeroUsize::new(2).unwrap(),
                    reserve: NonZeroUsize::new(2).unwrap(),
                }),
                &test_copy_settings(),
                0,
                NonZeroUsize::MIN,
                NonZeroUsize::new(2).unwrap(),
                &common::preserve::preserve_none(),
                remote::streams::RecvStream::new(Box::new(ObservedRead {
                    inner: input,
                    consumed,
                }) as remote::streams::BoxedRead),
                job_tracker.clone(),
                job_send,
                test_pool(&job_tracker),
                job_errors,
            )
            .await
        });
        Self {
            source,
            consumed: observations,
            replies: remote::streams::RecvStream::new(replies),
            send,
            tracker,
            errors,
            task,
        }
    }

    async fn consumed(&mut self, expected: usize) {
        tokio::time::timeout(TIMEOUT, async {
            while *self.consumed.borrow_and_update() < expected {
                self.consumed.changed().await.unwrap();
            }
        })
        .await
        .expect("the production decoder must consume the supplied prefix");
        assert_eq!(*self.consumed.borrow(), expected);
    }

    async fn reply(&mut self) -> Option<DestinationMessage> {
        tokio::time::timeout(TIMEOUT, self.replies.recv_object())
            .await
            .expect("receiver reply timed out")
            .unwrap()
    }

    async fn completed(&mut self) {
        assert!(matches!(
            self.reply().await,
            Some(DestinationMessage::DestinationDone)
        ));
        assert!(
            self.reply().await.is_none(),
            "Done must be the sole terminal reply"
        );
        tokio::time::timeout(TIMEOUT, &mut self.task)
            .await
            .expect("control receiver did not return")
            .unwrap()
            .unwrap();
        assert!(self.errors.take_error().is_none());
        assert!(self.tracker.with_state(|state| state.transfer_complete()));
    }
}

impl Drop for LifetimeReceiver {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn frame(message: &SourceMessage) -> Vec<u8> {
    let mut frame = Vec::new();
    remote::streams::SendStream::new(&mut frame)
        .send_control_message(message)
        .await
        .unwrap();
    frame
}

async fn empty_directory_frames(root: &std::path::Path) -> Vec<u8> {
    let mut frames = frame(&SourceMessage::DirectoryBegin {
        src: root.to_owned(),
        dst: root.to_owned(),
        metadata: remote::protocol::Metadata::from(
            &std::fs::metadata(root.parent().unwrap()).unwrap(),
        ),
        is_root: true,
        keep_if_empty: true,
        admission: DirectoryClass::Normal,
    })
    .await;
    frames.extend(
        frame(&SourceMessage::DirectoryEnd {
            src: root.to_owned(),
            dst: root.to_owned(),
            entry_count: 0,
        })
        .await,
    );
    frames
}

#[tokio::test]
async fn directory_release_progresses_during_a_partial_control_frame() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let preparation = PreparationGate::new(&root, 0);
    let finalization = Arc::new(tokio::sync::Semaphore::new(0));
    let mut receiver = LifetimeReceiver::new();
    receiver.tracker.gate_finalization(finalization.clone());
    let frames = empty_directory_frames(&root).await;
    receiver.source.write_all(&frames).await.unwrap();
    preparation.started().await;
    let complete = frame(&SourceMessage::DiscoveryComplete {
        has_root_item: true,
    })
    .await;
    assert!(complete.len() > 5, "the test requires an incomplete frame");
    receiver.source.write_all(&complete[..5]).await.unwrap();
    receiver.consumed(frames.len() + 5).await;
    preparation.release();
    assert!(matches!(receiver.reply().await,
        Some(DestinationMessage::DirectoryReady { src, dst }) if src == root && dst == root));
    finalization.add_permits(1);
    // no further source bytes arrive until this asynchronous lifetime release is acknowledged
    assert!(matches!(receiver.reply().await,
        Some(DestinationMessage::DirectoryReleased { src, dst }) if src == root && dst == root));
    assert_eq!(*receiver.consumed.borrow(), frames.len() + 5);
    assert!(
        !receiver
            .tracker
            .with_state(|state| state.transfer_complete())
    );
    receiver.source.write_all(&complete[5..]).await.unwrap();
    receiver.completed().await;
}

#[tokio::test]
async fn queued_directory_release_progresses_during_buffered_control_messages() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let child = root.join("child");
    let mut receiver = LifetimeReceiver::new();
    let metadata = remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap());
    for (path, is_root) in [(&root, true), (&child, false)] {
        receiver
            .source
            .write_all(
                &frame(&SourceMessage::DirectoryBegin {
                    src: path.clone(),
                    dst: path.clone(),
                    metadata: metadata.clone(),
                    is_root,
                    keep_if_empty: true,
                    admission: DirectoryClass::Normal,
                })
                .await,
            )
            .await
            .unwrap();
    }
    let mut ready = std::collections::HashSet::new();
    for _ in 0..2 {
        let Some(DestinationMessage::DirectoryReady { dst, .. }) = receiver.reply().await else {
            panic!("each admitted directory must become ready before End");
        };
        ready.insert(dst);
    }
    assert_eq!(ready, [root.clone(), child.clone()].into_iter().collect());
    let alias = receiver
        .tracker
        .with_state(|state| state.get_dir(&child))
        .unwrap();
    receiver
        .source
        .write_all(
            &frame(&SourceMessage::DirectoryEnd {
                src: child.clone(),
                dst: child.clone(),
                entry_count: 0,
            })
            .await,
        )
        .await
        .unwrap();
    tokio::time::timeout(TIMEOUT, async {
        while Arc::strong_count(&alias) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("child finalization must leave only the external descriptor alias");

    let finalization = Arc::new(tokio::sync::Semaphore::new(0));
    receiver.tracker.gate_finalization(finalization.clone());
    let mut burst = Vec::new();
    for index in 0..16 {
        let path = root.join(format!("skipped-{index}"));
        burst.extend(
            frame(&SourceMessage::FileSkipped {
                src: path.clone(),
                dst: path,
            })
            .await,
        );
    }
    burst.extend(
        frame(&SourceMessage::DirectoryEnd {
            src: root.clone(),
            dst: root.clone(),
            entry_count: 17,
        })
        .await,
    );
    burst.extend(
        frame(&SourceMessage::DiscoveryComplete {
            has_root_item: true,
        })
        .await,
    );
    assert!(
        burst.len() < 4096,
        "the entire burst must fit in the input buffer"
    );
    // this current-thread test queues all input and the release without yielding. A receiver that
    // always chooses ready input reaches the blocked root finalizer without returning child credit.
    receiver.source.write_all(&burst).await.unwrap();
    drop(alias);
    assert!(matches!(receiver.reply().await,
        Some(DestinationMessage::DirectoryReleased { src, dst }) if src == child && dst == child));
    assert!(!receiver.tracker.with_state(|state| state.is_done()));
    finalization.add_permits(1);
    assert!(matches!(receiver.reply().await,
        Some(DestinationMessage::DirectoryReleased { src, dst }) if src == root && dst == root));
    receiver.completed().await;
}

#[tokio::test]
async fn queued_malformed_frame_prevents_done_after_the_last_directory_release() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let mut receiver = LifetimeReceiver::new();
    receiver
        .source
        .write_all(
            &frame(&SourceMessage::DirectoryBegin {
                src: root.clone(),
                dst: root.clone(),
                metadata: remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap()),
                is_root: true,
                keep_if_empty: true,
                admission: DirectoryClass::Normal,
            })
            .await,
        )
        .await
        .unwrap();
    assert!(matches!(receiver.reply().await,
        Some(DestinationMessage::DirectoryReady { src, dst }) if src == root && dst == root));
    let alias = receiver
        .tracker
        .with_state(|state| state.get_dir(&root))
        .unwrap();
    receiver
        .source
        .write_all(
            &frame(&SourceMessage::DirectoryEnd {
                src: root.clone(),
                dst: root.clone(),
                entry_count: 0,
            })
            .await,
        )
        .await
        .unwrap();
    tokio::time::timeout(TIMEOUT, async {
        while Arc::strong_count(&alias) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("root finalization must leave only the external descriptor alias");
    let mut burst = frame(&SourceMessage::DiscoveryComplete {
        has_root_item: true,
    })
    .await;
    burst.extend([0u8; 4]);
    // both the valid marker and the invalid empty frame are ready before the final release queues.
    receiver.source.write_all(&burst).await.unwrap();
    drop(alias);
    let error = tokio::time::timeout(TIMEOUT, &mut receiver.task)
        .await
        .expect("the queued framing error must stop the receiver")
        .unwrap()
        .expect_err("flushing the last release must not hide a ready framing error");
    assert!(error.is::<ControlMessageFailure>());
    assert!(receiver.tracker.with_state(|state| state.is_done()));
    assert!(
        !receiver
            .tracker
            .with_state(|state| state.destination_done_sent())
    );
    receiver.tracker.close_stream().await;
    while let Some(reply) = receiver.reply().await {
        assert!(
            matches!(reply, DestinationMessage::DirectoryReleased { src, dst }
            if src == root && dst == root)
        );
    }
}

#[tokio::test]
async fn held_directory_alias_delays_release_and_done_after_logical_completion() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let preparation = PreparationGate::new(&root, 0);
    let finalization = Arc::new(tokio::sync::Semaphore::new(0));
    let mut receiver = LifetimeReceiver::new();
    receiver.tracker.gate_finalization(finalization.clone());
    let mut frames = empty_directory_frames(&root).await;
    frames.extend(
        frame(&SourceMessage::DiscoveryComplete {
            has_root_item: true,
        })
        .await,
    );
    receiver.source.write_all(&frames).await.unwrap();
    preparation.started().await;
    receiver.consumed(frames.len()).await;
    let send = receiver.send.clone();
    let held_send = send.lock().await;
    preparation.release();
    let alias = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Some(dir) = receiver.tracker.with_state(|state| state.get_dir(&root)) {
                break dir;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("secured directory was not published before Ready");
    drop(held_send);
    assert!(matches!(receiver.reply().await,
        Some(DestinationMessage::DirectoryReady { src, dst }) if src == root && dst == root));
    finalization.add_permits(1);
    tokio::time::timeout(TIMEOUT, receiver.tracker.wait_for_completion())
        .await
        .expect("logical completion must not wait for the external descriptor alias");
    assert!(receiver.tracker.with_state(|state| state.is_done()));
    assert!(
        !receiver
            .tracker
            .with_state(|state| state.destination_done_sent())
    );
    // poll one reply future across the negative check and release; do not restart a decode
    {
        let released = receiver.replies.recv_object::<DestinationMessage>();
        tokio::pin!(released);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut released)
                .await
                .is_err(),
            "a live descriptor alias must retain lifetime admission and prevent Done"
        );
        drop(alias);
        assert!(
            matches!(tokio::time::timeout(TIMEOUT, &mut released).await.unwrap().unwrap(),
            Some(DestinationMessage::DirectoryReleased { src, dst }) if src == root && dst == root)
        );
    }
    receiver.completed().await;
}
