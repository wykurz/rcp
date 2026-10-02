use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Fault {
    None,
    Write,
    PartialWrite,
    Flush,
    Panic,
    Pending,
}

#[derive(Default)]
struct Written {
    bytes: Vec<u8>,
    flushes: usize,
}

struct GroupWriter {
    state: Arc<std::sync::Mutex<Written>>,
    fault: Fault,
}

impl tokio::io::AsyncWrite for GroupWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if matches!(self.fault, Fault::Panic) {
            panic!("original grouped writer panic");
        }
        let mut written = self.state.lock().unwrap();
        if matches!(self.fault, Fault::Write)
            || (matches!(self.fault, Fault::PartialWrite) && !written.bytes.is_empty())
        {
            return std::task::Poll::Ready(Err(std::io::Error::other(
                "original grouped write failure",
            )));
        }
        let len = if matches!(self.fault, Fault::PartialWrite) {
            3
        } else {
            bytes.len()
        };
        written.bytes.extend_from_slice(&bytes[..len]);
        std::task::Poll::Ready(Ok(len))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.state.lock().unwrap().flushes += 1;
        match self.fault {
            Fault::Flush => {
                std::task::Poll::Ready(Err(std::io::Error::other("original grouped flush failure")))
            }
            Fault::Pending => std::task::Poll::Pending,
            _ => std::task::Poll::Ready(Ok(())),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

fn queued_group(context: &DiscoveryContext, count: usize) -> UnchangedGroup {
    let mut group = UnchangedGroup::default();
    for index in 0..count {
        let src = PathBuf::from(format!("s/{index}"));
        group.outcomes.push((
            SourceMessage::FileUnchanged {
                src: src.clone(),
                dst: format!("d/{index}").into(),
            },
            Obligation::new(context.fatal.clone(), src),
        ));
    }
    group
}

#[tokio::test]
async fn grouped_unchanged_finishes_only_after_the_whole_flush() -> anyhow::Result<()> {
    for count in [1, 7, UNCHANGED_GROUP_LIMIT] {
        let state = Arc::new(std::sync::Mutex::new(Written::default()));
        let (context, _) = submission_context(Box::new(GroupWriter {
            state: state.clone(),
            fault: Fault::None,
        }));
        let mut group = queued_group(&context, count);
        assert!(group.outcomes.iter().all(|(_, guard)| guard.armed));
        supervise(&context.fatal, group.flush(&context.control)).await?;
        assert!(group.outcomes.is_empty());
        assert_eq!(state.lock().unwrap().flushes, 1);
        let bytes = state.lock().unwrap().bytes.clone();
        let mut recv = remote::streams::RecvStream::new(bytes.as_slice());
        for index in 0..count {
            let Some(SourceMessage::FileUnchanged { src, dst }) = recv.recv_object().await? else {
                panic!("missing unchanged frame");
            };
            assert_eq!(src, Path::new(&format!("s/{index}")));
            assert_eq!(dst, Path::new(&format!("d/{index}")));
        }
        assert!(recv.recv_object::<SourceMessage>().await?.is_none());
        drop(group);
        assert!(context.fatal.take().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn grouped_unchanged_failure_preserves_armed_guards_and_original_cause() {
    for fault in [
        Fault::Write,
        Fault::PartialWrite,
        Fault::Flush,
        Fault::Panic,
    ] {
        let state = Arc::new(std::sync::Mutex::new(Written::default()));
        let (context, _) = submission_context(Box::new(GroupWriter {
            state: state.clone(),
            fault,
        }));
        let mut group = queued_group(&context, UNCHANGED_GROUP_LIMIT);
        assert!(
            supervise(&context.fatal, group.flush(&context.control))
                .await
                .is_err()
        );
        assert_eq!(group.outcomes.len(), UNCHANGED_GROUP_LIMIT);
        assert!(group.outcomes.iter().all(|(_, guard)| guard.armed));
        if matches!(fault, Fault::PartialWrite) {
            assert_eq!(state.lock().unwrap().bytes.len(), 3);
        }
        drop(group);
        let error = format!("{:#}", context.fatal.take().unwrap());
        assert!(
            error.contains(match fault {
                Fault::Panic => "original grouped writer panic",
                Fault::Flush => "original grouped flush failure",
                _ => "original grouped write failure",
            }),
            "{error}"
        );
        assert!(!error.contains("lost admitted"), "{error}");
    }
}

#[tokio::test]
async fn grouped_unchanged_cancellation_cannot_claim_completion() {
    for published in [false, true] {
        let state = Arc::new(std::sync::Mutex::new(Written::default()));
        let (context, _) = submission_context(Box::new(GroupWriter {
            state: state.clone(),
            fault: Fault::Pending,
        }));
        let mut group = queued_group(&context, UNCHANGED_GROUP_LIMIT);
        {
            let pending = supervise(&context.fatal, group.flush(&context.control));
            tokio::pin!(pending);
            assert!(futures::poll!(&mut pending).is_pending());
            assert!(!state.lock().unwrap().bytes.is_empty());
            if published {
                context
                    .fatal
                    .publish(anyhow::anyhow!("original peer failure"));
            }
        }
        assert_eq!(group.outcomes.len(), UNCHANGED_GROUP_LIMIT);
        assert!(group.outcomes.iter().all(|(_, guard)| guard.armed));
        drop(group);
        let error = format!("{:#}", context.fatal.take().unwrap());
        assert!(
            error.contains(if published {
                "original peer failure"
            } else {
                "lost admitted child obligation"
            }),
            "{error}"
        );
    }
}

async fn directory_skip_fixture(
    files: usize,
    wait_for_all: bool,
) -> anyhow::Result<(usize, Vec<SourceMessage>)> {
    struct CountWrites {
        writer: tokio::io::DuplexStream,
        flushes: Arc<AtomicUsize>,
    }
    impl tokio::io::AsyncWrite for CountWrites {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.writer).poll_write(cx, bytes)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            std::pin::Pin::new(&mut self.writer).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.writer).poll_shutdown(cx)
        }
    }
    let temp = tempfile::tempdir()?;
    let mut entries = Vec::new();
    for index in 0..files {
        let name = format!("file-{index}");
        let path = temp.path().join(&name);
        std::fs::write(&path, b"same")?;
        entries.push(ExistingEntry {
            name: name.into(),
            metadata: Metadata::from(&std::fs::metadata(path)?),
            size: 4,
            is_file: true,
        });
    }
    let (writer, reader) = tokio::io::duplex(128 * 1024);
    let flushes = Arc::new(AtomicUsize::new(0));
    let (mut context, mut jobs) = submission_context(Box::new(CountWrites {
        writer,
        flushes: flushes.clone(),
    }));
    let ready = Arc::new(tokio::sync::Semaphore::new(0));
    let classified = Arc::new(tokio::sync::Semaphore::new(0));
    let unblock_second = Arc::new(tokio::sync::Semaphore::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let hooks = Hooks::new({
        let ready = ready.clone();
        let classified = classified.clone();
        let unblock_second = unblock_second.clone();
        move |event, _, count| {
            let event = event.to_owned();
            let ready = ready.clone();
            let classified = classified.clone();
            let unblock_second = unblock_second.clone();
            let started = started.clone();
            Box::pin(async move {
                if event == "classifying" {
                    drop(ready.acquire().await?);
                    if !wait_for_all && started.fetch_add(1, Ordering::SeqCst) == 1 {
                        unblock_second.acquire().await?.forget();
                    }
                }
                if event == "classified" {
                    classified.add_permits(1);
                }
                if event == "drain" {
                    drop(ready.acquire().await?);
                    if wait_for_all {
                        classified.acquire_many(count as u32).await?.forget();
                    }
                }
                Ok(())
            })
        }
    });
    let configured = Arc::get_mut(&mut context).unwrap();
    configured.root = temp.path().to_owned();
    configured.hooks = Some(hooks.clone());
    let observation = Observation {
        pair: SrcDst {
            src: temp.path().to_owned(),
            dst: "/destination".into(),
        },
        name: temp.path().file_name().unwrap().to_owned(),
        kind: EntryKind::Dir,
        size: 0,
        metadata: Metadata::from(&std::fs::metadata(temp.path())?),
        target: None,
        included: true,
    };
    let scan = resources::Scan::Normal(context.branches.clone().acquire_owned().await?);
    let peer = async {
        let mut recv = remote::streams::RecvStream::new(reader);
        let mut messages = Vec::new();
        loop {
            let message = recv
                .recv_object::<SourceMessage>()
                .await?
                .context("source closed early")?;
            match &message {
                SourceMessage::DirectoryBegin { src, dst, .. } => {
                    context.registry.manifest(dst, entries.clone())?;
                    context.registry.resolve(src, dst, false)?;
                    ready.add_permits(1);
                }
                SourceMessage::FileUnchanged { .. } => {
                    unblock_second.add_permits(1);
                }
                SourceMessage::DirectoryEnd { entry_count, .. } => {
                    assert_eq!(*entry_count, files);
                    assert_eq!(
                        messages
                            .iter()
                            .filter(|m| matches!(m, SourceMessage::FileUnchanged { .. }))
                            .count(),
                        files
                    );
                    messages.push(message);
                    return anyhow::Ok(messages);
                }
                other => anyhow::bail!("unexpected message: {other:?}"),
            }
            messages.push(message);
        }
    };
    let (source, messages) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        common::task_scope::scope_tasks(async {
            tokio::join!(
                directory(context.clone(), Parent::Path, observation, true, scan),
                peer
            )
        }),
    )
    .await?;
    source?;
    assert!(
        jobs.try_recv().is_err(),
        "immediate skips consumed file admission"
    );
    assert!(context.fatal.take().is_none());
    assert_eq!(
        hooks
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _, _)| name == "classified")
            .count(),
        files
    );
    Ok((flushes.load(Ordering::SeqCst), messages?))
}

#[tokio::test]
async fn grouped_unchanged_ready_bursts_finish_at_batch_and_directory_boundaries()
-> anyhow::Result<()> {
    for count in [1, 7, 64, 65] {
        let (flushes, messages) = directory_skip_fixture(count, true).await?;
        assert_eq!(messages.len(), count + 2);
        assert!(matches!(
            messages.first(),
            Some(SourceMessage::DirectoryBegin { .. })
        ));
        assert!(matches!(
            messages.last(),
            Some(SourceMessage::DirectoryEnd { .. })
        ));
        assert!(
            flushes < 2 * count + 4,
            "ready burst retained individual flushing: {flushes}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn grouped_unchanged_flushes_before_waiting_for_another_classification() -> anyhow::Result<()>
{
    directory_skip_fixture(2, false).await?;
    Ok(())
}

#[tokio::test]
async fn grouped_unchanged_serialization_failure_preserves_original_cause() {
    use std::os::unix::ffi::OsStringExt;
    let state = Arc::new(std::sync::Mutex::new(Written::default()));
    let (context, _) = submission_context(Box::new(GroupWriter {
        state: state.clone(),
        fault: Fault::None,
    }));
    let mut group = queued_group(&context, 2);
    if let SourceMessage::FileUnchanged { src, .. } = &mut group.outcomes[1].0 {
        *src = std::ffi::OsString::from_vec(vec![0xff]).into();
    }
    let original = remote::streams::SendStream::new(tokio::io::sink())
        .send_control_message(&group.outcomes[1].0)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        supervise(&context.fatal, group.flush(&context.control))
            .await
            .is_err()
    );
    assert!(group.outcomes.iter().all(|(_, guard)| guard.armed));
    assert_eq!(state.lock().unwrap().flushes, 0);
    drop(group);
    let error = format!("{:#}", context.fatal.take().unwrap());
    assert!(error.contains(&original), "{error}");
    assert!(!error.contains("lost admitted"), "{error}");
}

#[tokio::test]
async fn grouped_unchanged_mixed_capacity_one_preserves_boundaries_and_recovery()
-> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().to_owned();
    for name in ["same-a", "same-b", "copy", "bad"] {
        std::fs::write(root.join(name), b"same")?;
    }
    std::os::unix::fs::symlink("copy", root.join("link"))?;
    std::fs::create_dir(root.join("pending"))?;
    std::fs::write(root.join("pending/file"), b"same")?;
    std::fs::create_dir(root.join("rejected"))?;
    std::fs::write(root.join("rejected/file"), b"same")?;
    let entry = |path: &Path| -> anyhow::Result<ExistingEntry> {
        Ok(ExistingEntry {
            name: path.file_name().unwrap().to_owned().into(),
            metadata: Metadata::from(&std::fs::metadata(path)?),
            size: 4,
            is_file: true,
        })
    };
    let root_manifest = vec![entry(&root.join("same-a"))?, entry(&root.join("same-b"))?];
    let pending_manifest = vec![entry(&root.join("pending/file"))?];
    let (writer, reader) = tokio::io::duplex(128 * 1024);
    let (mut context, mut jobs) = submission_context(Box::new(writer));
    let ready = Arc::new(tokio::sync::Semaphore::new(0));
    let rejected = Arc::new(tokio::sync::Semaphore::new(0));
    let classified = Arc::new(tokio::sync::Semaphore::new(0));
    let order = [
        "same-a", "same-b", "copy", "link", "pending", "rejected", "bad",
    ];
    let ordered = Arc::new(
        (0..order.len())
            .map(|index| tokio::sync::Semaphore::new(usize::from(index == 0)))
            .collect::<Vec<_>>(),
    );
    let hooks = Hooks::new({
        let root = root.clone();
        let ready = ready.clone();
        let rejected = rejected.clone();
        let classified = classified.clone();
        move |event, path, count| {
            let direct = path.parent() == Some(root.as_path());
            let is_root = path == root;
            let bad = path == root.join("bad");
            let rejected_child = path == root.join("rejected/file");
            let index = order.iter().position(|name| path == root.join(name));
            let ordered = ordered.clone();
            let event = event.to_owned();
            let ready = ready.clone();
            let rejected = rejected.clone();
            let classified = classified.clone();
            Box::pin(async move {
                if event == "classifying" && direct {
                    drop(ready.acquire().await?);
                }
                if event == "classifying" && rejected_child {
                    drop(rejected.acquire().await?);
                }
                if event == "classified" && direct {
                    let index = index.unwrap();
                    ordered[index].acquire().await?.forget();
                    classified.add_permits(1);
                    if let Some(next) = ordered.get(index + 1) {
                        next.add_permits(1);
                    }
                }
                if event == "classifying" && bad {
                    anyhow::bail!("recoverable classification failure: bad");
                }
                if event == "drain" && is_root {
                    classified.acquire_many(count as u32).await?.forget();
                }
                Ok(())
            })
        }
    });
    let configured = Arc::get_mut(&mut context).unwrap();
    configured.root = root.clone();
    configured.hooks = Some(hooks.clone());
    let (pool_tx, pool_rx) = async_channel::bounded(1);
    pool_tx
        .send(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        ))
        .await
        .unwrap();
    let pool = Arc::new(super::super::super::AcceptingSendStreamPool {
        recv: pool_rx,
        return_tx: pool_tx,
    });
    let worker = async {
        let mut names = Vec::new();
        for _ in 0..2 {
            let job = jobs.recv().await.context("file dispatcher ended")?;
            names.push(job.observation.pair.src.clone());
            file_job(context.clone(), pool.clone(), job).await?;
        }
        anyhow::Ok(names)
    };
    let observation = Observation {
        pair: SrcDst {
            src: root.clone(),
            dst: "/destination".into(),
        },
        name: root.file_name().unwrap().to_owned(),
        kind: EntryKind::Dir,
        size: 0,
        metadata: Metadata::from(&std::fs::metadata(&root)?),
        target: None,
        included: true,
    };
    let scan = resources::Scan::Normal(context.branches.clone().acquire_owned().await?);
    let parent = Parent::Hardened(Arc::new(
        Dir::open_root_dir(root.parent().unwrap(), false, common::Side::Source).await?,
    ));
    // the copy cannot take file admission until the peer receives a buffered unchanged outcome.
    // all root classifications are ready in order, so only the pre-admission flush can unblock it.
    let mut blocked_file = Some(context.files.clone().acquire_owned().await?);
    let peer = async {
        let mut recv = remote::streams::RecvStream::new(reader);
        let mut lifetimes = PeerLifetimes::default();
        let mut root_end = false;
        let mut pending_end = false;
        let mut rejected_end = false;
        let mut outcomes = std::collections::HashSet::new();
        let mut symlink = false;
        while !root_end || !pending_end || !rejected_end || outcomes.len() != 3 {
            match recv
                .recv_object::<SourceMessage>()
                .await?
                .context("source ended before outcomes")?
            {
                SourceMessage::DirectoryBegin { src, dst, .. } if src == root => {
                    lifetimes.begin(&src, &dst);
                    lifetimes.ready(&dst);
                    context.registry.manifest(&dst, root_manifest.clone())?;
                    context.registry.resolve(&src, &dst, false)?;
                    ready.add_permits(1);
                }
                SourceMessage::DirectoryBegin { src, dst, .. } if src == root.join("rejected") => {
                    lifetimes.begin(&src, &dst);
                    lifetimes.ready(&dst);
                    context.registry.resolve(&src, &dst, true)?;
                    rejected.add_permits(1);
                }
                SourceMessage::DirectoryBegin { src, dst, .. } => {
                    lifetimes.begin(&src, &dst);
                    assert_eq!(src, root.join("pending"));
                }
                SourceMessage::DirectoryEnd {
                    src,
                    dst,
                    entry_count,
                } if src == root.join("pending") => {
                    assert_eq!(entry_count, 1);
                    pending_end = true;
                    lifetimes.end(&dst);
                    lifetimes.ready(&dst);
                    context.registry.manifest(&dst, pending_manifest.clone())?;
                    context.registry.resolve(&src, &dst, false)?;
                }
                SourceMessage::DirectoryEnd {
                    src,
                    dst,
                    entry_count,
                } if src == root.join("rejected") => {
                    assert_eq!(entry_count, 0);
                    rejected_end = true;
                    lifetimes.end(&dst);
                }
                SourceMessage::DirectoryEnd {
                    src,
                    dst,
                    entry_count,
                } => {
                    assert_eq!(src, root);
                    lifetimes.end(&dst);
                    assert_eq!(entry_count, 6);
                    root_end = true;
                    assert!(
                        outcomes.contains(&root.join("same-a"))
                            && outcomes.contains(&root.join("same-b")),
                        "ready outcomes must precede parent End"
                    );
                    assert!(symlink);
                }
                SourceMessage::FileUnchanged { src, .. } => {
                    drop(blocked_file.take());
                    assert!(outcomes.insert(src));
                }
                SourceMessage::Symlink { src, target, .. } => {
                    assert_eq!(src, root.join("link"));
                    assert_eq!(target, PathBuf::from("copy"));
                    symlink = true;
                }
                other => anyhow::bail!("unexpected message: {other:?}"),
            }
            for released in lifetimes.completed() {
                context.registry.release(&released.src, &released.dst)?;
            }
        }
        assert!(
            lifetimes.0.is_empty(),
            "fake receiver left unfinished directory lifetimes"
        );
        assert_eq!(
            outcomes,
            [
                root.join("same-a"),
                root.join("same-b"),
                root.join("pending/file")
            ]
            .into()
        );
        anyhow::Ok(())
    };
    let (source, peer, worker) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        common::task_scope::scope_tasks(async {
            tokio::join!(
                directory(context.clone(), parent, observation, true, scan),
                peer,
                worker
            )
        }),
    )
    .await?;
    source?;
    peer?;
    assert_eq!(
        worker?
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        [root.join("copy"), root.join("pending/file")].into()
    );
    assert!(context.fatal.take().is_none());
    assert!(
        format!("{:#}", context.errors.take_error().unwrap())
            .contains("recoverable classification failure")
    );
    assert_eq!(context.files.available_permits(), 1);
    assert_eq!(context.classifiers.available_permits(), 1);
    assert!(context.registry.is_empty());
    Ok(())
}
