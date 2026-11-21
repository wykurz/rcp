use super::*;
use anyhow::Context as _;

pub(super) fn cursor_stream(cursor: &DirectoryCursor) -> &OwnedDirectoryStream {
    match &cursor.state {
        CursorState::Open { iterator, .. } => iterator,
        _ => panic!("cursor has no open stream"),
    }
}

fn gate_descriptor_stage(
    fd: RawFd,
    stage: Option<&str>,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    let Ok(mut path) = std::fs::read_link(format!("/proc/self/fd/{fd}")) else {
        return Ok(None);
    };
    if let Some(stage) = stage {
        path.push(stage);
    }
    crate::testutils::wait_on_blocking_path_gate(&path, fd)
        .map(crate::testutils::BlockingPathGateVisit::into_result)
        .transpose()
}

pub(super) fn gate_opened_descriptor(
    fd: RawFd,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    gate_descriptor_stage(fd, None)
}

pub(super) fn gate_classified_entry(
    fd: RawFd,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    gate_descriptor_stage(fd, Some(".rcp-classified-entry"))
}

pub(super) fn gate_directory_lockdown(
    fd: RawFd,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    gate_descriptor_stage(fd, Some(".rcp-directory-lockdown"))
}

pub(super) fn gate_directory_lockdown_chmod(
    fd: RawFd,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    gate_descriptor_stage(fd, Some(".rcp-directory-lockdown-chmod"))
}

pub(super) fn gate_directory_lockdown_rollback(
    fd: RawFd,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    gate_descriptor_stage(fd, Some(".rcp-directory-lockdown-rollback"))
}

pub(super) fn gate_directory_creation_acls(
    fd: RawFd,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    gate_descriptor_stage(fd, Some(".rcp-directory-creation-acls"))
}

pub(super) fn gate_directory_acl(
    fd: RawFd,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    gate_descriptor_stage(fd, Some(".rcp-directory-acl"))
}

pub(super) fn gate_directory_stat(
    fd: RawFd,
) -> std::io::Result<Option<crate::testutils::BlockingPathGateVisit>> {
    gate_descriptor_stage(fd, Some(".rcp-directory-stat"))
}

#[tokio::test]
async fn removes_a_leaf_in_one_job_and_leaves_a_directory_for_the_walk() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("child");
    std::fs::create_dir(&directory)?;
    std::fs::write(root.path().join("file"), b"abc")?;
    let parent = Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?;
    let submissions = crate::testutils::BlockingSubmissions::start();
    let removed = parent
        .unlink_if_nondirectory(OsStr::new("file"))
        .await?
        .context("expected a nondirectory")??;
    assert_eq!(submissions.count(), 1);
    assert_eq!(removed.kind(), EntryKind::File);
    assert_eq!(removed.size(), 3);
    assert!(!root.path().join("file").exists());
    assert!(
        parent
            .unlink_if_nondirectory(OsStr::new("child"))
            .await?
            .is_none()
    );
    assert!(directory.is_dir());
    Ok(())
}

#[tokio::test]
async fn cancelling_after_a_leaf_snapshot_keeps_the_file() -> anyhow::Result<()> {
    use futures::FutureExt as _;

    let root = tempfile::tempdir()?;
    let path = root.path().join("file");
    std::fs::write(&path, b"still here")?;
    let limits = crate::testutils::AdmissionLimit::new().await;
    limits.set_files_in_flight(1);
    let parent = Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?;
    let mut gate =
        crate::testutils::BlockingPathGate::install(root.path().join(".rcp-before-hinted-unlink"));
    let held = throttle::pending_meta_permit().await;
    let task = tokio::spawn(with_fd_admission(held.admission(), async move {
        let _held = held;
        parent.unlink_if_nondirectory(OsStr::new("file")).await
    }));

    let timeout = std::time::Duration::from_secs(10);
    let entered = tokio::time::timeout(timeout, gate.wait_started()).await;
    task.abort();
    let cancelled = matches!(task.await, Err(error) if error.is_cancelled());
    let mut next = Box::pin(throttle::pending_meta_permit());
    let retained = next.as_mut().now_or_never().is_none();
    gate.release_all();
    entered.context("unlink never reached its post-classification cancellation point")??;
    tokio::time::timeout(timeout, gate.wait_completed()).await??;
    if retained {
        drop(tokio::time::timeout(timeout, next).await?);
    }
    assert!(cancelled);
    assert!(
        retained,
        "detached worker released descriptor admission before it exited"
    );
    assert_eq!(std::fs::read(path)?, b"still here");
    Ok(())
}

#[tokio::test]
async fn preview_metadata_failure_is_not_unavailable_open_evidence() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let child = root.path().join("child");
    std::fs::create_dir(&child)?;
    let parent = Dir::open_root_dir(root.path(), false, congestion::Side::Destination).await?;
    let _failure = crate::testutils::BlockingPathGate::fail_once(
        child.join(".rcp-directory-stat"),
        libc::EACCES,
    );
    let error = open_preview_entry(&parent, OsStr::new("child"), false)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(error.to_string().contains("cannot fstat opened directory"));
    let open_error = parent
        .open_dir_with_meta(OsStr::new("missing"))
        .await
        .unwrap_err();
    assert_eq!(open_error.kind(), std::io::ErrorKind::NotFound);
    assert!(!open_error.to_string().contains("fstat"));
    Ok(())
}

async fn credit(gate: &Arc<tokio::sync::Semaphore>) -> Arc<tokio::sync::OwnedSemaphorePermit> {
    Arc::new(gate.clone().acquire_owned().await.unwrap())
}

#[tokio::test]
async fn directory_credit_outlives_parent_and_closes_cursor_before_release() -> anyhow::Result<()> {
    let root_path = crate::testutils::create_temp_dir().await?;
    tokio::fs::create_dir(root_path.join("child")).await?;
    let root = Dir::open_root_dir(&root_path, false, congestion::Side::Source).await?;
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let dir = root
        .open_dir_admitted(OsStr::new("child"), credit(&gate).await)
        .await?;
    let dir_probe = crate::testutils::FdIdentityProbe::capture(dir.fd.as_raw_fd())?;
    assert_eq!(
        gate.available_permits(),
        0,
        "an opened directory released its credit"
    );
    let cursor = dir.entries().await?;
    let cursor_probe =
        crate::testutils::FdIdentityProbe::capture(cursor_stream(&cursor).as_raw_fd())?;
    drop(dir);
    assert!(dir_probe.original_is_closed()?);
    assert_eq!(
        gate.available_permits(),
        0,
        "a live independent cursor lost its credit"
    );
    drop(cursor);
    assert_eq!(gate.available_permits(), 1);
    assert!(
        cursor_probe.original_is_closed()?,
        "credit returned before cursor closure"
    );
    Ok(())
}

#[tokio::test]
async fn following_cursor_retains_credit_and_follows_the_requested_symlink() -> anyhow::Result<()> {
    let root_path = crate::testutils::create_temp_dir().await?;
    tokio::fs::create_dir(root_path.join("target")).await?;
    tokio::fs::write(root_path.join("target/entry"), b"payload").await?;
    tokio::fs::symlink("target", root_path.join("link")).await?;
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let mut cursor = DirectoryCursor::open_following_symlinks(
        &root_path.join("link"),
        congestion::Side::Source,
        credit(&gate).await,
    )
    .await?;
    let probe = crate::testutils::FdIdentityProbe::capture(cursor_stream(&cursor).as_raw_fd())?;
    assert_eq!(
        gate.available_permits(),
        0,
        "following cursor released its descriptor credit"
    );
    let entries = cursor.next_batch(NonZeroUsize::new(64).unwrap()).await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "entry");
    drop(cursor);
    assert_eq!(gate.available_permits(), 1);
    assert!(probe.original_is_closed()?);
    Ok(())
}

#[tokio::test]
async fn cancelled_cursor_keeps_directory_credit_until_its_blocking_read_exits()
-> anyhow::Result<()> {
    for following in [false, true] {
        let root_path = crate::testutils::create_temp_dir().await?;
        tokio::fs::create_dir(root_path.join("child")).await?;
        let gate = Arc::new(tokio::sync::Semaphore::new(1));
        let mut cursor = if following {
            DirectoryCursor::open_following_symlinks(
                &root_path.join("child"),
                congestion::Side::Source,
                credit(&gate).await,
            )
            .await?
        } else {
            let root = Dir::open_root_dir(&root_path, false, congestion::Side::Source).await?;
            let dir = root
                .open_dir_admitted(OsStr::new("child"), credit(&gate).await)
                .await?;
            dir.entries().await?
        };
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut started_tx = Some(started_tx);
        cursor.read_hook = Some(Box::new(move |_, fd| {
            if let Some(started_tx) = started_tx.take() {
                let _ = started_tx.send(fd);
                release_rx.recv().map_err(std::io::Error::other)?;
            }
            Ok(())
        }));
        let task =
            tokio::spawn(async move { cursor.next_batch(NonZeroUsize::new(1).unwrap()).await });
        let fd = tokio::time::timeout(std::time::Duration::from_secs(5), started_rx).await??;
        let probe = crate::testutils::FdIdentityProbe::capture(fd)?;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!probe.original_is_closed()?);
        assert_eq!(
            gate.available_permits(),
            0,
            "cancelling the cursor returned credit while its read owns the fd"
        );
        release_tx
            .send(())
            .context("blocking read ended before release")?;
        let permit =
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.acquire()).await??;
        assert!(
            probe.original_is_closed()?,
            "read released its credit before closing the cursor"
        );
        drop(permit);
    }
    Ok(())
}

#[tokio::test]
async fn admitted_child_open_rejects_symlinks_and_returns_unused_credit() -> anyhow::Result<()> {
    let root_path = crate::testutils::create_temp_dir().await?;
    tokio::fs::create_dir(root_path.join("target")).await?;
    tokio::fs::symlink("target", root_path.join("link")).await?;
    let root = Dir::open_root_dir(&root_path, false, congestion::Side::Source).await?;
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    assert!(
        root.open_dir_admitted(OsStr::new("link"), credit(&gate).await)
            .await
            .is_err()
    );
    assert_eq!(gate.available_permits(), 1);
    assert!(
        root.open_dir_admitted(OsStr::new("target/.."), credit(&gate).await)
            .await
            .is_err()
    );
    assert_eq!(gate.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_directory_acl_read_keeps_credit_until_its_descriptor_closes()
-> anyhow::Result<()> {
    let root_path = crate::testutils::create_temp_dir().await?;
    let child = root_path.join("child");
    tokio::fs::create_dir(&child).await?;
    let root = Dir::open_root_dir(&root_path, false, congestion::Side::Source).await?;
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let dir = root
        .open_dir_admitted(OsStr::new("child"), credit(&gate).await)
        .await?;
    let mut barrier = crate::testutils::BlockingPathGate::install(&child);
    let task = tokio::spawn(async move { dir.read_acls().await });
    let fd =
        tokio::time::timeout(std::time::Duration::from_secs(5), barrier.wait_started()).await??;
    let probe = crate::testutils::FdIdentityProbe::capture(fd)?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!probe.original_is_closed()?);
    assert_eq!(
        gate.available_permits(),
        0,
        "cancelled ACL work retained a descriptor without its directory credit"
    );
    barrier.release_all();
    let permit = tokio::time::timeout(std::time::Duration::from_secs(5), gate.acquire()).await??;
    assert!(
        probe.original_is_closed()?,
        "ACL work returned credit before closing its descriptor"
    );
    drop(permit);
    Ok(())
}

#[tokio::test]
async fn cancelled_directory_open_keeps_credit_with_its_abandoned_output() -> anyhow::Result<()> {
    for following in [false, true] {
        let root_path = crate::testutils::create_temp_dir().await?;
        let child = root_path.join("child");
        tokio::fs::create_dir(&child).await?;
        let root = Dir::open_root_dir(&root_path, false, congestion::Side::Source).await?;
        let gate = Arc::new(tokio::sync::Semaphore::new(1));
        let credit = credit(&gate).await;
        let mut barrier = crate::testutils::BlockingPathGate::install(&child);
        let task = tokio::spawn(async move {
            if following {
                let _cursor = DirectoryCursor::open_following_symlinks(
                    &child,
                    congestion::Side::Source,
                    credit,
                )
                .await?;
            } else {
                let _dir = root.open_dir_admitted(OsStr::new("child"), credit).await?;
            }
            std::io::Result::Ok(())
        });
        let fd = tokio::time::timeout(std::time::Duration::from_secs(5), barrier.wait_started())
            .await??;
        let probe = crate::testutils::FdIdentityProbe::capture(fd)?;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!probe.original_is_closed()?);
        assert_eq!(
            gate.available_permits(),
            0,
            "cancelled open released credit before its blocking output closed"
        );
        barrier.release_all();
        let permit =
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.acquire()).await??;
        assert!(
            probe.original_is_closed()?,
            "abandoned open output returned credit before closing its descriptor"
        );
        drop(permit);
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_following_cursor_acl_read_keeps_credit_until_its_descriptor_closes()
-> anyhow::Result<()> {
    let root_path = crate::testutils::create_temp_dir().await?;
    let child = root_path.join("child");
    tokio::fs::create_dir(&child).await?;
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let mut cursor = DirectoryCursor::open_following_symlinks(
        &child,
        congestion::Side::Source,
        credit(&gate).await,
    )
    .await?;
    let cursor_fd = cursor_stream(&cursor).as_raw_fd();
    let mut barrier = crate::testutils::BlockingPathGate::install(&child);
    let task = tokio::spawn(async move { cursor.read_acls(congestion::Side::Source).await });
    let fd =
        tokio::time::timeout(std::time::Duration::from_secs(5), barrier.wait_started()).await??;
    assert_eq!(
        fd, cursor_fd,
        "ACL read opened or duplicated another descriptor"
    );
    let probe = crate::testutils::FdIdentityProbe::capture(fd)?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!probe.original_is_closed()?);
    assert_eq!(
        gate.available_permits(),
        0,
        "cancelled cursor ACL work retained a descriptor without its credit"
    );
    barrier.release_all();
    let permit = tokio::time::timeout(std::time::Duration::from_secs(5), gate.acquire()).await??;
    assert!(
        probe.original_is_closed()?,
        "cursor ACL work returned credit before closing its descriptor"
    );
    drop(permit);
    Ok(())
}

#[tokio::test]
async fn cursor_acl_reads_preserve_its_position_and_descriptor() -> anyhow::Result<()> {
    let root_path = crate::testutils::create_temp_dir().await?;
    tokio::fs::write(root_path.join("first"), b"x").await?;
    tokio::fs::write(root_path.join("second"), b"x").await?;
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let mut cursor = DirectoryCursor::open_following_symlinks(
        &root_path,
        congestion::Side::Source,
        credit(&gate).await,
    )
    .await?;
    let fd = cursor_stream(&cursor).as_raw_fd();
    let first = cursor.next_batch(NonZeroUsize::new(1).unwrap()).await?;
    let acls = cursor.read_acls(congestion::Side::Source).await?;
    assert!(acls.access.is_none());
    assert!(acls.default.is_none());
    assert_eq!(cursor_stream(&cursor).as_raw_fd(), fd);
    let second = cursor.next_batch(NonZeroUsize::new(1).unwrap()).await?;
    assert_ne!(first[0].name, second[0].name);
    assert!(
        cursor
            .next_batch(NonZeroUsize::new(1).unwrap())
            .await?
            .is_empty()
    );
    assert_eq!(gate.available_permits(), 1, "EOF retained cursor credit");
    drop(cursor);
    assert_eq!(gate.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn created_directory_lifetime_guard_runs_after_directory_and_cursor_close()
-> anyhow::Result<()> {
    struct Guard {
        descriptors: Arc<std::sync::Mutex<Vec<crate::testutils::FdIdentityProbe>>>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    impl std::fmt::Debug for Guard {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("DescriptorCloseGuard")
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            for fd in self.descriptors.lock().unwrap().iter() {
                assert!(
                    fd.original_is_closed().unwrap(),
                    "directory lease was released before its descriptor closed"
                );
            }
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let root_path = crate::testutils::create_temp_dir().await?;
    let root = Dir::open_root_dir(&root_path, false, congestion::Side::Destination).await?;
    let descriptors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = Arc::new(Guard {
        descriptors: descriptors.clone(),
        dropped: dropped.clone(),
    });
    let dir = root
        .make_dir_admitted(OsStr::new("child"), 0o700, guard.clone())
        .await?;
    let cursor = dir.entries().await?;
    descriptors.lock().unwrap().extend([
        crate::testutils::FdIdentityProbe::capture(dir.fd.as_raw_fd())?,
        crate::testutils::FdIdentityProbe::capture(cursor_stream(&cursor).as_raw_fd())?,
    ]);
    drop(guard);
    drop(dir);
    assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
    drop(cursor);
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn cancelled_directory_create_keeps_guard_until_abandoned_descriptor_closes()
-> anyhow::Result<()> {
    let root_path = crate::testutils::create_temp_dir().await?;
    let child = root_path.join("child");
    let root = Dir::open_root_dir(&root_path, false, congestion::Side::Destination).await?;
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let credit = credit(&gate).await;
    let mut barrier = crate::testutils::BlockingPathGate::install(&child);
    let task = tokio::spawn(async move {
        root.make_dir_admitted(OsStr::new("child"), 0o700, credit)
            .await
    });
    let fd =
        tokio::time::timeout(std::time::Duration::from_secs(5), barrier.wait_started()).await??;
    let probe = crate::testutils::FdIdentityProbe::capture(fd)?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!probe.original_is_closed()?);
    assert_eq!(
        gate.available_permits(),
        0,
        "cancelled create released its held descriptor guard"
    );
    barrier.release_all();
    let permit = tokio::time::timeout(std::time::Duration::from_secs(5), gate.acquire()).await??;
    assert!(
        probe.original_is_closed()?,
        "abandoned create returned credit before fd close"
    );
    drop(permit);
    Ok(())
}

#[test]
fn pending_unlink_admission_frees_the_only_blocking_worker() -> anyhow::Result<()> {
    let timeout = std::time::Duration::from_secs(5);
    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?
        .block_on(async {
            let tmp = tempfile::tempdir()?;
            let path = tmp.path().join("file");
            std::fs::write(&path, b"remove me")?;
            let limits = crate::testutils::AdmissionLimit::new().await;
            let root = Dir::open_root_dir(tmp.path(), false, congestion::Side::Source).await?;
            let unlink =
                throttle::Resource::meta(throttle::Side::Destination, throttle::MetadataOp::Unlink);
            limits.set_max_ops_in_flight(unlink, 1);
            let occupied = throttle::ops_in_flight_permit(unlink).await;
            let samples = Arc::new(congestion::testing::CollectingSink::new());
            congestion::install_sample_sink(samples.clone());
            let submissions = crate::testutils::BlockingSubmissions::start();
            let mut removal = Box::pin(root.unlink_if_nondirectory(OsStr::new("file")));
            assert!(futures::poll!(removal.as_mut()).is_pending());

            // wait for the actual stat before submitting a competing job to the sole worker
            let marker = path.clone();
            let witness = tokio::time::timeout(timeout, async {
                while samples.metadata_count_for(congestion::Side::Source) == 0 {
                    tokio::task::yield_now().await;
                }
                tokio::task::spawn_blocking(move || std::fs::read(marker)).await
            })
            .await;
            let before_release = submissions.count();
            drop(occupied);
            // releasing the held permit before examining a timeout also unblocks a regressed worker
            assert_eq!(
                witness.context("waiting unlink monopolized the blocking worker")???,
                b"remove me"
            );
            assert_eq!(before_release, 1, "unlink submitted a job before admission");
            let snapshot = limits
                .run_with_timeout(timeout, removal)
                .await??
                .context("file snapshot requested directory classification")??;
            assert_eq!(snapshot.kind(), EntryKind::File);
            assert_eq!(snapshot.size(), 9);
            assert!(!path.try_exists()?);
            assert_eq!(samples.metadata_count_for(congestion::Side::Source), 1);
            assert_eq!(samples.metadata_count_for(congestion::Side::Destination), 1);
            assert_eq!(
                submissions.count(),
                2,
                "the admitted unlink must be submitted exactly once"
            );
            Ok(())
        })
}
