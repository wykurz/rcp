use super::*;
use anyhow::Context as _;

// process-global fixture: nextest isolation or libtest --test-threads=1 is required
static ACL_READ_FAILURE_FD: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

/// Injects one ACL-read error for an owned descriptor without changing its file type or access.
pub(crate) struct AclReadFailure;

impl AclReadFailure {
    pub(crate) fn install(fd: RawFd) -> Self {
        assert!(fd >= 0);
        ACL_READ_FAILURE_FD
            .compare_exchange(
                -1,
                fd,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .expect("an ACL read failure fixture is already installed");
        Self
    }
}

impl Drop for AclReadFailure {
    fn drop(&mut self) {
        ACL_READ_FAILURE_FD.store(-1, std::sync::atomic::Ordering::SeqCst);
    }
}

pub(super) fn fail_acl_read_if_requested(fd: RawFd) -> std::io::Result<()> {
    // -2 disarms the error while keeping the fixture reserved until its guard drops
    if ACL_READ_FAILURE_FD
        .compare_exchange(
            fd,
            -2,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        )
        .is_ok()
    {
        return Err(std::io::Error::from_raw_os_error(libc::EIO));
    }
    Ok(())
}

pub(super) fn gate_opened_descriptor(fd: RawFd) -> Option<crate::testutils::BlockingPathGateVisit> {
    let path = std::fs::read_link(format!("/proc/self/fd/{fd}")).ok()?;
    crate::testutils::wait_on_blocking_path_gate(&path, fd)
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
        crate::testutils::FdIdentityProbe::capture(cursor.iterator.as_ref().unwrap().as_raw_fd())?;
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
    let probe =
        crate::testutils::FdIdentityProbe::capture(cursor.iterator.as_ref().unwrap().as_raw_fd())?;
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
    let cursor_fd = cursor.iterator.as_ref().unwrap().as_raw_fd();
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
    let fd = cursor.iterator.as_ref().unwrap().as_raw_fd();
    let first = cursor.next_batch(NonZeroUsize::new(1).unwrap()).await?;
    let acls = cursor.read_acls(congestion::Side::Source).await?;
    assert!(acls.access.is_none());
    assert!(acls.default.is_none());
    assert_eq!(cursor.iterator.as_ref().unwrap().as_raw_fd(), fd);
    let second = cursor.next_batch(NonZeroUsize::new(1).unwrap()).await?;
    assert_ne!(first[0].name, second[0].name);
    assert!(
        cursor
            .next_batch(NonZeroUsize::new(1).unwrap())
            .await?
            .is_empty()
    );
    assert_eq!(gate.available_permits(), 0);
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
        crate::testutils::FdIdentityProbe::capture(cursor.iterator.as_ref().unwrap().as_raw_fd())?,
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
