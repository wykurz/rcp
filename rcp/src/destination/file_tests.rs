//! Finalization assertions inspect process-global counters and admission state, and one test
//! lowers the process's file-size limit. Each test runs in its own child process so these checks
//! also remain exact under parallel libtest execution.

use super::*;
use std::io::Write as _;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

const PAYLOAD: &[u8] = b"received payload";

struct FdIdentityProbe {
    path: std::path::PathBuf,
    device: u64,
    inode: u64,
}

impl FdIdentityProbe {
    fn capture(fd: std::os::fd::RawFd) -> std::io::Result<Self> {
        let path = std::path::PathBuf::from(format!("/proc/self/fd/{fd}"));
        let metadata = std::fs::metadata(&path)?;
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    fn original_is_closed(&self) -> std::io::Result<bool> {
        match std::fs::metadata(&self.path) {
            Ok(metadata) => Ok((metadata.dev(), metadata.ino()) != (self.device, self.inode)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(error),
        }
    }
}

fn file_header(path: &std::path::Path) -> remote::protocol::File {
    let mut metadata = remote::protocol::Metadata::from(&std::fs::metadata(path).unwrap());
    metadata.mode = 0o751;
    metadata.atime = 1_000_000_500;
    metadata.atime_nsec = 123_456_789;
    metadata.mtime = 1_000_000_000;
    metadata.mtime_nsec = 987_654_321;
    remote::protocol::File {
        src: "source".into(),
        dst: path.to_owned(),
        size: PAYLOAD.len() as u64,
        metadata,
        is_root: true,
    }
}

fn create_file(path: &std::path::Path) -> std::fs::File {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(common::safedir::DST_FILE_CREATE_MODE)
        .open(path)
        .unwrap()
}

struct OccupiedWorker {
    // dropping the sender releases the worker on every exit, including a failed assertion
    release: Option<std::sync::mpsc::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl OccupiedWorker {
    async fn start() -> Self {
        let (release, released) = std::sync::mpsc::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            let _ = released.recv();
        });
        ready.await.unwrap();
        Self {
            release: Some(release),
            task,
        }
    }
    async fn finish(mut self) {
        drop(self.release.take());
        self.task.await.unwrap();
    }
}

fn one_worker_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

struct GatedPayload {
    started: Option<tokio::sync::oneshot::Sender<()>>,
    resume: Option<tokio::sync::oneshot::Receiver<()>>,
    remaining: usize,
    read: Arc<std::sync::atomic::AtomicUsize>,
}

impl tokio::io::AsyncRead for GatedPayload {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(started) = self.started.take() {
            started.send(()).unwrap();
        }
        if let Some(resume) = &mut self.resume {
            std::task::ready!(std::future::Future::poll(
                std::pin::Pin::new(resume),
                context
            ))
            .unwrap();
            self.resume = None;
        }
        let count = self.remaining.min(buffer.remaining());
        buffer.initialize_unfilled_to(count).fill(0x5a);
        buffer.advance(count);
        self.remaining -= count;
        self.read
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        std::task::Poll::Ready(Ok(()))
    }
}

#[test]
fn receiver_writes_whole_configured_chunks_and_reads_nothing_ahead() {
    const TEST_NAME: &str =
        "destination::file_tests::receiver_writes_whole_configured_chunks_and_reads_nothing_ahead";
    if crate::test_process::run_in_child(TEST_NAME) {
        return;
    }
    one_worker_runtime().block_on(async {
        for configured in [
            remote::TcpConfig::default().effective_buffer_size(),
            32 * 1024 * 1024,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let fixture = temp.path().join("fixture");
            drop(create_file(&fixture));
            let mut header = file_header(&fixture);
            header.dst = temp.path().join("destination");
            header.size = configured as u64 + 1;
            let parent = Arc::new(
                Dir::open_root_dir(temp.path(), false, common::Side::Destination)
                    .await
                    .unwrap(),
            );
            let (started, first_read) = tokio::sync::oneshot::channel();
            let (resume, resumed) = tokio::sync::oneshot::channel();
            let read = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut stream = remote::streams::RecvStream::new(Box::new(GatedPayload {
                started: Some(started),
                resume: Some(resumed),
                remaining: configured + 1,
                read: read.clone(),
            })
                as remote::streams::BoxedRead);
            let settings = common::copy::Settings {
                remote_copy_buffer_size: configured,
                ..testutils::copy_settings()
            };
            let preservation = common::preserve::preserve_all();
            let guard = throttle::open_file_permit().await;
            let mut receive = Box::pin(process_single_file(
                &settings,
                &preservation,
                &mut stream,
                &header,
                FileDestination::in_existing_parent(parent.clone(), OsStr::new("destination")),
                &guard,
            ));
            tokio::select! {
                _ = &mut receive => panic!("receiver finished before its first payload read"),
                ready = first_read => ready.unwrap(),
            }
            // creation has finished; hold the sole worker before allowing the first payload read
            let worker = OccupiedWorker::start().await;
            resume.send(()).unwrap();
            let pending = futures::poll!(receive.as_mut()).is_pending();
            let read_before_write_completed = read.load(std::sync::atomic::Ordering::Relaxed);
            worker.finish().await;
            tokio::time::timeout(std::time::Duration::from_secs(10), receive)
                .await
                .unwrap()
                .map_err(|error| error.source)
                .unwrap();
            assert!(pending, "the first write must wait for the occupied worker");
            // the first write carries one whole configured chunk: a hidden smaller limit would
            // have submitted it sooner, and nothing is read ahead of a write in flight
            assert_eq!(read_before_write_completed, configured);
            assert_eq!(
                std::fs::read(&header.dst).unwrap(),
                vec![0x5a; configured + 1]
            );
        }
    });
    crate::test_process::completed(TEST_NAME);
}

#[test]
fn receiver_finalization_applies_metadata_to_the_created_inode_through_its_descriptor() {
    const TEST_NAME: &str = "destination::file_tests::receiver_finalization_applies_metadata_to_the_created_inode_through_its_descriptor";
    if crate::test_process::run_in_child(TEST_NAME) {
        return;
    }
    one_worker_runtime().block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("destination");
        let held_path = tmp.path().join("held-destination");
        let mut file = create_file(&path);
        let identity = FdIdentityProbe::capture(file.as_raw_fd()).unwrap();
        let header = file_header(&path);
        let preservation = common::preserve::preserve_all();
        let counts = (progress().files_copied.get(), progress().bytes_copied.get());
        file.write_all(PAYLOAD).unwrap();
        let worker = OccupiedWorker::start().await;
        let mut finalize = Box::pin(finalize_received_file(file, &header, &preservation));
        let pending = futures::poll!(finalize.as_mut()).is_pending();
        let before = std::fs::metadata(&path).unwrap();
        let counted = (progress().files_copied.get(), progress().bytes_copied.get());
        std::fs::rename(&path, &held_path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let replacement = std::fs::metadata(&path).unwrap();
        worker.finish().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), finalize)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pending,
            "the metadata job must wait for the occupied worker"
        );
        // the payload is on disk and counted before its metadata is applied; until then the file
        // keeps its private creation mode
        assert_eq!(before.len(), header.size);
        assert_eq!(
            before.mode() & 0o7777,
            common::safedir::DST_FILE_CREATE_MODE
        );
        assert_eq!(counted, (counts.0 + 1, counts.1 + header.size));
        // the name was given to another file meanwhile: the metadata follows the descriptor
        let held = std::fs::metadata(&held_path).unwrap();
        assert_eq!(held.ino(), before.ino());
        assert_eq!(held.mode() & 0o7777, header.metadata.mode);
        assert_eq!(
            (held.atime(), held.atime_nsec()),
            (header.metadata.atime, header.metadata.atime_nsec)
        );
        assert_eq!(
            (held.mtime(), held.mtime_nsec()),
            (header.metadata.mtime, header.metadata.mtime_nsec)
        );
        assert_eq!(std::fs::read(&held_path).unwrap(), PAYLOAD);
        let untouched = std::fs::metadata(&path).unwrap();
        assert_eq!(untouched.ino(), replacement.ino());
        assert_eq!(untouched.mode(), replacement.mode());
        assert_eq!(
            (untouched.mtime(), untouched.mtime_nsec()),
            (replacement.mtime(), replacement.mtime_nsec())
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        assert!(identity.original_is_closed().unwrap());
        assert_eq!(progress().files_copied.get(), counts.0 + 1);
        assert_eq!(progress().bytes_copied.get(), counts.1 + header.size);
    });
    crate::test_process::completed(TEST_NAME);
}

#[test]
fn receiver_write_failure_keeps_the_file_private_and_uncounted() {
    const TEST_NAME: &str =
        "destination::file_tests::receiver_write_failure_keeps_the_file_private_and_uncounted";
    if crate::test_process::run_in_child(TEST_NAME) {
        return;
    }
    one_worker_runtime().block_on(async {
        let temp = tempfile::tempdir().unwrap();
        let fixture = temp.path().join("fixture");
        drop(create_file(&fixture));
        let mut header = file_header(&fixture);
        header.dst = temp.path().join("destination");
        let parent = Arc::new(
            Dir::open_root_dir(temp.path(), false, common::Side::Destination)
                .await
                .unwrap(),
        );
        let mut next = file_header(&fixture);
        next.dst = temp.path().join("next");
        next.size = 4;
        let mut wire = PAYLOAD.to_vec();
        remote::streams::SendStream::new(&mut wire)
            .send_control_message(&next)
            .await
            .unwrap();
        wire.extend_from_slice(b"next");
        let mut stream = remote::streams::RecvStream::new(
            Box::new(std::io::Cursor::new(wire)) as remote::streams::BoxedRead
        );
        let settings = testutils::copy_settings();
        let preservation = common::preserve::preserve_all();
        let counts = (progress().files_copied.get(), progress().bytes_copied.get());
        let guard = throttle::open_file_permit().await;
        // from here on the kernel refuses to grow any file of this process past eight bytes. The
        // test has a process of its own; the signal that comes with the refusal would end it
        // before the write could report the error.
        let limit = libc::rlimit {
            rlim_cur: 8,
            rlim_max: 8,
        };
        // SAFETY: both calls take plain values; the limit outlives the call that reads it.
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &raw const limit), 0);
        }
        let error = process_single_file(
            &settings,
            &preservation,
            &mut stream,
            &header,
            FileDestination::in_existing_parent(parent.clone(), OsStr::new("destination")),
            &guard,
        )
        .await
        .expect_err("a write the kernel refuses must fail the file");
        // the failed file is not copied, but all of its payload was taken off the wire
        assert_eq!(error.stream_state, StreamState::DataConsumed);
        assert_eq!(
            error.source.to_string(),
            "failed to write destination payload"
        );
        assert_eq!(
            error
                .source
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::EFBIG)
        );
        assert_eq!(
            (progress().files_copied.get(), progress().bytes_copied.get()),
            counts
        );
        let remaining = std::fs::metadata(&header.dst).unwrap();
        assert_eq!(remaining.len(), 8);
        assert_eq!(
            remaining.mode() & 0o7777,
            common::safedir::DST_FILE_CREATE_MODE
        );
        let received_next = stream
            .recv_object::<remote::protocol::File>()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received_next.dst, next.dst);
        process_single_file(
            &settings,
            &preservation,
            &mut stream,
            &received_next,
            FileDestination::in_existing_parent(parent.clone(), OsStr::new("next")),
            &guard,
        )
        .await
        .map_err(|error| error.source)
        .unwrap();
        assert_eq!(std::fs::read(&next.dst).unwrap(), b"next");
        assert_eq!(progress().files_copied.get(), counts.0 + 1);
        assert_eq!(progress().bytes_copied.get(), counts.1 + 4);
        assert!(
            stream
                .recv_object::<remote::protocol::File>()
                .await
                .unwrap()
                .is_none()
        );
    });
    crate::test_process::completed(TEST_NAME);
}

#[test]
fn cancelled_receiver_finalization_closes_the_owner_before_queued_metadata_runs() {
    const TEST_NAME: &str = "destination::file_tests::cancelled_receiver_finalization_closes_the_owner_before_queued_metadata_runs";
    if crate::test_process::run_in_child(TEST_NAME) {
        return;
    }
    one_worker_runtime().block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("destination");
        let mut file = create_file(&path);
        let identity = FdIdentityProbe::capture(file.as_raw_fd()).unwrap();
        let header = file_header(&path);
        let preservation = common::preserve::preserve_none();
        let _reset_admission = testutils::ResetAdmission;
        throttle::set_admission_limits(std::num::NonZeroUsize::new(1));
        let guard = throttle::open_file_permit().await;
        file.write_all(PAYLOAD).unwrap();
        let worker = OccupiedWorker::start().await;
        let metrics = tokio::runtime::Handle::current().metrics();
        let queued_before = metrics.blocking_queue_depth();
        let mut finalize = Box::pin(common::safedir::with_fd_admission(
            guard.admission(),
            finalize_received_file(file, &header, &preservation),
        ));
        let pending = futures::poll!(finalize.as_mut()).is_pending();
        let queued_after = metrics.blocking_queue_depth();
        drop(guard);
        drop(finalize);
        let closed_before_release = identity.original_is_closed();
        let capacity_before_release = throttle::open_file_permit().now_or_never();
        worker.finish().await;
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        assert!(pending);
        // the sole worker is occupied, so only the metadata submission can grow this queue
        assert_eq!(queued_before, 0);
        assert_eq!(queued_after, 1, "metadata must reach the blocking queue");
        assert!(closed_before_release.unwrap());
        assert!(capacity_before_release.is_some());
        assert_eq!(
            std::fs::metadata(&path).unwrap().mode() & 0o7777,
            common::safedir::DST_FILE_CREATE_MODE
        );
        assert_eq!(std::fs::read(path).unwrap(), PAYLOAD);
    });
    crate::test_process::completed(TEST_NAME);
}

#[tokio::test]
async fn receiver_metadata_failure_counts_payload_and_preserves_the_next_frame() {
    const TEST_NAME: &str = "destination::file_tests::receiver_metadata_failure_counts_payload_and_preserves_the_next_frame";
    if crate::test_process::run_in_child(TEST_NAME) {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let fixture = tmp.path().join("fixture");
    drop(create_file(&fixture));
    let mut header = file_header(&fixture);
    header.dst = tmp.path().join("failed-metadata");
    header.metadata.mtime_nsec = 2_000_000_000;
    let mut next = file_header(&fixture);
    next.dst = tmp.path().join("next");
    let mut wire = PAYLOAD.to_vec();
    remote::streams::SendStream::new(&mut wire)
        .send_control_message(&next)
        .await
        .unwrap();
    wire.extend_from_slice(PAYLOAD);
    let mut stream = remote::streams::RecvStream::new(
        Box::new(std::io::Cursor::new(wire)) as remote::streams::BoxedRead
    );
    let parent = Arc::new(
        Dir::open_root_dir(tmp.path(), false, common::Side::Destination)
            .await
            .unwrap(),
    );
    let settings = testutils::copy_settings();
    let preservation = common::preserve::preserve_all();
    let counts = (progress().files_copied.get(), progress().bytes_copied.get());
    let guard = throttle::open_file_permit().await;
    let error = process_single_file(
        &settings,
        &preservation,
        &mut stream,
        &header,
        FileDestination::in_existing_parent(parent.clone(), OsStr::new("failed-metadata")),
        &guard,
    )
    .await
    .expect_err("invalid timestamps must fail finalization");
    assert_eq!(error.stream_state, StreamState::DataConsumed);
    assert_eq!(
        error
            .source
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(libc::EINVAL)
    );
    assert!(format!("{:#}", error.source).contains("failed setting metadata"));
    assert_eq!(progress().files_copied.get(), counts.0 + 1);
    assert_eq!(progress().bytes_copied.get(), counts.1 + header.size);
    assert_eq!(
        std::fs::metadata(&header.dst).unwrap().mode() & 0o7777,
        common::safedir::DST_FILE_CREATE_MODE
    );
    assert_eq!(std::fs::read(&header.dst).unwrap(), PAYLOAD);
    let received_next = stream
        .recv_object::<remote::protocol::File>()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received_next.dst, next.dst);
    process_single_file(
        &settings,
        &preservation,
        &mut stream,
        &received_next,
        FileDestination::in_existing_parent(parent.clone(), OsStr::new("next")),
        &guard,
    )
    .await
    .map_err(|error| error.source)
    .unwrap();
    assert_eq!(std::fs::read(&next.dst).unwrap(), PAYLOAD);
    assert_eq!(
        std::fs::metadata(&next.dst).unwrap().mode() & 0o7777,
        next.metadata.mode
    );
    assert_eq!(progress().files_copied.get(), counts.0 + 2);
    assert_eq!(
        progress().bytes_copied.get(),
        counts.1 + header.size + next.size
    );
    crate::test_process::completed(TEST_NAME);
}

#[test]
fn final_write_failure_collects_and_continues_but_fail_early_stops() {
    const TEST_NAME: &str =
        "destination::file_tests::final_write_failure_collects_and_continues_but_fail_early_stops";
    if crate::test_process::run_in_child(TEST_NAME) {
        return;
    }
    one_worker_runtime().block_on(async {
        let temp = tempfile::tempdir().unwrap();
        let fixture = temp.path().join("fixture");
        drop(create_file(&fixture));
        let metadata = remote::protocol::Metadata::from(&std::fs::metadata(&fixture).unwrap());
        let limit = libc::rlimit {
            rlim_cur: 8,
            rlim_max: 8,
        };
        // SAFETY: this isolated child owns the limit and ignores its associated signal.
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &raw const limit), 0);
        }
        for fail_early in [false, true] {
            let root = temp
                .path()
                .join(if fail_early { "early" } else { "collect" });
            std::fs::create_dir(&root).unwrap();
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let send = remote::streams::SendStream::new(
                Box::new(tokio::io::sink()) as remote::streams::BoxedWrite
            );
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                Arc::new(tokio::sync::Mutex::new(send)),
                common::preserve::preserve_none(),
                fail_early,
                errors.clone(),
            );
            let admission = tracker
                .with_state(|state| state.admit_directory(&root, true))
                .unwrap();
            let directory = Arc::new(
                Dir::open_root_dir(&root, false, common::Side::Destination)
                    .await
                    .unwrap(),
            );
            tracker
                .register_directory(
                    admission,
                    directory,
                    remote::protocol::Metadata::from(&std::fs::metadata(&root).unwrap()),
                    false,
                    true,
                    None,
                )
                .unwrap();
            tracker.mark_announced(&root).await.unwrap();
            tracker.seal_directory(&root, 2).await.unwrap();
            tracker
                .with_state(|state| state.finish_discovery(true))
                .unwrap();
            let mut wire = Vec::new();
            for (name, payload) in [("failed", PAYLOAD), ("next", b"next".as_slice())] {
                let header = remote::protocol::File {
                    src: "/source".into(),
                    dst: root.join(name),
                    size: payload.len() as u64,
                    metadata: metadata.clone(),
                    is_root: false,
                };
                remote::streams::SendStream::new(&mut wire)
                    .send_control_message(&header)
                    .await
                    .unwrap();
                wire.extend_from_slice(payload);
            }
            let mut settings = testutils::copy_settings();
            settings.fail_early = fail_early;
            let counts = (progress().files_copied.get(), progress().bytes_copied.get());
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                handle_file_stream(
                    settings,
                    common::preserve::preserve_none(),
                    remote::streams::RecvStream::new(
                        Box::new(std::io::Cursor::new(wire)) as remote::streams::BoxedRead
                    ),
                    tracker.clone(),
                    errors.clone(),
                ),
            )
            .await
            .expect("file error stalled the session");
            let error = if fail_early {
                assert!(tracker.with_state(|state| state.is_closing()));
                assert!(!tracker.with_state(|state| state.is_done()));
                assert!(!root.join("next").exists());
                assert!(errors.take_error().is_none());
                assert_eq!(
                    (progress().files_copied.get(), progress().bytes_copied.get()),
                    counts
                );
                result.expect_err("fail-early must return the original write error")
            } else {
                result.unwrap();
                assert!(tracker.with_state(|state| state.is_done()));
                assert!(!tracker.with_state(|state| state.is_closing()));
                assert_eq!(std::fs::read(root.join("next")).unwrap(), b"next");
                assert_eq!(progress().files_copied.get(), counts.0 + 1);
                assert_eq!(progress().bytes_copied.get(), counts.1 + 4);
                errors.take_error().expect("failed file must be collected")
            };
            assert!(format!("{error:#}").contains("failed to write destination payload"));
            assert_eq!(
                error
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .raw_os_error(),
                Some(libc::EFBIG)
            );
            let failed = std::fs::metadata(root.join("failed")).unwrap();
            assert_eq!(failed.len(), 8);
            assert_eq!(
                failed.mode() & 0o7777,
                common::safedir::DST_FILE_CREATE_MODE
            );
        }
    });
    crate::test_process::completed(TEST_NAME);
}
