use super::*;
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

struct ResetFileAdmission;

impl Drop for ResetFileAdmission {
    fn drop(&mut self) {
        throttle::set_admission_limits(None);
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

#[test]
fn receiver_finalization_waits_for_delayed_writes_and_keeps_the_created_inode() {
    one_worker_runtime().block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("destination");
        let held_path = tmp.path().join("held-destination");
        let file = create_file(&path);
        let identity = FdIdentityProbe::capture(file.as_raw_fd()).unwrap();
        let header = file_header(&path);
        let preservation = common::preserve::preserve_all();
        let counts = (progress().files_copied.get(), progress().bytes_copied.get());
        let mut file = tokio::fs::File::from_std(file);
        let worker = OccupiedWorker::start().await;
        file.write_all(PAYLOAD).await.unwrap();
        let mut finalize = Box::pin(finalize_received_file(file, &header, &preservation));
        let pending = futures::poll!(finalize.as_mut()).is_pending();
        let before = std::fs::metadata(&path).unwrap();
        let uncounted = (progress().files_copied.get(), progress().bytes_copied.get());
        std::fs::rename(&path, &held_path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let replacement = std::fs::metadata(&path).unwrap();
        worker.finish().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), finalize)
            .await
            .unwrap()
            .unwrap();
        assert!(pending, "finalization must wait for the last Tokio write");
        assert_eq!(before.len(), 0);
        assert_eq!(
            before.mode() & 0o7777,
            common::safedir::DST_FILE_CREATE_MODE
        );
        assert_eq!(uncounted, counts);
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
}

#[test]
fn receiver_flush_failure_keeps_the_file_private_and_uncounted() {
    one_worker_runtime().block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("destination");
        drop(create_file(&path));
        let header = file_header(&path);
        let before = std::fs::metadata(&path).unwrap();
        // a real read-only descriptor lets Tokio accept bytes before its queued write fails
        let file = std::fs::File::open(&path).unwrap();
        let identity = FdIdentityProbe::capture(file.as_raw_fd()).unwrap();
        let mut file = tokio::fs::File::from_std(file);
        let preservation = common::preserve::preserve_all();
        let counts = (progress().files_copied.get(), progress().bytes_copied.get());
        let worker = OccupiedWorker::start().await;
        file.write_all(PAYLOAD).await.unwrap();
        let mut finalize = Box::pin(finalize_received_file(file, &header, &preservation));
        let pending = futures::poll!(finalize.as_mut()).is_pending();
        worker.finish().await;
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), finalize)
            .await
            .unwrap()
            .unwrap_err();
        assert!(pending);
        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::EBADF)
        );
        assert_eq!(
            (progress().files_copied.get(), progress().bytes_copied.get()),
            counts
        );
        let remaining = std::fs::metadata(&path).unwrap();
        assert_eq!(remaining.len(), 0);
        assert_eq!(remaining.mode(), before.mode());
        assert_eq!(
            (remaining.mtime(), remaining.mtime_nsec()),
            (before.mtime(), before.mtime_nsec())
        );
        assert!(identity.original_is_closed().unwrap());
    });
}

#[test]
fn cancelled_receiver_finalization_closes_the_owner_before_queued_metadata_runs() {
    one_worker_runtime().block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("destination");
        let file = create_file(&path);
        let identity = FdIdentityProbe::capture(file.as_raw_fd()).unwrap();
        let header = file_header(&path);
        let preservation = common::preserve::preserve_none();
        let _reset_admission = ResetFileAdmission;
        throttle::set_admission_limits(std::num::NonZeroUsize::new(1));
        let guard = throttle::open_file_permit().await;
        let mut file = tokio::fs::File::from_std(file);
        file.write_all(PAYLOAD).await.unwrap();
        file.flush().await.unwrap();
        let worker = OccupiedWorker::start().await;
        let mut finalize = Box::pin(common::safedir::with_fd_admission(
            guard.admission(),
            finalize_received_file(file, &header, &preservation),
        ));
        let pending = futures::poll!(finalize.as_mut()).is_pending();
        drop(guard);
        drop(finalize);
        let closed_before_release = identity.original_is_closed();
        let capacity_before_release = throttle::open_file_permit().now_or_never();
        worker.finish().await;
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        assert!(pending);
        assert!(closed_before_release.unwrap());
        assert!(capacity_before_release.is_some());
        assert_eq!(
            std::fs::metadata(&path).unwrap().mode() & 0o7777,
            common::safedir::DST_FILE_CREATE_MODE
        );
        assert_eq!(std::fs::read(path).unwrap(), PAYLOAD);
    });
}

#[tokio::test]
async fn receiver_metadata_failure_counts_payload_and_preserves_the_next_frame() {
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
    let settings = common::copy::Settings {
        reflink: Default::default(),
        local_copy_handoff: Default::default(),
        dereference: false,
        fail_early: false,
        overwrite: false,
        overwrite_compare: Default::default(),
        overwrite_filter: None,
        ignore_existing: false,
        chunk_size: 0,
        skip_specials: false,
        remote_copy_buffer_size: 1024,
        filter: None,
        dry_run: None,
        delete: None,
    };
    let preservation = common::preserve::preserve_all();
    let counts = (progress().files_copied.get(), progress().bytes_copied.get());
    let error = process_single_file(
        &settings,
        &preservation,
        &mut stream,
        &header,
        &parent,
        OsStr::new("failed-metadata"),
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
        &parent,
        OsStr::new("next"),
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
}
