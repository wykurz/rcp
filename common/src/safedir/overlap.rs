//! Directory identity evidence and transactional local traversal registration.

use super::{Dir, DirectoryDescriptor, FileMeta, nix_to_io, run_metadata_probed_blocking};
use nix::fcntl::{OFlag, openat};
use nix::sys::stat::{Mode, fstat};
use smallvec::SmallVec;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::sync::Arc;

pub(crate) type Identity = (u64, u64);
pub(crate) type Evidence = std::collections::HashSet<Identity>;
type Position = (u64, u64, u64);
// ordinary admission owns source + optional update and one output. Root ancestry can spill.
type Identities = SmallVec<[Identity; 2]>;

#[derive(Debug)]
pub(crate) struct Operand {
    pub(crate) source: std::path::PathBuf,
    pub(crate) update: Option<std::path::PathBuf>,
    pub(crate) destination: std::path::PathBuf,
    // diagnostic context only; the registry still owns every admission decision
    pub(crate) destination_ancestors: std::sync::OnceLock<Arc<Evidence>>,
}

impl std::fmt::Display for Operand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.source)?;
        if let Some(update) = &self.update {
            write!(f, " (update {update:?})")?;
        }
        write!(f, " -> {:?}", self.destination)
    }
}

#[derive(Default)]
struct Uses {
    committed: Option<Arc<Operand>>,
    // one pending owner is usual; concurrent aliases may spill until one commits.
    pending: SmallVec<[(u64, Arc<Operand>); 1]>,
}

impl Uses {
    fn reserve(&mut self, token: u64, operand: &Arc<Operand>) -> bool {
        if self.committed.is_some() || self.pending.iter().any(|(owner, _)| *owner == token) {
            return false;
        }
        self.pending.push((token, Arc::clone(operand)));
        true
    }

    fn commit(&mut self, operand: &Arc<Operand>) {
        if self.committed.is_none() {
            self.committed = Some(Arc::clone(operand));
            // permanent history subsumes every pending alias, including owners still in flight.
            self.pending = SmallVec::new();
        }
    }

    fn release(&mut self, token: u64) {
        if let Some(index) = self.pending.iter().position(|(owner, _)| *owner == token) {
            self.pending.swap_remove(index);
        }
    }

    fn owner(&self) -> &Arc<Operand> {
        self.committed
            .as_ref()
            .or_else(|| self.pending.first().map(|(_, owner)| owner))
            .expect("a use has an owner")
    }
}

#[derive(Default)]
struct DirectoryUses {
    next_token: u64,
    inputs: std::collections::HashMap<Identity, Uses>,
    outputs: std::collections::HashMap<Identity, Uses>,
}

/// Invocation history and ancestry snapshots. Neither cache retains descriptors or admission credits.
/// Ancestry snapshots are keyed by a held directory's inode and mount position, never its spelling.
#[derive(Default)]
pub(crate) struct DirectoryOverlap {
    uses: std::sync::Mutex<DirectoryUses>,
    ancestry: std::sync::Mutex<
        std::collections::HashMap<Position, Arc<tokio::sync::OnceCell<Arc<Evidence>>>>,
    >,
}

impl std::fmt::Debug for DirectoryOverlap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryOverlap").finish_non_exhaustive()
    }
}

/// A pending admission owns only its contributions. Clones travel into noncancellable blocking jobs.
#[derive(Clone)]
pub(crate) struct Reservation(Arc<ReservationInner>);

struct ReservationInner {
    registry: Arc<DirectoryOverlap>,
    token: u64,
    operand: Arc<Operand>,
    inputs: Identities,
    outputs: std::sync::Mutex<Identities>,
    committed: std::sync::atomic::AtomicBool,
}

impl Drop for ReservationInner {
    fn drop(&mut self) {
        if self.committed.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let mut uses = self
            .registry
            .uses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outputs = self
            .outputs
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let DirectoryUses {
            inputs: input_uses,
            outputs: output_uses,
            ..
        } = &mut *uses;
        for (map, identities) in [(input_uses, &self.inputs), (output_uses, &*outputs)] {
            for id in identities {
                let std::collections::hash_map::Entry::Occupied(mut entry) = map.entry(*id) else {
                    continue;
                };
                entry.get_mut().release(self.token);
                if entry.get().committed.is_none() && entry.get().pending.is_empty() {
                    entry.remove();
                }
            }
        }
    }
}

impl Reservation {
    pub(crate) fn commit(&self) {
        // creation and strict lockdown can commit before admission returns. Avoid reacquiring the
        // global lock on that common second call; the locked recheck still serializes first commits.
        if self.0.committed.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let mut uses = self
            .0
            .registry
            .uses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.0.committed.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let outputs = self
            .0
            .outputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let DirectoryUses {
            inputs: input_uses,
            outputs: output_uses,
            ..
        } = &mut *uses;
        for (map, identities) in [(input_uses, &self.0.inputs), (output_uses, &*outputs)] {
            for id in identities {
                let entry = map
                    .get_mut(id)
                    .expect("reservation owns its registered use");
                entry.commit(&self.0.operand);
            }
        }
        self.0
            .committed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn output(&self, meta: &FileMeta) -> std::io::Result<()> {
        let id = (meta.dev(), meta.ino());
        let mut uses = self
            .0
            .registry
            .uses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(owners) = uses.inputs.get(&id) {
            return Err(overlap_error(owners.owner(), &self.0.operand));
        }
        let owners = uses.outputs.entry(id).or_default();
        if self.0.committed.load(std::sync::atomic::Ordering::Acquire) {
            owners.commit(&self.0.operand);
        } else if owners.reserve(self.0.token, &self.0.operand) {
            self.0
                .outputs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(id);
        }
        Ok(())
    }
}

impl DirectoryOverlap {
    pub(crate) fn reserve(
        self: &Arc<Self>,
        inputs: &[&FileMeta],
        outputs: &Evidence,
        operand: Arc<Operand>,
    ) -> std::io::Result<Reservation> {
        let mut identities = Identities::new();
        for meta in inputs {
            let id = (meta.dev(), meta.ino());
            if !identities.contains(&id) {
                identities.push(id);
            }
        }
        let mut uses = self
            .uses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for id in &identities {
            if outputs.contains(id) {
                return Err(input_overlap_error(&operand, &operand, id));
            }
            if let Some(owners) = uses.outputs.get(id) {
                return Err(input_overlap_error(owners.owner(), &operand, id));
            }
        }
        for id in outputs {
            if let Some(owners) = uses.inputs.get(id) {
                return Err(overlap_error(owners.owner(), &operand));
            }
        }
        let token = uses.next_token;
        uses.next_token += 1;
        identities.retain(|id| uses.inputs.entry(*id).or_default().reserve(token, &operand));
        let mut registered_outputs = Identities::new();
        for id in outputs {
            if uses
                .outputs
                .entry(*id)
                .or_default()
                .reserve(token, &operand)
            {
                registered_outputs.push(*id);
            }
        }
        Ok(Reservation(Arc::new(ReservationInner {
            registry: Arc::clone(self),
            token,
            operand,
            inputs: identities,
            outputs: std::sync::Mutex::new(registered_outputs),
            committed: std::sync::atomic::AtomicBool::new(false),
        })))
    }

    pub(crate) async fn ancestors(&self, parent: &Dir) -> std::io::Result<Arc<Evidence>> {
        let current = Arc::clone(&parent.fd);
        let side = parent.side;
        let position = run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, {
            let current = Arc::clone(&current);
            move || {
                let stat = fstat(current.as_fd()).map_err(nix_to_io)?;
                Ok((stat.st_dev, stat.st_ino, mount_id(current.as_fd())?))
            }
        })
        .await?;
        let cell = self
            .ancestry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(position)
            .or_default()
            .clone();
        cell.get_or_try_init(|| async { ancestors(current, side, position).await.map(Arc::new) })
            .await
            .cloned()
    }
}

fn overlap_error(owner: &Arc<Operand>, current: &Arc<Operand>) -> std::io::Error {
    let message = if Arc::ptr_eq(owner, current) {
        format!("input and destination directories overlap in this operand {current}")
    } else {
        format!(
            "input and destination directories overlap between this operand {current} \
             and another operand {owner}"
        )
    };
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn input_overlap_error(
    owner: &Arc<Operand>,
    current: &Arc<Operand>,
    identity: &Identity,
) -> std::io::Error {
    let contains = |operand: &Operand| {
        operand
            .destination_ancestors
            .get()
            .is_some_and(|ancestors| ancestors.contains(identity))
    };
    let message = if contains(current) {
        format!("input directory overlaps destination ancestry for this operand {current}")
    } else if contains(owner) {
        // a shared ancestor is evidence about a destination, not the first committer's source
        format!(
            "input directory in this operand {current} overlaps destination ancestry for {:?}",
            owner.destination
        )
    } else {
        return overlap_error(owner, current);
    };
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

async fn ancestors(
    mut current: Arc<DirectoryDescriptor>,
    side: congestion::Side,
    position: Position,
) -> std::io::Result<Evidence> {
    let mut current_id = (position.0, position.1);
    let mut current_mount = position.2;
    let mut result = Evidence::from([current_id]);
    loop {
        let step = run_metadata_probed_blocking(side, congestion::MetadataOp::Stat, move || {
            let fd = match openat(
                current.as_fd(),
                c"..",
                OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(nix::errno::Errno::EACCES) => return Ok(None),
                Err(error) => return Err(nix_to_io(error)),
            };
            let stat = fstat(&fd).map_err(nix_to_io)?;
            let parent_id = (stat.st_dev, stat.st_ino);
            let parent_mount = mount_id(fd.as_fd())?;
            let same_mount = current_mount == parent_mount;
            // a nonrecursive bind of a namespace ancestor need not expose this destination mount.
            // equal identities across mounts continue through a directory bound onto its direct child
            if (parent_id == current_id && same_mount) || (parent_id != current_id && !same_mount) {
                return Ok(None);
            }
            let parent = Arc::new(DirectoryDescriptor {
                fd,
                credit: current.credit.clone(),
            });
            #[cfg(test)]
            let _visit = super::resource_tests::gate_opened_descriptor(parent.as_raw_fd())?;
            Ok(Some((parent, parent_id, parent_mount)))
        })
        .await?;
        let Some((parent, parent_id, parent_mount)) = step else {
            return Ok(result);
        };
        result.insert(parent_id);
        current = parent;
        current_id = parent_id;
        current_mount = parent_mount;
    }
}

fn mount_id(fd: BorrowedFd<'_>) -> std::io::Result<u64> {
    use rustix::fs::{AtFlags, StatxFlags, statx};
    if let Ok(stat) = statx(fd, c"", AtFlags::EMPTY_PATH, StatxFlags::MNT_ID)
        && stat.stx_mask & StatxFlags::MNT_ID.bits() != 0
    {
        return Ok(stat.stx_mnt_id);
    }
    // fdinfo supplies mount IDs on kernels predating statx's STATX_MNT_ID support
    let info = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", fd.as_raw_fd()))
        .map_err(|error| std::io::Error::other(MountIdentityError(error)))?;
    info.lines()
        .find_map(|line| line.strip_prefix("mnt_id:")?.trim().parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "cannot verify destination ancestry without a mount ID",
            )
        })
}

#[derive(Debug)]
struct MountIdentityError(std::io::Error);

impl std::fmt::Display for MountIdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cannot verify destination ancestry without a mount ID")
    }
}

impl std::error::Error for MountIdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

#[cfg(test)]
fn test_operand() -> Arc<Operand> {
    Arc::new(Operand {
        source: "source".into(),
        update: None,
        destination: "destination".into(),
        destination_ancestors: Default::default(),
    })
}

#[cfg(test)]
impl DirectoryOverlap {
    async fn prepare(self: &Arc<Self>, parent: &Dir) -> std::io::Result<()> {
        let evidence = self.ancestors(parent).await?;
        self.reserve(&[], &evidence, test_operand())?.commit();
        Ok(())
    }
    fn input(self: &Arc<Self>, meta: &FileMeta) -> std::io::Result<()> {
        self.reserve(&[meta], &Evidence::new(), test_operand())?
            .commit();
        Ok(())
    }
    fn output(self: &Arc<Self>, meta: &FileMeta) -> std::io::Result<()> {
        let reservation = self.reserve(&[], &Evidence::new(), test_operand())?;
        reservation.output(meta)?;
        reservation.commit();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::os::unix::fs::PermissionsExt;

    struct RestoreMode {
        path: std::path::PathBuf,
        permissions: std::fs::Permissions,
    }

    impl RestoreMode {
        fn set(path: &std::path::Path, mode: u32) -> std::io::Result<Self> {
            let permissions = std::fs::metadata(path)?.permissions();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
            Ok(Self {
                path: path.to_owned(),
                permissions,
            })
        }
    }

    impl Drop for RestoreMode {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.path, self.permissions.clone());
        }
    }

    // isolate the one-way strict switch under plain cargo test as well as nextest.
    fn strict_child(test: &str) -> anyhow::Result<bool> {
        if !crate::safedir::openat2_available() {
            return Ok(false);
        }
        if std::env::var("RCP_STRICT_ADMISSION_TEST").as_deref() == Ok(test) {
            crate::safedir::enable_strict_operand_resolution();
            return Ok(true);
        }
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", test, "--nocapture"])
            .env("RCP_STRICT_ADMISSION_TEST", test)
            .output()?;
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("running 1 test"));
        Ok(false)
    }

    fn reuse_settings() -> crate::copy::Settings {
        crate::copy::Settings {
            reflink: Default::default(),
            local_copy_handoff: Default::default(),
            dereference: false,
            fail_early: true,
            overwrite: true,
            overwrite_compare: Default::default(),
            overwrite_filter: None,
            ignore_existing: false,
            chunk_size: 0,
            skip_specials: false,
            remote_copy_buffer_size: 0,
            filter: None,
            dry_run: None,
            delete: None,
        }
    }

    #[test]
    fn strict_reuse_identity_mismatch_releases_history() -> anyhow::Result<()> {
        if !strict_child(
            concat!(
                module_path!(),
                "::strict_reuse_identity_mismatch_releases_history"
            )
            .trim_start_matches("common::"),
        )? {
            return Ok(());
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let root = tempfile::tempdir()?;
                let path = root.path().canonicalize()?;
                std::fs::create_dir(path.join("source"))?;
                std::fs::create_dir(path.join("output"))?;
                std::fs::set_permissions(
                    path.join("output"),
                    std::fs::Permissions::from_mode(0o755),
                )?;
                let parent = Arc::new(
                    Dir::open_root_dir(&path, false, congestion::Side::Destination).await?,
                );
                let source = parent.open_dir(OsStr::new("source")).await?.meta().await?;
                let registry = Arc::new(DirectoryOverlap::default());
                let reservation = registry.reserve(&[&source], &Evidence::new(), test_operand())?;
                let mut gate = crate::testutils::BlockingPathGate::install(
                    path.join("output/.rcp-classified-entry"),
                );
                let target = path.join("output");
                let task = tokio::spawn(async move {
                    crate::copy::resolve_dst_dir(
                        crate::get_progress(),
                        &parent,
                        OsStr::new("output"),
                        &target,
                        &reuse_settings(),
                        &reservation,
                    )
                    .await
                });
                tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started())
                    .await??;
                std::fs::rename(path.join("output"), path.join("original"))?;
                std::fs::create_dir(path.join("output"))?;
                std::fs::set_permissions(
                    path.join("output"),
                    std::fs::Permissions::from_mode(0o755),
                )?;
                gate.release_all();
                let error = task
                    .await?
                    .err()
                    .expect("mismatched held identities must fail");
                assert_eq!(
                    error
                        .source
                        .downcast_ref::<std::io::Error>()
                        .unwrap()
                        .raw_os_error(),
                    Some(libc::ESTALE)
                );
                assert_eq!(
                    std::fs::metadata(path.join("output"))?.permissions().mode() & 0o777,
                    0o755
                );
                assert_eq!(
                    std::fs::metadata(path.join("original"))?
                        .permissions()
                        .mode()
                        & 0o777,
                    0o755
                );
                registry.output(&source)?;
                anyhow::Ok(())
            })
    }

    #[test]
    fn strict_queued_lockdown_cancellation_releases_history() -> anyhow::Result<()> {
        if !strict_child(
            concat!(
                module_path!(),
                "::strict_queued_lockdown_cancellation_releases_history"
            )
            .trim_start_matches("common::"),
        )? {
            return Ok(());
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?
            .block_on(async {
                let root = tempfile::tempdir()?;
                let path = root.path().canonicalize()?;
                std::fs::create_dir(path.join("source"))?;
                std::fs::create_dir(path.join("output"))?;
                std::fs::set_permissions(
                    path.join("output"),
                    std::fs::Permissions::from_mode(0o755),
                )?;
                let parent = Arc::new(
                    Dir::open_root_dir(&path, false, congestion::Side::Destination).await?,
                );
                let source = parent.open_dir(OsStr::new("source")).await?.meta().await?;
                let registry = Arc::new(DirectoryOverlap::default());
                let reservation = registry.reserve(&[&source], &Evidence::new(), test_operand())?;
                let mut gate = crate::testutils::BlockingPathGate::install(path.join("output"));
                let target = path.join("output");
                let submissions = crate::testutils::BlockingSubmissions::start();
                let task = tokio::spawn(async move {
                    crate::copy::resolve_dst_dir(
                        crate::get_progress(),
                        &parent,
                        OsStr::new("output"),
                        &target,
                        &reuse_settings(),
                        &reservation,
                    )
                    .await
                });
                tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started())
                    .await??;
                let (release, blocked) = std::sync::mpsc::channel();
                let (started, ready) = tokio::sync::oneshot::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = started.send(());
                    let _ = blocked.recv();
                });
                gate.release_all();
                ready.await?;
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while submissions.count() < 4 {
                        tokio::task::yield_now().await;
                    }
                })
                .await?;
                task.abort();
                assert!(task.await.err().expect("cancelled task").is_cancelled());
                registry.output(&source)?;
                release.send(())?;
                blocker.await?;
                tokio::task::spawn_blocking(|| ()).await?;
                assert_eq!(
                    std::fs::metadata(path.join("output"))?.permissions().mode() & 0o777,
                    0o755
                );
                anyhow::Ok(())
            })
    }

    #[test]
    fn strict_started_lockdown_cancellation_keeps_history() -> anyhow::Result<()> {
        if !strict_child(
            concat!(
                module_path!(),
                "::strict_started_lockdown_cancellation_keeps_history"
            )
            .trim_start_matches("common::"),
        )? {
            return Ok(());
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let root = tempfile::tempdir()?;
                let path = root.path().canonicalize()?;
                std::fs::create_dir(path.join("source"))?;
                std::fs::create_dir(path.join("output"))?;
                let parent = Arc::new(
                    Dir::open_root_dir(&path, false, congestion::Side::Destination).await?,
                );
                let source = parent.open_dir(OsStr::new("source")).await?.meta().await?;
                let output = parent.open_dir(OsStr::new("output")).await?.meta().await?;
                let registry = Arc::new(DirectoryOverlap::default());
                let reservation = registry.reserve(&[&source], &Evidence::new(), test_operand())?;
                let mut gate = crate::testutils::BlockingPathGate::install(
                    path.join("output/.rcp-directory-lockdown"),
                );
                let target = path.join("output");
                let task = tokio::spawn(async move {
                    crate::copy::resolve_dst_dir(
                        crate::get_progress(),
                        &parent,
                        OsStr::new("output"),
                        &target,
                        &reuse_settings(),
                        &reservation,
                    )
                    .await
                });
                tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started())
                    .await??;
                task.abort();
                assert!(task.await.err().expect("cancelled task").is_cancelled());
                assert!(
                    registry.output(&source).is_err(),
                    "started work retains pending input"
                );
                gate.release_all();
                tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_completed())
                    .await??;
                assert!(
                    registry.output(&source).is_err(),
                    "mutation committed input history"
                );
                assert!(
                    registry.input(&output).is_err(),
                    "mutation committed output history"
                );
                assert_eq!(
                    std::fs::metadata(path.join("output"))?.permissions().mode() & 0o777,
                    0o700
                );
                anyhow::Ok(())
            })
    }

    #[test]
    fn strict_preview_retains_parent_authority() -> anyhow::Result<()> {
        if !strict_child(
            concat!(module_path!(), "::strict_preview_retains_parent_authority")
                .trim_start_matches("common::"),
        )? {
            return Ok(());
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                for link in [false, true] {
                    for state in [0, 1, 2] {
                        if state == 2 && nix::unistd::geteuid().is_root() {
                            continue;
                        }
                        let root = tempfile::tempdir()?;
                        let path = root.path().canonicalize()?;
                        std::fs::create_dir(path.join("source"))?;
                        let mut restore_mode = None;
                        let container = if state == 2 {
                            std::fs::create_dir(path.join("inaccessible"))?;
                            restore_mode =
                                Some(RestoreMode::set(&path.join("inaccessible"), 0o000)?);
                            path.join("inaccessible/container")
                        } else {
                            path.join("container")
                        };
                        if state == 1 {
                            std::fs::create_dir(&container)?;
                        }
                        let mut gate = crate::testutils::BlockingPathGate::install(
                            path.join("source/.rcp-classified-entry"),
                        );
                        let task_path = path.clone();
                        let target = container.join("output");
                        let task = tokio::spawn(async move {
                            let mut settings = reuse_settings();
                            settings.dry_run = Some(crate::config::DryRunMode::Brief);
                            if link {
                                crate::link::link(
                                    crate::get_progress(),
                                    &task_path,
                                    &task_path.join("source"),
                                    &target,
                                    &None,
                                    &crate::link::Settings {
                                        copy_settings: settings,
                                        update_compare: Default::default(),
                                        update_exclusive: false,
                                        filter: None,
                                        dry_run: Some(crate::config::DryRunMode::Brief),
                                        preserve: crate::preserve::preserve_none(),
                                    },
                                    false,
                                )
                                .await
                                .map(|_| ())
                                .map_err(|e| e.source)
                            } else {
                                crate::copy::copy(
                                    crate::get_progress(),
                                    &task_path.join("source"),
                                    &target,
                                    &settings,
                                    &crate::preserve::preserve_none(),
                                    false,
                                )
                                .await
                                .map(|_| ())
                                .map_err(|e| e.source)
                            }
                        });
                        tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            gate.wait_started(),
                        )
                        .await??;
                        drop(restore_mode.take());
                        if state == 1 {
                            std::fs::rename(&container, path.join("held"))?;
                        }
                        std::os::unix::fs::symlink(path.join("source"), &container)?;
                        gate.release_all();
                        task.await??;
                        assert!(!path.join("source/output").exists());
                    }
                }
                anyhow::Ok(())
            })
    }

    #[tokio::test]
    async fn held_identity_reuse_does_not_cache_mutable_metadata() -> anyhow::Result<()> {
        use crate::preserve::Metadata as _;
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("child"))?;
        let parent = Dir::open_root_dir(root.path(), false, congestion::Side::Destination).await?;
        let handle = parent.child(OsStr::new("child")).await?;
        let (child, snapshot) = parent.open_dir_with_meta(OsStr::new("child")).await?;
        let submissions = crate::testutils::BlockingSubmissions::start();
        child.verify_same_inode(&handle).await?;
        assert_eq!(
            submissions.count(),
            0,
            "identity is already bound to this held directory"
        );
        std::fs::set_permissions(
            root.path().join("child"),
            std::fs::Permissions::from_mode(0o700),
        )?;
        assert_eq!(child.meta().await?.permissions().mode() & 0o777, 0o700);
        assert_eq!(
            submissions.count(),
            1,
            "mutable metadata must still be sampled live"
        );
        drop(submissions);
        let unrelated = parent.open_dir(OsStr::new("child")).await?;
        let submissions = crate::testutils::BlockingSubmissions::start();
        unrelated.verify_same_inode(&handle).await?;
        assert_eq!(
            submissions.count(),
            1,
            "ordinary callers keep live identity verification"
        );
        assert_eq!(
            (snapshot.dev(), snapshot.ino()),
            (handle.dev(), handle.ino())
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_reservations_leave_other_owners_intact() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("output"))?;
        let dir = Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?;
        let meta = dir.meta().await?;
        let output = dir.open_dir(OsStr::new("output")).await?.meta().await?;
        let registry = Arc::new(DirectoryOverlap::default());
        let mut reservations = Vec::new();
        for _ in 0..8 {
            // aliased source/update and repeated output registration each belong to one token.
            let reservation =
                registry.reserve(&[&meta, &meta], &Evidence::new(), test_operand())?;
            reservation.output(&output)?;
            reservation.output(&output)?;
            reservations.push(reservation);
        }
        while let Some(reservation) = reservations.pop() {
            assert!(registry.output(&meta).is_err());
            assert!(registry.input(&output).is_err());
            drop(reservation);
        }
        registry.output(&meta)?;
        registry.input(&output)?;
        Ok(())
    }

    #[tokio::test]
    async fn source_and_update_reserve_atomically() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("update"))?;
        let dir = Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?;
        let source = dir.meta().await?;
        let update = dir.open_dir(OsStr::new("update")).await?.meta().await?;
        let registry = Arc::new(DirectoryOverlap::default());
        registry.output(&update)?;
        assert!(
            registry
                .reserve(&[&source, &update], &Evidence::new(), test_operand())
                .is_err()
        );
        registry.output(&source)?;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_admission_releases_only_its_tentative_claims() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let dir = Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?;
        let meta = dir.meta().await?;
        let registry = Arc::new(DirectoryOverlap::default());
        let reservation = registry.reserve(&[&meta], &Evidence::new(), test_operand())?;
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _reservation = reservation;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await?;
        assert!(registry.output(&meta).is_err());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        registry.output(&meta)?;
        Ok(())
    }

    #[tokio::test]
    async fn committed_history_survives_the_last_reservation_owner() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let dir = Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?;
        let meta = dir.meta().await?;
        let registry = Arc::new(DirectoryOverlap::default());
        let committed = registry.reserve(&[&meta], &Evidence::new(), test_operand())?;
        let tentative = registry.reserve(&[&meta], &Evidence::new(), test_operand())?;
        committed.commit();
        drop(committed);
        drop(tentative);
        assert!(registry.output(&meta).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn ancestry_cache_uses_physical_identity_without_retaining_credit() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let child_path = root.path().join("child");
        std::fs::create_dir(&child_path)?;
        let parent = Dir::open_root_dir(root.path(), false, congestion::Side::Destination).await?;
        let credits = Arc::new(tokio::sync::Semaphore::new(1));
        let credit = Arc::new(Arc::clone(&credits).acquire_owned().await?);
        let child = parent
            .open_dir_admitted(OsStr::new("child"), credit)
            .await?;
        let registry = DirectoryOverlap::default();
        let first = registry.ancestors(&child).await?;
        std::fs::rename(&child_path, root.path().join("moved"))?;
        let submissions = crate::testutils::BlockingSubmissions::start();
        let second = registry.ancestors(&child).await?;
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            submissions.count(),
            1,
            "cache lookup performs one held-position sample"
        );
        drop(submissions);
        drop(child);
        assert_eq!(
            credits.available_permits(),
            1,
            "evidence must not retain descriptor credit"
        );
        std::fs::create_dir(&child_path)?;
        let replacement = parent.open_dir(OsStr::new("child")).await?;
        let submissions = crate::testutils::BlockingSubmissions::start();
        let third = registry.ancestors(&replacement).await?;
        assert!(!Arc::ptr_eq(&first, &third));
        assert!(
            submissions.count() > 1,
            "replacement inode must walk its own ancestry"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_roots_share_one_ancestry_walk() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("deep/container"))?;
        let parent = Arc::new(
            Dir::open_root_dir(
                &root.path().join("deep/container"),
                false,
                congestion::Side::Destination,
            )
            .await?,
        );
        let submissions = crate::testutils::BlockingSubmissions::start();
        DirectoryOverlap::default().ancestors(&parent).await?;
        let one_walk = submissions.count();
        drop(submissions);
        let registry = Arc::new(DirectoryOverlap::default());
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let submissions = crate::testutils::BlockingSubmissions::start();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let registry = Arc::clone(&registry);
            let parent = Arc::clone(&parent);
            let barrier = Arc::clone(&barrier);
            tasks.spawn(async move {
                barrier.wait().await;
                registry.ancestors(&parent).await
            });
        }
        let first = tasks.join_next().await.unwrap()??;
        while let Some(result) = tasks.join_next().await {
            assert!(Arc::ptr_eq(&first, &result??));
        }
        assert_eq!(
            submissions.count(),
            one_walk + 7,
            "each root samples its position; only one walks ancestors"
        );
        Ok(())
    }

    #[tokio::test]
    async fn committed_duplicate_inputs_coalesce_provenance() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("output"))?;
        std::fs::create_dir(root.path().join("late-output"))?;
        let dir = Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?;
        let meta = dir.meta().await?;
        let output = dir.open_dir(OsStr::new("output")).await?.meta().await?;
        let late_output = dir
            .open_dir(OsStr::new("late-output"))
            .await?
            .meta()
            .await?;
        let registry = Arc::new(DirectoryOverlap::default());
        let owner = Arc::new(Operand {
            source: "committed-source".into(),
            update: None,
            destination: "committed-output".into(),
            destination_ancestors: Default::default(),
        });
        let committed = registry.reserve(&[&meta], &Evidence::new(), owner)?;
        committed.output(&output)?;
        let mut aliases = Vec::new();
        for _ in 0..8 {
            let alias = registry.reserve(&[&meta], &Evidence::new(), test_operand())?;
            alias.output(&output)?;
            alias.output(&late_output)?;
            aliases.push(alias);
        }
        committed.commit();
        committed.output(&late_output)?;
        // permanent history subsumes all concurrent pending ownership, not just this token's.
        {
            let uses = registry.uses.lock().unwrap();
            for entry in uses.inputs.values().chain(uses.outputs.values()) {
                assert!(entry.committed.is_some());
                assert!(entry.pending.is_empty());
            }
        }
        let later = registry.reserve(&[&meta], &Evidence::new(), test_operand())?;
        later.output(&output)?;
        {
            let uses = registry.uses.lock().unwrap();
            assert!(
                uses.inputs
                    .values()
                    .chain(uses.outputs.values())
                    .all(|entry| entry.pending.is_empty())
            );
        }
        // both rollback and commit of aliases preserve the first committed provenance.
        aliases.pop().unwrap().commit();
        drop(aliases);
        drop(later);
        drop(committed);
        for error in [
            registry.output(&meta).unwrap_err(),
            registry.input(&output).unwrap_err(),
            registry.input(&late_output).unwrap_err(),
        ] {
            assert!(error.to_string().contains("committed-source"));
            assert!(error.to_string().contains("committed-output"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn repeated_commit_does_not_wait_for_registry_lock() -> anyhow::Result<()> {
        let registry = Arc::new(DirectoryOverlap::default());
        let reservation = registry.reserve(&[], &Evidence::new(), test_operand())?;
        reservation.commit();
        let held = registry.uses.lock().unwrap();
        let (finished, done) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            reservation.commit();
            finished.send(()).unwrap();
        });
        let result = done.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        worker.join().unwrap();
        result.expect("an already-committed reservation must not contend for the registry lock");
        Ok(())
    }

    #[test]
    fn queued_creation_cancellation_releases_pending_inputs() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?;
        runtime.block_on(async {
            let root = tempfile::tempdir()?;
            std::fs::create_dir(root.path().join("source"))?;
            let parent = Arc::new(
                Dir::open_root_dir(root.path(), false, congestion::Side::Destination).await?,
            );
            let source = parent.open_dir(OsStr::new("source")).await?.meta().await?;
            let registry = Arc::new(DirectoryOverlap::default());
            let reservation = registry.reserve(&[&source], &Evidence::new(), test_operand())?;
            let (release, blocked) = std::sync::mpsc::channel();
            let (started, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = started.send(());
                let _ = blocked.recv();
            });
            ready.await?;
            let submissions = crate::testutils::BlockingSubmissions::start();
            let task = tokio::spawn(async move {
                parent
                    .make_dir_checked(OsStr::new("output"), 0o700, move |meta| {
                        reservation.output(meta)?;
                        reservation.commit();
                        Ok(())
                    })
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while submissions.count() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            task.abort();
            assert!(task.await.expect_err("cancelled task").is_cancelled());
            registry.output(&source)?;
            release.send(())?;
            blocker.await?;
            tokio::task::spawn_blocking(|| ()).await?;
            assert!(!root.path().join("output").exists());
            anyhow::Ok(())
        })
    }

    #[tokio::test]
    async fn cancelled_creation_keeps_input_until_blocking_work_commits_output()
    -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("source"))?;
        let parent =
            Arc::new(Dir::open_root_dir(root.path(), false, congestion::Side::Destination).await?);
        let source = parent.open_dir(OsStr::new("source")).await?.meta().await?;
        let registry = Arc::new(DirectoryOverlap::default());
        let reservation = registry.reserve(&[&source], &Evidence::new(), test_operand())?;
        let mut gate = crate::testutils::BlockingPathGate::install(root.path().join("output"));
        let (finished, done) = tokio::sync::oneshot::channel();
        let task_parent = Arc::clone(&parent);
        let task = tokio::spawn(async move {
            task_parent
                .make_dir_checked(OsStr::new("output"), 0o700, move |meta| {
                    reservation.output(meta)?;
                    reservation.commit();
                    let _ = finished.send(());
                    Ok(())
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started()).await??;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            registry.output(&source).is_err(),
            "started work still owns its inputs"
        );
        gate.release_all();
        tokio::time::timeout(std::time::Duration::from_secs(5), done).await??;
        let output = parent.open_dir(OsStr::new("output")).await?.meta().await?;
        assert!(
            registry.input(&output).is_err(),
            "committed output survives cancelled return"
        );
        assert!(registry.output(&source).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_directory_reuse_in_either_direction() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let dir = Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?;
        let meta = dir.meta().await?;
        let inputs_first = Arc::new(DirectoryOverlap::default());
        inputs_first.input(&meta)?;
        inputs_first.input(&meta)?;
        assert_eq!(
            inputs_first.output(&meta).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        let outputs_first = Arc::new(DirectoryOverlap::default());
        outputs_first.output(&meta)?;
        outputs_first.output(&meta)?;
        assert_eq!(
            outputs_first.input(&meta).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_input_and_output_cannot_both_enter() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let dir = Arc::new(Dir::open_root_dir(root.path(), false, congestion::Side::Source).await?);
        let meta = dir.meta().await?;
        for _ in 0..32 {
            let overlap = Arc::new(DirectoryOverlap::default());
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let input = tokio::spawn({
                let overlap = Arc::clone(&overlap);
                let barrier = Arc::clone(&barrier);
                let meta = meta.clone();
                async move {
                    barrier.wait().await;
                    overlap.input(&meta)
                }
            });
            barrier.wait().await;
            let output = overlap.output(&meta);
            assert_ne!(input.await?.is_ok(), output.is_ok());
        }
        Ok(())
    }

    #[tokio::test]
    async fn ancestry_uses_held_parent_after_rename() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let source = root.path().join("source");
        std::fs::create_dir_all(source.join("parent"))?;
        let input = Dir::open_root_dir(&source, false, congestion::Side::Source).await?;
        let parent = input.open_dir(OsStr::new("parent")).await?;
        std::fs::rename(&source, root.path().join("moved"))?;
        std::fs::create_dir(&source)?;
        let replacement = Dir::open_root_dir(&source, false, congestion::Side::Source).await?;
        let overlap = Arc::new(DirectoryOverlap::default());
        overlap.prepare(&parent).await?;
        assert!(overlap.input(&input.meta().await?).is_err());
        overlap.input(&replacement.meta().await?)?;
        Ok(())
    }

    #[tokio::test]
    async fn ancestry_does_not_require_read_permission_on_ancestors() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let ancestor = root.path().join("ancestor");
        std::fs::create_dir_all(ancestor.join("parent"))?;
        let parent = Dir::open_root_dir(
            &ancestor.join("parent"),
            false,
            congestion::Side::Destination,
        )
        .await?;
        std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o111))?;
        let result = Arc::new(DirectoryOverlap::default()).prepare(&parent).await;
        std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o700))?;
        result?;
        Ok(())
    }

    #[tokio::test]
    async fn ancestry_stops_at_an_unsearchable_held_ancestor() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let ancestor = root.path().join("ancestor");
        std::fs::create_dir_all(ancestor.join("parent"))?;
        let input = Dir::open_root_dir(&ancestor, false, congestion::Side::Source).await?;
        let parent = input.open_dir(OsStr::new("parent")).await?;
        std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o000))?;
        let overlap = Arc::new(DirectoryOverlap::default());
        let result = overlap.prepare(&parent).await;
        std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o700))?;
        result?;
        assert!(overlap.input(&input.meta().await?).is_err());
        assert!(overlap.input(&parent.meta().await?).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_ancestry_keeps_credit_until_its_parent_descriptor_closes()
    -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("child"))?;
        let root_dir =
            Dir::open_root_dir(root.path(), false, congestion::Side::Destination).await?;
        let credits = Arc::new(tokio::sync::Semaphore::new(1));
        let credit = Arc::new(Arc::clone(&credits).acquire_owned().await?);
        let child = root_dir
            .open_dir_admitted(OsStr::new("child"), credit)
            .await?;
        let mut gate = crate::testutils::BlockingPathGate::install(root.path());
        let task =
            tokio::spawn(
                async move { Arc::new(DirectoryOverlap::default()).prepare(&child).await },
            );
        let fd =
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_started()).await??;
        let probe = crate::testutils::FdIdentityProbe::capture(fd)?;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!probe.original_is_closed()?);
        assert_eq!(credits.available_permits(), 0);
        gate.release_all();
        let _permit =
            tokio::time::timeout(std::time::Duration::from_secs(5), credits.acquire()).await??;
        assert!(probe.original_is_closed()?);
        Ok(())
    }
}
