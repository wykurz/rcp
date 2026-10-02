//! Single-pass source discovery and directory readiness accounting.

mod resources;

use anyhow::Context;
use common::preserve::Metadata as _;
use common::safedir::Dir;
use common::walk::EntryKind;
use futures::FutureExt as _;
use remote::protocol::{DestinationMessage, ExistingEntry, Metadata, SourceMessage, SrcDst};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

type Manifest = Arc<HashMap<PathBuf, ExistingEntry>>;

#[derive(Clone)]
enum ReadyState {
    Pending,
    Ready(Manifest),
    Rejected,
}

struct Readiness(tokio::sync::watch::Sender<ReadyState>);

impl Readiness {
    fn rejected(&self) -> bool {
        matches!(*self.0.borrow(), ReadyState::Rejected)
    }
    async fn wait(&self) -> ReadyState {
        let state = self.0.borrow().clone();
        if !matches!(state, ReadyState::Pending) {
            return state;
        }
        let waiting = common::timing_scope!("source.directory.wait_ready");
        let mut receiver = self.0.subscribe();
        loop {
            let state = receiver.borrow_and_update().clone();
            if !matches!(state, ReadyState::Pending) {
                waiting.finish();
                return state;
            }
            receiver
                .changed()
                .await
                .expect("readiness sender is owned by waiter");
        }
    }
}

struct Pending {
    src: PathBuf,
    readiness: Arc<Readiness>,
    manifest: HashMap<PathBuf, ExistingEntry>,
    _credit: tokio::sync::OwnedSemaphorePermit,
}

struct Lifetime {
    src: PathBuf,
    credit: Option<Arc<resources::DirectoryCredit>>,
    ready: bool,
}

#[derive(Default)]
struct RegistryState {
    pending: HashMap<PathBuf, Pending>,
    lifetimes: HashMap<PathBuf, Lifetime>,
}

struct Registry {
    state: std::sync::Mutex<RegistryState>,
    credit: Arc<tokio::sync::Semaphore>,
}

impl Registry {
    fn new(capacity: usize) -> Self {
        Self {
            state: Default::default(),
            credit: Arc::new(tokio::sync::Semaphore::new(capacity)),
        }
    }
    async fn register(
        &self,
        pair: &SrcDst,
        lifetime_credit: Arc<resources::DirectoryCredit>,
    ) -> anyhow::Result<Arc<Readiness>> {
        let credit = common::timing_scope!("source.discovery.wait_credit")
            .measure(self.credit.clone().acquire_owned())
            .await?;
        let readiness = Arc::new(Readiness(
            tokio::sync::watch::channel(ReadyState::Pending).0,
        ));
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(
            !state.lifetimes.contains_key(&pair.dst),
            "duplicate source directory Begin: {:?}",
            pair.dst
        );
        state.pending.insert(
            pair.dst.clone(),
            Pending {
                src: pair.src.clone(),
                readiness: readiness.clone(),
                manifest: HashMap::new(),
                _credit: credit,
            },
        );
        state.lifetimes.insert(
            pair.dst.clone(),
            Lifetime {
                src: pair.src.clone(),
                credit: Some(lifetime_credit),
                ready: false,
            },
        );
        Ok(readiness)
    }
    fn manifest(&self, dst: &Path, entries: Vec<ExistingEntry>) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        let record = state
            .pending
            .get_mut(dst)
            .with_context(|| format!("manifest without pending directory Begin: {dst:?}"))?;
        for entry in entries {
            anyhow::ensure!(
                !record.manifest.contains_key(&entry.name),
                "duplicate manifest entry: {:?}",
                entry.name
            );
            record.manifest.insert(entry.name.clone(), entry);
        }
        Ok(())
    }
    fn resolve(&self, src: &Path, dst: &Path, rejected: bool) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        let record = state.pending.get(dst).with_context(|| {
            format!("unknown or duplicate directory acknowledgement: {src:?} -> {dst:?}")
        })?;
        anyhow::ensure!(
            record.src == src,
            "directory acknowledgement source mismatch: {src:?} -> {dst:?}"
        );
        let lifetime = state
            .lifetimes
            .get(dst)
            .expect("pending Begin owns a lifetime record");
        anyhow::ensure!(
            rejected || lifetime.credit.is_some(),
            "successful directory Ready after lifetime release: {src:?} -> {dst:?}"
        );
        let record = state.pending.remove(dst).unwrap();
        record.readiness.0.send_replace(if rejected {
            ReadyState::Rejected
        } else {
            ReadyState::Ready(Arc::new(record.manifest))
        });
        let lifetime = state.lifetimes.get_mut(dst).unwrap();
        lifetime.ready = true;
        if lifetime.credit.is_none() {
            state.lifetimes.remove(dst);
        }
        Ok(())
    }
    fn release(&self, src: &Path, dst: &Path) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        let record = state.lifetimes.get_mut(dst).with_context(|| {
            format!("unknown or duplicate directory release: {src:?} -> {dst:?}")
        })?;
        anyhow::ensure!(
            record.src == src,
            "directory release source mismatch: {src:?} -> {dst:?}"
        );
        anyhow::ensure!(
            record.credit.is_some(),
            "duplicate directory release: {src:?} -> {dst:?}"
        );
        drop(record.credit.take());
        if record.ready {
            state.lifetimes.remove(dst);
        }
        Ok(())
    }
    fn is_empty(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.pending.is_empty() && state.lifetimes.is_empty()
    }
}

pub(super) struct Fatal {
    error: std::sync::Mutex<Option<anyhow::Error>>,
    cancel: tokio_util::sync::CancellationToken,
    pool_shutdown: super::PoolShutdownToken,
    gates: std::sync::Mutex<Vec<Arc<tokio::sync::Semaphore>>>,
}

impl Fatal {
    pub(super) fn new(pool_shutdown: super::PoolShutdownToken) -> Self {
        Self {
            error: Default::default(),
            cancel: Default::default(),
            pool_shutdown,
            gates: Default::default(),
        }
    }
    fn publish(&self, error: anyhow::Error) {
        let mut primary = self.error.lock().unwrap();
        if primary.is_none() {
            *primary = Some(error);
        }
        // publication and admission closure precede any task abortion or pool cancellation
        for gate in self.gates.lock().unwrap().iter() {
            gate.close();
        }
        self.cancel.cancel();
        self.pool_shutdown.cancel();
    }
    pub(super) fn take(&self) -> Option<anyhow::Error> {
        self.error.lock().unwrap().take()
    }
}

struct Obligation {
    fatal: Arc<Fatal>,
    path: PathBuf,
    armed: bool,
}

impl Obligation {
    fn new(fatal: Arc<Fatal>, path: PathBuf) -> Self {
        Self {
            fatal,
            path,
            armed: true,
        }
    }
    fn finish(&mut self) {
        self.armed = false;
    }
}

impl Drop for Obligation {
    fn drop(&mut self) {
        if self.armed {
            self.fatal.publish(anyhow::anyhow!(
                "lost admitted child obligation: {:?}",
                self.path
            ));
        }
    }
}

const UNCHANGED_GROUP_LIMIT: usize = 64;

#[derive(Default)]
struct UnchangedGroup {
    outcomes: Vec<(SourceMessage, Obligation)>,
}

impl UnchangedGroup {
    fn push(&mut self, observation: &Observation, obligation: Obligation) {
        self.outcomes.push((
            SourceMessage::FileUnchanged {
                src: observation.pair.src.clone(),
                dst: observation.pair.dst.clone(),
            },
            obligation,
        ));
    }
    // a named projection keeps the borrowed iterator's lifetime general across recursive futures
    fn message((message, _): &(SourceMessage, Obligation)) -> &SourceMessage {
        message
    }
    async fn flush(
        &mut self,
        control: &remote::streams::BoxedSharedSendStream,
    ) -> anyhow::Result<()> {
        if self.outcomes.is_empty() {
            return Ok(());
        }
        control
            .lock()
            .await
            .send_control_messages(self.outcomes.iter().map(Self::message))
            .await?;
        for (_, obligation) in &mut self.outcomes {
            obligation.finish();
        }
        self.outcomes.clear();
        Ok(())
    }
}

pub(super) async fn supervise<T>(
    fatal: &Fatal,
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    let error = match std::panic::AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(value)) => return Ok(value),
        Ok(Err(error)) => error,
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic");
            anyhow::anyhow!("source worker panicked: {message}")
        }
    };
    fatal.publish(error);
    Err(anyhow::anyhow!("source worker failed"))
}

async fn obligated(
    guard: Obligation,
    future: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let mut guard = guard;
    let result = supervise(&guard.fatal, future).await;
    if result.is_ok() {
        guard.finish();
    }
    result
}

#[derive(Clone)]
enum Parent {
    Hardened(Arc<Dir>),
    Path,
}

struct Observation {
    pair: SrcDst,
    name: std::ffi::OsString,
    kind: EntryKind,
    size: u64,
    metadata: Metadata,
    target: Option<PathBuf>,
    included: bool,
}

async fn classify(
    context: &DiscoveryContext,
    parent: &Parent,
    pair: SrcDst,
    name: std::ffi::OsString,
    is_root: bool,
) -> anyhow::Result<Observation> {
    let metadata_guard = throttle::pending_meta_permit().await;
    common::safedir::with_fd_admission(metadata_guard.admission(), async {
        let (kind, size, metadata, symlink) = match parent {
            Parent::Hardened(dir) => {
                let handle = dir
                    .child(&name)
                    .await
                    .with_context(|| format!("failed reading metadata from {:?}", pair.src))?;
                if is_root {
                    common::safedir::warn_if_root_acl_unpreserved(
                        &handle,
                        &pair.src,
                        common::safedir::RootAclNotice::from(context.capture),
                    )
                    .await;
                }
                let kind = handle.kind();
                let size = handle.meta().size();
                let metadata = Metadata::from(handle.meta());
                let symlink = (kind == EntryKind::Symlink).then_some((handle, dir.side()));
                (kind, size, metadata, symlink)
            }
            Parent::Path => {
                let meta = common::walk::run_metadata_probed(
                    common::Side::Source,
                    common::MetadataOp::Stat,
                    tokio::fs::metadata(&pair.src),
                )
                .await
                .with_context(|| format!("failed reading source metadata {:?}", pair.src))?;
                (
                    EntryKind::from_metadata(&meta),
                    meta.len(),
                    Metadata::from(&meta),
                    None,
                )
            }
        };
        let mut observation = Observation {
            pair,
            name,
            kind,
            size,
            metadata,
            target: None,
            included: false,
        };
        observation.included = context.included(&observation, is_root);
        if observation.included
            && let Some((handle, side)) = symlink
        {
            let read = async {
                #[cfg(test)]
                context
                    .checkpoint("read_symlink", &observation.pair.src, 0)
                    .await?;
                anyhow::Ok(handle.read_symlink(side).await?)
            }
            .await
            .with_context(|| format!("failed reading source symlink {:?}", observation.pair.src))?;
            observation.target = Some(read.0);
            observation.metadata = Metadata::from(&read.1);
        }
        Ok(observation)
    })
    .await
}

struct FileJob {
    observation: Observation,
    parent: Parent,
    readiness: Option<Arc<Readiness>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    obligation: Option<Obligation>,
    is_root: bool,
}

struct DiscoveryContext {
    settings: common::copy::Settings,
    capture: remote::protocol::ExtendedMetadataCapture,
    root: PathBuf,
    control: remote::streams::BoxedSharedSendStream,
    registry: Registry,
    branches: Arc<tokio::sync::Semaphore>,
    directories: resources::DirectoryBudget,
    classifiers: Arc<tokio::sync::Semaphore>,
    files: Arc<tokio::sync::Semaphore>,
    file_tx: tokio::sync::mpsc::Sender<FileJob>,
    fatal: Arc<Fatal>,
    errors: Arc<common::error_collector::ErrorCollector>,
    discovery_sealed: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    hooks: Option<Arc<tests::Hooks>>,
}

impl DiscoveryContext {
    #[cfg(test)]
    async fn checkpoint(&self, event: &str, path: &Path, count: usize) -> anyhow::Result<()> {
        if let Some(hooks) = &self.hooks {
            hooks
                .events
                .lock()
                .unwrap()
                .push((event.to_owned(), path.to_owned(), count));
            (hooks.callback)(event, path, count).await?;
        }
        Ok(())
    }
    async fn send(&self, message: SourceMessage) -> anyhow::Result<()> {
        self.control
            .lock()
            .await
            .send_control_message(&message)
            .await
    }
    fn recover(&self, error: anyhow::Error) -> anyhow::Result<()> {
        if self.settings.fail_early {
            return Err(error);
        }
        tracing::error!("Source discovery failed: {error:#}");
        self.errors.push(error);
        Ok(())
    }
    async fn recover_after(
        &self,
        error: anyhow::Error,
        terminal: impl std::future::Future<Output = anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        if self.settings.fail_early {
            return Err(error);
        }
        tracing::error!("Source discovery failed: {error:#}");
        if let Err(terminal_error) = terminal.await {
            return Err(error.context(format!(
                "failed to report source discovery failure: {terminal_error:#}"
            )));
        }
        // discovery is joined before final error collection, even when this terminal message
        // completes the destination. Keep this operation's cause owned until its send succeeds.
        self.errors.push(error);
        Ok(())
    }
    fn included(&self, observation: &Observation, is_root: bool) -> bool {
        let Some(filter) = &self.settings.filter else {
            return true;
        };
        let is_dir = observation.kind == EntryKind::Dir;
        let path = if is_root {
            observation
                .pair
                .src
                .file_name()
                .map(Path::new)
                .unwrap_or(&observation.pair.src)
        } else {
            common::walk::relative_to_root(&observation.pair.src, &self.root)
        };
        let result = if is_root {
            filter.should_include_root_item(path, is_dir)
        } else {
            filter.should_include(path, is_dir)
        };
        matches!(result, common::filter::FilterResult::Included)
            || (!is_root
                && is_dir
                && matches!(result, common::filter::FilterResult::ExcludedByDefault)
                && filter
                    .includes
                    .iter()
                    .any(|pattern| filter.could_contain_matches(path, pattern)))
    }
    async fn begin(
        &self,
        pair: &SrcDst,
        metadata: Metadata,
        is_root: bool,
        keep_if_empty: bool,
        admission: &resources::DirectoryAdmission,
    ) -> anyhow::Result<Arc<Readiness>> {
        let readiness = self.registry.register(pair, admission.credit()).await?;
        self.send(SourceMessage::DirectoryBegin {
            src: pair.src.clone(),
            dst: pair.dst.clone(),
            metadata,
            is_root,
            keep_if_empty,
            admission: admission.class(),
        })
        .await?;
        Ok(readiness)
    }
    async fn tombstone(
        &self,
        pair: &SrcDst,
        metadata: Metadata,
        is_root: bool,
        admission: &resources::DirectoryAdmission,
    ) -> anyhow::Result<()> {
        self.begin(pair, metadata, is_root, true, admission).await?;
        self.end(pair, 0).await
    }
    async fn end(&self, pair: &SrcDst, count: usize) -> anyhow::Result<()> {
        self.send(SourceMessage::DirectoryEnd {
            src: pair.src.clone(),
            dst: pair.dst.clone(),
            entry_count: count,
        })
        .await
    }
    async fn failed_child(&self, pair: &SrcDst, error: anyhow::Error) -> anyhow::Result<()> {
        self.recover_after(
            error,
            self.send(SourceMessage::FileSkipped {
                src: pair.src.clone(),
                dst: pair.dst.clone(),
            }),
        )
        .await
    }
    fn skip_unchanged(&self, observation: &Observation, manifest: &Manifest) -> bool {
        let Some(existing) = manifest.get(Path::new(&observation.name)) else {
            return false;
        };
        let source = remote::protocol::FileMetadata {
            metadata: &observation.metadata,
            size: observation.size,
        };
        let destination = remote::protocol::FileMetadata {
            metadata: &existing.metadata,
            size: existing.size,
        };
        common::copy::skip_unchanged_send(
            &self.settings.overwrite_compare,
            self.settings.overwrite_filter,
            self.settings.ignore_existing,
            &source,
            Some(common::copy::ExistingDst {
                meta: &destination,
                is_file: existing.is_file,
            }),
        )
    }
    fn log_unchanged(&self, observation: &Observation) {
        tracing::info!(
            "destination already has identical file, skipping transfer (manifest): {:?} -> {:?}",
            observation.pair.src,
            observation.pair.dst
        );
    }
    async fn send_unchanged(&self, observation: &Observation) -> anyhow::Result<()> {
        self.log_unchanged(observation);
        self.send(SourceMessage::FileUnchanged {
            src: observation.pair.src.clone(),
            dst: observation.pair.dst.clone(),
        })
        .await
    }
    async fn submit_file(
        &self,
        observation: Observation,
        parent: Parent,
        readiness: Option<Arc<Readiness>>,
        obligation: Obligation,
        is_root: bool,
    ) -> anyhow::Result<()> {
        let ready = readiness.as_ref().map(|state| state.0.borrow().clone());
        if let Some(ReadyState::Ready(manifest)) = ready
            && self.skip_unchanged(&observation, &manifest)
        {
            // known skips need no file-task capacity. Pending manifests keep their asynchronous
            // file path so discovery never waits here for directory readiness.
            return obligated(obligation, self.send_unchanged(&observation)).await;
        }
        let permit = match self.files.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(error) => {
                self.fatal.publish(error.into());
                return Err(anyhow::anyhow!("file admission closed"));
            }
        };
        self.file_tx
            .send(FileJob {
                observation,
                parent,
                readiness,
                _permit: permit,
                obligation: Some(obligation),
                is_root,
            })
            .await
            .map_err(|_| anyhow::anyhow!("file dispatcher closed"))
    }
}

async fn file_job(
    context: Arc<DiscoveryContext>,
    pool: Arc<super::AcceptingSendStreamPool>,
    mut job: FileJob,
) -> anyhow::Result<()> {
    let obligation = job
        .obligation
        .take()
        .expect("a queued file owns its obligation");
    // the queued job retains its admitted parent through readiness and payload completion.
    obligated(obligation, async {
        let observation = &job.observation;
        if let Some(readiness) = &job.readiness {
            let state = readiness.wait().await;
            match state {
                ReadyState::Rejected => return Ok(()),
                ReadyState::Pending => unreachable!(),
                ReadyState::Ready(manifest) => {
                    if context.skip_unchanged(observation, &manifest) {
                        return context.send_unchanged(observation).await;
                    }
                }
            }
        }
        let read = match &job.parent {
            Parent::Hardened(dir) => {
                super::FileRead::Hardened(dir.clone(), observation.name.clone())
            }
            Parent::Path => super::FileRead::Path,
        };
        super::send_file_tcp(
            &context.settings,
            context.capture,
            &observation.pair.src,
            &observation.pair.dst,
            observation.size,
            job.is_root,
            pool,
            &context.errors,
            context.control.clone(),
            read,
            &context.fatal,
        )
        .await
    })
    .await
}

#[async_recursion::async_recursion]
async fn directory(
    context: Arc<DiscoveryContext>,
    parent: Parent,
    observation: Observation,
    is_root: bool,
    scan: resources::Scan,
) -> anyhow::Result<()> {
    let mut children = tokio::task::JoinSet::new();
    let mut classifications = tokio::task::JoinSet::new();
    // armed outcomes outlive the caught body so its original error or panic is published first
    let mut unchanged = UnchangedGroup::default();
    supervise(
        &context.fatal,
        directory_body(
            context.clone(),
            parent,
            observation,
            is_root,
            scan,
            &mut children,
            &mut classifications,
            &mut unchanged,
        ),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn directory_body(
    context: Arc<DiscoveryContext>,
    parent: Parent,
    observation: Observation,
    is_root: bool,
    scan_credit: resources::Scan,
    children: &mut tokio::task::JoinSet<anyhow::Result<()>>,
    classifications: &mut tokio::task::JoinSet<anyhow::Result<Option<Observation>>>,
    unchanged: &mut UnchangedGroup,
) -> anyhow::Result<()> {
    let scan = common::timing_scope!("source.directory.scan");
    let pair = observation.pair;
    let admission = common::timing_scope!("source.directory.wait_resources")
        .measure(context.directories.admit(scan_credit))
        .await
        .with_context(|| format!("failed admitting source directory {:?}", pair.src));
    let mut admission = match admission {
        Ok(admission) => admission,
        Err(error) if !is_root && error.is::<resources::ReservedDepthExhausted>() => {
            context.failed_child(&pair, error).await?;
            scan.finish();
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    #[cfg(test)]
    context
        .checkpoint(
            "directory_admitted",
            &pair.src,
            usize::from(admission.sequential()),
        )
        .await?;
    // opening is separate from metadata capture: only the former permits the legacy unreadable
    // root/-L tombstone with classification metadata and unknown ACLs
    let opened = match parent {
        Parent::Hardened(parent) => parent
            .open_dir_admitted(&observation.name, admission.credit())
            .await
            .map(|dir| Parent::Hardened(Arc::new(dir))),
        Parent::Path => Ok(Parent::Path),
    }
    .with_context(|| format!("failed opening source directory {:?}", pair.src));
    let opened = match opened {
        Ok(opened) => opened,
        Err(error) if is_root => {
            context
                .recover_after(error, async {
                    context
                        .tombstone(&pair, observation.metadata, is_root, &admission)
                        .await
                })
                .await?;
            scan.finish();
            return Ok(());
        }
        Err(error) => return context.failed_child(&pair, error).await,
    };
    let description = async {
        #[cfg(test)]
        context.checkpoint("metadata", &pair.src, 0).await?;
        let (metadata, cursor) = match &opened {
            Parent::Hardened(dir) => {
                let metadata = Metadata::from(&dir.meta().await?);
                let metadata = if context.capture.dir_acl {
                    metadata.with_acls(&dir.read_acls().await?)
                } else {
                    metadata
                };
                (
                    metadata,
                    dir.entries().await.with_context(|| {
                        format!("failed opening source directory cursor {:?}", pair.src)
                    }),
                )
            }
            Parent::Path => {
                #[rustfmt::skip]
                let cursor = common::safedir::DirectoryCursor::open_following_symlinks( // rcp-toctou-allow: -L path (dereference, documented not hardened)
                    &pair.src,
                    common::Side::Source,
                    admission.credit(),
                )
                .await
                .with_context(|| format!("failed opening source directory cursor {:?}", pair.src));
                let mut cursor = match cursor {
                    Ok(cursor) => cursor,
                    Err(error) => return anyhow::Ok((observation.metadata.clone(), Err(error))),
                };
                let metadata = if context.capture.dir_acl {
                    observation
                        .metadata
                        .clone()
                        .with_acls(&cursor.read_acls(common::Side::Source).await?)
                } else {
                    observation.metadata.clone()
                };
                (metadata, Ok(cursor))
            }
        };
        anyhow::Ok((metadata, cursor))
    }
    .await
    .with_context(|| format!("failed reading source directory metadata {:?}", pair.src));
    let (metadata, cursor) = match description {
        Ok(description) => description,
        Err(error) if !is_root => return context.failed_child(&pair, error).await,
        Err(error) => return Err(error),
    };
    let mut cursor = match cursor {
        Ok(cursor) => cursor,
        Err(error) => {
            context
                .recover_after(error, async {
                    context
                        .tombstone(&pair, metadata, is_root, &admission)
                        .await
                })
                .await?;
            scan.finish();
            return Ok(());
        }
    };
    #[cfg(test)]
    context.checkpoint("cursor", &pair.src, 0).await?;
    let keep_if_empty = is_root
        || context.settings.filter.as_ref().is_none_or(|filter| {
            filter.directly_matches_include(
                common::walk::relative_to_root(&pair.src, &context.root),
                true,
            )
        });
    let readiness = context
        .begin(&pair, metadata, is_root, keep_if_empty, &admission)
        .await?;
    let mut count = 0usize;
    loop {
        while let Some(result) = children.try_join_next() {
            result??;
        }
        if readiness.rejected() {
            break;
        }
        let names = match async {
            #[cfg(test)]
            context.checkpoint("read_batch", &pair.src, 0).await?;
            cursor
                .next_batch(std::num::NonZeroUsize::new(64).unwrap())
                .await
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| entry.name)
                        .collect::<Vec<_>>()
                })
                .map_err(anyhow::Error::from)
        }
        .await
        {
            Ok(names) => names,
            Err(error) => {
                context.recover(error.context(format!(
                    "failed enumerating source directory {:?}",
                    pair.src
                )))?;
                break;
            }
        };
        #[cfg(test)]
        context.checkpoint("batch", &pair.src, names.len()).await?;
        if names.is_empty() {
            break;
        }
        for name in names {
            let child = SrcDst {
                src: pair.src.join(&name),
                dst: pair.dst.join(&name),
            };
            let parent = opened.clone();
            let context = context.clone();
            let readiness = readiness.clone();
            common::task_scope::spawn_tracked(classifications, async move {
                // once admitted, classify cooperatively through syscall/Handle completion even
                // if this directory is rejected; a local rejection must not release P early
                let permit = context.classifiers.clone().acquire_owned().await?;
                if readiness.rejected() {
                    return anyhow::Ok(None);
                }
                #[cfg(test)]
                let observed_path = child.src.clone();
                let result = std::panic::AssertUnwindSafe(async {
                    #[cfg(test)]
                    context.checkpoint("classifying", &child.src, 0).await?;
                    classify(&context, &parent, child, name, false).await
                })
                .catch_unwind()
                .await;
                let result = match result {
                    Ok(result) => result,
                    Err(panic) => {
                        let message = panic
                            .downcast_ref::<&str>()
                            .copied()
                            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                            .unwrap_or("unknown panic");
                        context
                            .fatal
                            .publish(anyhow::anyhow!("source classifier panicked: {message}"));
                        Err(anyhow::anyhow!("source classifier failed"))
                    }
                };
                // fail-early must wake a directory suspended in inline descent instead of
                // waiting for that ancestor to poll this completed classification result
                let result = match result {
                    Err(error) if context.settings.fail_early && !readiness.rejected() => {
                        context.fatal.publish(error);
                        Err(anyhow::anyhow!("source classification failed"))
                    }
                    result => result,
                };
                drop(permit);
                #[cfg(test)]
                context.checkpoint("classified", &observed_path, 0).await?;
                result.map(Some)
            });
        }
        #[cfg(test)]
        context
            .checkpoint("drain", &pair.src, classifications.len())
            .await?;
        loop {
            let result = match classifications.try_join_next() {
                Some(result) => result,
                None => {
                    // never wait to fill a group: release ready outcomes before any blocking join
                    unchanged.flush(&context.control).await?;
                    let Some(result) = classifications.join_next().await else {
                        break;
                    };
                    result
                }
            };
            let result = result?;
            if readiness.rejected() {
                continue;
            }
            let observation = match result {
                Ok(Some(observation)) => observation,
                Ok(None) => continue,
                Err(error) => {
                    context.recover(error)?;
                    continue;
                }
            };
            if readiness.rejected() {
                continue;
            }
            if !observation.included {
                observation.kind.inc_skipped(super::progress());
                continue;
            }
            if observation.kind == EntryKind::Special {
                if context.settings.skip_specials {
                    super::progress().specials_skipped.inc();
                } else {
                    context.recover(anyhow::anyhow!(
                        "copy: {:?} -> {:?} failed, unsupported src file type",
                        observation.pair.src,
                        observation.pair.dst
                    ))?;
                }
                continue;
            }
            let immediate_unchanged = observation.kind == EntryKind::File
                && match &*readiness.0.borrow() {
                    ReadyState::Ready(manifest) => context.skip_unchanged(&observation, manifest),
                    _ => false,
                };
            if !immediate_unchanged {
                // file admission, structural sends and inline descent can all wait on peer work
                unchanged.flush(&context.control).await?;
            }
            count = count
                .checked_add(1)
                .context("source directory child count overflow")?;
            let obligation = Obligation::new(context.fatal.clone(), observation.pair.src.clone());
            if immediate_unchanged {
                unchanged.push(&observation, obligation);
                context.log_unchanged(&observation);
                if unchanged.outcomes.len() == UNCHANGED_GROUP_LIMIT {
                    unchanged.flush(&context.control).await?;
                }
                continue;
            }
            match observation.kind {
                EntryKind::File => {
                    context
                        .submit_file(
                            observation,
                            opened.clone(),
                            Some(readiness.clone()),
                            obligation,
                            false,
                        )
                        .await?
                }
                EntryKind::Symlink => {
                    obligated(
                        obligation,
                        context.send(SourceMessage::Symlink {
                            src: observation.pair.src,
                            dst: observation.pair.dst,
                            metadata: observation.metadata,
                            target: observation
                                .target
                                .expect("symlink classification includes target"),
                            is_root: false,
                        }),
                    )
                    .await?;
                }
                EntryKind::Dir => {
                    let parent = opened.clone();
                    let context = context.clone();
                    match admission.try_fork(&context.branches)? {
                        Some(scan) => common::task_scope::spawn_tracked(children, async move {
                            obligated(
                                obligation,
                                directory(context, parent, observation, false, scan),
                            )
                            .await
                        }),
                        None => {
                            // move normal scan credit into the continuation. The suspended
                            // ancestor owns none once its child finishes scanning. Reserved
                            // descendants instead inherit their guaranteed sequential lane.
                            let scan = admission.descend();
                            let resumed = context.clone();
                            let mut continuation = tokio::task::JoinSet::new();
                            common::task_scope::spawn_tracked(&mut continuation, async move {
                                obligated(
                                    obligation,
                                    directory(context, parent, observation, false, scan),
                                )
                                .await
                            });
                            continuation
                                .join_next()
                                .await
                                .context("inline directory continuation disappeared")???;
                            admission.resume(&resumed.branches).await?;
                        }
                    }
                }
                EntryKind::Special => unreachable!(),
            }
        }
    }
    drop(cursor);
    drop(opened);
    context.end(&pair, count).await?;
    scan.finish();
    // lifetime credit stays with the registry and descriptor aliases after structural End.
    // a later reserved admission waits only if those ended siblings consume all spare credit.
    drop(admission);
    while let Some(result) = children.join_next().await {
        result??;
    }
    Ok(())
}

async fn discover(context: Arc<DiscoveryContext>, root: SrcDst) -> anyhow::Result<bool> {
    let scope = common::timing_scope!("source.discovery");
    let branch = context.branches.clone().acquire_owned().await?;
    let (parent, name) = if context.settings.dereference {
        let notice = common::safedir::RootAclNotice::from(context.capture);
        if common::safedir::root_acl_probe_worth_reaching(notice)
            && let Ok((parent, name)) = super::open_root_parent(&root.src).await
        {
            common::safedir::warn_if_root_acl_unpreserved_at(&parent, &name, &root.src, notice)
                .await;
        }
        (
            Parent::Path,
            root.src.file_name().unwrap_or_default().to_owned(),
        )
    } else {
        let (parent, name) = super::open_root_parent(&root.src).await?;
        (Parent::Hardened(parent), name)
    };
    let observation = classify(&context, &parent, root, name, true).await?;
    let included = observation.included;
    let has_root_item = included && observation.kind != EntryKind::Special;
    if !included {
        observation.kind.inc_skipped(super::progress());
    } else {
        match observation.kind {
            EntryKind::Dir => {
                directory(
                    context.clone(),
                    parent,
                    observation,
                    true,
                    resources::Scan::Normal(branch),
                )
                .await?
            }
            EntryKind::File => {
                let obligation =
                    Obligation::new(context.fatal.clone(), observation.pair.src.clone());
                context
                    .submit_file(observation, parent, None, obligation, true)
                    .await?;
            }
            EntryKind::Symlink => {
                context
                    .send(SourceMessage::Symlink {
                        src: observation.pair.src,
                        dst: observation.pair.dst,
                        target: observation
                            .target
                            .expect("symlink classification includes target"),
                        metadata: observation.metadata,
                        is_root: true,
                    })
                    .await?
            }
            EntryKind::Special if context.settings.skip_specials => {
                super::progress().specials_skipped.inc();
            }
            EntryKind::Special => {
                anyhow::bail!("unsupported src file type: {:?}", observation.pair.src)
            }
        }
    }
    context
        .discovery_sealed
        .store(true, std::sync::atomic::Ordering::Release);
    context
        .send(SourceMessage::DiscoveryComplete { has_root_item })
        .await?;
    scope.finish();
    Ok(has_root_item)
}

struct Shutdown(Arc<Fatal>);

impl Drop for Shutdown {
    fn drop(&mut self) {
        for gate in self.0.gates.lock().unwrap().iter() {
            gate.close();
        }
        self.0.pool_shutdown.cancel();
        self.0.cancel.cancel();
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    settings: common::copy::Settings,
    capture: remote::protocol::ExtendedMetadataCapture,
    root: SrcDst,
    control: remote::streams::BoxedSharedSendStream,
    mut receiver: remote::streams::BoxedRecvStream,
    pool: Arc<super::AcceptingSendStreamPool>,
    fatal: Arc<Fatal>,
    branches: usize,
    pending: usize,
    admission: common::EndpointAdmission,
    files_source: common::FilesInFlightSource,
    receiver_limits: remote::protocol::DirectoryLimits,
    errors: Arc<common::error_collector::ErrorCollector>,
) -> anyhow::Result<()> {
    let (directories, branches, pending) = resources::DirectoryBudget::for_endpoint(
        admission,
        files_source,
        branches,
        pending,
        receiver_limits,
    )?;
    let (file_tx, mut file_rx) = tokio::sync::mpsc::channel(pending);
    #[cfg(test)]
    let directories = tests::DIRECTORY_LIMITS
        .try_with(|limits| {
            resources::DirectoryBudget::new(
                limits.normal.get(),
                limits
                    .reserve
                    .get()
                    .min(directories.reserve.available_permits()),
            )
        })
        .unwrap_or(directories);
    let branches = Arc::new(tokio::sync::Semaphore::new(branches));
    #[cfg(test)]
    let branches = tests::SCAN_CREDIT
        .try_with(Clone::clone)
        .unwrap_or(branches);
    let context = Arc::new(DiscoveryContext {
        settings,
        capture,
        root: root.src.clone(),
        control,
        registry: Registry::new(pending),
        branches,
        directories,
        classifiers: Arc::new(tokio::sync::Semaphore::new(pending)),
        files: Arc::new(tokio::sync::Semaphore::new(pending)),
        file_tx,
        fatal: fatal.clone(),
        errors,
        discovery_sealed: std::sync::atomic::AtomicBool::new(false),
        #[cfg(test)]
        hooks: tests::HOOKS.try_with(Clone::clone).ok(),
    });
    {
        // serialize registration with publication: an acceptor can fail before discovery starts
        let primary = fatal.error.lock().unwrap();
        let mut gates = fatal.gates.lock().unwrap();
        for gate in [
            context.registry.credit.clone(),
            context.branches.clone(),
            context.directories.normal.clone(),
            context.directories.reserve.clone(),
            context.directories.lane.clone(),
            context.classifiers.clone(),
            context.files.clone(),
        ] {
            if primary.is_some() {
                gate.close();
            }
            gates.push(gate);
        }
    }
    let mut discovery = tokio::task::JoinSet::new();
    let discovered = context.clone();
    common::task_scope::spawn_tracked(&mut discovery, async move {
        supervise(&discovered.fatal, discover(discovered.clone(), root)).await
    });
    // framed reads are not cancellation safe; the dedicated owner only forwards complete frames
    let (messages, mut received) = tokio::sync::mpsc::channel(16);
    let mut readers = tokio::task::JoinSet::new();
    let reader_fatal = fatal.clone();
    common::task_scope::spawn_tracked(&mut readers, async move {
        // the sender stays outside the caught body so panic publication precedes channel close
        supervise(&reader_fatal, async {
            loop {
                let message = receiver.recv_object::<DestinationMessage>().await;
                let terminal = !matches!(message, Ok(Some(_)));
                if messages.send(message).await.is_err() || terminal {
                    break;
                }
            }
            Ok(())
        })
        .await
    });
    let mut files = tokio::task::JoinSet::new();
    let mut discovery_done = false;
    let mut destination_done = false;
    let mut drain = None;
    // declared after all task owners so cancellation closes gates before aborting them
    let _shutdown = Shutdown(fatal.clone());
    let outcome: anyhow::Result<()> = async {
        loop {
            if destination_done && discovery_done && files.is_empty() && file_rx.is_empty() {
                anyhow::ensure!(context.registry.is_empty(), "DestinationDone with unacknowledged directory Begins");
                break;
            }
            tokio::select! {
                biased;
                _ = fatal.cancel.cancelled() => anyhow::bail!("source operation cancelled"),
                result = discovery.join_next(), if !discovery_done => {
                    result.context("discovery task disappeared")???;
                    discovery_done = true;
                    drain = Some(common::timing_scope!("source.files.drain"));
                }
                result = files.join_next(), if !files.is_empty() => { result.context("file task disappeared")???; }
                job = file_rx.recv(), if !discovery_done || !file_rx.is_empty() => {
                    let job = job.context("file submission channel closed")?;
                    common::task_scope::spawn_tracked(&mut files, file_job(context.clone(), pool.clone(), job));
                }
                message = received.recv(), if !destination_done => {
                    match message.context("control reader exited without a terminal message")??.context("destination closed control before DestinationDone")? {
                        DestinationMessage::DirectoryManifestChunk { dst, entries } => context.registry.manifest(&dst, entries)?,
                        DestinationMessage::DirectoryReady { src, dst } => context.registry.resolve(&src, &dst, false)?,
                        DestinationMessage::DirectorySkipped { src, dst } => context.registry.resolve(&src, &dst, true)?,
                        DestinationMessage::DirectoryReleased { src, dst } => context.registry.release(&src, &dst)?,
                        DestinationMessage::DestinationDone => {
                            anyhow::ensure!(context.discovery_sealed.load(std::sync::atomic::Ordering::Acquire), "DestinationDone before source discovery completed");
                            anyhow::ensure!(context.registry.is_empty(), "DestinationDone with unacknowledged directory Begins");
                            destination_done = true;
                            #[cfg(test)]
                            context.checkpoint("destination_done", &context.root, 0).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }.await;
    if let Err(error) = outcome {
        fatal.publish(error);
    }
    // always close admission before JoinSet drops can abort started classifiers; normal Done
    // drains every file result first and therefore never cancels an armed child obligation
    if fatal.cancel.is_cancelled() {
        discovery.abort_all();
        files.abort_all();
    } else if let Some(drain) = drain {
        drain.finish();
    }
    readers.abort_all();
    while let Some(result) = readers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => fatal.publish(error),
            Err(error) if !error.is_cancelled() => fatal.publish(error.into()),
            Err(_) => {}
        }
    }
    while discovery.join_next().await.is_some() {}
    while files.join_next().await.is_some() {}
    if let Some(error) = fatal.take() {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    tokio::task_local! { pub(super) static DIRECTORY_LIMITS: remote::protocol::DirectoryLimits; }
    tokio::task_local! { pub(super) static SCAN_CREDIT: Arc<tokio::sync::Semaphore>; }

    fn normal_directory_limit(normal: usize) -> remote::protocol::DirectoryLimits {
        remote::protocol::DirectoryLimits {
            normal: std::num::NonZeroUsize::new(normal).unwrap(),
            reserve: std::num::NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS).unwrap(),
        }
    }

    fn submission_context(
        writer: remote::streams::BoxedWrite,
    ) -> (Arc<DiscoveryContext>, tokio::sync::mpsc::Receiver<FileJob>) {
        let (file_tx, file_rx) = tokio::sync::mpsc::channel(1);
        let mut configuration = settings(false);
        configuration.overwrite = true;
        (
            Arc::new(DiscoveryContext {
                settings: configuration,
                capture: Default::default(),
                root: "/source".into(),
                control: Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                    writer,
                ))),
                registry: Registry::new(1),
                branches: Arc::new(tokio::sync::Semaphore::new(1)),
                directories: resources::DirectoryBudget::new(1, 1),
                classifiers: Arc::new(tokio::sync::Semaphore::new(1)),
                files: Arc::new(tokio::sync::Semaphore::new(1)),
                file_tx,
                fatal: Arc::new(Fatal::new(Default::default())),
                errors: Default::default(),
                discovery_sealed: Default::default(),
                hooks: None,
            }),
            file_rx,
        )
    }

    async fn unchanged_submission(
        context: &DiscoveryContext,
        ready: bool,
    ) -> anyhow::Result<(Observation, Arc<Readiness>, Obligation)> {
        let observation = Observation {
            pair: SrcDst {
                src: "/source/file".into(),
                dst: "/destination/file".into(),
            },
            name: "file".into(),
            kind: EntryKind::File,
            size: 1024,
            metadata: Metadata {
                mode: 0o100644,
                uid: 1000,
                gid: 1000,
                atime: 1,
                mtime: 2,
                atime_nsec: 0,
                mtime_nsec: 3,
                acls: remote::protocol::WireAcls::Unknown,
            },
            target: None,
            included: true,
        };
        let pair = SrcDst {
            src: "/source".into(),
            dst: "/destination".into(),
        };
        let readiness = context
            .registry
            .register(&pair, directory_credit().await)
            .await?;
        context.registry.manifest(
            &pair.dst,
            vec![ExistingEntry {
                name: "file".into(),
                is_file: true,
                metadata: observation.metadata.clone(),
                size: 1024,
            }],
        )?;
        if ready {
            context.registry.resolve(&pair.src, &pair.dst, false)?;
        }
        let obligation = Obligation::new(context.fatal.clone(), observation.pair.src.clone());
        Ok((observation, readiness, obligation))
    }

    #[tokio::test]
    async fn spawned_file_job_stays_below_large_allocation_threshold() -> anyhow::Result<()> {
        #[derive(Default)]
        struct SpawnSize(usize);
        impl tracing::field::Visit for SpawnSize {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if matches!(field.name(), "size.bytes" | "original_size.bytes") {
                    self.0 = self.0.max(value as usize);
                }
            }
        }
        struct Capture(Arc<std::sync::Mutex<Vec<usize>>>);
        impl tracing::Subscriber for Capture {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::Id {
                if attributes.metadata().target() == "tokio::task"
                    && attributes.metadata().name() == "runtime.spawn"
                {
                    let mut size = SpawnSize::default();
                    attributes.record(&mut size);
                    self.0.lock().unwrap().push(size.0);
                }
                tracing::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {}
            fn enter(&self, _: &tracing::Id) {}
            fn exit(&self, _: &tracing::Id) {}
        }

        let (context, mut queued) = submission_context(Box::new(tokio::io::sink()));
        let (observation, readiness, obligation) = unchanged_submission(&context, false).await?;
        context
            .submit_file(
                observation,
                Parent::Path,
                Some(readiness),
                obligation,
                false,
            )
            .await?;
        let job = queued.recv().await.unwrap();
        context
            .registry
            .resolve(Path::new("/source"), Path::new("/destination"), false)?;
        let (return_tx, recv) = async_channel::bounded(1);
        let pool = Arc::new(super::super::AcceptingSendStreamPool { recv, return_tx });
        let future = file_job(context, pool, job);
        let file_job_size = std::mem::size_of_val(&future);
        let sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = Capture(sizes.clone());
        common::task_scope::scope_tasks(async {
            let mut tasks = tokio::task::JoinSet::new();
            tracing::subscriber::with_default(subscriber, || {
                common::task_scope::spawn_tracked(&mut tasks, future);
            });
            tasks.join_next().await.unwrap()??;
            anyhow::Ok(())
        })
        .await?;
        let sizes = sizes.lock().unwrap();
        assert_eq!(
            sizes.len(),
            1,
            "the actual file-task spawn was not observed"
        );
        assert!(
            sizes[0] >= file_job_size,
            "spawn size omitted the original future"
        );
        eprintln!(
            "spawned file future: {} bytes; file_job: {file_job_size} bytes",
            sizes[0]
        );
        // tokio 1.53 boxes release futures over 16 KiB. Leave at least 4 KiB for task-scope
        // wrappers and subsequent fields; a near-threshold unboxed job is too fragile.
        assert!(
            sizes[0] <= 12 * 1024,
            "spawned file future is {} bytes (file_job: {file_job_size}); exceeds 12 KiB budget",
            sizes[0]
        );
        Ok(())
    }

    #[tokio::test]
    async fn ready_unchanged_file_bypasses_saturated_file_admission() -> anyhow::Result<()> {
        let (write, read) = tokio::io::duplex(4096);
        let (context, mut queued) = submission_context(Box::new(write));
        let _occupied = context.files.clone().acquire_owned().await?;
        let (observation, readiness, obligation) = unchanged_submission(&context, true).await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            context.submit_file(
                observation,
                Parent::Path,
                Some(readiness),
                obligation,
                false,
            ),
        )
        .await
        .context("ready unchanged file waited for file admission")??;
        assert!(matches!(
            queued.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let mut receive = remote::streams::RecvStream::new(read);
        assert!(matches!(receive.recv_object::<SourceMessage>().await?,
            Some(SourceMessage::FileUnchanged { src, dst })
                if src == Path::new("/source/file") && dst == Path::new("/destination/file")));
        assert!(context.fatal.take().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn pending_manifest_queues_file_before_readiness() -> anyhow::Result<()> {
        let (write, read) = tokio::io::duplex(4096);
        let (context, mut queued) = submission_context(Box::new(write));
        let occupied = context.files.clone().acquire_owned().await?;
        let (observation, readiness, obligation) = unchanged_submission(&context, false).await?;
        let submission = context.submit_file(
            observation,
            Parent::Path,
            Some(readiness),
            obligation,
            false,
        );
        tokio::pin!(submission);
        assert!(futures::poll!(&mut submission).is_pending());
        drop(occupied);
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut submission)
            .await
            .context("file submission waited for pending manifest")??;
        let job = queued.try_recv()?;
        let (send, receive) = async_channel::bounded(1);
        let pool = Arc::new(super::super::AcceptingSendStreamPool {
            recv: receive,
            return_tx: send,
        });
        let sending = file_job(context.clone(), pool, job);
        tokio::pin!(sending);
        assert!(futures::poll!(&mut sending).is_pending());
        context
            .registry
            .resolve(Path::new("/source"), Path::new("/destination"), false)?;
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut sending).await??;
        let mut receive = remote::streams::RecvStream::new(read);
        assert!(matches!(
            receive.recv_object::<SourceMessage>().await?,
            Some(SourceMessage::FileUnchanged { .. })
        ));
        assert!(context.fatal.take().is_none());
        assert_eq!(context.files.available_permits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn ready_unchanged_send_failure_keeps_original_cause() -> anyhow::Result<()> {
        let (context, mut queued) = submission_context(Box::new(FailingWriter));
        let _occupied = context.files.clone().acquire_owned().await?;
        let (observation, readiness, obligation) = unchanged_submission(&context, true).await?;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            context.submit_file(
                observation,
                Parent::Path,
                Some(readiness),
                obligation,
                false,
            ),
        )
        .await
        .context("ready unchanged failure waited for file admission")?;
        assert!(result.is_err());
        assert!(matches!(
            queued.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let error = context
            .fatal
            .take()
            .context("send failure was not published")?;
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .to_string(),
            "original payload send failure"
        );
        Ok(())
    }

    #[tokio::test]
    async fn pipelined_directory_descriptors_stay_within_admitted_ownership_bounds()
    -> anyhow::Result<()> {
        const NORMAL: usize = 1;
        const RESERVE: usize = 16;
        const LEAF_CAPACITY: usize = 4;
        let temp = tempfile::tempdir()?;
        for branch in 0..16 {
            let mut path = temp.path().join(format!("branch-{branch}"));
            for _ in 0..12 {
                std::fs::create_dir(&path)?;
                path.push("child");
            }
        }
        // a comb retains sibling work at every suspended level, and the root spans batches.
        for branch in 0..4 {
            let mut path = temp.path().join(format!("comb-{branch}"));
            for _ in 0..8 {
                std::fs::create_dir(&path)?;
                std::fs::create_dir(path.join("left"))?;
                std::fs::create_dir(path.join("right"))?;
                path.push("next");
            }
        }
        for index in 0..70 {
            std::fs::create_dir(temp.path().join(format!("wide-{index}")))?;
        }
        fn source_descriptors(root: &Path) -> std::io::Result<usize> {
            Ok(std::fs::read_dir("/proc/self/fd")?
                .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
                .filter(|target| target.starts_with(root))
                .count())
        }
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hooks = Hooks::new({
            let peak = peak.clone();
            let root = temp.path().to_owned();
            move |event, _, _| {
                let measure = event == "cursor";
                let peak = peak.clone();
                let root = root.clone();
                Box::pin(async move {
                    if measure {
                        peak.fetch_max(
                            source_descriptors(&root)?,
                            std::sync::atomic::Ordering::SeqCst,
                        );
                    }
                    Ok(())
                })
            }
        });
        let (source, send, recv, errors) =
            connection(temp.path(), false, LEAF_CAPACITY, LEAF_CAPACITY, Vec::new());
        let (source, peer) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            DIRECTORY_LIMITS.scope(
                remote::protocol::DirectoryLimits {
                    normal: std::num::NonZeroUsize::new(NORMAL).unwrap(),
                    reserve: std::num::NonZeroUsize::new(RESERVE).unwrap(),
                },
                HOOKS.scope(
                    hooks,
                    common::task_scope::scope_tasks(async {
                        tokio::join!(source, acknowledge_directories(send, recv))
                    }),
                ),
            ),
        )
        .await?;
        source?;
        let messages = peer?;
        assert!(!errors.has_errors());
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, SourceMessage::DirectoryBegin { .. }))
                .count(),
            359
        );
        let held = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            held >= 24,
            "the measurement must observe the live depth-12 path"
        );
        // each directory owns at most a held Dir and cursor, with four transient fds per leaf.
        // the trusted parent above this fixture is outside the measured subtree.
        let bound = 2 * (NORMAL + RESERVE) + 4 * LEAF_CAPACITY;
        assert!(
            held <= bound,
            "walk retained {held} descriptors; admission bounds ownership at {bound}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn completed_ancestors_release_scan_capacity_for_a_wide_frontier() -> anyhow::Result<()> {
        for scans in [4, 8] {
            for depth in [0, 3, 7] {
                for dereference in [false, true] {
                    let temp = tempfile::tempdir()?;
                    let mut frontier = temp.path().to_owned();
                    for index in 0..depth {
                        frontier.push(format!("prefix-{index}"));
                    }
                    std::fs::create_dir_all(&frontier)?;
                    for index in 0..scans {
                        std::fs::create_dir(frontier.join(format!("leaf-{index}")))?;
                    }
                    let entered = Arc::new(tokio::sync::Semaphore::new(0));
                    let release = Arc::new(tokio::sync::Semaphore::new(0));
                    let branches = Arc::new(tokio::sync::Semaphore::new(scans));
                    let hooks = Hooks::new({
                        let entered = entered.clone();
                        let release = release.clone();
                        let branches = branches.clone();
                        let frontier = frontier.clone();
                        move |event, path, _| {
                            let at_frontier = event == "cursor" && path == frontier;
                            let at_leaf = event == "cursor"
                                && path
                                    .file_name()
                                    .unwrap()
                                    .as_encoded_bytes()
                                    .starts_with(b"leaf-");
                            let entered = entered.clone();
                            let release = release.clone();
                            let branches = branches.clone();
                            Box::pin(async move {
                                if at_frontier {
                                    // wait until prefix scans relinquish their credits. An active
                                    // prefix can otherwise force the first leaf inline before the
                                    // remaining siblings can reach the test's leaf barrier.
                                    let available =
                                        branches.acquire_many_owned((scans - 1) as u32).await?;
                                    drop(available);
                                }
                                if at_leaf {
                                    entered.add_permits(1);
                                    release.acquire_owned().await?.forget();
                                }
                                Ok(())
                            })
                        }
                    });
                    let (source, send, recv, _) =
                        connection(temp.path(), dereference, scans, scans, Vec::new());
                    let frontier_progress = async {
                        entered.acquire_many_owned(scans as u32).await?.forget();
                        release.add_permits(scans);
                        anyhow::Ok(())
                    };
                    let (source, peer, frontier_progress) = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        SCAN_CREDIT.scope(branches.clone(), DIRECTORY_LIMITS.scope(normal_directory_limit(scans + depth + 2),
                            HOOKS.scope(hooks.clone(), common::task_scope::scope_tasks(async {
                                tokio::join!(source, acknowledge_directories(send, recv), frontier_progress)
                            })))),
                    ).await.with_context(|| format!(
                        "completed ancestors retained scan capacity: E={scans}, depth={depth}, dereference={dereference}; available={}; events={:?}", branches.available_permits(), hooks.events.lock().unwrap()))?;
                    source?;
                    peer?;
                    frontier_progress?;
                }
            }
        }
        Ok(())
    }
    async fn directory_credit() -> Arc<resources::DirectoryCredit> {
        let budget = resources::DirectoryBudget::new(1, 1);
        let scans = Arc::new(tokio::sync::Semaphore::new(1));
        budget
            .admit(resources::Scan::Normal(
                scans.acquire_owned().await.unwrap(),
            ))
            .await
            .unwrap()
            .credit()
    }

    #[tokio::test]
    async fn reserved_depth_failure_settles_the_child_without_aborting_collect_errors()
    -> anyhow::Result<()> {
        for fail_early in [false, true] {
            let (writer, reader) = tokio::io::duplex(4096);
            let (mut context, _files) = submission_context(Box::new(writer));
            Arc::get_mut(&mut context).unwrap().settings.fail_early = fail_early;
            let scans = Arc::new(tokio::sync::Semaphore::new(1));
            let mut root = context
                .directories
                .admit(resources::Scan::Normal(scans.acquire_owned().await?))
                .await?;
            let mut reserved = context.directories.admit(root.descend()).await?;
            let observation = Observation {
                pair: SrcDst {
                    src: "/source/deep".into(),
                    dst: "/destination/deep".into(),
                },
                name: "deep".into(),
                kind: EntryKind::Dir,
                size: 0,
                metadata: Metadata {
                    mode: 0o40700,
                    uid: 1000,
                    gid: 1000,
                    atime: 1,
                    mtime: 2,
                    atime_nsec: 0,
                    mtime_nsec: 0,
                    acls: remote::protocol::WireAcls::Unknown,
                },
                target: None,
                included: true,
            };
            let result = directory(
                context.clone(),
                Parent::Path,
                observation,
                false,
                reserved.descend(),
            )
            .await;
            if fail_early {
                result.expect_err("fail-early must stop on depth exhaustion");
                let error = context
                    .fatal
                    .take()
                    .expect("the original depth failure must be published");
                assert!(error.is::<resources::ReservedDepthExhausted>(), "{error:#}");
                assert!(context.fatal.cancel.is_cancelled());
            } else {
                result?;
                assert!(!context.fatal.cancel.is_cancelled());
                let error = context
                    .errors
                    .take_error()
                    .expect("depth exhaustion must be collected");
                assert!(error.is::<resources::ReservedDepthExhausted>(), "{error:#}");
                assert!(format!("{error:#}").contains("/source/deep"));
                let mut reader = remote::streams::RecvStream::new(
                    Box::new(reader) as remote::streams::BoxedRead
                );
                let message = reader.recv_object::<SourceMessage>().await?.unwrap();
                assert!(matches!(message, SourceMessage::FileSkipped { src, dst }
                    if src == Path::new("/source/deep") && dst == Path::new("/destination/deep")));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn reserved_tombstone_returns_before_release_while_retaining_lifetime_credit()
    -> anyhow::Result<()> {
        let (context, _) = submission_context(Box::new(tokio::io::sink()));
        let scans = Arc::new(tokio::sync::Semaphore::new(1));
        let mut normal = context
            .directories
            .admit(resources::Scan::Normal(scans.acquire_owned().await?))
            .await?;
        let reserved = context.directories.admit(normal.descend()).await?;
        let pair = SrcDst {
            src: "source/failed-cursor".into(),
            dst: "destination/failed-cursor".into(),
        };
        let metadata = Metadata {
            mode: 0o40700,
            uid: 1000,
            gid: 1000,
            atime: 1,
            mtime: 2,
            atime_nsec: 0,
            mtime_nsec: 0,
            acls: remote::protocol::WireAcls::Unknown,
        };
        context
            .tombstone(&pair, metadata, false, &reserved)
            .now_or_never()
            .expect("an ended tombstone must not wait for a receiver round trip")?;
        drop(reserved);
        assert_eq!(context.directories.reserve.available_permits(), 0);
        context.registry.resolve(&pair.src, &pair.dst, false)?;
        assert_eq!(
            context.directories.reserve.available_permits(),
            0,
            "Ready returned lifetime credit"
        );
        context.registry.release(&pair.src, &pair.dst)?;
        assert_eq!(context.directories.reserve.available_permits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn directory_release_is_distinct_from_readiness_and_local_descriptor_ownership()
    -> anyhow::Result<()> {
        let budget = resources::DirectoryBudget::new(1, 1);
        let scans = Arc::new(tokio::sync::Semaphore::new(1));
        let admission = budget
            .admit(resources::Scan::Normal(scans.acquire_owned().await?))
            .await?;
        let alias = admission.credit();
        let registry = Registry::new(1);
        let pair = SrcDst {
            src: "source".into(),
            dst: "destination".into(),
        };
        registry.register(&pair, admission.credit()).await?;
        drop(admission);
        registry.resolve(&pair.src, &pair.dst, false)?;
        assert_eq!(
            registry.credit.available_permits(),
            1,
            "Ready must return only Begin P"
        );
        assert_eq!(budget.normal.available_permits(), 0);
        assert!(
            !registry.is_empty(),
            "Ready removed the outstanding lifetime"
        );
        assert!(registry.release(Path::new("wrong"), &pair.dst).is_err());
        registry.release(&pair.src, &pair.dst)?;
        assert!(registry.is_empty());
        assert!(registry.release(&pair.src, &pair.dst).is_err());
        assert_eq!(
            budget.normal.available_permits(),
            0,
            "receiver release discarded a local alias"
        );
        drop(alias);
        assert_eq!(budget.normal.available_permits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_directory_release_can_precede_skipped_without_resolving_readiness()
    -> anyhow::Result<()> {
        let registry = Registry::new(1);
        let pair = SrcDst {
            src: "source".into(),
            dst: "destination".into(),
        };
        let ready = registry.register(&pair, directory_credit().await).await?;
        registry.release(&pair.src, &pair.dst)?;
        assert!(matches!(*ready.0.borrow(), ReadyState::Pending));
        assert!(registry.release(&pair.src, &pair.dst).is_err());
        assert!(registry.resolve(&pair.src, &pair.dst, false).is_err());
        registry.resolve(&pair.src, &pair.dst, true)?;
        assert!(matches!(ready.wait().await, ReadyState::Rejected));
        assert!(registry.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn acknowledgement_validates_both_paths_and_releases_credit_once() {
        let registry = Registry::new(1);
        let pair = remote::protocol::SrcDst {
            src: "src".into(),
            dst: "dst".into(),
        };
        let ready = registry
            .register(&pair, directory_credit().await)
            .await
            .unwrap();
        assert_eq!(registry.credit.available_permits(), 0);
        assert!(
            registry
                .resolve(std::path::Path::new("wrong"), &pair.dst, false)
                .is_err()
        );
        assert_eq!(registry.credit.available_permits(), 0);
        registry.resolve(&pair.src, &pair.dst, false).unwrap();
        assert!(matches!(ready.wait().await, ReadyState::Ready(_)));
        assert_eq!(registry.credit.available_permits(), 1);
        assert!(registry.resolve(&pair.src, &pair.dst, false).is_err());
        assert!(registry.manifest(&pair.dst, Vec::new()).is_err());
    }
    #[tokio::test]
    async fn rejection_is_terminal_and_discards_manifest() {
        let registry = Registry::new(1);
        let pair = remote::protocol::SrcDst {
            src: "src".into(),
            dst: "dst".into(),
        };
        let ready = registry
            .register(&pair, directory_credit().await)
            .await
            .unwrap();
        registry.resolve(&pair.src, &pair.dst, true).unwrap();
        assert!(matches!(ready.wait().await, ReadyState::Rejected));
        assert!(registry.resolve(&pair.src, &pair.dst, true).is_err());
        assert_eq!(registry.credit.available_permits(), 1);
    }
    #[tokio::test]
    async fn lost_obligation_cancels_admission_without_replacing_primary_error() {
        let fatal = Arc::new(Fatal::new(tokio_util::sync::CancellationToken::new()));
        let guard = Obligation::new(fatal.clone(), "entry".into());
        fatal.publish(anyhow::anyhow!("original cause"));
        drop(guard);
        assert!(fatal.cancel.is_cancelled());
        assert_eq!(fatal.take().unwrap().to_string(), "original cause");
    }
    #[tokio::test]
    async fn cancelled_obligation_is_fatal() {
        let fatal = Arc::new(Fatal::new(tokio_util::sync::CancellationToken::new()));
        drop(Obligation::new(fatal.clone(), "entry".into()));
        assert!(fatal.take().unwrap().to_string().contains("obligation"));
    }
    #[tokio::test]
    async fn collected_error_does_not_replace_the_failure_that_aborts_discovery()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("dangling");
        std::os::unix::fs::symlink("missing", &path)?;
        let (source, mut send, mut receive, errors) =
            connection(temp.path(), true, 1, 1, Vec::new());
        let peer = async {
            loop {
                match receive.recv_object::<SourceMessage>().await?.unwrap() {
                    SourceMessage::DirectoryBegin { src, dst, .. } => {
                        send.send_control_message(&DestinationMessage::DirectoryReady { src, dst })
                            .await?;
                    }
                    SourceMessage::DirectoryEnd { .. } => {
                        assert!(
                            errors.has_errors(),
                            "the earlier classification error was not collected"
                        );
                        send.close().await?;
                        return anyhow::Ok(());
                    }
                    other => anyhow::bail!("unexpected message: {other:?}"),
                }
            }
        };
        let (source, peer) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            common::task_scope::scope_tasks(async { tokio::join!(source, peer) }),
        )
        .await?;
        peer?;
        let error = source.unwrap_err();
        assert!(
            format!("{error:#}").contains("destination closed control"),
            "{error:#}"
        );
        Ok(())
    }
    #[tokio::test]
    async fn dereferenced_missing_entry_reports_its_source_path() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("dangling");
        std::os::unix::fs::symlink("missing", &path)?;
        let (source, send, receive, _) = connection(&path, true, 1, 1, Vec::new());
        let (source, _) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            common::task_scope::scope_tasks(async {
                tokio::join!(source, acknowledge_directories(send, receive))
            }),
        )
        .await?;
        let error = source.expect_err("dereferencing a dangling link must fail");
        assert!(
            format!("{error:#}").contains(path.to_str().unwrap()),
            "{error:#}"
        );
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        Ok(())
    }
    #[tokio::test]
    async fn directory_open_failure_reports_its_source_path() -> anyhow::Result<()> {
        for dereference in [false, true] {
            let temp = tempfile::tempdir()?;
            let path = temp.path().join("changed-directory");
            std::fs::create_dir(&path)?;
            let hooks = Hooks::new({
                let path = path.clone();
                move |event, observed, _| {
                    let replace = event == "classified" && observed == path;
                    let path = path.clone();
                    Box::pin(async move {
                        if replace {
                            std::fs::remove_dir(&path)?;
                            std::fs::write(path, b"replacement")?;
                        }
                        Ok(())
                    })
                }
            });
            let (source, send, receive, errors) =
                connection(temp.path(), dereference, 1, 1, Vec::new());
            let (source, peer) = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                HOOKS.scope(
                    hooks,
                    common::task_scope::scope_tasks(async {
                        tokio::join!(source, acknowledge_directories(send, receive))
                    }),
                ),
            )
            .await?;
            source?;
            peer?;
            let error = errors
                .take_error()
                .context("replacement was not reported")?;
            assert!(
                format!("{error:#}").contains(path.to_str().unwrap()),
                "{error:#}"
            );
            assert_eq!(
                error
                    .root_cause()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .raw_os_error(),
                Some(libc::ENOTDIR)
            );
        }
        Ok(())
    }
    #[tokio::test]
    async fn directory_enumeration_failure_reports_its_source_path() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let hooks = Hooks::new(|event, _, _| {
            let fail = event == "read_batch";
            Box::pin(async move {
                if fail {
                    return Err(std::io::Error::from_raw_os_error(libc::EACCES).into());
                }
                Ok(())
            })
        });
        let (source, send, receive, errors) = connection(temp.path(), false, 1, 1, Vec::new());
        let (source, peer) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HOOKS.scope(
                hooks,
                common::task_scope::scope_tasks(async {
                    tokio::join!(source, acknowledge_directories(send, receive))
                }),
            ),
        )
        .await?;
        source?;
        peer?;
        let error = errors
            .take_error()
            .context("enumeration failure was not reported")?;
        assert!(
            format!("{error:#}").contains(temp.path().to_str().unwrap()),
            "{error:#}"
        );
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        Ok(())
    }
    async fn symlink_read_failure(
        is_root: bool,
        excluded: bool,
        fail_early: bool,
    ) -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let link = temp.path().join("link");
        std::os::unix::fs::symlink("target", &link)?;
        let hooks = Hooks::new(|event, _, _| {
            let fail = event == "read_symlink";
            Box::pin(async move {
                if fail {
                    return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
                }
                Ok(())
            })
        });
        let mut configuration = settings(false);
        configuration.fail_early = fail_early;
        if excluded {
            let mut filter = common::filter::FilterSettings::new();
            filter.add_exclude("link")?;
            configuration.filter = Some(filter);
        }
        let root = if is_root { link.as_path() } else { temp.path() };
        let (source, send, receive, errors) =
            configured_connection(root, configuration, 1, 1, Vec::new());
        let (source, peer) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HOOKS.scope(
                hooks.clone(),
                common::task_scope::scope_tasks(async {
                    tokio::join!(source, acknowledge_directories(send, receive))
                }),
            ),
        )
        .await?;
        if excluded {
            source?;
            let messages = peer?;
            assert!(
                !errors.has_errors(),
                "an excluded symlink attempted a failing target read"
            );
            assert!(
                !hooks
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(event, _, _)| event == "read_symlink")
            );
            assert!(
                matches!(messages.last(), Some(SourceMessage::DiscoveryComplete { has_root_item }) if *has_root_item != is_root)
            );
        } else {
            let error = if is_root || fail_early {
                assert!(peer.is_err());
                source.expect_err("an unreadable root or fail-early symlink must abort")
            } else {
                source?;
                let messages = peer?;
                assert!(messages.iter().any(|message| matches!(
                    message,
                    SourceMessage::DirectoryEnd { entry_count: 0, .. }
                )));
                errors
                    .take_error()
                    .context("the symlink read failure was not collected")?
            };
            assert!(
                format!("{error:#}").contains(link.to_str().unwrap()),
                "{error:#}"
            );
            assert_eq!(
                error
                    .root_cause()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .raw_os_error(),
                Some(libc::EIO)
            );
        }
        Ok(())
    }
    #[tokio::test]
    async fn excluded_root_symlink_does_not_read_its_target() -> anyhow::Result<()> {
        symlink_read_failure(true, true, false).await
    }
    #[tokio::test]
    async fn excluded_child_symlink_does_not_read_its_target() -> anyhow::Result<()> {
        symlink_read_failure(false, true, false).await
    }
    #[tokio::test]
    async fn included_symlink_read_failures_finish_without_pending_children() -> anyhow::Result<()>
    {
        symlink_read_failure(true, false, false).await?;
        symlink_read_failure(false, false, false).await?;
        symlink_read_failure(false, false, true).await
    }
    fn settings(dereference: bool) -> common::copy::Settings {
        common::copy::Settings {
            reflink: Default::default(),
            dereference,
            fail_early: false,
            overwrite: false,
            overwrite_compare: Default::default(),
            overwrite_filter: None,
            ignore_existing: false,
            chunk_size: 0,
            skip_specials: false,
            remote_copy_buffer_size: 4096,
            filter: None,
            dry_run: None,
            delete: None,
        }
    }
    type HookCallback = Box<
        dyn Fn(&str, &Path, usize) -> futures::future::BoxFuture<'static, anyhow::Result<()>>
            + Send
            + Sync,
    >;

    pub(super) struct Hooks {
        pub(super) events: std::sync::Mutex<Vec<(String, PathBuf, usize)>>,
        pub(super) callback: HookCallback,
    }
    impl Hooks {
        fn new(
            callback: impl Fn(
                &str,
                &Path,
                usize,
            ) -> futures::future::BoxFuture<'static, anyhow::Result<()>>
            + Send
            + Sync
            + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                events: Default::default(),
                callback: Box::new(callback),
            })
        }
    }
    tokio::task_local! { pub(super) static HOOKS: Arc<Hooks>; }
    type SourceRun =
        std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>;
    fn connection(
        root: &Path,
        dereference: bool,
        e: usize,
        p: usize,
        writers: Vec<remote::streams::BoxedWrite>,
    ) -> (
        SourceRun,
        remote::streams::BoxedSendStream,
        remote::streams::BoxedRecvStream,
        Arc<common::error_collector::ErrorCollector>,
    ) {
        configured_connection(root, settings(dereference), e, p, writers)
    }
    fn configured_connection(
        root: &Path,
        configuration: common::copy::Settings,
        e: usize,
        p: usize,
        writers: Vec<remote::streams::BoxedWrite>,
    ) -> (
        SourceRun,
        remote::streams::BoxedSendStream,
        remote::streams::BoxedRecvStream,
        Arc<common::error_collector::ErrorCollector>,
    ) {
        let (source, destination) = tokio::io::duplex(1024 * 1024);
        let (source_read, source_write) = tokio::io::split(source);
        let (destination_read, destination_write) = tokio::io::split(destination);
        let (send, receive) = async_channel::bounded(e);
        for writer in writers {
            send.try_send(remote::streams::SendStream::new(writer))
                .unwrap();
        }
        let pool = Arc::new(super::super::AcceptingSendStreamPool {
            recv: receive,
            return_tx: send,
        });
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let future = run(
            configuration,
            Default::default(),
            SrcDst {
                src: root.to_owned(),
                dst: PathBuf::from("/destination"),
            },
            Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(source_write) as remote::streams::BoxedWrite,
            ))),
            remote::streams::RecvStream::new(Box::new(source_read) as remote::streams::BoxedRead),
            pool,
            Arc::new(Fatal::new(Default::default())),
            e,
            p,
            common::EndpointAdmission::Remote(
                common::RemoteResources::for_limits(
                    std::num::NonZeroU64::new(1024),
                    std::num::NonZeroUsize::new(e).unwrap(),
                    std::num::NonZeroUsize::new(e).unwrap(),
                    std::num::NonZeroUsize::new(p).unwrap(),
                )
                .unwrap(),
            ),
            common::FilesInFlightSource::Automatic,
            remote::protocol::DirectoryLimits {
                normal: std::num::NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS).unwrap(),
                reserve: std::num::NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS).unwrap(),
            },
            errors.clone(),
        );
        (
            Box::pin(future),
            remote::streams::SendStream::new(Box::new(destination_write)),
            remote::streams::RecvStream::new(Box::new(destination_read)),
            errors,
        )
    }
    struct PeerDirectory {
        src: PathBuf,
        ready: bool,
        ended: bool,
        children: usize,
    }

    #[derive(Default)]
    struct PeerLifetimes(HashMap<PathBuf, PeerDirectory>);

    impl PeerLifetimes {
        fn begin(&mut self, src: &Path, dst: &Path) {
            if let Some(parent) = dst.parent().and_then(|parent| self.0.get_mut(parent)) {
                parent.children += 1;
            }
            assert!(
                self.0
                    .insert(
                        dst.to_owned(),
                        PeerDirectory {
                            src: src.to_owned(),
                            ready: false,
                            ended: false,
                            children: 0,
                        }
                    )
                    .is_none()
            );
        }
        fn ready(&mut self, dst: &Path) {
            self.0.get_mut(dst).unwrap().ready = true;
        }
        fn end(&mut self, dst: &Path) {
            self.0.get_mut(dst).unwrap().ended = true;
        }
        fn completed(&mut self) -> Vec<SrcDst> {
            let mut completed = Vec::new();
            while let Some(dst) = self.0.iter().find_map(|(dst, entry)| {
                (entry.ready && entry.ended && entry.children == 0).then(|| dst.clone())
            }) {
                let record = self.0.remove(&dst).unwrap();
                if let Some(parent) = dst.parent().and_then(|parent| self.0.get_mut(parent)) {
                    parent.children -= 1;
                }
                completed.push(SrcDst {
                    src: record.src,
                    dst,
                });
            }
            completed
        }
        async fn flush(
            &mut self,
            send: &mut remote::streams::BoxedSendStream,
        ) -> anyhow::Result<()> {
            for pair in self.completed() {
                send.send_control_message(&DestinationMessage::DirectoryReleased {
                    src: pair.src,
                    dst: pair.dst,
                })
                .await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn fake_peer_retains_ended_ancestors_until_begun_children_finish() -> anyhow::Result<()> {
        let (writer, reader) = tokio::io::duplex(4096);
        let mut send =
            remote::streams::SendStream::new(Box::new(writer) as remote::streams::BoxedWrite);
        let mut receive =
            remote::streams::RecvStream::new(Box::new(reader) as remote::streams::BoxedRead);
        let mut lifetimes = PeerLifetimes::default();
        lifetimes.begin(Path::new("s"), Path::new("d"));
        lifetimes.begin(Path::new("s/c"), Path::new("d/c"));
        lifetimes.ready(Path::new("d"));
        lifetimes.ready(Path::new("d/c"));
        lifetimes.end(Path::new("d"));
        lifetimes.flush(&mut send).await?;
        assert_eq!(
            lifetimes.0.len(),
            2,
            "parent End incorrectly returned its lifetime"
        );
        lifetimes.end(Path::new("d/c"));
        lifetimes.flush(&mut send).await?;
        assert!(
            matches!(receive.recv_object::<DestinationMessage>().await?, Some(DestinationMessage::DirectoryReleased {dst, ..}) if dst == Path::new("d/c"))
        );
        assert!(
            matches!(receive.recv_object::<DestinationMessage>().await?, Some(DestinationMessage::DirectoryReleased {dst, ..}) if dst == Path::new("d"))
        );
        assert!(lifetimes.0.is_empty());
        Ok(())
    }

    async fn acknowledge_directories(
        mut send: remote::streams::BoxedSendStream,
        mut recv: remote::streams::BoxedRecvStream,
    ) -> anyhow::Result<Vec<SourceMessage>> {
        let mut messages = Vec::new();
        let mut lifetimes = PeerLifetimes::default();
        loop {
            let message = recv
                .recv_object::<SourceMessage>()
                .await?
                .context("source closed before discovery complete")?;
            match &message {
                SourceMessage::DirectoryBegin { src, dst, .. } => {
                    lifetimes.begin(src, dst);
                    send.send_control_message(&DestinationMessage::DirectoryReady {
                        src: src.clone(),
                        dst: dst.clone(),
                    })
                    .await?;
                    lifetimes.ready(dst);
                }
                SourceMessage::DirectoryEnd { dst, .. } => lifetimes.end(dst),
                SourceMessage::DiscoveryComplete { .. } => {
                    messages.push(message);
                    lifetimes.flush(&mut send).await?;
                    assert!(lifetimes.0.is_empty());
                    send.send_control_message(&DestinationMessage::DestinationDone)
                        .await?;
                    return Ok(messages);
                }
                _ => {}
            }
            lifetimes.flush(&mut send).await?;
            messages.push(message);
        }
    }
    #[tokio::test]
    async fn original_worker_error_and_panic_precede_obligation_drop() {
        for panic in [false, true] {
            let fatal = Arc::new(Fatal::new(Default::default()));
            let guard = Obligation::new(fatal.clone(), "admitted".into());
            let result = obligated(guard, async move {
                assert!(!panic, "original worker panic");
                Err(anyhow::anyhow!("original worker error"))
            })
            .await;
            assert!(result.is_err());
            let error = fatal.take().unwrap().to_string();
            assert!(
                error.contains(if panic {
                    "original worker panic"
                } else {
                    "original worker error"
                }),
                "{error}"
            );
        }
    }
    #[tokio::test]
    async fn capacity_one_deep_tree_uses_one_cursor_and_one_classification() -> anyhow::Result<()> {
        for dereference in [false, true] {
            let temp = tempfile::tempdir()?;
            let mut deep = temp.path().to_owned();
            for _ in 0..12 {
                deep.push("child");
                std::fs::create_dir(&deep)?;
            }
            let hooks = Hooks::new(|_, _, _| Box::pin(async { Ok(()) }));
            let (source, send, recv, errors) =
                connection(temp.path(), dereference, 1, 1, Vec::new());
            let (source, messages) = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                HOOKS.scope(
                    hooks.clone(),
                    common::task_scope::scope_tasks(async {
                        tokio::join!(source, acknowledge_directories(send, recv))
                    }),
                ),
            )
            .await?;
            source?;
            let messages = messages?;
            assert!(!errors.has_errors());
            assert_eq!(
                messages
                    .iter()
                    .filter(|m| matches!(m, SourceMessage::DirectoryBegin { .. }))
                    .count(),
                13
            );
            assert_eq!(
                messages
                    .iter()
                    .filter(|m| matches!(m, SourceMessage::DirectoryEnd { .. }))
                    .count(),
                13
            );
            assert!(matches!(
                messages.last(),
                Some(SourceMessage::DiscoveryComplete {
                    has_root_item: true
                })
            ));
            let events = hooks.events.lock().unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|(event, _, _)| event == "cursor")
                    .count(),
                13
            );
            let mut classified = std::collections::HashSet::new();
            for (_, path, _) in events.iter().filter(|(event, _, _)| event == "classified") {
                assert!(
                    classified.insert(path.clone()),
                    "classified twice: {path:?}"
                );
            }
            assert_eq!(classified.len(), 12);
        }
        Ok(())
    }
    struct FailingWriter;
    impl tokio::io::AsyncWrite for FailingWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::other("original payload send failure")))
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
            std::task::Poll::Pending
        }
    }
    #[tokio::test]
    async fn payload_failure_does_not_wait_for_never_ready_close() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("file");
        std::fs::write(&path, b"payload")?;
        let (source, send, recv, _) = connection(&path, false, 1, 1, vec![Box::new(FailingWriter)]);
        let (source, _peer) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            common::task_scope::scope_tasks(async {
                tokio::join!(source, acknowledge_directories(send, recv))
            }),
        )
        .await?;
        let error = source.unwrap_err();
        assert!(
            format!("{error:#}").contains("original payload send failure"),
            "{error:#}"
        );
        Ok(())
    }
    struct GatedWriter {
        started: Option<tokio::sync::mpsc::UnboundedSender<()>>,
        gate: futures::future::BoxFuture<'static, ()>,
        released: bool,
    }
    impl GatedWriter {
        fn new(
            started: tokio::sync::mpsc::UnboundedSender<()>,
            gate: Arc<tokio::sync::Semaphore>,
        ) -> Self {
            Self {
                started: Some(started),
                gate: Box::pin(async move {
                    gate.acquire_owned().await.unwrap().forget();
                }),
                released: false,
            }
        }
    }
    impl tokio::io::AsyncWrite for GatedWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if let Some(started) = self.started.take() {
                started.send(()).unwrap();
            }
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if !self.released {
                if self.gate.as_mut().poll(context).is_pending() {
                    return std::task::Poll::Pending;
                }
                self.released = true;
            }
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }
    #[tokio::test]
    async fn reserved_siblings_scan_before_prior_payloads_and_releases_finish() -> anyhow::Result<()>
    {
        for disconnect in [false, true] {
            let temp = tempfile::tempdir()?;
            for index in 0..8 {
                let leaf = temp.path().join(format!("reserved/leaf-{index}"));
                std::fs::create_dir_all(&leaf)?;
                std::fs::write(leaf.join("payload"), b"payload")?;
            }
            let (cursor_tx, mut cursors) = tokio::sync::mpsc::unbounded_channel();
            let hooks = Hooks::new(move |event, path, _| {
                let leaf = event == "cursor"
                    && path
                        .file_name()
                        .unwrap()
                        .as_encoded_bytes()
                        .starts_with(b"leaf-");
                let cursor_tx = cursor_tx.clone();
                Box::pin(async move {
                    if leaf {
                        let _ = cursor_tx.send(());
                    }
                    Ok(())
                })
            });
            let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let (source, mut send, mut recv, errors) = connection(
                temp.path(),
                false,
                1,
                32,
                vec![Box::new(GatedWriter::new(started, gate.clone()))],
            );
            let (ended_tx, mut ended) = tokio::sync::mpsc::unbounded_channel();
            let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            let peer = async {
                let mut lifetimes = PeerLifetimes::default();
                let mut ended_leaves = 0;
                loop {
                    tokio::select! {
                        _ = close_rx.recv(), if disconnect => { send.close().await?; return anyhow::Ok(()); }
                        message = recv.recv_object::<SourceMessage>() => {
                            match message?.context("source closed before discovery completed")? {
                                SourceMessage::DirectoryBegin { src, dst, .. } => {
                                    lifetimes.begin(&src, &dst);
                                    lifetimes.ready(&dst);
                                    send.send_control_message(&DestinationMessage::DirectoryReady {src, dst}).await?;
                                }
                                SourceMessage::DirectoryEnd { src, dst, .. } => {
                                    if src.file_name().unwrap().as_encoded_bytes().starts_with(b"leaf-") {
                                        ended_leaves += 1;
                                        let _ = ended_tx.send(());
                                    }
                                    lifetimes.end(&dst);
                                    if ended_leaves == 8 {
                                        lifetimes.flush(&mut send).await?;
                                    }
                                }
                                SourceMessage::DiscoveryComplete { .. } => {
                                    if disconnect {
                                        close_rx.recv().await.context("disconnect was not requested")?;
                                        send.close().await?;
                                        return Ok(());
                                    }
                                    lifetimes.flush(&mut send).await?;
                                    send.send_control_message(&DestinationMessage::DestinationDone).await?;
                                    return Ok(());
                                }
                                _ => {}
                            }
                        }
                    }
                }
            };
            let observe = async {
                cursors.recv().await.context("first leaf never opened")?;
                starts.recv().await.context("first payload never started")?;
                ended.recv().await.context("first leaf never sealed")?;
                for _ in 1..8 {
                    assert!(
                        tokio::time::timeout(std::time::Duration::from_secs(1), cursors.recv())
                            .await
                            .is_ok_and(|cursor| cursor.is_some()),
                        "reserved siblings must progress while prior file parents and receiver releases remain held"
                    );
                }
                if disconnect {
                    close_tx.send(())?;
                } else {
                    gate.add_permits(1);
                }
                anyhow::Ok(())
            };
            let (source, peer, observe) = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                DIRECTORY_LIMITS.scope(
                    normal_directory_limit(1),
                    HOOKS.scope(
                        hooks,
                        common::task_scope::scope_tasks(async {
                            tokio::join!(source, peer, observe)
                        }),
                    ),
                ),
            )
            .await?;
            peer?;
            observe?;
            if disconnect {
                assert!(
                    format!("{:#}", source.unwrap_err()).contains("destination closed control")
                );
            } else {
                source?;
                assert!(!errors.has_errors());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn control_disconnect_cancels_exhausted_reserve_sibling_wait() -> anyhow::Result<()> {
        struct WaitSignal(tokio::sync::mpsc::UnboundedSender<()>);
        impl tracing::Subscriber for WaitSignal {
            fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
                metadata.target() == "rcp::timing"
            }
            fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::Id {
                if attributes.metadata().name() == "source.directory.wait_release" {
                    let _ = self.0.send(());
                }
                tracing::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {}
            fn enter(&self, _: &tracing::Id) {}
            fn exit(&self, _: &tracing::Id) {}
        }
        let temp = tempfile::tempdir()?;
        let reserved = temp.path().join("reserved");
        for leaf in ["first", "second"] {
            std::fs::create_dir_all(reserved.join(leaf))?;
        }
        let (blocked, mut waiting) = tokio::sync::mpsc::unbounded_channel();
        // this current-thread test keeps the subscriber installed while spawned discovery polls.
        let _subscriber = tracing::subscriber::set_default(WaitSignal(blocked));
        let hooks = Hooks::new(|_, _, _| Box::pin(async { Ok(()) }));
        let (source, mut send, mut recv, errors) = connection(temp.path(), false, 1, 1, Vec::new());
        let (ended_tx, mut ended) = tokio::sync::mpsc::unbounded_channel();
        let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let peer = async {
            let mut leaf_begins = 0;
            let mut leaf_ends = 0;
            loop {
                tokio::select! {
                    _ = close_rx.recv() => {
                        send.close().await?;
                        return anyhow::Ok((leaf_begins, leaf_ends));
                    }
                    message = recv.recv_object::<SourceMessage>() => {
                        match message?.context("source closed before the requested disconnect")? {
                            SourceMessage::DirectoryBegin { src, dst, .. } => {
                                if src.parent() == Some(reserved.as_path()) {
                                    leaf_begins += 1;
                                }
                                send.send_control_message(&DestinationMessage::DirectoryReady { src, dst }).await?;
                            }
                            SourceMessage::DirectoryEnd { src, .. } => {
                                anyhow::ensure!(src.parent() == Some(reserved.as_path()), "an ancestor ended before its blocked sibling");
                                leaf_ends += 1;
                                ended_tx.send(())?;
                                // withhold release: this ended sibling owns the only spare reserve credit.
                            }
                            other => anyhow::bail!("unexpected message while reserve is exhausted: {other:?}"),
                        }
                    }
                }
            }
        };
        let disconnect = async {
            ended
                .recv()
                .await
                .context("first reserved leaf never ended")?;
            waiting
                .recv()
                .await
                .context("reserve sibling admission never waited")?;
            close_tx.send(())?;
            anyhow::Ok(())
        };
        let (source, peer, disconnected) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            DIRECTORY_LIMITS.scope(
                remote::protocol::DirectoryLimits {
                    normal: std::num::NonZeroUsize::MIN,
                    reserve: std::num::NonZeroUsize::new(2).unwrap(),
                },
                HOOKS.scope(
                    hooks.clone(),
                    common::task_scope::scope_tasks(async {
                        tokio::join!(source, peer, disconnect)
                    }),
                ),
            ),
        )
        .await
        .context("disconnect did not cancel exhausted reserve admission")?;
        disconnected?;
        assert_eq!(
            peer?,
            (1, 1),
            "another leaf crossed exhausted reserve admission"
        );
        let error = source.expect_err("peer disconnect must fail the blocked source");
        assert_eq!(
            error.root_cause().to_string(),
            "destination closed control before DestinationDone",
            "{error:#}"
        );
        assert!(!error.is::<resources::ReservedDepthExhausted>());
        assert!(
            !errors.has_errors(),
            "sibling backpressure was recorded as a copy error"
        );
        assert_eq!(
            hooks
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|(event, path, _)| {
                    event == "directory_admitted" && path.parent() == Some(reserved.as_path())
                })
                .count(),
            1,
            "a second reserved sibling was admitted before cancellation",
        );
        Ok(())
    }

    #[tokio::test]
    async fn two_payloads_start_before_wide_directory_eof_and_every_name_is_classified_once()
    -> anyhow::Result<()> {
        for dereference in [false, true] {
            let temp = tempfile::tempdir()?;
            for index in 0..150 {
                std::fs::write(temp.path().join(format!("file{index:03}")), b"x")?;
            }
            let hooks = Hooks::new(|_, _, _| Box::pin(async { Ok(()) }));
            let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let writers = (0..2)
                .map(|_| {
                    Box::new(GatedWriter::new(started.clone(), gate.clone()))
                        as remote::streams::BoxedWrite
                })
                .collect();
            let (source, send, recv, _) = connection(temp.path(), dereference, 2, 2, writers);
            let observe = async {
                starts.recv().await.context("first sender did not start")?;
                starts.recv().await.context("second sender did not start")?;
                {
                    let events = hooks.events.lock().unwrap();
                    let batches: Vec<_> = events
                        .iter()
                        .filter(|(event, _, _)| event == "batch")
                        .map(|(_, _, count)| *count)
                        .collect();
                    assert_eq!(
                        batches,
                        vec![64],
                        "both headers must start while the first batch still applies file backpressure"
                    );
                }
                gate.add_permits(2);
                anyhow::Ok(())
            };
            let (source, peer, observe) = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                HOOKS.scope(
                    hooks.clone(),
                    common::task_scope::scope_tasks(async {
                        tokio::join!(source, acknowledge_directories(send, recv), observe)
                    }),
                ),
            )
            .await?;
            source?;
            observe?;
            let messages = peer?;
            assert!(messages.iter().any(|m| matches!(
                m,
                SourceMessage::DirectoryEnd {
                    entry_count: 150,
                    ..
                }
            )));
            let events = hooks.events.lock().unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|(event, _, _)| event == "cursor")
                    .count(),
                1
            );
            let classified: Vec<_> = events
                .iter()
                .filter(|(event, _, _)| event == "classified")
                .map(|(_, path, _)| path)
                .collect();
            assert_eq!(classified.len(), 150);
            assert_eq!(
                classified
                    .into_iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                150
            );
        }
        Ok(())
    }
    #[tokio::test]
    async fn delayed_manifest_ready_unblocks_capacity_one_without_opening_unchanged_file()
    -> anyhow::Result<()> {
        for dereference in [false, true] {
            let temp = tempfile::tempdir()?;
            let file = temp.path().join("file");
            std::fs::write(&file, b"same")?;
            let metadata = Metadata::from(&std::fs::metadata(&file)?);
            let mut configuration = settings(dereference);
            configuration.ignore_existing = true;
            let (source, mut send, mut recv, _) = configured_connection(
                temp.path(),
                configuration,
                1,
                1,
                vec![Box::new(FailingWriter)],
            );
            let peer = async {
                let mut pair = None;
                let mut complete = false;
                let mut unchanged = false;
                while !complete || !unchanged {
                    match recv
                        .recv_object::<SourceMessage>()
                        .await?
                        .context("source ended")?
                    {
                        SourceMessage::DirectoryBegin { src, dst, .. } => {
                            pair = Some(SrcDst { src, dst });
                        }
                        SourceMessage::DirectoryEnd { entry_count, .. } => {
                            assert_eq!(entry_count, 1);
                            let pair = pair.as_ref().unwrap();
                            send.send_control_message(
                                &DestinationMessage::DirectoryManifestChunk {
                                    dst: pair.dst.clone(),
                                    entries: vec![ExistingEntry {
                                        name: "file".into(),
                                        metadata: metadata.clone(),
                                        size: 4,
                                        is_file: true,
                                    }],
                                },
                            )
                            .await?;
                            send.send_control_message(&DestinationMessage::DirectoryReady {
                                src: pair.src.clone(),
                                dst: pair.dst.clone(),
                            })
                            .await?;
                        }
                        SourceMessage::FileUnchanged { .. } => {
                            unchanged = true;
                            let pair = pair.as_ref().unwrap();
                            send.send_control_message(&DestinationMessage::DirectoryReleased {
                                src: pair.src.clone(),
                                dst: pair.dst.clone(),
                            })
                            .await?;
                        }
                        SourceMessage::DiscoveryComplete { .. } => {
                            complete = true;
                        }
                        other => anyhow::bail!("unexpected message: {other:?}"),
                    }
                }
                send.send_control_message(&DestinationMessage::DestinationDone)
                    .await?;
                anyhow::Ok(())
            };
            let (source, peer) = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                common::task_scope::scope_tasks(async { tokio::join!(source, peer) }),
            )
            .await?;
            source?;
            peer?;
        }
        Ok(())
    }
    #[tokio::test]
    async fn preheader_filesystem_error_survives_failed_skip_send() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let parent = Arc::new(Dir::open_root_dir(temp.path(), false, common::Side::Source).await?);
        let (send, receive) = async_channel::bounded(1);
        send.try_send(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        ))
        .unwrap();
        let pool = Arc::new(super::super::AcceptingSendStreamPool {
            recv: receive,
            return_tx: send,
        });
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let fatal = Arc::new(Fatal::new(Default::default()));
        let missing = temp.path().join("missing");
        let guard = Obligation::new(fatal.clone(), missing.clone());
        let configuration = settings(false);
        let result = obligated(
            guard,
            super::super::send_file_tcp(
                &configuration,
                Default::default(),
                &missing,
                Path::new("/destination/missing"),
                1,
                false,
                pool,
                &errors,
                Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                    Box::new(FailingWriter) as remote::streams::BoxedWrite,
                ))),
                super::super::FileRead::Hardened(parent, "missing".into()),
                &fatal,
            ),
        )
        .await;
        assert!(result.is_err());
        let error = fatal.take().unwrap();
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::NotFound,
            "{error:#}"
        );
        Ok(())
    }
    #[tokio::test]
    async fn fail_early_sibling_classification_wakes_suspended_inline_directory()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        std::fs::create_dir(temp.path().join("branch"))?;
        std::fs::write(temp.path().join("failure"), b"x")?;
        let inline_started = Arc::new(tokio::sync::Semaphore::new(0));
        let never = Arc::new(tokio::sync::Semaphore::new(0));
        let hooks = Hooks::new({
            let inline_started = inline_started.clone();
            let never = never.clone();
            move |event, path, _| {
                let name = path.file_name().unwrap().to_string_lossy();
                let fails = event == "classifying" && name == "failure";
                let inline = event == "cursor" && name == "branch";
                let inline_started = inline_started.clone();
                let never = never.clone();
                Box::pin(async move {
                    if inline {
                        inline_started.add_permits(1);
                        never.acquire_owned().await.unwrap().forget();
                    }
                    if fails {
                        inline_started.acquire_owned().await.unwrap().forget();
                        anyhow::bail!("original sibling classifier failure");
                    }
                    Ok(())
                })
            }
        });
        let mut configuration = settings(false);
        configuration.fail_early = true;
        // P=2 permits the intentionally delayed sibling stat to remain independently polled
        let (source, send, recv, _) =
            configured_connection(temp.path(), configuration, 1, 2, Vec::new());
        let (source, _peer) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HOOKS.scope(
                hooks,
                common::task_scope::scope_tasks(async {
                    tokio::join!(source, acknowledge_directories(send, recv))
                }),
            ),
        )
        .await?;
        let error = source.unwrap_err();
        assert!(
            format!("{error:#}").contains("original sibling classifier failure"),
            "{error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_payload_never_returns_a_partial_frame_to_the_pool() -> anyhow::Result<()> {
        struct PanicWriter;
        impl tokio::io::AsyncWrite for PanicWriter {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                _: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                panic!("original writer panic");
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
                std::task::Poll::Pending
            }
        }
        let temp = tempfile::tempdir()?;
        std::fs::write(temp.path().join("file"), b"payload")?;
        let parent = Arc::new(Dir::open_root_dir(temp.path(), false, common::Side::Source).await?);
        let (send, receive) = async_channel::bounded(1);
        send.try_send(remote::streams::SendStream::new(
            Box::new(PanicWriter) as remote::streams::BoxedWrite
        ))
        .unwrap();
        let pool = Arc::new(super::super::AcceptingSendStreamPool {
            recv: receive,
            return_tx: send,
        });
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let fatal = Arc::new(Fatal::new(Default::default()));
        let path = temp.path().join("file");
        let guard = Obligation::new(fatal.clone(), path.clone());
        let configuration = settings(false);
        let result = obligated(
            guard,
            super::super::send_file_tcp(
                &configuration,
                Default::default(),
                &path,
                Path::new("/destination/file"),
                7,
                false,
                pool.clone(),
                &errors,
                Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                    Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
                ))),
                super::super::FileRead::Hardened(parent, "file".into()),
                &fatal,
            ),
        )
        .await;
        assert!(result.is_err());
        assert!(
            fatal
                .take()
                .unwrap()
                .to_string()
                .contains("original writer panic")
        );
        assert_eq!(
            pool.recv.len(),
            0,
            "partial frame was returned before fatal publication"
        );
        Ok(())
    }
    #[tokio::test]
    async fn rejection_drains_admitted_classifier_before_releasing_capacity_one()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        std::fs::create_dir(temp.path().join("a"))?;
        std::fs::write(temp.path().join("a/blocked"), b"x")?;
        std::fs::create_dir_all(temp.path().join("b/leaf"))?;
        let started = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let b_begin = Arc::new(tokio::sync::Semaphore::new(0));
        let hooks = Hooks::new({
            let started = started.clone();
            let release = release.clone();
            let root = temp.path().to_owned();
            move |event, path, _| {
                let blocked = event == "classifying" && path == root.join("a/blocked");
                let second = event == "classified" && path == root.join("b");
                let started = started.clone();
                let release = release.clone();
                Box::pin(async move {
                    if blocked {
                        started.add_permits(2);
                        release.acquire_owned().await.unwrap().forget();
                    }
                    if second {
                        started.acquire_owned().await.unwrap().forget();
                    }
                    Ok(())
                })
            }
        });
        let (source, mut send, mut recv, _) = connection(temp.path(), false, 2, 1, Vec::new());
        let peer = async {
            let mut rejected_ends = 0;
            let mut lifetimes = PeerLifetimes::default();
            loop {
                match recv
                    .recv_object::<SourceMessage>()
                    .await?
                    .context("source ended")?
                {
                    SourceMessage::DirectoryBegin { src, dst, .. } => {
                        lifetimes.begin(&src, &dst);
                        lifetimes.ready(&dst);
                        let reject = src == temp.path().join("a");
                        if reject {
                            started.clone().acquire_owned().await.unwrap().forget();
                        }
                        if src == temp.path().join("b") {
                            b_begin.add_permits(1);
                        }
                        let message = if reject {
                            DestinationMessage::DirectorySkipped { src, dst }
                        } else {
                            DestinationMessage::DirectoryReady { src, dst }
                        };
                        send.send_control_message(&message).await?;
                    }
                    SourceMessage::DirectoryEnd {
                        src,
                        dst,
                        entry_count,
                    } => {
                        if src == temp.path().join("a") {
                            assert_eq!(entry_count, 0);
                            rejected_ends += 1;
                        }
                        lifetimes.end(&dst);
                        lifetimes.flush(&mut send).await?;
                    }
                    SourceMessage::DiscoveryComplete { .. } => {
                        send.send_control_message(&DestinationMessage::DestinationDone)
                            .await?;
                        break;
                    }
                    SourceMessage::FileSkipped { .. } => {
                        anyhow::bail!("rejected directory manufactured a file outcome")
                    }
                    _ => {}
                }
            }
            assert_eq!(rejected_ends, 1);
            anyhow::Ok(())
        };
        let observe =
            async {
                b_begin.clone().acquire_owned().await.unwrap().forget();
                // b's Begin proves the dispatcher consumed a's Skipped and returned its separate
                // Begin credit, while a's admitted classifier still owns the only classification P
                let events = hooks.events.lock().unwrap();
                assert!(!events.iter().any(|(event, path, _)| event == "classifying"
                    && path == &temp.path().join("b/leaf")));
                assert!(!events.iter().any(|(event, path, _)| event == "classified"
                    && path == &temp.path().join("a/blocked")));
                drop(events);
                release.add_permits(1);
            };
        let (source, peer, ()) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            // isolate classification admission: both sibling directories need independent
            // resource groups while the first classifier deliberately waits for the second Begin.
            DIRECTORY_LIMITS.scope(
                normal_directory_limit(3),
                HOOKS.scope(
                    hooks.clone(),
                    common::task_scope::scope_tasks(async { tokio::join!(source, peer, observe) }),
                ),
            ),
        )
        .await?;
        source?;
        peer?;
        assert!(
            hooks
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|(event, path, _)| event == "classified"
                    && path == &temp.path().join("a/blocked")),
            "local rejection aborted the admitted classifier instead of draining it"
        );
        Ok(())
    }
    #[tokio::test]
    async fn capacity_one_classifier_finishes_while_ancestor_descends_inline() -> anyhow::Result<()>
    {
        for dereference in [false, true] {
            let temp = tempfile::tempdir()?;
            std::fs::create_dir_all(temp.path().join("a/leaf"))?;
            std::fs::create_dir_all(temp.path().join("b/leaf"))?;
            let entered = Arc::new(tokio::sync::Semaphore::new(0));
            let classifications = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let hooks = Hooks::new({
                let root = temp.path().to_owned();
                let entered = entered.clone();
                let classifications = classifications.clone();
                move |event, path, _| {
                    let direct = path.parent() == Some(root.as_path());
                    let second = event == "classifying"
                        && direct
                        && classifications.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1;
                    let inline = event == "cursor" && direct;
                    let entered = entered.clone();
                    Box::pin(async move {
                        if inline {
                            entered.add_permits(1);
                        }
                        if second {
                            entered.acquire_owned().await.unwrap().forget();
                        }
                        Ok(())
                    })
                }
            });
            let (source, send, recv, _) = connection(temp.path(), dereference, 1, 1, Vec::new());
            let (source, peer) = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                HOOKS.scope(
                    hooks,
                    common::task_scope::scope_tasks(async {
                        tokio::join!(source, acknowledge_directories(send, recv))
                    }),
                ),
            )
            .await?;
            source?;
            assert_eq!(
                peer?
                    .iter()
                    .filter(|message| matches!(message, SourceMessage::DirectoryEnd { .. }))
                    .count(),
                5
            );
            assert_eq!(classifications.load(std::sync::atomic::Ordering::SeqCst), 2);
        }
        Ok(())
    }
    #[tokio::test]
    async fn directory_metadata_failure_skips_counted_child_but_aborts_root() -> anyhow::Result<()>
    {
        for root_failure in [false, true] {
            let temp = tempfile::tempdir()?;
            std::fs::create_dir(temp.path().join("bad"))?;
            std::fs::create_dir(temp.path().join("good"))?;
            let failed = if root_failure {
                temp.path().to_owned()
            } else {
                temp.path().join("bad")
            };
            let hooks = Hooks::new(move |event, path, _| {
                let fails = event == "metadata" && path == failed;
                Box::pin(async move {
                    anyhow::ensure!(!fails, "original directory metadata failure");
                    Ok(())
                })
            });
            let (source, send, recv, errors) = connection(temp.path(), false, 1, 1, Vec::new());
            let (source, peer) = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                HOOKS.scope(
                    hooks,
                    common::task_scope::scope_tasks(async {
                        tokio::join!(source, acknowledge_directories(send, recv))
                    }),
                ),
            )
            .await?;
            if root_failure {
                assert!(
                    format!("{:#}", source.unwrap_err())
                        .contains("original directory metadata failure")
                );
                assert!(peer.is_err());
            } else {
                source?;
                let messages = peer?;
                assert!(
                    format!("{:#}", errors.take_error().unwrap())
                        .contains("original directory metadata failure")
                );
                assert_eq!(messages.iter().filter(|message| matches!(message, SourceMessage::FileSkipped { src, .. } if src == &temp.path().join("bad"))).count(), 1);
                assert!(!messages.iter().any(|message| matches!(message, SourceMessage::DirectoryBegin { src, .. } if src == &temp.path().join("bad"))));
                assert!(messages.iter().any(|message| matches!(message, SourceMessage::DirectoryEnd { src, entry_count: 2, .. } if src == temp.path())));
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn enumeration_error_seals_only_admitted_first_batch_and_drains_files()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        for index in 0..100 {
            std::fs::write(temp.path().join(format!("file{index:03}")), b"x")?;
        }
        let batches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hooks = Hooks::new({
            let batches = batches.clone();
            move |event, _, _| {
                let fail = event == "read_batch"
                    && batches.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1;
                Box::pin(async move {
                    anyhow::ensure!(!fail, "original directory enumeration failure");
                    Ok(())
                })
            }
        });
        let (source, send, recv, errors) =
            connection(temp.path(), false, 1, 1, vec![Box::new(tokio::io::sink())]);
        let (source, peer) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HOOKS.scope(
                hooks,
                common::task_scope::scope_tasks(async {
                    tokio::join!(source, acknowledge_directories(send, recv))
                }),
            ),
        )
        .await?;
        source?;
        assert!(peer?.iter().any(|message| matches!(
            message,
            SourceMessage::DirectoryEnd {
                entry_count: 64,
                ..
            }
        )));
        assert!(
            format!("{:#}", errors.take_error().unwrap())
                .contains("original directory enumeration failure")
        );
        assert_eq!(batches.load(std::sync::atomic::Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn capacity_one_deep_dereference_tree_does_not_overflow_the_poll_stack()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let mut deep = temp.path().to_owned();
        for _ in 0..256 {
            deep.push("d");
            std::fs::create_dir(&deep)?;
        }
        let (source, send, recv, _) = connection(temp.path(), true, 1, 1, Vec::new());
        let (source, peer) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            common::task_scope::scope_tasks(async {
                tokio::join!(source, acknowledge_directories(send, recv))
            }),
        )
        .await?;
        source?;
        assert_eq!(
            peer?
                .iter()
                .filter(|message| matches!(message, SourceMessage::DirectoryEnd { .. }))
                .count(),
            257
        );
        Ok(())
    }
    #[tokio::test]
    async fn destination_done_drains_late_payload_failure_without_cancelling_its_obligation()
    -> anyhow::Result<()> {
        struct LateFailure {
            header_written: bool,
            after_done: futures::future::BoxFuture<'static, ()>,
        }
        impl tokio::io::AsyncWrite for LateFailure {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                context: &mut std::task::Context<'_>,
                bytes: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                if !self.header_written {
                    self.header_written = true;
                    return std::task::Poll::Ready(Ok(bytes.len()));
                }
                if self.after_done.as_mut().poll(context).is_pending() {
                    return std::task::Poll::Pending;
                }
                std::task::Poll::Ready(Err(std::io::Error::other("original late payload failure")))
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
                std::task::Poll::Pending
            }
        }
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("file");
        std::fs::write(&path, b"payload")?;
        let done = Arc::new(tokio::sync::Semaphore::new(0));
        let hooks = Hooks::new({
            let done = done.clone();
            move |event, _, _| {
                if event == "destination_done" {
                    done.add_permits(1);
                }
                Box::pin(async { Ok(()) })
            }
        });
        let writer = LateFailure {
            header_written: false,
            after_done: Box::pin(async move {
                done.acquire_owned().await.unwrap().forget();
            }),
        };
        let (source, send, recv, _) = connection(&path, false, 1, 1, vec![Box::new(writer)]);
        let (source, peer) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HOOKS.scope(
                hooks.clone(),
                common::task_scope::scope_tasks(async {
                    tokio::join!(source, acknowledge_directories(send, recv))
                }),
            ),
        )
        .await?;
        peer?;
        assert!(
            hooks
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|(event, _, _)| event == "destination_done")
        );
        let error = source.unwrap_err();
        assert!(
            format!("{error:#}").contains("original late payload failure"),
            "{error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn ready_wait_timing_is_only_created_for_pending_readiness() {
        struct CountWaits(Arc<std::sync::atomic::AtomicUsize>);
        impl tracing::Subscriber for CountWaits {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                if attributes.metadata().name() == "source.directory.wait_ready" {
                    self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {}
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        tracing::instrument::WithSubscriber::with_subscriber(
            async {
                let ready = Readiness(
                    tokio::sync::watch::channel(ReadyState::Ready(Arc::new(HashMap::new()))).0,
                );
                assert!(matches!(ready.wait().await, ReadyState::Ready(_)));
                assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
                ready.0.send_replace(ReadyState::Pending);
                let wait = ready.wait();
                tokio::pin!(wait);
                assert!(futures::poll!(wait.as_mut()).is_pending());
                assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
                ready.0.send_replace(ReadyState::Rejected);
                assert!(matches!(wait.await, ReadyState::Rejected));
                assert!(matches!(ready.wait().await, ReadyState::Rejected));
                assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
            },
            CountWaits(count.clone()),
        )
        .await;
    }
    #[tokio::test]
    async fn control_reader_panic_preserves_original_cause() -> anyhow::Result<()> {
        struct PanicReader;
        impl tokio::io::AsyncRead for PanicReader {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                _: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                panic!("original framed reader panic");
            }
        }
        let temp = tempfile::tempdir()?;
        let (send, receive) = async_channel::bounded(1);
        let pool = Arc::new(super::super::AcceptingSendStreamPool {
            recv: receive,
            return_tx: send,
        });
        let source = run(
            settings(false),
            Default::default(),
            SrcDst {
                src: temp.path().to_owned(),
                dst: "/destination".into(),
            },
            Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
            ))),
            remote::streams::RecvStream::new(Box::new(PanicReader) as remote::streams::BoxedRead),
            pool,
            Arc::new(Fatal::new(Default::default())),
            1,
            1,
            common::EndpointAdmission::Disabled,
            common::FilesInFlightSource::Automatic,
            remote::protocol::DirectoryLimits {
                normal: std::num::NonZeroUsize::new(2).unwrap(),
                reserve: std::num::NonZeroUsize::MIN,
            },
            Default::default(),
        );
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            common::task_scope::scope_tasks(source),
        )
        .await?
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("original framed reader panic"),
            "{error:#}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn delayed_ready_does_not_serialize_directory_heavy_discovery_with_fd_headroom()
    -> anyhow::Result<()> {
        use futures::StreamExt as _;
        static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
            std::sync::LazyLock::new(common::progress::Progress::new);
        let fixture = tempfile::tempdir()?;
        let frontier = fixture.path().join("frontier");
        let mut metadata = HashMap::new();
        for index in 0..256 {
            let leaf = frontier.join(format!("leaf-{index}"));
            std::fs::create_dir_all(&leaf)?;
            common::filegen::write_file(&PROGRESS, leaf.join("file"), 1, 1, 0)
                .await
                .map_err(|error| error.source)?;
            metadata.insert(
                leaf.clone(),
                Metadata::from(&std::fs::metadata(leaf.join("file"))?),
            );
        }
        for streams in [4, 8] {
            for delay_ms in [0, 2] {
                let pending = streams * 4;
                let hooks = Hooks::new(|event, _, sequential| {
                    let reserved = event == "directory_admitted" && sequential != 0;
                    Box::pin(async move {
                        anyhow::ensure!(
                            !reserved,
                            "entered sequential reserve with descriptor headroom"
                        );
                        Ok(())
                    })
                });
                let mut configuration = settings(false);
                configuration.overwrite = true;
                let (source, mut send, mut receive, errors) = configured_connection(
                    fixture.path(),
                    configuration,
                    streams,
                    pending,
                    Vec::new(),
                );
                let peer = async {
                    let mut initial = Vec::new();
                    let mut first_batch = true;
                    let mut replies = futures::stream::FuturesUnordered::<
                        futures::future::BoxFuture<'static, (PathBuf, PathBuf)>,
                    >::new();
                    let mut begins = 0;
                    let mut unchanged = 0;
                    let mut discovered = false;
                    let mut lifetimes = PeerLifetimes::default();
                    let delayed = |pair: (PathBuf, PathBuf)| async move {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        pair
                    };
                    while !discovered || unchanged != 256 || !replies.is_empty() {
                        tokio::select! {
                            Some((src, dst)) = replies.next(), if !replies.is_empty() => {
                                send.send_control_message(&DestinationMessage::DirectoryManifestChunk {
                                    dst: dst.clone(),
                                    entries: vec![ExistingEntry {
                                        name: "file".into(), is_file: true, size: 1,
                                        metadata: metadata[&src].clone(),
                                    }],
                                }).await?;
                                send.send_control_message(&DestinationMessage::DirectoryReady { src: src.clone(), dst: dst.clone() }).await?;
                                lifetimes.ready(&dst);
                                lifetimes.flush(&mut send).await?;
                            }
                            message = receive.recv_object::<SourceMessage>() => {
                                match message?.context("source closed before all unchanged outcomes")? {
                                    SourceMessage::DirectoryBegin { src, dst, .. } => {
                                        lifetimes.begin(&src, &dst);
                                        begins += 1;
                                        if !metadata.contains_key(&src) {
                                            lifetimes.ready(&dst);
                                            send.send_control_message(&DestinationMessage::DirectoryReady { src, dst }).await?;
                                        } else if first_batch {
                                            // retain P distinct file parents before any Ready can
                                            // release one; scans still have real descriptor headroom.
                                            initial.push((src, dst));
                                            if initial.len() == pending {
                                                first_batch = false;
                                                for pair in initial.drain(..) { replies.push(Box::pin(delayed(pair))); }
                                            }
                                        } else {
                                            replies.push(Box::pin(delayed((src, dst))));
                                        }
                                    }
                                    SourceMessage::FileUnchanged { .. } => unchanged += 1,
                                    SourceMessage::DirectoryEnd { dst, .. } => {
                                        lifetimes.end(&dst);
                                        lifetimes.flush(&mut send).await?;
                                    },
                                    SourceMessage::DiscoveryComplete { .. } => discovered = true,
                                    other => anyhow::bail!("unexpected source outcome: {other:?}"),
                                }
                            }
                        }
                    }
                    assert_eq!(begins, 258);
                    send.send_control_message(&DestinationMessage::DestinationDone)
                        .await?;
                    anyhow::Ok(())
                };
                let (source, peer) = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    HOOKS.scope(
                        hooks,
                        common::task_scope::scope_tasks(async { tokio::join!(source, peer) }),
                    ),
                )
                .await
                .with_context(|| format!("E={streams}, Ready delay={delay_ms}ms"))?;
                source?;
                peer?;
                assert!(!errors.has_errors());
            }
        }
        Ok(())
    }
    mod coalescing_tests;
}
