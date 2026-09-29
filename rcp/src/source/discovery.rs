//! Single-pass source discovery and directory readiness accounting.

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

struct Registry {
    pending: std::sync::Mutex<HashMap<PathBuf, Pending>>,
    credit: Arc<tokio::sync::Semaphore>,
}

impl Registry {
    fn new(capacity: usize) -> Self {
        Self {
            pending: Default::default(),
            credit: Arc::new(tokio::sync::Semaphore::new(capacity)),
        }
    }
    async fn register(&self, pair: &SrcDst) -> anyhow::Result<Arc<Readiness>> {
        let credit = common::timing_scope!("source.discovery.wait_credit")
            .measure(self.credit.clone().acquire_owned())
            .await?;
        let readiness = Arc::new(Readiness(
            tokio::sync::watch::channel(ReadyState::Pending).0,
        ));
        let mut pending = self.pending.lock().unwrap();
        anyhow::ensure!(
            !pending.contains_key(&pair.dst),
            "duplicate source directory Begin: {:?}",
            pair.dst
        );
        pending.insert(
            pair.dst.clone(),
            Pending {
                src: pair.src.clone(),
                readiness: readiness.clone(),
                manifest: HashMap::new(),
                _credit: credit,
            },
        );
        Ok(readiness)
    }
    fn manifest(&self, dst: &Path, entries: Vec<ExistingEntry>) -> anyhow::Result<()> {
        let mut pending = self.pending.lock().unwrap();
        let record = pending
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
        let mut pending = self.pending.lock().unwrap();
        let record = pending.get(dst).with_context(|| {
            format!("unknown or duplicate directory acknowledgement: {src:?} -> {dst:?}")
        })?;
        anyhow::ensure!(
            record.src == src,
            "directory acknowledgement source mismatch: {src:?} -> {dst:?}"
        );
        let record = pending.remove(dst).unwrap();
        let state = if rejected {
            ReadyState::Rejected
        } else {
            ReadyState::Ready(Arc::new(record.manifest))
        };
        record.readiness.0.send_replace(state);
        Ok(())
    }
    fn is_empty(&self) -> bool {
        self.pending.lock().unwrap().is_empty()
    }
}

pub(super) struct Fatal {
    error: std::sync::Mutex<Option<anyhow::Error>>,
    cancel: tokio_util::sync::CancellationToken,
    pool_shutdown: super::PoolShutdownToken,
    gates: std::sync::Mutex<Vec<Arc<tokio::sync::Semaphore>>>,
    errors: Arc<common::error_collector::ErrorCollector>,
}

impl Fatal {
    pub(super) fn new(
        pool_shutdown: super::PoolShutdownToken,
        errors: Arc<common::error_collector::ErrorCollector>,
    ) -> Self {
        Self {
            error: Default::default(),
            cancel: Default::default(),
            pool_shutdown,
            gates: Default::default(),
            errors,
        }
    }
    fn publish(&self, error: anyhow::Error) {
        let mut primary = self.error.lock().unwrap();
        if primary.is_none() {
            *primary = Some(self.errors.take_error().unwrap_or(error));
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
}

async fn classify(
    parent: &Parent,
    pair: SrcDst,
    name: std::ffi::OsString,
    notice: Option<common::safedir::RootAclNotice>,
) -> anyhow::Result<Observation> {
    let (kind, size, metadata, target) = match parent {
        Parent::Hardened(dir) => {
            let handle = dir
                .child(&name)
                .await
                .with_context(|| format!("failed reading metadata from {:?}", pair.src))?;
            if let Some(notice) = notice {
                common::safedir::warn_if_root_acl_unpreserved(&handle, &pair.src, notice).await;
            }
            let kind = handle.kind();
            let size = handle.meta().size();
            let (metadata, target) = if kind == EntryKind::Symlink {
                let (target, meta) = handle.read_symlink(dir.side()).await?;
                (Metadata::from(&meta), Some(target))
            } else {
                (Metadata::from(handle.meta()), None)
            };
            (kind, size, metadata, target)
        }
        Parent::Path => {
            let meta = common::walk::run_metadata_probed(
                common::Side::Source,
                common::MetadataOp::Stat,
                tokio::fs::metadata(&pair.src),
            )
            .await?;
            (
                EntryKind::from_metadata(&meta),
                meta.len(),
                Metadata::from(&meta),
                None,
            )
        }
    };
    Ok(Observation {
        pair,
        name,
        kind,
        size,
        metadata,
        target,
    })
}

enum Cursor {
    Hardened(common::safedir::DirectoryCursor),
    Path(tokio::fs::ReadDir),
}

impl Cursor {
    async fn next_batch(&mut self) -> anyhow::Result<Vec<std::ffi::OsString>> {
        match self {
            Self::Hardened(cursor) => Ok(cursor
                .next_batch(std::num::NonZeroUsize::new(64).unwrap())
                .await?
                .into_iter()
                .map(|entry| entry.name)
                .collect()),
            Self::Path(cursor) => {
                throttle::get_ops_token().await;
                let mut batch = Vec::with_capacity(64);
                while batch.len() < 64 {
                    match cursor.next_entry().await? {
                        Some(entry) => batch.push(entry.file_name()),
                        None => break,
                    }
                }
                Ok(batch)
            }
        }
    }
}

struct FileJob {
    observation: Observation,
    parent: Parent,
    readiness: Option<Arc<Readiness>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    obligation: Obligation,
    is_root: bool,
}

struct DiscoveryContext {
    settings: common::copy::Settings,
    capture: remote::protocol::ExtendedMetadataCapture,
    root: PathBuf,
    control: remote::streams::BoxedSharedSendStream,
    registry: Registry,
    branches: Arc<tokio::sync::Semaphore>,
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
    ) -> anyhow::Result<Arc<Readiness>> {
        let readiness = self.registry.register(pair).await?;
        self.send(SourceMessage::DirectoryBegin {
            src: pair.src.clone(),
            dst: pair.dst.clone(),
            metadata,
            is_root,
            keep_if_empty,
        })
        .await?;
        Ok(readiness)
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
        self.recover(error)?;
        self.send(SourceMessage::FileSkipped {
            src: pair.src.clone(),
            dst: pair.dst.clone(),
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
                obligation,
                is_root,
            })
            .await
            .map_err(|_| anyhow::anyhow!("file dispatcher closed"))
    }
}

async fn file_job(
    context: Arc<DiscoveryContext>,
    pool: Arc<super::AcceptingSendStreamPool>,
    job: FileJob,
) -> anyhow::Result<()> {
    let FileJob {
        observation,
        parent,
        readiness,
        _permit,
        obligation,
        is_root,
    } = job;
    obligated(obligation, async {
        if let Some(readiness) = readiness {
            let state = readiness.wait().await;
            match state {
                ReadyState::Rejected => return Ok(()),
                ReadyState::Pending => unreachable!(),
                ReadyState::Ready(manifest) => {
                    if let Some(existing) = manifest.get(Path::new(&observation.name)) {
                        let source = remote::protocol::FileMetadata {
                            metadata: &observation.metadata,
                            size: observation.size,
                        };
                        let destination = remote::protocol::FileMetadata {
                            metadata: &existing.metadata,
                            size: existing.size,
                        };
                        if common::copy::skip_unchanged_send(
                            &context.settings.overwrite_compare,
                            context.settings.overwrite_filter,
                            context.settings.ignore_existing,
                            &source,
                            Some(common::copy::ExistingDst {
                                meta: &destination,
                                is_file: existing.is_file,
                            }),
                        ) {
                            tracing::info!("destination already has identical file, skipping transfer (manifest): {:?} -> {:?}", observation.pair.src, observation.pair.dst);
                            return context
                                .send(SourceMessage::FileUnchanged {
                                    src: observation.pair.src,
                                    dst: observation.pair.dst,
                                })
                                .await;
                        }
                    }
                }
            }
        }
        let read = match parent {
            Parent::Hardened(dir) => super::FileRead::Hardened(dir, observation.name),
            Parent::Path => super::FileRead::Path,
        };
        super::send_file_tcp(
            &context.settings,
            context.capture,
            &observation.pair.src,
            &observation.pair.dst,
            observation.size,
            is_root,
            pool,
            &context.errors,
            context.control.clone(),
            read,
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
) -> anyhow::Result<()> {
    let mut children = tokio::task::JoinSet::new();
    let mut classifications = tokio::task::JoinSet::new();
    supervise(
        &context.fatal,
        directory_body(
            context.clone(),
            parent,
            observation,
            is_root,
            &mut children,
            &mut classifications,
        ),
    )
    .await
}

async fn directory_body(
    context: Arc<DiscoveryContext>,
    parent: Parent,
    observation: Observation,
    is_root: bool,
    children: &mut tokio::task::JoinSet<anyhow::Result<()>>,
    classifications: &mut tokio::task::JoinSet<anyhow::Result<Option<Observation>>>,
) -> anyhow::Result<()> {
    let scan = common::timing_scope!("source.directory.scan");
    let pair = observation.pair;
    // opening is separate from metadata capture: only the former permits the legacy unreadable
    // root/-L tombstone with classification metadata and unknown ACLs
    let opened = match parent {
        Parent::Hardened(parent) => parent
            .open_dir(&observation.name)
            .await
            .map(|dir| Parent::Hardened(Arc::new(dir))),
        Parent::Path => Ok(Parent::Path),
    };
    let opened = match opened {
        Ok(opened) => opened,
        Err(error) if is_root => {
            context.recover(error.into())?;
            context
                .begin(&pair, observation.metadata, is_root, true)
                .await?;
            context.end(&pair, 0).await?;
            scan.finish();
            return Ok(());
        }
        Err(error) => return context.failed_child(&pair, error.into()).await,
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
                (metadata, dir.entries().await.map(Cursor::Hardened))
            }
            Parent::Path => {
                throttle::get_ops_token().await;
                let cursor = tokio::fs::read_dir(&pair.src).await.map(Cursor::Path);
                if cursor.is_err() {
                    return anyhow::Ok((observation.metadata.clone(), cursor));
                }
                let metadata = if context.capture.dir_acl {
                    observation
                        .metadata
                        .clone()
                        .with_acls(&super::read_dir_acls_by_path(&pair.src).await?)
                } else {
                    observation.metadata.clone()
                };
                (metadata, cursor)
            }
        };
        anyhow::Ok((metadata, cursor))
    }
    .await;
    let (metadata, cursor) = match description {
        Ok(description) => description,
        Err(error) if !is_root => return context.failed_child(&pair, error).await,
        Err(error) => return Err(error),
    };
    let mut cursor = match cursor {
        Ok(cursor) => cursor,
        Err(error) => {
            context.recover(error.into())?;
            context.begin(&pair, metadata, is_root, true).await?;
            context.end(&pair, 0).await?;
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
        .begin(&pair, metadata, is_root, keep_if_empty)
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
            cursor.next_batch().await
        }
        .await
        {
            Ok(names) => names,
            Err(error) => {
                context.recover(error)?;
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
                    classify(&parent, child, name, None).await
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
        while let Some(result) = classifications.join_next().await {
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
            if !context.included(&observation, false) {
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
            count = count
                .checked_add(1)
                .context("source directory child count overflow")?;
            let obligation = Obligation::new(context.fatal.clone(), observation.pair.src.clone());
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
                    match context.branches.clone().try_acquire_owned() {
                        Ok(branch) => common::task_scope::spawn_tracked(children, async move {
                            let _branch = branch;
                            obligated(obligation, directory(context, parent, observation, false))
                                .await
                        }),
                        Err(tokio::sync::TryAcquireError::NoPermits) => {
                            // continue this same logical branch on a fresh task stack, immediately
                            // joining it before this ancestor dispatches anything else. No extra E
                            // permit is acquired, and deeply nested polls cannot overflow the stack
                            let mut continuation = tokio::task::JoinSet::new();
                            common::task_scope::spawn_tracked(&mut continuation, async move {
                                obligated(
                                    obligation,
                                    directory(context, parent, observation, false),
                                )
                                .await
                            });
                            continuation
                                .join_next()
                                .await
                                .context("inline directory continuation disappeared")???;
                        }
                        Err(error) => {
                            context.fatal.publish(error.into());
                            return Err(anyhow::anyhow!("branch admission closed"));
                        }
                    }
                }
                EntryKind::Special => unreachable!(),
            }
        }
    }
    drop(cursor);
    context.end(&pair, count).await?;
    scan.finish();
    while let Some(result) = children.join_next().await {
        result??;
    }
    Ok(())
}

async fn discover(context: Arc<DiscoveryContext>, root: SrcDst) -> anyhow::Result<bool> {
    let scope = common::timing_scope!("source.discovery");
    let _branch = context.branches.clone().acquire_owned().await?;
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
    let observation = classify(
        &parent,
        root,
        name,
        Some(common::safedir::RootAclNotice::from(context.capture)),
    )
    .await?;
    let included = context.included(&observation, true);
    let has_root_item = included && observation.kind != EntryKind::Special;
    if !included {
        observation.kind.inc_skipped(super::progress());
    } else {
        match observation.kind {
            EntryKind::Dir => directory(context.clone(), parent, observation, true).await?,
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
    errors: Arc<common::error_collector::ErrorCollector>,
) -> anyhow::Result<()> {
    let (file_tx, mut file_rx) = tokio::sync::mpsc::channel(pending);
    let context = Arc::new(DiscoveryContext {
        settings,
        capture,
        root: root.src.clone(),
        control,
        registry: Registry::new(pending),
        branches: Arc::new(tokio::sync::Semaphore::new(branches)),
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
    #[tokio::test]
    async fn acknowledgement_validates_both_paths_and_releases_credit_once() {
        let registry = Registry::new(1);
        let pair = remote::protocol::SrcDst {
            src: "src".into(),
            dst: "dst".into(),
        };
        let ready = registry.register(&pair).await.unwrap();
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
        let ready = registry.register(&pair).await.unwrap();
        registry.resolve(&pair.src, &pair.dst, true).unwrap();
        assert!(matches!(ready.wait().await, ReadyState::Rejected));
        assert!(registry.resolve(&pair.src, &pair.dst, true).is_err());
        assert_eq!(registry.credit.available_permits(), 1);
    }
    #[tokio::test]
    async fn lost_obligation_cancels_admission_without_replacing_primary_error() {
        let fatal = Arc::new(Fatal::new(
            tokio_util::sync::CancellationToken::new(),
            Default::default(),
        ));
        let guard = Obligation::new(fatal.clone(), "entry".into());
        fatal.publish(anyhow::anyhow!("original cause"));
        drop(guard);
        assert!(fatal.cancel.is_cancelled());
        assert_eq!(fatal.take().unwrap().to_string(), "original cause");
    }
    #[tokio::test]
    async fn cancelled_obligation_is_fatal() {
        let fatal = Arc::new(Fatal::new(
            tokio_util::sync::CancellationToken::new(),
            Default::default(),
        ));
        drop(Obligation::new(fatal.clone(), "entry".into()));
        assert!(fatal.take().unwrap().to_string().contains("obligation"));
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
            Arc::new(Fatal::new(Default::default(), errors.clone())),
            e,
            p,
            errors.clone(),
        );
        (
            Box::pin(future),
            remote::streams::SendStream::new(Box::new(destination_write)),
            remote::streams::RecvStream::new(Box::new(destination_read)),
            errors,
        )
    }
    async fn acknowledge_directories(
        mut send: remote::streams::BoxedSendStream,
        mut recv: remote::streams::BoxedRecvStream,
    ) -> anyhow::Result<Vec<SourceMessage>> {
        let mut messages = Vec::new();
        loop {
            let message = recv
                .recv_object::<SourceMessage>()
                .await?
                .context("source closed before discovery complete")?;
            match &message {
                SourceMessage::DirectoryBegin { src, dst, .. } => {
                    send.send_control_message(&DestinationMessage::DirectoryReady {
                        src: src.clone(),
                        dst: dst.clone(),
                    })
                    .await?
                }
                SourceMessage::DiscoveryComplete { .. } => {
                    messages.push(message);
                    send.send_control_message(&DestinationMessage::DestinationDone)
                        .await?;
                    return Ok(messages);
                }
                _ => {}
            }
            messages.push(message);
        }
    }
    #[tokio::test]
    async fn original_worker_error_and_panic_precede_obligation_drop() {
        for panic in [false, true] {
            let fatal = Arc::new(Fatal::new(Default::default(), Default::default()));
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
        let fatal = Arc::new(Fatal::new(Default::default(), errors.clone()));
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
        let fatal = Arc::new(Fatal::new(Default::default(), errors.clone()));
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
            loop {
                match recv
                    .recv_object::<SourceMessage>()
                    .await?
                    .context("source ended")?
                {
                    SourceMessage::DirectoryBegin { src, dst, .. } => {
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
                        src, entry_count, ..
                    } if src == temp.path().join("a") => {
                        assert_eq!(entry_count, 0);
                        rejected_ends += 1;
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
            HOOKS.scope(
                hooks.clone(),
                common::task_scope::scope_tasks(async { tokio::join!(source, peer, observe) }),
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
                    source
                        .unwrap_err()
                        .to_string()
                        .contains("original directory metadata failure")
                );
                assert!(peer.is_err());
            } else {
                source?;
                let messages = peer?;
                assert!(
                    errors
                        .take_error()
                        .unwrap()
                        .to_string()
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
            errors
                .take_error()
                .unwrap()
                .to_string()
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
            Arc::new(Fatal::new(Default::default(), Default::default())),
            1,
            1,
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
}
