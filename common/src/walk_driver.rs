//! Generic single-tree safe-walk driver.
//!
//! This module owns the recursive directory-walk *skeleton* that copy, chmod, and
//! rm previously each hand-coded. A tool supplies a [`WalkVisitor`]; the driver
//! drives the traversal:
//!
//! 1. gated `read_entries` on the open hardened directory,
//! 2. per child: derive explicit admission from its type hint,
//! 3. apply the filter against [`EntryCx::filter_path`]: ordinary visitors retain cheap hints and
//!    admit only a `DT_UNKNOWN` probe. An authoritative visitor may reject from a reliable hint when
//!    that omission is terminal for its current context; otherwise it admits, rechecks, and transfers
//!    the exact `AdmittedEntry`,
//! 4. admit each remaining possible leaf immediately before spawn. Join/fold via
//!    [`join_and_fold`] (NOT batched: see [`walk_dir_contents`] for why admission and spawn share one
//!    loop step),
//! 5. in each task: consume the transferred classification or classify via [`Dir::child`], then
//!    either checked
//!    [`WalkVisitor::visit_leaf`] (holding the permit) or — for a directory — **drop the permit and
//!    end its inner scope**, [`WalkVisitor::dir_pre`], recurse, [`WalkVisitor::dir_post`].
//!
//! ## The single invariant home
//!
//! The "drop the leaf permit before recursing into a directory" invariant — the
//! root cause of the hold-and-wait deadlock class (see [`walk::LeafPermit`]) —
//! lives in **exactly one place**: the directory branch shared by [`process_entry`] and
//! `process_admitted_entry`.
//! Leaves hold their permit across [`WalkVisitor::visit_leaf`]; the directory
//! branch `drop`s it before any further work. No visitor ever hand-drops a leaf
//! permit, so the invariant cannot silently migrate back to N parallel sites.
//!
//! ## Cancellation safety
//!
//! Spawned tasks must be `'static`, and an already-started `spawn_blocking` job is not cancellable,
//! so every per-entry context is **owned**: [`EntryCx`] clones `Arc<Dir>` plus
//! owned `OsString`/`PathBuf` rather than borrowing, exactly as the existing
//! per-tool walks do. A dropped surrounding future (timeout or Ctrl-C) therefore
//! can never leave a spawned task holding a dangling borrow. Fail-early traversal
//! aborts spawned siblings and its task scope waits for recursively owned async work. A blocking job
//! that has already started remains non-cancellable; its fd-admission lease outlives the cancelled
//! waiter without delaying the runtime's bounded shutdown policy.
//!
//! ## How copy maps onto this trait
//!
//! `copy` is the reference single-tree visitor (`rcp::copy::CopyVisitor`); its
//! mapping shaped this trait.
//! `CopyVisitor` holds the run-constant state (`dst_root`, `filter_base`,
//! `Settings`, `preserve::Settings`, the opened top-level destination parent — the
//! *source* root needs no field, since each entry's source path is its
//! [`EntryCx::real_path`]) and:
//!
//! - **`type Summary`** = `copy::Summary`.
//! - **`type DirContext`** = the *destination* parent for one level:
//!   `{ dst_dir: Option<Arc<Dir>>, is_fresh: bool }` (`None` dst = dry-run). This
//!   is how the single-tree driver carries copy's second tree — each child reads
//!   its destination parent from `parent_ctx` rather than the driver modeling two
//!   trees. [`WalkVisitor::root_dir_context`] returns the opened top-level
//!   destination (`Some(dst)`/`None`) with the initial `is_fresh`.
//! - **`type DirState`** = `{ dst_dir, dst_parent, dst_name, we_created, src_meta,
//!   is_root, base }` — what `dir_post` needs to apply directory metadata
//!   (`src_meta` is read from the opened enumeration `Dir`, pairing it with the copied contents),
//!   run empty-dir cleanup (`dst_parent.rmdir_at(dst_name)`), and `--delete`-prune,
//!   plus the `base` create/unchanged contribution it folds with the children.
//! - **`visit_leaf`** dispatches on `kind`: `File` → `copy_file_fd`, `Symlink` →
//!   `copy_symlink_fd`, `Special` → skip-or-error. The admitted `permit` remains held across every
//!   fd-bearing leaf-dispatch path, including recursive destination removal under overwrite.
//!   `--dereference` of a symlink-to-dir stays inside `visit_leaf`: it transfers
//!   the permit into the path-based target root walk (the one deliberately
//!   non-fd-relative path). That walk releases the permit after authoritative
//!   directory classification and before descent.
//! - **`dir_pre`** runs `resolve_dst_dir`: `DirResolution::Skip` →
//!   [`DirAction::Skip`] (`--ignore-existing` hit a non-dir); `Proceed{dir,..}` →
//!   [`DirAction::Descend`] whose `dir` is the *source* dir (opened via
//!   `src_parent.open_dir(name)`), `child_ctx` carries the resolved `dst_dir` +
//!   child `is_fresh`, and `state` carries the `DirState`.
//! - **`dir_post`** receives the children's folded `Result`: on `Ok` it runs the
//!   `--delete` prune (keep-set = `processed.names()`), empty-dir cleanup, and
//!   `set_dir_metadata_fd` (post-order); on `Err` (a non-fail-early child failure)
//!   it skips the destructive prune, still applies directory metadata, and returns
//!   the combined error — exactly as `copy_dir_contents`'s tail did.
//! - **`on_skip`** mirrors copy's inline filter-skip: `report_skip` in dry-run +
//!   `skipped_summary_for(kind)`.
//! - **`permit_kind`** = `OpenFile`; every entry not positively hinted as a
//!   directory is admitted before spawn. Roots and delegated entries acquire
//!   before authoritative classification. A positive directory hint may classify
//!   without admission; if stale, that first handle is closed before admission is
//!   awaited and classification is repeated. An authoritative directory releases
//!   any provisional permit before descent.
//!
//! The delegated-subtree case (rlink handing copy an update-only/type-changed
//! subtree rooted below the original filter root) is carried by seeding the root
//! [`EntryCx::filter_path`] with the subtree's logical base, so the filter still
//! matches at the entry's true path while `rel_path`/`real_path` stay relative to
//! the delegated root.
//!
//! The dry-run "directory" path (no destination dir, contents still traversed for
//! reporting) is just `DirContext.dst_dir == None` threaded through — the same
//! branch copy already has. No part of copy needs a trait shape this module does
//! not provide, which is why the trait stops here (no second-tree concept leaks
//! into the driver — that asymmetry is what keeps rlink on the substrate, not the
//! visitor; see docs/tocttou.md, "One shared traversal driver").

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::sync::Arc;

use async_recursion::async_recursion;

use crate::error::OperationError;

#[derive(Clone)]
pub(crate) struct TaskTracker {
    state: Arc<TaskTrackerState>,
}

struct TaskTrackerState {
    active: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
}

struct TrackedTask(TaskTracker);

pin_project_lite::pin_project! {
    /// One child future paired with the task-scope guard that accounts for it.
    ///
    /// Field order is load-bearing: cancelling even an unpolled Tokio task must destroy the child
    /// future and all of its captures before [`TrackedTask`] decrements the scope's active count.
    struct TrackedFuture<F> {
        #[pin]
        future: F,
        _tracked: TrackedTask,
    }
}

impl<F> std::future::Future for TrackedFuture<F>
where
    F: std::future::Future,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.project().future.poll(cx)
    }
}

impl TaskTracker {
    fn new() -> Self {
        Self {
            state: Arc::new(TaskTrackerState {
                active: std::sync::atomic::AtomicUsize::new(0),
                idle: tokio::sync::Notify::new(),
            }),
        }
    }

    fn enter(&self) -> TrackedTask {
        self.state
            .active
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        TrackedTask(self.clone())
    }

    async fn wait_idle(&self) {
        loop {
            let idle = self.state.idle.notified();
            if self.state.active.load(std::sync::atomic::Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }
}

impl Drop for TrackedTask {
    fn drop(&mut self) {
        if self
            .0
            .state
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            self.0.state.idle.notify_one();
        }
    }
}

tokio::task_local! {
    static TASK_TRACKER: TaskTracker;
}

/// Runs an operation in a task scope and waits for cancelled descendants to finish dropping.
pub(crate) async fn scope_tasks<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    if TASK_TRACKER.try_with(Clone::clone).is_ok() {
        return future.await;
    }
    let tracker = TaskTracker::new();
    let output = TASK_TRACKER.scope(tracker.clone(), future).await;
    tracker.wait_idle().await;
    output
}

/// Spawns work tracked by the current task scope.
pub(crate) fn spawn_tracked<T, F>(join_set: &mut tokio::task::JoinSet<T>, future: F)
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    let tracker = TASK_TRACKER
        .try_with(Clone::clone)
        .expect("tracked work must run inside a task scope");
    let tracked = tracker.enter();
    join_set.spawn(TASK_TRACKER.scope(
        tracker,
        TrackedFuture {
            future,
            _tracked: tracked,
        },
    ));
}

use crate::progress::Progress;
use crate::safedir::{Dir, Handle};
use crate::walk::{self, AdmittedEntry, AdmittedLeaf, EntryAdmission, EntryKind, PermitKind};

/// A per-run summary accumulated by the walk.
///
/// Every tool's `Summary` (copy/chmod/rm/link) already satisfies these bounds
/// (`Default + Add + Send + 'static`); requiring exactly them keeps the driver
/// generic over the tool without depending on any tool's concrete counters.
pub trait WalkSummary: Default + std::ops::Add<Output = Self> + Send + Sized + 'static {}

impl<T> WalkSummary for T where T: Default + std::ops::Add<Output = T> + Send + Sized + 'static {}

/// Owned per-entry context handed to every [`WalkVisitor`] method.
///
/// All fields are owned so the whole context can move into a spawned task (tasks
/// are `'static`). It carries the hardened parent [`Dir`] (an `Arc`, cloned, never
/// borrowed) and the entry's accumulated paths.
#[derive(Clone)]
pub struct EntryCx {
    /// The hardened directory that contains this entry. Cloned into each child
    /// task; every open below it is `O_NOFOLLOW`.
    pub parent: Arc<Dir>,
    /// This entry's name within `parent`.
    pub name: OsString,
    /// Path accumulated from the walk root to this entry (empty for the root
    /// entry). Joined onto the tool's root it reconstructs the real path; used for
    /// diagnostics and path reconstruction.
    pub rel_path: PathBuf,
    /// The path the driver feeds to the include/exclude filter for this entry: the
    /// entry's **logical** path relative to the filter root. Usually equals
    /// `rel_path`, but a tool processing a *delegated subtree* (rlink handing an
    /// update-only or type-changed subtree to copy, which is rooted below the
    /// original filter root) seeds the root entry's `filter_path` with that subtree's
    /// logical base so the filter still matches at the entry's true path (e.g.
    /// `cache/keep.txt`, not the bare `keep.txt` relative to the delegated root). The
    /// driver extends it by one component per level alongside `rel_path`.
    pub filter_path: PathBuf,
    /// `root.join(rel_path)` — the reconstructed real filesystem path, for
    /// diagnostics and the deliberately-path-based features (`-L`/`--delete`).
    pub real_path: PathBuf,
    /// Whether this is a dry run (no filesystem mutation).
    pub dry_run: bool,
    /// The process-global progress tracker.
    pub prog_track: &'static Progress,
}

impl EntryCx {
    /// Build the child context for `child_name` within `child_dir`, extending the
    /// accumulated `rel_path`/`filter_path`/`real_path` by one component. `child_dir`
    /// is the hardened directory the child lives in (for a directory entry's
    /// contents, the opened directory itself; for the root, the root directory).
    #[must_use]
    pub fn child(&self, child_dir: Arc<Dir>, child_name: &OsStr) -> EntryCx {
        EntryCx {
            parent: child_dir,
            name: child_name.to_owned(),
            rel_path: self.rel_path.join(child_name),
            filter_path: self.filter_path.join(child_name),
            real_path: self.real_path.join(child_name),
            dry_run: self.dry_run,
            prog_track: self.prog_track,
        }
    }
}

/// What a [`WalkVisitor::dir_pre`] decided to do with a directory entry.
///
/// Generic over the tool's summary `Sum` (carried by [`Self::Skip`]), the
/// per-directory inherited `Ctx` (carried by [`Self::Descend`] to this
/// directory's children), and the per-directory `State` (carried by
/// [`Self::Descend`] to [`WalkVisitor::dir_post`]).
pub enum DirAction<Sum, Ctx, State> {
    /// Do not descend; the whole subtree contributes `Sum` and nothing else
    /// (e.g. `--ignore-existing` hit a non-directory destination, or a
    /// filtered-out directory).
    Skip(Sum),
    /// Descend into `dir`; `child_ctx` is the inherited context handed to *this
    /// directory's children* (copy's destination dir + freshness; chmod/rm: `()`),
    /// and `state` is carried, in the same task, to [`WalkVisitor::dir_post`].
    Descend {
        /// The hardened directory whose contents to walk. For copy this is the
        /// *source* directory being read; the destination travels in `child_ctx`.
        dir: Arc<Dir>,
        /// The inherited context the driver clones into each child task and hands
        /// to the child's [`WalkVisitor::visit_leaf`] / [`WalkVisitor::dir_pre`].
        /// This is how copy threads the destination parent directory down one
        /// level without the driver knowing a second tree exists.
        child_ctx: Ctx,
        /// Tool state threaded from `dir_pre` to `dir_post` in the same task
        /// (copy's `we_created` + dst handle for metadata; rm's `RelaxedDirGuard` +
        /// snapshot; chmod's `()`).
        state: State,
    },
}

/// The names of the non-skipped children the driver actually spawned for a
/// directory, in enumeration order.
///
/// Handed to [`WalkVisitor::dir_post`] so a visitor can build a `--delete`
/// keep-set (the set of destination names with a source counterpart) without
/// re-reading the directory.
#[derive(Debug, Default)]
pub struct ProcessedChildren {
    names: Vec<OsString>,
}

impl ProcessedChildren {
    /// The spawned children's names, in enumeration order.
    #[must_use]
    pub fn names(&self) -> &[OsString] {
        &self.names
    }

    /// Move the names out (e.g. straight into a `--delete` keep-set).
    #[must_use]
    pub fn into_names(self) -> Vec<OsString> {
        self.names
    }
}

/// The `Result` a [`WalkVisitor::dir_pre`] produces: a [`DirAction`] over the
/// visitor's three associated types, or its [`OperationError`]. A named alias so
/// the trait method's signature stays readable.
pub type DirPreResult<V> = Result<
    DirAction<
        <V as WalkVisitor>::Summary,
        <V as WalkVisitor>::DirContext,
        <V as WalkVisitor>::DirState,
    >,
    OperationError<<V as WalkVisitor>::Summary>,
>;

/// A tool's policy for a single-tree safe walk.
///
/// The driver calls these to make the per-entry decisions it cannot know itself;
/// it owns everything else (enumeration, permit lifecycle, spawning, the
/// drop-before-recurse invariant, error fold). All futures are `+ Send` (RPITIT)
/// so the driver can spawn them; the visitor is shared as `Arc<V>`.
pub trait WalkVisitor: Send + Sync + 'static {
    /// Per-run summary type (the tool's `Summary`).
    type Summary: WalkSummary;
    /// Inherited per-directory context: what a directory's children need *from
    /// that directory* (an "inherited attribute" of the tree walk). The driver
    /// clones it into each child task and hands it to the child's
    /// [`Self::visit_leaf`] / [`Self::dir_pre`].
    ///
    /// This is the single-tree driver's bridge to copy's second (destination)
    /// tree: copy puts the open destination directory handle (plus its freshness)
    /// here, so each child can create/overwrite its own destination entry without
    /// the driver ever modeling a second tree. chmod and rm — which only ever
    /// need the source parent the driver already provides — use `()`.
    type DirContext: Clone + Send + Sync + 'static;
    /// State threaded from [`Self::dir_pre`] to [`Self::dir_post`] within one
    /// task (so it need not be `Send` across the per-child spawn boundary —
    /// `dir_pre`/recurse/`dir_post` all run in the same task).
    type DirState: Send;

    /// The inherited context for the walk *root's* children — the seed of the
    /// `DirContext` chain. copy returns its top-level destination directory here;
    /// chmod/rm return `()`. (The root entry itself is processed with this same
    /// context as its "parent" context.)
    fn root_dir_context(&self) -> Self::DirContext;

    /// Which backpressure pool a leaf permit comes from for this tool.
    fn permit_kind(&self) -> PermitKind;

    /// Whether the walk stops at the first error (`--fail-early`).
    fn fail_early(&self) -> bool;

    /// The active filter, if any (drives [`walk::filter_is_dir`] /
    /// [`walk::should_skip_entry_ref`]).
    fn filter(&self) -> Option<&crate::filter::FilterSettings>;

    /// Whether a filter decision must retain its authoritative entry until dispatch.
    ///
    /// The default keeps best-effort `d_type` filtering cheap for non-destructive walks. A visitor
    /// whose included filter result authorizes type-sensitive work overrides this so a stale hint
    /// cannot make that decision for one type and then act on a separately classified entry. A
    /// reliable hint may still reject cheaply when [`Self::filter_allows_hint_only_skip`] confirms
    /// that omitting the child cannot authorize later work.
    fn filter_requires_admitted_entry(&self) -> bool {
        false
    }

    /// Whether a reliable-hint exclusion may terminate before authoritative admission.
    ///
    /// The default preserves the cheap exclusion path: no entry action follows a skip. A visitor
    /// whose downstream bookkeeping can turn an omitted child into an action may disable that path
    /// for the current directory context, forcing the exact admitted decision instead.
    fn filter_allows_hint_only_skip(&self, _dir_ctx: &Self::DirContext) -> bool {
        true
    }

    /// Account for an entry the filter excluded, returning its summary
    /// contribution. Called by the driver for each filtered-out child *instead of*
    /// spawning it, so the tool's `*_skipped` counters and dry-run skip reporting
    /// stay tool-owned (the driver is generic over the summary and dry-run mode).
    ///
    /// For ordinary visitors, `kind` comes from the cheap `getdents` hint when available and
    /// `DT_UNKNOWN` is resolved authoritatively as directory/non-directory. An authoritative-filter
    /// visitor receives either the reliable kind that rejected cheaply or the exact admitted kind
    /// from its final recheck. `skip_result` is the `FilterResult` that caused the exclusion. The
    /// driver still increments the shared progress counter via [`EntryKind::inc_skipped`] — override
    /// only to add the summary counters and the `--dry-run` "skip …" line.
    ///
    /// The default does nothing (returns `Default`), which suits metadata-only
    /// walks and the smoke tests; copy/chmod/rm override it to mirror their
    /// existing `skipped_summary_for` + `report_skip` behavior.
    fn on_skip(
        &self,
        _cx: &EntryCx,
        _kind: EntryKind,
        _skip_result: &crate::filter::FilterResult,
    ) -> Self::Summary {
        Self::Summary::default()
    }

    /// Process a non-directory entry (file / symlink / special). `parent_ctx` is the inherited
    /// context of the directory containing this entry (copy's destination parent + freshness).
    /// `leaf` keeps its classification handle structurally ahead of its admission permit for the
    /// full visit.
    fn visit_leaf(
        &self,
        cx: &EntryCx,
        parent_ctx: &Self::DirContext,
        leaf: AdmittedLeaf,
    ) -> impl std::future::Future<Output = Result<Self::Summary, OperationError<Self::Summary>>> + Send;

    /// Pre-order step for a directory entry, run *after* the leaf permit has been
    /// dropped and *before* the contents are walked. `parent_ctx` is the inherited
    /// context of the *containing* directory. Returns [`DirAction::Skip`] to prune
    /// the subtree or [`DirAction::Descend`] to walk it (supplying the child
    /// context for this directory's own children, and the `dir_post` state).
    ///
    /// chmod applies the pre-order mode change here (unless deferred) and opens
    /// the dir; copy resolves the destination directory (mkdir/overwrite/skip) and
    /// puts it in the child context; rm snapshots metadata and arms its
    /// `RelaxedDirGuard`.
    fn dir_pre(
        &self,
        cx: &EntryCx,
        parent_ctx: &Self::DirContext,
        handle: &Handle,
    ) -> impl std::future::Future<Output = DirPreResult<Self>> + Send;

    /// Post-order step for a directory entry, run *after* its contents are walked,
    /// in the same task as `dir_pre`. `state` is the [`DirAction::Descend`] state;
    /// `processed` lists the spawned children; `child_result` is the contents' folded
    /// outcome — `Ok(summary)` when every child succeeded, or `Err` carrying the
    /// combined child error and the partial summary when one or more children failed
    /// **without** `fail_early`. (Neither has `dir_pre`'s own contribution folded in
    /// — the visitor carries that in `state` and folds it here.)
    ///
    /// `dir_post` is **not** called when `fail_early` is set and a child failed: that
    /// case aborts the subtree's already-spawned siblings and returns the error after
    /// the enclosing async task scope has quiesced, without any post-order work — so a
    /// fail-early return never applies post-order finalization (copy's directory metadata /
    /// `--delete` prune). When `fail_early` is unset, `dir_post` IS called
    /// with `Err(..)` so the visitor can still apply safe post-order finalization
    /// (copy applies directory metadata even after a partial failure, but skips the
    /// destructive `--delete` prune) and then return the combined error.
    ///
    /// copy applies directory metadata, empty-dir cleanup, and `--delete` prune;
    /// chmod applies the deferred post-order change; rm runs the time filter, the
    /// `rmdir`, and defuses its guard. A visitor that wants the historical
    /// "finalize only on full success" behavior simply propagates the `Err`.
    fn dir_post(
        &self,
        cx: &EntryCx,
        state: Self::DirState,
        processed: &ProcessedChildren,
        child_result: Result<Self::Summary, OperationError<Self::Summary>>,
    ) -> impl std::future::Future<Output = Result<Self::Summary, OperationError<Self::Summary>>> + Send;
}

/// Process one already-located entry: classify it authoritatively via
/// [`Dir::child`], then dispatch.
///
/// - **Non-directory:** call [`WalkVisitor::visit_leaf`] holding admission.
/// - **Directory:** drop admission — **the one and only drop-before-recurse
///   site** — then [`WalkVisitor::dir_pre`]; on [`DirAction::Descend`], walk the
///   contents via [`walk_dir_contents`] (threading the child context) and finish
///   with [`WalkVisitor::dir_post`].
///
/// `parent_ctx` is the inherited context of the directory that contains this
/// entry. `cx.parent` must be that (hardened) directory. On a classification
/// error the entry's own error is surfaced — the same fail-closed behavior the
/// per-tool walks have.
pub async fn process_entry<V>(
    visitor: Arc<V>,
    cx: EntryCx,
    parent_ctx: V::DirContext,
    admission: impl Into<EntryAdmission> + Send,
) -> Result<V::Summary, OperationError<V::Summary>>
where
    V: WalkVisitor,
{
    scope_tasks(process_entry_tracked(
        visitor,
        cx,
        parent_ctx,
        EntrySource::Unclassified(admission.into()),
    ))
    .await
}

/// Process an entry whose authoritative classification already informed its scheduling decision.
///
/// Destructive filters use this boundary so the shared driver consumes the exact handle they
/// classified instead of resolving the name again and potentially acting on a different type.
pub(crate) async fn process_admitted_entry<V>(
    visitor: Arc<V>,
    cx: EntryCx,
    parent_ctx: V::DirContext,
    entry: AdmittedEntry,
) -> Result<V::Summary, OperationError<V::Summary>>
where
    V: WalkVisitor,
{
    scope_tasks(process_entry_tracked(
        visitor,
        cx,
        parent_ctx,
        EntrySource::Admitted(entry),
    ))
    .await
}

enum EntrySource {
    Unclassified(EntryAdmission),
    Admitted(AdmittedEntry),
}

#[async_recursion]
async fn process_entry_tracked<V>(
    visitor: Arc<V>,
    cx: EntryCx,
    parent_ctx: V::DirContext,
    source: EntrySource,
) -> Result<V::Summary, OperationError<V::Summary>>
where
    V: WalkVisitor,
{
    let _ops_guard = cx.prog_track.ops.guard();
    enum AdmittedDispatch<S> {
        Leaf(Result<S, OperationError<S>>),
        Directory(Handle),
    }

    let (hinted_directory, admitted_entry) = match source {
        EntrySource::Admitted(entry) => (None, Some(entry)),
        EntrySource::Unclassified(mut admission) => {
            // a positive directory hint gets one unadmitted classification. if stale, close that
            // first leaf handle before waiting for admission; final classification and leaf work
            // then run inside the newly admitted region below.
            let hinted_directory = if matches!(admission, EntryAdmission::HintedDirectory) {
                match walk::classify_entry(&cx.parent, &cx.name).await {
                    Ok(handle) if handle.kind() == EntryKind::Dir => Some(handle),
                    Ok(handle) => {
                        drop(handle);
                        admission = EntryAdmission::RootOrDelegated;
                        None
                    }
                    Err(err) => {
                        let err = anyhow::Error::new(err)
                            .context(format!("failed reading metadata from {:?}", &cx.real_path));
                        return Err(OperationError::new(err, Default::default()));
                    }
                }
            } else {
                None
            };
            if hinted_directory.is_some() {
                (hinted_directory, None)
            } else {
                admission = walk::ensure_entry_admission(visitor.permit_kind(), admission).await;
                let entry =
                    walk::classify_admitted_entry(&cx.parent, &cx.name, admission.into_permit())
                        .await
                        .map_err(|err| {
                            let err = anyhow::Error::new(err).context(format!(
                                "failed reading metadata from {:?}",
                                &cx.real_path
                            ));
                            OperationError::new(err, Default::default())
                        })?;
                (None, Some(entry))
            }
        }
    };

    let handle = match hinted_directory {
        Some(handle) => handle,
        None => {
            let entry = admitted_entry.expect("an admitted classification must be present");
            let weak_admission = entry.admission();
            let dispatch = crate::safedir::with_optional_fd_admission(weak_admission, async {
                match entry.into_leaf() {
                    Ok(leaf) => {
                        return AdmittedDispatch::Leaf(
                            visitor.visit_leaf(&cx, &parent_ctx, leaf).await,
                        );
                    }
                    Err(handle) => {
                        // ── the single drop-before-recurse site ──────────────────────────────
                        // release provisional admission inside its scope, then return only the
                        // directory handle. ending the scope restores any outer pool before
                        // dir_pre can recurse.
                        AdmittedDispatch::Directory(handle)
                    }
                }
            })
            .await;
            match dispatch {
                AdmittedDispatch::Leaf(result) => return result,
                AdmittedDispatch::Directory(handle) => handle,
            }
        }
    };
    let action = visitor.dir_pre(&cx, &parent_ctx, &handle).await?;
    // dir_pre has copied everything its state needs and opened the directory used for descent.
    // the classification handle is redundant from here on and must not inflate the per-depth fd
    // baseline alongside those deliberately unbudgeted recursive directory handles.
    drop(handle);
    match action {
        DirAction::Skip(summary) => Ok(summary),
        DirAction::Descend {
            dir,
            child_ctx,
            state,
        } => {
            match walk_dir_contents(Arc::clone(&visitor), dir, &cx, &child_ctx).await {
                Ok((child_summary, processed)) => {
                    visitor
                        .dir_post(&cx, state, &processed, Ok(child_summary))
                        .await
                }
                // a child failed. with `fail_early` the subtree's already-spawned siblings have
                // been aborted and NO post-order work runs — the error propagates as-is after the
                // enclosing async task scope quiesces. without `fail_early`, `dir_post` IS still
                // invoked,
                // with the combined error, so the visitor can apply safe post-order finalization
                // (copy's directory metadata) while skipping destructive work (copy's `--delete`
                // prune) and then return the combined error. `processed` is not recoverable on the
                // error path, so an empty list is passed — the only consumer (a `--delete` keep-set)
                // is skipped on error anyway.
                Err(walk_err) => {
                    if visitor.fail_early() {
                        Err(walk_err)
                    } else {
                        visitor
                            .dir_post(&cx, state, &ProcessedChildren::default(), Err(walk_err))
                            .await
                    }
                }
            }
        }
    }
}

/// Walk the contents of an open hardened directory.
///
/// Enumerates `dir` (gated `read_entries`) and applies the visitor's filter. Ordinary visitors admit
/// only `DT_UNKNOWN` filter probes and otherwise retain cheap hints. An authoritative-filter visitor
/// may reject from a reliable hint when its context permits a terminal omission; every other entry is
/// admitted, reclassified, filtered again, and transferred into the child task through that exact
/// entry. Each remaining possible leaf is admitted immediately before spawn, and tasks are joined
/// with a fold + fail-early via [`join_and_fold`].
///
/// Returns the folded child summary (filter-skip contributions included) and the
/// [`ProcessedChildren`] list of the names that were spawned. `parent_cx`
/// describes the directory entry itself (its `rel_path`/`real_path` are the base
/// the children extend). `dir_ctx` is the inherited context of `dir` (the context
/// its children receive) — for the root walk this is
/// [`WalkVisitor::root_dir_context`].
///
/// ## Acquire-then-spawn ordering
///
/// Each leaf permit is acquired and the child task is spawned **in the same loop
/// iteration**, before the next child's permit is acquired. This is load-bearing
/// for backpressure correctness: a directory may have more permit-taking leaf
/// children than the pool has permits. If permits for *every* child were acquired
/// before *any* task was spawned (batch-acquire-then-spawn), the acquire loop would
/// block on permit `N+1` while the first `N` permits are held by not-yet-running
/// tasks — a self-deadlock against a saturated pool. Spawning each task as soon as
/// its permit is taken lets running tasks release permits the loop is waiting on.
pub async fn walk_dir_contents<V>(
    visitor: Arc<V>,
    dir: Arc<Dir>,
    parent_cx: &EntryCx,
    dir_ctx: &V::DirContext,
) -> Result<(V::Summary, ProcessedChildren), OperationError<V::Summary>>
where
    V: WalkVisitor,
{
    scope_tasks(walk_dir_contents_tracked(visitor, dir, parent_cx, dir_ctx)).await
}

#[async_recursion]
async fn walk_dir_contents_tracked<V>(
    visitor: Arc<V>,
    dir: Arc<Dir>,
    parent_cx: &EntryCx,
    dir_ctx: &V::DirContext,
) -> Result<(V::Summary, ProcessedChildren), OperationError<V::Summary>>
where
    V: WalkVisitor,
{
    let entries = match dir.read_entries().await {
        Ok(entries) => entries,
        Err(err) => {
            let err = anyhow::Error::new(err)
                .context(format!("cannot read directory {:?}", &parent_cx.real_path));
            return Err(OperationError::new(err, Default::default()));
        }
    };
    walk_dir_entries(visitor, dir, parent_cx, dir_ctx, entries).await
}

/// Records one filtered exclusion in the shared progress and visitor-owned summary.
fn record_filter_skip<V: WalkVisitor>(
    visitor: &V,
    parent_cx: &EntryCx,
    child_cx: &EntryCx,
    kind: EntryKind,
    skip_result: &crate::filter::FilterResult,
    skipped_summary: &mut V::Summary,
) {
    tracing::debug!("skipping {:?} due to filter", &child_cx.real_path);
    kind.inc_skipped(parent_cx.prog_track);
    *skipped_summary =
        std::mem::take(skipped_summary) + visitor.on_skip(child_cx, kind, skip_result);
}

/// Process an already-enumerated directory listing.
///
/// Keeping enumeration separate from admission/scheduling gives tests a deterministic way to
/// supply unavailable or stale hints without requiring a particular filesystem race. Production
/// callers always arrive through [`walk_dir_contents`].
pub(crate) async fn walk_dir_entries<V>(
    visitor: Arc<V>,
    dir: Arc<Dir>,
    parent_cx: &EntryCx,
    dir_ctx: &V::DirContext,
    entries: Vec<(OsString, Option<EntryKind>)>,
) -> Result<(V::Summary, ProcessedChildren), OperationError<V::Summary>>
where
    V: WalkVisitor,
{
    let mut skipped_summary = V::Summary::default();
    let mut processed = ProcessedChildren::default();
    let mut join_set = tokio::task::JoinSet::new();
    for (entry_name, hint) in entries {
        let mut admission = Some(EntryAdmission::from_hint(hint));
        let mut admitted_entry = None;
        // build the child's owned context once; reused whether it is skipped or spawned, and gives
        // an authoritative probe failure its operation-path context.
        let child_cx = parent_cx.child(Arc::clone(&dir), &entry_name);
        let authoritative_filter =
            visitor.filter().is_some() && visitor.filter_requires_admitted_entry();
        if authoritative_filter
            && visitor.filter_allows_hint_only_skip(dir_ctx)
            && let Some(hinted_kind) = hint
            && let Some(skip_result) = walk::should_skip_entry_ref(
                visitor.filter(),
                &child_cx.filter_path,
                hinted_kind == EntryKind::Dir,
            )
        {
            // the visitor confirmed that omitting this child cannot authorize downstream work, so
            // a reliable hint may retain this cheap path. any provisional include instead reaches
            // the admitted exact recheck below.
            record_filter_skip(
                visitor.as_ref(),
                parent_cx,
                &child_cx,
                hinted_kind,
                &skip_result,
                &mut skipped_summary,
            );
            continue;
        }
        let entry_is_dir = if authoritative_filter {
            // destructive filtering classifies once under admission and transfers that exact
            // handle into dispatch. `d_type` may cheaply exclude, but cannot authorize an action.
            let initial_admission = admission
                .take()
                .expect("entry admission must be present")
                .require_admission();
            let entry_admission =
                walk::ensure_entry_admission(visitor.permit_kind(), initial_admission).await;
            let entry = match walk::classify_admitted_entry(
                &dir,
                &entry_name,
                entry_admission.into_permit(),
            )
            .await
            {
                Ok(entry) => entry,
                Err(error) => {
                    abort_and_join(&mut join_set).await;
                    let error = anyhow::Error::new(error).context(format!(
                        "failed reading metadata from {:?}",
                        &child_cx.real_path
                    ));
                    return Err(OperationError::new(error, skipped_summary));
                }
            };
            let entry_is_dir = entry.kind() == EntryKind::Dir;
            admitted_entry = Some(entry);
            entry_is_dir
        } else {
            // a DT_UNKNOWN filter decision opens an O_PATH handle, so it is the only ordinary
            // filter path that acquires before filtering. reliable hints keep cheap skips outside
            // leaf admission for non-destructive walks.
            if hint.is_none() && visitor.filter().is_some() {
                let entry_admission = walk::ensure_entry_admission(
                    visitor.permit_kind(),
                    admission.take().expect("entry admission must be present"),
                )
                .await;
                admission = Some(entry_admission);
            }
            // dt_unknown uses one authoritative fstat for the filter decision. Dispatch still
            // classifies separately because non-destructive visitors do not act on this type.
            match crate::safedir::with_optional_fd_admission(
                admission.as_ref().and_then(EntryAdmission::admission),
                walk::filter_is_dir(visitor.filter(), &dir, &entry_name, hint, false),
            )
            .await
            {
                Ok(entry_is_dir) => entry_is_dir,
                Err(error) => {
                    abort_and_join(&mut join_set).await;
                    let error = anyhow::Error::new(error).context(format!(
                        "failed reading metadata from {:?}",
                        &child_cx.real_path
                    ));
                    return Err(OperationError::new(error, skipped_summary));
                }
            }
        };
        if let Some(skip_result) =
            walk::should_skip_entry_ref(visitor.filter(), &child_cx.filter_path, entry_is_dir)
        {
            // destructive visitors use the transferred entry's exact kind for skip accounting.
            // ordinary visitors retain the getdents hint, with DT_UNKNOWN falling back to the
            // authoritative dir/non-dir decision already computed for the filter. the driver does
            // the shared progress increment; the visitor owns summary and dry-run reporting.
            let entry_kind = admitted_entry
                .as_ref()
                .map(AdmittedEntry::kind)
                .unwrap_or_else(|| {
                    hint.unwrap_or(if entry_is_dir {
                        EntryKind::Dir
                    } else {
                        EntryKind::File
                    })
                });
            record_filter_skip(
                visitor.as_ref(),
                parent_cx,
                &child_cx,
                entry_kind,
                &skip_result,
                &mut skipped_summary,
            );
            continue;
        }
        // own everything moved into the task (cancellation safety): the child context
        // (source parent Arc + owned name/paths), the visitor handle, and a clone of
        // the inherited context (copy's destination parent dir).
        let task_visitor = Arc::clone(&visitor);
        let task_ctx = dir_ctx.clone();
        processed.names.push(entry_name);
        if let Some(entry) = admitted_entry {
            spawn_tracked(
                &mut join_set,
                process_admitted_entry(task_visitor, child_cx, task_ctx, entry),
            );
        } else {
            // every included possible leaf acquires immediately before spawn. a positive directory
            // hint retains its explicit exception state into the worker.
            let admission = walk::ensure_entry_admission(
                visitor.permit_kind(),
                admission.expect("unclassified entry admission must be present"),
            )
            .await;
            spawn_tracked(
                &mut join_set,
                process_entry(task_visitor, child_cx, task_ctx, admission),
            );
        }
    }
    let folded =
        join_and_fold::<V::Summary>(join_set, visitor.fail_early(), skipped_summary).await?;
    Ok((folded, processed))
}

/// Cancels every remaining task and waits for their direct futures to finish dropping.
pub(crate) async fn abort_and_join<T: 'static>(join_set: &mut tokio::task::JoinSet<T>) {
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}
}

/// Join an already-populated `JoinSet` of per-child tasks and fold their summaries
/// with fail-early / error-collection semantics — the shared join engine behind
/// the directory walk.
///
/// The directory walk spawns into the `JoinSet` incrementally (acquire-then-spawn,
/// see [`walk_dir_contents`]) and hands it here so the held leaf permits are
/// released by running tasks rather than all held before the first task runs.
/// `base` seeds the fold (the walk passes the filter-skip contributions). On
/// `fail_early`, the first task error aborts the remaining tasks. The enclosing
/// task scope waits for recursively owned async tasks to finish dropping before the operation
/// returns. Already-started blocking jobs are not cancellable and do not delay this return; their
/// separate fd-admission leases remain live until their work and abandoned outputs finish.
/// Otherwise all errors are collected and deduplicated, and the single
/// combined error (if any) is returned with the full folded summary.
pub async fn join_and_fold<S>(
    mut join_set: tokio::task::JoinSet<Result<S, OperationError<S>>>,
    fail_early: bool,
    base: S,
) -> Result<S, OperationError<S>>
where
    S: WalkSummary,
{
    let mut summary = base;
    let errors = crate::error_collector::ErrorCollector::default();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(child_summary)) => summary = summary + child_summary,
            Ok(Err(error)) => {
                tracing::error!("walk child failed with: {:#}", &error);
                summary = summary + error.summary;
                if fail_early {
                    abort_and_join(&mut join_set).await;
                    return Err(OperationError::new(error.source, summary));
                }
                errors.push(error.source);
            }
            Err(join_error) => {
                if fail_early {
                    abort_and_join(&mut join_set).await;
                    return Err(OperationError::new(join_error.into(), summary));
                }
                errors.push(join_error.into());
            }
        }
    }
    if let Some(error) = errors.into_error() {
        return Err(OperationError::new(error, summary));
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterSettings;
    use crate::progress::Progress;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    static PROGRESS: std::sync::LazyLock<Progress> = std::sync::LazyLock::new(Progress::new);

    /// A minimal `Summary` for the driver tests: counts files, dirs, and symlinks.
    #[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
    struct CountSummary {
        files: usize,
        dirs: usize,
        symlinks: usize,
    }

    impl std::ops::Add for CountSummary {
        type Output = Self;
        fn add(self, other: Self) -> Self {
            Self {
                files: self.files + other.files,
                dirs: self.dirs + other.dirs,
                symlinks: self.symlinks + other.symlinks,
            }
        }
    }

    /// A trivial visitor that just counts entries by kind. Exercises RPITIT +
    /// `Send` + recursion (compile and run). `DirState = ()`; leaf permit comes
    /// from the pending-meta pool for every possible leaf.
    struct CountingVisitor {
        /// counts every spawned leaf, to prove `visit_leaf` ran under backpressure.
        leaves_seen: Arc<AtomicUsize>,
        /// optional filter used by ordering tests.
        filter: Option<FilterSettings>,
        /// whether the filter authorizes destructive work and needs an exact entry transfer.
        authoritative_filter: bool,
    }

    impl WalkVisitor for CountingVisitor {
        type Summary = CountSummary;
        type DirContext = ();
        type DirState = ();

        fn root_dir_context(&self) {}

        fn permit_kind(&self) -> PermitKind {
            PermitKind::PendingMeta
        }
        fn fail_early(&self) -> bool {
            false
        }
        fn filter(&self) -> Option<&FilterSettings> {
            self.filter.as_ref()
        }
        fn filter_requires_admitted_entry(&self) -> bool {
            self.authoritative_filter
        }

        async fn visit_leaf(
            &self,
            _cx: &EntryCx,
            _parent_ctx: &(),
            leaf: AdmittedLeaf,
        ) -> Result<CountSummary, OperationError<CountSummary>> {
            self.leaves_seen.fetch_add(1, Ordering::SeqCst);
            Ok(match leaf.kind() {
                EntryKind::Symlink => CountSummary {
                    symlinks: 1,
                    ..Default::default()
                },
                _ => CountSummary {
                    files: 1,
                    ..Default::default()
                },
            })
        }

        async fn dir_pre(
            &self,
            cx: &EntryCx,
            _parent_ctx: &(),
            _handle: &Handle,
        ) -> Result<DirAction<CountSummary, (), ()>, OperationError<CountSummary>> {
            // open the directory's contents fd (O_NOFOLLOW) and descend.
            let dir = cx.parent.open_dir(&cx.name).await.map_err(|err| {
                OperationError::new(
                    anyhow::Error::new(err)
                        .context(format!("cannot open directory {:?}", &cx.real_path)),
                    Default::default(),
                )
            })?;
            Ok(DirAction::Descend {
                dir: Arc::new(dir),
                child_ctx: (),
                state: (),
            })
        }

        async fn dir_post(
            &self,
            _cx: &EntryCx,
            _state: (),
            _processed: &ProcessedChildren,
            child_result: Result<CountSummary, OperationError<CountSummary>>,
        ) -> Result<CountSummary, OperationError<CountSummary>> {
            // count this directory itself, post-order. a child error propagates (this test visitor
            // has `fail_early == false` but never errors, so the `Ok` arm is what runs).
            let child_summary = child_result?;
            Ok(child_summary
                + CountSummary {
                    dirs: 1,
                    ..Default::default()
                })
        }
    }

    /// Build an `EntryCx` for the root directory `name` under `parent`.
    fn root_cx(parent: Arc<Dir>, name: &OsStr, real_path: PathBuf) -> EntryCx {
        EntryCx {
            parent,
            name: name.to_owned(),
            rel_path: PathBuf::new(),
            filter_path: PathBuf::new(),
            real_path,
            dry_run: false,
            prog_track: &PROGRESS,
        }
    }

    struct DropNotice(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    struct BlockingDropNotice {
        started: Option<tokio::sync::oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl Drop for BlockingDropNotice {
        fn drop(&mut self) {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
            }
            let _ = self.release.recv();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn task_scope_waits_for_an_unpolled_spawn_to_be_cancelled() {
        let (dropped_tx, mut dropped_rx) = tokio::sync::oneshot::channel();
        scope_tasks(async move {
            let mut join_set = tokio::task::JoinSet::new();
            let drop_notice = DropNotice(Some(dropped_tx));
            spawn_tracked(&mut join_set, async move {
                let _drop_notice = drop_notice;
                std::future::pending::<()>().await;
            });
        })
        .await;
        assert_eq!(
            dropped_rx.try_recv(),
            Ok(()),
            "task scope returned before its unpolled spawned child was destroyed"
        );
    }

    #[test]
    fn task_scope_waits_for_an_unpolled_child_to_finish_dropping() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        runtime.block_on(async {
            // occupy the sole runtime worker so `spawn_tracked` can publish, then abort, a child
            // that Tokio has not polled. `block_on` itself continues driving the scope on this
            // calling thread.
            let (worker_started_tx, worker_started_rx) = tokio::sync::oneshot::channel();
            let (release_worker_tx, release_worker_rx) = std::sync::mpsc::channel();
            let worker = tokio::spawn(async move {
                let _ = worker_started_tx.send(());
                let _ = release_worker_rx.recv();
            });
            worker_started_rx
                .await
                .expect("the sole runtime worker did not start its blocker");

            let (drop_started_tx, drop_started_rx) = tokio::sync::oneshot::channel();
            let (release_drop_tx, release_drop_rx) = std::sync::mpsc::channel();
            let child_drop = BlockingDropNotice {
                started: Some(drop_started_tx),
                release: release_drop_rx,
            };
            let mut scoped = Box::pin(scope_tasks(async move {
                let mut join_set = tokio::task::JoinSet::new();
                spawn_tracked(&mut join_set, async move {
                    let _child_drop = child_drop;
                    std::future::pending::<()>().await;
                });
                // tokio guarantees spawn does not poll synchronously. With its sole worker still
                // occupied, dropping this set aborts a deterministically unpolled child.
                drop(join_set);
            }));
            assert!(
                futures::poll!(scoped.as_mut()).is_pending(),
                "the task scope must wait for its registered child"
            );

            release_worker_tx
                .send(())
                .expect("the worker blocker ended before release");
            drop_started_rx
                .await
                .expect("the aborted child did not start dropping");
            let scope_poll = futures::poll!(scoped.as_mut());
            let waited_for_child_drop = scope_poll.is_pending();

            let release_result = release_drop_tx.send(());
            if scope_poll.is_pending() {
                scoped.await;
            }
            let worker_result = worker.await;
            release_result.expect("the child finished dropping before release");
            worker_result.expect("the worker blocker panicked");
            assert!(
                waited_for_child_drop,
                "task scope returned while an unpolled child's captures were still dropping"
            );
            anyhow::Ok(())
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fail_early_cancels_and_quiesces_nested_async_work() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, mut dropped_rx) = tokio::sync::oneshot::channel();
        let nested_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_completed = Arc::clone(&nested_completed);
        let result = scope_tasks(async move {
            let mut join_set = tokio::task::JoinSet::new();
            spawn_tracked(&mut join_set, async move {
                let mut nested = tokio::task::JoinSet::new();
                spawn_tracked(&mut nested, async move {
                    let _drop_notice = DropNotice(Some(dropped_tx));
                    let _ = started_tx.send(());
                    std::future::pending::<()>().await;
                    task_completed.store(true, std::sync::atomic::Ordering::SeqCst);
                });
                while nested.join_next().await.is_some() {}
                Ok(CountSummary::default())
            });
            started_rx.await.expect("sibling task did not start");
            spawn_tracked(&mut join_set, async {
                Err(OperationError::new(
                    anyhow::anyhow!("stop the walk"),
                    CountSummary::default(),
                ))
            });
            join_and_fold(join_set, true, CountSummary::default()).await
        })
        .await;

        assert!(result.is_err());
        assert!(
            dropped_rx.try_recv().is_ok(),
            "fail-early returned before nested sibling work was dropped"
        );
        assert!(!nested_completed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fail_early_returns_while_non_cancellable_work_retains_admission() {
        let admission = crate::testutils::AdmissionLimit::new().await;
        admission.set_max_open_files(1);
        let open_file_guard = throttle::open_file_permit().await;
        let fd_admission = open_file_guard.admission();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let (inner_done_tx, inner_done_rx) = tokio::sync::oneshot::channel();

        let operation = tokio::spawn(scope_tasks(async move {
            let mut join_set = tokio::task::JoinSet::new();
            spawn_tracked(&mut join_set, async move {
                let _open_file_guard = open_file_guard;
                let result = crate::safedir::with_fd_admission(fd_admission, async move {
                    crate::safedir::run_fd_admitted_blocking(move || {
                        let _ = started_tx.send(());
                        let _ = release_rx.recv();
                        let _ = completed_tx.send(());
                        Ok(())
                    })
                    .await
                })
                .await;
                result
                    .map(|()| CountSummary::default())
                    .map_err(|error| OperationError::new(error.into(), CountSummary::default()))
            });
            started_rx.await.expect("blocking sibling did not start");
            spawn_tracked(&mut join_set, async {
                Err(OperationError::new(
                    anyhow::anyhow!("stop the walk"),
                    CountSummary::default(),
                ))
            });
            let result = join_and_fold(join_set, true, CountSummary::default()).await;
            let _ = inner_done_tx.send(());
            result
        }));

        inner_done_rx
            .await
            .expect("fail-early join did not cancel its async siblings");
        tokio::task::yield_now().await;
        let returned_before_blocking_release = operation.is_finished();
        let mut second_permit = Box::pin(throttle::open_file_permit());
        let admission_retained = futures::poll!(second_permit.as_mut()).is_pending();

        let release_result = release_tx.send(());
        let cleanup = admission
            .run_with_timeout(Duration::from_secs(20), async {
                let completion = completed_rx.await;
                let permit = second_permit.await;
                drop(permit);
                let operation_result = operation.await;
                (completion, operation_result)
            })
            .await;

        release_result.expect("blocking sibling ended before release");
        let (completion, operation_result) = cleanup.expect("blocking sibling did not quiesce");
        completion.expect("blocking sibling did not report completion");
        assert!(
            operation_result
                .expect("scoped fail-early operation panicked")
                .is_err()
        );
        assert!(
            returned_before_blocking_release,
            "fail-early waited for non-cancellable blocking work"
        );
        assert!(
            admission_retained,
            "cancelled blocking work released admission before finishing"
        );
    }

    // The driver compiles and runs end-to-end (RPITIT + Send + recursion) and counts
    // a real tree correctly. `setup_test_dir` builds foo/{0.txt, bar/{1,2,3.txt},
    // baz/{4.txt, 5.txt->sym, 6.txt->sym}}: under `foo` there are 5 files, 2
    // subdirectories, and 2 symlinks.
    #[tokio::test]
    async fn counts_entries_in_a_tree() -> anyhow::Result<()> {
        let tmp = crate::testutils::setup_test_dir().await?;
        let foo = tmp.join("foo");
        // open the foo directory as the hardened root to walk its contents.
        let root = Arc::new(Dir::open_root_dir(&foo, false, congestion::Side::Source).await?);
        let leaves_seen = Arc::new(AtomicUsize::new(0));
        let visitor = Arc::new(CountingVisitor {
            leaves_seen: Arc::clone(&leaves_seen),
            filter: None,
            authoritative_filter: false,
        });
        let cx = root_cx(Arc::clone(&root), std::ffi::OsStr::new("foo"), foo.clone());
        let (summary, processed) = walk_dir_contents(visitor, root, &cx, &()).await?;
        assert_eq!(
            summary,
            CountSummary {
                files: 5,
                dirs: 2,
                symlinks: 2,
            },
            "the walk must count every entry once, by kind"
        );
        // the top-level processed list is foo's direct children: 0.txt, bar, baz.
        assert_eq!(processed.names().len(), 3, "foo has three direct children");
        assert_eq!(
            leaves_seen.load(Ordering::SeqCst),
            7,
            "every non-directory leaf (5 files + 2 symlinks) was visited"
        );
        Ok(())
    }

    /// Driver-level deadlock regression. The module name carries the
    /// `max_open_files` substring so nextest's serial test-group isolates this
    /// process-wide throttle mutation (see `.config/nextest.toml`).
    mod max_open_files_tests {
        use super::*;
        use anyhow::Context;
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::MetadataExt;

        #[derive(Clone, Copy)]
        enum LeafOutcome {
            Success,
            Error,
        }

        struct LeafLifetimeVisitor {
            started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<std::os::fd::RawFd>>>,
            release: Arc<tokio::sync::Notify>,
            outcome: LeafOutcome,
        }

        struct DirPreLifetimeVisitor {
            file_path: std::path::PathBuf,
            started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
            completed: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        }

        impl WalkVisitor for DirPreLifetimeVisitor {
            type Summary = CountSummary;
            type DirContext = ();
            type DirState = ();

            fn root_dir_context(&self) {}

            fn permit_kind(&self) -> PermitKind {
                PermitKind::PendingMeta
            }

            fn fail_early(&self) -> bool {
                false
            }

            fn filter(&self) -> Option<&FilterSettings> {
                None
            }

            async fn visit_leaf(
                &self,
                _cx: &EntryCx,
                _parent_ctx: &(),
                _leaf: AdmittedLeaf,
            ) -> Result<CountSummary, OperationError<CountSummary>> {
                unreachable!("dir-pre lifetime visitor must not see a leaf")
            }

            async fn dir_pre(
                &self,
                _cx: &EntryCx,
                _parent_ctx: &(),
                _handle: &Handle,
            ) -> DirPreResult<Self> {
                let file_path = self.file_path.clone();
                let started = self
                    .started
                    .lock()
                    .expect("started lock poisoned")
                    .take()
                    .expect("dir_pre runs once");
                let release = self
                    .release
                    .lock()
                    .expect("release lock poisoned")
                    .take()
                    .expect("dir_pre runs once");
                let completed = self
                    .completed
                    .lock()
                    .expect("completed lock poisoned")
                    .take()
                    .expect("dir_pre runs once");
                crate::safedir::run_fd_admitted_blocking(move || {
                    let file = std::fs::File::open(file_path)?;
                    let _ = started.send(());
                    release.recv().map_err(std::io::Error::other)?;
                    drop(file);
                    let _ = completed.send(());
                    Ok(())
                })
                .await
                .map_err(|err| {
                    OperationError::new(anyhow::Error::new(err), CountSummary::default())
                })?;
                Ok(DirAction::Skip(CountSummary::default()))
            }

            async fn dir_post(
                &self,
                _cx: &EntryCx,
                _state: (),
                _processed: &ProcessedChildren,
                _child_result: Result<CountSummary, OperationError<CountSummary>>,
            ) -> Result<CountSummary, OperationError<CountSummary>> {
                unreachable!("dir-pre lifetime visitor does not descend")
            }
        }

        impl WalkVisitor for LeafLifetimeVisitor {
            type Summary = CountSummary;
            type DirContext = ();
            type DirState = ();

            fn root_dir_context(&self) {}

            fn permit_kind(&self) -> PermitKind {
                PermitKind::PendingMeta
            }

            fn fail_early(&self) -> bool {
                false
            }

            fn filter(&self) -> Option<&FilterSettings> {
                None
            }

            async fn visit_leaf(
                &self,
                _cx: &EntryCx,
                _parent_ctx: &(),
                leaf: AdmittedLeaf,
            ) -> Result<CountSummary, OperationError<CountSummary>> {
                let raw_fd = leaf.handle().as_fd().as_raw_fd();
                if let Some(started) = self.started.lock().expect("started lock poisoned").take() {
                    let _ = started.send(raw_fd);
                }
                self.release.notified().await;
                match self.outcome {
                    LeafOutcome::Success => Ok(CountSummary {
                        files: 1,
                        ..Default::default()
                    }),
                    LeafOutcome::Error => Err(OperationError::new(
                        anyhow::anyhow!("leaf operation failed"),
                        CountSummary::default(),
                    )),
                }
            }

            async fn dir_pre(
                &self,
                _cx: &EntryCx,
                _parent_ctx: &(),
                _handle: &Handle,
            ) -> DirPreResult<Self> {
                unreachable!("leaf lifetime visitor must not see a directory")
            }

            async fn dir_post(
                &self,
                _cx: &EntryCx,
                _state: (),
                _processed: &ProcessedChildren,
                _child_result: Result<CountSummary, OperationError<CountSummary>>,
            ) -> Result<CountSummary, OperationError<CountSummary>> {
                unreachable!("leaf lifetime visitor must not recurse")
            }
        }

        async fn start_leaf_lifetime_operation(
            outcome: LeafOutcome,
        ) -> anyhow::Result<(
            crate::testutils::AdmissionLimit,
            tokio::task::JoinHandle<Result<CountSummary, OperationError<CountSummary>>>,
            std::os::fd::RawFd,
            Arc<tokio::sync::Notify>,
            std::path::PathBuf,
        )> {
            let root = crate::testutils::create_temp_dir().await?;
            let leaf_path = root.join("leaf");
            tokio::fs::write(&leaf_path, b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let parent = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let release = Arc::new(tokio::sync::Notify::new());
            let visitor = Arc::new(LeafLifetimeVisitor {
                started: std::sync::Mutex::new(Some(started_tx)),
                release: Arc::clone(&release),
                outcome,
            });
            let cx = root_cx(parent, OsStr::new("leaf"), leaf_path);
            let permit =
                walk::preacquire_leaf_permit(PermitKind::PendingMeta, Some(EntryKind::File)).await;
            let task = tokio::spawn(process_entry(visitor, cx, (), permit));
            let raw_fd = started_rx.await.context("leaf visitor did not start")?;
            Ok((admission, task, raw_fd, release, root))
        }

        #[tokio::test(flavor = "current_thread")]
        async fn successful_leaf_holds_admission_through_visitor() -> anyhow::Result<()> {
            let (_admission, task, raw_fd, release, root) =
                start_leaf_lifetime_operation(LeafOutcome::Success).await?;
            let mut next_permit = Box::pin(throttle::pending_meta_permit());
            assert!(
                futures::poll!(next_permit.as_mut()).is_pending(),
                "leaf admission returned while its classification handle was live"
            );
            release.notify_one();
            let summary = task.await?.map_err(|error| error.source)?;
            assert_eq!(summary.files, 1);
            assert!(
                crate::testutils::fd_is_closed(raw_fd),
                "successful leaf returned with its classification handle open"
            );
            let permit = tokio::time::timeout(Duration::from_secs(1), next_permit)
                .await
                .context("successful leaf did not return admission")?;
            drop(permit);
            tokio::fs::remove_dir_all(root).await?;
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn failed_leaf_holds_admission_through_visitor() -> anyhow::Result<()> {
            let (_admission, task, raw_fd, release, root) =
                start_leaf_lifetime_operation(LeafOutcome::Error).await?;
            let mut next_permit = Box::pin(throttle::pending_meta_permit());
            assert!(
                futures::poll!(next_permit.as_mut()).is_pending(),
                "leaf admission returned while its classification handle was live"
            );
            release.notify_one();
            let error = task.await?.expect_err("leaf operation must fail");
            assert!(format!("{:#}", error.source).contains("leaf operation failed"));
            assert!(
                crate::testutils::fd_is_closed(raw_fd),
                "failed leaf returned with its classification handle open"
            );
            let permit = tokio::time::timeout(Duration::from_secs(1), next_permit)
                .await
                .context("failed leaf did not return admission")?;
            drop(permit);
            tokio::fs::remove_dir_all(root).await?;
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn cancelled_leaf_drops_handle_and_returns_admission() -> anyhow::Result<()> {
            let (_admission, task, raw_fd, _release, root) =
                start_leaf_lifetime_operation(LeafOutcome::Success).await?;
            let mut next_permit = Box::pin(throttle::pending_meta_permit());
            assert!(
                futures::poll!(next_permit.as_mut()).is_pending(),
                "leaf admission returned while its classification handle was live"
            );
            task.abort();
            let task_error = task.await.expect_err("leaf task must be cancelled");
            assert!(task_error.is_cancelled());
            assert!(
                crate::testutils::fd_is_closed(raw_fd),
                "cancelled leaf retained its classification handle"
            );
            let permit = tokio::time::timeout(Duration::from_secs(1), next_permit)
                .await
                .context("cancelled leaf did not return admission")?;
            drop(permit);
            tokio::fs::remove_dir_all(root).await?;
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn directory_ends_inner_scope_before_dir_pre_blocking_work() -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let directory_path = root.join("directory");
            let file_path = root.join("owned-by-dir-pre");
            tokio::fs::create_dir(&directory_path).await?;
            tokio::fs::write(&file_path, b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let parent = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
            let visitor = Arc::new(DirPreLifetimeVisitor {
                file_path,
                started: std::sync::Mutex::new(Some(started_tx)),
                release: std::sync::Mutex::new(Some(release_rx)),
                completed: std::sync::Mutex::new(Some(completed_tx)),
            });
            let cx = root_cx(parent, OsStr::new("directory"), directory_path);
            let outer_guard = throttle::open_file_permit().await;
            let outer_admission = outer_guard.admission();
            let task = tokio::spawn(crate::safedir::with_fd_admission(
                outer_admission,
                async move {
                    let _outer_guard = outer_guard;
                    process_entry(visitor, cx, (), EntryAdmission::RootOrDelegated).await
                },
            ));
            started_rx
                .await
                .context("dir_pre blocking work did not start")?;
            task.abort();
            let task_error = task.await.expect_err("directory task must be cancelled");
            assert!(task_error.is_cancelled());
            let mut next_permit = Box::pin(throttle::open_file_permit());
            let admission_was_retained = futures::poll!(next_permit.as_mut()).is_pending();
            let release_result = release_tx.send(());
            completed_rx
                .await
                .context("dir_pre blocking work did not complete")?;
            release_result.context("dir_pre blocking work ended before release")?;
            assert!(
                admission_was_retained,
                "the expired inner directory admission masked the live outer leaf admission"
            );
            drop(
                tokio::time::timeout(Duration::from_secs(1), next_permit)
                    .await
                    .context("dir_pre blocking work did not return outer admission")?,
            );
            tokio::fs::remove_dir_all(root).await?;
            Ok(())
        }

        fn inode_is_open(path: &std::path::Path) -> anyhow::Result<bool> {
            let metadata = std::fs::symlink_metadata(path)?;
            for entry in std::fs::read_dir("/proc/self/fd")? {
                let entry = entry?;
                let Ok(open_metadata) = std::fs::metadata(entry.path()) else {
                    continue;
                };
                if open_metadata.dev() == metadata.dev() && open_metadata.ino() == metadata.ino() {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        /// A caller that did not pre-acquire must reserve capacity before the driver's fd-based
        /// authoritative classification, rather than opening a `Handle` first.
        #[tokio::test]
        async fn entry_acquires_capacity_before_classification() -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let leaf_path = root.join("leaf");
            tokio::fs::write(&leaf_path, b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let parent = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let leaves_seen = Arc::new(AtomicUsize::new(0));
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::clone(&leaves_seen),
                filter: None,
                authoritative_filter: false,
            });
            let stat_resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            admission.set_max_ops_in_flight(stat_resource, 1);
            let held_stat = throttle::ops_in_flight_permit(stat_resource).await;
            let cx = root_cx(parent, std::ffi::OsStr::new("leaf"), leaf_path);
            let operation = process_entry(visitor, cx, (), None);
            tokio::pin!(operation);
            let stopped_at_stat_gate = futures::poll!(operation.as_mut()).is_pending();
            let mut second_permit = Box::pin(throttle::pending_meta_permit());
            let classification_bypassed_admission =
                futures::poll!(second_permit.as_mut()).is_ready();
            drop(second_permit);
            drop(held_stat);
            let result = admission
                .run_with_timeout(Duration::from_secs(20), operation.as_mut())
                .await;
            assert!(
                stopped_at_stat_gate,
                "entry did not reach the held stat gate"
            );
            assert!(
                !classification_bypassed_admission,
                "entry reached fd-based classification before pending-metadata admission"
            );
            let summary = result
                .map_err(|_| anyhow::anyhow!("entry did not resume after capacity was released"))?
                .map_err(|error| error.source)?;
            assert_eq!(summary.files, 1);
            assert_eq!(leaves_seen.load(Ordering::SeqCst), 1);
            Ok(())
        }

        /// A `DT_UNKNOWN` entry that needs fd-based classification for filtering must
        /// obtain admission before that probe, even when the filter ultimately skips it.
        #[tokio::test]
        async fn filtered_unknown_entry_waits_before_authoritative_filter_probe()
        -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            tokio::fs::write(root.join("leaf"), b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            let dir = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let mut filter = FilterSettings::default();
            filter.add_exclude("leaf")?;
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::new(AtomicUsize::new(0)),
                filter: Some(filter),
                authoritative_filter: false,
            });
            let parent_cx = root_cx(Arc::clone(&dir), std::ffi::OsStr::new("root"), root.clone());
            admission.set_max_open_files(1);
            let stat_resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            admission.set_max_ops_in_flight(stat_resource, 1);
            let held_stat = throttle::ops_in_flight_permit(stat_resource).await;
            let walk = scope_tasks(walk_dir_entries(
                visitor,
                dir,
                &parent_cx,
                &(),
                vec![(std::ffi::OsString::from("leaf"), None)],
            ));
            tokio::pin!(walk);
            let stopped_at_stat_gate = futures::poll!(walk.as_mut()).is_pending();
            let mut second_permit = Box::pin(throttle::pending_meta_permit());
            let classification_bypassed_admission =
                futures::poll!(second_permit.as_mut()).is_ready();
            drop(second_permit);
            drop(held_stat);
            let result = admission
                .run_with_timeout(Duration::from_secs(20), walk.as_mut())
                .await;
            assert!(
                stopped_at_stat_gate,
                "DT_UNKNOWN filter probe did not reach the held stat gate"
            );
            assert!(
                !classification_bypassed_admission,
                "DT_UNKNOWN filter classification bypassed admission"
            );
            let (summary, processed) = result
                .map_err(|_| {
                    anyhow::anyhow!(
                        "filtered entry did not resume after stat capacity was released"
                    )
                })?
                .map_err(|error| error.source)?;
            assert_eq!(summary, CountSummary::default());
            assert!(
                processed.names().is_empty(),
                "filtered entry must not spawn"
            );
            Ok(())
        }

        /// An authoritative-filter visitor may reject a reliable hint without consuming capacity;
        /// only a provisionally included entry needs the admitted final decision.
        #[tokio::test]
        async fn authoritative_filter_rejects_reliable_hint_without_admission() -> anyhow::Result<()>
        {
            let root = crate::testutils::create_temp_dir().await?;
            tokio::fs::write(root.join("leaf"), b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let held = throttle::pending_meta_permit().await;
            let dir = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let mut filter = FilterSettings::default();
            filter.add_exclude("leaf")?;
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::new(AtomicUsize::new(0)),
                filter: Some(filter),
                authoritative_filter: true,
            });
            let parent_cx = root_cx(Arc::clone(&dir), OsStr::new("root"), root);
            let walk = scope_tasks(walk_dir_entries(
                visitor,
                dir,
                &parent_cx,
                &(),
                vec![(OsString::from("leaf"), Some(EntryKind::File))],
            ));
            tokio::pin!(walk);
            let first_poll = futures::poll!(walk.as_mut());
            let completed_while_saturated = first_poll.is_ready();
            drop(held);
            let result = match first_poll {
                std::task::Poll::Ready(result) => result,
                std::task::Poll::Pending => admission
                    .run_with_timeout(Duration::from_secs(20), walk.as_mut())
                    .await
                    .context("filtered hinted file did not resume after capacity was released")?,
            };
            let (summary, processed) = result.map_err(|error| error.source)?;
            assert!(
                completed_while_saturated,
                "an authoritative visitor's cheap filter skip waited for admission"
            );
            assert_eq!(summary.files, 0);
            assert!(processed.names().is_empty());
            Ok(())
        }

        /// A destructive filter must ignore a stale `d_type` before deciding to act.
        #[tokio::test]
        async fn destructive_filter_uses_transferred_authoritative_kind() -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            tokio::fs::create_dir(root.join("cache")).await?;
            tokio::fs::write(root.join("cache").join("child"), b"protected").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let dir = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let mut filter = FilterSettings::default();
            filter.add_exclude("cache/")?;
            let leaves_seen = Arc::new(AtomicUsize::new(0));
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::clone(&leaves_seen),
                filter: Some(filter),
                authoritative_filter: true,
            });
            let parent_cx = root_cx(Arc::clone(&dir), OsStr::new("root"), root);
            let (summary, processed) = admission
                .run_with_timeout(
                    Duration::from_secs(20),
                    scope_tasks(walk_dir_entries(
                        visitor,
                        dir,
                        &parent_cx,
                        &(),
                        vec![(OsString::from("cache"), Some(EntryKind::File))],
                    )),
                )
                .await
                .context("destructive stale-hint filter did not terminate")?
                .map_err(|error| error.source)?;
            assert_eq!(summary, CountSummary::default());
            assert!(processed.names().is_empty());
            assert_eq!(leaves_seen.load(Ordering::SeqCst), 0);
            Ok(())
        }

        /// A provisionally included directory hint cannot bypass admission before exact dispatch.
        #[tokio::test]
        async fn authoritative_filter_included_directory_hint_waits_for_admission()
        -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            tokio::fs::write(root.join("leaf"), b"x").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let held = throttle::pending_meta_permit().await;
            let dir = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let mut filter = FilterSettings::default();
            filter.add_exclude("protected/")?;
            let leaves_seen = Arc::new(AtomicUsize::new(0));
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::clone(&leaves_seen),
                filter: Some(filter),
                authoritative_filter: true,
            });
            let parent_cx = root_cx(Arc::clone(&dir), OsStr::new("root"), root);
            let walk = scope_tasks(walk_dir_entries(
                visitor,
                dir,
                &parent_cx,
                &(),
                vec![(OsString::from("leaf"), Some(EntryKind::Dir))],
            ));
            tokio::pin!(walk);
            let waited_for_admission = futures::poll!(walk.as_mut()).is_pending();
            drop(held);
            let (summary, processed) = admission
                .run_with_timeout(Duration::from_secs(20), walk.as_mut())
                .await
                .context("destructive stale-directory classification did not resume")?
                .map_err(|error| error.source)?;
            assert!(
                waited_for_admission,
                "authoritative filtering trusted an included directory hint outside admission"
            );
            assert_eq!(summary.files, 1);
            assert_eq!(summary.dirs, 0);
            assert_eq!(processed.names(), &[OsString::from("leaf")]);
            assert_eq!(leaves_seen.load(Ordering::SeqCst), 1);
            Ok(())
        }

        /// A transferred directory classification must release cap-one admission before descent.
        #[tokio::test]
        async fn destructive_filter_transfer_does_not_hold_admission_across_deep_tree()
        -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let depth = 12;
            let mut current = root.join("extra");
            tokio::fs::create_dir(&current).await?;
            for level in 0..depth {
                tokio::fs::write(current.join(format!("leaf-{level}")), b"x").await?;
                if level + 1 < depth {
                    current = current.join(format!("dir-{level}"));
                    tokio::fs::create_dir(&current).await?;
                }
            }
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let dir = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let mut filter = FilterSettings::default();
            filter.add_exclude("protected/")?;
            let leaves_seen = Arc::new(AtomicUsize::new(0));
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::clone(&leaves_seen),
                filter: Some(filter),
                authoritative_filter: true,
            });
            let parent_cx = root_cx(Arc::clone(&dir), OsStr::new("root"), root);
            let (summary, processed) = admission
                .run_with_timeout(
                    Duration::from_secs(20),
                    scope_tasks(walk_dir_entries(
                        visitor,
                        dir,
                        &parent_cx,
                        &(),
                        vec![(OsString::from("extra"), Some(EntryKind::File))],
                    )),
                )
                .await
                .context("destructive classified-directory transfer deadlocked at cap one")?
                .map_err(|error| error.source)?;
            assert_eq!(summary.files, depth);
            assert_eq!(summary.dirs, depth);
            assert_eq!(leaves_seen.load(Ordering::SeqCst), depth);
            assert_eq!(processed.names(), &[OsString::from("extra")]);
            Ok(())
        }

        /// A positive directory hint is the one classification allowed outside leaf admission.
        #[tokio::test]
        async fn hinted_directory_classifies_while_pool_is_saturated() -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            tokio::fs::create_dir(root.join("dir")).await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let held = throttle::pending_meta_permit().await;
            let parent = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::new(AtomicUsize::new(0)),
                filter: None,
                authoritative_filter: false,
            });
            let parent_cx = root_cx(Arc::clone(&parent), OsStr::new("root"), root);
            let result = admission
                .run_with_timeout(
                    Duration::from_secs(1),
                    scope_tasks(walk_dir_entries(
                        visitor,
                        parent,
                        &parent_cx,
                        &(),
                        vec![(OsString::from("dir"), Some(EntryKind::Dir))],
                    )),
                )
                .await;
            drop(held);
            let (summary, processed) = result
                .context("hinted directory waited for pending-metadata admission")?
                .map_err(|error| error.source)?;
            assert_eq!(summary.dirs, 1);
            assert_eq!(processed.names(), &[OsString::from("dir")]);
            Ok(())
        }

        /// A stale directory hint must close its first handle, admit, and classify the name again.
        #[tokio::test(flavor = "current_thread")]
        async fn stale_directory_hint_closes_admits_and_reclassifies() -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let entry = root.join("entry");
            tokio::fs::write(&entry, b"old").await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let held_leaf = throttle::pending_meta_permit().await;
            let parent = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let stat_resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            admission.set_max_ops_in_flight(stat_resource, 1);
            let held_stat = throttle::ops_in_flight_permit(stat_resource).await;
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::new(AtomicUsize::new(0)),
                filter: None,
                authoritative_filter: false,
            });
            let cx = root_cx(parent, OsStr::new("entry"), entry.clone());
            let mut task = tokio::spawn(process_entry(
                visitor,
                cx,
                (),
                walk::EntryAdmission::HintedDirectory,
            ));
            tokio::task::yield_now().await;
            drop(held_stat);
            let held_stat = throttle::ops_in_flight_permit(stat_resource).await;
            let mut first_handle_closed = false;
            for _ in 0..100 {
                if !inode_is_open(&entry)? {
                    first_handle_closed = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            tokio::fs::remove_file(&entry).await?;
            tokio::fs::create_dir(&entry).await?;
            drop(held_leaf);
            tokio::task::yield_now().await;
            drop(held_stat);
            let task_result = admission
                .run_with_timeout(Duration::from_secs(20), &mut task)
                .await
                .context("stale hinted entry did not resume after admission was released")?;
            let summary = task_result?.map_err(|error| error.source)?;
            assert!(
                first_handle_closed,
                "stale hinted-directory classification retained its first leaf handle while waiting"
            );
            assert_eq!(
                summary,
                CountSummary {
                    dirs: 1,
                    ..Default::default()
                },
                "the admitted reclassification must observe the replacement directory"
            );
            Ok(())
        }

        /// A failed authoritative filter probe is an operation error, not a cheap non-directory.
        #[tokio::test]
        async fn failed_authoritative_filter_probe_propagates() -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            let admission = crate::testutils::AdmissionLimit::new().await;
            let dir = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let mut filter = FilterSettings::default();
            filter.add_exclude("vanished")?;
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::new(AtomicUsize::new(0)),
                filter: Some(filter),
                authoritative_filter: false,
            });
            let parent_cx = root_cx(Arc::clone(&dir), OsStr::new("root"), root.clone());
            let result = admission
                .run_with_timeout(
                    Duration::from_secs(20),
                    scope_tasks(walk_dir_entries(
                        visitor,
                        dir,
                        &parent_cx,
                        &(),
                        vec![(OsString::from("vanished"), None)],
                    )),
                )
                .await
                .context("failed filter probe did not terminate")?;
            let error = result.expect_err("a missing authoritative probe target must fail");
            let rendered = format!("{:#}", error.source);
            assert!(rendered.contains("failed reading metadata from"));
            assert!(rendered.contains("vanished"));
            Ok(())
        }

        /// Driver-level regression for the drop-before-recurse invariant.
        ///
        /// With the pending-meta pool sized to a single permit, we pre-acquire that
        /// one permit and hand it to `process_entry` for an entry that is
        /// AUTHORITATIVELY a directory (mirroring the spawn loop's hinted-leaf
        /// pre-acquire when getdents mis-hints, or a swap between getdents and
        /// `child()`). The directory's child file then needs its own pending-meta
        /// permit to be visited.
        ///
        /// WITHOUT the fix, `process_entry` would hold that one permit across the
        /// recursion and the child's `preacquire_leaf_permit` would block forever
        /// (pool size 1, already held) — the timeout fires. WITH the fix, the
        /// directory branch drops the permit before recursing, the child acquires it,
        /// and the walk completes well within the timeout.
        #[tokio::test]
        async fn hinted_leaf_that_is_dir_drops_permit_before_recursion() -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            // `d` is a real directory holding one child file `c`.
            let dir_path = root.join("d");
            tokio::fs::create_dir(&dir_path).await?;
            tokio::fs::write(dir_path.join("c"), b"x").await?;
            // size the pending-meta pool to a single permit (the `set_max_open_files`
            // knob sizes both pools).
            let admission = crate::testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            // open the container of `d` and classify `d`: an authoritative directory.
            let parent = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let name = std::ffi::OsStr::new("d");
            let handle = parent.child(name).await?;
            assert_eq!(
                handle.kind(),
                EntryKind::Dir,
                "fixture `d` must be a directory"
            );
            drop(handle);
            let leaves_seen = Arc::new(AtomicUsize::new(0));
            let visitor = Arc::new(CountingVisitor {
                leaves_seen: Arc::clone(&leaves_seen),
                filter: None,
                authoritative_filter: false,
            });
            let cx = root_cx(Arc::clone(&parent), name, dir_path.clone());
            // pre-acquire the single permit exactly as the spawn loop does for a
            // hinted leaf, and hand it to `process_entry`. the fix drops it before
            // recursing.
            let permit =
                walk::preacquire_leaf_permit(PermitKind::PendingMeta, Some(EntryKind::File)).await;
            assert!(permit.is_some(), "the pre-acquire must take the one permit");
            let result = admission
                .run_with_timeout(
                    Duration::from_secs(20),
                    process_entry(visitor, cx, (), permit),
                )
                .await;
            let summary = result
                .map_err(|_| {
                    anyhow::anyhow!(
                        "process_entry hung — leaf permit held across directory recursion (deadlock)"
                    )
                })?
                .map_err(|e| e.source)?;
            assert_eq!(
                summary,
                CountSummary {
                    files: 1,
                    dirs: 1,
                    symlinks: 0,
                },
                "the directory and its one child file are both counted"
            );
            assert_eq!(
                leaves_seen.load(Ordering::SeqCst),
                1,
                "the child file was visited (its permit was acquired after the drop)"
            );
            Ok(())
        }
    }
}
