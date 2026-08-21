//! Generic single-tree safe-walk driver.
//!
//! This module owns the recursive directory-walk *skeleton* that copy, chmod, and
//! rm previously each hand-coded. A tool supplies a [`WalkVisitor`]; the driver
//! drives the traversal:
//!
//! 1. gated `read_entries` on the open hardened directory,
//! 2. per child: derive an explicit [`walk::EntryAdmission`] from its type hint,
//! 3. fallible authoritative-or-hinted [`walk::filter_is_dir`] +
//!    [`walk::should_skip_entry`] filter decision (against the child's
//!    [`EntryCx::filter_path`]); only a `DT_UNKNOWN` filter probe admits first,
//! 4. admit each remaining possible leaf immediately before spawn, joining/folding via
//!    [`join_and_fold`] (NOT batched: see [`walk_dir_contents`] for why the permit
//!    is acquired and the task spawned in the same loop step),
//! 5. in each task: authoritative [`Dir::child`] classification, then either
//!    [`WalkVisitor::visit_leaf`] (holding the permit) or — for a directory —
//!    **drop the permit**, [`WalkVisitor::dir_pre`], recurse, [`WalkVisitor::dir_post`].
//!
//! ## The single invariant home
//!
//! The "drop the leaf permit before recursing into a directory" invariant — the
//! root cause of the hold-and-wait deadlock class (see [`walk::LeafPermit`]) —
//! lives in **exactly one place**: the directory branch of [`process_entry`].
//! Leaves hold their permit across [`WalkVisitor::visit_leaf`]; the directory
//! branch `drop`s it before any further work. No visitor ever hand-drops a leaf
//! permit, so the invariant cannot silently migrate back to N parallel sites.
//!
//! ## Cancellation safety
//!
//! Spawned tasks must be `'static`, and `spawn_blocking` work is not cancellable,
//! so every per-entry context is **owned**: [`EntryCx`] clones `Arc<Dir>` plus
//! owned `OsString`/`PathBuf` rather than borrowing, exactly as the existing
//! per-tool walks do. A dropped surrounding future (timeout or Ctrl-C) therefore
//! can never leave a spawned task holding a dangling borrow. Fail-early traversal
//! aborts spawned siblings and its task scope waits for recursively owned work.
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
//!   (`src_meta` is taken from `dir_pre`'s classification `Handle`, no extra stat),
//!   run empty-dir cleanup (`dst_parent.rmdir_at(dst_name)`), and `--delete`-prune,
//!   plus the `base` create/unchanged contribution it folds with the children.
//! - **`visit_leaf`** dispatches on `kind`: `File` → `copy_file_fd`, `Symlink` →
//!   `copy_symlink_fd`, `Special` → skip-or-error. The admitted `permit` remains
//!   held across every non-recursive fd-bearing leaf operation.
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
    let task_tracker = tracker.clone();
    join_set.spawn(TASK_TRACKER.scope(tracker, async move {
        let _tracked = task_tracker.enter();
        future.await
    }));
}

/// Tracks non-cancellable blocking work in the current task scope, when present.
pub(crate) fn blocking_task_guard() -> Option<impl Drop + Send + 'static> {
    TASK_TRACKER
        .try_with(Clone::clone)
        .ok()
        .map(|tracker| tracker.enter())
}
use crate::progress::Progress;
use crate::safedir::{Dir, Handle};
use crate::walk::{self, AdmittedLeaf, EntryAdmission, EntryKind, PermitKind};

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

    /// Account for an entry the filter excluded, returning its summary
    /// contribution. Called by the driver for each filtered-out child *instead of*
    /// spawning it, so the tool's `*_skipped` counters and dry-run skip reporting
    /// stay tool-owned (the driver is generic over the summary and dry-run mode).
    ///
    /// `kind` comes from the cheap `getdents` hint when available. With an active
    /// filter, `DT_UNKNOWN` is resolved authoritatively as directory/non-directory
    /// before skip accounting. `skip_result` is the `FilterResult` that caused the
    /// exclusion. The driver still increments the shared progress counter via
    /// [`EntryKind::inc_skipped`] — override only to add the summary counters and
    /// the `--dry-run` "skip …" line.
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
    /// the enclosing task scope has quiesced, without any post-order work — so a
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
        admission.into(),
    ))
    .await
}

#[async_recursion]
async fn process_entry_tracked<V>(
    visitor: Arc<V>,
    cx: EntryCx,
    parent_ctx: V::DirContext,
    admission: EntryAdmission,
) -> Result<V::Summary, OperationError<V::Summary>>
where
    V: WalkVisitor,
{
    let _ops_guard = cx.prog_track.ops.guard();
    let mut admission = admission;
    // a positive directory hint gets one unadmitted classification. if stale, close that first
    // leaf handle before waiting for admission; final classification and leaf work then run inside
    // the newly admitted region below.
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

    enum AdmittedDispatch<S> {
        Leaf(Result<S, OperationError<S>>),
        Directory(Handle),
    }

    let handle = match hinted_directory {
        Some(handle) => {
            drop(admission);
            handle
        }
        None => {
            admission = walk::ensure_entry_admission(visitor.permit_kind(), admission).await;
            let weak_admission = admission.admission();
            let dispatch = crate::safedir::with_optional_fd_admission(weak_admission, async {
                // authoritative classification: one fstat, in one place. a symlink swap between
                // the getdents hint and here is caught (O_NOFOLLOW) and classified as Symlink.
                let handle = match walk::classify_entry(&cx.parent, &cx.name).await {
                    Ok(handle) => handle,
                    Err(err) => {
                        let err = anyhow::Error::new(err)
                            .context(format!("failed reading metadata from {:?}", &cx.real_path));
                        return AdmittedDispatch::Leaf(Err(OperationError::new(
                            err,
                            Default::default(),
                        )));
                    }
                };
                if handle.kind() != EntryKind::Dir {
                    let leaf = AdmittedLeaf::new(handle, admission.into_permit());
                    return AdmittedDispatch::Leaf(
                        visitor.visit_leaf(&cx, &parent_ctx, leaf).await,
                    );
                }
                // ── the single drop-before-recurse site ──────────────────────────────────────
                // release provisional admission inside its scope, then return only the directory
                // handle. ending the scope restores any outer pool before dir_pre can recurse.
                drop(admission);
                AdmittedDispatch::Directory(handle)
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
                // enclosing task scope quiesces. without `fail_early`, `dir_post` IS still invoked,
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
/// Enumerates `dir` (gated `read_entries`), admits only `DT_UNKNOWN` entries that need an
/// authoritative filter probe, applies the visitor's filter, and then admits each remaining
/// possible leaf immediately before spawning one [`process_entry`] task per non-skipped child.
/// Positive directory hints carry their explicit unadmitted state into classification. Tasks are
/// joined with a fold + fail-early via [`join_and_fold`].
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

/// Process an already-enumerated directory listing.
///
/// Keeping enumeration separate from admission/scheduling gives tests a deterministic way to
/// supply `DT_UNKNOWN` hints without requiring a particular filesystem. Production callers always
/// arrive through [`walk_dir_contents`].
async fn walk_dir_entries<V>(
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
        let mut admission = EntryAdmission::from_hint(hint);
        // build the child's owned context once; reused whether it is skipped or spawned, and gives
        // an authoritative probe failure its operation-path context.
        let child_cx = parent_cx.child(Arc::clone(&dir), &entry_name);
        // a DT_UNKNOWN filter decision opens an O_PATH handle, so it is the only filter path that
        // acquires before filtering. reliable hints keep cheap skips outside leaf admission.
        if hint.is_none() && visitor.filter().is_some() {
            admission = walk::ensure_entry_admission(visitor.permit_kind(), admission).await;
        }
        // the FILTER `is_dir` decision uses the AUTHORITATIVE type when the getdents
        // hint is DT_UNKNOWN and a filter is active (one extra fstat only then, never
        // follows a symlink) — the single classification path that closes the
        // DT_UNKNOWN-omits-a-subtree bug class.
        // used only for the FILTER decision; the recurse-vs-leaf choice is made later from the
        // AUTHORITATIVE `child()` handle in `process_entry`, so there is no control-flow
        // dependence here that would need `force_authoritative`.
        let entry_is_dir = match crate::safedir::with_optional_fd_admission(
            admission.admission(),
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
        };
        if let Some(skip_result) =
            walk::should_skip_entry_ref(visitor.filter(), &child_cx.filter_path, entry_is_dir)
        {
            // classification for the skipped-counter dispatch and the visitor's skip accounting
            // uses the getdents hint, but for DT_UNKNOWN (`None`) falls back to the AUTHORITATIVE
            // dir/non-dir decision already computed above for the filter. this branch only runs
            // with an active filter, so `entry_is_dir` is the fstat-resolved value (no extra
            // syscall) — matching the per-tool walks, which dispatched on the authoritative
            // `file_type()`, so a real directory reported as DT_UNKNOWN is counted as
            // `directories_skipped`, not `files_skipped`. (A DT_UNKNOWN symlink still counts as a
            // file here; the subtree-scale dir mis-count is the one that matters.) The driver does
            // the shared progress increment; the visitor's `on_skip` does the tool-specific summary
            // + dry-run reporting.
            let entry_kind = hint.unwrap_or(if entry_is_dir {
                EntryKind::Dir
            } else {
                EntryKind::File
            });
            tracing::debug!("skipping {:?} due to filter", &child_cx.real_path);
            entry_kind.inc_skipped(parent_cx.prog_track);
            skipped_summary =
                skipped_summary + visitor.on_skip(&child_cx, entry_kind, &skip_result);
            continue;
        }
        // every included possible leaf acquires immediately before spawn. a positive directory
        // hint retains its explicit exception state into the worker.
        admission = walk::ensure_entry_admission(visitor.permit_kind(), admission).await;
        // own everything moved into the task (cancellation safety): the child context
        // (source parent Arc + owned name/paths), the visitor handle, and a clone of
        // the inherited context (copy's destination parent dir).
        let task_visitor = Arc::clone(&visitor);
        let task_ctx = dir_ctx.clone();
        processed.names.push(entry_name);
        spawn_tracked(
            &mut join_set,
            process_entry(task_visitor, child_cx, task_ctx, admission),
        );
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
/// task scope waits for recursively owned tasks and blocking work to finish
/// dropping before the operation returns.
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

    #[tokio::test(flavor = "current_thread")]
    async fn fail_early_cancels_and_quiesces_nested_sibling_work() {
        struct DropNotice(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for DropNotice {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, mut dropped_rx) = tokio::sync::oneshot::channel();
        let nested_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_completed = Arc::clone(&nested_completed);
        let blocking_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let blocking_task_completed = Arc::clone(&blocking_completed);
        let result = scope_tasks(async move {
            let mut join_set = tokio::task::JoinSet::new();
            spawn_tracked(&mut join_set, async move {
                let mut nested = tokio::task::JoinSet::new();
                spawn_tracked(&mut nested, async move {
                    let _drop_notice = DropNotice(Some(dropped_tx));
                    let task_guard = blocking_task_guard().expect("nested task must be tracked");
                    let _blocking_task = tokio::task::spawn_blocking(move || {
                        std::thread::sleep(Duration::from_millis(50));
                        blocking_task_completed.store(true, std::sync::atomic::Ordering::SeqCst);
                        drop(task_guard);
                    });
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
        assert!(
            blocking_completed.load(std::sync::atomic::Ordering::SeqCst),
            "fail-early returned before non-cancellable nested work finished"
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
            Ok((admission, task, raw_fd, release))
        }

        fn fd_is_closed(raw_fd: std::os::fd::RawFd) -> bool {
            let result = unsafe { libc::fcntl(raw_fd, libc::F_GETFD) };
            result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
        }

        #[tokio::test(flavor = "current_thread")]
        async fn successful_leaf_keeps_admission_until_its_handle_closes() -> anyhow::Result<()> {
            let (_admission, task, raw_fd, release) =
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
                fd_is_closed(raw_fd),
                "successful leaf returned with its classification handle open"
            );
            let permit = tokio::time::timeout(Duration::from_secs(1), next_permit)
                .await
                .context("successful leaf did not return admission")?;
            drop(permit);
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn failed_leaf_keeps_admission_until_its_handle_closes() -> anyhow::Result<()> {
            let (_admission, task, raw_fd, release) =
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
                fd_is_closed(raw_fd),
                "failed leaf returned with its classification handle open"
            );
            let permit = tokio::time::timeout(Duration::from_secs(1), next_permit)
                .await
                .context("failed leaf did not return admission")?;
            drop(permit);
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn cancelled_leaf_keeps_admission_until_its_handle_closes() -> anyhow::Result<()> {
            let (_admission, task, raw_fd, _release) =
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
                fd_is_closed(raw_fd),
                "cancelled leaf retained its classification handle"
            );
            let permit = tokio::time::timeout(Duration::from_secs(1), next_permit)
                .await
                .context("cancelled leaf did not return admission")?;
            drop(permit);
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

        /// A reliable file hint lets filtering reject the entry without consuming leaf capacity.
        #[tokio::test]
        async fn filtered_hinted_file_does_not_wait_for_admission() -> anyhow::Result<()> {
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
                "a cheap filter skip waited for pending-metadata admission"
            );
            assert_eq!(summary.files, 0);
            assert!(processed.names().is_empty());
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
