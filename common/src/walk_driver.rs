//! Generic single-tree safe-walk driver.
//!
//! This module owns the recursive directory-walk *skeleton* that copy, chmod, and
//! rm previously each hand-coded. A tool supplies a [`WalkVisitor`]; the driver
//! drives the traversal:
//!
//! 1. gated `read_entries` on the open hardened directory,
//! 2. per child: authoritative-or-hinted [`walk::filter_is_dir`] +
//!    [`walk::should_skip_entry`] filter decision (against the child's
//!    [`EntryCx::filter_path`]), then [`walk::preacquire_leaf_permit`] per the
//!    visitor's policy,
//! 3. acquire-then-spawn one task per non-skipped child, joining/folding via
//!    [`join_and_fold`] (NOT batched: see [`walk_dir_contents`] for why the permit
//!    is acquired and the task spawned in the same loop step),
//! 4. in each task: authoritative [`Dir::child`] classification, then either
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
//! per-tool walks do. A dropped surrounding future (timeout, `fail_early` abort,
//! Ctrl-C) therefore can never leave a spawned task holding a dangling borrow.
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
//!   `copy_symlink_fd`, `Special` → skip-or-error. The pre-acquired `permit` is
//!   the open-files guard `copy_file_fd` needs; it is dropped for symlink/special.
//!   `--dereference` of a symlink-to-dir stays inside `visit_leaf`: it drops the
//!   permit and calls the path-based `copy()` recursively (the one deliberately
//!   non-fd path), which the trait expresses fine — `visit_leaf` is a plain async
//!   fn that may itself recurse without going through the driver.
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
//! - **`permit_kind`** = `OpenFile`; **`want_permit`** = "hint is `File`" (copy
//!   only pre-acquires for a regular-file hint — symlinks may deref to dirs, and
//!   DT_UNKNOWN might be a dir).
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
use crate::progress::Progress;
use crate::safedir::{Dir, Handle};
use crate::walk::{self, EntryKind, LeafPermit, PermitKind};

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

    /// Whether to pre-acquire a leaf permit for a child with this `getdents`
    /// `d_type` `hint`. Must return `false` for a hinted directory (it would
    /// recurse and a held permit could deadlock); the canonical policy is
    /// "known non-directory only". `hint == None` (DT_UNKNOWN) is a tool choice.
    fn want_permit(&self, hint: Option<EntryKind>) -> bool;

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
    /// `kind` is the cheap `getdents`-hint classification (DT_UNKNOWN treated as a
    /// file), matching the per-tool walks' skip dispatch; `skip_result` is the
    /// `FilterResult` that caused the exclusion. The driver still increments the
    /// shared progress counter via [`EntryKind::inc_skipped`] — override only to
    /// add the summary counters and the `--dry-run` "skip …" line.
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

    /// Process a non-directory entry (file / symlink / special). `parent_ctx` is
    /// the inherited context of the directory containing this entry (copy's
    /// destination parent + freshness). The pre-acquired `permit` (if any) is held
    /// for the duration and dropped on return.
    fn visit_leaf(
        &self,
        cx: &EntryCx,
        parent_ctx: &Self::DirContext,
        handle: Handle,
        kind: EntryKind,
        permit: Option<LeafPermit>,
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
    /// case aborts the subtree immediately (the surrounding `JoinSet` drops, aborting
    /// siblings) and the error is returned without any post-order work — so a
    /// fail-early abort never applies post-order finalization (copy's directory
    /// metadata / `--delete` prune). When `fail_early` is unset, `dir_post` IS called
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
/// - **Non-directory:** call [`WalkVisitor::visit_leaf`] holding `permit`.
/// - **Directory:** `drop(permit)` — **the one and only drop-before-recurse
///   site** — then [`WalkVisitor::dir_pre`]; on [`DirAction::Descend`], walk the
///   contents via [`walk_dir_contents`] (threading the child context) and finish
///   with [`WalkVisitor::dir_post`].
///
/// `parent_ctx` is the inherited context of the directory that contains this
/// entry. `cx.parent` must be that (hardened) directory. On a classification
/// error the entry's own error is surfaced — the same fail-closed behavior the
/// per-tool walks have.
#[async_recursion]
pub async fn process_entry<V>(
    visitor: Arc<V>,
    cx: EntryCx,
    parent_ctx: V::DirContext,
    permit: Option<LeafPermit>,
) -> Result<V::Summary, OperationError<V::Summary>>
where
    V: WalkVisitor,
{
    let _ops_guard = cx.prog_track.ops.guard();
    // authoritative classification: one fstat, in one place. a symlink swap between
    // the getdents hint and here is caught (O_NOFOLLOW) and classified as Symlink.
    let handle = match cx.parent.child(&cx.name).await {
        Ok(handle) => handle,
        Err(err) => {
            let err = anyhow::Error::new(err)
                .context(format!("failed reading metadata from {:?}", &cx.real_path));
            return Err(OperationError::new(err, Default::default()));
        }
    };
    let kind = handle.kind();
    if kind != EntryKind::Dir {
        // leaf: the permit is held across the (non-recursive) leaf work and dropped
        // when `visit_leaf` returns. nothing below this can recurse.
        return visitor
            .visit_leaf(&cx, &parent_ctx, handle, kind, permit)
            .await;
    }
    // ── the single drop-before-recurse site ──────────────────────────────────
    // an authoritative directory recurses; its children acquire their own permits.
    // releasing the (only ever pre-acquired for a hinted leaf) permit now is what
    // makes the hold-and-wait deadlock structurally impossible.
    drop(permit);
    match visitor.dir_pre(&cx, &parent_ctx, &handle).await? {
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
                // a child failed. with `fail_early` the subtree is aborted immediately and NO
                // post-order work runs (siblings were aborted when the `JoinSet` dropped) — the
                // error propagates as-is. without `fail_early`, `dir_post` IS still invoked, with
                // the combined error, so the visitor can apply safe post-order finalization (copy's
                // directory metadata) while skipping destructive work (copy's `--delete` prune) and
                // then return the combined error. `processed` is not recoverable on the error path,
                // so an empty list is passed — the only consumer (a `--delete` keep-set) is skipped
                // on error anyway.
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
/// Enumerates `dir` (gated `read_entries`), applies the visitor's filter to each
/// child, pre-acquires a leaf permit per the visitor's policy, and spawns one
/// [`process_entry`] task per non-skipped child. Joins them with a fold +
/// fail-early via [`join_and_fold`].
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
#[async_recursion]
pub async fn walk_dir_contents<V>(
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
    let mut skipped_summary = V::Summary::default();
    let mut processed = ProcessedChildren::default();
    let mut join_set = tokio::task::JoinSet::new();
    for (entry_name, hint) in entries {
        // the FILTER `is_dir` decision uses the AUTHORITATIVE type when the getdents
        // hint is DT_UNKNOWN and a filter is active (one extra fstat only then, never
        // follows a symlink) — the single classification path that closes the
        // DT_UNKNOWN-omits-a-subtree bug class.
        // used only for the FILTER decision; the recurse-vs-leaf choice is made later from the
        // AUTHORITATIVE `child()` handle in `process_entry`, so there is no control-flow
        // dependence here that would need `force_authoritative`.
        let entry_is_dir =
            walk::filter_is_dir(visitor.filter(), &dir, &entry_name, hint, false).await;
        // build the child's owned context once; reused whether it is skipped or spawned.
        let child_cx = parent_cx.child(Arc::clone(&dir), &entry_name);
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
        // pre-acquire the leaf permit per the visitor's policy, then IMMEDIATELY spawn the task so
        // the held permit can be released by the running task (see the acquire-then-spawn note
        // above). a hinted directory takes none (it recurses); `process_entry` re-classifies and
        // drops it for a hinted leaf that turns out to be a directory.
        let permit =
            walk::preacquire_leaf_permit(visitor.permit_kind(), hint, |h| visitor.want_permit(h))
                .await;
        // own everything moved into the task (cancellation safety): the child context
        // (source parent Arc + owned name/paths), the visitor handle, and a clone of
        // the inherited context (copy's destination parent dir).
        let task_visitor = Arc::clone(&visitor);
        let task_ctx = dir_ctx.clone();
        processed.names.push(entry_name);
        join_set
            .spawn(async move { process_entry(task_visitor, child_cx, task_ctx, permit).await });
    }
    let folded =
        join_and_fold::<V::Summary>(join_set, visitor.fail_early(), skipped_summary).await?;
    Ok((folded, processed))
}

/// Join an already-populated `JoinSet` of per-child tasks and fold their summaries
/// with fail-early / error-collection semantics — the shared join engine behind
/// the directory walk.
///
/// The directory walk spawns into the `JoinSet` incrementally (acquire-then-spawn,
/// see [`walk_dir_contents`]) and hands it here so the held leaf permits are
/// released by running tasks rather than all held before the first task runs.
/// `base` seeds the fold (the walk passes the filter-skip contributions). On
/// `fail_early`, the first task error returns immediately, carrying the summary
/// accumulated so far; the remaining tasks are aborted when the `JoinSet` is
/// dropped. Otherwise all errors are collected and deduplicated, and the single
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
                    // dropping `join_set` here aborts the still-running children.
                    return Err(OperationError::new(error.source, summary));
                }
                errors.push(error.source);
            }
            Err(join_error) => {
                if fail_early {
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
    /// from the pending-meta pool, taken for known non-directory hints only.
    struct CountingVisitor {
        /// counts every spawned leaf, to prove `visit_leaf` ran under backpressure.
        leaves_seen: Arc<AtomicUsize>,
    }

    impl WalkVisitor for CountingVisitor {
        type Summary = CountSummary;
        type DirContext = ();
        type DirState = ();

        fn root_dir_context(&self) {}

        fn permit_kind(&self) -> PermitKind {
            PermitKind::PendingMeta
        }
        fn want_permit(&self, hint: Option<EntryKind>) -> bool {
            // known non-directory only; a hinted dir (or DT_UNKNOWN) takes none.
            hint.is_some_and(|k| k != EntryKind::Dir)
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
            _handle: Handle,
            kind: EntryKind,
            permit: Option<LeafPermit>,
        ) -> Result<CountSummary, OperationError<CountSummary>> {
            self.leaves_seen.fetch_add(1, Ordering::SeqCst);
            // hold the permit across the leaf work, then drop it (mirrors real tools).
            drop(permit);
            Ok(match kind {
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
            throttle::set_max_open_files(1);
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
            });
            let cx = root_cx(Arc::clone(&parent), name, dir_path.clone());
            // pre-acquire the single permit exactly as the spawn loop does for a
            // hinted leaf, and hand it to `process_entry`. the fix drops it before
            // recursing.
            let permit = walk::preacquire_leaf_permit(
                PermitKind::PendingMeta,
                Some(EntryKind::File),
                |_| true,
            )
            .await;
            assert!(permit.is_some(), "the pre-acquire must take the one permit");
            let result = tokio::time::timeout(
                Duration::from_secs(20),
                process_entry(visitor, cx, (), permit),
            )
            .await;
            // restore the default (disabled) pool before asserting so a failure can't
            // strand the tiny limit for a concurrent test.
            throttle::set_max_open_files(0);
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
