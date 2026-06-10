use anyhow::{Context, anyhow};
use async_recursion::async_recursion;
use std::sync::Arc;
use tracing::instrument;

use crate::copy;
use crate::copy::{
    EmptyDirAction, Settings as CopySettings, Summary as CopySummary, check_empty_dir_cleanup,
};
use crate::filecmp;
use crate::preserve;
use crate::progress;
use crate::safedir::Dir;
use crate::walk::{self, EntryKind, LeafPermit, PermitKind};

/// Error type for link operations. See [`crate::error::OperationError`] for
/// logging conventions and rationale.
pub type Error = crate::error::OperationError<Summary>;

#[derive(Debug, Clone)]
pub struct Settings {
    pub copy_settings: CopySettings,
    pub update_compare: filecmp::MetadataCmpSettings,
    pub update_exclusive: bool,
    /// filter settings for include/exclude patterns
    pub filter: Option<crate::filter::FilterSettings>,
    /// dry-run mode for previewing operations
    pub dry_run: Option<crate::config::DryRunMode>,
    /// metadata preservation settings
    pub preserve: preserve::Settings,
}

/// Summary with the appropriate `*_skipped` counter set to 1 for the given entry kind.
/// Special files count as `files_skipped` to match the historical mapping used
/// when filters skip an entry (`specials_skipped` is reserved for `--skip-specials`).
fn skipped_summary_for(kind: EntryKind) -> Summary {
    let copy_summary = match kind {
        EntryKind::Dir => CopySummary {
            directories_skipped: 1,
            ..Default::default()
        },
        EntryKind::Symlink => CopySummary {
            symlinks_skipped: 1,
            ..Default::default()
        },
        EntryKind::File | EntryKind::Special => CopySummary {
            files_skipped: 1,
            ..Default::default()
        },
    };
    Summary {
        copy_summary,
        ..Default::default()
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Summary {
    pub hard_links_created: usize,
    pub hard_links_unchanged: usize,
    pub copy_summary: CopySummary,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            hard_links_created: self.hard_links_created + other.hard_links_created,
            hard_links_unchanged: self.hard_links_unchanged + other.hard_links_unchanged,
            copy_summary: self.copy_summary + other.copy_summary,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}\n\
            link:\n\
            -----\n\
            hard-links created: {}\n\
            hard links unchanged: {}\n",
            &self.copy_summary, self.hard_links_created, self.hard_links_unchanged
        )
    }
}

/// Hard-link the already-classified source entry (pinned by `src_handle`) to `dst_name` within
/// `dst_dir`, fd-relative and inode-exact.
///
/// `dst_dir.hard_link_handle_at` links the EXACT inode `src_handle` pins (via its `O_PATH` fd,
/// using `linkat(.., "/proc/self/fd/N", .., AT_SYMLINK_FOLLOW)`) rather than re-resolving the
/// source by name. This closes a TOCTOU window the old by-name `linkat` had: on an actor-writable
/// source, `name` could be swapped to a different inode (symlink, FIFO, another file) between
/// classification and the link, so the by-name link would target the replacement while rlink
/// reported a hard-linked file. Linking the pinned inode means we either hard-link the exact
/// regular file we classified or fail closed (`ENOENT` when its last link was removed) — never the
/// swapped-in replacement. `linkat` still refuses to hard-link a directory (`EPERM`).
///
/// On `EEXIST` under `--overwrite`, the existing destination is re-classified through `dst_dir`'s
/// fd and, if it is an identical hard link (same dev+ino), left as is; otherwise it is removed via
/// the recheck-guarded [`copy::remove_existing`] and the link is retried — mirroring copy's
/// fd-relative overwrite branches.
#[instrument(skip(prog_track, settings))]
#[allow(clippy::too_many_arguments)]
async fn hard_link_entry_fd(
    prog_track: &'static progress::Progress,
    src_handle: &crate::safedir::Handle,
    dst_dir: &Arc<Dir>,
    dst_name: &std::ffi::OsStr,
    dst_path: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    let mut link_summary = Summary::default();
    match dst_dir.hard_link_handle_at(src_handle, dst_name).await {
        Ok(()) => {}
        Err(error)
            if settings.copy_settings.overwrite
                && error.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            tracing::debug!("'dst' already exists, check if we need to update");
            let dst_handle = dst_dir
                .child(dst_name)
                .await
                .with_context(|| format!("cannot read {dst_path:?} metadata"))
                .map_err(|err| Error::new(err, Default::default()))?;
            // identical hard link: same file type and same (dev, ino) as the source entry. Both
            // handles pin their inodes (O_PATH), so a matching (dev, ino) genuinely proves the two
            // names already resolve to the same inode — no change needed.
            if dst_handle.kind() == src_handle.kind()
                && dst_handle.dev() == src_handle.dev()
                && dst_handle.ino() == src_handle.ino()
            {
                tracing::debug!("no change, leaving file as is");
                prog_track.hard_links_unchanged.inc();
                return Ok(Summary {
                    hard_links_unchanged: 1,
                    ..Default::default()
                });
            }
            tracing::info!("'dst' file type changed, removing and hard-linking");
            // recheck-guarded, fd-relative removal contained to dst_dir (mirrors copy.rs).
            let rm_summary = copy::remove_existing(
                prog_track,
                dst_dir,
                dst_name,
                dst_path,
                &dst_handle,
                &settings.copy_settings,
            )
            .await
            .map_err(|err| {
                link_summary.copy_summary.rm_summary = err.summary.rm_summary;
                Error::new(err.source, link_summary)
            })?;
            link_summary.copy_summary.rm_summary = rm_summary;
            dst_dir
                .hard_link_handle_at(src_handle, dst_name)
                .await
                .with_context(|| format!("failed to hard link to {dst_path:?}"))
                .map_err(|err| Error::new(err, link_summary))?;
        }
        Err(error) => {
            return Err(Error::new(
                anyhow::Error::from(error).context(format!("failed to hard link to {dst_path:?}")),
                link_summary,
            ));
        }
    }
    prog_track.hard_links_created.inc();
    link_summary.hard_links_created = 1;
    Ok(link_summary)
}

/// Public entry point for link operations.
///
/// The dual-tree link walk is fd-based: the source, optional `update`, and destination roots are
/// opened relative to their parent directories and every per-entry operation is performed through
/// file-descriptor-relative syscalls (see [`crate::safedir`]). Hard links are made inode-exact
/// through the already-classified source `Handle` (`linkat` via `/proc/self/fd/N` with
/// `AT_SYMLINK_FOLLOW`), so the link targets the exact regular file that was classified, even if
/// its directory entry is concurrently swapped — never a re-resolved name; entries that must be
/// copied instead of hard-linked are delegated to `copy::copy_child` with the held parent `Dir`s —
/// no path is re-resolved from a root. This closes the TOCTOU window the old path-based walk had
/// between classifying an entry and acting on it. `--dereference` is the one exception — copy still
/// resolves symlinks by path (`canonicalize`) and is not hardened.
#[instrument(skip(prog_track, settings))]
pub async fn link(
    prog_track: &'static progress::Progress,
    cwd: &std::path::Path,
    src: &std::path::Path,
    dst: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &Settings,
    is_fresh: bool,
) -> Result<Summary, Error> {
    // `cwd` is retained for API/signature parity (callers still pass it) but the fd-based walk
    // reconstructs every path from the explicit roots, so it is no longer threaded into the walk.
    let _ = cwd;
    // A missing --update root is destructive under both --update-exclusive (materialized set =
    // update set, so nothing materializes) AND --delete (the source-only keep_set makes any dst
    // entry the missing update tree WOULD have protected look extraneous, and prune wipes it).
    // In either case `link_internal` hits the recursive early-return / silent `None` fallback
    // before that destruction would happen, so rlink reports success — silently preserving
    // stale dst (--update-exclusive) or silently pruning would-be-protected entries (--delete).
    // Reject at the public entry so a typo'd --update can't quietly do the wrong thing. The
    // plain "--update without --delete or --update-exclusive" case still falls back to no-update
    // mode (long-standing behavior), and recursive child-level "update missing" cases stay
    // handled inside link_internal — they correctly no-op so the parent's prune removes their
    // dst counterpart per the documented semantics.
    if let Some(update_path) = update.as_ref()
        && (settings.update_exclusive || settings.copy_settings.delete.is_some())
    {
        match crate::walk::run_metadata_probed(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            tokio::fs::symlink_metadata(update_path),
        )
        .await
        {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::new(
                    anyhow!(
                        "--update path {:?} does not exist (rejected under --delete or --update-exclusive to avoid silently pruning destination entries the update tree would otherwise have preserved)",
                        update_path
                    ),
                    Default::default(),
                ));
            }
            Err(err) => {
                return Err(Error::new(
                    anyhow::Error::new(err).context(format!(
                        "failed reading metadata from update {:?}",
                        update_path
                    )),
                    Default::default(),
                ));
            }
        }
    }
    // Source: decompose via the shared helper so `.`/`..` operands (e.g. `rlink . dst`, `rlink
    // tree/.. dst`) are canonicalized to a real directory + basename instead of being rejected; `/`
    // is still rejected. (The destination and `--update` operands keep their direct split below.)
    let src_operand = crate::walk::split_root_operand(src)
        .await
        .map_err(|err| Error::new(err, Default::default()))?;
    let src = src_operand.display.as_path();
    let src_name = src_operand.name.as_os_str();
    // check filter for top-level source (files, directories, and symlinks)
    if let Some(ref filter) = settings.filter {
        let src_metadata = crate::walk::run_metadata_probed(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            tokio::fs::symlink_metadata(src),
        )
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))
        .map_err(|err| Error::new(err, Default::default()))?;
        let is_dir = src_metadata.is_dir();
        let result = filter.should_include_root_item(std::path::Path::new(src_name), is_dir);
        match result {
            crate::filter::FilterResult::Included => {}
            result => {
                let kind = EntryKind::from_metadata(&src_metadata);
                if let Some(mode) = settings.dry_run {
                    crate::dry_run::report_skip(src, &result, mode, kind.label_long());
                }
                kind.inc_skipped(prog_track);
                return Ok(skipped_summary_for(kind));
            }
        }
    }
    // Open the parent directories of the source, destination, and (optional) update roots so each
    // root entry is opened and classified relative to a directory fd — the same fd-relative path
    // every nested entry takes. The roots are then handed to `link_internal` by their basenames,
    // exactly like child entries. The source was decomposed above (via `split_root_operand`, which
    // canonicalizes `.`/`..` and rejects `/`); the destination keeps the direct split — a `.`/`..`
    // destination is not a meaningful link target, and rejecting it avoids clobbering the cwd.
    let (Some(dst_parent_path), Some(_dst_name)) = (dst.parent(), dst.file_name()) else {
        return Err(Error::new(
            anyhow!(
                "link destination {:?} has no parent directory or file name",
                dst
            ),
            Default::default(),
        ));
    };
    // empty parent (relative path with a single component) means the current directory.
    let resolve_parent = |p: &std::path::Path| -> std::path::PathBuf {
        if p.as_os_str().is_empty() {
            std::path::PathBuf::from(".")
        } else {
            p.to_path_buf()
        }
    };
    // the helper already normalized the source's empty parent to ".".
    let src_parent_path = src_operand.parent.clone();
    let dst_parent_path = resolve_parent(dst_parent_path);
    // open the operand's TRUSTED parent prefix following symlinks normally (the prefix is trusted
    // up to and including the operand's container — only entries strictly below the named root are
    // O_NOFOLLOW-hardened). a symlinked parent (e.g. `rlink symlinkdir/src dst`) is followed.
    let src_parent = Dir::open_parent_dir(&src_parent_path, congestion::Side::Source)
        .await
        .with_context(|| format!("cannot open source parent directory {:?}", src_parent_path))
        .map_err(|err| Error::new(err, Default::default()))?;
    // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
    let src_parent = Arc::new(src_parent.into_tree());
    // In dry-run we never touch the destination, so we don't open its parent at all (it may not
    // even exist). `dst_parent == None` is the signal throughout the walk that destination
    // operations must be skipped.
    let dst_parent = if settings.dry_run.is_some() {
        None
    } else {
        // the destination's TRUSTED parent prefix is resolved following symlinks (see the source
        // parent above): a symlinked destination container must be followed into the real dir.
        let dir = Dir::open_parent_dir(&dst_parent_path, congestion::Side::Destination)
            .await
            .with_context(|| {
                format!(
                    "cannot open destination parent directory {:?}",
                    dst_parent_path
                )
            })
            .map_err(|err| Error::new(err, Default::default()))?;
        // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
        Some(Arc::new(dir.into_tree()))
    };
    // The update tree (if present) is rooted at `update`; open its parent and remember the root
    // basename so `link_internal` can classify it via the held fd. A missing update root is handled
    // inside `link_internal` (recursive early-return / silent None fallback) exactly as before.
    //
    // For plain `--update` (no `--delete`, no `--update-exclusive`) a missing parent is treated the
    // same as a missing update root: fall back silently to no-update mode. This preserves the long-
    // standing behavior where `rlink --update /tmp/no/such src dst` (with `/tmp/no` absent) proceeds
    // by linking from `src` rather than erroring — the existing missing-update-root fallback already
    // applies to that case; the parent-open merely must not ENOENT-fail before `link_internal` can
    // apply it. Under `--delete` or `--update-exclusive` the missing-root is already rejected above,
    // so those cases never reach here with a missing update; any open_parent_dir error there is
    // unexpected and propagates as before.
    let update_parent = match update.as_ref() {
        Some(update_path) => {
            // decompose the update operand the same way as the source: the update tree is a READ
            // tree, so `.`/`..`/`dir/..` are meaningful and `split_root_operand` canonicalizes them
            // (and rejects `/`). This makes `rlink --update . src dst` / `--update tree/.. src dst`
            // work instead of erroring, matching the source-operand handling. The helper already
            // normalizes an empty parent to ".", so no `resolve_parent` is needed here.
            let update_operand = crate::walk::split_root_operand(update_path)
                .await
                .map_err(|err| Error::new(err, Default::default()))?;
            let update_parent_path = update_operand.parent;
            let update_name = update_operand.name;
            // the update tree's TRUSTED parent prefix is resolved following symlinks (see the
            // source parent above): a symlinked update container must be followed into the real dir.
            let fallback_eligible =
                !settings.update_exclusive && settings.copy_settings.delete.is_none();
            match Dir::open_parent_dir(&update_parent_path, congestion::Side::Source).await {
                // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below).
                Ok(dir) => Some((Arc::new(dir.into_tree()), update_name)),
                Err(err)
                    if fallback_eligible
                        && (matches!(
                            err.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                        ) || err.raw_os_error() == Some(libc::ENOTDIR)) =>
                {
                    // the update path's parent (or an ancestor) doesn't exist — treat the whole
                    // update tree as absent and fall back to no-update mode, exactly as when the
                    // update ROOT itself is missing (handled inside link_internal).
                    tracing::debug!(
                        "update parent {:?} not found ({:#}); falling back to no-update mode",
                        update_parent_path,
                        err
                    );
                    None
                }
                Err(err) => {
                    return Err(Error::new(
                        anyhow::Error::new(err).context(format!(
                            "cannot open update parent directory {:?}",
                            update_parent_path
                        )),
                        Default::default(),
                    ));
                }
            }
        }
        None => None,
    };
    let update_ref = update_parent
        .as_ref()
        .map(|(dir, name)| (dir, name.as_os_str()));
    link_internal(
        prog_track,
        &src_parent,
        update_ref,
        dst_parent.as_ref(),
        src_name,
        src,
        dst,
        update.as_deref(),
        std::path::Path::new(""),
        settings,
        is_fresh,
        None,
    )
    .await
}
/// Tracks which child names will be materialized at the destination for a single directory
/// pass, used by `--delete` to decide what to prune. Operations are named after their
/// semantic intent so the call sites don't repeat the gating conditions (delete-on-vs-off,
/// `--update-exclusive` carve-out, skip-special-vs-real materialization).
///
/// When `--delete` is off the inner set is `None` and every method is a no-op — zero heap
/// cost in the hot path.
struct DeleteKeepSet {
    inner: Option<std::collections::HashSet<std::ffi::OsString>>,
    /// Under `--update-exclusive` with an active update tree, the source loop must NOT
    /// register source-only entries — only the update set materializes.
    src_records_disabled: bool,
}

impl DeleteKeepSet {
    fn new(
        delete: Option<&copy::DeleteSettings>,
        update_exclusive: bool,
        update_present: bool,
    ) -> Self {
        Self {
            inner: delete.is_some().then(std::collections::HashSet::new),
            src_records_disabled: update_exclusive && update_present,
        }
    }
    /// Source loop: this src entry passed the filter. Called even when `--skip-specials`
    /// will skip materialization — the dst counterpart still needs to be retained.
    fn record_src(&mut self, name: &std::ffi::OsStr) {
        if let Some(set) = &mut self.inner
            && !self.src_records_disabled
        {
            set.insert(name.to_owned());
        }
    }
    /// Update loop: this update entry passed the filter at its logical path.
    fn record_update(&mut self, name: &std::ffi::OsStr) {
        if let Some(set) = &mut self.inner {
            set.insert(name.to_owned());
        }
    }
    /// Borrow the underlying set for `prune_extraneous`. `None` means `--delete` is off and
    /// the caller should skip the prune entirely.
    fn as_set(&self) -> Option<&std::collections::HashSet<std::ffi::OsString>> {
        self.inner.as_ref()
    }
}

/// Per-entry worker of the fd-based dual-tree link walk.
///
/// `src_parent` is the open source directory holding `name`; `update` is `Some((dir, name))` when
/// an update tree is present at this level (its directory handle plus the update root's basename
/// for the root entry); `dst_parent` is the open destination directory (`None` in dry-run).
/// `src_root`/`dst_root`/`update_root` are the user-specified roots and `rel_path` is this entry's
/// path relative to those roots (empty for the root entry) — joined onto a root they reconstruct
/// the real path used for diagnostics, `rm`, and `--dereference`. `rel_path` is also this entry's
/// logical filter path.
///
/// The src entry is classified via `src_parent.child(name)` (fstat-authoritative; the getdents
/// hint is only a spawn-loop heuristic). When an update entry exists at this name it is classified
/// too, and the hard-link-vs-copy decision mirrors the old `--update` overlay logic exactly:
/// a type-mismatch / changed file / symlink in the update tree is COPIED from the update version
/// via [`copy::copy_child`]; an unchanged file is HARD-LINKED from the source; a directory recurses
/// the dual tree. With no update tree, a source file is hard-linked, a source symlink is copied,
/// and a directory recurses.
///
/// `permit` is the leaf permit the spawn loop pre-acquired for a regular-file src hint (via
/// [`walk::preacquire_leaf_permit`]). It is USED only on the one path that copies a *changed*
/// same-type regular file from the update tree (the data copy reuses it); on every other path it is
/// dropped at the single consolidated drop site below — see that comment for why this is rlink's
/// acknowledged dual-tree special case.
#[instrument(skip(prog_track, src_parent, update, dst_parent, settings, permit))]
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn link_internal(
    prog_track: &'static progress::Progress,
    src_parent: &Arc<Dir>,
    update: Option<(&Arc<Dir>, &std::ffi::OsStr)>,
    dst_parent: Option<&Arc<Dir>>,
    name: &std::ffi::OsStr,
    src_root: &std::path::Path,
    dst_root: &std::path::Path,
    update_root: Option<&std::path::Path>,
    rel_path: &std::path::Path,
    settings: &Settings,
    is_fresh: bool,
    permit: Option<LeafPermit>,
) -> Result<Summary, Error> {
    let _prog_guard = prog_track.ops.guard();
    // real filesystem paths reconstructed from the roots + accumulated relative path. used for
    // diagnostics, the path-based `--delete` prune scan / `rm`, the `--dereference` canonicalize
    // fallback inside copy, and to derive `dst_name`. joining an empty `rel_path` (the root entry)
    // would append a trailing separator, so use the root verbatim when `rel_path` is empty.
    let (src_path, dst_path) = if rel_path.as_os_str().is_empty() {
        (src_root.to_path_buf(), dst_root.to_path_buf())
    } else {
        (src_root.join(rel_path), dst_root.join(rel_path))
    };
    let update_path = update_root.map(|root| {
        if rel_path.as_os_str().is_empty() {
            root.to_path_buf()
        } else {
            root.join(rel_path)
        }
    });
    // the destination entry's name within `dst_parent`. for nested entries this equals the source
    // `name`, but for the root the source and destination basenames differ (e.g. linking `foo` to
    // `bar`), so destination operations must use this name.
    let dst_name = dst_path
        .file_name()
        .ok_or_else(|| {
            Error::new(
                anyhow!("link destination {:?} has no file name", &dst_path),
                Default::default(),
            )
        })?
        .to_owned();
    tracing::debug!("classifying source entry");
    let src_handle = src_parent
        .child(name)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src_path))
        .map_err(|err| Error::new(err, Default::default()))?;
    // classify the update entry at this name (if an update tree is present at this level). a
    // NotFound is the "this path is missing from update" case; under --update-exclusive it means
    // we're done (nothing materializes), otherwise we fall back to no-update mode for this entry.
    let mut update_handle = match update {
        Some((update_dir, update_name)) => {
            tracing::debug!("classifying 'update' entry");
            match update_dir.child(update_name).await {
                Ok(handle) => Some(handle),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if settings.update_exclusive {
                        // the path is missing from update, we're done
                        return Ok(Default::default());
                    }
                    None
                }
                Err(error) => {
                    return Err(Error::new(
                        anyhow::Error::new(error)
                            .context(format!("failed reading metadata from {:?}", &update_path)),
                        Default::default(),
                    ));
                }
            }
        }
        None => None,
    };
    // Re-evaluate the filter using the UPDATE entry's authoritative type before letting it drive
    // the materialization decision below. The spawn loop in `link_dir_contents` evaluated the
    // filter against the SOURCE entry's type; when the same name is a TYPE MISMATCH (e.g. src
    // `cache` is a file, update `cache` is a directory) a type-dependent pattern like the dir-only
    // `cache/` can pass the src (a dir-only pattern doesn't match a file) yet exclude the update.
    // Without this check the type-mismatch branch would `delegate_copy` the excluded update entry
    // (`copy_child` does NOT re-apply the top-level filter to the delegated root), copying an
    // excluded subtree. Note a filtered-out update reaching here is necessarily a type mismatch: if
    // the pattern were type-independent it would have excluded the src too, so the src loop would
    // not have spawned this worker.
    if let Some(handle) = update_handle.as_ref() {
        let update_is_dir = handle.kind() == EntryKind::Dir;
        // For the ROOT entry (`rel_path` empty) judge the update side with root-item semantics —
        // symmetric with how the source root was filtered (`should_include_root_item` at the top of
        // `link`) and with how main's delegated copy filtered the update root — rather than the
        // nested `should_include("")` path, which would judge the root by different rules than the
        // source (e.g. a non-anchored `--include data` includes a root file `data` under root-item
        // semantics but not under the nested empty-path check). Nested entries keep the accumulated
        // `rel_path` with the normal nested filter.
        let update_excluded = match settings.filter.as_ref() {
            Some(filter) if rel_path.as_os_str().is_empty() => {
                let (_, update_name) =
                    update.expect("update_handle is Some only when update is Some");
                !matches!(
                    filter.should_include_root_item(
                        std::path::Path::new(update_name),
                        update_is_dir,
                    ),
                    crate::filter::FilterResult::Included
                )
            }
            _ => walk::should_skip_entry(&settings.filter, rel_path, update_is_dir).is_some(),
        };
        if update_excluded {
            if settings.update_exclusive {
                // --update-exclusive materializes only the (filter-passing) update set, so an
                // excluded update entry materializes nothing — exactly the NotFound case above.
                // The src is not materialized; under --delete its keep-set entry was never recorded
                // (`record_src` is a no-op when `src_records_disabled`), so the dst counterpart is
                // pruned/exclude-protected by the prune scan, never materialized-then-pruned.
                tracing::debug!(
                    "update entry {:?} is filtered out under --update-exclusive; materializing nothing",
                    update_path
                );
                return Ok(Default::default());
            }
            // Normal --update (union): the update's version of this name is excluded, so the src's
            // version stands. Treat the update entry as absent and fall through to the no-update
            // src handling (hard-link a src file, copy a src symlink, recurse a src dir). The src
            // already passed its own filter in the spawn loop, so its --delete keep-set entry
            // correctly stays recorded.
            tracing::debug!(
                "update entry {:?} is filtered out; falling back to source-only handling",
                update_path
            );
            update_handle = None;
        }
    }
    // ── rlink's acknowledged dual-tree special case (design spec §4): the single permit-drop site ──
    //
    // The spawn loop in `link_dir_contents` pre-acquired this leaf permit (open-files pool) for a
    // regular-file src `hint`, but the authoritative dual-tree decision below may instead COPY
    // (possibly recursing, when the update entry is a directory), HARD-LINK, SKIP, or recurse a
    // directory. The permit is USED on exactly ONE path: copying a *changed* same-type regular file
    // from the update tree (`copy_child` reuses it for the data copy). On every other path — both
    // the recursing ones (a type-mismatch where the update side may be a directory, and a same-type
    // directory) and the non-recursive leaf ones (hard-link, symlink copy, special, no-update) — the
    // permit must NOT be held: a recursing path holding it across `copy_child`/`link_dir_entry` is
    // the hold-and-wait deadlock the leaf-permit lifecycle eliminates, and a leaf path holding the
    // mismatched permit across its own `.await` is pointless.
    //
    // So decide once, here, before any `.await` that isn't the intended copy: extract the open-files
    // guard for the file-changed-copy path and drop the permit for everything else. Replacing the
    // seven hand-maintained `drop(open_file_guard)` sites with this one is the Class-1 cleanup; this
    // is the one site exempted from the Phase H no-manual-drop lint, because rlink's dual-tree walk
    // is not a `WalkVisitor` and cannot use the driver's single drop-before-recurse home.
    let is_file_changed_copy = match update_handle.as_ref() {
        Some(update_handle) => {
            update_handle.kind() == src_handle.kind()
                && update_handle.kind() == EntryKind::File
                && !filecmp::metadata_equal(
                    &settings.update_compare,
                    src_handle.meta(),
                    update_handle.meta(),
                )
        }
        None => false,
    };
    let copy_guard: Option<throttle::OpenFileGuard> = if is_file_changed_copy {
        // keep the permit for the data copy: hand the inner open-files guard to `copy_child`.
        match permit {
            Some(LeafPermit::OpenFile(guard)) => Some(guard),
            // a non-OpenFile permit can never reach rlink (its `want` only takes from the OpenFile
            // pool), and `None` means the pool was disabled — either way there is nothing to pass.
            _ => None,
        }
    } else {
        // every non-copy / recursing branch: release the leaf permit now (the consolidated drop).
        drop(permit);
        None
    };
    if let Some(update_handle) = update_handle.as_ref() {
        let (update_dir, update_name) = update.unwrap();
        let update_path = update_path.as_deref().unwrap();
        if update_handle.kind() != src_handle.kind() {
            // file type changed, just copy the updated one
            tracing::debug!(
                "link: file type of {:?} ({:?}) and {:?} ({:?}) differs - copying from update",
                src_path,
                src_handle.kind(),
                update_path,
                update_handle.kind()
            );
            // the leaf permit was already released at the consolidated drop site above (this is the
            // type-mismatch path, where the update side may be a directory and we recurse via copy).
            // delegate at this entry's logical path so that, under --delete, pruning inside the
            // delegated subtree matches include/exclude descendants at the correct filter root
            // (e.g. `node/*.log`). pass the HELD update parent + name, never a re-resolved path.
            return delegate_copy(
                prog_track,
                update_dir,
                dst_parent,
                update_name,
                update_path,
                &dst_path,
                rel_path,
                settings,
                is_fresh,
                None,
            )
            .await;
        }
        if update_handle.kind() == EntryKind::File {
            // check if the file is unchanged and if so hard-link, otherwise copy from the updated one
            if filecmp::metadata_equal(
                &settings.update_compare,
                src_handle.meta(),
                update_handle.meta(),
            ) {
                // unchanged file: hard-link from src. the permit was already dropped above (this is
                // not the file-changed-copy path), so `copy_guard` is None and there is nothing held.
                tracing::debug!("no change, hard link 'src'");
                if settings.dry_run.is_some() {
                    crate::dry_run::report_action("link", &src_path, Some(&dst_path), "file");
                    return Ok(Summary {
                        hard_links_created: 1,
                        ..Default::default()
                    });
                }
                let dst_dir =
                    dst_parent.expect("destination parent must be open for a real hard link");
                return hard_link_entry_fd(
                    prog_track,
                    &src_handle,
                    dst_dir,
                    &dst_name,
                    &dst_path,
                    settings,
                )
                .await;
            }
            tracing::debug!(
                "link: {:?} metadata has changed, copying from {:?}",
                src_path,
                update_path
            );
            // changed file: delegate to copy, reusing the pre-acquired permit (the common path: the
            // spawn loop pre-acquires for regular-file hints). `copy_guard` is the open-files guard
            // the consolidated decision above extracted for exactly this path. copy_child
            // re-classifies and applies its own --overwrite/dry-run logic for a file.
            return delegate_copy(
                prog_track,
                update_dir,
                dst_parent,
                update_name,
                update_path,
                &dst_path,
                rel_path,
                settings,
                is_fresh,
                copy_guard,
            )
            .await;
        }
        if update_handle.kind() == EntryKind::Symlink {
            // update symlink: copy it. the permit was already dropped at the consolidated site.
            tracing::debug!("'update' is a symlink so just symlink that");
            return delegate_copy(
                prog_track,
                update_dir,
                dst_parent,
                update_name,
                update_path,
                &dst_path,
                rel_path,
                settings,
                is_fresh,
                None,
            )
            .await;
        }
    } else {
        // update hasn't been specified (or is absent at this name): hard-link a source file,
        // copy a source symlink.
        // the permit (if any) was already released at the consolidated drop site above, so the
        // no-update hard-link / symlink-copy paths here hold nothing.
        tracing::debug!("no 'update' entry");
        if src_handle.kind() == EntryKind::File {
            if settings.dry_run.is_some() {
                crate::dry_run::report_action("link", &src_path, Some(&dst_path), "file");
                return Ok(Summary {
                    hard_links_created: 1,
                    ..Default::default()
                });
            }
            let dst_dir = dst_parent.expect("destination parent must be open for a real hard link");
            return hard_link_entry_fd(
                prog_track,
                &src_handle,
                dst_dir,
                &dst_name,
                &dst_path,
                settings,
            )
            .await;
        }
        if src_handle.kind() == EntryKind::Symlink {
            tracing::debug!("'src' is a symlink so just symlink that");
            return delegate_copy(
                prog_track, src_parent, dst_parent, name, &src_path, &dst_path, rel_path, settings,
                is_fresh, None,
            )
            .await;
        }
    }
    if src_handle.kind() != EntryKind::Dir {
        // special file (or unsupported type): non-recursive; the permit is already released above.
        if settings.copy_settings.skip_specials {
            tracing::debug!(
                "skipping special file {:?} (kind: {:?})",
                src_path,
                src_handle.kind()
            );
            if let Some(mode) = settings.dry_run {
                match mode {
                    crate::config::DryRunMode::Brief => {}
                    crate::config::DryRunMode::All => println!("skip special {:?}", src_path),
                    crate::config::DryRunMode::Explain => {
                        println!(
                            "skip special {:?} (unsupported file type: {:?})",
                            src_path,
                            src_handle.kind()
                        );
                    }
                }
            }
            prog_track.specials_skipped.inc();
            return Ok(Summary {
                copy_summary: CopySummary {
                    specials_skipped: 1,
                    ..Default::default()
                },
                ..Default::default()
            });
        }
        return Err(Error::new(
            anyhow!(
                "copy: {:?} -> {:?} failed, unsupported src file type: {:?}",
                src_path,
                dst_path,
                src_handle.kind()
            ),
            Default::default(),
        ));
    }
    // directory: recurse the dual tree. the leaf permit was released at the consolidated drop site
    // above — this recursing path never holds it (the hold-and-wait deadlock invariant).
    debug_assert!(
        update_handle.is_none() || update_handle.as_ref().unwrap().kind() == EntryKind::Dir
    );
    // Only drive the dual-tree update walk when an update directory entry actually exists at this
    // name. If `update_handle` is None (the update tree has no counterpart for this src dir, the
    // recursive "update missing" case), process this subtree in no-update mode: hard-link the whole
    // source subtree. Passing the parent update tuple here would make `link_dir_entry` try to
    // `open_dir` a non-existent update child.
    let update_for_dir = update.filter(|_| update_handle.is_some());
    let update_root_for_dir = update_root.filter(|_| update_handle.is_some());
    link_dir_entry(
        prog_track,
        src_parent,
        update_for_dir,
        dst_parent,
        name,
        &dst_name,
        src_root,
        dst_root,
        update_root_for_dir,
        rel_path,
        &src_path,
        &dst_path,
        update_path.as_deref().filter(|_| update_handle.is_some()),
        settings,
        is_fresh,
    )
    .await
}

/// Delegate a single entry to the fd-based copy ([`copy::copy_child`]), passing the HELD parent
/// directory handles plus the entry `name` — never re-resolving a path. `filter_base` for the
/// delegation is the entry's logical relative path (so `--delete` pruning inside the subtree
/// matches include/exclude patterns at the entry's true path). The returned copy summary is folded
/// into a link `Summary`.
#[allow(clippy::too_many_arguments)]
async fn delegate_copy(
    prog_track: &'static progress::Progress,
    src_parent: &Arc<Dir>,
    dst_parent: Option<&Arc<Dir>>,
    name: &std::ffi::OsStr,
    src_path: &std::path::Path,
    dst_path: &std::path::Path,
    filter_base: &std::path::Path,
    settings: &Settings,
    is_fresh: bool,
    open_file_guard: Option<throttle::OpenFileGuard>,
) -> Result<Summary, Error> {
    let copy_summary = copy::copy_child(
        prog_track,
        src_parent,
        dst_parent,
        name,
        src_path,
        dst_path,
        filter_base,
        &settings.copy_settings,
        &settings.preserve,
        is_fresh,
        open_file_guard,
    )
    .await
    .map_err(|err| {
        let copy_summary = err.summary;
        Error::new(
            err.source,
            Summary {
                copy_summary,
                ..Default::default()
            },
        )
    })?;
    Ok(Summary {
        copy_summary,
        ..Default::default()
    })
}

/// Resolve (create / reuse / overwrite) the destination directory fd-relative, open the source
/// (and update) directories, then recurse via [`link_dir_contents`]. Mirrors copy's
/// [`copy::resolve_dst_dir`] for the overwrite branches (recheck-guarded, fd-relative removal).
#[allow(clippy::too_many_arguments)]
async fn link_dir_entry(
    prog_track: &'static progress::Progress,
    src_parent: &Arc<Dir>,
    update: Option<(&Arc<Dir>, &std::ffi::OsStr)>,
    dst_parent: Option<&Arc<Dir>>,
    name: &std::ffi::OsStr,
    dst_name: &std::ffi::OsStr,
    src_root: &std::path::Path,
    dst_root: &std::path::Path,
    update_root: Option<&std::path::Path>,
    rel_path: &std::path::Path,
    src_path: &std::path::Path,
    dst_path: &std::path::Path,
    update_path: Option<&std::path::Path>,
    settings: &Settings,
    is_fresh: bool,
) -> Result<Summary, Error> {
    let src_dir = src_parent
        .open_dir(name)
        .await
        .with_context(|| format!("cannot open directory {:?} for reading", src_path))
        .map_err(|err| Error::new(err, Default::default()))?;
    let src_dir = Arc::new(src_dir);
    // open the update directory too (it has the same file type as src here — both are dirs).
    let update_dir = match update {
        Some((update_parent, update_name)) => {
            let dir = update_parent
                .open_dir(update_name)
                .await
                .with_context(|| {
                    format!("cannot open update directory {:?} for reading", update_path)
                })
                .map_err(|err| Error::new(err, Default::default()))?;
            Some(Arc::new(dir))
        }
        None => None,
    };
    // dry-run: report the directory and traverse its contents, but never create a destination dir.
    if settings.dry_run.is_some() {
        crate::dry_run::report_action("link", src_path, Some(dst_path), "dir");
        let base = Summary {
            copy_summary: CopySummary {
                directories_created: 1, // report as would-be-created
                ..Default::default()
            },
            ..Default::default()
        };
        return link_dir_contents(
            prog_track,
            &src_dir,
            update_dir.as_ref(),
            None, // dry-run: no destination dir
            None, // dry-run: no destination parent
            dst_name,
            src_root,
            dst_root,
            update_root,
            rel_path,
            src_path,
            dst_path,
            true, // treat as "created" so empty-dir cleanup can suppress the dry-run count
            is_fresh,
            settings,
            base,
        )
        .await;
    }
    // real link: dst_parent is Some.
    let dst_parent = dst_parent.expect("destination parent must be open for a real link");
    let copy::DirSlot {
        dir: dst_dir,
        summary: base,
        is_fresh: child_is_fresh,
        we_created,
    } = match copy::resolve_dst_dir(
        prog_track,
        dst_parent,
        dst_name,
        dst_path,
        &settings.copy_settings,
        is_fresh,
    )
    .await
    .map_err(|err| {
        Error::new(
            err.source,
            Summary {
                copy_summary: err.summary,
                ..Default::default()
            },
        )
    })? {
        copy::DirResolution::Skip(summary) => {
            return Ok(Summary {
                copy_summary: summary,
                ..Default::default()
            });
        }
        copy::DirResolution::Proceed(slot) => slot,
    };
    link_dir_contents(
        prog_track,
        &src_dir,
        update_dir.as_ref(),
        Some(&dst_dir),
        Some(dst_parent),
        dst_name,
        src_root,
        dst_root,
        update_root,
        rel_path,
        src_path,
        dst_path,
        we_created,
        child_is_fresh,
        settings,
        Summary {
            copy_summary: base,
            ..Default::default()
        },
    )
    .await
}

/// The dual-tree body of a directory link: enumerate the source entries (hard-linking unchanged
/// files, delegating copies, recursing into subdirectories), then enumerate the update entries and
/// copy those not present in the source, then run `--delete` pruning, empty-directory cleanup, and
/// finally apply the directory's own metadata.
///
/// `dst_dir == None` / `dst_parent == None` means dry-run (no destination mutation). `base` carries
/// the `directories_created`/`directories_unchanged` contribution from resolving this directory.
#[allow(clippy::too_many_arguments)]
async fn link_dir_contents(
    prog_track: &'static progress::Progress,
    src_dir: &Arc<Dir>,
    update_dir: Option<&Arc<Dir>>,
    dst_dir: Option<&Arc<Dir>>,
    dst_parent: Option<&Arc<Dir>>,
    dst_name: &std::ffi::OsStr,
    src_root: &std::path::Path,
    dst_root: &std::path::Path,
    update_root: Option<&std::path::Path>,
    rel_path: &std::path::Path,
    src_path: &std::path::Path,
    dst_path: &std::path::Path,
    we_created_this_dir: bool,
    is_fresh: bool,
    settings: &Settings,
    base: Summary,
) -> Result<Summary, Error> {
    tracing::debug!("process contents of 'src' directory");
    let src_entries = src_dir
        .read_entries()
        .await
        .with_context(|| format!("cannot open directory {src_path:?} for reading"))
        .map_err(|err| Error::new(err, base))?;
    let mut link_summary = base;
    let mut join_set = tokio::task::JoinSet::new();
    let errors = crate::error_collector::ErrorCollector::default();
    // create a set of all the files we already processed
    let mut processed_files = std::collections::HashSet::new();
    // Keep-set for --delete: names that will be materialized at the destination. See
    // `DeleteKeepSet` for the semantics of `record_src` / `record_update` /
    // `drop_src_when_update_filtered`. No-op when --delete is off, so the call sites stay
    // unconditional in the hot path.
    let mut keep_set = DeleteKeepSet::new(
        settings.copy_settings.delete.as_ref(),
        settings.update_exclusive,
        update_dir.is_some(),
    );
    // iterate through src entries and recursively call "link" on each one
    for (entry_name, hint) in src_entries {
        // classification for the special-skip, symlink-dispatch, and permit pre-acquire decisions
        // uses the cheap getdents hint; `link_internal` re-classifies authoritatively via fstat
        // before acting. an unknown hint (DT_UNKNOWN) is treated as a regular file for those, the
        // same default the old path-based walk used when `file_type()` was unavailable.
        let entry_kind = hint.unwrap_or(EntryKind::File);
        let entry_is_symlink = entry_kind == EntryKind::Symlink;
        let entry_rel = rel_path.join(&entry_name);
        let entry_path = src_path.join(&entry_name);
        // the dir-ness that drives the FILTER decision AND the dry-run recurse-vs-leaf branch
        // below must be AUTHORITATIVE: on a DT_UNKNOWN entry, defaulting to non-dir would wrongly
        // omit a real directory's whole subtree under an is_dir-dependent filter, or (in dry-run,
        // even with NO filter) preview it as a leaf and never descend into it. `filter_is_dir`
        // fstats when a filter is active OR — via force_authoritative — when dry-run needs the
        // type for control flow. One extra fstat only in those DT_UNKNOWN cases (never follows
        // symlinks).
        let entry_is_dir = walk::filter_is_dir(
            settings.filter.as_ref(),
            src_dir,
            &entry_name,
            hint,
            settings.dry_run.is_some(),
        )
        .await;
        // apply filter if configured (logical path == entry_rel, since link's filter_base is empty)
        if let Some(skip_result) =
            walk::should_skip_entry(&settings.filter, &entry_rel, entry_is_dir)
        {
            if let Some(mode) = settings.dry_run {
                crate::dry_run::report_skip(&entry_path, &skip_result, mode, entry_kind.label());
            }
            tracing::debug!("skipping {:?} due to filter", &entry_path);
            link_summary = link_summary + skipped_summary_for(entry_kind);
            entry_kind.inc_skipped(prog_track);
            continue;
        }
        // keep-set: a source entry has a destination counterpart that must not be pruned, even
        // when --skip-specials skips copying it (computed before the skip-specials check below).
        keep_set.record_src(&entry_name);
        // skip special files (sockets, FIFOs, devices) when --skip-specials is set
        if settings.copy_settings.skip_specials && entry_kind == EntryKind::Special {
            tracing::debug!("skipping special file {:?}", &entry_path);
            if let Some(mode) = settings.dry_run {
                match mode {
                    crate::config::DryRunMode::Brief => {}
                    crate::config::DryRunMode::All => {
                        println!("skip special {:?}", &entry_path)
                    }
                    crate::config::DryRunMode::Explain => {
                        println!(
                            "skip special {:?} (unsupported file type: {:?})",
                            &entry_path, entry_kind
                        );
                    }
                }
            }
            link_summary.copy_summary.specials_skipped += 1;
            prog_track.specials_skipped.inc();
            continue;
        }
        processed_files.insert(entry_name.clone());
        // dry-run for non-directory entries: report the would-be action without recursing.
        if settings.dry_run.is_some() && !entry_is_dir {
            let dst_entry_path = dst_path.join(&entry_name);
            crate::dry_run::report_action(
                "link",
                &entry_path,
                Some(&dst_entry_path),
                entry_kind.label(),
            );
            if entry_is_symlink {
                // for symlinks in dry-run, count as symlink (in copy_summary)
                link_summary.copy_summary.symlinks_created += 1;
            } else {
                // for files in dry-run, count the "would be created" hard link.
                // N.B. when an update tree is present, link_internal decides file-vs-copy
                // per-entry; the dry-run hint here counts a hard link, matching the old walk which
                // likewise counted a hard link for a regular-file src entry in dry-run.
                link_summary.hard_links_created += 1;
            }
            continue;
        }
        // for regular-file entries, pre-acquire a leaf permit (open-files pool) BEFORE spawning so we
        // don't create unbounded tasks. `preacquire_leaf_permit` is the shared lifecycle primitive:
        // its `want` opts in only for a regular-file hint, so directories take none (they recurse and
        // would deadlock against a saturated pool), symlinks take none (they pass through to copy,
        // which handles permits internally), and a DT_UNKNOWN hint takes none for the same reason —
        // exactly the original `hint == Some(File)` policy. `link_internal` re-classifies
        // authoritatively and either uses the permit (changed-file copy) or drops it.
        //
        // Acquire-then-IMMEDIATELY-spawn (the permit is moved into `do_link` and spawned on the next
        // line, in the same loop step) is load-bearing: collecting a Vec of pre-acquired permits and
        // spawning later would hold N permits before any task runs and self-deadlock a saturated pool.
        // This mirrors the single-tree driver's incremental acquire-then-spawn loop
        // (`walk_driver::walk_dir_contents`, joined via `walk_driver::join_and_fold`).
        let permit = walk::preacquire_leaf_permit(PermitKind::OpenFile, hint, |h| {
            h == Some(EntryKind::File)
        })
        .await;
        let src_parent = Arc::clone(src_dir);
        let dst_parent = dst_dir.map(Arc::clone);
        let update_parent = update_dir.map(Arc::clone);
        let settings = settings.clone();
        let src_root = src_root.to_owned();
        let dst_root = dst_root.to_owned();
        let update_root = update_root.map(std::path::Path::to_path_buf);
        let do_link = move || async move {
            let update_ref = update_parent
                .as_ref()
                .map(|dir| (dir, entry_name.as_os_str()));
            link_internal(
                prog_track,
                &src_parent,
                update_ref,
                dst_parent.as_ref(),
                &entry_name,
                &src_root,
                &dst_root,
                update_root.as_deref(),
                &entry_rel,
                &settings,
                is_fresh,
                permit,
            )
            .await
        };
        join_set.spawn(do_link());
    }
    // only process update if the path was provided and the directory is present
    if let Some(update_dir) = update_dir {
        let update_root = update_root.expect("update_dir present implies update_root present");
        tracing::debug!("process contents of 'update' directory");
        let update_entries = update_dir
            .read_entries()
            .await
            .with_context(|| {
                format!(
                    "cannot open directory {:?} for reading",
                    update_path_dbg(update_root, rel_path)
                )
            })
            .map_err(|err| Error::new(err, link_summary))?;
        // Iterate through update entries and for each one that's not present in src, copy it.
        //
        // We deliberately do NOT pre-acquire any permit here. Two cycles rule out the
        // straightforward options:
        //   * `open_file_permit`: copy_child → copy_internal re-acquires open-files for each file;
        //     a saturated pool would deadlock the inner acquire if we held one across the call.
        //   * `pending_meta_permit`: with --overwrite, copy_child → copy_file_fd → rm::rm drains
        //     pending_meta for child entries (rm.rs spawn loop). N tasks here each holding a
        //     pending_meta permit would deadlock waiting on each other's inner rm.
        //
        // The spawn count at this site is naturally bounded by the number of update-only entries
        // (user input — typically modest). Each spawned task's work is throttled by copy's own
        // internal open-files backpressure inside copy_internal's spawn loop.
        for (entry_name, hint) in update_entries {
            let entry_kind = hint.unwrap_or(EntryKind::File);
            let entry_rel = rel_path.join(&entry_name);
            // the FILTER `is_dir` decision must use the AUTHORITATIVE type: on a DT_UNKNOWN update
            // entry with an is_dir-dependent include filter, defaulting to non-dir would wrongly
            // omit a real directory's whole subtree. one extra fstat only in that DT_UNKNOWN+filter
            // case (never follows symlinks).
            let entry_is_dir = walk::filter_is_dir(
                settings.filter.as_ref(),
                update_dir,
                &entry_name,
                hint,
                // used only for the filter decision here — no dry-run recurse-vs-leaf shortcut
                // (delegate_copy reclassifies authoritatively) — so no need to force an fstat.
                false,
            )
            .await;
            // evaluate the filter for this update entry at its logical path. This MUST run
            // regardless of `--delete`: `copy_child` wraps `copy_internal`, which (unlike the old
            // path-based `copy_with_filter_base`) does NOT re-apply a top-level filter to the entry
            // it is handed, so without this skip an `--exclude`'d update-only entry would be copied.
            let skip_result = walk::should_skip_entry(&settings.filter, &entry_rel, entry_is_dir);
            let filtered_out = skip_result.is_some();
            // keep-set: every filter-passing update entry is materialized at the destination
            // (entries also in `src` are linked, update-only entries are copied). Computed before
            // the dedup `continue` so entries also present in `src` are covered — this is what
            // makes --update-exclusive mirror the update set exactly.
            //
            // A filtered-out update entry contributes NOTHING here and never drops an existing
            // src-side registration: when the src loop materialized a same-name entry it can only
            // be a TYPE MISMATCH (a type-independent pattern would have excluded the src too), and
            // the union semantics keep the src's version (`link_internal` falls back to source-only
            // handling), so its keep-set entry must survive — pruning it would delete what we just
            // materialized. Under --update-exclusive `record_src` was a no-op, so there is likewise
            // nothing to keep. Either way the filtered-out branch leaves the keep-set untouched.
            if settings.copy_settings.delete.is_some() && !filtered_out {
                keep_set.record_update(&entry_name);
            }
            if processed_files.contains(&entry_name) {
                // we already must have considered this file, skip it
                continue;
            }
            // filtered-out update-only entry: skip the delegation entirely (matching the old
            // `copy_with_filter_base`'s top-level filter), and record the skip in the summary /
            // counters exactly as the source loop does for a filtered src entry.
            if let Some(skip_result) = skip_result {
                let update_entry_path = update_root.join(&entry_rel);
                if let Some(mode) = settings.dry_run {
                    crate::dry_run::report_skip(
                        &update_entry_path,
                        &skip_result,
                        mode,
                        entry_kind.label(),
                    );
                }
                tracing::debug!(
                    "skipping update entry {:?} due to filter",
                    &update_entry_path
                );
                link_summary = link_summary + skipped_summary_for(entry_kind);
                entry_kind.inc_skipped(prog_track);
                continue;
            }
            tracing::debug!("found a new entry in the 'update' directory");
            let update_entry_path = update_root.join(&entry_rel);
            let dst_entry_path = dst_path.join(&entry_name);
            let update_parent = Arc::clone(update_dir);
            let dst_parent = dst_dir.map(Arc::clone);
            let settings = settings.clone();
            let do_copy = move || async move {
                // filter-base for the delegated copy: this update entry's path relative to the
                // source root, so any --delete pruning inside it matches the include/exclude filter
                // at the entry's true relative path (e.g. cache/*.log), not relative to the entry.
                delegate_copy(
                    prog_track,
                    &update_parent,
                    dst_parent.as_ref(),
                    &entry_name,
                    &update_entry_path,
                    &dst_entry_path,
                    &entry_rel,
                    &settings,
                    is_fresh,
                    None,
                )
                .await
            };
            join_set.spawn(do_copy());
        }
    }
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(result) => match result {
                Ok(summary) => link_summary = link_summary + summary,
                Err(error) => {
                    tracing::error!(
                        "link: {:?} -> {:?} failed with: {:#}",
                        src_path,
                        dst_path,
                        &error
                    );
                    link_summary = link_summary + error.summary;
                    if settings.copy_settings.fail_early {
                        return Err(Error::new(error.source, link_summary));
                    }
                    errors.push(error.source);
                }
            },
            Err(error) => {
                if settings.copy_settings.fail_early {
                    return Err(Error::new(error.into(), link_summary));
                }
                errors.push(error.into());
            }
        }
    }
    // rsync-style --delete for rlink: remove destination entries the link operation did not
    // materialize. `keep_set` holds exactly the materialized names: src ∪ update normally, or just
    // the update set under --update-exclusive (where source-only entries are not materialized and
    // so are pruned, matching `rsync --link-dest --delete`).
    if let Some(delete_settings) = &settings.copy_settings.delete {
        if errors.has_errors() {
            // rsync-style safety: skip pruning when this subtree's link/update pass reported errors
            // — deleting based on a run that did not fully succeed could remove data unexpectedly.
            tracing::warn!(
                "skipping --delete pruning of {:?} because the link/update pass reported errors",
                dst_path
            );
        } else {
            // the prune scan runs through the destination directory's own pinned fd. In a real link
            // we already hold it (`dst_dir`); in --dry-run the create-or-overwrite step is skipped,
            // so open it `O_NOFOLLOW|O_DIRECTORY` (dereference=false) — a symlink or non-directory
            // fails closed and prune is skipped, never following the symlink to delete a tree
            // OUTSIDE the destination. A missing dir likewise skips.
            let prune_dir: Option<Arc<Dir>> = match dst_dir {
                Some(dir) => Some(Arc::clone(dir)),
                None => {
                    match Dir::open_root_dir(dst_path, false, congestion::Side::Destination).await {
                        Ok(dir) => Some(Arc::new(dir)),
                        Err(err)
                            if matches!(
                                err.kind(),
                                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                            ) || err.raw_os_error() == Some(libc::ELOOP)
                                || err.raw_os_error() == Some(libc::ENOTDIR) =>
                        {
                            tracing::debug!(
                                "skipping --delete pruning of {:?}: not a real directory",
                                dst_path
                            );
                            None
                        }
                        Err(err) => {
                            let err = anyhow::Error::new(err).context(format!(
                                "cannot open destination {dst_path:?} for delete scan"
                            ));
                            if settings.copy_settings.fail_early {
                                return Err(Error::new(err, link_summary));
                            }
                            errors.push(err);
                            None
                        }
                    }
                }
            };
            if let Some(prune_dir) = prune_dir {
                match crate::delete::prune_extraneous(
                    prog_track,
                    &prune_dir,
                    rel_path,
                    keep_set
                        .as_set()
                        .expect("--delete is on, so DeleteKeepSet is active"),
                    settings.filter.as_ref(),
                    delete_settings,
                    settings.copy_settings.fail_early,
                    settings.dry_run,
                )
                .await
                {
                    Ok(rm_summary) => {
                        link_summary.copy_summary.rm_summary =
                            link_summary.copy_summary.rm_summary + rm_summary;
                    }
                    Err(err) => {
                        link_summary.copy_summary.rm_summary =
                            link_summary.copy_summary.rm_summary + err.summary;
                        if settings.copy_settings.fail_early {
                            return Err(Error::new(err.source, link_summary));
                        }
                        errors.push(err.source);
                    }
                }
            }
        }
    }
    // when filtering is active and we created this directory, check if anything was actually
    // linked/copied into it. if nothing was linked, we may need to clean up the empty directory.
    let this_dir_count = usize::from(we_created_this_dir);
    let child_dirs_created = link_summary
        .copy_summary
        .directories_created
        .saturating_sub(this_dir_count);
    let anything_linked = link_summary.hard_links_created > 0
        || link_summary.copy_summary.files_copied > 0
        || link_summary.copy_summary.symlinks_created > 0
        || child_dirs_created > 0;
    let is_root = rel_path.as_os_str().is_empty();
    match check_empty_dir_cleanup(
        settings.filter.as_ref(),
        we_created_this_dir,
        anything_linked,
        rel_path,
        is_root,
        settings.dry_run.is_some(),
    ) {
        EmptyDirAction::Keep => { /* proceed with metadata application */ }
        EmptyDirAction::DryRunSkip => {
            tracing::debug!(
                "dry-run: directory {:?} would not be created (nothing to link inside)",
                dst_path
            );
            link_summary.copy_summary.directories_created = 0;
            // a child error collected during the walk must still surface — otherwise a
            // traversal-only directory whose only child FAILED becomes "empty", is skipped here, and
            // the failed link is reported as success (mirrors copy::finalize_dir).
            if errors.has_errors() {
                return Err(Error::new(errors.into_error().unwrap(), link_summary));
            }
            return Ok(link_summary);
        }
        EmptyDirAction::Remove => {
            tracing::debug!(
                "directory {:?} has nothing to link inside, removing empty directory",
                dst_path
            );
            // remove the empty directory fd-relative, through its parent dir handle: `rmdir_at`
            // operates on `dst_name` within the held `dst_parent` fd (never by path) and only
            // succeeds on an empty directory, so it is contained to `dst_parent`. `dst_parent` is
            // always Some here (None only in dry-run, where this arm is unreachable).
            let rmdir_result = match dst_parent {
                Some(dst_parent) => dst_parent.rmdir_at(dst_name).await,
                None => {
                    crate::walk::run_metadata_probed(
                        congestion::Side::Destination,
                        congestion::MetadataOp::RmDir,
                        tokio::fs::remove_dir(dst_path),
                    )
                    .await
                }
            };
            match rmdir_result {
                Ok(()) => {
                    link_summary.copy_summary.directories_created = 0;
                    // surface a collected child error even though the empty directory was removed,
                    // so a failed child link is never reported as success (mirrors
                    // copy::finalize_dir).
                    if errors.has_errors() {
                        return Err(Error::new(errors.into_error().unwrap(), link_summary));
                    }
                    return Ok(link_summary);
                }
                Err(err) => {
                    // removal failed (not empty, permission error, etc.) — keep directory
                    tracing::debug!(
                        "failed to remove empty directory {:?}: {:#}, keeping",
                        dst_path,
                        &err
                    );
                    // fall through to apply metadata
                }
            }
        }
    }
    // apply directory metadata regardless of whether all children linked successfully. the
    // directory itself was created/opened above. skipped in dry-run (no directory exists). prefer
    // the update directory's metadata when an update tree is present at this level (it is the
    // materialized version, matching the old `update_metadata_opt` preference), else the source
    // directory's. The metadata is read from the SAME fd whose contents were enumerated (read-side
    // fidelity, docs/tocttou.md), not the classify handles.
    tracing::debug!("set 'dst' directory metadata");
    let metadata_result = match dst_dir {
        Some(dst_dir) => {
            let meta_dir = update_dir.unwrap_or(src_dir);
            match meta_dir.meta().await {
                Ok(preserve_meta) => {
                    crate::safedir::set_dir_metadata_fd(&settings.preserve, &preserve_meta, dst_dir)
                        .await
                }
                Err(e) => Err(e),
            }
        }
        None => Ok(()),
    };
    if errors.has_errors() {
        // child failures take precedence - log metadata error if it also failed
        if let Err(metadata_err) = metadata_result {
            tracing::error!(
                "link: {:?} -> {:?} failed to set directory metadata: {:#}",
                src_path,
                dst_path,
                &metadata_err
            );
        }
        // unwrap is safe: has_errors() guarantees into_error() returns Some
        return Err(Error::new(errors.into_error().unwrap(), link_summary));
    }
    // no child failures, so metadata error is the primary error
    metadata_result
        .with_context(|| format!("failed setting directory metadata on {:?}", dst_path))
        .map_err(|err| Error::new(err, link_summary))?;
    Ok(link_summary)
}

/// Reconstruct an update entry's path purely for a diagnostic message.
fn update_path_dbg(
    update_root: &std::path::Path,
    rel_path: &std::path::Path,
) -> std::path::PathBuf {
    if rel_path.as_os_str().is_empty() {
        update_root.to_path_buf()
    } else {
        update_root.join(rel_path)
    }
}

#[cfg(test)]
mod link_tests {
    use crate::rm;
    use crate::testutils;
    use std::os::unix::fs::PermissionsExt;
    use tracing_test::traced_test;

    use super::*;

    static PROGRESS: std::sync::LazyLock<progress::Progress> =
        std::sync::LazyLock::new(progress::Progress::new);

    mod delete_keep_set_tests {
        //! Pure-logic unit tests for `DeleteKeepSet`. No filesystem needed — these pin the
        //! src-vs-update materialization rules so a future refactor can't silently break them.

        use super::super::DeleteKeepSet;
        use crate::copy::DeleteSettings;
        use std::ffi::{OsStr, OsString};

        fn delete_on() -> DeleteSettings {
            DeleteSettings {
                delete_excluded: false,
            }
        }

        #[test]
        fn record_src_no_op_when_delete_off() {
            let mut k = DeleteKeepSet::new(None, false, false);
            k.record_src(OsStr::new("foo"));
            assert!(k.as_set().is_none());
        }

        #[test]
        fn record_src_no_op_under_update_exclusive_with_update() {
            // `--update-exclusive` with an active update tree means the materialized set is
            // the update set; source-only entries must NOT be retained.
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d), true, true);
            k.record_src(OsStr::new("src_only"));
            assert!(!k.as_set().unwrap().contains(OsStr::new("src_only")));
        }

        #[test]
        fn record_src_records_when_update_exclusive_without_update() {
            // `--update-exclusive` is a no-op (carve-out doesn't apply) when no `--update`
            // path is given.
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d), true, false);
            k.record_src(OsStr::new("foo"));
            assert!(k.as_set().unwrap().contains(OsStr::new("foo")));
        }

        #[test]
        fn record_src_records_in_normal_delete_mode() {
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d), false, false);
            k.record_src(OsStr::new("foo"));
            assert!(k.as_set().unwrap().contains(OsStr::new("foo")));
        }

        #[test]
        fn record_update_always_records_when_delete_on() {
            // The update loop registers ALL filter-passing update entries, irrespective of
            // `--update-exclusive` — the update set IS the materialized set in that mode.
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d), true, true);
            k.record_update(OsStr::new("from_update"));
            assert!(k.as_set().unwrap().contains(OsStr::new("from_update")));
        }

        #[test]
        fn record_update_no_op_when_delete_off() {
            let mut k = DeleteKeepSet::new(None, false, false);
            k.record_update(OsStr::new("from_update"));
            assert!(k.as_set().is_none());
        }

        #[test]
        fn filtered_out_update_keeps_materialized_src_entry_in_normal_mode() {
            // Type-mismatch under normal `--update` (union): src had a regular file at `node`,
            // update has a dir at `node/` excluded by the dir-only `node/` pattern. The src's
            // version of the name stands (`link_internal` falls back to source-only handling), so
            // the keep-set entry recorded by the source loop MUST survive — pruning it would
            // delete the file we just materialized. The filtered-out update branch is therefore a
            // no-op on the keep-set: `node` stays.
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d), false, true);
            k.record_src(OsStr::new("node"));
            // (the update loop's filtered-out branch records nothing and removes nothing)
            assert!(
                k.as_set().unwrap().contains(OsStr::new("node")),
                "src entry must stay in the keep-set when its update counterpart is filtered out"
            );
        }

        #[test]
        fn filtered_out_update_keeps_skipped_special() {
            // The skip-special case: source loop ran `record_src` but never reached
            // `processed_files.insert` (it `continue`d on the skip-special branch). The dst
            // counterpart must be retained per --skip-specials semantics, and a filtered-out
            // update entry at the same name does not change that.
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d), false, true);
            k.record_src(OsStr::new("pipe"));
            assert!(k.as_set().unwrap().contains(OsStr::new("pipe")));
        }

        #[test]
        fn full_directory_pass_keep_set_union_semantics() {
            // Models the union of src + update under plain `--delete --update` (no
            // --update-exclusive). Names: src has `keep`, `pipe` (special, skipped),
            // `node` (file). update has `from_upd`, `node` (a dir excluded by the dir-only
            // `node/` pattern). Under union semantics the excluded update `node/` does not
            // displace the src `node` file: `link_internal` materializes the src version, so
            // `node` STAYS in the keep-set (the filtered-out update branch records/removes
            // nothing). This is the corrected behavior versus the old type-mismatch bug, where
            // the excluded update dir was copied AND the src keep-set entry was dropped.
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d), false, true);

            // source loop
            k.record_src(OsStr::new("keep"));
            k.record_src(OsStr::new("pipe")); // --skip-specials: continues, processed_files NOT populated
            k.record_src(OsStr::new("node"));

            // update loop: only filter-passing update entries are recorded; `node` is filtered out
            // and so contributes nothing (and does not drop the src `node`).
            k.record_update(OsStr::new("from_upd"));

            let set: std::collections::HashSet<OsString> = k.as_set().unwrap().clone();
            let expected: std::collections::HashSet<OsString> =
                ["keep", "pipe", "node", "from_upd"]
                    .into_iter()
                    .map(OsString::from)
                    .collect();
            assert_eq!(set, expected);
        }
    }

    fn common_settings(dereference: bool, overwrite: bool) -> Settings {
        Settings {
            copy_settings: CopySettings {
                dereference,
                fail_early: false,
                overwrite,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            update_compare: filecmp::MetadataCmpSettings {
                size: true,
                mtime: true,
                ..Default::default()
            },
            update_exclusive: false,
            filter: None,
            dry_run: None,
            preserve: preserve::preserve_all(),
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_basic_link() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &None,
            &common_settings(false, false),
            false,
        )
        .await?;
        assert_eq!(summary.hard_links_created, 5);
        assert_eq!(summary.copy_summary.files_copied, 0);
        assert_eq!(summary.copy_summary.symlinks_created, 2);
        assert_eq!(summary.copy_summary.directories_created, 3);
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    // Regression: a source operand whose final component is `.`/`..` (e.g. `rlink tree/.. dst`)
    // must be linked, not rejected — `split_root_operand` canonicalizes it. Uses `tree/sub/..`
    // (== `tree`) rather than `.` to avoid touching the process-wide cwd.
    #[tokio::test]
    async fn links_dot_dot_source_operand() -> Result<(), anyhow::Error> {
        use std::os::unix::fs::MetadataExt;
        let tmp = testutils::create_temp_dir().await?;
        let tree = tmp.join("tree");
        tokio::fs::create_dir(&tree).await?;
        tokio::fs::write(tree.join("a.txt"), "hello").await?;
        tokio::fs::create_dir(tree.join("sub")).await?;
        let src = tree.join("sub").join(".."); // == tree
        let dst = tmp.join("dst");
        let summary = link(
            &PROGRESS,
            &tmp,
            &src,
            &dst,
            &None,
            &common_settings(false, false),
            false,
        )
        .await?;
        assert_eq!(
            summary.hard_links_created, 1,
            "the dot-dot source's file must be hard-linked"
        );
        assert!(
            dst.join("sub").is_dir(),
            "the dot-dot source's subdir must be created"
        );
        // the dst file shares the src inode (a hard link, not a copy).
        let src_ino = std::fs::metadata(tree.join("a.txt"))?.ino();
        let dst_ino = std::fs::metadata(dst.join("a.txt"))?.ino();
        assert_eq!(src_ino, dst_ino, "dst must be a hard link to the src inode");
        Ok(())
    }

    // Regression: an `--update` operand whose final component is `.`/`..` (e.g.
    // `rlink --update tree/.. src dst`) must be accepted, not rejected — the update tree is a READ
    // tree, so `split_root_operand` canonicalizes it the same as the source. Uses `tree/sub/..`
    // (== `tree`) rather than `.` to avoid touching the process-wide cwd; src == update == tree so
    // the file links deterministically from the update tree.
    #[tokio::test]
    async fn links_dot_dot_update_operand() -> Result<(), anyhow::Error> {
        use std::os::unix::fs::MetadataExt;
        let tmp = testutils::create_temp_dir().await?;
        let tree = tmp.join("tree");
        tokio::fs::create_dir(&tree).await?;
        tokio::fs::write(tree.join("a.txt"), "hello").await?;
        tokio::fs::create_dir(tree.join("sub")).await?;
        let dst = tmp.join("dst");
        // the --update operand spelled with a trailing `..` (== tree); it must be canonicalized and
        // used, not rejected with "has no parent directory or file name".
        let update_operand = tree.join("sub").join(".."); // == tree
        let summary = link(
            &PROGRESS,
            &tmp,
            &tree,
            &dst,
            &Some(update_operand),
            &common_settings(false, false),
            false,
        )
        .await?;
        assert_eq!(
            summary.hard_links_created, 1,
            "the file must be hard-linked from the dot-dot update tree"
        );
        // the dst file shares the update tree's inode (linked from it, not copied).
        let update_ino = std::fs::metadata(tree.join("a.txt"))?.ino();
        let dst_ino = std::fs::metadata(dst.join("a.txt"))?.ino();
        assert_eq!(
            update_ino, dst_ino,
            "dst must be hard-linked from the update tree inode"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_basic_link_update() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Some(test_path.join("foo")),
            &common_settings(false, false),
            false,
        )
        .await?;
        assert_eq!(summary.hard_links_created, 5);
        assert_eq!(summary.copy_summary.files_copied, 0);
        assert_eq!(summary.copy_summary.symlinks_created, 2);
        assert_eq!(summary.copy_summary.directories_created, 3);
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_basic_link_empty_src() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        tokio::fs::create_dir(tmp_dir.join("baz")).await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            test_path,
            &test_path.join("baz"), // empty source
            &test_path.join("bar"),
            &Some(test_path.join("foo")),
            &common_settings(false, false),
            false,
        )
        .await?;
        assert_eq!(summary.hard_links_created, 0);
        assert_eq!(summary.copy_summary.files_copied, 5);
        assert_eq!(summary.copy_summary.symlinks_created, 2);
        assert_eq!(summary.copy_summary.directories_created, 3);
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_destination_permission_error_includes_root_cause()
    -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let readonly_parent = test_path.join("readonly_dest");
        tokio::fs::create_dir(&readonly_parent).await?;
        tokio::fs::set_permissions(&readonly_parent, std::fs::Permissions::from_mode(0o555))
            .await?;

        let mut settings = common_settings(false, false);
        settings.copy_settings.fail_early = true;

        let result = link(
            &PROGRESS,
            test_path,
            &test_path.join("foo"),
            &readonly_parent.join("bar"),
            &None,
            &settings,
            false,
        )
        .await;

        // restore permissions to allow temporary directory cleanup
        tokio::fs::set_permissions(&readonly_parent, std::fs::Permissions::from_mode(0o755))
            .await?;

        assert!(result.is_err(), "link into read-only parent should fail");
        let err = result.unwrap_err();
        let err_msg = format!("{:#}", err.source);
        assert!(
            err_msg.to_lowercase().contains("permission denied") || err_msg.contains("EACCES"),
            "Error message must include permission denied text. Got: {}",
            err_msg
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn hard_link_file_into_readonly_parent_returns_error() -> Result<(), anyhow::Error> {
        // regression: hard_link_helper used to silently ignore non-AlreadyExists errors
        // and report hard_links_created=1 when the underlying hard_link call had failed
        let tmp_dir = testutils::setup_test_dir().await?;
        let src = tmp_dir.join("src.txt");
        tokio::fs::write(&src, "content").await?;
        let readonly_parent = tmp_dir.join("readonly_parent");
        tokio::fs::create_dir(&readonly_parent).await?;
        tokio::fs::set_permissions(&readonly_parent, std::fs::Permissions::from_mode(0o555))
            .await?;
        let dst = readonly_parent.join("dst.txt");
        let settings = common_settings(false, false);
        let result = link(&PROGRESS, &tmp_dir, &src, &dst, &None, &settings, false).await;
        tokio::fs::set_permissions(&readonly_parent, std::fs::Permissions::from_mode(0o755))
            .await?;
        let err = result.expect_err("link into read-only parent should fail");
        assert_eq!(err.summary.hard_links_created, 0);
        let err_msg = format!("{:#}", err.source);
        assert!(
            err_msg.to_lowercase().contains("permission denied") || err_msg.contains("EACCES"),
            "error should include root cause, got: {err_msg}"
        );
        Ok(())
    }

    pub async fn setup_update_dir(tmp_dir: &std::path::Path) -> Result<(), anyhow::Error> {
        // update
        // |- 0.txt
        // |- bar
        //    |- 1.txt
        //    |- 2.txt -> ../0.txt
        let foo_path = tmp_dir.join("update");
        tokio::fs::create_dir(&foo_path).await.unwrap();
        tokio::fs::write(foo_path.join("0.txt"), "0-new")
            .await
            .unwrap();
        let bar_path = foo_path.join("bar");
        tokio::fs::create_dir(&bar_path).await.unwrap();
        tokio::fs::write(bar_path.join("1.txt"), "1-new")
            .await
            .unwrap();
        tokio::fs::symlink("../1.txt", bar_path.join("2.txt"))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_update() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        setup_update_dir(&tmp_dir).await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Some(test_path.join("update")),
            &common_settings(false, false),
            false,
        )
        .await?;
        assert_eq!(summary.hard_links_created, 2);
        assert_eq!(summary.copy_summary.files_copied, 2);
        assert_eq!(summary.copy_summary.symlinks_created, 3);
        assert_eq!(summary.copy_summary.directories_created, 3);
        // compare subset of src and dst
        testutils::check_dirs_identical(
            &test_path.join("foo").join("baz"),
            &test_path.join("bar").join("baz"),
            testutils::FileEqualityCheck::HardLink,
        )
        .await?;
        // compare update and dst
        testutils::check_dirs_identical(
            &test_path.join("update"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_update_exclusive() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        setup_update_dir(&tmp_dir).await?;
        let test_path = tmp_dir.as_path();
        let mut settings = common_settings(false, false);
        settings.update_exclusive = true;
        let summary = link(
            &PROGRESS,
            test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Some(test_path.join("update")),
            &settings,
            false,
        )
        .await?;
        // we should end up with same directory as the update
        // |- 0.txt
        // |- bar
        //    |- 1.txt
        //    |- 2.txt -> ../0.txt
        assert_eq!(summary.hard_links_created, 0);
        assert_eq!(summary.copy_summary.files_copied, 2);
        assert_eq!(summary.copy_summary.symlinks_created, 1);
        assert_eq!(summary.copy_summary.directories_created, 2);
        // compare update and dst
        testutils::check_dirs_identical(
            &test_path.join("update"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    async fn setup_test_dir_and_link() -> Result<std::path::PathBuf, anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &None,
            &common_settings(false, false),
            false,
        )
        .await?;
        assert_eq!(summary.hard_links_created, 5);
        assert_eq!(summary.copy_summary.symlinks_created, 2);
        assert_eq!(summary.copy_summary.directories_created, 3);
        Ok(tmp_dir)
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_overwrite_basic() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_link().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar  <---------------------------------------- REMOVE
            //    |- 1.txt  <----------------------------------- REMOVE
            //    |- 2.txt  <----------------------------------- REMOVE
            //    |- 3.txt  <----------------------------------- REMOVE
            // |- baz
            //    |- 4.txt
            //    |- 5.txt -> ../bar/2.txt <-------------------- REMOVE
            //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("bar"),
                &rm::Settings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &rm::Settings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 3);
            assert_eq!(summary.symlinks_removed, 1);
            assert_eq!(summary.directories_removed, 1);
        }
        let summary = link(
            &PROGRESS,
            &tmp_dir,
            &tmp_dir.join("foo"),
            output_path,
            &None,
            &common_settings(false, true), // overwrite!
            false,
        )
        .await?;
        assert_eq!(summary.hard_links_created, 3);
        assert_eq!(summary.copy_summary.symlinks_created, 1);
        assert_eq!(summary.copy_summary.directories_created, 1);
        testutils::check_dirs_identical(
            &tmp_dir.join("foo"),
            output_path,
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_update_overwrite_basic() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_link().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar  <---------------------------------------- REMOVE
            //    |- 1.txt  <----------------------------------- REMOVE
            //    |- 2.txt  <----------------------------------- REMOVE
            //    |- 3.txt  <----------------------------------- REMOVE
            // |- baz
            //    |- 4.txt
            //    |- 5.txt -> ../bar/2.txt <-------------------- REMOVE
            //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("bar"),
                &rm::Settings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &rm::Settings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 3);
            assert_eq!(summary.symlinks_removed, 1);
            assert_eq!(summary.directories_removed, 1);
        }
        setup_update_dir(&tmp_dir).await?;
        // update
        // |- 0.txt
        // |- bar
        //    |- 1.txt
        //    |- 2.txt -> ../0.txt
        let summary = link(
            &PROGRESS,
            &tmp_dir,
            &tmp_dir.join("foo"),
            output_path,
            &Some(tmp_dir.join("update")),
            &common_settings(false, true), // overwrite!
            false,
        )
        .await?;
        assert_eq!(summary.hard_links_created, 1); // 3.txt
        assert_eq!(summary.copy_summary.files_copied, 2); // 0.txt, 1.txt
        assert_eq!(summary.copy_summary.symlinks_created, 2); // 2.txt, 5.txt
        assert_eq!(summary.copy_summary.directories_created, 1);
        // compare subset of src and dst
        testutils::check_dirs_identical(
            &tmp_dir.join("foo").join("baz"),
            &tmp_dir.join("bar").join("baz"),
            testutils::FileEqualityCheck::HardLink,
        )
        .await?;
        // compare update and dst
        testutils::check_dirs_identical(
            &tmp_dir.join("update"),
            &tmp_dir.join("bar"),
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_overwrite_hardlink_file() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_link().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar
            //    |- 1.txt  <----------------------------------- REPLACE W/ FILE
            //    |- 2.txt  <----------------------------------- REPLACE W/ SYMLINK
            //    |- 3.txt  <----------------------------------- REPLACE W/ DIRECTORY
            // |- baz    <-------------------------------------- REPLACE W/ FILE
            //    |- ...
            let bar_path = output_path.join("bar");
            let summary = rm::rm(
                &PROGRESS,
                &bar_path.join("1.txt"),
                &rm::Settings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &bar_path.join("2.txt"),
                    &rm::Settings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?
                + rm::rm(
                    &PROGRESS,
                    &bar_path.join("3.txt"),
                    &rm::Settings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz"),
                    &rm::Settings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 4);
            assert_eq!(summary.symlinks_removed, 2);
            assert_eq!(summary.directories_removed, 1);
            // REPLACE with a file, a symlink, a directory and a file
            tokio::fs::write(bar_path.join("1.txt"), "1-new")
                .await
                .unwrap();
            tokio::fs::symlink("../0.txt", bar_path.join("2.txt"))
                .await
                .unwrap();
            tokio::fs::create_dir(&bar_path.join("3.txt"))
                .await
                .unwrap();
            tokio::fs::write(&output_path.join("baz"), "baz")
                .await
                .unwrap();
        }
        let summary = link(
            &PROGRESS,
            &tmp_dir,
            &tmp_dir.join("foo"),
            output_path,
            &None,
            &common_settings(false, true), // overwrite!
            false,
        )
        .await?;
        assert_eq!(summary.hard_links_created, 4);
        assert_eq!(summary.copy_summary.files_copied, 0);
        assert_eq!(summary.copy_summary.symlinks_created, 2);
        assert_eq!(summary.copy_summary.directories_created, 1);
        testutils::check_dirs_identical(
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            testutils::FileEqualityCheck::HardLink,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_overwrite_error() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_link().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar
            //    |- 1.txt  <----------------------------------- REPLACE W/ FILE
            //    |- 2.txt  <----------------------------------- REPLACE W/ SYMLINK
            //    |- 3.txt  <----------------------------------- REPLACE W/ DIRECTORY
            // |- baz    <-------------------------------------- REPLACE W/ FILE
            //    |- ...
            let bar_path = output_path.join("bar");
            let summary = rm::rm(
                &PROGRESS,
                &bar_path.join("1.txt"),
                &rm::Settings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &bar_path.join("2.txt"),
                    &rm::Settings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?
                + rm::rm(
                    &PROGRESS,
                    &bar_path.join("3.txt"),
                    &rm::Settings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz"),
                    &rm::Settings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 4);
            assert_eq!(summary.symlinks_removed, 2);
            assert_eq!(summary.directories_removed, 1);
            // REPLACE with a file, a symlink, a directory and a file
            tokio::fs::write(bar_path.join("1.txt"), "1-new")
                .await
                .unwrap();
            tokio::fs::symlink("../0.txt", bar_path.join("2.txt"))
                .await
                .unwrap();
            tokio::fs::create_dir(&bar_path.join("3.txt"))
                .await
                .unwrap();
            tokio::fs::write(&output_path.join("baz"), "baz")
                .await
                .unwrap();
        }
        let source_path = &tmp_dir.join("foo");
        // unreadable
        tokio::fs::set_permissions(
            &source_path.join("baz"),
            std::fs::Permissions::from_mode(0o000),
        )
        .await?;
        // bar
        // |- ...
        // |- baz <- NON READABLE
        match link(
            &PROGRESS,
            &tmp_dir,
            &tmp_dir.join("foo"),
            output_path,
            &None,
            &common_settings(false, true), // overwrite!
            false,
        )
        .await
        {
            Ok(_) => panic!("Expected the link to error!"),
            Err(error) => {
                tracing::info!("{}", &error);
                assert_eq!(error.summary.hard_links_created, 3);
                assert_eq!(error.summary.copy_summary.files_copied, 0);
                assert_eq!(error.summary.copy_summary.symlinks_created, 0);
                assert_eq!(error.summary.copy_summary.directories_created, 0);
                assert_eq!(error.summary.copy_summary.rm_summary.files_removed, 1);
                assert_eq!(error.summary.copy_summary.rm_summary.directories_removed, 1);
                assert_eq!(error.summary.copy_summary.rm_summary.symlinks_removed, 1);
            }
        }
        Ok(())
    }

    /// Verify that directory metadata is applied even when child link operations fail.
    /// This is a regression test for a bug where directory permissions were not preserved
    /// when linking with fail_early=false and some children failed to link.
    #[tokio::test]
    #[traced_test]
    async fn test_link_directory_metadata_applied_on_child_error() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // create source directory with specific permissions
        let src_dir = test_path.join("src");
        tokio::fs::create_dir(&src_dir).await?;
        tokio::fs::set_permissions(&src_dir, std::fs::Permissions::from_mode(0o750)).await?;
        // create a readable file (will be linked successfully)
        tokio::fs::write(src_dir.join("readable.txt"), "content").await?;
        // create a subdirectory with a file, then make the subdirectory unreadable
        // this will cause the recursive walk to fail when trying to read subdirectory contents
        let unreadable_subdir = src_dir.join("unreadable_subdir");
        tokio::fs::create_dir(&unreadable_subdir).await?;
        tokio::fs::write(unreadable_subdir.join("hidden.txt"), "secret").await?;
        tokio::fs::set_permissions(&unreadable_subdir, std::fs::Permissions::from_mode(0o000))
            .await?;
        let dst_dir = test_path.join("dst");
        // link with fail_early=false
        let result = link(
            &PROGRESS,
            test_path,
            &src_dir,
            &dst_dir,
            &None,
            &common_settings(false, false),
            false,
        )
        .await;
        // restore permissions so cleanup can succeed
        tokio::fs::set_permissions(&unreadable_subdir, std::fs::Permissions::from_mode(0o755))
            .await?;
        // verify the operation returned an error (unreadable subdirectory should fail)
        assert!(
            result.is_err(),
            "link should fail due to unreadable subdirectory"
        );
        let error = result.unwrap_err();
        // verify the readable file was linked successfully
        assert_eq!(error.summary.hard_links_created, 1);
        // verify the destination directory exists and has the correct permissions
        let dst_metadata = tokio::fs::metadata(&dst_dir).await?;
        assert!(dst_metadata.is_dir());
        let actual_mode = dst_metadata.permissions().mode() & 0o7777;
        assert_eq!(
            actual_mode, 0o750,
            "directory should have preserved source permissions (0o750), got {:o}",
            actual_mode
        );
        Ok(())
    }
    mod filter_tests {
        use super::*;
        use crate::filter::FilterSettings;
        /// Test that path-based patterns (with /) work correctly with nested paths.
        #[tokio::test]
        #[traced_test]
        async fn test_path_pattern_matches_nested_files() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // create filter that should only link files in bar/ directory
            let mut filter = FilterSettings::new();
            filter.add_include("bar/*.txt").unwrap();
            let summary = link(
                &PROGRESS,
                test_path,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // should only link files matching bar/*.txt pattern (bar/1.txt, bar/2.txt, bar/3.txt)
            assert_eq!(
                summary.hard_links_created, 3,
                "should link 3 files matching bar/*.txt"
            );
            // verify the right files were linked
            assert!(
                test_path.join("dst/bar/1.txt").exists(),
                "bar/1.txt should be linked"
            );
            assert!(
                test_path.join("dst/bar/2.txt").exists(),
                "bar/2.txt should be linked"
            );
            assert!(
                test_path.join("dst/bar/3.txt").exists(),
                "bar/3.txt should be linked"
            );
            // verify files outside the pattern don't exist
            assert!(
                !test_path.join("dst/0.txt").exists(),
                "0.txt should not be linked"
            );
            Ok(())
        }
        /// Regression: with a filter active and `fail_early = false`, a directory whose only
        /// traversed child FAILS becomes "empty" and is pruned by the empty-dir cleanup — the child
        /// failure must still surface, not be masked as success. copy.rs's `finalize_dir` guards
        /// this in its DryRunSkip/Remove arms; `link_dir_contents` must do the same.
        #[tokio::test]
        #[traced_test]
        async fn test_filter_pruned_empty_dir_surfaces_child_error() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let test_path = tmp_dir.as_path();
            // src/ is the root; src/sub/ is traversal-only under the filter (it does not directly
            // match), and its sole child `unreadable/` (mode 0o000) fails to open during the walk.
            // nothing links into sub/, so the empty-dir cleanup prunes it.
            let src_dir = test_path.join("src");
            let unreadable = src_dir.join("sub").join("unreadable");
            tokio::fs::create_dir_all(&unreadable).await?;
            tokio::fs::write(unreadable.join("x.txt"), "secret").await?;
            tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).await?;
            // an include pattern that matches nothing present forces traversal of sub/ and
            // unreadable/ without directly matching sub/, so sub/ is "traversal-only".
            let mut filter = FilterSettings::new();
            filter.add_include("*.match").unwrap();
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter);
            let result = link(
                &PROGRESS,
                test_path,
                &src_dir,
                &test_path.join("dst"),
                &None,
                &settings,
                false,
            )
            .await;
            // restore perms so the temp dir can be cleaned up
            tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o755)).await?;
            assert!(
                result.is_err(),
                "a child link failure inside a filter-pruned empty directory must surface as an \
                 error, not be masked as success"
            );
            Ok(())
        }
        /// As above but in dry-run mode, which hits the `DryRunSkip` arm instead of `Remove`: a
        /// collected child error must still surface rather than being reported as a clean dry run.
        #[tokio::test]
        #[traced_test]
        async fn test_filter_pruned_empty_dir_surfaces_child_error_dry_run()
        -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let test_path = tmp_dir.as_path();
            let src_dir = test_path.join("src");
            let unreadable = src_dir.join("sub").join("unreadable");
            tokio::fs::create_dir_all(&unreadable).await?;
            tokio::fs::write(unreadable.join("x.txt"), "secret").await?;
            tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).await?;
            let mut filter = FilterSettings::new();
            filter.add_include("*.match").unwrap();
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter);
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            settings.copy_settings.dry_run = Some(crate::config::DryRunMode::Brief);
            let result = link(
                &PROGRESS,
                test_path,
                &src_dir,
                &test_path.join("dst"),
                &None,
                &settings,
                false,
            )
            .await;
            tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o755)).await?;
            assert!(
                result.is_err(),
                "dry-run must also surface the child error, not report a clean run"
            );
            Ok(())
        }
        /// Test that filters are applied to top-level file arguments.
        #[tokio::test]
        #[traced_test]
        async fn test_filter_applies_to_single_file_source() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // create filter that excludes .txt files
            let mut filter = FilterSettings::new();
            filter.add_exclude("*.txt").unwrap();
            let summary = link(
                &PROGRESS,
                test_path,
                &test_path.join("foo/0.txt"), // single file source
                &test_path.join("dst/0.txt"),
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // the file should NOT be linked because it matches the exclude pattern
            assert_eq!(
                summary.hard_links_created, 0,
                "file matching exclude pattern should not be linked"
            );
            assert!(
                !test_path.join("dst/0.txt").exists(),
                "excluded file should not exist at destination"
            );
            Ok(())
        }
        /// Test that filters apply to root directories with simple exclude patterns.
        #[tokio::test]
        #[traced_test]
        async fn test_filter_applies_to_root_directory() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create a directory that should be excluded
            tokio::fs::create_dir_all(test_path.join("excluded_dir")).await?;
            tokio::fs::write(test_path.join("excluded_dir/file.txt"), "content").await?;
            // create filter that excludes *_dir/ directories
            let mut filter = FilterSettings::new();
            filter.add_exclude("*_dir/").unwrap();
            let result = link(
                &PROGRESS,
                &test_path,
                &test_path.join("excluded_dir"),
                &test_path.join("dst"),
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // directory should NOT be linked because it matches exclude pattern
            assert_eq!(
                result.copy_summary.directories_created, 0,
                "root directory matching exclude should not be created"
            );
            assert!(
                !test_path.join("dst").exists(),
                "excluded root directory should not exist at destination"
            );
            Ok(())
        }
        /// Test that filters apply to root symlinks with simple exclude patterns.
        #[tokio::test]
        #[traced_test]
        async fn test_filter_applies_to_root_symlink() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create a target file and a symlink to it
            tokio::fs::write(test_path.join("target.txt"), "content").await?;
            tokio::fs::symlink(
                test_path.join("target.txt"),
                test_path.join("excluded_link"),
            )
            .await?;
            // create filter that excludes *_link
            let mut filter = FilterSettings::new();
            filter.add_exclude("*_link").unwrap();
            let result = link(
                &PROGRESS,
                &test_path,
                &test_path.join("excluded_link"),
                &test_path.join("dst"),
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // symlink should NOT be copied because it matches exclude pattern
            assert_eq!(
                result.copy_summary.symlinks_created, 0,
                "root symlink matching exclude should not be created"
            );
            assert!(
                !test_path.join("dst").exists(),
                "excluded root symlink should not exist at destination"
            );
            Ok(())
        }
        /// Test combined include and exclude patterns (exclude takes precedence).
        #[tokio::test]
        #[traced_test]
        async fn test_combined_include_exclude_patterns() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // test structure from setup_test_dir:
            // foo/
            //   0.txt
            //   bar/ (1.txt, 2.txt, 3.txt)
            //   baz/ (4.txt, 5.txt symlink, 6.txt symlink)
            // include all .txt files in bar/, but exclude 2.txt specifically
            let mut filter = FilterSettings::new();
            filter.add_include("bar/*.txt").unwrap();
            filter.add_exclude("bar/2.txt").unwrap();
            let summary = link(
                &PROGRESS,
                test_path,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // should link: bar/1.txt, bar/3.txt = 2 hard links
            // should skip: bar/2.txt (excluded by pattern), 0.txt (excluded by default - no match) = 2 files
            assert_eq!(summary.hard_links_created, 2, "should create 2 hard links");
            assert_eq!(
                summary.copy_summary.files_skipped, 2,
                "should skip 2 files (bar/2.txt excluded, 0.txt no match)"
            );
            // verify
            assert!(
                test_path.join("dst/bar/1.txt").exists(),
                "bar/1.txt should be linked"
            );
            assert!(
                !test_path.join("dst/bar/2.txt").exists(),
                "bar/2.txt should be excluded"
            );
            assert!(
                test_path.join("dst/bar/3.txt").exists(),
                "bar/3.txt should be linked"
            );
            Ok(())
        }
        /// Test that skipped counts accurately reflect what was filtered.
        #[tokio::test]
        #[traced_test]
        async fn test_skipped_counts_comprehensive() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // test structure from setup_test_dir:
            // foo/
            //   0.txt
            //   bar/ (1.txt, 2.txt, 3.txt)
            //   baz/ (4.txt, 5.txt symlink, 6.txt symlink)
            // exclude bar/ directory entirely
            let mut filter = FilterSettings::new();
            filter.add_exclude("bar/").unwrap();
            let summary = link(
                &PROGRESS,
                test_path,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // linked: 0.txt (1 hard link), baz/4.txt (1 hard link)
            // symlinks copied: 5.txt, 6.txt
            // skipped: bar directory (1 dir)
            assert_eq!(summary.hard_links_created, 2, "should create 2 hard links");
            assert_eq!(
                summary.copy_summary.symlinks_created, 2,
                "should copy 2 symlinks"
            );
            assert_eq!(
                summary.copy_summary.directories_skipped, 1,
                "should skip 1 directory (bar)"
            );
            // bar should not exist in dst
            assert!(
                !test_path.join("dst/bar").exists(),
                "bar directory should not be linked"
            );
            Ok(())
        }
        /// Test that empty directories are not created when they were only traversed to look
        /// for matches (regression test for bug where --include='foo' would create empty dir baz).
        #[tokio::test]
        #[traced_test]
        async fn test_empty_dir_not_created_when_only_traversed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // src/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("foo"), "content").await?;
            tokio::fs::write(src_path.join("bar"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = link(
                &PROGRESS,
                &test_path,
                &src_path,
                &test_path.join("dst"),
                &None,
                &Settings {
                    copy_settings: copy::Settings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // only 'foo' should be linked
            assert_eq!(summary.hard_links_created, 1, "should link only 'foo' file");
            assert_eq!(
                summary.copy_summary.directories_created, 1,
                "should create only root directory (not empty 'baz')"
            );
            // verify foo was linked
            assert!(
                test_path.join("dst").join("foo").exists(),
                "foo should be linked"
            );
            // verify bar was not linked (not matching include pattern)
            assert!(
                !test_path.join("dst").join("bar").exists(),
                "bar should not be linked"
            );
            // verify empty baz directory was NOT created
            assert!(
                !test_path.join("dst").join("baz").exists(),
                "empty baz directory should NOT be created"
            );
            Ok(())
        }
        /// Test that directories with only non-matching content are not created at destination.
        /// This is different from empty directories - the source dir has content but none matches.
        #[tokio::test]
        #[traced_test]
        async fn test_dir_with_nonmatching_content_not_created() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // src/
            //   foo (file)
            //   baz/
            //     qux (file - doesn't match 'foo')
            //     quux (file - doesn't match 'foo')
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("foo"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            tokio::fs::write(src_path.join("baz").join("qux"), "content").await?;
            tokio::fs::write(src_path.join("baz").join("quux"), "content").await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = link(
                &PROGRESS,
                &test_path,
                &src_path,
                &test_path.join("dst"),
                &None,
                &Settings {
                    copy_settings: copy::Settings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // only 'foo' should be linked
            assert_eq!(summary.hard_links_created, 1, "should link only 'foo' file");
            assert_eq!(
                summary.copy_summary.files_skipped, 2,
                "should skip 2 files (qux and quux)"
            );
            assert_eq!(
                summary.copy_summary.directories_created, 1,
                "should create only root directory (not 'baz' with non-matching content)"
            );
            // verify foo was linked
            assert!(
                test_path.join("dst").join("foo").exists(),
                "foo should be linked"
            );
            // verify baz directory was NOT created (even though source baz has content)
            assert!(
                !test_path.join("dst").join("baz").exists(),
                "baz directory should NOT be created (no matching content inside)"
            );
            Ok(())
        }
        /// Test that empty directories are not reported as created in dry-run mode
        /// when they were only traversed.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_empty_dir_not_reported_as_created() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // src/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("foo"), "content").await?;
            tokio::fs::write(src_path.join("bar"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = link(
                &PROGRESS,
                &test_path,
                &src_path,
                &test_path.join("dst"),
                &None,
                &Settings {
                    copy_settings: copy::Settings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: Some(crate::config::DryRunMode::Explain),
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // only 'foo' should be reported as would-be-linked
            assert_eq!(
                summary.hard_links_created, 1,
                "should report only 'foo' would be linked"
            );
            assert_eq!(
                summary.copy_summary.directories_created, 1,
                "should report only root directory would be created (not empty 'baz')"
            );
            // verify nothing was actually created (dry-run mode)
            assert!(
                !test_path.join("dst").exists(),
                "dst should not exist in dry-run"
            );
            Ok(())
        }
        /// Test that existing directories are NOT removed when using --overwrite,
        /// even if nothing is linked into them due to filters.
        #[tokio::test]
        #[traced_test]
        async fn test_existing_dir_not_removed_with_overwrite() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create source structure:
            // src/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("foo"), "content").await?;
            tokio::fs::write(src_path.join("bar"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            // create destination with baz directory already existing
            let dst_path = test_path.join("dst");
            tokio::fs::create_dir(&dst_path).await?;
            tokio::fs::create_dir(dst_path.join("baz")).await?;
            // add a marker file inside dst/baz to verify we don't touch it
            tokio::fs::write(dst_path.join("baz").join("marker.txt"), "existing").await?;
            // include only 'foo' file - baz should not match
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = link(
                &PROGRESS,
                &test_path,
                &src_path,
                &dst_path,
                &None,
                &Settings {
                    copy_settings: copy::Settings {
                        dereference: false,
                        fail_early: false,
                        overwrite: true, // enable overwrite mode
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // foo should be linked
            assert_eq!(summary.hard_links_created, 1, "should link only 'foo' file");
            // dst and baz should be unchanged (both already existed)
            assert_eq!(
                summary.copy_summary.directories_unchanged, 2,
                "root dst and baz directories should be unchanged"
            );
            assert_eq!(
                summary.copy_summary.directories_created, 0,
                "should not create any directories"
            );
            // verify foo was linked
            assert!(dst_path.join("foo").exists(), "foo should be linked");
            // verify bar was NOT linked
            assert!(!dst_path.join("bar").exists(), "bar should not be linked");
            // verify existing baz directory still exists with its content
            assert!(
                dst_path.join("baz").exists(),
                "existing baz directory should still exist"
            );
            assert!(
                dst_path.join("baz").join("marker.txt").exists(),
                "existing content in baz should still exist"
            );
            Ok(())
        }

        /// Regression: an update-only entry matching an `--exclude` pattern must NOT be copied to
        /// the destination when `--delete` is OFF. The fd-based link delegates update-only entries
        /// to `copy::copy_child` (which wraps `copy_internal` and does not re-apply a top-level
        /// filter), so the update loop must evaluate the filter itself — independently of `--delete`
        /// — and skip the delegation, matching the old path-based `copy_with_filter_base`.
        #[tokio::test]
        #[traced_test]
        async fn update_only_excluded_entry_not_copied_without_delete() -> Result<(), anyhow::Error>
        {
            let test_path = testutils::create_temp_dir().await?;
            // src has `keep.txt`; update has `keep.txt` (also in src) plus update-only `extra.txt`
            // and `wanted.txt`. With `--exclude extra.txt` and NO `--delete`, `extra.txt` must be
            // skipped while `wanted.txt` is copied.
            let src = test_path.join("src");
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::write(src.join("keep.txt"), "keep").await?;
            tokio::fs::write(update.join("keep.txt"), "keep").await?;
            tokio::fs::write(update.join("extra.txt"), "EXCLUDED").await?;
            tokio::fs::write(update.join("wanted.txt"), "wanted").await?;

            let mut filter = FilterSettings::new();
            filter.add_exclude("extra.txt").unwrap();
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter);
            // --delete is OFF (the bug only manifests with delete off).
            assert!(settings.copy_settings.delete.is_none());

            let summary = link(
                &PROGRESS,
                &test_path,
                &src,
                &dst,
                &Some(update.clone()),
                &settings,
                false,
            )
            .await?;

            assert!(
                !dst.join("extra.txt").exists(),
                "update-only entry matching --exclude must NOT be copied when --delete is off"
            );
            assert!(
                dst.join("wanted.txt").exists(),
                "non-excluded update-only entry should be copied"
            );
            assert!(dst.join("keep.txt").exists(), "shared entry should exist");
            assert_eq!(
                summary.copy_summary.files_skipped, 1,
                "the excluded update-only file should be counted skipped"
            );
            Ok(())
        }

        /// Verify a hard-link relationship between two paths by inode + device identity.
        fn are_hardlinked(a: &std::path::Path, b: &std::path::Path) -> bool {
            use std::os::unix::fs::MetadataExt;
            match (std::fs::symlink_metadata(a), std::fs::symlink_metadata(b)) {
                (Ok(ma), Ok(mb)) => ma.ino() == mb.ino() && ma.dev() == mb.dev(),
                _ => false,
            }
        }

        /// The chatgpt-codex re-review scenario (PR #247): in rlink's dual-tree walk the source
        /// loop evaluates the filter against the SOURCE entry's type. When `src/cache` is a FILE
        /// and `update/cache` is a DIRECTORY, a dir-only exclude `cache/` passes the src file (a
        /// dir-only pattern doesn't match a file), so `link_internal` runs and hits its
        /// type-mismatch branch. Before the fix that branch unconditionally delegated a copy of the
        /// UPDATE entry — and `copy_child` does not re-apply the top-level filter to the delegated
        /// root — so the excluded `cache/` directory was copied. The fix re-checks the filter using
        /// the UPDATE entry's type; the excluded update is dropped and, under union (`--update`)
        /// semantics, the src `cache` FILE is materialized instead.
        ///
        /// This test FAILS without the fix: `dst/cache` is created as the excluded update directory
        /// (and the src file is not materialized).
        #[tokio::test]
        #[traced_test]
        async fn type_mismatch_excluded_update_dir_not_copied_src_file_kept()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            // src `cache` is a FILE; update `cache` is a DIRECTORY (the type mismatch).
            tokio::fs::write(src.join("cache"), "SRC-FILE").await?;
            tokio::fs::create_dir(update.join("cache")).await?;
            tokio::fs::write(update.join("cache").join("inner.dat"), "EXCLUDED").await?;
            // a non-conflicting shared file to confirm normal linking still happens.
            tokio::fs::write(src.join("keep.txt"), "keep").await?;
            tokio::fs::write(update.join("keep.txt"), "keep").await?;
            // pin an identical mtime (incl. nsec) on both `keep.txt` copies so the
            // size+mtime `update_compare` deterministically treats them as unchanged and
            // hard-links from src. Two separate writes can otherwise land on different
            // nanoseconds, flakily comparing as changed and copying instead (the bytes are
            // identical either way) — see PR #247 CI flake on test-musl-debug.
            let keep_mtime = filetime::FileTime::from_unix_time(1_700_000_000, 0);
            filetime::set_file_mtime(src.join("keep.txt"), keep_mtime)?;
            filetime::set_file_mtime(update.join("keep.txt"), keep_mtime)?;

            let mut filter = FilterSettings::new();
            filter.add_exclude("cache/").unwrap(); // dir-only: matches a dir `cache`, not a file
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter);
            assert!(settings.copy_settings.delete.is_none());

            let summary = link(
                &PROGRESS,
                &test_path,
                &src,
                &dst,
                &Some(update.clone()),
                &settings,
                false,
            )
            .await?;

            // the excluded update directory must NOT be copied.
            assert!(
                !dst.join("cache").join("inner.dat").exists(),
                "excluded update directory `cache/` must not be copied"
            );
            assert!(
                !dst.join("cache").is_dir(),
                "dst/cache must not be the excluded update directory"
            );
            // the src `cache` FILE stands (union semantics) and is hard-linked from src.
            assert!(
                dst.join("cache").is_file(),
                "src `cache` file must be materialized when the update dir is excluded"
            );
            assert_eq!(
                tokio::fs::read_to_string(dst.join("cache")).await?,
                "SRC-FILE"
            );
            assert!(
                are_hardlinked(&src.join("cache"), &dst.join("cache")),
                "the src `cache` file must be hard-linked into the destination"
            );
            assert!(
                dst.join("keep.txt").exists(),
                "shared entry should still link"
            );
            // `cache` (src file, union) and `keep.txt` (unchanged) are both hard-linked from src;
            // nothing is copied. Exactly one directory is created — the `dst` root — proving the
            // excluded `cache/` subtree added no directory.
            assert_eq!(summary.hard_links_created, 2);
            assert_eq!(summary.copy_summary.files_copied, 0);
            assert_eq!(
                summary.copy_summary.directories_created, 1,
                "only the dst root is created; the excluded `cache/` dir must not be"
            );
            Ok(())
        }

        /// The REVERSE type mismatch (the symmetric code path): `src/data` is a DIRECTORY and
        /// `update/data` is a FILE. The dir-only include `data/` matches the directory form of the
        /// name but not the file form, and `data/**` includes the directory's contents. So the src
        /// `data` directory (and its `inner.txt`) passes the filter and the src loop spawns the
        /// worker, while the update `data` FILE is `ExcludedByDefault` (no include matches a file
        /// named `data`). The type-mismatch branch re-checks the filter using the update FILE's
        /// type, finds it excluded, and (union semantics) materializes the src DIRECTORY instead of
        /// copying the excluded update file. Without the fix the excluded update file would replace
        /// the src directory at the destination.
        #[tokio::test]
        #[traced_test]
        async fn reverse_type_mismatch_excluded_update_file_not_copied_src_dir_kept()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            // src `data` is a DIRECTORY (with a file inside); update `data` is a FILE.
            tokio::fs::create_dir(src.join("data")).await?;
            tokio::fs::write(src.join("data").join("inner.txt"), "SRC-DIR-CONTENT").await?;
            tokio::fs::write(update.join("data"), "UPDATE-FILE-EXCLUDED").await?;

            // `data/` (dir-only) includes the directory form of the name; `data/**` includes its
            // contents. The update FILE `data` matches neither and is excluded by type — the
            // symmetric form of the bot's scenario.
            let mut filter = FilterSettings::new();
            filter.add_include("data/").unwrap();
            filter.add_include("data/**").unwrap();
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter);
            assert!(settings.copy_settings.delete.is_none());

            link(
                &PROGRESS,
                &test_path,
                &src,
                &dst,
                &Some(update.clone()),
                &settings,
                false,
            )
            .await?;

            // the excluded update FILE must NOT overwrite/replace the src directory.
            assert!(
                dst.join("data").is_dir(),
                "src `data` directory must be materialized when the update file is excluded"
            );
            assert!(
                dst.join("data").join("inner.txt").exists(),
                "src directory contents must be linked through"
            );
            assert!(
                are_hardlinked(
                    &src.join("data").join("inner.txt"),
                    &dst.join("data").join("inner.txt")
                ),
                "src directory's file must be hard-linked into the destination"
            );
            Ok(())
        }

        /// `--update-exclusive` + the type-mismatch scenario: src `cache` is a FILE, update `cache`
        /// is a DIRECTORY excluded by `cache/`. Under exclusive mode only the (filter-passing)
        /// update set materializes, so an EXCLUDED update entry materializes NOTHING — the src is
        /// not materialized (it is not a fallback under exclusivity), and no stale src copy is left.
        /// This mirrors the NotFound-under-exclusive case (`return Ok(Default::default())`).
        #[tokio::test]
        #[traced_test]
        async fn type_mismatch_excluded_update_dir_update_exclusive_materializes_nothing()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::write(src.join("cache"), "SRC-FILE").await?;
            tokio::fs::create_dir(update.join("cache")).await?;
            tokio::fs::write(update.join("cache").join("inner.dat"), "EXCLUDED").await?;
            // a filter-passing update-only file proves the rest of the exclusive copy still works.
            tokio::fs::write(update.join("wanted.txt"), "wanted").await?;

            let mut filter = FilterSettings::new();
            filter.add_exclude("cache/").unwrap();
            let mut settings = common_settings(false, false);
            settings.update_exclusive = true;
            settings.filter = Some(filter);

            link(
                &PROGRESS,
                &test_path,
                &src,
                &dst,
                &Some(update.clone()),
                &settings,
                false,
            )
            .await?;

            assert!(
                !dst.join("cache").exists(),
                "under --update-exclusive an excluded-update type-mismatch must materialize nothing \
                 (no excluded dir, no stale src file)"
            );
            assert!(
                dst.join("wanted.txt").exists(),
                "filter-passing update-only entries are still copied under --update-exclusive"
            );
            Ok(())
        }

        /// `--delete` + the type-mismatch scenario under normal `--update`: the src `cache` FILE is
        /// materialized (union) and MUST be retained by the keep-set — never materialized-then-pruned
        /// — while a pre-existing extraneous dst entry is removed. Also confirms the excluded update
        /// directory leaves no leftover. `prune_extraneous` would otherwise prune the dst `cache`
        /// file (a dir-only `cache/` exclude does not protect a file), so correctness depends on
        /// `cache` staying in the keep-set.
        #[tokio::test]
        #[traced_test]
        async fn type_mismatch_excluded_update_dir_delete_keeps_src_file()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::create_dir(&dst).await?;
            tokio::fs::write(src.join("cache"), "SRC-FILE").await?;
            tokio::fs::create_dir(update.join("cache")).await?;
            tokio::fs::write(update.join("cache").join("inner.dat"), "EXCLUDED").await?;
            // pre-existing extraneous dst entry that --delete should prune.
            tokio::fs::write(dst.join("stale.txt"), "stale").await?;

            let mut filter = FilterSettings::new();
            filter.add_exclude("cache/").unwrap();
            let mut settings = common_settings(false, true); // --delete implies --overwrite
            settings.filter = Some(filter);
            settings.copy_settings.delete = Some(copy::DeleteSettings {
                delete_excluded: false,
            });

            link(
                &PROGRESS,
                &test_path,
                &src,
                &dst,
                &Some(update.clone()),
                &settings,
                false,
            )
            .await?;

            assert!(
                dst.join("cache").is_file(),
                "src `cache` file must survive --delete (kept in the keep-set, not pruned)"
            );
            assert_eq!(
                tokio::fs::read_to_string(dst.join("cache")).await?,
                "SRC-FILE"
            );
            assert!(
                !dst.join("cache").is_dir(),
                "the excluded update directory must leave no leftover"
            );
            assert!(
                !dst.join("stale.txt").exists(),
                "extraneous dst entry must be pruned by --delete"
            );
            Ok(())
        }
    }
    mod dry_run_tests {
        use super::*;
        /// Test that dry-run mode for files doesn't create hard links.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_file_does_not_create_link() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let src_file = test_path.join("foo/0.txt");
            let dst_file = test_path.join("dst_link.txt");
            // verify destination doesn't exist
            assert!(
                !dst_file.exists(),
                "destination should not exist before dry-run"
            );
            let summary = link(
                &PROGRESS,
                test_path,
                &src_file,
                &dst_file,
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // verify destination still doesn't exist
            assert!(!dst_file.exists(), "dry-run should not create hard link");
            // verify summary reports what would be created
            assert_eq!(
                summary.hard_links_created, 1,
                "dry-run should report 1 hard link that would be created"
            );
            Ok(())
        }
        /// Test that dry-run mode for directories doesn't create the destination directory.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_directory_does_not_create_destination() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let dst_path = test_path.join("nonexistent_dst");
            // verify destination doesn't exist
            assert!(
                !dst_path.exists(),
                "destination should not exist before dry-run"
            );
            let summary = link(
                &PROGRESS,
                test_path,
                &test_path.join("foo"),
                &dst_path,
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // verify destination still doesn't exist
            assert!(
                !dst_path.exists(),
                "dry-run should not create destination directory"
            );
            // verify summary reports what would be created
            assert!(
                summary.hard_links_created > 0,
                "dry-run should report hard links that would be created"
            );
            Ok(())
        }
        /// Test that dry-run mode correctly reports symlinks (not as hard links).
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_symlinks_counted_correctly() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // baz contains: 4.txt (file), 5.txt (symlink), 6.txt (symlink)
            let src_path = test_path.join("foo/baz");
            let dst_path = test_path.join("dst_baz");
            // verify destination doesn't exist
            assert!(
                !dst_path.exists(),
                "destination should not exist before dry-run"
            );
            let summary = link(
                &PROGRESS,
                test_path,
                &src_path,
                &dst_path,
                &None,
                &Settings {
                    copy_settings: CopySettings {
                        dereference: false,
                        fail_early: false,
                        overwrite: false,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
                    preserve: preserve::preserve_all(),
                },
                false,
            )
            .await?;
            // verify destination still doesn't exist
            assert!(!dst_path.exists(), "dry-run should not create destination");
            // baz contains 1 regular file (4.txt) and 2 symlinks (5.txt, 6.txt)
            assert_eq!(
                summary.hard_links_created, 1,
                "dry-run should report 1 hard link (for 4.txt)"
            );
            assert_eq!(
                summary.copy_summary.symlinks_created, 2,
                "dry-run should report 2 symlinks (5.txt and 6.txt)"
            );
            Ok(())
        }
    }

    /// Verify that fail-early preserves the summary from the failing subtree.
    ///
    /// Regression test: the fail-early return path in the join loop must
    /// accumulate error.summary from the failing child into the parent's
    /// link_summary. Without this, directories_created from the child subtree
    /// would be lost.
    #[tokio::test]
    #[traced_test]
    async fn test_fail_early_preserves_summary_from_failing_subtree() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // src/sub/  has a file and an unreadable subdirectory:
        //   src/sub/good.txt            <-- links successfully
        //   src/sub/unreadable_dir/     <-- mode 000, can't be traversed
        //     src/sub/unreadable_dir/f.txt
        let src_dir = test_path.join("src");
        let sub_dir = src_dir.join("sub");
        let bad_dir = sub_dir.join("unreadable_dir");
        tokio::fs::create_dir_all(&bad_dir).await?;
        tokio::fs::write(sub_dir.join("good.txt"), "content").await?;
        tokio::fs::write(bad_dir.join("f.txt"), "data").await?;
        tokio::fs::set_permissions(&bad_dir, std::fs::Permissions::from_mode(0o000)).await?;
        let dst_dir = test_path.join("dst");
        let result = link(
            &PROGRESS,
            test_path,
            &src_dir,
            &dst_dir,
            &None,
            &Settings {
                copy_settings: CopySettings {
                    fail_early: true,
                    ..common_settings(false, false).copy_settings
                },
                ..common_settings(false, false)
            },
            false,
        )
        .await;
        // restore permissions for cleanup
        tokio::fs::set_permissions(&bad_dir, std::fs::Permissions::from_mode(0o755)).await?;
        let error = result.expect_err("link should fail due to unreadable directory");
        // sub/'s link_internal created dst/sub/ (directories_created=1) before
        // its join loop encountered the unreadable_dir error. that directory
        // creation must be reflected in the error summary propagated up to the
        // top-level caller.
        assert!(
            error.summary.copy_summary.directories_created >= 2,
            "fail-early summary should include directories from the failing subtree, \
             got directories_created={} (expected >= 2: dst/ and dst/sub/)",
            error.summary.copy_summary.directories_created
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_specials_skips_socket_in_link() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("src_dir");
        let dst = test_path.join("dst_dir");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::write(src.join("file.txt"), "hello").await?;
        let _listener = std::os::unix::net::UnixListener::bind(src.join("test.sock"))?;
        let mut settings = common_settings(false, false);
        settings.copy_settings.skip_specials = true;
        let summary = link(&PROGRESS, test_path, &src, &dst, &None, &settings, false).await?;
        assert_eq!(summary.hard_links_created, 1);
        assert_eq!(summary.copy_summary.specials_skipped, 1);
        assert!(dst.join("file.txt").exists());
        assert!(!dst.join("test.sock").exists());
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_skips_pruning_when_link_has_errors() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("foo");
        let dst = test_path.join("bar");
        // baseline link establishes the destination (no delete)
        link(
            &PROGRESS,
            test_path,
            &src,
            &dst,
            &None,
            &common_settings(false, false),
            false,
        )
        .await?;
        // an extraneous file that --delete would normally prune
        tokio::fs::write(dst.join("extraneous.txt"), b"junk").await?;
        // make a source sub-directory unreadable so traversal fails (fail_early is false).
        // a directory is used because --overwrite with mtime-equal files skips copying
        // identical files; a directory's read_dir fails unconditionally when mode is 0o000.
        let unreadable = src.join("baz");
        let original = tokio::fs::metadata(&unreadable).await?.permissions();
        tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).await?;

        let delete_settings = Settings {
            copy_settings: CopySettings {
                overwrite: true,
                fail_early: false,
                delete: Some(copy::DeleteSettings {
                    delete_excluded: false,
                }),
                ..common_settings(false, true).copy_settings
            },
            ..common_settings(false, true)
        };
        let result = link(
            &PROGRESS,
            test_path,
            &src,
            &dst,
            &None,
            &delete_settings,
            false,
        )
        .await;

        tokio::fs::set_permissions(&unreadable, original).await?;

        assert!(
            result.is_err(),
            "link of the unreadable directory should fail"
        );
        assert!(
            dst.join("extraneous.txt").exists(),
            "pruning must be skipped when the link/update pass reported errors"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_specials_top_level_socket_in_link() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src_socket = test_path.join("test.sock");
        let dst = test_path.join("dst.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&src_socket)?;
        let mut settings = common_settings(false, false);
        settings.copy_settings.skip_specials = true;
        let summary = link(
            &PROGRESS,
            test_path,
            &src_socket,
            &dst,
            &None,
            &settings,
            false,
        )
        .await?;
        assert_eq!(summary.copy_summary.specials_skipped, 1);
        assert_eq!(summary.hard_links_created, 0);
        assert!(!dst.exists());
        Ok(())
    }

    /// Stress tests exercising max-open-files saturation during link.
    mod max_open_files_tests {
        use super::*;

        /// deep + wide link: directory tree deeper than the open-files limit, with files
        /// at every level. verifies no deadlock occurs (directories don't consume permits).
        #[tokio::test]
        #[traced_test]
        async fn deep_tree_no_deadlock_under_open_files_saturation() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let dst = tmp_dir.join("dst");
            let depth = 20;
            let files_per_level = 5;
            let limit = 4;
            // create a directory chain deeper than the permit limit, with files at each level
            let mut dir = src.clone();
            for level in 0..depth {
                tokio::fs::create_dir_all(&dir).await?;
                for f in 0..files_per_level {
                    tokio::fs::write(
                        dir.join(format!("f{}_{}.txt", level, f)),
                        format!("L{}F{}", level, f),
                    )
                    .await?;
                }
                dir = dir.join(format!("d{}", level));
            }
            throttle::set_max_open_files(limit);
            let summary = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                link(
                    &PROGRESS,
                    tmp_dir.as_path(),
                    &src,
                    &dst,
                    &None,
                    &common_settings(false, false),
                    false,
                ),
            )
            .await
            .context("link timed out — possible deadlock")?
            .context("link failed")?;
            assert_eq!(summary.hard_links_created, depth * files_per_level);
            assert_eq!(summary.copy_summary.directories_created, depth);
            // spot-check that hard links work by reading content at a few levels
            let mut check_dir = dst.clone();
            for level in 0..depth {
                let content =
                    tokio::fs::read_to_string(check_dir.join(format!("f{}_0.txt", level))).await?;
                assert_eq!(content, format!("L{}F0", level));
                check_dir = check_dir.join(format!("d{}", level));
            }
            Ok(())
        }

        /// Regression: link_internal's spawn-time guard must be released before
        /// delegating to copy::copy on the file-type-changed path.
        ///
        /// Scenario: many src entries are regular files (so the spawn loop
        /// pre-acquires open-files permits for them), but the corresponding
        /// `update` entries are directories (file types differ). link_internal
        /// then calls copy::copy on the update directory, which enters
        /// copy_internal. If the spawn-time permit were still held while
        /// copy::copy ran, copy_internal's own open-files acquire for any
        /// inner file would deadlock against a saturated pool.
        #[tokio::test]
        #[traced_test]
        async fn parallel_update_filetype_change_no_deadlock() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let update = tmp_dir.join("update");
            let dst = tmp_dir.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            let n = 8;
            // src/eN: regular files. update/eN: directories with inner files.
            // file types differ -> link takes the !is_file_type_same branch
            // -> calls copy::copy(update/eN, dst/eN).
            for i in 0..n {
                tokio::fs::write(src.join(format!("e{}", i)), format!("src-{}", i)).await?;
                let upd_subdir = update.join(format!("e{}", i));
                tokio::fs::create_dir(&upd_subdir).await?;
                for j in 0..3 {
                    tokio::fs::write(
                        upd_subdir.join(format!("inner_{}.txt", j)),
                        format!("upd-{}-{}", i, j),
                    )
                    .await?;
                }
            }
            // saturate the open-files pool: spawn-time permits held by every
            // outer link task would block copy::copy's inner permit acquires.
            throttle::set_max_open_files(2);
            let summary = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                link(
                    &PROGRESS,
                    tmp_dir.as_path(),
                    &src,
                    &dst,
                    &Some(update.clone()),
                    &common_settings(false, false),
                    false,
                ),
            )
            .await
            .context(
                "link timed out — caller-supplied open-files guard not released before copy::copy",
            )?
            .context("link failed")?;
            // every entry was a type-mismatch -> copied from update.
            // copy::copy on a directory creates the dir and copies inner files.
            assert_eq!(summary.copy_summary.directories_created, n + 1); // +1 for dst itself
            assert_eq!(summary.copy_summary.files_copied, n * 3);
            // verify content came from update, not src
            for i in 0..n {
                for j in 0..3 {
                    let content =
                        tokio::fs::read_to_string(dst.join(format!("e{}/inner_{}.txt", i, j)))
                            .await?;
                    assert_eq!(content, format!("upd-{}-{}", i, j));
                }
            }
            Ok(())
        }

        /// Regression: the "update-only entries" spawn loop must not deadlock
        /// against copy::copy's open-files OR against rm::rm's pending-meta.
        ///
        /// Scenario: update has many regular files that don't exist in src.
        /// The loop at site 3 spawns a copy::copy task per entry under a
        /// saturated open-files pool. copy::copy's internal acquires must
        /// proceed normally — site 3 must not be holding open-files.
        #[tokio::test]
        #[traced_test]
        async fn update_only_entries_bounded_no_deadlock() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let update = tmp_dir.join("update");
            let dst = tmp_dir.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            // src is empty; update has many regular files. Every update entry
            // is "missing in src" -> hits the site-3 spawn loop.
            let n = 50;
            for i in 0..n {
                tokio::fs::write(update.join(format!("u{}", i)), format!("upd-{}", i)).await?;
            }
            throttle::set_max_open_files(2);
            let summary = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                link(
                    &PROGRESS,
                    tmp_dir.as_path(),
                    &src,
                    &dst,
                    &Some(update.clone()),
                    &common_settings(false, false),
                    false,
                ),
            )
            .await
            .context("link timed out — site-3 spawn loop deadlock")?
            .context("link failed")?;
            // dst gets the src directory plus a copy of every update file
            assert_eq!(summary.copy_summary.directories_created, 1);
            assert_eq!(summary.copy_summary.files_copied, n);
            for i in 0..n {
                let content = tokio::fs::read_to_string(dst.join(format!("u{}", i))).await?;
                assert_eq!(content, format!("upd-{}", i));
            }
            Ok(())
        }

        /// Regression for the link site-3 ↔ rm pending-meta self-deadlock.
        ///
        /// Scenario: update has many entries not in src; dst already has
        /// directories at those same names; the user passes --overwrite. Each
        /// site-3 task runs copy::copy → copy_file → rm::rm to remove the
        /// preexisting dst directory before placing the regular-file copy.
        /// rm::rm draws from the pending-meta pool. If site 3 also held
        /// pending-meta across copy::copy, every running task would hold a
        /// permit while waiting on inner rm to acquire one — classic
        /// self-deadlock once the pool is saturated.
        #[tokio::test]
        #[traced_test]
        async fn update_only_overwrite_preexisting_dirs_no_deadlock() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let update = tmp_dir.join("update");
            let dst = tmp_dir.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::create_dir(&dst).await?;
            let n = 12;
            for i in 0..n {
                // update/uN is a regular file (site 3 will copy it).
                tokio::fs::write(update.join(format!("u{}", i)), format!("upd-{}", i)).await?;
                // dst/uN is a preexisting directory with inner files. With
                // --overwrite, copy_file calls rm::rm to wipe it, which
                // recurses into pending-meta.
                let dst_subdir = dst.join(format!("u{}", i));
                tokio::fs::create_dir(&dst_subdir).await?;
                for j in 0..3 {
                    tokio::fs::write(
                        dst_subdir.join(format!("inner_{}.txt", j)),
                        format!("old-{}-{}", i, j),
                    )
                    .await?;
                }
            }
            // saturate both pools to force the deadlock if the cycle existed.
            throttle::set_max_open_files(2);
            let summary = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                link(
                    &PROGRESS,
                    tmp_dir.as_path(),
                    &src,
                    &dst,
                    &Some(update.clone()),
                    &common_settings(false, true), // overwrite=true
                    false,
                ),
            )
            .await
            .context("link timed out — pending-meta self-deadlock between site 3 and inner rm")?
            .context("link failed")?;
            // each preexisting dst/uN directory gets removed and replaced
            // with a regular-file copy from update/uN.
            assert_eq!(summary.copy_summary.files_copied, n);
            assert_eq!(summary.copy_summary.rm_summary.files_removed, n * 3);
            assert_eq!(summary.copy_summary.rm_summary.directories_removed, n);
            // verify content came from update
            for i in 0..n {
                let content = tokio::fs::read_to_string(dst.join(format!("u{}", i))).await?;
                assert_eq!(content, format!("upd-{}", i));
            }
            Ok(())
        }
    }

    /// TOCTOU hardening: a source entry being hard-linked is concurrently swapped between a real
    /// regular file and a symlink to a sentinel OUTSIDE the source tree. rlink classifies the entry
    /// via `child` (fstat) before acting and links the pinned inode inode-exactly
    /// (`hard_link_handle_at`), so a swap is either caught (the entry is linked/copied as a symlink,
    /// or the op fails closed) or the real file is hard-linked. The sentinel's secret content must
    /// NEVER appear at the destination as a regular file, and the sentinel inode must never gain a
    /// new hard link.
    mod race_tests {
        use super::*;

        // Repeatedly swap `dir/entry_name` between a real regular file (content `REAL_CONTENT`) and
        // a symlink pointing at `sentinel`, using rename so each individual state is atomic. Runs on
        // a dedicated OS thread so it makes progress regardless of the tokio runtime's scheduling.
        fn spawn_file_symlink_swapper(
            dir: std::path::PathBuf,
            entry_name: &'static str,
            sentinel: std::path::PathBuf,
            stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        ) -> std::thread::JoinHandle<()> {
            std::thread::spawn(move || {
                let entry = dir.join(entry_name);
                let staged_real = dir.join("__staged_real");
                let staged_link = dir.join("__staged_link");
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = std::fs::remove_file(&staged_real);
                    if std::fs::write(&staged_real, b"REAL_CONTENT").is_err() {
                        continue;
                    }
                    let _ = std::fs::rename(&staged_real, &entry);
                    let _ = std::fs::remove_file(&staged_link);
                    let _ = std::os::unix::fs::symlink(&sentinel, &staged_link);
                    let _ = std::fs::rename(&staged_link, &entry);
                }
            })
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        #[traced_test]
        async fn hard_link_entry_swap_never_leaks_sentinel() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let test_path = tmp_dir.as_path();
            // sentinel lives OUTSIDE the source tree with distinctive content; we also track its
            // hard-link count to prove `linkat(flags=0)` never gives it a new hard link.
            let sentinel = test_path.join("sentinel_secret");
            tokio::fs::write(&sentinel, "SENTINEL_SECRET_CONTENT").await?;
            let sentinel_links_before = {
                use std::os::unix::fs::MetadataExt;
                tokio::fs::symlink_metadata(&sentinel).await?.nlink()
            };
            let src = test_path.join("src");
            let sub = src.join("sub");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&sub).await?;
            tokio::fs::write(sub.join("entry"), "REAL_CONTENT").await?;

            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let swapper =
                spawn_file_symlink_swapper(sub.clone(), "entry", sentinel.clone(), stop.clone());

            // overwrite=true so each iteration's destination need not be empty; no update tree, so
            // `src/sub/entry` takes the hard-link path (or copy-as-symlink when caught mid-swap).
            let settings = common_settings(false, true);
            let mut caught_swaps = 0usize;
            let mut linked_real = 0usize;
            for i in 0..200 {
                let dst = test_path.join(format!("dst_{i}"));
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    link(&PROGRESS, test_path, &src, &dst, &None, &settings, false),
                )
                .await
                .expect("link must not hang under concurrent swapping");
                match result {
                    Ok(_) => {}
                    Err(_) => caught_swaps += 1, // a swap was caught mid-link (failed closed)
                }
                // CORE ASSERTION: if a regular file landed at the destination it holds the REAL
                // content — never the sentinel's secret. The entry may instead be a symlink
                // (linkat made a hard link to the symlink inode, or copy reproduced the symlink) or
                // be absent. A symlink that resolves to the sentinel is fine: it is a link, not a
                // copy of the secret bytes, and it did not give the sentinel a new hard link.
                let entry_dst = dst.join("sub").join("entry");
                if let Ok(md) = tokio::fs::symlink_metadata(&entry_dst).await
                    && md.file_type().is_file()
                {
                    let content = tokio::fs::read_to_string(&entry_dst).await?;
                    assert_ne!(
                        content, "SENTINEL_SECRET_CONTENT",
                        "iteration {i}: sentinel content leaked into the destination as a regular file"
                    );
                    assert_eq!(
                        content, "REAL_CONTENT",
                        "iteration {i}: a regular destination file must hold the real content"
                    );
                    linked_real += 1;
                }
                let _ = tokio::fs::remove_dir_all(&dst).await;
            }

            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            swapper.join().expect("swapper thread panicked");

            // the sentinel must never have gained a hard link from a `linkat` that followed the
            // swapped-in symlink (flags=0 links the symlink inode itself, not its target).
            let sentinel_links_after = {
                use std::os::unix::fs::MetadataExt;
                tokio::fs::symlink_metadata(&sentinel).await?.nlink()
            };
            assert_eq!(
                sentinel_links_after, sentinel_links_before,
                "the sentinel file must never gain a hard link (linkat must not follow the symlink)"
            );
            // sanity: the run did observable work (this is not the safety assertion — the safety
            // assertions above hold on every iteration regardless of timing).
            tracing::info!(
                "link file/symlink swap: caught_swaps={caught_swaps}, linked_real={linked_real}"
            );
            assert!(
                caught_swaps + linked_real > 0,
                "expected at least one observable outcome across 200 iterations"
            );
            Ok(())
        }
    }
}
