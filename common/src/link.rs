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
use crate::safedir::{self, Dir, Handle};
use crate::walk::{
    self, AdmittedEntry, AdmittedLeaf, EntryAdmission, EntryKind, LeafPermit, PermitKind,
};

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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum UpdateRootRequirement {
    Optional,
    RequiredForDestructiveOperation,
}

impl UpdateRootRequirement {
    fn from_settings(settings: &Settings) -> Self {
        if settings.update_exclusive || settings.copy_settings.delete.is_some() {
            Self::RequiredForDestructiveOperation
        } else {
            Self::Optional
        }
    }

    fn is_required(self) -> bool {
        self == Self::RequiredForDestructiveOperation
    }

    fn for_entry(self, rel_path: &std::path::Path) -> Self {
        if rel_path.as_os_str().is_empty() {
            self
        } else {
            Self::Optional
        }
    }

    #[allow(clippy::result_large_err)]
    fn classify<T>(
        self,
        result: std::io::Result<T>,
        update_path: &std::path::Path,
    ) -> Result<Option<T>, Error> {
        match result {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !self.is_required() => {
                Ok(None)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(missing_destructive_update_error(update_path))
            }
            Err(error) => Err(Error::new(
                anyhow::Error::new(error)
                    .context(format!("failed reading metadata from {update_path:?}")),
                Default::default(),
            )),
        }
    }
}

fn missing_destructive_update_error(update_path: &std::path::Path) -> Error {
    Error::new(
        anyhow!(
            "--update path {:?} does not exist (rejected under --delete or --update-exclusive to avoid silently pruning destination entries the update tree would otherwise have preserved)",
            update_path
        ),
        Default::default(),
    )
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
///
/// No metadata is applied here, and `f:acl` in particular must never reach this path: a hard-linked
/// destination SHARES the source's inode, so writing an ACL "to the destination" would rewrite the
/// SOURCE's permissions. There is nothing to apply anyway — the shared inode already carries the
/// source's metadata verbatim, which is the whole point of a hard link. `f:acl` therefore applies
/// only on rlink's real copy path ([`copy::copy_child`], for changed files under `--update`).
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
    crate::walk_driver::scope_tasks(link_inner(
        prog_track, cwd, src, dst, update, settings, is_fresh,
    ))
    .await
}

#[cfg(test)]
type AfterUpdatePrecheckHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
static AFTER_UPDATE_PRECHECK_HOOKS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, AfterUpdatePrecheckHook>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
struct AfterUpdatePrecheckHookRegistration {
    update_path: std::path::PathBuf,
}

#[cfg(test)]
impl Drop for AfterUpdatePrecheckHookRegistration {
    fn drop(&mut self) {
        AFTER_UPDATE_PRECHECK_HOOKS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.update_path);
    }
}

#[cfg(test)]
fn install_after_update_precheck_hook(
    update_path: std::path::PathBuf,
    hook: AfterUpdatePrecheckHook,
) -> AfterUpdatePrecheckHookRegistration {
    let replaced = AFTER_UPDATE_PRECHECK_HOOKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(update_path.clone(), hook);
    assert!(replaced.is_none(), "update precheck hook already installed");
    AfterUpdatePrecheckHookRegistration { update_path }
}

#[cfg(test)]
fn run_after_update_precheck_hook(update_path: &std::path::Path) {
    let hook = AFTER_UPDATE_PRECHECK_HOOKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(update_path);
    if let Some(hook) = hook {
        hook();
    }
}

async fn link_inner(
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
    let update_root_requirement = UpdateRootRequirement::from_settings(settings);
    // reserve root admission before any strict probe or operand-parent open, then transfer it into
    // link_internal. an authoritative directory releases it before dual-tree descent.
    let permit = walk::ensure_leaf_permit(PermitKind::OpenFile, None).await;
    enum LinkRootSetup {
        Complete(Summary),
        Ready {
            src_operand: crate::walk::RootOperand,
            src_parent: Arc<Dir>,
            dst_parent: Option<Arc<Dir>>,
            update_parent: Option<(Arc<Dir>, std::ffi::OsString)>,
            admission: LinkEntryAdmission,
        },
    }
    let setup_admission = permit.as_ref().map(LeafPermit::admission);
    let setup = safedir::with_optional_fd_admission(setup_admission, async move {
        // a missing --update root is destructive under both --update-exclusive (materialized set =
        // update set, so nothing materializes) AND --delete (the source-only keep_set makes any dst
        // entry the missing update tree WOULD have protected look extraneous, and prune wipes it).
        // in either case `link_internal` hits the recursive early-return / silent `None` fallback
        // before that destruction would happen, so rlink reports success — silently preserving
        // stale dst (--update-exclusive) or silently pruning would-be-protected entries (--delete).
        // reject up front so the usual error ordering catches a typo'd --update before later operand
        // work. The same typed requirement is retained through the authoritative root classification,
        // closing the race after this probe. Plain "--update without --delete or --update-exclusive"
        // still falls back to no-update mode, and recursive child-level "update missing" cases remain
        // ordinary no-op/source-only decisions.
        if let Some(update_path) = update.as_ref() {
            if crate::safedir::strict_operand_resolution() {
                // under strict operand resolution, validate the update operand prefix UP FRONT
                // (fd-relative, `openat2(RESOLVE_NO_SYMLINKS)`), unconditionally for ALL --update — so a
                // plain --update with an excluded source (which returns before the update parent would
                // otherwise be opened) cannot slip a symlinked update prefix through. A symlink in a
                // directory component fails closed (ELOOP); this also serves the destructive-mode
                // existence check below.
                let kind =
                    crate::safedir::strict_probe_dst_kind(update_path, congestion::Side::Source)
                        .await
                        .map_err(|err| {
                            Error::new(
                                anyhow::Error::new(err).context(format!(
                                    "failed reading metadata from update {update_path:?}"
                                )),
                                Default::default(),
                            )
                        })?;
                if update_root_requirement.is_required() && kind.is_none() {
                    return Err(missing_destructive_update_error(update_path));
                }
            } else if update_root_requirement.is_required() {
                match crate::walk::run_metadata_probed(
                    congestion::Side::Source,
                    congestion::MetadataOp::Stat,
                    tokio::fs::symlink_metadata(update_path),
                )
                .await
                {
                    Ok(_) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        return Err(missing_destructive_update_error(update_path));
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
        }
        #[cfg(test)]
        if let Some(update_path) = update.as_ref() {
            run_after_update_precheck_hook(update_path);
        }
        // source: decompose via the shared helper so `.`/`..` operands (e.g. `rlink . dst`, `rlink
        // tree/.. dst`) are canonicalized to a real directory + basename instead of being rejected; `/`
        // is still rejected. (The destination and `--update` operands keep their direct split below.)
        let src_operand = crate::walk::split_root_operand(src)
            .await
            .map_err(|err| Error::new(err, Default::default()))?;
        let src = src_operand.display.as_path();
        let src_name = src_operand.name.as_os_str();
        // under strict operand resolution (--require-toctou-safe) the parent prefix is opened BEFORE
        // the root filter check, so a symlinked source prefix fails closed even when the root would be
        // skipped by a filter, and the root is classified fd-relative. On the default path the filter
        // check keeps the historical path-based `symlink_metadata` classification and runs FIRST: an
        // excluded root under an execute-only (0111, searchable-not-readable) parent must skip cleanly,
        // and the O_RDONLY parent open would fail EACCES there.
        let strict_src_parent: Option<Arc<Dir>> = if crate::safedir::strict_operand_resolution() {
            let parent = Dir::open_parent_dir(&src_operand.parent, congestion::Side::Source)
                .await
                .with_context(|| {
                    format!(
                        "cannot open source parent directory {:?}",
                        src_operand.parent
                    )
                })
                .map_err(|err| Error::new(err, Default::default()))?;
            // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
            Some(Arc::new(parent.into_tree()))
        } else {
            None
        };
        // the destination's parent path, split leniently for the up-front strict validation below (a
        // `.`/`..`/`/` destination has no distinct parent+name; the authoritative split — which rejects
        // such a destination — runs AFTER the filter, preserving default-mode ordering so a filtered-out
        // source still skips cleanly). empty parent (a single-component relative path) means ".".
        let dst_parent_path_opt: Option<&std::path::Path> = match (dst.parent(), dst.file_name()) {
            (Some(parent), Some(_)) if parent.as_os_str().is_empty() => {
                Some(std::path::Path::new("."))
            }
            (Some(parent), Some(_)) => Some(parent),
            _ => None,
        };
        // under strict operand resolution, resolve the destination parent UP FRONT — before the source
        // root-filter early-return, unconditionally — so a symlinked destination prefix fails closed in
        // every mode regardless of filters/flags (see copy::copy for the same pattern). An absent parent
        // (dry-run previewing into a not-yet-created tree, or a filtered-out root) is not a symlink
        // violation; only a symlinked prefix component (ELOOP) fails closed here.
        let strict_dst_parent: Option<Arc<Dir>> = match (
            crate::safedir::strict_operand_resolution(),
            dst_parent_path_opt,
        ) {
            (true, Some(dst_parent_path)) => {
                match Dir::open_parent_dir(dst_parent_path, congestion::Side::Destination).await {
                    Ok(parent) => Some(Arc::new(parent.into_tree())),
                    Err(err)
                        if err.kind() == std::io::ErrorKind::NotFound
                            || err.raw_os_error() == Some(libc::ENOTDIR) =>
                    {
                        None
                    }
                    Err(err) => {
                        return Err(Error::new(
                            anyhow::Error::new(err).context(format!(
                                "cannot open destination parent directory {dst_parent_path:?}"
                            )),
                            Default::default(),
                        ));
                    }
                }
            }
            // default mode, or a degenerate destination (rejected by the authoritative split below)
            _ => None,
        };
        // a source-only root can preserve the historical cheap early filter. with --update, the root
        // is one logical entry backed by two independently typed operands, so defer to the joint
        // fd-relative setup decision after both handles are classified.
        if update.is_none()
            && let Some(ref filter) = settings.filter
        {
            let (kind, is_dir) = match &strict_src_parent {
                // strict: classify via the held parent fd (O_NOFOLLOW), never by path
                Some(parent) => {
                    let root_handle = parent
                        .child(src_name)
                        .await
                        .with_context(|| format!("failed reading metadata from {:?}", &src))
                        .map_err(|err| Error::new(err, Default::default()))?;
                    (root_handle.kind(), root_handle.kind() == EntryKind::Dir)
                }
                None => {
                    let src_metadata = crate::walk::run_metadata_probed(
                        congestion::Side::Source,
                        congestion::MetadataOp::Stat,
                        tokio::fs::symlink_metadata(src),
                    )
                    .await
                    .with_context(|| format!("failed reading metadata from {:?}", &src))
                    .map_err(|err| Error::new(err, Default::default()))?;
                    (
                        EntryKind::from_metadata(&src_metadata),
                        src_metadata.is_dir(),
                    )
                }
            };
            let result = filter.should_include_root_item(std::path::Path::new(src_name), is_dir);
            match result {
                crate::filter::FilterResult::Included => {}
                result => {
                    if let Some(mode) = settings.dry_run {
                        crate::dry_run::report_skip(src, &result, mode, kind.label_long());
                    }
                    kind.inc_skipped(prog_track);
                    return Ok(LinkRootSetup::Complete(skipped_summary_for(kind)));
                }
            }
        }
        // open the parent directories of the source and (optional) update roots for the
        // walk so each root entry is opened and classified relative to a directory fd — the same
        // fd-relative path every nested entry takes. The roots are then handed to `link_internal` by
        // their basenames, exactly like child entries. under strict operand resolution the source
        // parent was already opened and validated above, so reuse its held fd here.
        let src_parent = match strict_src_parent {
            Some(parent) => parent,
            None => {
                let parent = Dir::open_parent_dir(&src_operand.parent, congestion::Side::Source)
                    .await
                    .with_context(|| {
                        format!(
                            "cannot open source parent directory {:?}",
                            src_operand.parent
                        )
                    })
                    .map_err(|err| Error::new(err, Default::default()))?;
                // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
                Arc::new(parent.into_tree())
            }
        };
        // the update tree (if present) is rooted at `update`; open its parent and remember the root
        // basename so setup can classify it via the held fd. A missing root/parent silently falls
        // back only for optional plain update; destructive delete/update or update-exclusive is
        // enforced again by the authoritative typed requirement.
        //
        // for plain `--update` (no `--delete`, no `--update-exclusive`) a missing parent is treated the
        // same as a missing update root: fall back silently to no-update mode. This preserves the long-
        // standing behavior where `rlink --update /tmp/no/such src dst` (with `/tmp/no` absent) proceeds
        // by linking from `src` rather than erroring — the existing missing-update-root fallback already
        // applies to that case; the parent-open merely must not ENOENT-fail before `link_internal` can
        // apply it. Under `--delete` or `--update-exclusive`, parent failure propagates and root absence
        // is rejected again by whichever authoritative root classification site retains the handle.
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
                let fallback_eligible = !update_root_requirement.is_required();
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
        // a dual root must be filtered before destination validation, and its type authorization must
        // stay bound to the exact handles that informed it. classify both roots once, select the
        // surviving side, and thread those handles into link_internal; same-class file/directory
        // payloads retain their documented by-name semantics below that authorization boundary.
        let admission = match (settings.filter.as_ref(), update.as_ref()) {
            (Some(filter), Some(update_path)) => {
                let src_handle = src_parent
                    .child(src_name)
                    .await
                    .with_context(|| format!("failed reading metadata from {src:?}"))
                    .map_err(|err| Error::new(err, Default::default()))?;
                let mut entry = AdmittedLinkEntry::new(src_handle, permit);
                entry.update_handle = match update_parent.as_ref() {
                    Some((parent, update_name)) => update_root_requirement
                        .classify(parent.child(update_name).await, update_path)?,
                    None => None,
                };
                if settings.update_exclusive && entry.update_handle.is_none() {
                    return Ok(LinkRootSetup::Complete(Default::default()));
                }
                match select_filtered_root(
                    filter,
                    src_name,
                    entry.src_handle.kind(),
                    entry.update_handle.as_ref().map(Handle::kind),
                    settings.update_exclusive,
                ) {
                    RootFilterSelection::SourceOnly => entry.update_handle = None,
                    RootFilterSelection::WithUpdate => {}
                    RootFilterSelection::Skip { kind, result } => {
                        if let Some(mode) = settings.dry_run {
                            crate::dry_run::report_skip(src, &result, mode, kind.label_long());
                        }
                        kind.inc_skipped(prog_track);
                        return Ok(LinkRootSetup::Complete(skipped_summary_for(kind)));
                    }
                }
                LinkEntryAdmission::Filtered(entry)
            }
            _ => LinkEntryAdmission::from(EntryAdmission::from(permit)),
        };
        // authoritative destination split runs only after every applicable root filter. a `.`/`..`/`/`
        // destination is not a meaningful link target, and rejecting it avoids clobbering the cwd.
        // empty parent (a single-component relative path) means the current directory.
        let (Some(dst_parent_path), Some(_dst_name)) = (dst.parent(), dst.file_name()) else {
            return Err(Error::new(
                anyhow!(
                    "link destination {:?} has no parent directory or file name",
                    dst
                ),
                Default::default(),
            ));
        };
        let dst_parent_path = if dst_parent_path.as_os_str().is_empty() {
            std::path::Path::new(".")
        } else {
            dst_parent_path
        };
        // one `listxattr` on the source ROOT per run — a constant, not a per-entry probe — warning that
        // a source root carrying an ACL is about to be linked by settings that drop it. WITHOUT
        // `--update`, files count as already safe whatever `f:acl` says: a hard-linked destination
        // file IS the source inode, so its ACL cannot be dropped (and must never be written through,
        // which is why `f:acl` applies only on rlink's real copy path). WITH `--update` that certainty
        // is gone — a changed file is materialized by COPYING the update-tree entry, which drops its
        // acl unless `f:acl` is on — so the file half then defers to the setting like any copy. (the
        // probe still examines the SOURCE root; whether the update entry differs — and so whether the
        // copy path actually runs — is not known yet, matching the probe's documented
        // root-only-heuristic character.) whether the notice is wanted AT ALL still comes from the
        // preserve settings, which for rlink default to `all` — metadata fidelity is what the tool is
        // for — so a bare `rlink` asks for it where a bare `rcp` does not.
        let root_acl_notice = crate::safedir::RootAclNotice {
            file_acl_preserved: update.is_none() || settings.preserve.file.acl,
            ..crate::safedir::RootAclNotice::for_preserve(&settings.preserve)
        };
        match &admission {
            LinkEntryAdmission::Filtered(entry) => {
                crate::safedir::warn_if_root_acl_unpreserved(
                    &entry.src_handle,
                    src,
                    root_acl_notice,
                )
                .await;
            }
            LinkEntryAdmission::Unclassified { .. } => {
                crate::safedir::warn_if_root_acl_unpreserved_at(
                    &src_parent,
                    src_name,
                    src,
                    root_acl_notice,
                )
                .await;
            }
        }
        // in dry-run we never touch the destination, so we don't open its parent at all (it may not
        // even exist). `dst_parent == None` is the signal throughout the walk that destination
        // operations must be skipped. in a real link, reuse the strict-validated parent, or open it
        // following symlinks (default mode; a symlinked destination container is followed into the real
        // dir).
        let dst_parent = if settings.dry_run.is_some() {
            None
        } else {
            match strict_dst_parent {
                Some(parent) => Some(parent),
                None => {
                    let dir = Dir::open_parent_dir(dst_parent_path, congestion::Side::Destination)
                        .await
                        .with_context(|| {
                            format!(
                                "cannot open destination parent directory {:?}",
                                dst_parent_path
                            )
                        })
                        .map_err(|err| Error::new(err, Default::default()))?;
                    // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below).
                    Some(Arc::new(dir.into_tree()))
                }
            }
        };
        Ok(LinkRootSetup::Ready {
            src_operand,
            src_parent,
            dst_parent,
            update_parent,
            admission,
        })
    })
    .await?;
    let (src_operand, src_parent, dst_parent, update_parent, admission) = match setup {
        LinkRootSetup::Complete(summary) => return Ok(summary),
        LinkRootSetup::Ready {
            src_operand,
            src_parent,
            dst_parent,
            update_parent,
            admission,
        } => (
            src_operand,
            src_parent,
            dst_parent,
            update_parent,
            admission,
        ),
    };
    let src = src_operand.display.as_path();
    let src_name = src_operand.name.as_os_str();
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
        update_root_requirement,
        settings,
        is_fresh,
        admission,
    )
    .await
    .map(|result| result.summary)
}
/// Tracks which child names exact entry decisions protect from `--delete` for one directory pass.
/// Source workers fold their final selection here; exactly classified update-only specials record
/// synchronously because they deliberately skip task dispatch.
///
/// When `--delete` is off the inner set is `None` and every method is a no-op — zero heap
/// cost in the hot path.
struct DeleteKeepSet {
    inner: Option<std::collections::HashSet<std::ffi::OsString>>,
}

impl DeleteKeepSet {
    fn new(delete: Option<&copy::DeleteSettings>) -> Self {
        Self {
            inner: delete.is_some().then(std::collections::HashSet::new),
        }
    }
    /// Exact worker result: retain a name only when the final joint decision selected an action.
    fn record_exact(&mut self, name: std::ffi::OsString) {
        if let Some(set) = &mut self.inner {
            set.insert(name);
        }
    }
    /// Borrow the underlying set for `prune_extraneous`. `None` means `--delete` is off and
    /// the caller should skip the prune entirely.
    fn as_set(&self) -> Option<&std::collections::HashSet<std::ffi::OsString>> {
        self.inner.as_ref()
    }
}

/// Keeps both comparison handles structurally ahead of their shared admission permit.
struct AdmittedLinkEntry<S = Handle, U = Handle, P = LeafPermit> {
    src_handle: S,
    update_handle: Option<U>,
    permit: Option<P>,
}

/// Owns exactly one link entry's classification and admission state.
enum LinkEntryAdmission {
    Unclassified {
        admission: EntryAdmission,
        hinted_src_handle: Option<Handle>,
    },
    Filtered(AdmittedLinkEntry),
}

/// One exact link-entry decision and the destination-protection bit it authorizes.
struct LinkEntryResult {
    summary: Summary,
    protect_destination: bool,
}

impl LinkEntryResult {
    fn selected(summary: Summary) -> Self {
        Self {
            summary,
            protect_destination: true,
        }
    }

    fn filtered(summary: Summary) -> Self {
        Self {
            summary,
            protect_destination: false,
        }
    }
}

/// Result folded from one spawned source/update entry.
struct LinkTaskResult {
    summary: Summary,
    keep_name: Option<std::ffi::OsString>,
}

impl LinkTaskResult {
    fn from_link(name: std::ffi::OsString, result: LinkEntryResult) -> Self {
        Self {
            summary: result.summary,
            keep_name: result.protect_destination.then_some(name),
        }
    }

    fn from_update(name: std::ffi::OsString, summary: Summary) -> Self {
        Self {
            summary,
            keep_name: Some(name),
        }
    }

    fn fold(self, summary: &mut Summary, keep_set: &mut DeleteKeepSet) {
        if let Some(name) = self.keep_name {
            keep_set.record_exact(name);
        }
        *summary = *summary + self.summary;
    }
}

impl LinkEntryAdmission {
    fn admission(&self) -> Option<throttle::FdAdmission> {
        match self {
            Self::Unclassified { admission, .. } => admission.admission(),
            Self::Filtered(entry) => entry.permit.as_ref().map(LeafPermit::admission),
        }
    }
}

impl From<EntryAdmission> for LinkEntryAdmission {
    fn from(admission: EntryAdmission) -> Self {
        Self::Unclassified {
            admission,
            hinted_src_handle: None,
        }
    }
}

/// Select which side of a dual-root entry survives its joint filter decision.
enum RootFilterSelection {
    SourceOnly,
    WithUpdate,
    Skip {
        kind: EntryKind,
        result: crate::filter::FilterResult,
    },
}

/// Filter a link root once in its source-root-relative namespace using both authoritative types.
fn select_filtered_root(
    filter: &crate::filter::FilterSettings,
    logical_name: &std::ffi::OsStr,
    src_kind: EntryKind,
    update_kind: Option<EntryKind>,
    update_exclusive: bool,
) -> RootFilterSelection {
    let logical_name = std::path::Path::new(logical_name);
    let src_result = filter.should_include_root_item(logical_name, src_kind == EntryKind::Dir);
    let src_included = matches!(&src_result, crate::filter::FilterResult::Included);
    let Some(update_kind) = update_kind else {
        return if src_included {
            RootFilterSelection::SourceOnly
        } else {
            RootFilterSelection::Skip {
                kind: src_kind,
                result: src_result,
            }
        };
    };
    let update_result =
        filter.should_include_root_item(logical_name, update_kind == EntryKind::Dir);
    if matches!(&update_result, crate::filter::FilterResult::Included) {
        RootFilterSelection::WithUpdate
    } else if !update_exclusive && src_included {
        RootFilterSelection::SourceOnly
    } else {
        let (kind, result) = if update_exclusive {
            (update_kind, update_result)
        } else {
            (src_kind, src_result)
        };
        RootFilterSelection::Skip { kind, result }
    }
}

/// Select which exact side of a nested dual-tree entry survives its filter decision.
fn select_filtered_nested(
    filter: &crate::filter::FilterSettings,
    logical_path: &std::path::Path,
    src_kind: EntryKind,
    update_kind: Option<EntryKind>,
    update_exclusive: bool,
) -> RootFilterSelection {
    let src_result = filter.should_include(logical_path, src_kind == EntryKind::Dir);
    let src_included = matches!(&src_result, crate::filter::FilterResult::Included);
    let Some(update_kind) = update_kind else {
        return if src_included {
            RootFilterSelection::SourceOnly
        } else {
            RootFilterSelection::Skip {
                kind: src_kind,
                result: src_result,
            }
        };
    };
    let update_result = filter.should_include(logical_path, update_kind == EntryKind::Dir);
    if matches!(&update_result, crate::filter::FilterResult::Included) {
        RootFilterSelection::WithUpdate
    } else if !update_exclusive && src_included {
        RootFilterSelection::SourceOnly
    } else {
        let (kind, result) = if update_exclusive {
            (update_kind, update_result)
        } else {
            (src_kind, src_result)
        };
        RootFilterSelection::Skip { kind, result }
    }
}

fn drop_link_handles_before_permit<S, U, P>(
    src_handle: S,
    update_handle: Option<U>,
    permit: Option<P>,
) {
    drop(src_handle);
    drop(update_handle);
    drop(permit);
}

impl AdmittedLinkEntry<Handle, Handle, LeafPermit> {
    fn new(src_handle: Handle, permit: Option<LeafPermit>) -> Self {
        Self {
            src_handle,
            update_handle: None,
            permit,
        }
    }

    // the directory outcome returns the original private bundle so its two handles can be closed
    // before the permit. boxing that expected recursive transition would allocate per directory.
    #[allow(clippy::result_large_err)]
    fn into_source_leaf(self) -> Result<AdmittedLeaf, Self> {
        let Self {
            src_handle,
            update_handle,
            permit,
        } = self;
        match AdmittedLeaf::try_new(src_handle, permit) {
            Ok(leaf) => {
                drop(update_handle);
                Ok(leaf)
            }
            Err((src_handle, permit)) => Err(Self {
                src_handle,
                update_handle,
                permit,
            }),
        }
    }

    fn into_update_entry(self) -> AdmittedEntry {
        let Self {
            src_handle,
            update_handle,
            permit,
        } = self;
        drop(src_handle);
        let update_handle =
            update_handle.expect("update action requires a classified update entry");
        AdmittedEntry::new(update_handle, permit)
    }

    fn into_source_entry(self) -> AdmittedEntry {
        let Self {
            src_handle,
            update_handle,
            permit,
        } = self;
        drop(update_handle);
        AdmittedEntry::new(src_handle, permit)
    }

    fn close_for_directory(self) -> bool {
        let Self {
            src_handle,
            update_handle,
            permit,
        } = self;
        let has_update_dir = update_handle
            .as_ref()
            .is_some_and(|handle| handle.kind() == EntryKind::Dir);
        drop_link_handles_before_permit(src_handle, update_handle, permit);
        has_update_dir
    }
}

/// Process one entry in the dual source/update tree.
///
/// The source entry is classified via `src_parent.child(name)` (fstat-authoritative; the getdents
/// hint is only a spawn-loop heuristic). When an update entry exists at this name it is classified
/// too, and the hard-link-vs-copy decision mirrors the old `--update` overlay logic exactly:
/// a type mismatch, changed file, or symlink in the update tree is copied from the update version
/// via [`copy::copy_child`]; an unchanged file is hard-linked from the source; a directory recurses
/// through the dual tree. With no update tree, a source file is hard-linked, a source symlink is
/// copied, and a directory recurses.
///
/// `admission` is the explicit scheduling state supplied by the directory loop. Every
/// leaf-dispatch branch retains a held permit through its final fd-bearing work, including nested
/// overwrite removal; direct directory recursion releases it before descent, while delegated copy
/// consumes the already-classified update entry and owns its leaf-or-directory transition.
#[instrument(skip(prog_track, src_parent, update, dst_parent, settings, admission))]
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
    update_root_requirement: UpdateRootRequirement,
    settings: &Settings,
    is_fresh: bool,
    admission: LinkEntryAdmission,
) -> Result<LinkEntryResult, Error> {
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
    let mut admission = admission;
    if let LinkEntryAdmission::Unclassified {
        admission: entry_admission,
        hinted_src_handle,
    } = &mut admission
    {
        let current = std::mem::replace(entry_admission, EntryAdmission::RootOrDelegated);
        *entry_admission = if update.is_some() {
            current.require_admission()
        } else {
            current
        };
        // a positive source-directory hint gets one unadmitted classification only when no
        // separate update entry participates in the decision. if stale, close the first handle
        // before acquiring and reclassify inside the admitted worker region below.
        if matches!(entry_admission, EntryAdmission::HintedDirectory) {
            tracing::debug!("classifying hinted source entry");
            let handle = walk::classify_entry(src_parent, name)
                .await
                .with_context(|| format!("failed reading metadata from {:?}", &src_path))
                .map_err(|err| Error::new(err, Default::default()))?;
            if handle.kind() == EntryKind::Dir {
                *hinted_src_handle = Some(handle);
            } else {
                drop(handle);
                *entry_admission = EntryAdmission::RootOrDelegated;
            }
        }
        if hinted_src_handle.is_none() || update.is_some() {
            let current = std::mem::replace(entry_admission, EntryAdmission::RootOrDelegated);
            *entry_admission = walk::ensure_entry_admission(PermitKind::OpenFile, current).await;
        }
    }
    let weak_admission = admission.admission();

    enum LinkDispatch {
        Complete(LinkEntryResult),
        Directory { has_update_dir: bool },
    }

    let dispatch = safedir::with_optional_fd_admission(weak_admission, async {
        let (mut entry, root_filter_applied) = match admission {
            LinkEntryAdmission::Filtered(entry) => (entry, true),
            LinkEntryAdmission::Unclassified {
                admission,
                hinted_src_handle,
            } => {
                let src_handle = match hinted_src_handle {
                    Some(handle) => handle,
                    None => {
                        tracing::debug!("classifying source entry");
                        walk::classify_entry(src_parent, name)
                            .await
                            .with_context(|| {
                                format!("failed reading metadata from {:?}", &src_path)
                            })
                            .map_err(|err| Error::new(err, Default::default()))?
                    }
                };
                let mut entry = AdmittedLinkEntry::new(src_handle, admission.into_permit());
                // classify the update entry at this name (if an update tree is present at this
                // level). root absence is rejected when destructive settings require the operand;
                // nested absence remains the ordinary source-only / update-exclusive no-op case.
                entry.update_handle = match update {
                    Some((update_dir, update_name)) => {
                        tracing::debug!("classifying 'update' entry");
                        let update_path = update_path
                            .as_deref()
                            .expect("update directory present implies update path present");
                        update_root_requirement
                            .for_entry(rel_path)
                            .classify(update_dir.child(update_name).await, update_path)?
                    }
                    None => None,
                };
                if update.is_some() && settings.update_exclusive && entry.update_handle.is_none() {
                    return Ok(LinkDispatch::Complete(LinkEntryResult::filtered(
                        Default::default(),
                    )));
                }
                (entry, false)
            }
        };
        // a root is one logical item in the source namespace even when the update operand has another
        // basename. decide from both fd-classified types together: normal mode is their filtered union,
        // while --update-exclusive selects only a filter-passing update side.
        if !root_filter_applied
            && rel_path.as_os_str().is_empty()
            && let Some(filter) = settings.filter.as_ref()
        {
            match select_filtered_root(
                filter,
                name,
                entry.src_handle.kind(),
                entry.update_handle.as_ref().map(Handle::kind),
                settings.update_exclusive,
            ) {
                RootFilterSelection::SourceOnly => entry.update_handle = None,
                RootFilterSelection::WithUpdate => {}
                RootFilterSelection::Skip { kind, result } => {
                    if let Some(mode) = settings.dry_run {
                        crate::dry_run::report_skip(&src_path, &result, mode, kind.label_long());
                    }
                    kind.inc_skipped(prog_track);
                    return Ok(LinkDispatch::Complete(LinkEntryResult::filtered(
                        skipped_summary_for(kind),
                    )));
                }
            }
        // a nested source loop may have filtered from an advisory hint or a DT_UNKNOWN probe whose
        // handle was closed before this worker ran. Re-evaluate BOTH sides from the exact handles that
        // can drive materialization, so a later type swap cannot inherit either earlier decision.
        } else if !rel_path.as_os_str().is_empty()
            && let Some(filter) = settings.filter.as_ref()
        {
            match select_filtered_nested(
                filter,
                rel_path,
                entry.src_handle.kind(),
                entry.update_handle.as_ref().map(Handle::kind),
                settings.update_exclusive,
            ) {
                RootFilterSelection::SourceOnly => entry.update_handle = None,
                RootFilterSelection::WithUpdate => {}
                RootFilterSelection::Skip { kind, result } => {
                    if let Some(mode) = settings.dry_run {
                        crate::dry_run::report_skip(&src_path, &result, mode, kind.label_long());
                    }
                    kind.inc_skipped(prog_track);
                    return Ok(LinkDispatch::Complete(LinkEntryResult::filtered(
                        skipped_summary_for(kind),
                    )));
                }
            }
        }
        // from this point every leaf-dispatch action retains the private entry bundle or an
        // `AdmittedLeaf` through its final fd-bearing work, including nested overwrite removal. A
        // delegated update action closes the comparison source handle, then transfers the selected
        // update handle and permit together as one AdmittedEntry. copy consumes that exact owner through
        // the shared driver, including its directory drop-before-recursion transition. Direct dual-tree
        // directory recursion closes admission and both handles here.
        if let Some(update_entry) = entry.update_handle.as_ref() {
            let (update_dir, update_name) = update.unwrap();
            let update_path = update_path.as_deref().unwrap();
            if update_entry.kind() != entry.src_handle.kind() {
                // file type changed, just copy the updated one
                tracing::debug!(
                    "link: file type of {:?} ({:?}) and {:?} ({:?}) differs - copying from update",
                    src_path,
                    entry.src_handle.kind(),
                    update_path,
                    update_entry.kind()
                );
                // delegate at this entry's logical path so that, under --delete, pruning inside the
                // delegated subtree matches include/exclude descendants at the correct filter root
                // (e.g. `node/*.log`). Pass the held update parent + name and transfer the exact selected
                // update handle with its admission. The shared copy driver releases provisional
                // admission itself when that handle is an authoritative directory.
                let admission = entry.into_update_entry();
                return delegate_copy(
                    prog_track,
                    update_dir,
                    dst_parent,
                    update_name,
                    update_path,
                    &dst_path,
                    rel_path,
                    copy::DeleteScanAnchor::new(dst_root, rel_path),
                    settings,
                    is_fresh,
                    admission,
                )
                .await
                .map(|summary| LinkDispatch::Complete(LinkEntryResult::selected(summary)));
            }
            if update_entry.kind() == EntryKind::File {
                // check if the file is unchanged and if so hard-link, otherwise copy from the updated one
                if filecmp::metadata_equal(
                    &settings.update_compare,
                    entry.src_handle.meta(),
                    update_entry.meta(),
                ) {
                    // unchanged file: hard-link from src while retaining admission across `linkat`.
                    tracing::debug!("no change, hard link 'src'");
                    let leaf = match entry.into_source_leaf() {
                        Ok(leaf) => leaf,
                        Err(entry) => {
                            return Ok(LinkDispatch::Directory {
                                has_update_dir: entry.close_for_directory(),
                            });
                        }
                    };
                    if settings.dry_run.is_some() {
                        crate::dry_run::report_action("link", &src_path, Some(&dst_path), "file");
                        return Ok(LinkDispatch::Complete(LinkEntryResult::selected(Summary {
                            hard_links_created: 1,
                            ..Default::default()
                        })));
                    }
                    let dst_dir =
                        dst_parent.expect("destination parent must be open for a real hard link");
                    return hard_link_entry_fd(
                        prog_track,
                        leaf.handle(),
                        dst_dir,
                        &dst_name,
                        &dst_path,
                        settings,
                    )
                    .await
                    .map(|summary| LinkDispatch::Complete(LinkEntryResult::selected(summary)));
                }
                tracing::debug!(
                    "link: {:?} metadata has changed, copying from {:?}",
                    src_path,
                    update_path
                );
                // changed file: delegate to copy, transferring the same admission guard.
                let admission = entry.into_update_entry();
                return delegate_copy(
                    prog_track,
                    update_dir,
                    dst_parent,
                    update_name,
                    update_path,
                    &dst_path,
                    rel_path,
                    copy::DeleteScanAnchor::new(dst_root, rel_path),
                    settings,
                    is_fresh,
                    admission,
                )
                .await
                .map(|summary| LinkDispatch::Complete(LinkEntryResult::selected(summary)));
            }
            if update_entry.kind() == EntryKind::Symlink {
                // update symlink: copy it under the same leaf admission guard, including any overwrite
                // removal it triggers
                tracing::debug!("'update' is a symlink so just symlink that");
                let admission = entry.into_update_entry();
                return delegate_copy(
                    prog_track,
                    update_dir,
                    dst_parent,
                    update_name,
                    update_path,
                    &dst_path,
                    rel_path,
                    copy::DeleteScanAnchor::new(dst_root, rel_path),
                    settings,
                    is_fresh,
                    admission,
                )
                .await
                .map(|summary| LinkDispatch::Complete(LinkEntryResult::selected(summary)));
            }
        } else {
            // update hasn't been specified (or is absent at this name): hard-link a source file or copy
            // a source symlink, retaining admission across all leaf work and nested overwrite removal
            tracing::debug!("no 'update' entry");
            if entry.src_handle.kind() == EntryKind::File {
                let leaf = match entry.into_source_leaf() {
                    Ok(leaf) => leaf,
                    Err(entry) => {
                        return Ok(LinkDispatch::Directory {
                            has_update_dir: entry.close_for_directory(),
                        });
                    }
                };
                if settings.dry_run.is_some() {
                    crate::dry_run::report_action("link", &src_path, Some(&dst_path), "file");
                    return Ok(LinkDispatch::Complete(LinkEntryResult::selected(Summary {
                        hard_links_created: 1,
                        ..Default::default()
                    })));
                }
                let dst_dir =
                    dst_parent.expect("destination parent must be open for a real hard link");
                return hard_link_entry_fd(
                    prog_track,
                    leaf.handle(),
                    dst_dir,
                    &dst_name,
                    &dst_path,
                    settings,
                )
                .await
                .map(|summary| LinkDispatch::Complete(LinkEntryResult::selected(summary)));
            }
            if entry.src_handle.kind() == EntryKind::Symlink {
                tracing::debug!("'src' is a symlink so just symlink that");
                let admission = entry.into_source_entry();
                return delegate_copy(
                    prog_track,
                    src_parent,
                    dst_parent,
                    name,
                    &src_path,
                    &dst_path,
                    rel_path,
                    copy::DeleteScanAnchor::new(dst_root, rel_path),
                    settings,
                    is_fresh,
                    admission,
                )
                .await
                .map(|summary| LinkDispatch::Complete(LinkEntryResult::selected(summary)));
            }
        }
        if entry.src_handle.kind() != EntryKind::Dir {
            // special file (or unsupported type): retain admission until this branch returns and drops
            // the source/update classification handles.
            if settings.copy_settings.skip_specials {
                tracing::debug!(
                    "skipping special file {:?} (kind: {:?})",
                    src_path,
                    entry.src_handle.kind()
                );
                if let Some(mode) = settings.dry_run {
                    match mode {
                        crate::config::DryRunMode::Brief => {}
                        crate::config::DryRunMode::All => println!("skip special {:?}", src_path),
                        crate::config::DryRunMode::Explain => {
                            println!(
                                "skip special {:?} (unsupported file type: {:?})",
                                src_path,
                                entry.src_handle.kind()
                            );
                        }
                    }
                }
                prog_track.specials_skipped.inc();
                return Ok(LinkDispatch::Complete(LinkEntryResult::selected(Summary {
                    copy_summary: CopySummary {
                        specials_skipped: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                })));
            }
            return Err(Error::new(
                anyhow!(
                    "copy: {:?} -> {:?} failed, unsupported src file type: {:?}",
                    src_path,
                    dst_path,
                    entry.src_handle.kind()
                ),
                Default::default(),
            ));
        }
        // directory: release before recursing the dual tree. this is rlink's direct counterpart to the
        // shared driver's drop-before-recurse boundary.
        debug_assert!(
            entry.update_handle.is_none()
                || entry.update_handle.as_ref().unwrap().kind() == EntryKind::Dir
        );
        // only drive the dual-tree update walk when an update directory entry actually exists at this
        // name. If `update_handle` is None (the update tree has no counterpart for this src dir, the
        // recursive "update missing" case), process this subtree in no-update mode: hard-link the whole
        // source subtree. Passing the parent update tuple here would make `link_dir_entry` try to
        // `open_dir` a non-existent update child.
        let has_update_dir = entry.close_for_directory();
        Ok(LinkDispatch::Directory { has_update_dir })
    })
    .await?;
    let has_update_dir = match dispatch {
        LinkDispatch::Complete(summary) => return Ok(summary),
        LinkDispatch::Directory { has_update_dir } => has_update_dir,
    };
    let update_for_dir = update.filter(|_| has_update_dir);
    let update_root_for_dir = update_root.filter(|_| has_update_dir);
    let update_path_for_dir = update_path.as_deref().filter(|_| has_update_dir);
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
        update_path_for_dir,
        settings,
        is_fresh,
    )
    .await
    .map(LinkEntryResult::selected)
}

/// Delegate a single entry to the fd-based copy ([`copy::copy_child`]), passing the HELD parent
/// directory handles plus the entry `name` — never re-resolving a path. `filter_base` for the
/// delegation is the entry's logical relative path (so `--delete` pruning inside the subtree
/// matches include/exclude patterns at the entry's true path). `delete_scan_anchor` independently
/// retains the original outer destination operand and physical subtree base. The returned copy
/// summary is folded into a link `Summary`.
#[allow(clippy::too_many_arguments)]
async fn delegate_copy(
    prog_track: &'static progress::Progress,
    src_parent: &Arc<Dir>,
    dst_parent: Option<&Arc<Dir>>,
    name: &std::ffi::OsStr,
    src_path: &std::path::Path,
    dst_path: &std::path::Path,
    filter_base: &std::path::Path,
    delete_scan_anchor: copy::DeleteScanAnchor,
    settings: &Settings,
    is_fresh: bool,
    admission: AdmittedEntry,
) -> Result<Summary, Error> {
    // link's dry-run mode owns whether a destination handle exists. Keep delegated copy behavior
    // aligned even for direct API callers that did not duplicate the mode into `copy_settings`;
    // the normal CLI path already agrees and pays no clone.
    let normalized_copy_settings =
        (settings.copy_settings.dry_run != settings.dry_run).then(|| {
            let mut copy_settings = settings.copy_settings.clone();
            copy_settings.dry_run = settings.dry_run;
            copy_settings
        });
    let copy_settings = normalized_copy_settings
        .as_ref()
        .unwrap_or(&settings.copy_settings);
    let copy_summary = copy::copy_child(
        prog_track,
        src_parent,
        dst_parent,
        name,
        src_path,
        dst_path,
        filter_base,
        copy_settings,
        &settings.preserve,
        is_fresh,
        delete_scan_anchor,
        copy::CopyEntryAdmission::Filtered(admission),
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
            None, // dry-run: no destination dir is opened or locked, so nothing to restore
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
        reused_lock,
    } = match copy::resolve_dst_dir(
        prog_track,
        dst_parent,
        dst_name,
        dst_path,
        &settings.copy_settings,
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
        reused_lock,
        settings,
        Summary {
            copy_summary: base,
            ..Default::default()
        },
    )
    .await
}

/// Derive source-entry admission from its type hint and whether an update tree participates.
///
/// A source hint says nothing about a separate update entry, so a dual-tree decision always needs
/// admission before either entry is classified.
fn source_entry_admission(hint: Option<EntryKind>, has_update: bool) -> EntryAdmission {
    let admission = EntryAdmission::from_hint(hint);
    if has_update {
        // the source hint says nothing about the separate update entry, so the combined decision
        // must acquire before it is spawned or opens either classification handle.
        admission.require_admission()
    } else {
        admission
    }
}

/// Whether a preliminary hint decision must defer to the exact worker outcome.
fn requires_exact_entry_outcome(settings: &Settings) -> bool {
    settings.copy_settings.delete.is_some() || settings.dry_run.is_some()
}

/// Advisory source-loop decision made before the exact link worker runs.
enum SourceEntryDecision {
    /// The cheap filter decision is terminal in an ordinary real run.
    Filtered {
        kind: EntryKind,
        result: crate::filter::FilterResult,
    },
    /// The entry still needs its exact worker decision.
    Dispatch(EntryAdmission),
}

/// Apply source-loop hint fast paths and return the admission state to transfer into the worker.
async fn select_source_for_dispatch(
    src_dir: &Arc<Dir>,
    name: &std::ffi::OsStr,
    hint: Option<EntryKind>,
    relative_path: &std::path::Path,
    settings: &Settings,
    has_update: bool,
) -> std::io::Result<SourceEntryDecision> {
    let kind = hint.unwrap_or(EntryKind::File);
    let mut admission = source_entry_admission(hint, has_update);
    if settings.filter.is_some() {
        // a DT_UNKNOWN filter decision opens an O_PATH handle, so it is the only source-only
        // decision path that acquires before filtering. reliable hints keep cheap exits outside
        // open-file admission.
        if hint.is_none() {
            admission = walk::ensure_entry_admission(PermitKind::OpenFile, admission).await;
        }
        // on DT_UNKNOWN, an exact directory probe is required for is-dir-dependent filters;
        // reliable hints avoid the extra fstat. The worker repeats the joint filter decision
        // from both final handles before acting.
        let entry_is_dir = safedir::with_optional_fd_admission(
            admission.admission(),
            walk::filter_is_dir(settings.filter.as_ref(), src_dir, name, hint, false),
        )
        .await?;
        if let Some(result) = walk::should_skip_entry(&settings.filter, relative_path, entry_is_dir)
            && !requires_exact_entry_outcome(settings)
        {
            return Ok(SourceEntryDecision::Filtered { kind, result });
        }
    }
    Ok(SourceEntryDecision::Dispatch(admission))
}

/// Filter/admission state for an update-only directory entry.
enum UpdateOnlyDecision {
    /// A reliable hint or exact classification excludes this entry.
    Skipped {
        kind: EntryKind,
        result: crate::filter::FilterResult,
    },
    /// A reliable included hint still needs authoritative classification before dispatch.
    Hinted(EntryKind),
    /// The exact classification and its admission are ready to transfer into copy.
    Admitted(AdmittedEntry),
}

fn select_exact_update_only_entry(
    entry: AdmittedEntry,
    relative_path: &std::path::Path,
    filter: Option<&crate::filter::FilterSettings>,
) -> UpdateOnlyDecision {
    let kind = entry.kind();
    match walk::should_skip_entry_ref(filter, relative_path, kind == EntryKind::Dir) {
        Some(result) => UpdateOnlyDecision::Skipped { kind, result },
        None => UpdateOnlyDecision::Admitted(entry),
    }
}

/// Apply the cheap reliable-hint filter fast path, or classify a `DT_UNKNOWN` entry once and retain
/// that exact owner for dispatch.
async fn select_update_only_for_dispatch(
    update_dir: &Arc<Dir>,
    name: &std::ffi::OsStr,
    hint: Option<EntryKind>,
    relative_path: &std::path::Path,
    settings: &Settings,
) -> std::io::Result<UpdateOnlyDecision> {
    let filter = settings.filter.as_ref();
    if let Some(kind) = hint {
        return Ok(
            match walk::should_skip_entry_ref(filter, relative_path, kind == EntryKind::Dir) {
                Some(result) if !requires_exact_entry_outcome(settings) => {
                    UpdateOnlyDecision::Skipped { kind, result }
                }
                _ => UpdateOnlyDecision::Hinted(kind),
            },
        );
    }
    let admission =
        walk::ensure_entry_admission(PermitKind::OpenFile, EntryAdmission::RootOrDelegated).await;
    let entry = walk::classify_admitted_entry(update_dir, name, admission.into_permit()).await?;
    Ok(select_exact_update_only_entry(entry, relative_path, filter))
}

/// Turn a reliable included hint into the exact owner used for dispatch.
///
/// A positive directory hint gets one unadmitted classification. If it is stale and resolves to a
/// leaf, that handle is closed before waiting for admission and classifying again.
async fn classify_hinted_update_only_for_dispatch(
    update_dir: &Arc<Dir>,
    name: &std::ffi::OsStr,
    hint: EntryKind,
    relative_path: &std::path::Path,
    filter: Option<&crate::filter::FilterSettings>,
) -> std::io::Result<UpdateOnlyDecision> {
    let entry = if hint == EntryKind::Dir {
        let handle = walk::classify_entry(update_dir, name).await?;
        if handle.kind() == EntryKind::Dir {
            AdmittedEntry::new(handle, None)
        } else {
            drop(handle);
            let admission =
                walk::ensure_entry_admission(PermitKind::OpenFile, EntryAdmission::RootOrDelegated)
                    .await;
            walk::classify_admitted_entry(update_dir, name, admission.into_permit()).await?
        }
    } else {
        let admission =
            walk::ensure_entry_admission(PermitKind::OpenFile, EntryAdmission::RootOrDelegated)
                .await;
        walk::classify_admitted_entry(update_dir, name, admission.into_permit()).await?
    };
    Ok(select_exact_update_only_entry(entry, relative_path, filter))
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
    reused_lock: Option<crate::safedir::ReusedDirLock>,
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
    // keep-set for --delete: final exact outcomes own every source name; an exactly classified
    // update-only special records synchronously because it deliberately skips task dispatch.
    let mut keep_set = DeleteKeepSet::new(settings.copy_settings.delete.as_ref());
    // iterate through src entries and recursively call "link" on each one
    for (entry_name, hint) in src_entries {
        let entry_rel = rel_path.join(&entry_name);
        let entry_path = src_path.join(&entry_name);
        let decision = match select_source_for_dispatch(
            src_dir,
            &entry_name,
            hint,
            &entry_rel,
            settings,
            update_dir.is_some(),
        )
        .await
        {
            Ok(decision) => decision,
            Err(error) => {
                crate::walk_driver::abort_and_join(&mut join_set).await;
                return Err(Error::new(
                    anyhow::Error::new(error)
                        .context(format!("failed reading metadata from {entry_path:?}")),
                    link_summary,
                ));
            }
        };
        let admission = match decision {
            SourceEntryDecision::Filtered { kind, result } => {
                if let Some(mode) = settings.dry_run {
                    crate::dry_run::report_skip(&entry_path, &result, mode, kind.label());
                }
                tracing::debug!("skipping {:?} due to filter", &entry_path);
                link_summary = link_summary + skipped_summary_for(kind);
                kind.inc_skipped(prog_track);
                continue;
            }
            SourceEntryDecision::Dispatch(admission) => admission,
        };
        processed_files.insert(entry_name.clone());
        let admission = walk::ensure_entry_admission(PermitKind::OpenFile, admission).await;
        // Acquire-then-IMMEDIATELY-spawn (the permit is moved into `do_link` and spawned on the next
        // line, in the same loop step) is load-bearing: collecting a Vec of pre-acquired permits and
        // spawning later would hold N permits before any task runs and self-deadlock a saturated pool.
        // This mirrors the single-tree driver's incremental acquire-then-spawn loop
        // (`walk_driver::walk_dir_contents`, joined via `walk_driver::join_and_fold`).
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
                UpdateRootRequirement::Optional,
                &settings,
                is_fresh,
                admission.into(),
            )
            .await
            .map(|result| LinkTaskResult::from_link(entry_name, result))
        };
        crate::walk_driver::spawn_tracked(&mut join_set, do_link());
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
        // iterate through update entries and copy names absent from src. reliable excluded hints
        // keep their cheap skip in ordinary real runs; delete and preview runs reclassify before an
        // observable decision. every entry that can reach copy transfers its exact handle and
        // admission.
        for (entry_name, hint) in update_entries {
            let entry_rel = rel_path.join(&entry_name);
            let update_entry_path = update_root.join(&entry_rel);
            if processed_files.contains(&entry_name) {
                // the source worker owns the exact joint source/update selection and reports its
                // destination-protection decision when it completes.
                continue;
            }
            let decision = match select_update_only_for_dispatch(
                update_dir,
                &entry_name,
                hint,
                &entry_rel,
                settings,
            )
            .await
            {
                Ok(decision) => decision,
                Err(error) => {
                    crate::walk_driver::abort_and_join(&mut join_set).await;
                    return Err(Error::new(
                        anyhow::Error::new(error).context(format!(
                            "failed reading metadata from {update_entry_path:?}"
                        )),
                        link_summary,
                    ));
                }
            };
            let decision = match decision {
                UpdateOnlyDecision::Hinted(kind) => match classify_hinted_update_only_for_dispatch(
                    update_dir,
                    &entry_name,
                    kind,
                    &entry_rel,
                    settings.filter.as_ref(),
                )
                .await
                {
                    Ok(decision) => decision,
                    Err(error) => {
                        crate::walk_driver::abort_and_join(&mut join_set).await;
                        return Err(Error::new(
                            anyhow::Error::new(error).context(format!(
                                "failed reading metadata from {update_entry_path:?}"
                            )),
                            link_summary,
                        ));
                    }
                },
                decision => decision,
            };
            let entry = match decision {
                UpdateOnlyDecision::Skipped { kind, result } => {
                    if let Some(mode) = settings.dry_run {
                        crate::dry_run::report_skip(
                            &update_entry_path,
                            &result,
                            mode,
                            kind.label(),
                        );
                    }
                    tracing::debug!(
                        "skipping update entry {:?} due to filter",
                        &update_entry_path
                    );
                    link_summary = link_summary + skipped_summary_for(kind);
                    kind.inc_skipped(prog_track);
                    continue;
                }
                UpdateOnlyDecision::Admitted(entry) => entry,
                UpdateOnlyDecision::Hinted(_) => {
                    unreachable!("an update-only hint must be classified before dispatch")
                }
            };
            let entry_kind = entry.kind();
            if settings.copy_settings.skip_specials && entry_kind == EntryKind::Special {
                keep_set.record_exact(entry_name);
                tracing::debug!("skipping special file {:?}", &update_entry_path);
                if let Some(mode) = settings.dry_run {
                    match mode {
                        crate::config::DryRunMode::Brief => {}
                        crate::config::DryRunMode::All => {
                            println!("skip special {:?}", &update_entry_path)
                        }
                        crate::config::DryRunMode::Explain => {
                            println!(
                                "skip special {:?} (unsupported file type: {:?})",
                                &update_entry_path, entry_kind
                            );
                        }
                    }
                }
                link_summary.copy_summary.specials_skipped += 1;
                prog_track.specials_skipped.inc();
                continue;
            }
            tracing::debug!("found a new entry in the 'update' directory");
            let dst_entry_path = dst_path.join(&entry_name);
            let update_parent = Arc::clone(update_dir);
            let dst_parent = dst_dir.map(Arc::clone);
            let settings = settings.clone();
            let delete_scan_anchor = copy::DeleteScanAnchor::new(dst_root, &entry_rel);
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
                    delete_scan_anchor,
                    &settings,
                    is_fresh,
                    entry,
                )
                .await
                .map(|summary| LinkTaskResult::from_update(entry_name, summary))
            };
            crate::walk_driver::spawn_tracked(&mut join_set, do_copy());
        }
    }
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(result) => match result {
                Ok(result) => result.fold(&mut link_summary, &mut keep_set),
                Err(error) => {
                    tracing::error!(
                        "link: {:?} -> {:?} failed with: {:#}",
                        src_path,
                        dst_path,
                        &error
                    );
                    link_summary = link_summary + error.summary;
                    if settings.copy_settings.fail_early {
                        crate::walk_driver::abort_and_join(&mut join_set).await;
                        return Err(Error::new(error.source, link_summary));
                    }
                    errors.push(error.source);
                }
            },
            Err(error) => {
                if settings.copy_settings.fail_early {
                    crate::walk_driver::abort_and_join(&mut join_set).await;
                    return Err(Error::new(error.into(), link_summary));
                }
                errors.push(error.into());
            }
        }
    }
    // rsync-style --delete for rlink: remove destination entries the final exact decisions did not
    // protect. `keep_set` holds the selected src ∪ update names normally, or only selected update
    // names under --update-exclusive; deliberately skipped specials retain their historical
    // protection even though no new entry is materialized.
    if let Some(delete_settings) = &settings.copy_settings.delete {
        if errors.has_errors() {
            // rsync-style safety: skip pruning when this subtree's link/update pass reported errors
            // — deleting based on a run that did not fully succeed could remove data unexpectedly.
            tracing::warn!(
                "skipping --delete pruning of {:?} because the link/update pass reported errors",
                dst_path
            );
        } else {
            // a real link already holds the destination directory. Dry-run descends from the
            // original named operand one `O_NOFOLLOW` component at a time, so the reconstructed
            // `dst_path` remains diagnostics-only and a symlinked operand root cannot become an
            // intermediate redirect into another tree.
            let prune_dir: Option<Arc<Dir>> = match dst_dir {
                Some(dir) => Some(Arc::clone(dir)),
                None => match crate::safedir::open_existing_dir_beneath_operand(
                    dst_root,
                    rel_path,
                    congestion::Side::Destination,
                )
                .await
                {
                    Ok(Some(dir)) => Some(Arc::new(dir)),
                    Ok(None) => {
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
                },
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
            // for a reused directory locked down under strict mode, put back the ACLs the
            // lockdown stripped and restore the original owner component-wise, then apply source
            // metadata (see set_reused_dir_metadata_fd — no transient window hands the directory to
            // a hostile prior owner); None for fresh dirs.
            async {
                let preserve_meta = meta_dir.meta().await?;
                // the ACLs come from the same fd as the metadata, and only when `d:acl` was asked
                // for — rlink creates its directories fresh, so unlike files there is no shared
                // inode and applying them is ordinary destination work.
                let preserve_acls = if settings.preserve.dir.acl {
                    Some(meta_dir.read_acls().await?)
                } else {
                    None
                };
                crate::safedir::set_reused_dir_metadata_fd(
                    &settings.preserve,
                    &preserve_meta,
                    preserve_acls.as_ref(),
                    reused_lock,
                    dst_dir,
                )
                .await
            }
            .await
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

    #[test]
    fn directory_close_primitive_drops_both_handles_before_permit() {
        let events = testutils::DropEvents::default();
        drop_link_handles_before_permit(
            events.probe("source handle"),
            Some(events.probe("update handle")),
            Some(events.probe("permit")),
        );
        assert_eq!(
            events.snapshot(),
            ["source handle", "update handle", "permit"]
        );
    }

    #[test]
    fn admitted_link_entry_drops_both_handles_before_permit() {
        let events = testutils::DropEvents::default();
        let entry = AdmittedLinkEntry {
            src_handle: events.probe("source handle"),
            update_handle: Some(events.probe("update handle")),
            permit: Some(events.probe("permit")),
        };
        drop(entry);
        assert_eq!(
            events.snapshot(),
            ["source handle", "update handle", "permit"]
        );
    }

    mod delete_keep_set_tests {
        //! Pure-logic unit tests for `DeleteKeepSet`. No filesystem needed — these pin exact
        //! worker ownership so a future refactor cannot silently restore hint-based keeps.

        use super::super::DeleteKeepSet;
        use crate::copy::DeleteSettings;
        use std::ffi::{OsStr, OsString};

        fn delete_on() -> DeleteSettings {
            DeleteSettings {
                delete_excluded: false,
            }
        }

        #[test]
        fn exact_result_no_op_when_delete_off() {
            let mut k = DeleteKeepSet::new(None);
            k.record_exact(OsString::from("foo"));
            assert!(k.as_set().is_none());
        }

        #[test]
        fn exact_materialized_update_exclusive_duplicate_is_retained() {
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d));
            let mut summary = super::super::Summary::default();
            super::super::LinkTaskResult::from_link(
                OsString::from("node"),
                super::super::LinkEntryResult::selected(Default::default()),
            )
            .fold(&mut summary, &mut k);
            assert!(
                k.as_set().unwrap().contains(OsStr::new("node")),
                "the exact worker's materialized update selection must own the keep decision"
            );
        }

        #[test]
        fn selected_source_entry_is_retained_in_normal_mode() {
            let d = delete_on();
            let mut k = DeleteKeepSet::new(Some(&d));
            let mut summary = super::super::Summary::default();
            super::super::LinkTaskResult::from_link(
                OsString::from("node"),
                super::super::LinkEntryResult::selected(Default::default()),
            )
            .fold(&mut summary, &mut k);
            assert!(
                k.as_set().unwrap().contains(OsStr::new("node")),
                "the exact worker's selected source outcome must protect its destination"
            );
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
            let mut k = DeleteKeepSet::new(Some(&d));

            let mut summary = super::super::Summary::default();
            for name in ["keep", "pipe", "node"] {
                super::super::LinkTaskResult::from_link(
                    OsString::from(name),
                    super::super::LinkEntryResult::selected(Default::default()),
                )
                .fold(&mut summary, &mut k);
            }
            super::super::LinkTaskResult::from_update(
                OsString::from("from_upd"),
                Default::default(),
            )
            .fold(&mut summary, &mut k);

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

    struct RestoreRenamedUpdateRoot {
        original: std::path::PathBuf,
        renamed: std::path::PathBuf,
    }

    impl Drop for RestoreRenamedUpdateRoot {
        fn drop(&mut self) {
            if self.renamed.exists() {
                let _ = std::fs::rename(&self.renamed, &self.original);
            }
        }
    }

    struct DisappearingUpdateRootHook {
        _registration: AfterUpdatePrecheckHookRegistration,
        _cleanup: std::sync::Arc<std::sync::Mutex<Option<RestoreRenamedUpdateRoot>>>,
    }

    fn disappear_update_root_after_precheck(
        update: &std::path::Path,
    ) -> DisappearingUpdateRootHook {
        let original = update.to_owned();
        let renamed = update.with_extension("removed-after-precheck");
        let cleanup = std::sync::Arc::new(std::sync::Mutex::new(None));
        let hook_cleanup = Arc::clone(&cleanup);
        let registration = install_after_update_precheck_hook(
            original.clone(),
            Box::new(move || {
                std::fs::rename(&original, &renamed)
                    .expect("rename update root after its early precheck");
                *hook_cleanup
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(RestoreRenamedUpdateRoot { original, renamed });
            }),
        );
        DisappearingUpdateRootHook {
            _registration: registration,
            _cleanup: cleanup,
        }
    }

    async fn setup_disappearing_update_root_case() -> Result<
        (
            std::path::PathBuf,
            std::path::PathBuf,
            std::path::PathBuf,
            std::path::PathBuf,
        ),
        anyhow::Error,
    > {
        let test_path = testutils::create_temp_dir().await?;
        let src = test_path.join("src");
        let update = test_path.join("update");
        let dst = test_path.join("dst");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::create_dir(&update).await?;
        tokio::fs::create_dir(&dst).await?;
        tokio::fs::write(src.join("source-only.txt"), "SOURCE").await?;
        tokio::fs::write(update.join("protected.txt"), "UPDATE").await?;
        tokio::fs::write(dst.join("source-only.txt"), "DESTINATION").await?;
        tokio::fs::write(dst.join("protected.txt"), "PROTECTED DESTINATION").await?;
        Ok((test_path, src, update, dst))
    }

    fn assert_missing_destructive_update_error(error: &Error) {
        let message = format!("{:#}", error.source);
        assert!(
            message.contains("--update path") && message.contains("does not exist"),
            "missing destructive update error was unclear: {message}"
        );
    }

    #[tokio::test]
    async fn update_exclusive_errors_if_update_root_disappears_after_precheck()
    -> Result<(), anyhow::Error> {
        let (test_path, src, update, dst) = setup_disappearing_update_root_case().await?;
        let _hook = disappear_update_root_after_precheck(&update);
        let mut settings = common_settings(false, true);
        settings.update_exclusive = true;
        let result = link(
            &PROGRESS,
            &test_path,
            &src,
            &dst,
            &Some(update.clone()),
            &settings,
            false,
        )
        .await;
        assert_eq!(
            tokio::fs::read_to_string(dst.join("source-only.txt")).await?,
            "DESTINATION"
        );
        assert_eq!(
            tokio::fs::read_to_string(dst.join("protected.txt")).await?,
            "PROTECTED DESTINATION"
        );
        let error = result.expect_err("a vanished update root must fail update-exclusive");
        assert_missing_destructive_update_error(&error);
        Ok(())
    }

    #[tokio::test]
    async fn delete_update_does_not_prune_if_update_root_disappears_after_precheck()
    -> Result<(), anyhow::Error> {
        let (test_path, src, update, dst) = setup_disappearing_update_root_case().await?;
        let _hook = disappear_update_root_after_precheck(&update);
        let mut settings = common_settings(false, true);
        settings.copy_settings.delete = Some(copy::DeleteSettings {
            delete_excluded: false,
        });
        settings.filter = Some(crate::filter::FilterSettings::new());
        let result = link(
            &PROGRESS,
            &test_path,
            &src,
            &dst,
            &Some(update.clone()),
            &settings,
            false,
        )
        .await;
        assert_eq!(
            tokio::fs::read_to_string(dst.join("source-only.txt")).await?,
            "DESTINATION"
        );
        assert_eq!(
            tokio::fs::read_to_string(dst.join("protected.txt")).await?,
            "PROTECTED DESTINATION"
        );
        let error = result.expect_err("a vanished update root must stop delete pruning");
        assert_missing_destructive_update_error(&error);
        Ok(())
    }

    #[tokio::test]
    async fn plain_update_falls_back_if_update_root_disappears_after_precheck()
    -> Result<(), anyhow::Error> {
        let (test_path, src, update, dst) = setup_disappearing_update_root_case().await?;
        let _hook = disappear_update_root_after_precheck(&update);
        link(
            &PROGRESS,
            &test_path,
            &src,
            &dst,
            &Some(update),
            &common_settings(false, true),
            false,
        )
        .await?;
        assert_eq!(
            tokio::fs::read_to_string(dst.join("source-only.txt")).await?,
            "SOURCE"
        );
        assert_eq!(
            tokio::fs::read_to_string(dst.join("protected.txt")).await?,
            "PROTECTED DESTINATION"
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_exclusive_without_update_links_the_source_normally() -> Result<(), anyhow::Error>
    {
        use std::os::unix::fs::MetadataExt;
        let test_path = testutils::create_temp_dir().await?;
        let src = test_path.join("src");
        let dst = test_path.join("dst");
        tokio::fs::write(&src, "SOURCE").await?;
        let mut settings = common_settings(false, false);
        settings.update_exclusive = true;
        let summary = link(&PROGRESS, &test_path, &src, &dst, &None, &settings, false).await?;
        assert_eq!(summary.hard_links_created, 1);
        assert_eq!(tokio::fs::read_to_string(&dst).await?, "SOURCE");
        let src_metadata = tokio::fs::metadata(&src).await?;
        let dst_metadata = tokio::fs::metadata(&dst).await?;
        assert_eq!(
            (src_metadata.dev(), src_metadata.ino()),
            (dst_metadata.dev(), dst_metadata.ino())
        );
        Ok(())
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

    // regression: a source operand whose final component is `.`/`..` (e.g. `rlink tree/.. dst`)
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
                tracing::info!("{:#}", &error);
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

        async fn run_with_open_file_cleanup<F>(
            admission: &testutils::AdmissionLimit,
            operation: F,
        ) -> Result<F::Output, anyhow::Error>
        where
            F: std::future::Future,
        {
            let output = admission
                .run_with_timeout(std::time::Duration::from_secs(20), operation)
                .await
                .context("admission-sensitive filter operation did not finish")?;
            let permit = admission
                .run_with_timeout(
                    std::time::Duration::from_secs(20),
                    throttle::open_file_permit(),
                )
                .await
                .context("filter operation did not return open-file capacity")?;
            drop(permit);
            Ok(output)
        }

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
        /// An update root is filtered under the source root's logical name in union mode.
        #[tokio::test]
        #[traced_test]
        async fn update_root_filter_uses_source_name_in_union_mode() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("foo");
            let update = test_path.join("bar");
            let dst = test_path.join("dst");
            tokio::fs::write(&src, "SRC").await?;
            tokio::fs::write(&update, "UPDATED").await?;
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let mut settings = common_settings(false, false);
            settings.copy_settings.filter = Some(filter.clone());
            settings.filter = Some(filter);
            let summary = link(
                &PROGRESS,
                &test_path,
                &src,
                &dst,
                &Some(update),
                &settings,
                false,
            )
            .await?;
            assert_eq!(tokio::fs::read_to_string(&dst).await?, "UPDATED");
            assert_eq!(summary.copy_summary.files_copied, 1);
            Ok(())
        }
        /// An update root is filtered under the source root's logical name in exclusive mode.
        #[tokio::test]
        #[traced_test]
        async fn update_root_filter_uses_source_name_in_exclusive_mode() -> Result<(), anyhow::Error>
        {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("foo");
            let update = test_path.join("bar");
            let dst = test_path.join("dst");
            tokio::fs::write(&src, "SRC").await?;
            tokio::fs::write(&update, "UPDATED").await?;
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let mut settings = common_settings(false, false);
            settings.update_exclusive = true;
            settings.copy_settings.filter = Some(filter.clone());
            settings.filter = Some(filter);
            let summary = link(
                &PROGRESS,
                &test_path,
                &src,
                &dst,
                &Some(update),
                &settings,
                false,
            )
            .await?;
            assert_eq!(tokio::fs::read_to_string(&dst).await?, "UPDATED");
            assert_eq!(summary.copy_summary.files_copied, 1);
            Ok(())
        }
        /// A filter-passing update directory keeps a root excluded for the source-file type.
        #[tokio::test]
        #[traced_test]
        async fn update_directory_can_include_root_excluded_for_source_file()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src_parent = test_path.join("source");
            let update_parent = test_path.join("update");
            let src = src_parent.join("data");
            let update = update_parent.join("data");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src_parent).await?;
            tokio::fs::create_dir(&update_parent).await?;
            tokio::fs::write(&src, "SOURCE-FILE").await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::write(update.join("inner.txt"), "UPDATE-DIR").await?;
            let mut filter = FilterSettings::new();
            filter.add_include("data/").unwrap();
            filter.add_include("data/**").unwrap();
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter);
            let summary = link(
                &PROGRESS,
                &test_path,
                &src,
                &dst,
                &Some(update),
                &settings,
                false,
            )
            .await?;
            assert_eq!(
                tokio::fs::read_to_string(dst.join("inner.txt")).await?,
                "UPDATE-DIR"
            );
            assert_eq!(summary.copy_summary.files_copied, 1);
            Ok(())
        }
        /// A filter-selected update file cannot authorize a replacement directory.
        #[tokio::test]
        #[traced_test]
        async fn filtered_update_file_swap_to_directory_never_materializes_replacement()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::write(&src, "SRC").await?;
            tokio::fs::write(&update, "SELECTED-UPDATE-FILE").await?;

            let admission_limits = testutils::AdmissionLimit::new().await;
            admission_limits.set_max_open_files(1);
            let permit = walk::ensure_leaf_permit(PermitKind::OpenFile, None).await;
            let src_parent = Arc::new(
                Dir::open_parent_dir(&test_path, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let dst_parent = Arc::new(
                Dir::open_parent_dir(&test_path, congestion::Side::Destination)
                    .await?
                    .into_tree(),
            );
            let src_handle = src_parent.child(std::ffi::OsStr::new("src")).await?;
            let update_handle = src_parent.child(std::ffi::OsStr::new("update")).await?;

            let mut filter = FilterSettings::new();
            filter.add_exclude("src/")?;
            assert!(matches!(
                select_filtered_root(
                    &filter,
                    std::ffi::OsStr::new("src"),
                    src_handle.kind(),
                    Some(update_handle.kind()),
                    false,
                ),
                RootFilterSelection::WithUpdate
            ));
            let mut entry = AdmittedLinkEntry::new(src_handle, permit);
            entry.update_handle = Some(update_handle);

            // replace the selected file after filtering. the replacement directory is excluded by
            // the same root rule and must never become the object delegated to copy.
            tokio::fs::remove_file(&update).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::write(update.join("replacement.txt"), "UNSELECTED").await?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter);

            let result = run_with_open_file_cleanup(
                &admission_limits,
                link_internal(
                    &PROGRESS,
                    &src_parent,
                    Some((&src_parent, std::ffi::OsStr::new("update"))),
                    Some(&dst_parent),
                    std::ffi::OsStr::new("src"),
                    &src,
                    &dst,
                    Some(&update),
                    std::path::Path::new(""),
                    UpdateRootRequirement::Optional,
                    &settings,
                    false,
                    LinkEntryAdmission::Filtered(entry),
                ),
            )
            .await?;

            assert!(
                result.is_err(),
                "a selected file reopened as a directory must fail closed"
            );
            assert!(
                !dst.exists(),
                "a failed-closed type swap must not create a destination"
            );
            Ok(())
        }

        /// A source-only symlink selected by type cannot authorize a replacement directory.
        #[tokio::test]
        #[traced_test]
        async fn filtered_source_only_symlink_swap_to_directory_never_materializes_replacement()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::write(test_path.join("target"), "TARGET").await?;
            tokio::fs::symlink("target", &src).await?;
            tokio::fs::create_dir(&update).await?;

            let admission_limits = testutils::AdmissionLimit::new().await;
            admission_limits.set_max_open_files(1);
            let permit = walk::ensure_leaf_permit(PermitKind::OpenFile, None).await;
            let parent = Arc::new(
                Dir::open_parent_dir(&test_path, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let dst_parent = Arc::new(
                Dir::open_parent_dir(&test_path, congestion::Side::Destination)
                    .await?
                    .into_tree(),
            );
            let src_handle = parent.child(std::ffi::OsStr::new("src")).await?;
            let update_handle = parent.child(std::ffi::OsStr::new("update")).await?;
            let mut filter = FilterSettings::new();
            filter.add_exclude("src/")?;
            assert!(matches!(
                select_filtered_root(
                    &filter,
                    std::ffi::OsStr::new("src"),
                    src_handle.kind(),
                    Some(update_handle.kind()),
                    false,
                ),
                RootFilterSelection::SourceOnly
            ));
            drop(update_handle);
            let entry = AdmittedLinkEntry::new(src_handle, permit);

            // replace the selected symlink after the joint filter decision. the replacement
            // directory is excluded by that same logical root rule.
            tokio::fs::remove_file(&src).await?;
            tokio::fs::create_dir(&src).await?;
            tokio::fs::write(src.join("replacement.txt"), "UNSELECTED").await?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);

            run_with_open_file_cleanup(
                &admission_limits,
                link_internal(
                    &PROGRESS,
                    &parent,
                    Some((&parent, std::ffi::OsStr::new("update"))),
                    Some(&dst_parent),
                    std::ffi::OsStr::new("src"),
                    &src,
                    &dst,
                    Some(&update),
                    std::path::Path::new(""),
                    UpdateRootRequirement::Optional,
                    &settings,
                    false,
                    LinkEntryAdmission::Filtered(entry),
                ),
            )
            .await??;

            assert!(
                !dst.is_dir(),
                "the replacement directory passed the selected symlink's filter decision"
            );
            assert_eq!(
                tokio::fs::read_link(&dst).await?,
                std::path::Path::new("target")
            );
            Ok(())
        }

        /// A same-type replacement remains the by-name payload copied after admitted selection.
        #[tokio::test]
        #[traced_test]
        async fn admitted_update_file_swap_to_file_copies_replacement_content()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::write(&src, "S").await?;
            tokio::fs::write(&update, "SELECTED-UPDATE").await?;

            let admission_limits = testutils::AdmissionLimit::new().await;
            admission_limits.set_max_open_files(1);
            let permit = walk::ensure_leaf_permit(PermitKind::OpenFile, None).await;
            let parent = Arc::new(
                Dir::open_parent_dir(&test_path, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let dst_parent = Arc::new(
                Dir::open_parent_dir(&test_path, congestion::Side::Destination)
                    .await?
                    .into_tree(),
            );
            let src_handle = parent.child(std::ffi::OsStr::new("src")).await?;
            let update_handle = parent.child(std::ffi::OsStr::new("update")).await?;
            let mut entry = AdmittedLinkEntry::new(src_handle, permit);
            entry.update_handle = Some(update_handle);

            tokio::fs::remove_file(&update).await?;
            tokio::fs::write(&update, "REPLACEMENT-CONTENT").await?;
            run_with_open_file_cleanup(
                &admission_limits,
                link_internal(
                    &PROGRESS,
                    &parent,
                    Some((&parent, std::ffi::OsStr::new("update"))),
                    Some(&dst_parent),
                    std::ffi::OsStr::new("src"),
                    &src,
                    &dst,
                    Some(&update),
                    std::path::Path::new(""),
                    UpdateRootRequirement::Optional,
                    &common_settings(false, false),
                    false,
                    LinkEntryAdmission::Filtered(entry),
                ),
            )
            .await??;

            assert_eq!(
                tokio::fs::read_to_string(&dst).await?,
                "REPLACEMENT-CONTENT"
            );
            Ok(())
        }

        /// A forced-unknown update-only file cannot authorize an excluded replacement directory.
        #[tokio::test]
        #[traced_test]
        async fn update_only_unknown_file_swap_to_excluded_directory_never_materializes_replacement()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            let admission_limits = testutils::AdmissionLimit::new().await;
            admission_limits.set_max_open_files(1);
            tokio::fs::create_dir(&update).await?;
            tokio::fs::create_dir(&dst).await?;
            let entry_path = update.join("node");
            let dst_path = dst.join("node");
            tokio::fs::write(&entry_path, "SELECTED-UPDATE-FILE").await?;
            let update_parent =
                Arc::new(Dir::open_root_dir(&update, false, congestion::Side::Source).await?);
            let dst_parent =
                Arc::new(Dir::open_root_dir(&dst, false, congestion::Side::Destination).await?);
            let mut filter = FilterSettings::new();
            filter.add_exclude("node/")?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            let selected = select_update_only_for_dispatch(
                &update_parent,
                std::ffi::OsStr::new("node"),
                None,
                std::path::Path::new("node"),
                &settings,
            )
            .await?;
            let UpdateOnlyDecision::Admitted(entry) = selected else {
                panic!("the forced-unknown file must pass the non-directory filter")
            };
            assert_eq!(entry.kind(), EntryKind::File);

            tokio::fs::remove_file(&entry_path).await?;
            tokio::fs::create_dir(&entry_path).await?;
            tokio::fs::write(entry_path.join("replacement.txt"), "UNSELECTED").await?;
            let result = run_with_open_file_cleanup(
                &admission_limits,
                delegate_copy(
                    &PROGRESS,
                    &update_parent,
                    Some(&dst_parent),
                    std::ffi::OsStr::new("node"),
                    &entry_path,
                    &dst_path,
                    std::path::Path::new("node"),
                    copy::DeleteScanAnchor::new(&dst, std::path::Path::new("node")),
                    &settings,
                    false,
                    entry,
                ),
            )
            .await?;

            assert!(
                result.is_err(),
                "a selected file reopened as a directory must fail closed"
            );
            assert!(
                !dst_path.exists(),
                "a failed-closed type swap must not create a destination"
            );
            Ok(())
        }

        /// A nested source rechecks the exact type after a `DT_UNKNOWN` filter probe.
        #[tokio::test]
        #[traced_test]
        async fn nested_source_unknown_file_swap_to_excluded_directory_is_rechecked()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&dst).await?;
            let entry_path = src.join("node");
            tokio::fs::write(&entry_path, "SELECTED-FILE").await?;
            tokio::fs::write(dst.join("node"), "STALE-DESTINATION").await?;
            let src_parent =
                Arc::new(Dir::open_root_dir(&src, false, congestion::Side::Source).await?);
            let dst_parent =
                Arc::new(Dir::open_root_dir(&dst, false, congestion::Side::Destination).await?);
            let mut filter = FilterSettings::new();
            filter.add_exclude("node/")?;
            let initially_is_dir = walk::filter_is_dir(
                Some(&filter),
                &src_parent,
                std::ffi::OsStr::new("node"),
                None,
                false,
            )
            .await?;
            assert!(!initially_is_dir);
            assert!(
                walk::should_skip_entry(&Some(filter.clone()), std::path::Path::new("node"), false)
                    .is_none()
            );

            tokio::fs::remove_file(&entry_path).await?;
            tokio::fs::create_dir(&entry_path).await?;
            tokio::fs::write(entry_path.join("replacement.txt"), "UNSELECTED").await?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            let delete_settings = copy::DeleteSettings {
                delete_excluded: true,
            };
            let mut keep_set = DeleteKeepSet::new(Some(&delete_settings));
            let result = crate::walk_driver::scope_tasks(link_internal(
                &PROGRESS,
                &src_parent,
                None,
                Some(&dst_parent),
                std::ffi::OsStr::new("node"),
                &src,
                &dst,
                None,
                std::path::Path::new("node"),
                UpdateRootRequirement::Optional,
                &settings,
                false,
                EntryAdmission::RootOrDelegated.into(),
            ))
            .await?;
            let mut summary = Summary::default();
            LinkTaskResult::from_link(std::ffi::OsString::from("node"), result)
                .fold(&mut summary, &mut keep_set);
            crate::delete::prune_extraneous(
                &PROGRESS,
                &dst_parent,
                std::path::Path::new(""),
                keep_set.as_set().expect("delete is enabled"),
                settings.filter.as_ref(),
                &delete_settings,
                false,
                None,
            )
            .await?;

            assert!(
                !dst.join("node").exists(),
                "the exact filtered result did not remove the stale outer keep decision"
            );
            assert_eq!(summary.copy_summary.directories_skipped, 1);
            Ok(())
        }

        /// A stale excluded-directory hint cannot suppress an included file when delete will consume
        /// the resulting keep-set.
        #[tokio::test]
        #[traced_test]
        async fn destructive_source_stale_directory_hint_rechecks_included_file()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&dst).await?;
            tokio::fs::write(src.join("node"), "CURRENT-SOURCE").await?;
            tokio::fs::write(dst.join("node"), "STALE-DESTINATION").await?;
            let src_dir =
                Arc::new(Dir::open_root_dir(&src, false, congestion::Side::Source).await?);
            let dst_dir =
                Arc::new(Dir::open_root_dir(&dst, false, congestion::Side::Destination).await?);

            let mut filter = FilterSettings::new();
            filter.add_exclude("node/")?;
            let delete_settings = copy::DeleteSettings {
                delete_excluded: false,
            };
            let mut settings = common_settings(false, true);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            settings.copy_settings.delete = Some(delete_settings.clone());
            let decision = select_source_for_dispatch(
                &src_dir,
                std::ffi::OsStr::new("node"),
                Some(EntryKind::Dir),
                std::path::Path::new("node"),
                &settings,
                false,
            )
            .await?;

            let mut summary = Summary::default();
            let mut keep_set = DeleteKeepSet::new(Some(&delete_settings));
            match decision {
                SourceEntryDecision::Filtered { kind, .. } => {
                    summary = summary + skipped_summary_for(kind);
                }
                SourceEntryDecision::Dispatch(admission) => {
                    let admission =
                        walk::ensure_entry_admission(PermitKind::OpenFile, admission).await;
                    let result = crate::walk_driver::scope_tasks(link_internal(
                        &PROGRESS,
                        &src_dir,
                        None,
                        Some(&dst_dir),
                        std::ffi::OsStr::new("node"),
                        &src,
                        &dst,
                        None,
                        std::path::Path::new("node"),
                        UpdateRootRequirement::Optional,
                        &settings,
                        false,
                        admission.into(),
                    ))
                    .await?;
                    LinkTaskResult::from_link(std::ffi::OsString::from("node"), result)
                        .fold(&mut summary, &mut keep_set);
                }
            }
            crate::delete::prune_extraneous(
                &PROGRESS,
                &dst_dir,
                std::path::Path::new(""),
                keep_set.as_set().expect("delete is enabled"),
                settings.filter.as_ref(),
                &delete_settings,
                false,
                None,
            )
            .await?;

            assert_eq!(
                tokio::fs::read_to_string(dst.join("node")).await?,
                "CURRENT-SOURCE",
                "the exact included file must materialize and protect its destination name"
            );
            assert_eq!(summary.hard_links_created, 1);
            Ok(())
        }

        /// Dry-run reporting must come from the exact type rather than a stale excluded-directory
        /// hint.
        #[tokio::test]
        #[traced_test]
        async fn dry_run_source_stale_directory_hint_reports_included_file()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::write(src.join("node"), "CURRENT-SOURCE").await?;
            let src_dir =
                Arc::new(Dir::open_root_dir(&src, false, congestion::Side::Source).await?);

            let mut filter = FilterSettings::new();
            filter.add_exclude("node/")?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            let decision = select_source_for_dispatch(
                &src_dir,
                std::ffi::OsStr::new("node"),
                Some(EntryKind::Dir),
                std::path::Path::new("node"),
                &settings,
                false,
            )
            .await?;

            let mut summary = Summary::default();
            let mut keep_set = DeleteKeepSet::new(None);
            match decision {
                SourceEntryDecision::Filtered { kind, .. } => {
                    summary = summary + skipped_summary_for(kind);
                }
                SourceEntryDecision::Dispatch(admission) => {
                    let admission =
                        walk::ensure_entry_admission(PermitKind::OpenFile, admission).await;
                    let result = crate::walk_driver::scope_tasks(link_internal(
                        &PROGRESS,
                        &src_dir,
                        None,
                        None,
                        std::ffi::OsStr::new("node"),
                        &src,
                        &dst,
                        None,
                        std::path::Path::new("node"),
                        UpdateRootRequirement::Optional,
                        &settings,
                        false,
                        admission.into(),
                    ))
                    .await?;
                    LinkTaskResult::from_link(std::ffi::OsString::from("node"), result)
                        .fold(&mut summary, &mut keep_set);
                }
            }

            assert_eq!(summary.hard_links_created, 1);
            assert_eq!(summary.copy_summary.directories_skipped, 0);
            assert!(!dst.join("node").exists());
            Ok(())
        }

        /// A stale special-file hint cannot suppress a regular file that the exact worker would
        /// materialize.
        #[tokio::test]
        #[traced_test]
        async fn skip_specials_stale_special_hint_rechecks_regular_file()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src = test_path.join("src");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&dst).await?;
            tokio::fs::write(src.join("node"), "CURRENT-SOURCE").await?;
            tokio::fs::write(dst.join("node"), "STALE-DESTINATION").await?;
            let src_dir =
                Arc::new(Dir::open_root_dir(&src, false, congestion::Side::Source).await?);
            let dst_dir =
                Arc::new(Dir::open_root_dir(&dst, false, congestion::Side::Destination).await?);

            let delete_settings = copy::DeleteSettings {
                delete_excluded: false,
            };
            let mut settings = common_settings(false, true);
            settings.copy_settings.skip_specials = true;
            settings.copy_settings.delete = Some(delete_settings.clone());
            let decision = select_source_for_dispatch(
                &src_dir,
                std::ffi::OsStr::new("node"),
                Some(EntryKind::Special),
                std::path::Path::new("node"),
                &settings,
                false,
            )
            .await?;

            let mut summary = Summary::default();
            let mut keep_set = DeleteKeepSet::new(Some(&delete_settings));
            match decision {
                SourceEntryDecision::Filtered { kind, .. } => {
                    summary = summary + skipped_summary_for(kind);
                }
                SourceEntryDecision::Dispatch(admission) => {
                    let admission =
                        walk::ensure_entry_admission(PermitKind::OpenFile, admission).await;
                    let result = crate::walk_driver::scope_tasks(link_internal(
                        &PROGRESS,
                        &src_dir,
                        None,
                        Some(&dst_dir),
                        std::ffi::OsStr::new("node"),
                        &src,
                        &dst,
                        None,
                        std::path::Path::new("node"),
                        UpdateRootRequirement::Optional,
                        &settings,
                        false,
                        admission.into(),
                    ))
                    .await?;
                    LinkTaskResult::from_link(std::ffi::OsString::from("node"), result)
                        .fold(&mut summary, &mut keep_set);
                }
            }
            crate::delete::prune_extraneous(
                &PROGRESS,
                &dst_dir,
                std::path::Path::new(""),
                keep_set.as_set().expect("delete is enabled"),
                None,
                &delete_settings,
                false,
                None,
            )
            .await?;

            assert_eq!(
                tokio::fs::read_to_string(dst.join("node")).await?,
                "CURRENT-SOURCE",
                "the exact regular file must replace stale destination content"
            );
            assert_eq!(summary.hard_links_created, 1);
            assert_eq!(summary.copy_summary.specials_skipped, 0);
            Ok(())
        }

        /// An update-only stale excluded-directory hint must be classified exactly before a delete
        /// keep-set is finalized.
        #[tokio::test]
        #[traced_test]
        async fn destructive_update_stale_directory_hint_rechecks_included_file()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&update).await?;
            tokio::fs::create_dir(&dst).await?;
            tokio::fs::write(update.join("node"), "CURRENT-UPDATE").await?;
            tokio::fs::write(dst.join("node"), "STALE-DESTINATION").await?;
            let update_dir =
                Arc::new(Dir::open_root_dir(&update, false, congestion::Side::Source).await?);
            let dst_dir =
                Arc::new(Dir::open_root_dir(&dst, false, congestion::Side::Destination).await?);

            let mut filter = FilterSettings::new();
            filter.add_exclude("node/")?;
            let delete_settings = copy::DeleteSettings {
                delete_excluded: false,
            };
            let mut settings = common_settings(false, true);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            settings.copy_settings.delete = Some(delete_settings.clone());
            let decision = select_update_only_for_dispatch(
                &update_dir,
                std::ffi::OsStr::new("node"),
                Some(EntryKind::Dir),
                std::path::Path::new("node"),
                &settings,
            )
            .await?;
            let decision = match decision {
                UpdateOnlyDecision::Hinted(kind) => {
                    classify_hinted_update_only_for_dispatch(
                        &update_dir,
                        std::ffi::OsStr::new("node"),
                        kind,
                        std::path::Path::new("node"),
                        settings.filter.as_ref(),
                    )
                    .await?
                }
                decision => decision,
            };

            let mut summary = Summary::default();
            let mut keep_set = DeleteKeepSet::new(Some(&delete_settings));
            match decision {
                UpdateOnlyDecision::Skipped { kind, .. } => {
                    summary = summary + skipped_summary_for(kind);
                }
                UpdateOnlyDecision::Admitted(entry) => {
                    let result = delegate_copy(
                        &PROGRESS,
                        &update_dir,
                        Some(&dst_dir),
                        std::ffi::OsStr::new("node"),
                        &update.join("node"),
                        &dst.join("node"),
                        std::path::Path::new("node"),
                        copy::DeleteScanAnchor::new(&dst, std::path::Path::new("node")),
                        &settings,
                        false,
                        entry,
                    )
                    .await?;
                    LinkTaskResult::from_update(std::ffi::OsString::from("node"), result)
                        .fold(&mut summary, &mut keep_set);
                }
                UpdateOnlyDecision::Hinted(_) => {
                    unreachable!("a reliable hint must be classified before dispatch")
                }
            }
            crate::delete::prune_extraneous(
                &PROGRESS,
                &dst_dir,
                std::path::Path::new(""),
                keep_set.as_set().expect("delete is enabled"),
                settings.filter.as_ref(),
                &delete_settings,
                false,
                None,
            )
            .await?;

            assert_eq!(
                tokio::fs::read_to_string(dst.join("node")).await?,
                "CURRENT-UPDATE",
                "the exact included update file must materialize and protect its destination name"
            );
            assert_eq!(summary.copy_summary.files_copied, 1);
            Ok(())
        }

        /// Update-only preview reporting must also reclassify a stale excluded-directory hint.
        #[tokio::test]
        #[traced_test]
        async fn dry_run_update_stale_directory_hint_reports_included_file()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let update = test_path.join("update");
            let dst = test_path.join("dst");
            tokio::fs::create_dir(&update).await?;
            tokio::fs::write(update.join("node"), "CURRENT-UPDATE").await?;
            let update_dir =
                Arc::new(Dir::open_root_dir(&update, false, congestion::Side::Source).await?);

            let mut filter = FilterSettings::new();
            filter.add_exclude("node/")?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            let decision = select_update_only_for_dispatch(
                &update_dir,
                std::ffi::OsStr::new("node"),
                Some(EntryKind::Dir),
                std::path::Path::new("node"),
                &settings,
            )
            .await?;
            let decision = match decision {
                UpdateOnlyDecision::Hinted(kind) => {
                    classify_hinted_update_only_for_dispatch(
                        &update_dir,
                        std::ffi::OsStr::new("node"),
                        kind,
                        std::path::Path::new("node"),
                        settings.filter.as_ref(),
                    )
                    .await?
                }
                decision => decision,
            };

            let mut summary = Summary::default();
            let mut keep_set = DeleteKeepSet::new(None);
            match decision {
                UpdateOnlyDecision::Skipped { kind, .. } => {
                    summary = summary + skipped_summary_for(kind);
                }
                UpdateOnlyDecision::Admitted(entry) => {
                    let result = delegate_copy(
                        &PROGRESS,
                        &update_dir,
                        None,
                        std::ffi::OsStr::new("node"),
                        &update.join("node"),
                        &dst.join("node"),
                        std::path::Path::new("node"),
                        copy::DeleteScanAnchor::new(&dst, std::path::Path::new("node")),
                        &settings,
                        false,
                        entry,
                    )
                    .await?;
                    LinkTaskResult::from_update(std::ffi::OsString::from("node"), result)
                        .fold(&mut summary, &mut keep_set);
                }
                UpdateOnlyDecision::Hinted(_) => {
                    unreachable!("a reliable hint must be classified before dispatch")
                }
            }

            assert_eq!(summary.copy_summary.files_copied, 1);
            assert_eq!(summary.copy_summary.directories_skipped, 0);
            assert!(!dst.join("node").exists());
            Ok(())
        }

        /// A jointly excluded dual root skips before validating an unused destination.
        #[tokio::test]
        #[traced_test]
        async fn excluded_dual_root_skips_before_destination_validation()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let src_parent = test_path.join("source");
            let update_parent = test_path.join("update");
            let src = src_parent.join("blocked");
            let update = update_parent.join("blocked");
            tokio::fs::create_dir(&src_parent).await?;
            tokio::fs::create_dir(&update_parent).await?;
            tokio::fs::write(&src, "SRC").await?;
            tokio::fs::write(&update, "UPDATE").await?;
            let mut filter = FilterSettings::new();
            filter.add_exclude("blocked").unwrap();
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter);
            let summary = link(
                &PROGRESS,
                &test_path,
                &src,
                std::path::Path::new("/"),
                &Some(update),
                &settings,
                false,
            )
            .await?;
            assert_eq!(summary.copy_summary.files_skipped, 1);
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

        #[tokio::test]
        async fn update_delete_preview_keeps_direct_and_delegated_pruning_beneath_outer_dst()
        -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src = root.join("src");
            let update = root.join("update");
            let outside = root.join("outside");
            let dst = root.join("dst");
            tokio::fs::create_dir_all(src.join("direct")).await?;
            tokio::fs::create_dir_all(update.join("delegated")).await?;
            tokio::fs::create_dir_all(outside.join("direct")).await?;
            tokio::fs::create_dir_all(outside.join("delegated")).await?;
            tokio::fs::write(outside.join("direct/stale"), "DIRECT-OUTSIDE").await?;
            tokio::fs::write(outside.join("delegated/stale"), "DELEGATED-OUTSIDE").await?;
            tokio::fs::symlink(&outside, &dst).await?;
            let mut settings = common_settings(false, true);
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            settings.copy_settings.dry_run = settings.dry_run;
            settings.copy_settings.delete = Some(copy::DeleteSettings {
                delete_excluded: false,
            });
            let summary = link(
                &PROGRESS,
                &root,
                &src,
                &dst,
                &Some(update.clone()),
                &settings,
                false,
            )
            .await?;
            assert_eq!(summary.copy_summary.rm_summary.files_removed, 0);
            assert_eq!(
                tokio::fs::read_to_string(outside.join("direct/stale")).await?,
                "DIRECT-OUTSIDE"
            );
            assert_eq!(
                tokio::fs::read_to_string(outside.join("delegated/stale")).await?,
                "DELEGATED-OUTSIDE"
            );

            let real_dst = root.join("real-dst");
            tokio::fs::create_dir_all(real_dst.join("direct")).await?;
            tokio::fs::create_dir_all(real_dst.join("delegated")).await?;
            tokio::fs::write(real_dst.join("delegated/stale"), "INSIDE-STALE").await?;
            let real_summary = link(
                &PROGRESS,
                &root,
                &src,
                &real_dst,
                &Some(update),
                &settings,
                false,
            )
            .await?;
            assert_eq!(real_summary.copy_summary.rm_summary.files_removed, 1);
            assert_eq!(
                tokio::fs::read_to_string(real_dst.join("delegated/stale")).await?,
                "INSIDE-STALE"
            );
            Ok(())
        }

        #[tokio::test]
        async fn update_exclusive_delete_preview_keeps_selected_shared_file()
        -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src = root.join("src");
            let update = root.join("update");
            let dst = root.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::create_dir(&dst).await?;
            tokio::fs::write(src.join("shared"), b"old").await?;
            tokio::fs::write(update.join("shared"), b"replacement").await?;
            tokio::fs::write(dst.join("shared"), b"destination").await?;
            tokio::fs::write(dst.join("stale"), b"stale").await?;

            let mut settings = common_settings(false, false);
            settings.update_exclusive = true;
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            settings.copy_settings.dry_run = settings.dry_run;
            settings.copy_settings.delete = Some(copy::DeleteSettings {
                delete_excluded: false,
            });

            let summary = link(
                &PROGRESS,
                &root,
                &src,
                &dst,
                &Some(update),
                &settings,
                false,
            )
            .await?;

            assert_eq!(summary.hard_links_created, 0);
            assert_eq!(summary.copy_summary.files_copied, 1);
            assert_eq!(summary.copy_summary.rm_summary.files_removed, 1);
            assert!(dst.join("shared").exists());
            assert!(dst.join("stale").exists());
            Ok(())
        }

        #[tokio::test]
        async fn update_exclusive_delete_preview_omits_source_only_file()
        -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src = root.join("src");
            let update = root.join("update");
            let dst = root.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::create_dir(&dst).await?;
            tokio::fs::write(src.join("source-only"), b"source").await?;
            tokio::fs::write(dst.join("source-only"), b"destination").await?;

            let mut settings = common_settings(false, false);
            settings.update_exclusive = true;
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            settings.copy_settings.dry_run = settings.dry_run;
            settings.copy_settings.delete = Some(copy::DeleteSettings {
                delete_excluded: false,
            });

            let summary = link(
                &PROGRESS,
                &root,
                &src,
                &dst,
                &Some(update),
                &settings,
                false,
            )
            .await?;

            assert_eq!(summary.hard_links_created, 0);
            assert_eq!(summary.copy_summary.files_copied, 0);
            assert_eq!(summary.copy_summary.rm_summary.files_removed, 1);
            assert!(dst.join("source-only").exists());
            Ok(())
        }

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

        #[tokio::test]
        async fn update_directory_transition_retains_exact_handle_and_held_admission()
        -> anyhow::Result<()> {
            use std::os::fd::AsRawFd as _;

            let root = testutils::create_temp_dir().await?;
            tokio::fs::write(root.join("source"), b"source").await?;
            tokio::fs::create_dir(root.join("update")).await?;
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let parent = Dir::open_root_dir(&root, false, congestion::Side::Source).await?;
            let src_handle = parent.child(std::ffi::OsStr::new("source")).await?;
            let update_handle = parent.child(std::ffi::OsStr::new("update")).await?;
            let src_fd = src_handle.as_fd().as_raw_fd();
            let update_fd = update_handle.as_fd().as_raw_fd();
            let mut entry = AdmittedLinkEntry::new(
                src_handle,
                Some(LeafPermit::OpenFile(throttle::open_file_permit().await)),
            );
            entry.update_handle = Some(update_handle);

            let update_entry = entry.into_update_entry();
            let retained_directory = update_entry.kind() == EntryKind::Dir;
            let transferred_held_admission = update_entry.admission().is_some();
            let source_closed = testutils::fd_is_closed(src_fd);
            let update_stayed_open = !testutils::fd_is_closed(update_fd);
            let mut next_permit = Box::pin(throttle::open_file_permit());
            let (capacity_stayed_held, early_permit) = match futures::poll!(next_permit.as_mut()) {
                std::task::Poll::Pending => (true, None),
                std::task::Poll::Ready(permit) => (false, Some(permit)),
            };
            drop(update_entry);
            let update_closed = testutils::fd_is_closed(update_fd);
            let reacquire_result = match early_permit {
                Some(permit) => Ok(permit),
                None => {
                    tokio::time::timeout(std::time::Duration::from_secs(1), next_permit.as_mut())
                        .await
                        .context("the delegated directory did not return open-file capacity")
                }
            };
            let reacquire_result = reacquire_result.map(drop);
            drop(parent);
            let cleanup_result = tokio::fs::remove_dir_all(root).await;
            reacquire_result?;
            cleanup_result?;
            assert!(
                retained_directory,
                "the admitted update owner lost its authoritative directory type"
            );
            assert!(transferred_held_admission);
            assert!(source_closed);
            assert!(update_stayed_open);
            assert!(update_closed);
            assert!(
                capacity_stayed_held,
                "the admitted update owner released capacity before copy dispatch"
            );
            Ok(())
        }

        /// A reliable file hint lets the rlink filter skip before open-file admission.
        #[tokio::test]
        async fn filtered_hinted_file_does_not_wait_for_admission() -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src = root.join("src");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::write(src.join("leaf"), b"x").await?;
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let held = throttle::open_file_permit().await;
            let src_dir =
                Arc::new(Dir::open_root_dir(&src, false, congestion::Side::Source).await?);
            let hints = src_dir.read_entries().await?;
            assert_eq!(
                hints,
                vec![(std::ffi::OsString::from("leaf"), Some(EntryKind::File))],
                "the fixture filesystem must provide the reliable hint this test exercises"
            );
            let mut filter = crate::filter::FilterSettings::new();
            filter.add_exclude("leaf")?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            let dst = root.join("dst");
            let operation = link_dir_contents(
                &PROGRESS,
                &src_dir,
                None,
                None,
                None,
                std::ffi::OsStr::new("dst"),
                &src,
                &dst,
                None,
                std::path::Path::new(""),
                &src,
                &dst,
                false,
                false,
                None,
                &settings,
                Summary::default(),
            );
            let result = admission
                .run_with_timeout(std::time::Duration::from_secs(1), operation)
                .await;
            drop(held);
            let summary = result.context("rlink filter skip waited for open-file admission")??;
            assert_eq!(summary.copy_summary.files_skipped, 1);
            assert_eq!(summary.hard_links_created, 0);
            Ok(())
        }

        /// A reliable update-only file hint lets the filter skip before copy admission.
        #[tokio::test]
        async fn filtered_hinted_update_file_does_not_wait_for_admission()
        -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src = root.join("src");
            let update = root.join("update");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::write(update.join("leaf"), b"x").await?;
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let held = throttle::open_file_permit().await;
            let src_dir =
                Arc::new(Dir::open_root_dir(&src, false, congestion::Side::Source).await?);
            let update_dir =
                Arc::new(Dir::open_root_dir(&update, false, congestion::Side::Source).await?);
            let hints = update_dir.read_entries().await?;
            assert_eq!(
                hints,
                vec![(std::ffi::OsString::from("leaf"), Some(EntryKind::File))],
                "the fixture filesystem must provide the reliable hint this test exercises"
            );
            let mut filter = crate::filter::FilterSettings::new();
            filter.add_exclude("leaf")?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            let dst = root.join("dst");
            let operation = crate::walk_driver::scope_tasks(link_dir_contents(
                &PROGRESS,
                &src_dir,
                Some(&update_dir),
                None,
                None,
                std::ffi::OsStr::new("dst"),
                &src,
                &dst,
                Some(&update),
                std::path::Path::new(""),
                &src,
                &dst,
                false,
                false,
                None,
                &settings,
                Summary::default(),
            ));
            let result = admission
                .run_with_timeout(std::time::Duration::from_secs(1), operation)
                .await;
            drop(held);
            let summary =
                result.context("rlink update filter skip waited for open-file admission")??;
            assert_eq!(summary.copy_summary.files_skipped, 1);
            assert_eq!(summary.copy_summary.files_copied, 0);
            Ok(())
        }

        #[test]
        fn source_directory_hint_requires_admission_only_for_a_dual_tree_decision() {
            assert!(matches!(
                source_entry_admission(Some(EntryKind::Dir), true),
                EntryAdmission::RootOrDelegated
            ));
            assert!(matches!(
                source_entry_admission(Some(EntryKind::Dir), false),
                EntryAdmission::HintedDirectory
            ));
        }

        /// Rlink preserves a positive directory hint through its worker dispatch.
        #[tokio::test]
        async fn hinted_directory_classifies_while_pool_is_saturated() -> Result<(), anyhow::Error>
        {
            let root = testutils::create_temp_dir().await?;
            let src_path = root.join("src");
            let dst_path = root.join("dst");
            tokio::fs::create_dir(&src_path).await?;
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let held = throttle::open_file_permit().await;
            let src_parent = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let mut settings = common_settings(false, false);
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            settings.copy_settings.dry_run = settings.dry_run;
            let result = admission
                .run_with_timeout(
                    std::time::Duration::from_secs(1),
                    link_internal(
                        &PROGRESS,
                        &src_parent,
                        None,
                        None,
                        std::ffi::OsStr::new("src"),
                        &src_path,
                        &dst_path,
                        None,
                        std::path::Path::new(""),
                        UpdateRootRequirement::Optional,
                        &settings,
                        false,
                        walk::EntryAdmission::HintedDirectory.into(),
                    ),
                )
                .await;
            drop(held);
            let summary = result.context("hinted rlink directory waited for admission")??;
            assert_eq!(summary.summary.copy_summary.directories_created, 1);
            Ok(())
        }

        /// A separate update counterpart makes a source-directory hint insufficient to classify
        /// the combined link decision without leaf admission.
        #[tokio::test(flavor = "current_thread")]
        async fn hinted_source_with_update_admits_before_source_classification()
        -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src_root = root.join("src");
            let update_root = root.join("update");
            let dst_root = root.join("dst");
            tokio::fs::create_dir(&src_root).await?;
            tokio::fs::create_dir(&update_root).await?;
            tokio::fs::create_dir(src_root.join("entry")).await?;
            tokio::fs::write(update_root.join("entry"), b"x").await?;

            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let stat_resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            admission.set_max_ops_in_flight(stat_resource, 1);
            let src_parent =
                Arc::new(Dir::open_root_dir(&src_root, false, congestion::Side::Source).await?);
            let update_parent =
                Arc::new(Dir::open_root_dir(&update_root, false, congestion::Side::Source).await?);
            let held_stat = throttle::ops_in_flight_permit(stat_resource).await;
            let mut settings = common_settings(false, false);
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            settings.copy_settings.dry_run = settings.dry_run;

            let operation = link_internal(
                &PROGRESS,
                &src_parent,
                Some((&update_parent, std::ffi::OsStr::new("entry"))),
                None,
                std::ffi::OsStr::new("entry"),
                &src_root,
                &dst_root,
                Some(&update_root),
                std::path::Path::new("entry"),
                UpdateRootRequirement::Optional,
                &settings,
                false,
                EntryAdmission::HintedDirectory.into(),
            );
            tokio::pin!(operation);
            assert!(
                futures::poll!(operation.as_mut()).is_pending(),
                "source classification must stop at the saturated Stat gate"
            );
            let mut permit_probe = Box::pin(throttle::open_file_permit());
            let admitted_before_source = futures::poll!(permit_probe.as_mut()).is_pending();
            drop(permit_probe);

            drop(held_stat);
            let summary = admission
                .run_with_timeout(std::time::Duration::from_secs(20), operation.as_mut())
                .await
                .context("rlink did not resume after source classification was released")??;
            assert!(
                admitted_before_source,
                "the hinted source opened before admission for its separate update decision"
            );
            assert_eq!(summary.summary.copy_summary.files_copied, 1);
            Ok(())
        }

        /// An update-only directory preserves its hint through delegation into copy.
        #[tokio::test]
        async fn hinted_update_directory_classifies_while_pool_is_saturated()
        -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src = root.join("src");
            let update = root.join("update");
            let dst = root.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            tokio::fs::create_dir(update.join("dir")).await?;
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let held = throttle::open_file_permit().await;
            let src_dir =
                Arc::new(Dir::open_root_dir(&src, false, congestion::Side::Source).await?);
            let update_dir =
                Arc::new(Dir::open_root_dir(&update, false, congestion::Side::Source).await?);
            let mut settings = common_settings(false, false);
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            settings.copy_settings.dry_run = settings.dry_run;
            let operation = crate::walk_driver::scope_tasks(link_dir_contents(
                &PROGRESS,
                &src_dir,
                Some(&update_dir),
                None,
                None,
                std::ffi::OsStr::new("dst"),
                &src,
                &dst,
                Some(&update),
                std::path::Path::new(""),
                &src,
                &dst,
                false,
                false,
                None,
                &settings,
                Summary::default(),
            ));
            let result = admission
                .run_with_timeout(std::time::Duration::from_secs(1), operation)
                .await;
            drop(held);
            let summary =
                result.context("hinted update directory waited for delegated copy admission")??;
            assert_eq!(summary.copy_summary.directories_created, 1);
            Ok(())
        }

        /// Public root setup must reserve open-file capacity before strict probes or operand-parent
        /// opens. A filtered root otherwise returns before `link_internal` reaches its admission
        /// point, and concurrent callers can bypass the budget entirely.
        #[tokio::test]
        async fn filtered_public_root_waits_for_admission_before_setup() -> Result<(), anyhow::Error>
        {
            let root = testutils::create_temp_dir().await?;
            let src = root.join("src");
            let dst = root.join("dst");
            tokio::fs::write(&src, b"x").await?;
            let mut filter = crate::filter::FilterSettings::new();
            filter.add_exclude("src")?;
            let mut settings = common_settings(false, false);
            settings.filter = Some(filter.clone());
            settings.copy_settings.filter = Some(filter);
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let stat_resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            admission.set_max_ops_in_flight(stat_resource, 1);
            let held_stat = throttle::ops_in_flight_permit(stat_resource).await;
            let operation = link(&PROGRESS, &root, &src, &dst, &None, &settings, false);
            tokio::pin!(operation);
            let stopped_at_stat_gate = futures::poll!(operation.as_mut()).is_pending();
            let mut second_permit = Box::pin(throttle::open_file_permit());
            let setup_bypassed_admission = futures::poll!(second_permit.as_mut()).is_ready();
            drop(second_permit);
            drop(held_stat);
            let result = admission
                .run_with_timeout(std::time::Duration::from_secs(20), operation.as_mut())
                .await;
            assert!(
                stopped_at_stat_gate,
                "rlink root did not reach the held stat gate"
            );
            let summary =
                result.context("rlink root did not resume after stat capacity was released")??;
            assert!(
                !setup_bypassed_admission,
                "rlink performed root setup before open-file admission"
            );
            assert_eq!(summary.copy_summary.files_skipped, 1);
            Ok(())
        }

        /// A root or delegated rlink worker without a caller-supplied guard must reserve capacity
        /// before opening its source classification handle.
        #[tokio::test]
        async fn root_acquires_capacity_before_classification() -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src_path = root.join("src");
            let dst_path = root.join("dst");
            tokio::fs::write(&src_path, b"x").await?;
            let admission = testutils::AdmissionLimit::new().await;
            let src_parent = Arc::new(
                Dir::open_parent_dir(&root, congestion::Side::Source)
                    .await?
                    .into_tree(),
            );
            let mut settings = common_settings(false, false);
            settings.dry_run = Some(crate::config::DryRunMode::Brief);
            settings.copy_settings.dry_run = settings.dry_run;
            admission.set_max_open_files(1);
            let stat_resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            admission.set_max_ops_in_flight(stat_resource, 1);
            let held_stat = throttle::ops_in_flight_permit(stat_resource).await;
            let operation = link_internal(
                &PROGRESS,
                &src_parent,
                None,
                None,
                std::ffi::OsStr::new("src"),
                &src_path,
                &dst_path,
                None,
                std::path::Path::new(""),
                UpdateRootRequirement::Optional,
                &settings,
                false,
                EntryAdmission::RootOrDelegated.into(),
            );
            tokio::pin!(operation);
            let stopped_at_stat_gate = futures::poll!(operation.as_mut()).is_pending();
            let mut second_permit = Box::pin(throttle::open_file_permit());
            let classification_bypassed_admission =
                futures::poll!(second_permit.as_mut()).is_ready();
            drop(second_permit);
            drop(held_stat);
            let result = admission
                .run_with_timeout(std::time::Duration::from_secs(20), operation.as_mut())
                .await;
            assert!(
                stopped_at_stat_gate,
                "rlink root did not reach the held stat gate"
            );
            assert!(
                !classification_bypassed_admission,
                "rlink reached fd-based classification before open-file admission"
            );
            let summary = result.context(
                "rlink root did not resume after classification capacity was released",
            )??;
            assert_eq!(summary.summary.hard_links_created, 1);
            Ok(())
        }

        /// A hard-link leaf must retain its admission guard through the actual
        /// fd-bearing `linkat`, not release it after source/update classification.
        #[tokio::test]
        async fn hard_link_holds_open_file_capacity_until_link_finishes()
        -> Result<(), anyhow::Error> {
            let root = testutils::create_temp_dir().await?;
            let src_root = root.join("src");
            let dst_root = root.join("dst");
            tokio::fs::create_dir(&src_root).await?;
            tokio::fs::create_dir(&dst_root).await?;
            tokio::fs::write(src_root.join("entry"), b"x").await?;
            let src_parent =
                Arc::new(Dir::open_root_dir(&src_root, false, congestion::Side::Source).await?);
            let dst_parent = Arc::new(
                Dir::open_root_dir(&dst_root, false, congestion::Side::Destination).await?,
            );
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let permit = Some(LeafPermit::OpenFile(throttle::open_file_permit().await));
            let hard_link_resource = throttle::Resource::meta(
                throttle::Side::Destination,
                throttle::MetadataOp::HardLink,
            );
            admission.set_max_ops_in_flight(hard_link_resource, 1);
            let held_hard_link = throttle::ops_in_flight_permit(hard_link_resource).await;
            let dst_entry = dst_root.join("entry");
            let task_dst_entry = dst_entry.clone();
            let mut task = tokio::spawn(async move {
                link_internal(
                    &PROGRESS,
                    &src_parent,
                    None,
                    Some(&dst_parent),
                    std::ffi::OsStr::new("entry"),
                    &src_root.join("entry"),
                    &task_dst_entry,
                    None,
                    std::path::Path::new(""),
                    UpdateRootRequirement::Optional,
                    &common_settings(false, false),
                    true,
                    EntryAdmission::from(permit).into(),
                )
                .await
            });
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let second_permit = tokio::time::timeout(
                std::time::Duration::from_millis(500),
                throttle::open_file_permit(),
            )
            .await;
            let released_before_link_finished = second_permit.is_ok();
            drop(second_permit);
            drop(held_hard_link);
            let task_result = admission
                .run_with_timeout(std::time::Duration::from_secs(20), &mut task)
                .await
                .context("hard link did not resume after metadata capacity was released")?;
            let summary = task_result?.map_err(|error| error.source)?;
            assert!(
                !released_before_link_finished,
                "hard-link work released its open-file permit before linkat completed"
            );
            assert_eq!(summary.summary.hard_links_created, 1);
            assert!(dst_entry.exists());
            Ok(())
        }

        /// Deep + wide link: a directory tree deeper than the open-files limit, with files at every
        /// level. Verifies directories do not retain leaf admission across recursion.
        #[tokio::test]
        #[traced_test]
        async fn deep_tree_no_deadlock_under_open_files_saturation() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let dst = tmp_dir.join("dst");
            let depth = 20;
            let files_per_level = 5;
            let limit = 1;
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
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(limit);
            let summary = admission
                .run_with_timeout(
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

        /// Matching source/update directories release cap-one admission before direct recursion.
        /// Retaining the permit in [`AdmittedLinkEntry::close_for_directory`] would leave the next
        /// directory in this chain waiting forever for the only slot.
        #[tokio::test]
        #[traced_test]
        async fn matching_dual_directory_chain_releases_admission_before_recursion()
        -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let update = tmp_dir.join("update");
            let dst = tmp_dir.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            let depth = 12;
            let mut src_dir = src.clone();
            let mut update_dir = update.clone();
            for level in 0..depth {
                let name = format!("d{level}");
                src_dir.push(&name);
                update_dir.push(&name);
                tokio::fs::create_dir(&src_dir).await?;
                tokio::fs::create_dir(&update_dir).await?;
            }
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let summary = admission
                .run_with_timeout(
                    std::time::Duration::from_secs(5),
                    link(
                        &PROGRESS,
                        tmp_dir.as_path(),
                        &src,
                        &dst,
                        &Some(update),
                        &common_settings(false, false),
                        false,
                    ),
                )
                .await
                .context("link timed out — direct dual-directory recursion retained admission")?
                .context("link failed")?;
            assert_eq!(summary.copy_summary.directories_created, depth + 1);
            assert!(
                src_dir
                    .strip_prefix(&src)
                    .is_ok_and(|tail| dst.join(tail).is_dir())
            );
            Ok(())
        }

        /// A file-type-changed directory must release transferred admission before recursion.
        ///
        /// Scenario: many src entries are regular files (so the spawn loop
        /// pre-acquires open-files permits for them), but the corresponding
        /// `update` entries are directories (file types differ). link_internal
        /// then closes the source comparison handle and transfers the selected update handle plus
        /// admission into `copy_child`. The copy driver dispatches that exact classification and
        /// releases admission before directory descent; retaining it while children acquire would
        /// deadlock against a saturated pool.
        #[tokio::test]
        #[traced_test]
        async fn type_changed_directory_releases_admission_before_recursion()
        -> Result<(), anyhow::Error> {
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
            // saturate the pool so retaining delegated admission during descent blocks child work.
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(2);
            let summary = admission
                .run_with_timeout(
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
                    "link timed out — delegated admission was retained during directory descent",
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

        /// Update-only scheduling must stop before spawning a second fd-bearing copy when the
        /// first owns all open-file capacity. The ReadLink gate keeps that first worker live long
        /// enough to observe the number of started entry tasks deterministically.
        #[tokio::test]
        async fn update_only_spawning_stops_at_open_file_capacity() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let update = tmp_dir.join("update");
            let dst = tmp_dir.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&update).await?;
            let n = 8;
            for i in 0..n {
                tokio::fs::symlink(format!("target-{i}"), update.join(format!("u{i}"))).await?;
            }
            let progress: &'static progress::Progress =
                Box::leak(Box::new(progress::Progress::new()));
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let readlink_resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::ReadLink);
            admission.set_max_ops_in_flight(readlink_resource, 1);
            let held_readlink = throttle::ops_in_flight_permit(readlink_resource).await;
            let mut task = tokio::spawn(async move {
                link(
                    progress,
                    &tmp_dir,
                    &src,
                    &dst,
                    &Some(update),
                    &common_settings(false, false),
                    false,
                )
                .await
            });
            let first_worker = tokio::time::timeout(std::time::Duration::from_secs(20), async {
                loop {
                    if progress.ops.get().started >= 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            if let Err(error) = first_worker {
                drop(held_readlink);
                admission.quiesce(&mut task).await;
                return Err(error).context("the first update-only worker did not start");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let started_while_blocked = progress.ops.get().started;
            drop(held_readlink);
            let task_result = admission
                .run_with_timeout(std::time::Duration::from_secs(20), &mut task)
                .await
                .context("update-only copies did not resume after ReadLink was released")?;
            let summary = task_result?.map_err(|error| error.source)?;
            assert_eq!(
                started_while_blocked, 2,
                "root plus exactly one admitted update-only worker should be live"
            );
            assert_eq!(summary.copy_summary.symlinks_created, n);
            Ok(())
        }

        /// Regression: the update-only entries loop must transfer open-file admission into
        /// copy_child without deadlocking against copy's own classification or rm's pending-meta.
        ///
        /// Scenario: update has many regular files that don't exist in src.
        /// The loop at site 3 spawns a copy::copy task per entry under a
        /// saturated open-files pool. Each outer iteration transfers its guard into copy_child, so
        /// delegated classification does not reacquire while the caller retains capacity.
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
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(2);
            let summary = admission
                .run_with_timeout(
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

        /// Regression for update-only copy ↔ rm cross-pool self-deadlock.
        ///
        /// Scenario: update has many entries not in src; dst already has
        /// directories at those same names; the user passes --overwrite. Each
        /// update-only task delegates through `copy_child` / `copy_file_fd`, whose
        /// `remove_existing` invokes `rm_child` to remove the preexisting destination directory.
        /// rm draws from PendingMeta. If the outer task also held PendingMeta across delegated copy,
        /// every running task would hold a permit while waiting on inner rm to acquire one — a
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
                // --overwrite, `remove_existing` invokes `rm_child` to wipe it, which recurses under
                // PendingMeta admission
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
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(2);
            let summary = admission
                .run_with_timeout(
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
