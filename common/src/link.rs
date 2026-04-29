use anyhow::{Context, anyhow};
use async_recursion::async_recursion;
use std::os::linux::fs::MetadataExt as LinuxMetadataExt;
use tracing::instrument;

use crate::copy;
use crate::copy::{
    EmptyDirAction, Settings as CopySettings, Summary as CopySummary, check_empty_dir_cleanup,
};
use crate::filecmp;
use crate::preserve;
use crate::progress;
use crate::rm;
use crate::walk::{self, EntryKind};

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

fn is_hard_link(md1: &std::fs::Metadata, md2: &std::fs::Metadata) -> bool {
    copy::is_file_type_same(md1, md2)
        && md2.st_dev() == md1.st_dev()
        && md2.st_ino() == md1.st_ino()
}

#[instrument(skip(prog_track, settings))]
async fn hard_link_helper(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    src_metadata: &std::fs::Metadata,
    dst: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    let mut link_summary = Summary::default();
    match crate::walk::run_metadata_probed(
        congestion::Side::Destination,
        tokio::fs::hard_link(src, dst),
    )
    .await
    {
        Ok(()) => {}
        Err(error)
            if settings.copy_settings.overwrite
                && error.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            tracing::debug!("'dst' already exists, check if we need to update");
            let dst_metadata = crate::walk::run_metadata_probed(
                congestion::Side::Destination,
                tokio::fs::symlink_metadata(dst),
            )
            .await
            .with_context(|| format!("cannot read {dst:?} metadata"))
            .map_err(|err| Error::new(err, Default::default()))?;
            if is_hard_link(src_metadata, &dst_metadata) {
                tracing::debug!("no change, leaving file as is");
                prog_track.hard_links_unchanged.inc();
                return Ok(Summary {
                    hard_links_unchanged: 1,
                    ..Default::default()
                });
            }
            tracing::info!("'dst' file type changed, removing and hard-linking");
            let rm_summary = rm::rm(
                prog_track,
                dst,
                &rm::Settings {
                    fail_early: settings.copy_settings.fail_early,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await
            .map_err(|err| {
                let rm_summary = err.summary;
                link_summary.copy_summary.rm_summary = rm_summary;
                Error::new(err.source, link_summary)
            })?;
            link_summary.copy_summary.rm_summary = rm_summary;
            crate::walk::run_metadata_probed(
                congestion::Side::Destination,
                tokio::fs::hard_link(src, dst),
            )
            .await
            .with_context(|| format!("failed to hard link {src:?} to {dst:?}"))
            .map_err(|err| Error::new(err, link_summary))?;
        }
        Err(error) => {
            return Err(Error::new(
                anyhow::Error::from(error)
                    .context(format!("failed to hard link {src:?} to {dst:?}")),
                link_summary,
            ));
        }
    }
    prog_track.hard_links_created.inc();
    link_summary.hard_links_created = 1;
    Ok(link_summary)
}

/// Public entry point for link operations.
/// Internally delegates to link_internal with source_root tracking for proper filter matching.
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
    // check filter for top-level source (files, directories, and symlinks)
    if let Some(ref filter) = settings.filter {
        let src_name = src.file_name().map(std::path::Path::new);
        if let Some(name) = src_name {
            let src_metadata = crate::walk::run_metadata_probed(
                congestion::Side::Source,
                tokio::fs::symlink_metadata(src),
            )
            .await
            .with_context(|| format!("failed reading metadata from {:?}", &src))
            .map_err(|err| Error::new(err, Default::default()))?;
            let is_dir = src_metadata.is_dir();
            let result = filter.should_include_root_item(name, is_dir);
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
    }
    link_internal(
        prog_track, cwd, src, dst, src, update, settings, is_fresh, None,
    )
    .await
}
#[instrument(skip(prog_track, settings, open_file_guard))]
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn link_internal(
    prog_track: &'static progress::Progress,
    cwd: &std::path::Path,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &Settings,
    mut is_fresh: bool,
    open_file_guard: Option<throttle::OpenFileGuard>,
) -> Result<Summary, Error> {
    let _prog_guard = prog_track.ops.guard();
    tracing::debug!("reading source metadata");
    let src_metadata = crate::walk::run_metadata_probed(
        congestion::Side::Source,
        tokio::fs::symlink_metadata(src),
    )
    .await
    .with_context(|| format!("failed reading metadata from {:?}", &src))
    .map_err(|err| Error::new(err, Default::default()))?;
    let update_metadata_opt = match update {
        Some(update) => {
            tracing::debug!("reading 'update' metadata");
            let update_metadata_res = crate::walk::run_metadata_probed(
                congestion::Side::Source,
                tokio::fs::symlink_metadata(update),
            )
            .await;
            match update_metadata_res {
                Ok(update_metadata) => Some(update_metadata),
                Err(error) => {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        if settings.update_exclusive {
                            // the path is missing from update, we're done
                            return Ok(Default::default());
                        }
                        None
                    } else {
                        return Err(Error::new(
                            anyhow!("failed reading metadata from {:?}", &update),
                            Default::default(),
                        ));
                    }
                }
            }
        }
        None => None,
    };
    if let Some(update_metadata) = update_metadata_opt.as_ref() {
        let update = update.as_ref().unwrap();
        if !copy::is_file_type_same(&src_metadata, update_metadata) {
            // file type changed, just copy the updated one
            tracing::debug!(
                "link: file type of {:?} ({:?}) and {:?} ({:?}) differs - copying from update",
                src,
                src_metadata.file_type(),
                update,
                update_metadata.file_type()
            );
            // release any caller-supplied open-files permit before delegating
            // to copy::copy. The permit was acquired for the src entry's file
            // type at the spawn site, but here `update` has a *different* file
            // type (we just checked `!is_file_type_same`), so the permit is
            // mismatched. More importantly, copy::copy → copy_internal will
            // acquire its own open-files permit for any file it copies; if we
            // were still holding one here, a saturated pool would deadlock the
            // inner acquire.
            drop(open_file_guard);
            let copy_summary = copy::copy(
                prog_track,
                update,
                dst,
                &settings.copy_settings,
                &settings.preserve,
                is_fresh,
            )
            .await
            .map_err(|err| {
                let copy_summary = err.summary;
                let link_summary = Summary {
                    copy_summary,
                    ..Default::default()
                };
                Error::new(err.source, link_summary)
            })?;
            return Ok(Summary {
                copy_summary,
                ..Default::default()
            });
        }
        if update_metadata.is_file() {
            // check if the file is unchanged and if so hard-link, otherwise copy from the updated one
            if filecmp::metadata_equal(&settings.update_compare, &src_metadata, update_metadata) {
                tracing::debug!("no change, hard link 'src'");
                return hard_link_helper(prog_track, src, &src_metadata, dst, settings).await;
            }
            tracing::debug!(
                "link: {:?} metadata has changed, copying from {:?}",
                src,
                update
            );
            // use the caller's pre-acquired permit (the spawn loop pre-acquires
            // for regular-file entries so this is the common path); fall back to
            // acquiring a new one for callers that don't pre-acquire (top-level
            // `link` and the file-type-changed path above).
            let _guard = match open_file_guard {
                Some(g) => g,
                None => throttle::open_file_permit().await,
            };
            return Ok(Summary {
                copy_summary: copy::copy_file(
                    prog_track,
                    update,
                    dst,
                    update_metadata,
                    &settings.copy_settings,
                    &settings.preserve,
                    is_fresh,
                )
                .await
                .map_err(|err| {
                    let copy_summary = err.summary;
                    let link_summary = Summary {
                        copy_summary,
                        ..Default::default()
                    };
                    Error::new(err.source, link_summary)
                })?,
                ..Default::default()
            });
        }
        if update_metadata.is_symlink() {
            tracing::debug!("'update' is a symlink so just symlink that");
            // use "copy" function to handle the overwrite logic
            let copy_summary = copy::copy(
                prog_track,
                update,
                dst,
                &settings.copy_settings,
                &settings.preserve,
                is_fresh,
            )
            .await
            .map_err(|err| {
                let copy_summary = err.summary;
                let link_summary = Summary {
                    copy_summary,
                    ..Default::default()
                };
                Error::new(err.source, link_summary)
            })?;
            return Ok(Summary {
                copy_summary,
                ..Default::default()
            });
        }
    } else {
        // update hasn't been specified, if this is a file just hard-link the source or symlink if it's a symlink
        tracing::debug!("no 'update' specified");
        if src_metadata.is_file() {
            // handle dry-run mode for top-level files
            if settings.dry_run.is_some() {
                crate::dry_run::report_action("link", src, Some(dst), "file");
                return Ok(Summary {
                    hard_links_created: 1,
                    ..Default::default()
                });
            }
            return hard_link_helper(prog_track, src, &src_metadata, dst, settings).await;
        }
        if src_metadata.is_symlink() {
            tracing::debug!("'src' is a symlink so just symlink that");
            // use "copy" function to handle the overwrite logic
            let copy_summary = copy::copy(
                prog_track,
                src,
                dst,
                &settings.copy_settings,
                &settings.preserve,
                is_fresh,
            )
            .await
            .map_err(|err| {
                let copy_summary = err.summary;
                let link_summary = Summary {
                    copy_summary,
                    ..Default::default()
                };
                Error::new(err.source, link_summary)
            })?;
            return Ok(Summary {
                copy_summary,
                ..Default::default()
            });
        }
    }
    if !src_metadata.is_dir() {
        if settings.copy_settings.skip_specials {
            tracing::debug!(
                "skipping special file {:?} (type: {:?})",
                src,
                src_metadata.file_type()
            );
            if let Some(mode) = settings.dry_run {
                match mode {
                    crate::config::DryRunMode::Brief => {}
                    crate::config::DryRunMode::All => println!("skip special {:?}", src),
                    crate::config::DryRunMode::Explain => {
                        println!(
                            "skip special {:?} (unsupported file type: {:?})",
                            src,
                            src_metadata.file_type()
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
                src,
                dst,
                src_metadata.file_type()
            ),
            Default::default(),
        ));
    }
    assert!(update_metadata_opt.is_none() || update_metadata_opt.as_ref().unwrap().is_dir());
    tracing::debug!("process contents of 'src' directory");
    let mut src_entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {src:?} for reading"))
        .map_err(|err| Error::new(err, Default::default()))?;
    // handle dry-run mode for directories at the top level
    if settings.dry_run.is_some() {
        crate::dry_run::report_action("link", src, Some(dst), "dir");
        // still need to recurse to show contents
    }
    let copy_summary = if settings.dry_run.is_some() {
        // skip actual directory creation in dry-run mode
        CopySummary {
            directories_created: 1,
            ..Default::default()
        }
    } else if let Err(error) =
        crate::walk::run_metadata_probed(congestion::Side::Destination, tokio::fs::create_dir(dst))
            .await
    {
        assert!(!is_fresh, "unexpected error creating directory: {:?}", &dst);
        if settings.copy_settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists {
            // check if the destination is a directory - if so, leave it
            //
            // N.B. the permissions may prevent us from writing to it but the alternative is to open up the directory
            // while we're writing to it which isn't safe
            let dst_metadata = crate::walk::run_metadata_probed(
                congestion::Side::Destination,
                tokio::fs::metadata(dst),
            )
            .await
            .with_context(|| format!("failed reading metadata from {:?}", &dst))
            .map_err(|err| Error::new(err, Default::default()))?;
            if dst_metadata.is_dir() {
                tracing::debug!("'dst' is a directory, leaving it as is");
                CopySummary {
                    directories_unchanged: 1,
                    ..Default::default()
                }
            } else {
                tracing::info!("'dst' is not a directory, removing and creating a new one");
                let mut copy_summary = CopySummary::default();
                let rm_summary = rm::rm(
                    prog_track,
                    dst,
                    &rm::Settings {
                        fail_early: settings.copy_settings.fail_early,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await
                .map_err(|err| {
                    let rm_summary = err.summary;
                    copy_summary.rm_summary = rm_summary;
                    Error::new(
                        err.source,
                        Summary {
                            copy_summary,
                            ..Default::default()
                        },
                    )
                })?;
                crate::walk::run_metadata_probed(
                    congestion::Side::Destination,
                    tokio::fs::create_dir(dst),
                )
                .await
                .with_context(|| format!("cannot create directory {dst:?}"))
                .map_err(|err| {
                    copy_summary.rm_summary = rm_summary;
                    Error::new(
                        err,
                        Summary {
                            copy_summary,
                            ..Default::default()
                        },
                    )
                })?;
                // anything copied into dst may assume they don't need to check for conflicts
                is_fresh = true;
                CopySummary {
                    rm_summary,
                    directories_created: 1,
                    ..Default::default()
                }
            }
        } else {
            return Err(error)
                .with_context(|| format!("cannot create directory {dst:?}"))
                .map_err(|err| Error::new(err, Default::default()))?;
        }
    } else {
        // new directory created, anything copied into dst may assume they don't need to check for conflicts
        is_fresh = true;
        CopySummary {
            directories_created: 1,
            ..Default::default()
        }
    };
    // track whether we created this directory (vs it already existing)
    // this is used later to decide if we should clean up an empty directory
    let we_created_this_dir = copy_summary.directories_created == 1;
    let mut link_summary = Summary {
        copy_summary,
        ..Default::default()
    };
    let mut join_set = tokio::task::JoinSet::new();
    let errors = crate::error_collector::ErrorCollector::default();
    // create a set of all the files we already processed
    let mut processed_files = std::collections::HashSet::new();
    // iterate through src entries and recursively call "link" on each one
    loop {
        let Some((src_entry, entry_file_type)) =
            crate::walk::next_entry_probed(&mut src_entries, congestion::Side::Source, || {
                format!("failed traversing directory {:?}", &src)
            })
            .await
            .map_err(|err| Error::new(err, link_summary))?
        else {
            break;
        };
        let cwd_path = cwd.to_owned();
        let entry_path = src_entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let entry_kind = EntryKind::from_file_type(entry_file_type.as_ref());
        let entry_is_dir = entry_kind == EntryKind::Dir;
        let entry_is_symlink = entry_kind == EntryKind::Symlink;
        // compute relative path from source_root for filter matching
        let relative_path = entry_path.strip_prefix(source_root).unwrap_or(&entry_path);
        // apply filter if configured
        if let Some(skip_result) =
            walk::should_skip_entry(&settings.filter, relative_path, entry_is_dir)
        {
            if let Some(mode) = settings.dry_run {
                crate::dry_run::report_skip(&entry_path, &skip_result, mode, entry_kind.label());
            }
            tracing::debug!("skipping {:?} due to filter", &entry_path);
            link_summary = link_summary + skipped_summary_for(entry_kind);
            entry_kind.inc_skipped(prog_track);
            continue;
        }
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
                            &entry_path,
                            entry_file_type.unwrap()
                        );
                    }
                }
            }
            link_summary.copy_summary.specials_skipped += 1;
            prog_track.specials_skipped.inc();
            continue;
        }
        processed_files.insert(entry_name.to_owned());
        let dst_path = dst.join(entry_name);
        let update_path = update.as_ref().map(|s| s.join(entry_name));
        // handle dry-run mode for link operations
        if let Some(_mode) = settings.dry_run {
            crate::dry_run::report_action("link", &entry_path, Some(&dst_path), entry_kind.label());
            // for directories in dry-run, still need to recurse to show all entries
            if entry_is_dir {
                let settings = settings.clone();
                let source_root = source_root.to_owned();
                let do_link = || async move {
                    link_internal(
                        prog_track,
                        &cwd_path,
                        &entry_path,
                        &dst_path,
                        &source_root,
                        &update_path,
                        &settings,
                        true,
                        None,
                    )
                    .await
                };
                join_set.spawn(do_link());
            } else if entry_is_symlink {
                // for symlinks in dry-run, count as symlink (in copy_summary)
                link_summary.copy_summary.symlinks_created += 1;
            } else {
                // for files in dry-run, count the "would be created" hard link
                link_summary.hard_links_created += 1;
            }
            continue;
        }
        let settings = settings.clone();
        let source_root = source_root.to_owned();
        // for regular-file entries, acquire the open file permit BEFORE spawning so
        // we don't create unbounded tasks. mirrors the pattern in copy.rs.
        // directories must NOT pre-acquire because they recurse and would deadlock
        // against a saturated semaphore. symlinks aren't pre-acquired because they
        // can pass through to copy::copy which handles permits internally.
        let entry_is_regular_file = entry_file_type.as_ref().is_some_and(|ft| ft.is_file());
        let open_file_guard = if entry_is_regular_file {
            Some(throttle::open_file_permit().await)
        } else {
            None
        };
        let do_link = || async move {
            link_internal(
                prog_track,
                &cwd_path,
                &entry_path,
                &dst_path,
                &source_root,
                &update_path,
                &settings,
                is_fresh,
                open_file_guard,
            )
            .await
        };
        join_set.spawn(do_link());
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(src_entries);
    // only process update if the path was provided and the directory is present
    if update_metadata_opt.is_some() {
        let update = update.as_ref().unwrap();
        tracing::debug!("process contents of 'update' directory");
        let mut update_entries = tokio::fs::read_dir(update)
            .await
            .with_context(|| format!("cannot open directory {:?} for reading", &update))
            .map_err(|err| Error::new(err, link_summary))?;
        // Iterate through update entries and for each one that's not present in src call "copy".
        //
        // We deliberately do NOT pre-acquire any permit here. Two cycles rule out the
        // straightforward options:
        //   * `open_file_permit`: copy::copy → copy_internal re-acquires open-files for
        //     each file; a saturated pool would deadlock the inner acquire if we held one
        //     across the call.
        //   * `pending_meta_permit`: with --overwrite, copy::copy → copy_file → rm::rm
        //     drains pending_meta for child entries (rm.rs spawn loop). N tasks here each
        //     holding a pending_meta permit would deadlock waiting on each other's inner rm.
        //
        // The spawn count at this site is naturally bounded by the number of update-only
        // entries (user input — typically modest) and per-task tokio overhead is small.
        // Each spawned task's actual work is throttled by copy::copy's own internal
        // open-files backpressure inside copy_internal's spawn loop.
        loop {
            let Some((update_entry, _entry_file_type)) = crate::walk::next_entry_probed(
                &mut update_entries,
                congestion::Side::Source,
                || format!("failed traversing directory {:?}", &update),
            )
            .await
            .map_err(|err| Error::new(err, link_summary))?
            else {
                break;
            };
            let entry_path = update_entry.path();
            let entry_name = entry_path.file_name().unwrap();
            if processed_files.contains(entry_name) {
                // we already must have considered this file, skip it
                continue;
            }
            tracing::debug!("found a new entry in the 'update' directory");
            let dst_path = dst.join(entry_name);
            let update_path = update.join(entry_name);
            let settings = settings.clone();
            let do_copy = || async move {
                let copy_summary = copy::copy(
                    prog_track,
                    &update_path,
                    &dst_path,
                    &settings.copy_settings,
                    &settings.preserve,
                    is_fresh,
                )
                .await
                .map_err(|err| {
                    link_summary.copy_summary = link_summary.copy_summary + err.summary;
                    Error::new(err.source, link_summary)
                })?;
                Ok(Summary {
                    copy_summary,
                    ..Default::default()
                })
            };
            join_set.spawn(do_copy());
        }
        // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
        // one thing we CAN do however is to drop it as soon as we're done with it
        drop(update_entries);
    }
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(result) => match result {
                Ok(summary) => link_summary = link_summary + summary,
                Err(error) => {
                    tracing::error!(
                        "link: {:?} {:?} -> {:?} failed with: {:#}",
                        src,
                        update,
                        dst,
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
    let relative_path = src.strip_prefix(source_root).unwrap_or(src);
    let is_root = src == source_root;
    match check_empty_dir_cleanup(
        settings.filter.as_ref(),
        we_created_this_dir,
        anything_linked,
        relative_path,
        is_root,
        settings.dry_run.is_some(),
    ) {
        EmptyDirAction::Keep => { /* proceed with metadata application */ }
        EmptyDirAction::DryRunSkip => {
            tracing::debug!(
                "dry-run: directory {:?} would not be created (nothing to link inside)",
                &dst
            );
            link_summary.copy_summary.directories_created = 0;
            return Ok(link_summary);
        }
        EmptyDirAction::Remove => {
            tracing::debug!(
                "directory {:?} has nothing to link inside, removing empty directory",
                &dst
            );
            match crate::walk::run_metadata_probed(
                congestion::Side::Destination,
                tokio::fs::remove_dir(dst),
            )
            .await
            {
                Ok(()) => {
                    link_summary.copy_summary.directories_created = 0;
                    return Ok(link_summary);
                }
                Err(err) => {
                    // removal failed (not empty, permission error, etc.) — keep directory
                    tracing::debug!(
                        "failed to remove empty directory {:?}: {:#}, keeping",
                        &dst,
                        &err
                    );
                    // fall through to apply metadata
                }
            }
        }
    }
    // apply directory metadata regardless of whether all children linked successfully.
    // the directory itself was created earlier in this function (we would have returned
    // early if create_dir failed), so we should preserve the source metadata.
    // skip metadata setting in dry-run mode since directory wasn't actually created
    tracing::debug!("set 'dst' directory metadata");
    let metadata_result = if settings.dry_run.is_some() {
        Ok(()) // skip metadata setting in dry-run mode
    } else {
        let preserve_metadata = if let Some(update_metadata) = update_metadata_opt.as_ref() {
            update_metadata
        } else {
            &src_metadata
        };
        preserve::set_dir_metadata(&settings.preserve, preserve_metadata, dst).await
    };
    if errors.has_errors() {
        // child failures take precedence - log metadata error if it also failed
        if let Err(metadata_err) = metadata_result {
            tracing::error!(
                "link: {:?} {:?} -> {:?} failed to set directory metadata: {:#}",
                src,
                update,
                dst,
                &metadata_err
            );
        }
        // unwrap is safe: has_errors() guarantees into_error() returns Some
        return Err(Error::new(errors.into_error().unwrap(), link_summary));
    }
    // no child failures, so metadata error is the primary error
    metadata_result.map_err(|err| Error::new(err, link_summary))?;
    Ok(link_summary)
}

#[cfg(test)]
mod link_tests {
    use crate::testutils;
    use std::os::unix::fs::PermissionsExt;
    use tracing_test::traced_test;

    use super::*;

    static PROGRESS: std::sync::LazyLock<progress::Progress> =
        std::sync::LazyLock::new(progress::Progress::new);

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
}
