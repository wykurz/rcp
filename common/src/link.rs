use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use std::os::linux::fs::MetadataExt as LinuxMetadataExt;
use tracing::instrument;

use crate::config::DryRunMode;
use crate::copy;
use crate::copy::{
    check_empty_dir_cleanup, EmptyDirAction, Settings as CopySettings, Summary as CopySummary,
};
use crate::filecmp;
use crate::filter::{FilterResult, FilterSettings};
use crate::preserve;
use crate::progress;
use crate::rm;

lazy_static! {
    static ref RLINK_PRESERVE_SETTINGS: preserve::Settings = preserve::preserve_all();
}

/// Error type for link operations that preserves operation summary even on failure.
///
/// # Logging Convention
/// When logging this error, use `{:#}` or `{:?}` format to preserve the error chain:
/// ```ignore
/// tracing::error!("operation failed: {:#}", &error); // ✅ Shows full chain
/// tracing::error!("operation failed: {:?}", &error); // ✅ Shows full chain
/// ```
/// The Display implementation also shows the full chain, but workspace linting enforces `{:#}`
/// for consistency.
#[derive(Debug, thiserror::Error)]
#[error("{source:#}")]
pub struct Error {
    #[source]
    pub source: anyhow::Error,
    pub summary: Summary,
}

impl Error {
    #[must_use]
    pub fn new(source: anyhow::Error, summary: Summary) -> Self {
        Error { source, summary }
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub copy_settings: CopySettings,
    pub update_compare: filecmp::MetadataCmpSettings,
    pub update_exclusive: bool,
    /// filter settings for include/exclude patterns
    pub filter: Option<crate::filter::FilterSettings>,
    /// dry-run mode for previewing operations
    pub dry_run: Option<crate::config::DryRunMode>,
}

/// Reports a dry-run action for link operations
fn report_dry_run_link(src: &std::path::Path, dst: &std::path::Path, entry_type: &str) {
    println!("would link {} {:?} -> {:?}", entry_type, src, dst);
}

/// Reports a skipped entry during dry-run
fn report_dry_run_skip(
    path: &std::path::Path,
    result: &FilterResult,
    mode: DryRunMode,
    entry_type: &str,
) {
    match mode {
        DryRunMode::Brief => { /* brief mode doesn't show skipped files */ }
        DryRunMode::All => {
            println!("skip {} {:?}", entry_type, path);
        }
        DryRunMode::Explain => match result {
            FilterResult::ExcludedByDefault => {
                println!(
                    "skip {} {:?} (no include pattern matched)",
                    entry_type, path
                );
            }
            FilterResult::ExcludedByPattern(pattern) => {
                println!("skip {} {:?} (excluded by '{}')", entry_type, path, pattern);
            }
            FilterResult::Included => { /* shouldn't happen */ }
        },
    }
}

/// Check if a path should be filtered out
fn should_skip_entry(
    filter: &Option<FilterSettings>,
    relative_path: &std::path::Path,
    is_dir: bool,
) -> Option<FilterResult> {
    if let Some(ref f) = filter {
        let result = f.should_include(relative_path, is_dir);
        match result {
            FilterResult::Included => None,
            _ => Some(result),
        }
    } else {
        None
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
            "{}hard-links created: {}\nhard links unchanged: {}\n",
            &self.copy_summary, self.hard_links_created, self.hard_links_unchanged
        )
    }
}

fn is_hard_link(md1: &std::fs::Metadata, md2: &std::fs::Metadata) -> bool {
    copy::is_file_type_same(md1, md2)
        && md2.st_dev() == md1.st_dev()
        && md2.st_ino() == md1.st_ino()
}

#[instrument(skip(prog_track))]
async fn hard_link_helper(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    src_metadata: &std::fs::Metadata,
    dst: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    let mut link_summary = Summary::default();
    if let Err(error) = tokio::fs::hard_link(src, dst).await {
        if settings.copy_settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists {
            tracing::debug!("'dst' already exists, check if we need to update");
            let dst_metadata = tokio::fs::symlink_metadata(dst)
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
                },
            )
            .await
            .map_err(|err| {
                let rm_summary = err.summary;
                link_summary.copy_summary.rm_summary = rm_summary;
                Error::new(err.source, link_summary)
            })?;
            link_summary.copy_summary.rm_summary = rm_summary;
            tokio::fs::hard_link(src, dst)
                .await
                .with_context(|| format!("failed to hard link {:?} to {:?}", src, dst))
                .map_err(|err| Error::new(err, link_summary))?;
        }
    }
    prog_track.hard_links_created.inc();
    link_summary.hard_links_created = 1;
    Ok(link_summary)
}

/// Public entry point for link operations.
/// Internally delegates to link_internal with source_root tracking for proper filter matching.
#[instrument(skip(prog_track))]
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
            let src_metadata = tokio::fs::symlink_metadata(src)
                .await
                .with_context(|| format!("failed reading metadata from {:?}", &src))
                .map_err(|err| Error::new(err, Default::default()))?;
            let is_dir = src_metadata.is_dir();
            let result = filter.should_include_root_item(name, is_dir);
            match result {
                crate::filter::FilterResult::Included => {}
                result => {
                    if let Some(mode) = settings.dry_run {
                        let entry_type = if src_metadata.is_dir() {
                            "directory"
                        } else if src_metadata.file_type().is_symlink() {
                            "symlink"
                        } else {
                            "file"
                        };
                        report_dry_run_skip(src, &result, mode, entry_type);
                    }
                    // return summary with skipped count
                    let skipped_summary = if src_metadata.is_dir() {
                        Summary {
                            copy_summary: CopySummary {
                                directories_skipped: 1,
                                ..Default::default()
                            },
                            ..Default::default()
                        }
                    } else if src_metadata.file_type().is_symlink() {
                        Summary {
                            copy_summary: CopySummary {
                                symlinks_skipped: 1,
                                ..Default::default()
                            },
                            ..Default::default()
                        }
                    } else {
                        Summary {
                            copy_summary: CopySummary {
                                files_skipped: 1,
                                ..Default::default()
                            },
                            ..Default::default()
                        }
                    };
                    return Ok(skipped_summary);
                }
            }
        }
    }
    link_internal(prog_track, cwd, src, dst, src, update, settings, is_fresh).await
}
#[instrument(skip(prog_track))]
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
) -> Result<Summary, Error> {
    let _prog_guard = prog_track.ops.guard();
    tracing::debug!("reading source metadata");
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))
        .map_err(|err| Error::new(err, Default::default()))?;
    let update_metadata_opt = match update {
        Some(update) => {
            tracing::debug!("reading 'update' metadata");
            let update_metadata_res = tokio::fs::symlink_metadata(update).await;
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
            let copy_summary = copy::copy(
                prog_track,
                update,
                dst,
                &settings.copy_settings,
                &RLINK_PRESERVE_SETTINGS,
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
            return Ok(Summary {
                copy_summary: copy::copy_file(
                    prog_track,
                    update,
                    dst,
                    &settings.copy_settings,
                    &RLINK_PRESERVE_SETTINGS,
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
                &RLINK_PRESERVE_SETTINGS,
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
                report_dry_run_link(src, dst, "file");
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
                &RLINK_PRESERVE_SETTINGS,
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
        report_dry_run_link(src, dst, "dir");
        // still need to recurse to show contents
    }
    let copy_summary = if settings.dry_run.is_some() {
        // skip actual directory creation in dry-run mode
        CopySummary {
            directories_created: 1,
            ..Default::default()
        }
    } else if let Err(error) = tokio::fs::create_dir(dst).await {
        assert!(!is_fresh, "unexpected error creating directory: {:?}", &dst);
        if settings.copy_settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists {
            // check if the destination is a directory - if so, leave it
            //
            // N.B. the permissions may prevent us from writing to it but the alternative is to open up the directory
            // while we're writing to it which isn't safe
            let dst_metadata = tokio::fs::metadata(dst)
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
                tokio::fs::create_dir(dst)
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
    let mut all_children_succeeded = true;
    // create a set of all the files we already processed
    let mut processed_files = std::collections::HashSet::new();
    // iterate through src entries and recursively call "link" on each one
    while let Some(src_entry) = src_entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &src))
        .map_err(|err| Error::new(err, link_summary))?
    {
        // it's better to await the token here so that we throttle the syscalls generated by the
        // DirEntry call. the ops-throttle will never cause a deadlock (unlike max-open-files limit)
        // so it's safe to do here.
        throttle::get_ops_token().await;
        let cwd_path = cwd.to_owned();
        let entry_path = src_entry.path();
        let entry_name = entry_path.file_name().unwrap();
        // check entry type for filter matching and dry-run reporting
        let entry_file_type = src_entry.file_type().await.ok();
        let entry_is_dir = entry_file_type.map(|ft| ft.is_dir()).unwrap_or(false);
        let entry_is_symlink = entry_file_type.map(|ft| ft.is_symlink()).unwrap_or(false);
        // compute relative path from source_root for filter matching
        let relative_path = entry_path.strip_prefix(source_root).unwrap_or(&entry_path);
        // apply filter if configured
        if let Some(skip_result) = should_skip_entry(&settings.filter, relative_path, entry_is_dir)
        {
            if let Some(mode) = settings.dry_run {
                let entry_type = if entry_is_dir {
                    "dir"
                } else if entry_is_symlink {
                    "symlink"
                } else {
                    "file"
                };
                report_dry_run_skip(&entry_path, &skip_result, mode, entry_type);
            }
            tracing::debug!("skipping {:?} due to filter", &entry_path);
            // increment skipped counters
            if entry_is_dir {
                link_summary.copy_summary.directories_skipped += 1;
            } else if entry_is_symlink {
                link_summary.copy_summary.symlinks_skipped += 1;
            } else {
                link_summary.copy_summary.files_skipped += 1;
            }
            continue;
        }
        processed_files.insert(entry_name.to_owned());
        let dst_path = dst.join(entry_name);
        let update_path = update.as_ref().map(|s| s.join(entry_name));
        // handle dry-run mode for link operations
        if let Some(_mode) = settings.dry_run {
            let entry_type = if entry_is_dir {
                "dir"
            } else if entry_is_symlink {
                "symlink"
            } else {
                "file"
            };
            report_dry_run_link(&entry_path, &dst_path, entry_type);
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
        // iterate through update entries and for each one that's not present in src call "copy"
        while let Some(update_entry) = update_entries
            .next_entry()
            .await
            .with_context(|| format!("failed traversing directory {:?}", &update))
            .map_err(|err| Error::new(err, link_summary))?
        {
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
                    &RLINK_PRESERVE_SETTINGS,
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
                    if settings.copy_settings.fail_early {
                        return Err(error);
                    }
                    all_children_succeeded = false;
                }
            },
            Err(error) => {
                if settings.copy_settings.fail_early {
                    return Err(Error::new(error.into(), link_summary));
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
            match tokio::fs::remove_dir(dst).await {
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
        preserve::set_dir_metadata(&RLINK_PRESERVE_SETTINGS, preserve_metadata, dst).await
    };
    if !all_children_succeeded {
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
        return Err(Error::new(
            anyhow!("link: {:?} {:?} -> {:?} failed!", src, update, dst),
            link_summary,
        ))?;
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

    lazy_static! {
        static ref PROGRESS: progress::Progress = progress::Progress::new();
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
                chunk_size: 0,
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
    async fn test_link_destination_permission_error_includes_root_cause(
    ) -> Result<(), anyhow::Error> {
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: Some(crate::config::DryRunMode::Explain),
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: Some(filter),
                    dry_run: None,
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
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
                        chunk_size: 0,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                    },
                    update_compare: Default::default(),
                    update_exclusive: false,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
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
}
