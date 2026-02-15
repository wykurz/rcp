use std::os::unix::fs::MetadataExt;

use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use throttle::get_file_iops_tokens;
use tracing::instrument;

use crate::config::DryRunMode;
use crate::filecmp;
use crate::filter::{FilterResult, FilterSettings};
use crate::preserve;
use crate::progress;
use crate::rm;
use crate::rm::{Settings as RmSettings, Summary as RmSummary};

/// Error type for copy operations that preserves operation summary even on failure.
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
    pub dereference: bool,
    pub fail_early: bool,
    pub overwrite: bool,
    pub overwrite_compare: filecmp::MetadataCmpSettings,
    pub chunk_size: u64,
    /// Buffer size for remote copy file transfer operations in bytes.
    ///
    /// This is only used for remote copy operations and controls the buffer size
    /// when copying data between files and network streams. The actual buffer is
    /// capped to the file size to avoid over-allocation for small files.
    pub remote_copy_buffer_size: usize,
    /// filter settings for include/exclude patterns
    pub filter: Option<crate::filter::FilterSettings>,
    /// dry-run mode for previewing operations
    pub dry_run: Option<crate::config::DryRunMode>,
}

/// Reports a dry-run action for copy operations
fn report_dry_run_copy(src: &std::path::Path, dst: &std::path::Path, entry_type: &str) {
    println!("would copy {} {:?} -> {:?}", entry_type, src, dst);
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

/// Result of checking if an empty directory should be cleaned up.
/// Used when filtering is active and a directory we created ended up empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyDirAction {
    /// keep the directory (directly matched or no filter active)
    Keep,
    /// directory was only traversed, remove it
    Remove,
    /// dry-run mode, don't count this directory in summary
    DryRunSkip,
}

/// Determine what to do with an empty directory when filtering is active.
///
/// This is called when we created a directory but nothing was copied into it
/// (no files, symlinks, or child directories). The decision depends on whether
/// the directory itself was directly matched by an include pattern, or if we
/// only entered it to look for potential matches inside.
///
/// # Arguments
/// * `filter` - the active filter settings (None means no filtering)
/// * `we_created_dir` - whether we created this directory (vs it already existed)
/// * `anything_copied` - whether any content was copied into this directory
/// * `relative_path` - path relative to the source root (for pattern matching)
/// * `is_root` - whether this is the root (user-specified) source directory
/// * `is_dry_run` - whether we're in dry-run mode
pub fn check_empty_dir_cleanup(
    filter: Option<&FilterSettings>,
    we_created_dir: bool,
    anything_copied: bool,
    relative_path: &std::path::Path,
    is_root: bool,
    is_dry_run: bool,
) -> EmptyDirAction {
    // if no filter active or something was copied, keep the directory
    if filter.is_none() || anything_copied {
        return EmptyDirAction::Keep;
    }
    // if we didn't create this directory, don't remove it
    if !we_created_dir {
        return EmptyDirAction::Keep;
    }
    // never remove the root directory — it's the user-specified source
    if is_root {
        return EmptyDirAction::Keep;
    }
    // filter is guaranteed to be Some here (checked above)
    let f = filter.unwrap();
    // check if directory directly matches include pattern
    if f.directly_matches_include(relative_path, true) {
        return EmptyDirAction::Keep;
    }
    // directory was only traversed for potential matches
    if is_dry_run {
        EmptyDirAction::DryRunSkip
    } else {
        EmptyDirAction::Remove
    }
}

#[instrument]
pub fn is_file_type_same(md1: &std::fs::Metadata, md2: &std::fs::Metadata) -> bool {
    let ft1 = md1.file_type();
    let ft2 = md2.file_type();
    ft1.is_dir() == ft2.is_dir()
        && ft1.is_file() == ft2.is_file()
        && ft1.is_symlink() == ft2.is_symlink()
}

#[instrument(skip(prog_track))]
pub async fn copy_file(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
    preserve: &preserve::Settings,
    is_fresh: bool,
) -> Result<Summary, Error> {
    // handle dry-run mode for files - still read metadata to report accurate size
    if settings.dry_run.is_some() {
        let src_metadata = tokio::fs::symlink_metadata(src)
            .await
            .with_context(|| format!("failed reading metadata from {:?}", &src))
            .map_err(|err| Error::new(err, Default::default()))?;
        report_dry_run_copy(src, dst, "file");
        return Ok(Summary {
            files_copied: 1,
            bytes_copied: src_metadata.len(),
            ..Default::default()
        });
    }
    let _open_file_guard = throttle::open_file_permit().await;
    tracing::debug!("opening 'src' for reading and 'dst' for writing");
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))
        .map_err(|err| Error::new(err, Default::default()))?;
    get_file_iops_tokens(settings.chunk_size, src_metadata.size()).await;
    let mut rm_summary = RmSummary::default();
    if !is_fresh && dst.exists() {
        if settings.overwrite {
            tracing::debug!("file exists, check if it's identical");
            let dst_metadata = tokio::fs::symlink_metadata(dst)
                .await
                .with_context(|| format!("failed reading metadata from {:?}", &dst))
                .map_err(|err| Error::new(err, Default::default()))?;
            if is_file_type_same(&src_metadata, &dst_metadata)
                && filecmp::metadata_equal(
                    &settings.overwrite_compare,
                    &src_metadata,
                    &dst_metadata,
                )
            {
                tracing::debug!("file is identical, skipping");
                prog_track.files_unchanged.inc();
                return Ok(Summary {
                    files_unchanged: 1,
                    ..Default::default()
                });
            }
            tracing::info!("file is different, removing existing file");
            // note tokio::fs::overwrite cannot handle this path being e.g. a directory
            rm_summary = rm::rm(
                prog_track,
                dst,
                &RmSettings {
                    fail_early: settings.fail_early,
                    filter: None,
                    dry_run: None,
                },
            )
            .await
            .map_err(|err| {
                let rm_summary = err.summary;
                let copy_summary = Summary {
                    rm_summary,
                    ..Default::default()
                };
                Error::new(err.source, copy_summary)
            })?;
        } else {
            return Err(Error::new(
                anyhow!(
                    "destination {:?} already exists, did you intend to specify --overwrite?",
                    dst
                ),
                Default::default(),
            ));
        }
    }
    tracing::debug!("copying data");
    let mut copy_summary = Summary {
        rm_summary,
        ..Default::default()
    };
    tokio::fs::copy(src, dst)
        .await
        .with_context(|| format!("failed copying {:?} to {:?}", &src, &dst))
        .map_err(|err| Error::new(err, copy_summary))?;
    prog_track.files_copied.inc();
    prog_track.bytes_copied.add(src_metadata.len());
    tracing::debug!("setting permissions");
    preserve::set_file_metadata(preserve, &src_metadata, dst)
        .await
        .map_err(|err| Error::new(err, copy_summary))?;
    // we mark files as "copied" only after all metadata is set as well
    copy_summary.bytes_copied += src_metadata.len();
    copy_summary.files_copied += 1;
    Ok(copy_summary)
}

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    pub bytes_copied: u64,
    pub files_copied: usize,
    pub symlinks_created: usize,
    pub directories_created: usize,
    pub files_unchanged: usize,
    pub symlinks_unchanged: usize,
    pub directories_unchanged: usize,
    pub files_skipped: usize,
    pub symlinks_skipped: usize,
    pub directories_skipped: usize,
    pub rm_summary: RmSummary,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            bytes_copied: self.bytes_copied + other.bytes_copied,
            files_copied: self.files_copied + other.files_copied,
            symlinks_created: self.symlinks_created + other.symlinks_created,
            directories_created: self.directories_created + other.directories_created,
            files_unchanged: self.files_unchanged + other.files_unchanged,
            symlinks_unchanged: self.symlinks_unchanged + other.symlinks_unchanged,
            directories_unchanged: self.directories_unchanged + other.directories_unchanged,
            files_skipped: self.files_skipped + other.files_skipped,
            symlinks_skipped: self.symlinks_skipped + other.symlinks_skipped,
            directories_skipped: self.directories_skipped + other.directories_skipped,
            rm_summary: self.rm_summary + other.rm_summary,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "bytes copied: {}\n\
            files copied: {}\n\
            symlinks created: {}\n\
            directories created: {}\n\
            files unchanged: {}\n\
            symlinks unchanged: {}\n\
            directories unchanged: {}\n\
            files skipped: {}\n\
            symlinks skipped: {}\n\
            directories skipped: {}\n\
            {}",
            bytesize::ByteSize(self.bytes_copied),
            self.files_copied,
            self.symlinks_created,
            self.directories_created,
            self.files_unchanged,
            self.symlinks_unchanged,
            self.directories_unchanged,
            self.files_skipped,
            self.symlinks_skipped,
            self.directories_skipped,
            &self.rm_summary,
        )
    }
}

/// Public entry point for copy operations.
/// Internally delegates to copy_internal with source_root tracking for proper filter matching.
#[instrument(skip(prog_track))]
pub async fn copy(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
    preserve: &preserve::Settings,
    is_fresh: bool,
) -> Result<Summary, Error> {
    // check filter for top-level source (files, directories, and symlinks)
    if let Some(ref filter) = settings.filter {
        let src_name = src.file_name().map(std::path::Path::new);
        if let Some(name) = src_name {
            let src_metadata = tokio::fs::symlink_metadata(src)
                .await
                .with_context(|| format!("failed reading metadata from src: {:?}", &src))
                .map_err(|err| Error::new(err, Default::default()))?;
            let is_dir = src_metadata.is_dir();
            let result = filter.should_include_root_item(name, is_dir);
            match result {
                crate::filter::FilterResult::Included => {}
                result => {
                    if let Some(mode) = settings.dry_run {
                        let entry_type = if src_metadata.is_dir() {
                            "directory"
                        } else if src_metadata.is_symlink() {
                            "symlink"
                        } else {
                            "file"
                        };
                        report_dry_run_skip(src, &result, mode, entry_type);
                    }
                    // return summary with skipped count
                    let skipped_summary = if src_metadata.is_dir() {
                        Summary {
                            directories_skipped: 1,
                            ..Default::default()
                        }
                    } else if src_metadata.is_symlink() {
                        Summary {
                            symlinks_skipped: 1,
                            ..Default::default()
                        }
                    } else {
                        Summary {
                            files_skipped: 1,
                            ..Default::default()
                        }
                    };
                    return Ok(skipped_summary);
                }
            }
        }
    }
    copy_internal(prog_track, src, dst, src, settings, preserve, is_fresh).await
}
#[instrument(skip(prog_track))]
#[async_recursion]
async fn copy_internal(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    settings: &Settings,
    preserve: &preserve::Settings,
    mut is_fresh: bool,
) -> Result<Summary, Error> {
    let _ops_guard = prog_track.ops.guard();
    tracing::debug!("reading source metadata");
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))
        .map_err(|err| Error::new(err, Default::default()))?;
    if settings.dereference && src_metadata.is_symlink() {
        let link = tokio::fs::canonicalize(&src)
            .await
            .with_context(|| format!("failed reading src symlink {:?}", &src))
            .map_err(|err| Error::new(err, Default::default()))?;
        return copy(prog_track, &link, dst, settings, preserve, is_fresh).await;
    }
    if src_metadata.is_file() {
        return copy_file(prog_track, src, dst, settings, preserve, is_fresh).await;
    }
    if src_metadata.is_symlink() {
        // handle dry-run mode for symlinks
        if settings.dry_run.is_some() {
            report_dry_run_copy(src, dst, "symlink");
            return Ok(Summary {
                symlinks_created: 1,
                ..Default::default()
            });
        }
        let mut rm_summary = RmSummary::default();
        let link = tokio::fs::read_link(src)
            .await
            .with_context(|| format!("failed reading symlink {:?}", &src))
            .map_err(|err| Error::new(err, Default::default()))?;
        // try creating a symlink, if dst path exists and overwrite is set - remove and try again
        if let Err(error) = tokio::fs::symlink(&link, dst).await {
            if settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists {
                let dst_metadata = tokio::fs::symlink_metadata(dst)
                    .await
                    .with_context(|| format!("failed reading metadata from dst: {:?}", &dst))
                    .map_err(|err| Error::new(err, Default::default()))?;
                if is_file_type_same(&src_metadata, &dst_metadata) {
                    let dst_link = tokio::fs::read_link(dst)
                        .await
                        .with_context(|| format!("failed reading dst symlink: {:?}", &dst))
                        .map_err(|err| Error::new(err, Default::default()))?;
                    if link == dst_link {
                        tracing::debug!(
                            "'dst' is a symlink and points to the same location as 'src'"
                        );
                        if preserve.symlink.any() {
                            // do we need to update the metadata for this symlink?
                            let dst_metadata = tokio::fs::symlink_metadata(dst)
                                .await
                                .with_context(|| {
                                    format!("failed reading metadata from dst: {:?}", &dst)
                                })
                                .map_err(|err| Error::new(err, Default::default()))?;
                            if !filecmp::metadata_equal(
                                &settings.overwrite_compare,
                                &src_metadata,
                                &dst_metadata,
                            ) {
                                tracing::debug!("'dst' metadata is different, updating");
                                preserve::set_symlink_metadata(preserve, &src_metadata, dst)
                                    .await
                                    .map_err(|err| Error::new(err, Default::default()))?;
                                prog_track.symlinks_removed.inc();
                                prog_track.symlinks_created.inc();
                                return Ok(Summary {
                                    rm_summary: RmSummary {
                                        symlinks_removed: 1,
                                        ..Default::default()
                                    },
                                    symlinks_created: 1,
                                    ..Default::default()
                                });
                            }
                        }
                        tracing::debug!("symlink already exists, skipping");
                        prog_track.symlinks_unchanged.inc();
                        return Ok(Summary {
                            symlinks_unchanged: 1,
                            ..Default::default()
                        });
                    }
                    tracing::debug!("'dst' is a symlink but points to a different path, updating");
                } else {
                    tracing::info!("'dst' is not a symlink, updating");
                }
                rm_summary = rm::rm(
                    prog_track,
                    dst,
                    &RmSettings {
                        fail_early: settings.fail_early,
                        filter: None,
                        dry_run: None,
                    },
                )
                .await
                .map_err(|err| {
                    let rm_summary = err.summary;
                    let copy_summary = Summary {
                        rm_summary,
                        ..Default::default()
                    };
                    Error::new(err.source, copy_summary)
                })?;
                tokio::fs::symlink(&link, dst)
                    .await
                    .with_context(|| format!("failed creating symlink {:?}", &dst))
                    .map_err(|err| {
                        let copy_summary = Summary {
                            rm_summary,
                            ..Default::default()
                        };
                        Error::new(err, copy_summary)
                    })?;
            } else {
                return Err(Error::new(
                    anyhow!("failed creating symlink {:?}", &dst),
                    Default::default(),
                ));
            }
        }
        preserve::set_symlink_metadata(preserve, &src_metadata, dst)
            .await
            .map_err(|err| {
                let copy_summary = Summary {
                    rm_summary,
                    ..Default::default()
                };
                Error::new(err, copy_summary)
            })?;
        prog_track.symlinks_created.inc();
        return Ok(Summary {
            rm_summary,
            symlinks_created: 1,
            ..Default::default()
        });
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
    // handle dry-run mode for directories at the top level
    if settings.dry_run.is_some() {
        report_dry_run_copy(src, dst, "dir");
        // still need to recurse to show contents
    }
    tracing::debug!("process contents of 'src' directory");
    let mut entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {src:?} for reading"))
        .map_err(|err| Error::new(err, Default::default()))?;
    // in dry-run mode, skip directory creation but still traverse contents
    let mut copy_summary = if settings.dry_run.is_some() {
        Summary {
            directories_created: 1, // report as would be created
            ..Default::default()
        }
    } else if let Err(error) = tokio::fs::create_dir(dst).await {
        assert!(
            !is_fresh,
            "unexpected error creating directory: {dst:?}: {error}"
        );
        if settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists {
            // check if the destination is a directory - if so, leave it
            //
            // N.B. the permissions may prevent us from writing to it but the alternative is to open up the directory
            // while we're writing to it which isn't safe
            let dst_metadata = tokio::fs::metadata(dst)
                .await
                .with_context(|| format!("failed reading metadata from dst: {:?}", &dst))
                .map_err(|err| Error::new(err, Default::default()))?;
            if dst_metadata.is_dir() {
                tracing::debug!("'dst' is a directory, leaving it as is");
                prog_track.directories_unchanged.inc();
                Summary {
                    directories_unchanged: 1,
                    ..Default::default()
                }
            } else {
                tracing::info!("'dst' is not a directory, removing and creating a new one");
                let rm_summary = rm::rm(
                    prog_track,
                    dst,
                    &RmSettings {
                        fail_early: settings.fail_early,
                        filter: None,
                        dry_run: None,
                    },
                )
                .await
                .map_err(|err| {
                    let rm_summary = err.summary;
                    let copy_summary = Summary {
                        rm_summary,
                        ..Default::default()
                    };
                    Error::new(err.source, copy_summary)
                })?;
                tokio::fs::create_dir(dst)
                    .await
                    .with_context(|| format!("cannot create directory {dst:?}"))
                    .map_err(|err| {
                        let copy_summary = Summary {
                            rm_summary,
                            ..Default::default()
                        };
                        Error::new(err, copy_summary)
                    })?;
                // anything copied into dst may assume they don't need to check for conflicts
                is_fresh = true;
                prog_track.directories_created.inc();
                Summary {
                    rm_summary,
                    directories_created: 1,
                    ..Default::default()
                }
            }
        } else {
            let error = Err::<(), std::io::Error>(error)
                .with_context(|| format!("cannot create directory {:?}", dst))
                .unwrap_err();
            tracing::error!("{:#}", &error);
            return Err(Error::new(error, Default::default()));
        }
    } else {
        // new directory created, anything copied into dst may assume they don't need to check for conflicts
        is_fresh = true;
        prog_track.directories_created.inc();
        Summary {
            directories_created: 1,
            ..Default::default()
        }
    };
    // track whether we created this directory (vs it already existing)
    // this is used later to decide if we should clean up an empty directory
    let we_created_this_dir = copy_summary.directories_created == 1;
    let mut join_set = tokio::task::JoinSet::new();
    let mut all_children_succeeded = true;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing src directory {:?}", &src))
        .map_err(|err| Error::new(err, copy_summary))?
    {
        // it's better to await the token here so that we throttle the syscalls generated by the
        // DirEntry call. the ops-throttle will never cause a deadlock (unlike max-open-files limit)
        // so it's safe to do here.
        throttle::get_ops_token().await;
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        // check entry type for filter matching and skip counting
        let entry_file_type = entry.file_type().await.ok();
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
                copy_summary.directories_skipped += 1;
            } else if entry_is_symlink {
                copy_summary.symlinks_skipped += 1;
            } else {
                copy_summary.files_skipped += 1;
            }
            continue;
        }
        // spawn recursive call - dry-run reporting is handled by copy_internal
        // (copy_file, symlink handling, and directory handling all have their own dry-run reporting)
        let settings = settings.clone();
        let preserve = *preserve;
        let source_root = source_root.to_owned();
        let do_copy = || async move {
            copy_internal(
                prog_track,
                &entry_path,
                &dst_path,
                &source_root,
                &settings,
                &preserve,
                is_fresh,
            )
            .await
        };
        join_set.spawn(do_copy());
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(entries);
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(result) => match result {
                Ok(summary) => copy_summary = copy_summary + summary,
                Err(error) => {
                    tracing::error!("copy: {:?} -> {:?} failed with: {:#}", src, dst, &error);
                    copy_summary = copy_summary + error.summary;
                    if settings.fail_early {
                        return Err(Error::new(error.source, copy_summary));
                    }
                    all_children_succeeded = false;
                }
            },
            Err(error) => {
                if settings.fail_early {
                    return Err(Error::new(error.into(), copy_summary));
                }
            }
        }
    }
    // when filtering is active and we created this directory, check if anything was actually
    // copied into it. if nothing was copied, we may need to clean up the empty directory.
    let this_dir_count = usize::from(we_created_this_dir);
    let child_dirs_created = copy_summary
        .directories_created
        .saturating_sub(this_dir_count);
    let anything_copied = copy_summary.files_copied > 0
        || copy_summary.symlinks_created > 0
        || child_dirs_created > 0;
    let relative_path = src.strip_prefix(source_root).unwrap_or(src);
    let is_root = src == source_root;
    match check_empty_dir_cleanup(
        settings.filter.as_ref(),
        we_created_this_dir,
        anything_copied,
        relative_path,
        is_root,
        settings.dry_run.is_some(),
    ) {
        EmptyDirAction::Keep => { /* proceed with metadata application */ }
        EmptyDirAction::DryRunSkip => {
            tracing::debug!(
                "dry-run: directory {:?} would not be created (nothing to copy inside)",
                &dst
            );
            copy_summary.directories_created = 0;
            return Ok(copy_summary);
        }
        EmptyDirAction::Remove => {
            tracing::debug!(
                "directory {:?} has nothing to copy inside, removing empty directory",
                &dst
            );
            match tokio::fs::remove_dir(dst).await {
                Ok(()) => {
                    copy_summary.directories_created = 0;
                    return Ok(copy_summary);
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
    // apply directory metadata regardless of whether all children copied successfully.
    // the directory itself was created earlier in this function (we would have returned
    // early if create_dir failed), so we should preserve the source metadata.
    // skip metadata setting in dry-run mode since directory wasn't actually created
    tracing::debug!("set 'dst' directory metadata");
    let metadata_result = if settings.dry_run.is_some() {
        Ok(()) // skip metadata setting in dry-run mode
    } else {
        preserve::set_dir_metadata(preserve, &src_metadata, dst).await
    };
    if !all_children_succeeded {
        // child failures take precedence - log metadata error if it also failed
        if let Err(metadata_err) = metadata_result {
            tracing::error!(
                "copy: {:?} -> {:?} failed to set directory metadata: {:#}",
                src,
                dst,
                &metadata_err
            );
        }
        return Err(Error::new(
            anyhow!("copy: {:?} -> {:?} failed!", src, dst),
            copy_summary,
        ))?;
    }
    // no child failures, so metadata error is the primary error
    metadata_result.map_err(|err| Error::new(err, copy_summary))?;
    Ok(copy_summary)
}

#[cfg(test)]
mod copy_tests {
    use crate::testutils;
    use anyhow::Context;
    use std::os::unix::fs::PermissionsExt;
    use tracing_test::traced_test;

    use super::*;

    lazy_static! {
        static ref PROGRESS: progress::Progress = progress::Progress::new();
        static ref NO_PRESERVE_SETTINGS: preserve::Settings = preserve::preserve_default();
        static ref DO_PRESERVE_SETTINGS: preserve::Settings = preserve::preserve_all();
    }

    #[tokio::test]
    #[traced_test]
    async fn check_basic_copy() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
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
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 3);
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn no_read_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let filepaths = vec![
            test_path.join("foo").join("0.txt"),
            test_path.join("foo").join("baz"),
        ];
        for fpath in &filepaths {
            // change file permissions to not readable
            tokio::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o000)).await?;
        }
        match copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
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
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await
        {
            Ok(_) => panic!("Expected the copy to error!"),
            Err(error) => {
                tracing::info!("{}", &error);
                // foo
                // |- 0.txt  // <- no read permission
                // |- bar
                //    |- 1.txt
                //    |- 2.txt
                //    |- 3.txt
                // |- baz   // <- no read permission
                //    |- 4.txt
                //    |- 5.txt -> ../bar/2.txt
                //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
                assert_eq!(error.summary.files_copied, 3);
                assert_eq!(error.summary.symlinks_created, 0);
                assert_eq!(error.summary.directories_created, 2);
            }
        }
        // make source directory same as what we expect destination to be
        for fpath in &filepaths {
            tokio::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o700)).await?;
            if tokio::fs::symlink_metadata(fpath).await?.is_file() {
                tokio::fs::remove_file(fpath).await?;
            } else {
                tokio::fs::remove_dir_all(fpath).await?;
            }
        }
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn check_default_mode() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        // set file to executable
        tokio::fs::set_permissions(
            tmp_dir.join("foo").join("0.txt"),
            std::fs::Permissions::from_mode(0o700),
        )
        .await?;
        // set file executable AND also set sticky bit, setuid and setgid
        let exec_sticky_file = tmp_dir.join("foo").join("bar").join("1.txt");
        tokio::fs::set_permissions(&exec_sticky_file, std::fs::Permissions::from_mode(0o3770))
            .await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
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
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 3);
        // clear the setuid, setgid and sticky bit for comparison
        tokio::fs::set_permissions(
            &exec_sticky_file,
            std::fs::Permissions::from_mode(
                std::fs::symlink_metadata(&exec_sticky_file)?
                    .permissions()
                    .mode()
                    & 0o0777,
            ),
        )
        .await?;
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn no_write_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // directory - readable and non-executable
        let non_exec_dir = test_path.join("foo").join("bogey");
        tokio::fs::create_dir(&non_exec_dir).await?;
        tokio::fs::set_permissions(&non_exec_dir, std::fs::Permissions::from_mode(0o400)).await?;
        // directory - readable and executable
        tokio::fs::set_permissions(
            &test_path.join("foo").join("baz"),
            std::fs::Permissions::from_mode(0o500),
        )
        .await?;
        // file
        tokio::fs::set_permissions(
            &test_path.join("foo").join("baz").join("4.txt"),
            std::fs::Permissions::from_mode(0o440),
        )
        .await?;
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
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
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 4);
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn dereference() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // make files pointed to by symlinks have different permissions than the symlink itself
        let src1 = &test_path.join("foo").join("bar").join("2.txt");
        let src2 = &test_path.join("foo").join("bar").join("3.txt");
        let test_mode = 0o440;
        for f in [src1, src2] {
            tokio::fs::set_permissions(f, std::fs::Permissions::from_mode(test_mode)).await?;
        }
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
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
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 7);
        assert_eq!(summary.symlinks_created, 0);
        assert_eq!(summary.directories_created, 3);
        // ...
        // |- baz
        //    |- 4.txt
        //    |- 5.txt -> ../bar/2.txt
        //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
        let dst1 = &test_path.join("bar").join("baz").join("5.txt");
        let dst2 = &test_path.join("bar").join("baz").join("6.txt");
        for f in [dst1, dst2] {
            let metadata = tokio::fs::symlink_metadata(f)
                .await
                .with_context(|| format!("failed reading metadata from {:?}", &f))?;
            assert!(metadata.is_file());
            // check that the permissions are the same as the source file modulo no sticky bit, setuid and setgid
            assert_eq!(metadata.permissions().mode() & 0o777, test_mode);
        }
        Ok(())
    }

    async fn cp_compare(
        cp_args: &[&str],
        rcp_settings: &Settings,
        preserve: bool,
    ) -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // run a cp command to copy the files
        let cp_output = tokio::process::Command::new("cp")
            .args(cp_args)
            .arg(test_path.join("foo"))
            .arg(test_path.join("bar"))
            .output()
            .await?;
        assert!(cp_output.status.success());
        // now run rcp
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("baz"),
            rcp_settings,
            if preserve {
                &DO_PRESERVE_SETTINGS
            } else {
                &NO_PRESERVE_SETTINGS
            },
            false,
        )
        .await?;
        if rcp_settings.dereference {
            assert_eq!(summary.files_copied, 7);
            assert_eq!(summary.symlinks_created, 0);
        } else {
            assert_eq!(summary.files_copied, 5);
            assert_eq!(summary.symlinks_created, 2);
        }
        assert_eq!(summary.directories_created, 3);
        testutils::check_dirs_identical(
            &test_path.join("bar"),
            &test_path.join("baz"),
            if preserve {
                testutils::FileEqualityCheck::Timestamp
            } else {
                testutils::FileEqualityCheck::Basic
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_compat() -> Result<(), anyhow::Error> {
        cp_compare(
            &["-r"],
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
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
            false,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_compat_preserve() -> Result<(), anyhow::Error> {
        cp_compare(
            &["-r", "-p"],
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
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
            true,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_compat_dereference() -> Result<(), anyhow::Error> {
        cp_compare(
            &["-r", "-L"],
            &Settings {
                dereference: true,
                fail_early: false,
                overwrite: false,
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
            false,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_compat_preserve_and_dereference() -> Result<(), anyhow::Error> {
        cp_compare(
            &["-r", "-p", "-L"],
            &Settings {
                dereference: true,
                fail_early: false,
                overwrite: false,
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
            true,
        )
        .await?;
        Ok(())
    }

    async fn setup_test_dir_and_copy() -> Result<std::path::PathBuf, anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
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
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 3);
        Ok(tmp_dir)
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_overwrite_basic() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_copy().await?;
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
                &RmSettings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings {
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
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
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
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 3);
        assert_eq!(summary.symlinks_created, 1);
        assert_eq!(summary.directories_created, 1);
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
    async fn test_cp_overwrite_dir_file() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_copy().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar
            //    |- 1.txt  <------------------------------------- REMOVE
            //    |- 2.txt
            //    |- 3.txt
            // |- baz  <------------------------------------------ REMOVE
            //    |- 4.txt  <------------------------------------- REMOVE
            //    |- 5.txt -> ../bar/2.txt <---------------------- REMOVE
            //    |- 6.txt -> (absolute path) .../foo/bar/3.txt <- REMOVE
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("bar").join("1.txt"),
                &RmSettings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz"),
                    &RmSettings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 2);
            assert_eq!(summary.symlinks_removed, 2);
            assert_eq!(summary.directories_removed, 1);
        }
        {
            // replace bar/1.txt file with a directory
            tokio::fs::create_dir(&output_path.join("bar").join("1.txt")).await?;
            // replace baz directory with a file
            tokio::fs::write(&output_path.join("baz"), "baz").await?;
        }
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
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
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.rm_summary.files_removed, 1);
        assert_eq!(summary.rm_summary.symlinks_removed, 0);
        assert_eq!(summary.rm_summary.directories_removed, 1);
        assert_eq!(summary.files_copied, 2);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 1);
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
    async fn test_cp_overwrite_symlink_file() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_copy().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- baz
            //    |- 4.txt  <------------------------------------- REMOVE
            //    |- 5.txt -> ../bar/2.txt <---------------------- REMOVE
            // ...
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("baz").join("4.txt"),
                &RmSettings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 1);
            assert_eq!(summary.symlinks_removed, 1);
            assert_eq!(summary.directories_removed, 0);
        }
        {
            // replace baz/4.txt file with a symlink
            tokio::fs::symlink("../0.txt", &output_path.join("baz").join("4.txt")).await?;
            // replace baz/5.txt symlink with a file
            tokio::fs::write(&output_path.join("baz").join("5.txt"), "baz").await?;
        }
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
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
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.rm_summary.files_removed, 1);
        assert_eq!(summary.rm_summary.symlinks_removed, 1);
        assert_eq!(summary.rm_summary.directories_removed, 0);
        assert_eq!(summary.files_copied, 1);
        assert_eq!(summary.symlinks_created, 1);
        assert_eq!(summary.directories_created, 0);
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
    async fn test_cp_overwrite_symlink_dir() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_copy().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar  <------------------------------------------ REMOVE
            //    |- 1.txt  <------------------------------------- REMOVE
            //    |- 2.txt  <------------------------------------- REMOVE
            //    |- 3.txt  <------------------------------------- REMOVE
            // |- baz
            //    |- 5.txt -> ../bar/2.txt <---------------------- REMOVE
            // ...
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("bar"),
                &RmSettings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings {
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
        {
            // replace bar directory with a symlink
            tokio::fs::symlink("0.txt", &output_path.join("bar")).await?;
            // replace baz/5.txt symlink with a directory
            tokio::fs::create_dir(&output_path.join("baz").join("5.txt")).await?;
        }
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
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
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.rm_summary.files_removed, 0);
        assert_eq!(summary.rm_summary.symlinks_removed, 1);
        assert_eq!(summary.rm_summary.directories_removed, 1);
        assert_eq!(summary.files_copied, 3);
        assert_eq!(summary.symlinks_created, 1);
        assert_eq!(summary.directories_created, 1);
        assert_eq!(summary.files_unchanged, 2);
        assert_eq!(summary.symlinks_unchanged, 1);
        assert_eq!(summary.directories_unchanged, 2);
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
    async fn test_cp_overwrite_error() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
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
            &NO_PRESERVE_SETTINGS, // we want timestamps to differ!
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 3);
        let source_path = &test_path.join("foo");
        let output_path = &tmp_dir.join("bar");
        // unreadable
        tokio::fs::set_permissions(
            &source_path.join("bar"),
            std::fs::Permissions::from_mode(0o000),
        )
        .await?;
        tokio::fs::set_permissions(
            &source_path.join("baz").join("4.txt"),
            std::fs::Permissions::from_mode(0o000),
        )
        .await?;
        // bar
        // |- 0.txt
        // |- bar  <---------------------------------------- NON READABLE
        // |- baz
        //    |- 4.txt  <----------------------------------- NON READABLE
        //    |- 5.txt -> ../bar/2.txt
        //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
        match copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
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
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await
        {
            Ok(_) => panic!("Expected the copy to error!"),
            Err(error) => {
                tracing::info!("{}", &error);
                assert_eq!(error.summary.files_copied, 1);
                assert_eq!(error.summary.symlinks_created, 2);
                assert_eq!(error.summary.directories_created, 0);
                assert_eq!(error.summary.rm_summary.files_removed, 2);
                assert_eq!(error.summary.rm_summary.symlinks_removed, 2);
                assert_eq!(error.summary.rm_summary.directories_removed, 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_dereference_symlink_chain() -> Result<(), anyhow::Error> {
        // Create a fresh temporary directory to avoid conflicts
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // Create a chain of symlinks: foo -> bar -> baz (actual file)
        let baz_file = test_path.join("baz_file.txt");
        tokio::fs::write(&baz_file, "final content").await?;
        let bar_link = test_path.join("bar_link");
        let foo_link = test_path.join("foo_link");
        // Create chain: foo_link -> bar_link -> baz_file.txt
        tokio::fs::symlink(&baz_file, &bar_link).await?;
        tokio::fs::symlink(&bar_link, &foo_link).await?;
        // Create source directory with the symlink chain
        let src_dir = test_path.join("src_chain");
        tokio::fs::create_dir(&src_dir).await?;
        // Copy the chain into the source directory
        tokio::fs::symlink("../foo_link", &src_dir.join("foo")).await?;
        tokio::fs::symlink("../bar_link", &src_dir.join("bar")).await?;
        tokio::fs::symlink("../baz_file.txt", &src_dir.join("baz")).await?;
        // Test with dereference - should copy 3 files with same content
        let summary = copy(
            &PROGRESS,
            &src_dir,
            &test_path.join("dst_with_deref"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
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
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 3); // foo, bar, baz all copied as files
        assert_eq!(summary.symlinks_created, 0); // dereference is set
        assert_eq!(summary.directories_created, 1);
        let dst_dir = test_path.join("dst_with_deref");
        // Verify all three are now regular files with the same content
        let foo_content = tokio::fs::read_to_string(dst_dir.join("foo")).await?;
        let bar_content = tokio::fs::read_to_string(dst_dir.join("bar")).await?;
        let baz_content = tokio::fs::read_to_string(dst_dir.join("baz")).await?;
        assert_eq!(foo_content, "final content");
        assert_eq!(bar_content, "final content");
        assert_eq!(baz_content, "final content");
        // Verify they are all regular files, not symlinks
        assert!(dst_dir.join("foo").is_file());
        assert!(dst_dir.join("bar").is_file());
        assert!(dst_dir.join("baz").is_file());
        assert!(!dst_dir.join("foo").is_symlink());
        assert!(!dst_dir.join("bar").is_symlink());
        assert!(!dst_dir.join("baz").is_symlink());
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_dereference_symlink_to_directory() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // Create a directory with specific permissions and content
        let target_dir = test_path.join("target_dir");
        tokio::fs::create_dir(&target_dir).await?;
        tokio::fs::set_permissions(&target_dir, std::fs::Permissions::from_mode(0o755)).await?;
        // Add some files to the directory
        tokio::fs::write(target_dir.join("file1.txt"), "content1").await?;
        tokio::fs::write(target_dir.join("file2.txt"), "content2").await?;
        tokio::fs::set_permissions(
            &target_dir.join("file1.txt"),
            std::fs::Permissions::from_mode(0o644),
        )
        .await?;
        tokio::fs::set_permissions(
            &target_dir.join("file2.txt"),
            std::fs::Permissions::from_mode(0o600),
        )
        .await?;
        // Create a symlink pointing to the directory
        let dir_symlink = test_path.join("dir_symlink");
        tokio::fs::symlink(&target_dir, &dir_symlink).await?;
        // Test copying the symlink with dereference - should copy as a directory
        let summary = copy(
            &PROGRESS,
            &dir_symlink,
            &test_path.join("copied_dir"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
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
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 2); // file1.txt, file2.txt
        assert_eq!(summary.symlinks_created, 0); // dereference is set
        assert_eq!(summary.directories_created, 1); // copied_dir
        let copied_dir = test_path.join("copied_dir");
        // Verify the directory and its contents were copied
        assert!(copied_dir.is_dir());
        assert!(!copied_dir.is_symlink()); // Should be a real directory, not a symlink
                                           // Verify files were copied with correct content
        let file1_content = tokio::fs::read_to_string(copied_dir.join("file1.txt")).await?;
        let file2_content = tokio::fs::read_to_string(copied_dir.join("file2.txt")).await?;
        assert_eq!(file1_content, "content1");
        assert_eq!(file2_content, "content2");
        // Verify permissions were preserved
        let copied_dir_metadata = tokio::fs::metadata(&copied_dir).await?;
        let file1_metadata = tokio::fs::metadata(copied_dir.join("file1.txt")).await?;
        let file2_metadata = tokio::fs::metadata(copied_dir.join("file2.txt")).await?;
        assert_eq!(copied_dir_metadata.permissions().mode() & 0o777, 0o755);
        assert_eq!(file1_metadata.permissions().mode() & 0o777, 0o644);
        assert_eq!(file2_metadata.permissions().mode() & 0o777, 0o600);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_dereference_permissions_preserved() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // Create files with specific permissions
        let file1 = test_path.join("file1.txt");
        let file2 = test_path.join("file2.txt");
        tokio::fs::write(&file1, "content1").await?;
        tokio::fs::write(&file2, "content2").await?;
        tokio::fs::set_permissions(&file1, std::fs::Permissions::from_mode(0o755)).await?;
        tokio::fs::set_permissions(&file2, std::fs::Permissions::from_mode(0o640)).await?;
        // Create symlinks pointing to these files
        let symlink1 = test_path.join("symlink1");
        let symlink2 = test_path.join("symlink2");
        tokio::fs::symlink(&file1, &symlink1).await?;
        tokio::fs::symlink(&file2, &symlink2).await?;
        // Test copying symlinks with dereference and preserve
        let summary1 = copy(
            &PROGRESS,
            &symlink1,
            &test_path.join("copied_file1.txt"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings::default(),
                chunk_size: 0,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
            },
            &DO_PRESERVE_SETTINGS, // <- important!
            false,
        )
        .await?;
        let summary2 = copy(
            &PROGRESS,
            &symlink2,
            &test_path.join("copied_file2.txt"),
            &Settings {
                dereference: true,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings::default(),
                chunk_size: 0,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary1.files_copied, 1);
        assert_eq!(summary1.symlinks_created, 0);
        assert_eq!(summary2.files_copied, 1);
        assert_eq!(summary2.symlinks_created, 0);
        let copied1 = test_path.join("copied_file1.txt");
        let copied2 = test_path.join("copied_file2.txt");
        // Verify files are regular files, not symlinks
        assert!(copied1.is_file());
        assert!(!copied1.is_symlink());
        assert!(copied2.is_file());
        assert!(!copied2.is_symlink());
        // Verify content was copied correctly
        let content1 = tokio::fs::read_to_string(&copied1).await?;
        let content2 = tokio::fs::read_to_string(&copied2).await?;
        assert_eq!(content1, "content1");
        assert_eq!(content2, "content2");
        // Verify permissions from the target files were preserved (not symlink permissions)
        let copied1_metadata = tokio::fs::metadata(&copied1).await?;
        let copied2_metadata = tokio::fs::metadata(&copied2).await?;
        assert_eq!(copied1_metadata.permissions().mode() & 0o777, 0o755);
        assert_eq!(copied2_metadata.permissions().mode() & 0o777, 0o640);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_dereference_dir() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        // symlink bar to bar-link
        tokio::fs::symlink("bar", &tmp_dir.join("foo").join("bar-link")).await?;
        // symlink bar-link to bar-link-link
        tokio::fs::symlink("bar-link", &tmp_dir.join("foo").join("bar-link-link")).await?;
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
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
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 13); // 0.txt, 3x bar/(1.txt, 2.txt, 3.txt), baz/(4.txt, 5.txt, 6.txt)
        assert_eq!(summary.symlinks_created, 0); // dereference is set
        assert_eq!(summary.directories_created, 5);
        // check_dirs_identical doesn't handle dereference so let's do it manually
        tokio::process::Command::new("cp")
            .args(["-r", "-L"])
            .arg(tmp_dir.join("foo"))
            .arg(tmp_dir.join("bar-cp"))
            .output()
            .await?;
        testutils::check_dirs_identical(
            &tmp_dir.join("bar"),
            &tmp_dir.join("bar-cp"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    /// Tests to verify error messages include root causes for debugging
    mod error_message_tests {
        use super::*;

        /// Helper to extract full error message with chain
        fn get_full_error_message(error: &Error) -> String {
            format!("{:#}", error.source)
        }

        #[tokio::test]
        #[traced_test]
        async fn test_permission_error_includes_root_cause() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let unreadable = tmp_dir.join("unreadable.txt");
            tokio::fs::write(&unreadable, "test").await?;
            tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).await?;

            let result = copy_file(
                &PROGRESS,
                &unreadable,
                &tmp_dir.join("dest.txt"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await;

            assert!(result.is_err(), "Should fail with permission error");
            let err_msg = get_full_error_message(&result.unwrap_err());

            // The error message MUST include the root cause
            assert!(
                err_msg.to_lowercase().contains("permission")
                    || err_msg.contains("EACCES")
                    || err_msg.contains("denied"),
                "Error message must include permission-related text. Got: {}",
                err_msg
            );
            Ok(())
        }

        #[tokio::test]
        #[traced_test]
        async fn test_nonexistent_source_includes_root_cause() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;

            let result = copy_file(
                &PROGRESS,
                &tmp_dir.join("does_not_exist.txt"),
                &tmp_dir.join("dest.txt"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await;

            assert!(result.is_err());
            let err_msg = get_full_error_message(&result.unwrap_err());

            assert!(
                err_msg.to_lowercase().contains("no such file")
                    || err_msg.to_lowercase().contains("not found")
                    || err_msg.contains("ENOENT"),
                "Error message must include file not found text. Got: {}",
                err_msg
            );
            Ok(())
        }

        #[tokio::test]
        #[traced_test]
        async fn test_unreadable_directory_includes_root_cause() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let unreadable_dir = tmp_dir.join("unreadable_dir");
            tokio::fs::create_dir(&unreadable_dir).await?;
            tokio::fs::set_permissions(&unreadable_dir, std::fs::Permissions::from_mode(0o000))
                .await?;

            let result = copy(
                &PROGRESS,
                &unreadable_dir,
                &tmp_dir.join("dest"),
                &Settings {
                    dereference: false,
                    fail_early: true,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await;

            assert!(result.is_err());
            let err_msg = get_full_error_message(&result.unwrap_err());

            assert!(
                err_msg.to_lowercase().contains("permission")
                    || err_msg.contains("EACCES")
                    || err_msg.contains("denied"),
                "Error message must include permission-related text. Got: {}",
                err_msg
            );

            // Clean up - restore permissions so cleanup can remove it
            tokio::fs::set_permissions(&unreadable_dir, std::fs::Permissions::from_mode(0o700))
                .await?;
            Ok(())
        }

        #[tokio::test]
        #[traced_test]
        async fn test_destination_permission_error_includes_root_cause() -> Result<(), anyhow::Error>
        {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let readonly_parent = test_path.join("readonly_dest");
            tokio::fs::create_dir(&readonly_parent).await?;
            tokio::fs::set_permissions(&readonly_parent, std::fs::Permissions::from_mode(0o555))
                .await?;

            let result = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &readonly_parent.join("copy"),
                &Settings {
                    dereference: false,
                    fail_early: true,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await;

            // restore permissions so cleanup succeeds even when copy fails
            tokio::fs::set_permissions(&readonly_parent, std::fs::Permissions::from_mode(0o755))
                .await?;

            assert!(result.is_err(), "copy into read-only parent should fail");
            let err_msg = get_full_error_message(&result.unwrap_err());

            assert!(
                err_msg.to_lowercase().contains("permission denied") || err_msg.contains("EACCES"),
                "Error message must include permission denied text. Got: {}",
                err_msg
            );
            Ok(())
        }
    }

    mod empty_dir_cleanup_tests {
        use super::*;
        use std::path::Path;
        #[test]
        fn test_check_empty_dir_cleanup_no_filter() {
            // when no filter, always keep
            assert_eq!(
                check_empty_dir_cleanup(None, true, false, Path::new("any"), false, false),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_something_copied() {
            // when content was copied, keep
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, true, Path::new("any"), false, false),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_not_created() {
            // when we didn't create the directory, keep
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(
                    Some(&filter),
                    false,
                    false,
                    Path::new("any"),
                    false,
                    false
                ),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_directly_matched() {
            // when directory directly matches include pattern, keep
            let mut filter = FilterSettings::new();
            filter.add_include("target/").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(
                    Some(&filter),
                    true,
                    false,
                    Path::new("target"),
                    false,
                    false
                ),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_traversed_only() {
            // when directory was only traversed, remove
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, false, Path::new("src"), false, false),
                EmptyDirAction::Remove
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_dry_run() {
            // in dry-run mode, skip instead of remove
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, false, Path::new("src"), false, true),
                EmptyDirAction::DryRunSkip
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_root_always_kept() {
            // root directory is never removed, even with filter and nothing copied
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, false, Path::new(""), true, false),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_root_kept_in_dry_run() {
            // root directory is kept even in dry-run mode
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, false, Path::new(""), true, true),
                EmptyDirAction::Keep
            );
        }
    }

    /// Verify that directory metadata is applied even when child operations fail.
    /// This is a regression test for a bug where directory permissions were not preserved
    /// when copying with fail_early=false and some children failed to copy.
    #[tokio::test]
    #[traced_test]
    async fn test_directory_metadata_applied_on_child_error() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // create source directory with specific permissions
        let src_dir = test_path.join("src");
        tokio::fs::create_dir(&src_dir).await?;
        tokio::fs::set_permissions(&src_dir, std::fs::Permissions::from_mode(0o750)).await?;
        // create a readable file and an unreadable file inside
        let readable_file = src_dir.join("readable.txt");
        tokio::fs::write(&readable_file, "content").await?;
        let unreadable_file = src_dir.join("unreadable.txt");
        tokio::fs::write(&unreadable_file, "secret").await?;
        tokio::fs::set_permissions(&unreadable_file, std::fs::Permissions::from_mode(0o000))
            .await?;
        let dst_dir = test_path.join("dst");
        // copy with fail_early=false and preserve=all
        let result = copy(
            &PROGRESS,
            &src_dir,
            &dst_dir,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: Default::default(),
                chunk_size: 0,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await;
        // restore permissions so cleanup can succeed
        tokio::fs::set_permissions(&unreadable_file, std::fs::Permissions::from_mode(0o644))
            .await?;
        // verify the operation returned an error (unreadable file should fail)
        assert!(result.is_err(), "copy should fail due to unreadable file");
        let error = result.unwrap_err();
        // verify some files were copied (the readable one)
        assert_eq!(error.summary.files_copied, 1);
        assert_eq!(error.summary.directories_created, 1);
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
        /// This test exposes the bug where only entry_name is passed to the filter
        /// instead of the relative path.
        #[tokio::test]
        #[traced_test]
        async fn test_path_pattern_matches_nested_files() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // test directory structure from setup_test_dir:
            // foo/
            //   0.txt
            //   bar/
            //     1.txt
            //     2.txt
            //   baz/
            //     3.txt -> ../0.txt (symlink)
            //     4.txt
            //     5 -> ../bar (symlink)
            // create filter that should match bar/*.txt (files in bar directory)
            let mut filter = FilterSettings::new();
            filter.add_include("bar/*.txt").unwrap();
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // should only copy files matching bar/*.txt pattern
            // bar/1.txt, bar/2.txt, and bar/3.txt should be copied
            assert_eq!(
                summary.files_copied, 3,
                "should copy 3 files matching bar/*.txt"
            );
            // verify the right files exist
            assert!(
                test_path.join("dst/bar/1.txt").exists(),
                "bar/1.txt should be copied"
            );
            assert!(
                test_path.join("dst/bar/2.txt").exists(),
                "bar/2.txt should be copied"
            );
            assert!(
                test_path.join("dst/bar/3.txt").exists(),
                "bar/3.txt should be copied"
            );
            // verify files outside the pattern don't exist
            assert!(
                !test_path.join("dst/0.txt").exists(),
                "0.txt should not be copied"
            );
            Ok(())
        }
        /// Test that anchored patterns (starting with /) match only at root.
        #[tokio::test]
        #[traced_test]
        async fn test_anchored_pattern_matches_only_at_root() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // create filter that should match /bar/** (bar directory and all its contents)
            let mut filter = FilterSettings::new();
            filter.add_include("/bar/**").unwrap();
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // should only copy bar directory and its contents
            assert!(
                test_path.join("dst/bar").exists(),
                "bar directory should be copied"
            );
            assert!(
                !test_path.join("dst/baz").exists(),
                "baz directory should not be copied"
            );
            assert!(
                !test_path.join("dst/0.txt").exists(),
                "0.txt should not be copied"
            );
            // verify summary counts
            assert_eq!(
                summary.files_copied, 3,
                "should copy 3 files in bar (1.txt, 2.txt, 3.txt)"
            );
            assert_eq!(
                summary.directories_created, 2,
                "should create 2 directories (root dst + bar)"
            );
            // skipped: 0.txt (file) and baz (directory) - baz contents not counted since dir is skipped
            assert_eq!(summary.files_skipped, 1, "should skip 1 file (0.txt)");
            assert_eq!(
                summary.directories_skipped, 1,
                "should skip 1 directory (baz)"
            );
            Ok(())
        }
        /// Test that double-star patterns (**) match across directories.
        #[tokio::test]
        #[traced_test]
        async fn test_double_star_pattern_matches_nested() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // create filter that should match all .txt files at any depth
            let mut filter = FilterSettings::new();
            filter.add_include("**/*.txt").unwrap();
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // should copy all .txt files: 0.txt, bar/1.txt, bar/2.txt, bar/3.txt, baz/4.txt
            assert_eq!(
                summary.files_copied, 5,
                "should copy all 5 .txt files with **/*.txt pattern"
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
            let result = copy(
                &PROGRESS,
                &test_path.join("foo/0.txt"), // single file source
                &test_path.join("dst.txt"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // the file should NOT be copied because it matches the exclude pattern
            assert_eq!(
                result.files_copied, 0,
                "file matching exclude pattern should not be copied"
            );
            assert!(
                !test_path.join("dst.txt").exists(),
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
            let result = copy(
                &PROGRESS,
                &test_path.join("excluded_dir"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // directory should NOT be copied because it matches exclude pattern
            assert_eq!(
                result.directories_created, 0,
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
            let result = copy(
                &PROGRESS,
                &test_path.join("excluded_link"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // symlink should NOT be copied because it matches exclude pattern
            assert_eq!(
                result.symlinks_created, 0,
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
            // include all .txt files, but exclude bar/2.txt specifically
            let mut filter = FilterSettings::new();
            filter.add_include("**/*.txt").unwrap();
            filter.add_exclude("bar/2.txt").unwrap();
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // should copy: 0.txt, bar/1.txt, bar/3.txt, baz/4.txt = 4 files
            // should skip: bar/2.txt (excluded by pattern) = 1 file
            // symlinks 5.txt and 6.txt don't match *.txt include pattern (symlinks, not files)
            assert_eq!(summary.files_copied, 4, "should copy 4 .txt files");
            assert_eq!(
                summary.files_skipped, 1,
                "should skip 1 file (bar/2.txt excluded)"
            );
            // verify specific files
            assert!(
                test_path.join("dst/bar/1.txt").exists(),
                "bar/1.txt should be copied"
            );
            assert!(
                !test_path.join("dst/bar/2.txt").exists(),
                "bar/2.txt should be excluded"
            );
            assert!(
                test_path.join("dst/bar/3.txt").exists(),
                "bar/3.txt should be copied"
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
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // copied: 0.txt (1 file), baz/4.txt (1 file), 5.txt symlink, 6.txt symlink
            // skipped: bar directory (1 dir) - contents not counted since whole dir skipped
            // directories: foo (root), baz = 2
            assert_eq!(summary.files_copied, 2, "should copy 2 files");
            assert_eq!(summary.symlinks_created, 2, "should copy 2 symlinks");
            assert_eq!(
                summary.directories_created, 2,
                "should create 2 directories"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "should skip 1 directory (bar)"
            );
            assert_eq!(
                summary.files_skipped, 0,
                "no files skipped (bar contents not counted)"
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
            let summary = copy(
                &PROGRESS,
                &src_path,
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // only 'foo' should be copied
            assert_eq!(summary.files_copied, 1, "should copy only 'foo' file");
            assert_eq!(
                summary.directories_created, 1,
                "should create only root directory (not empty 'baz')"
            );
            // verify foo was copied
            assert!(
                test_path.join("dst").join("foo").exists(),
                "foo should be copied"
            );
            // verify bar was not copied (not matching include pattern)
            assert!(
                !test_path.join("dst").join("bar").exists(),
                "bar should not be copied"
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
            let summary = copy(
                &PROGRESS,
                &src_path,
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // only 'foo' should be copied
            assert_eq!(summary.files_copied, 1, "should copy only 'foo' file");
            assert_eq!(
                summary.files_skipped, 2,
                "should skip 2 files (qux and quux)"
            );
            assert_eq!(
                summary.directories_created, 1,
                "should create only root directory (not 'baz' with non-matching content)"
            );
            // verify foo was copied
            assert!(
                test_path.join("dst").join("foo").exists(),
                "foo should be copied"
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
            let summary = copy(
                &PROGRESS,
                &src_path,
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: Some(crate::config::DryRunMode::Explain),
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // only 'foo' should be reported as would-be-copied
            assert_eq!(
                summary.files_copied, 1,
                "should report only 'foo' would be copied"
            );
            assert_eq!(
                summary.directories_created, 1,
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
        /// even if nothing is copied into them due to filters.
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
            let summary = copy(
                &PROGRESS,
                &src_path,
                &dst_path,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: true, // enable overwrite mode
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // foo should be copied
            assert_eq!(summary.files_copied, 1, "should copy only 'foo' file");
            // dst and baz should be unchanged (both already existed)
            assert_eq!(
                summary.directories_unchanged, 2,
                "root dst and baz directories should be unchanged"
            );
            assert_eq!(
                summary.directories_created, 0,
                "should not create any directories"
            );
            // verify foo was copied
            assert!(dst_path.join("foo").exists(), "foo should be copied");
            // verify bar was NOT copied
            assert!(!dst_path.join("bar").exists(), "bar should not be copied");
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
        /// Test that dry-run mode for directories doesn't create the destination
        /// and doesn't try to set metadata on non-existent directories.
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
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &dst_path,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
                },
                &NO_PRESERVE_SETTINGS,
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
                summary.directories_created > 0,
                "dry-run should report directories that would be created"
            );
            assert!(
                summary.files_copied > 0,
                "dry-run should report files that would be copied"
            );
            Ok(())
        }
        /// Test that root directory is always created even when nothing matches
        /// the include pattern. The root is the user-specified source — it should
        /// never be removed/skipped due to empty-dir cleanup.
        #[tokio::test]
        #[traced_test]
        async fn test_root_dir_preserved_when_nothing_matches() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // src/
            //   bar.log (doesn't match *.txt)
            //   baz/ (empty directory)
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("bar.log"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            // include only *.txt - nothing in source matches
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            let dst_path = test_path.join("dst");
            let summary = copy(
                &PROGRESS,
                &src_path,
                &dst_path,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // no files should be copied
            assert_eq!(summary.files_copied, 0, "no files match *.txt");
            // root directory should still be created
            assert_eq!(
                summary.directories_created, 1,
                "root directory should always be created"
            );
            assert!(dst_path.exists(), "root destination directory should exist");
            // non-matching subdirectories should not be created
            assert!(
                !dst_path.join("baz").exists(),
                "empty baz should not be created"
            );
            Ok(())
        }
        /// Test that root directory is counted in dry-run even when nothing matches.
        #[tokio::test]
        #[traced_test]
        async fn test_root_dir_counted_in_dry_run_when_nothing_matches() -> Result<(), anyhow::Error>
        {
            let test_path = testutils::create_temp_dir().await?;
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("bar.log"), "content").await?;
            // include only *.txt - nothing matches
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            let dst_path = test_path.join("dst");
            let summary = copy(
                &PROGRESS,
                &src_path,
                &dst_path,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    chunk_size: 0,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: Some(crate::config::DryRunMode::Explain),
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            assert_eq!(summary.files_copied, 0, "no files match *.txt");
            assert_eq!(
                summary.directories_created, 1,
                "root directory should be counted in dry-run"
            );
            assert!(
                !dst_path.exists(),
                "nothing should be created in dry-run mode"
            );
            Ok(())
        }
    }
}
