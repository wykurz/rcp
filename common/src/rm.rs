use anyhow::{Context, anyhow};
use async_recursion::async_recursion;
use std::os::unix::fs::PermissionsExt;
use tracing::instrument;

use crate::filter::TimeFilter;
use crate::progress;
use crate::walk::{self, EntryKind};

/// Error type for remove operations. See [`crate::error::OperationError`] for
/// logging conventions and rationale.
pub type Error = crate::error::OperationError<Summary>;

#[derive(Debug, Clone)]
pub struct Settings {
    pub fail_early: bool,
    /// filter settings for include/exclude patterns
    pub filter: Option<crate::filter::FilterSettings>,
    /// time-based filter (mtime/btime); applied to each entry individually (files,
    /// symlinks, and directories). This is an entry filter, not a subtree gate:
    /// directories are always traversed, and the filter only decides whether each
    /// entry — including the directory itself, after its children are processed — is
    /// eligible for removal. A directory whose own timestamps are too recent is left
    /// intact even when its children have been removed; a non-empty leftover directory
    /// is logged at info and not treated as an error.
    pub time_filter: Option<TimeFilter>,
    /// dry-run mode for previewing operations
    pub dry_run: Option<crate::config::DryRunMode>,
}

/// Returns true when `err`'s chain contains an `io::Error` with `ErrorKind::Unsupported`.
/// Used to downgrade time-filter eval failures on filesystems / entry types that don't
/// report btime (e.g. many symlinks) from `error!` to `warn!` so they don't flood logs
/// on otherwise-successful runs.
fn is_unsupported_io_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| io_err.kind() == std::io::ErrorKind::Unsupported)
    })
}

/// Summary with the appropriate `*_skipped` counter set to 1 for the given entry kind.
/// Special files count as `files_skipped` to match the historical mapping used
/// when filters skip an entry.
fn skipped_summary_for(kind: EntryKind) -> Summary {
    match kind {
        EntryKind::Dir => Summary {
            directories_skipped: 1,
            ..Default::default()
        },
        EntryKind::Symlink => Summary {
            symlinks_skipped: 1,
            ..Default::default()
        },
        EntryKind::File | EntryKind::Special => Summary {
            files_skipped: 1,
            ..Default::default()
        },
    }
}

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    pub bytes_removed: u64,
    pub files_removed: usize,
    pub symlinks_removed: usize,
    pub directories_removed: usize,
    pub files_skipped: usize,
    pub symlinks_skipped: usize,
    pub directories_skipped: usize,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            bytes_removed: self.bytes_removed + other.bytes_removed,
            files_removed: self.files_removed + other.files_removed,
            symlinks_removed: self.symlinks_removed + other.symlinks_removed,
            directories_removed: self.directories_removed + other.directories_removed,
            files_skipped: self.files_skipped + other.files_skipped,
            symlinks_skipped: self.symlinks_skipped + other.symlinks_skipped,
            directories_skipped: self.directories_skipped + other.directories_skipped,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "bytes removed: {}\n\
            files removed: {}\n\
            symlinks removed: {}\n\
            directories removed: {}\n\
            files skipped: {}\n\
            symlinks skipped: {}\n\
            directories skipped: {}\n",
            bytesize::ByteSize(self.bytes_removed),
            self.files_removed,
            self.symlinks_removed,
            self.directories_removed,
            self.files_skipped,
            self.symlinks_skipped,
            self.directories_skipped
        )
    }
}

/// Public entry point for remove operations.
/// Internally delegates to rm_internal with source_root tracking for proper filter matching.
#[instrument(skip(prog_track, settings))]
pub async fn rm(
    prog_track: &'static progress::Progress,
    path: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    // check filter for top-level path (files, directories, and symlinks)
    if let Some(ref filter) = settings.filter {
        let path_name = path.file_name().map(std::path::Path::new);
        if let Some(name) = path_name {
            let path_metadata = crate::walk::run_metadata_probed(
                congestion::Side::Source,
                tokio::fs::symlink_metadata(path),
            )
            .await
            .with_context(|| format!("failed reading metadata from {:?}", &path))
            .map_err(|err| Error::new(err, Default::default()))?;
            let is_dir = path_metadata.is_dir();
            let result = filter.should_include_root_item(name, is_dir);
            match result {
                crate::filter::FilterResult::Included => {}
                result => {
                    let kind = EntryKind::from_metadata(&path_metadata);
                    if let Some(mode) = settings.dry_run {
                        crate::dry_run::report_skip(path, &result, mode, kind.label_long());
                    }
                    kind.inc_skipped(prog_track);
                    return Ok(skipped_summary_for(kind));
                }
            }
        }
    }
    // note: the time filter (applied to files, symlinks, and directories) is handled
    // inside rm_internal, so we don't duplicate the check here.
    rm_internal(prog_track, path, path, settings).await
}
#[instrument(skip(prog_track, settings))]
#[async_recursion]
async fn rm_internal(
    prog_track: &'static progress::Progress,
    path: &std::path::Path,
    source_root: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    let _ops_guard = prog_track.ops.guard();
    tracing::debug!("read path metadata");
    let src_metadata = crate::walk::run_metadata_probed(
        congestion::Side::Source,
        tokio::fs::symlink_metadata(path),
    )
    .await
    .with_context(|| format!("failed reading metadata from {:?}", &path))
    .map_err(|err| Error::new(err, Default::default()))?;
    if !src_metadata.is_dir() {
        tracing::debug!("not a directory, just remove");
        let is_symlink = src_metadata.file_type().is_symlink();
        let file_size = if is_symlink { 0 } else { src_metadata.len() };
        // apply time filter before removing (files/symlinks only)
        if let Some(ref time_filter) = settings.time_filter {
            let entry_type = if is_symlink { "symlink" } else { "file" };
            let make_skipped_summary = || {
                tracing::debug!("skipping {:?} due to time filter", &path);
                if is_symlink {
                    prog_track.symlinks_skipped.inc();
                    Summary {
                        symlinks_skipped: 1,
                        ..Default::default()
                    }
                } else {
                    prog_track.files_skipped.inc();
                    Summary {
                        files_skipped: 1,
                        ..Default::default()
                    }
                }
            };
            match time_filter.matches(&src_metadata) {
                Ok(result) => {
                    if let Some(skip_reason) = result.as_skip_reason() {
                        if let Some(mode) = settings.dry_run {
                            crate::dry_run::report_time_skip(path, skip_reason, mode, entry_type);
                        }
                        return Ok(make_skipped_summary());
                    }
                }
                Err(err) => {
                    let err = err.context(format!("failed evaluating time filter on {:?}", &path));
                    if settings.fail_early {
                        return Err(Error::new(err, Default::default()));
                    }
                    // log and skip — never delete an entry whose age we cannot verify.
                    // btime being unsupported (common for symlinks) is expected noise, so
                    // downgrade to warn; anything else is unexpected and stays at error.
                    if is_unsupported_io_error(&err) {
                        tracing::warn!(
                            "time filter evaluation unsupported for {} {:?}, skipping: {:#}",
                            entry_type,
                            &path,
                            &err
                        );
                    } else {
                        tracing::error!(
                            "time filter evaluation failed for {} {:?}, skipping: {:#}",
                            entry_type,
                            &path,
                            &err
                        );
                    }
                    return Ok(make_skipped_summary());
                }
            }
        }
        // handle dry-run mode for files/symlinks
        if settings.dry_run.is_some() {
            let entry_type = if is_symlink { "symlink" } else { "file" };
            crate::dry_run::report_action("remove", path, None, entry_type);
            return Ok(Summary {
                bytes_removed: file_size,
                files_removed: if is_symlink { 0 } else { 1 },
                symlinks_removed: if is_symlink { 1 } else { 0 },
                ..Default::default()
            });
        }
        crate::walk::run_metadata_probed(
            congestion::Side::Destination,
            tokio::fs::remove_file(path),
        )
        .await
        .with_context(|| format!("failed removing {:?}", &path))
        .map_err(|err| Error::new(err, Default::default()))?;
        if is_symlink {
            prog_track.symlinks_removed.inc();
            return Ok(Summary {
                symlinks_removed: 1,
                ..Default::default()
            });
        }
        prog_track.files_removed.inc();
        prog_track.bytes_removed.add(file_size);
        return Ok(Summary {
            bytes_removed: file_size,
            files_removed: 1,
            ..Default::default()
        });
    }
    tracing::debug!("remove contents of the directory first");
    // only change permissions if not in dry-run mode
    if settings.dry_run.is_none() && src_metadata.permissions().readonly() {
        tracing::debug!("directory is read-only - change the permissions");
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))
            .await
            .with_context(|| {
                format!(
                    "failed to make '{:?}' directory readable and writeable",
                    &path
                )
            })
            .map_err(|err| Error::new(err, Default::default()))?;
    }
    let mut entries = tokio::fs::read_dir(path)
        .await
        .with_context(|| format!("failed reading directory {:?}", &path))
        .map_err(|err| Error::new(err, Default::default()))?;
    let mut join_set = tokio::task::JoinSet::new();
    let errors = crate::error_collector::ErrorCollector::default();
    let mut skipped_files = 0;
    let mut skipped_symlinks = 0;
    let mut skipped_dirs = 0;
    loop {
        let Some((entry, entry_file_type)) =
            crate::walk::next_entry_probed(&mut entries, congestion::Side::Source, || {
                format!("failed traversing directory {:?}", &path)
            })
            .await
            .map_err(|err| Error::new(err, Default::default()))?
        else {
            break;
        };
        let entry_path = entry.path();
        let entry_kind = EntryKind::from_file_type(entry_file_type.as_ref());
        let entry_is_dir = entry_kind == EntryKind::Dir;
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
            // increment skipped counters - will be added to rm_summary below
            match entry_kind {
                EntryKind::Dir => skipped_dirs += 1,
                EntryKind::Symlink => skipped_symlinks += 1,
                EntryKind::File | EntryKind::Special => skipped_files += 1,
            }
            entry_kind.inc_skipped(prog_track);
            continue;
        }
        let settings = settings.clone();
        let source_root = source_root.to_owned();
        // for positively-known leaf entries (files, symlinks, special),
        // acquire the pending-meta permit BEFORE spawning so we don't create
        // unbounded tasks. We deliberately skip pre-acquire when
        // `entry_file_type` is None (file_type() lookup failed): the entry
        // could actually be a directory, and a chain of such unknown-typed
        // directories holding permits while recursing would deadlock the
        // pending-meta pool. Directories also skip pre-acquire for the same
        // reason. We use the pending-meta semaphore (not open-files) because
        // rm operations don't hold fds — and rm is reachable from copy_file's
        // overwrite path, which already holds an open-files permit; using a
        // distinct semaphore avoids that cross-pool deadlock.
        let known_leaf = entry_file_type.as_ref().is_some_and(|ft| !ft.is_dir());
        let pending_guard = if known_leaf {
            Some(throttle::pending_meta_permit().await)
        } else {
            None
        };
        let do_rm = || async move {
            let _pending_guard = pending_guard;
            rm_internal(prog_track, &entry_path, &source_root, &settings).await
        };
        join_set.spawn(do_rm());
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(entries);
    let mut rm_summary = Summary {
        directories_removed: 0,
        files_skipped: skipped_files,
        symlinks_skipped: skipped_symlinks,
        directories_skipped: skipped_dirs,
        ..Default::default()
    };
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(result) => match result {
                Ok(summary) => rm_summary = rm_summary + summary,
                Err(error) => {
                    tracing::error!("remove: {:?} failed with: {:#}", path, &error);
                    rm_summary = rm_summary + error.summary;
                    errors.push(error.source);
                    if settings.fail_early {
                        break;
                    }
                }
            },
            Err(error) => {
                errors.push(error.into());
                if settings.fail_early {
                    break;
                }
            }
        }
    }
    if errors.has_errors() {
        // unwrap is safe: has_errors() guarantees into_error() returns Some
        return Err(Error::new(errors.into_error().unwrap(), rm_summary));
    }
    tracing::debug!("finally remove the empty directory");
    let anything_removed = rm_summary.files_removed > 0
        || rm_summary.symlinks_removed > 0
        || rm_summary.directories_removed > 0;
    let anything_skipped = rm_summary.files_skipped > 0
        || rm_summary.symlinks_skipped > 0
        || rm_summary.directories_skipped > 0;
    // a directory is "traversed only" when include filters are active, nothing was removed
    // from it, and the directory itself doesn't directly match an include pattern. such
    // directories were only entered to search for matching content inside and should be
    // left intact. directories that directly match an include pattern (e.g. --include target/)
    // should be removed even if empty. exclude-only filters never produce traversed-only
    // directories because directly_matches_include returns true when no includes exist.
    let relative_path = path.strip_prefix(source_root).unwrap_or(path);
    let traversed_only = !anything_removed
        && settings
            .filter
            .as_ref()
            .is_some_and(|f| f.has_includes() && !f.directly_matches_include(relative_path, true));
    // evaluate the directory's own time filter to decide whether to remove it.
    // the time filter is an entry filter, not a subtree gate: children are already handled
    // by their own recursive calls, so this decision only controls the final remove_dir.
    // returns Ok(true) = proceed, Ok(false) = skip (too new), Err propagates a fail-early.
    // the src_metadata captured at entry is used so rrm's own mutations during traversal
    // don't change the answer.
    let dir_passes_time_filter: bool = if let Some(ref time_filter) = settings.time_filter {
        match time_filter.matches(&src_metadata) {
            Ok(result) => match result.as_skip_reason() {
                Some(reason) => {
                    if let Some(mode) = settings.dry_run {
                        crate::dry_run::report_time_skip(path, reason, mode, "dir");
                    }
                    false
                }
                None => true,
            },
            Err(err) => {
                let err = err.context(format!("failed evaluating time filter on {:?}", &path));
                if settings.fail_early {
                    return Err(Error::new(err, rm_summary));
                }
                // log and skip — never remove a directory whose age we cannot verify.
                // btime being unsupported on the filesystem is expected noise; downgrade
                // to warn. anything else is unexpected and stays at error.
                if is_unsupported_io_error(&err) {
                    tracing::warn!(
                        "time filter evaluation unsupported for dir {:?}, leaving it intact: {:#}",
                        &path,
                        &err
                    );
                } else {
                    tracing::error!(
                        "time filter evaluation failed for dir {:?}, leaving it intact: {:#}",
                        &path,
                        &err
                    );
                }
                false
            }
        }
    } else {
        true
    };
    // handle dry-run mode for directories.
    // `traversed_only` catches dirs only entered to search for include pattern matches.
    // `anything_skipped` catches dirs that would still have content after partial removal.
    // `!dir_passes_time_filter` catches dirs whose own timestamps disqualify removal.
    // the real-mode path below only needs `traversed_only` and `!dir_passes_time_filter`
    // because the subsequent `remove_dir` call handles the non-empty case via ENOTEMPTY.
    if settings.dry_run.is_some() {
        if traversed_only || anything_skipped || !dir_passes_time_filter {
            tracing::debug!(
                "dry-run: directory {:?} would not be removed (removed={}, skipped={}, time_ok={})",
                &path,
                anything_removed,
                anything_skipped,
                dir_passes_time_filter
            );
            if !dir_passes_time_filter {
                prog_track.directories_skipped.inc();
                rm_summary.directories_skipped += 1;
            }
        } else {
            crate::dry_run::report_action("remove", path, None, "dir");
            rm_summary.directories_removed += 1;
        }
        return Ok(rm_summary);
    }
    // skip directories that were only traversed to look for include matches.
    // not needed for exclude-only filters or directly-matched directories.
    // non-empty directories are handled by the ENOTEMPTY check below.
    if traversed_only {
        tracing::debug!(
            "directory {:?} had nothing removed, leaving it intact",
            &path
        );
        return Ok(rm_summary);
    }
    // skip directories whose own timestamps don't satisfy the time filter.
    // children have already been processed; this only gates the dir's own removal.
    if !dir_passes_time_filter {
        tracing::debug!(
            "directory {:?} skipped by time filter, leaving it intact",
            &path
        );
        prog_track.directories_skipped.inc();
        rm_summary.directories_skipped += 1;
        return Ok(rm_summary);
    }
    // when filtering is active, directories may not be empty because we only removed
    // matching files (includes) or skipped excluded files; use remove_dir (not remove_dir_all)
    // so non-empty directories fail gracefully with ENOTEMPTY
    let any_filter_active = settings.filter.is_some() || settings.time_filter.is_some();
    match crate::walk::run_metadata_probed(
        congestion::Side::Destination,
        tokio::fs::remove_dir(path),
    )
    .await
    {
        Ok(()) => {
            prog_track.directories_removed.inc();
            rm_summary.directories_removed += 1;
        }
        Err(err) if any_filter_active => {
            // with filtering, it's expected that directories may not be empty because we only
            // removed matching files; raw_os_error 39 is ENOTEMPTY on Linux. this is not an
            // error — surface it at info so users can see which directories survived.
            if err.kind() == std::io::ErrorKind::DirectoryNotEmpty || err.raw_os_error() == Some(39)
            {
                tracing::info!(
                    "directory {:?} not empty after filtering, leaving it intact",
                    &path
                );
            } else {
                return Err(Error::new(
                    anyhow!(err).context(format!("failed removing directory {:?}", &path)),
                    rm_summary,
                ));
            }
        }
        Err(err) => {
            return Err(Error::new(
                anyhow!(err).context(format!("failed removing directory {:?}", &path)),
                rm_summary,
            ));
        }
    }
    Ok(rm_summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DryRunMode;
    use crate::testutils;
    use tracing_test::traced_test;

    static PROGRESS: std::sync::LazyLock<progress::Progress> =
        std::sync::LazyLock::new(progress::Progress::new);

    #[tokio::test]
    #[traced_test]
    async fn no_write_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let filepaths = vec![
            test_path.join("foo").join("0.txt"),
            test_path.join("foo").join("bar").join("2.txt"),
            test_path.join("foo").join("baz").join("4.txt"),
            test_path.join("foo").join("baz"),
        ];
        for fpath in &filepaths {
            // change file permissions to not readable and not writable
            tokio::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o555)).await?;
        }
        let summary = rm(
            &PROGRESS,
            &test_path.join("foo"),
            &Settings {
                fail_early: false,
                filter: None,
                dry_run: None,
                time_filter: None,
            },
        )
        .await?;
        assert!(!test_path.join("foo").exists());
        assert_eq!(summary.files_removed, 5);
        assert_eq!(summary.symlinks_removed, 2);
        assert_eq!(summary.directories_removed, 3);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn parent_dir_no_write_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // make parent directory read-only (no write permission)
        tokio::fs::set_permissions(
            &test_path.join("foo").join("bar"),
            std::fs::Permissions::from_mode(0o555),
        )
        .await?;
        let result = rm(
            &PROGRESS,
            &test_path.join("foo").join("bar").join("2.txt"),
            &Settings {
                fail_early: true,
                filter: None,
                dry_run: None,
                time_filter: None,
            },
        )
        .await;
        // should fail with permission denied error
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_string = format!("{:#}", err);
        // verify the error chain includes "Permission denied"
        assert!(
            err_string.contains("Permission denied") || err_string.contains("permission denied"),
            "Error should contain 'Permission denied' but got: {}",
            err_string
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
            // create filter that should only remove files in bar/ directory
            let mut filter = FilterSettings::new();
            filter.add_include("bar/*.txt").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // should only remove files matching bar/*.txt pattern (bar/1.txt, bar/2.txt, bar/3.txt)
            assert_eq!(
                summary.files_removed, 3,
                "should remove 3 files matching bar/*.txt"
            );
            // each file is 1 byte ("1", "2", "3")
            assert_eq!(summary.bytes_removed, 3, "should report 3 bytes removed");
            // verify the right files were removed
            assert!(
                !test_path.join("foo/bar/1.txt").exists(),
                "bar/1.txt should be removed"
            );
            assert!(
                !test_path.join("foo/bar/2.txt").exists(),
                "bar/2.txt should be removed"
            );
            assert!(
                !test_path.join("foo/bar/3.txt").exists(),
                "bar/3.txt should be removed"
            );
            // verify files outside the pattern still exist
            assert!(
                test_path.join("foo/0.txt").exists(),
                "0.txt should still exist"
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
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo/0.txt"), // single file source
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // the file should NOT be removed because it matches the exclude pattern
            assert_eq!(
                summary.files_removed, 0,
                "file matching exclude pattern should not be removed"
            );
            assert!(
                test_path.join("foo/0.txt").exists(),
                "excluded file should still exist"
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
            let result = rm(
                &PROGRESS,
                &test_path.join("excluded_dir"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // directory should NOT be removed because it matches exclude pattern
            assert_eq!(
                result.directories_removed, 0,
                "root directory matching exclude should not be removed"
            );
            assert!(
                test_path.join("excluded_dir").exists(),
                "excluded root directory should still exist"
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
            let result = rm(
                &PROGRESS,
                &test_path.join("excluded_link"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // symlink should NOT be removed because it matches exclude pattern
            assert_eq!(
                result.symlinks_removed, 0,
                "root symlink matching exclude should not be removed"
            );
            assert!(
                test_path.join("excluded_link").exists(),
                "excluded root symlink should still exist"
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
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // should remove: bar/1.txt, bar/3.txt = 2 files
            // should skip: bar/2.txt (excluded by pattern), 0.txt (excluded by default - no match) = 2 files
            assert_eq!(summary.files_removed, 2, "should remove 2 files");
            assert_eq!(
                summary.files_skipped, 2,
                "should skip 2 files (bar/2.txt excluded, 0.txt no match)"
            );
            // verify
            assert!(
                !test_path.join("foo/bar/1.txt").exists(),
                "bar/1.txt should be removed"
            );
            assert!(
                test_path.join("foo/bar/2.txt").exists(),
                "bar/2.txt should be excluded"
            );
            assert!(
                !test_path.join("foo/bar/3.txt").exists(),
                "bar/3.txt should be removed"
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
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // removed: 0.txt, baz/4.txt = 2 files
            // removed: baz/5.txt symlink, baz/6.txt symlink = 2 symlinks
            // removed: baz = 1 directory (foo cannot be removed because bar still exists)
            // skipped: bar directory (1 dir) - contents not counted since whole dir skipped
            assert_eq!(summary.files_removed, 2, "should remove 2 files");
            assert_eq!(summary.symlinks_removed, 2, "should remove 2 symlinks");
            assert_eq!(
                summary.directories_removed, 1,
                "should remove 1 directory (baz only, foo not empty)"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "should skip 1 directory (bar)"
            );
            // bar should still exist
            assert!(
                test_path.join("foo/bar").exists(),
                "bar directory should still exist"
            );
            // foo should still exist (not empty because bar is still there)
            assert!(
                test_path.join("foo").exists(),
                "foo directory should still exist (contains bar)"
            );
            Ok(())
        }
        /// Test that empty directories are not removed when they were only traversed to look
        /// for matches (regression test for bug where --include='foo' would remove empty dir baz).
        #[tokio::test]
        #[traced_test]
        async fn test_empty_dir_not_removed_when_only_traversed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::write(test_path.join("bar"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // only 'foo' should be removed
            assert_eq!(summary.files_removed, 1, "should remove only 'foo' file");
            assert_eq!(
                summary.directories_removed, 0,
                "should NOT remove empty 'baz' directory"
            );
            // verify foo was removed
            assert!(!test_path.join("foo").exists(), "foo should be removed");
            // verify bar still exists (not matching include pattern)
            assert!(test_path.join("bar").exists(), "bar should still exist");
            // verify empty baz directory still exists
            assert!(
                test_path.join("baz").exists(),
                "empty baz directory should NOT be removed"
            );
            Ok(())
        }
        /// Test that empty directories ARE removed with exclude-only filters.
        /// Unlike include filters (where empty dirs are only traversed for matches),
        /// exclude-only filters should not prevent removal of empty directories.
        #[tokio::test]
        #[traced_test]
        async fn test_exclude_only_removes_empty_directory() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   bar.log (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::write(test_path.join("bar.log"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // exclude only .log files
            let mut filter = FilterSettings::new();
            filter.add_exclude("*.log").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // foo should be removed, bar.log should be skipped, baz/ should be removed
            assert_eq!(summary.files_removed, 1, "should remove 'foo'");
            assert_eq!(summary.files_skipped, 1, "should skip 'bar.log'");
            assert_eq!(
                summary.directories_removed, 1,
                "should remove empty 'baz' directory"
            );
            assert!(!test_path.join("foo").exists(), "foo should be removed");
            assert!(
                test_path.join("bar.log").exists(),
                "bar.log should still exist"
            );
            assert!(
                !test_path.join("baz").exists(),
                "empty baz directory should be removed"
            );
            Ok(())
        }
        /// Test that empty directories are not removed in dry-run mode when only traversed.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_empty_dir_not_reported_as_removed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::write(test_path.join("bar"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: Some(DryRunMode::Explain),
                    time_filter: None,
                },
            )
            .await?;
            // only 'foo' should be reported as would-be-removed
            assert_eq!(
                summary.files_removed, 1,
                "should report only 'foo' would be removed"
            );
            assert_eq!(
                summary.directories_removed, 0,
                "should NOT report empty 'baz' would be removed"
            );
            // verify nothing was actually removed (dry-run mode)
            assert!(test_path.join("foo").exists(), "foo should still exist");
            assert!(test_path.join("bar").exists(), "bar should still exist");
            assert!(test_path.join("baz").exists(), "baz should still exist");
            Ok(())
        }
        /// Test that an empty directory directly matching an include pattern IS removed.
        /// Unlike traversed-only directories, directly matched ones are explicit targets.
        #[tokio::test]
        #[traced_test]
        async fn test_include_directly_matched_empty_dir_is_removed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // include pattern that directly matches the directory
            let mut filter = FilterSettings::new();
            filter.add_include("baz/").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            assert_eq!(
                summary.directories_removed, 1,
                "should remove directly matched empty 'baz' directory"
            );
            assert_eq!(summary.files_removed, 0, "should not remove 'foo'");
            assert!(test_path.join("foo").exists(), "foo should still exist");
            assert!(
                !test_path.join("baz").exists(),
                "directly matched empty baz directory should be removed"
            );
            Ok(())
        }
    }
    mod dry_run_tests {
        use super::*;
        use crate::filter::FilterSettings;
        /// Test that dry-run mode doesn't modify permissions on read-only directories.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_preserves_readonly_permissions() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let readonly_dir = test_path.join("foo/bar");
            // make the directory read-only
            tokio::fs::set_permissions(&readonly_dir, std::fs::Permissions::from_mode(0o555))
                .await?;
            // verify it's read-only
            let before_mode = tokio::fs::metadata(&readonly_dir)
                .await?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                before_mode, 0o555,
                "directory should be read-only before dry-run"
            );
            let summary = rm(
                &PROGRESS,
                &readonly_dir,
                &Settings {
                    fail_early: false,
                    filter: None,
                    dry_run: Some(DryRunMode::Brief),
                    time_filter: None,
                },
            )
            .await?;
            // verify the directory still exists (dry-run shouldn't remove it)
            assert!(
                readonly_dir.exists(),
                "directory should still exist after dry-run"
            );
            // verify permissions weren't changed
            let after_mode = tokio::fs::metadata(&readonly_dir)
                .await?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                after_mode, 0o555,
                "dry-run should not modify directory permissions"
            );
            // verify summary shows what would be removed
            assert!(
                summary.directories_removed > 0 || summary.files_removed > 0,
                "dry-run should report what would be removed"
            );
            Ok(())
        }
        /// Test that dry-run mode with filtering correctly handles directories that
        /// wouldn't be empty after filtering.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_with_filter_non_empty_directory() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // test structure from setup_test_dir:
            // foo/
            //   0.txt
            //   bar/ (1.txt, 2.txt, 3.txt)
            //   baz/ (4.txt, 5.txt symlink, 6.txt symlink)
            // exclude bar/ - so foo would not be empty after removing (bar still there)
            let mut filter = crate::filter::FilterSettings::new();
            filter.add_exclude("bar/").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: Some(DryRunMode::Brief),
                    time_filter: None,
                },
            )
            .await?;
            // dry-run shouldn't actually remove anything
            assert!(
                test_path.join("foo").exists(),
                "foo should still exist after dry-run"
            );
            // verify summary reflects what WOULD happen:
            // - files: 0.txt, baz/4.txt would be removed = 2
            // - symlinks: baz/5.txt, baz/6.txt would be removed = 2
            // - directories: baz would be removed, but NOT foo (bar is skipped, so foo not empty)
            // - skipped: bar directory = 1
            assert_eq!(
                summary.files_removed, 2,
                "should report 2 files would be removed"
            );
            assert_eq!(
                summary.symlinks_removed, 2,
                "should report 2 symlinks would be removed"
            );
            assert_eq!(
                summary.directories_removed, 1,
                "should report only baz (not foo) would be removed"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "should report bar directory skipped"
            );
            Ok(())
        }
        /// Test that dry-run with exclude-only filter correctly reports empty directories
        /// as would-be-removed (unlike include filters where empty dirs are only traversed).
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_exclude_only_reports_empty_dir_removed() -> Result<(), anyhow::Error>
        {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   bar.log (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::write(test_path.join("bar.log"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // exclude only .log files
            let mut filter = FilterSettings::new();
            filter.add_exclude("*.log").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: Some(DryRunMode::Explain),
                    time_filter: None,
                },
            )
            .await?;
            // foo should be reported as would-be-removed, bar.log skipped, baz/ removed
            assert_eq!(
                summary.files_removed, 1,
                "should report 'foo' would be removed"
            );
            assert_eq!(
                summary.files_skipped, 1,
                "should report 'bar.log' would be skipped"
            );
            assert_eq!(
                summary.directories_removed, 1,
                "should report empty 'baz' directory would be removed"
            );
            // verify nothing was actually removed (dry-run mode)
            assert!(test_path.join("foo").exists(), "foo should still exist");
            assert!(
                test_path.join("bar.log").exists(),
                "bar.log should still exist"
            );
            assert!(test_path.join("baz").exists(), "baz should still exist");
            Ok(())
        }
        /// Test that dry-run correctly reports removal of an empty directory that directly
        /// matches an include pattern (not merely traversed).
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_include_directly_matched_empty_dir_reported()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // include pattern that directly matches the directory
            let mut filter = FilterSettings::new();
            filter.add_include("baz/").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: Some(DryRunMode::Explain),
                    time_filter: None,
                },
            )
            .await?;
            assert_eq!(
                summary.directories_removed, 1,
                "should report directly matched empty 'baz' would be removed"
            );
            assert_eq!(summary.files_removed, 0, "should not report 'foo'");
            // verify nothing was actually removed (dry-run mode)
            assert!(test_path.join("foo").exists(), "foo should still exist");
            assert!(test_path.join("baz").exists(), "baz should still exist");
            Ok(())
        }
    }
    mod time_filter_tests {
        use super::*;
        use crate::filter::TimeFilter;

        fn set_mtime_age(path: &std::path::Path, age: std::time::Duration) -> anyhow::Result<()> {
            let past = filetime::FileTime::from_system_time(std::time::SystemTime::now() - age);
            filetime::set_file_mtime(path, past)?;
            Ok(())
        }

        /// File with mtime older than threshold is removed.
        #[tokio::test]
        #[traced_test]
        async fn removes_files_older_than_modified_before() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let file = test_path.join("old.txt");
            tokio::fs::write(&file, "x").await?;
            set_mtime_age(&file, std::time::Duration::from_secs(7200))?;
            // age test_path so the root dir passes its own time filter check
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            assert_eq!(summary.files_removed, 1, "old file should be removed");
            assert_eq!(summary.files_skipped, 0);
            assert!(!file.exists(), "old.txt should be removed");
            Ok(())
        }

        /// File with mtime newer than threshold is skipped.
        #[tokio::test]
        #[traced_test]
        async fn keeps_files_newer_than_modified_before() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let file = test_path.join("new.txt");
            tokio::fs::write(&file, "x").await?;
            set_mtime_age(&file, std::time::Duration::from_secs(60))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            assert_eq!(summary.files_removed, 0, "new file should not be removed");
            assert_eq!(summary.files_skipped, 1, "new file should be skipped");
            assert!(file.exists(), "new.txt should still exist");
            Ok(())
        }

        /// A fresh subdirectory is descended into (children are handled individually),
        /// but the fresh_dir itself is not removed because its own mtime is too recent.
        #[tokio::test]
        #[traced_test]
        async fn fresh_subdirectory_is_descended_but_not_removed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_file = test_path.join("old.txt");
            let fresh_dir = test_path.join("fresh_dir");
            let fresh_child = fresh_dir.join("fresh_child.txt");
            let old_child = fresh_dir.join("old_child.txt");
            tokio::fs::write(&old_file, "x").await?;
            tokio::fs::create_dir(&fresh_dir).await?;
            tokio::fs::write(&fresh_child, "x").await?;
            tokio::fs::write(&old_child, "x").await?;
            set_mtime_age(&old_file, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&old_child, std::time::Duration::from_secs(7200))?;
            // fresh_child keeps its recent mtime; so does fresh_dir (we took the mtime
            // snapshot before remove_file mutates it)
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            // we descend into fresh_dir: old_child removed, fresh_child skipped
            assert_eq!(summary.files_removed, 2, "old.txt and old_child removed");
            assert_eq!(
                summary.files_skipped, 1,
                "fresh_child skipped inside fresh_dir"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "fresh_dir itself is skipped at removal time"
            );
            assert_eq!(
                summary.directories_removed, 0,
                "root survives because fresh_dir is still inside it"
            );
            assert!(!old_file.exists());
            assert!(!old_child.exists(), "old_child inside fresh_dir removed");
            assert!(
                fresh_dir.exists(),
                "fresh_dir kept despite its old child being removed"
            );
            assert!(fresh_child.exists(), "fresh_child inside fresh_dir kept");
            Ok(())
        }

        /// An old directory that still holds a new (skipped) file survives as non-empty.
        /// The leftover-dir case is not treated as an error.
        #[tokio::test]
        #[traced_test]
        async fn old_dir_with_new_file_leaves_non_empty_dir_without_error()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_dir = test_path.join("old_dir");
            tokio::fs::create_dir(&old_dir).await?;
            let new_file = old_dir.join("new.txt");
            tokio::fs::write(&new_file, "x").await?;
            set_mtime_age(&new_file, std::time::Duration::from_secs(60))?;
            set_mtime_age(&old_dir, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let result = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await;
            let summary = result.expect("ENOTEMPTY should not surface as an error");
            assert_eq!(summary.files_skipped, 1, "new file should be skipped");
            assert_eq!(
                summary.directories_removed, 0,
                "old_dir cannot be removed while new.txt remains"
            );
            assert!(old_dir.exists(), "old_dir should still exist");
            assert!(new_file.exists(), "new.txt should still exist");
            // the 'left intact' message is logged at info level
            assert!(
                logs_contain("not empty after filtering, leaving it intact"),
                "should log ENOTEMPTY case at info"
            );
            Ok(())
        }

        /// An old, already-empty directory is removed by the time filter run.
        #[tokio::test]
        #[traced_test]
        async fn old_empty_directory_is_removed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_empty = test_path.join("old_empty");
            tokio::fs::create_dir(&old_empty).await?;
            set_mtime_age(&old_empty, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            // both old_empty and test_path itself are removed
            assert_eq!(summary.directories_removed, 2);
            assert!(!old_empty.exists());
            assert!(!test_path.exists());
            Ok(())
        }

        /// Time filter combines with glob exclude — both must pass for removal.
        #[tokio::test]
        #[traced_test]
        async fn time_filter_combines_with_glob_exclude() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_keep = test_path.join("keep.log");
            let old_drop = test_path.join("drop.txt");
            let new_drop = test_path.join("recent.txt");
            tokio::fs::write(&old_keep, "x").await?;
            tokio::fs::write(&old_drop, "x").await?;
            tokio::fs::write(&new_drop, "x").await?;
            set_mtime_age(&old_keep, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&old_drop, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&new_drop, std::time::Duration::from_secs(60))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let mut filter = crate::filter::FilterSettings::new();
            filter.add_exclude("*.log").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            // only old_drop passes both filters
            assert_eq!(summary.files_removed, 1, "only old_drop should be removed");
            assert_eq!(
                summary.files_skipped, 2,
                "old_keep and recent_drop should be skipped"
            );
            assert!(
                old_keep.exists(),
                "keep.log excluded by glob, should remain"
            );
            assert!(!old_drop.exists(), "drop.txt should be removed");
            assert!(new_drop.exists(), "recent.txt should remain (too new)");
            Ok(())
        }

        /// Dry-run with time filter previews removal without modifying files.
        #[tokio::test]
        #[traced_test]
        async fn time_filter_with_dry_run() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_file = test_path.join("old.txt");
            let new_file = test_path.join("new.txt");
            tokio::fs::write(&old_file, "x").await?;
            tokio::fs::write(&new_file, "x").await?;
            set_mtime_age(&old_file, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&new_file, std::time::Duration::from_secs(60))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: Some(DryRunMode::Explain),
                },
            )
            .await?;
            assert_eq!(
                summary.files_removed, 1,
                "should report old file would be removed"
            );
            assert_eq!(
                summary.files_skipped, 1,
                "should report new file would be skipped"
            );
            assert!(old_file.exists(), "old.txt should still exist (dry-run)");
            assert!(new_file.exists(), "new.txt should still exist (dry-run)");
            Ok(())
        }

        /// A fresh top-level directory is traversed (its old children are removed),
        /// but the root itself is not removed because its own mtime is too recent.
        #[tokio::test]
        #[traced_test]
        async fn fresh_top_level_directory_is_traversed_but_not_removed()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_inside = test_path.join("old.txt");
            tokio::fs::write(&old_inside, "x").await?;
            set_mtime_age(&old_inside, std::time::Duration::from_secs(7200))?;
            // test_path itself is left fresh (recent mtime)
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            assert_eq!(
                summary.files_removed, 1,
                "old child should be removed despite fresh parent"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "fresh root itself is skipped at removal time"
            );
            assert_eq!(
                summary.directories_removed, 0,
                "fresh root must not be removed"
            );
            assert!(test_path.exists(), "fresh root should still exist");
            assert!(!old_inside.exists(), "old child should be gone");
            Ok(())
        }

        /// Time filter on a single-file root argument increments skip when too new.
        #[tokio::test]
        #[traced_test]
        async fn time_filter_on_root_file_argument() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let new_file = test_path.join("new.txt");
            tokio::fs::write(&new_file, "x").await?;
            set_mtime_age(&new_file, std::time::Duration::from_secs(60))?;
            let summary = rm(
                &PROGRESS,
                &new_file,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            assert_eq!(summary.files_removed, 0);
            assert_eq!(
                summary.files_skipped, 1,
                "root file too new should be skipped"
            );
            assert!(new_file.exists(), "root file should still exist");
            Ok(())
        }
    }

    /// Stress tests exercising max-open-files saturation during rm.
    mod max_open_files_tests {
        use super::*;

        /// wide rm: many files with a very low open-files limit.
        /// verifies all files are removed correctly under permit saturation.
        #[tokio::test]
        #[traced_test]
        async fn wide_rm_under_open_files_saturation() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let file_count = 200;
            for i in 0..file_count {
                tokio::fs::write(
                    test_path.join(format!("{}.txt", i)),
                    format!("content-{}", i),
                )
                .await?;
            }
            // set a very low limit to force permit contention
            throttle::set_max_open_files(4);
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: true,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            assert_eq!(summary.files_removed, file_count);
            assert_eq!(summary.directories_removed, 1);
            assert!(!test_path.exists());
            Ok(())
        }

        /// deep + wide rm: directory tree deeper than the open-files limit, with files
        /// at every level. verifies no deadlock occurs (directories don't consume permits).
        #[tokio::test]
        #[traced_test]
        async fn deep_tree_no_deadlock_under_open_files_saturation() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let depth = 20;
            let files_per_level = 5;
            let limit = 4;
            // create a directory chain deeper than the permit limit, with files at each level
            let mut dir = test_path.clone();
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
                rm(
                    &PROGRESS,
                    &test_path,
                    &Settings {
                        fail_early: true,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                ),
            )
            .await
            .context("rm timed out — possible deadlock")?
            .context("rm failed")?;
            assert_eq!(summary.files_removed, depth * files_per_level);
            assert_eq!(summary.directories_removed, depth);
            assert!(!test_path.exists());
            Ok(())
        }

        /// Locks down the boolean used at the rm spawn site to decide whether
        /// to pre-acquire a pending-meta permit. A naive `entry_is_dir = false
        /// ⇒ pre-acquire` policy treats unknown-typed entries (when
        /// `DirEntry::file_type()` fails) as leaves, so the spawned task
        /// holds the permit even if the entry is actually a directory and
        /// recurses. A chain of such entries can deadlock the pool. The
        /// safer pattern — `pre-acquire iff positively-known-not-directory`
        /// — keeps the predicate `false` for unknown types.
        #[test]
        fn pre_acquire_skips_unknown_filetype() -> Result<(), anyhow::Error> {
            let tmp = std::env::temp_dir().join(format!(
                "rcp_pre_acquire_test_{}_{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::create_dir(&tmp)?;
            let dir_path = tmp.join("d");
            std::fs::create_dir(&dir_path)?;
            let file_path = tmp.join("f");
            std::fs::write(&file_path, "x")?;
            let dir_ft = std::fs::metadata(&dir_path)?.file_type();
            let file_ft = std::fs::metadata(&file_path)?.file_type();
            // The exact predicate used in the rm spawn site:
            let known_leaf =
                |ft: Option<std::fs::FileType>| ft.as_ref().is_some_and(|t| !t.is_dir());
            assert!(!known_leaf(None), "unknown filetype must skip pre-acquire");
            assert!(!known_leaf(Some(dir_ft)), "directory must skip pre-acquire");
            assert!(known_leaf(Some(file_ft)), "regular file must pre-acquire");
            std::fs::remove_dir_all(&tmp).ok();
            Ok(())
        }
    }
}
