use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use std::os::unix::fs::PermissionsExt;
use tracing::instrument;

use crate::config::DryRunMode;
use crate::filter::{FilterResult, FilterSettings};
use crate::progress;

/// Error type for remove operations that preserves operation summary even on failure.
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
    fn new(source: anyhow::Error, summary: Summary) -> Self {
        Error { source, summary }
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub fail_early: bool,
    /// filter settings for include/exclude patterns
    pub filter: Option<crate::filter::FilterSettings>,
    /// dry-run mode for previewing operations
    pub dry_run: Option<crate::config::DryRunMode>,
}

/// Reports a dry-run action for remove operations
fn report_dry_run_rm(path: &std::path::Path, entry_type: &str) {
    println!("would remove {} {:?}", entry_type, path);
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

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Summary {
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
            "files removed: {}\n\
            symlinks removed: {}\n\
            directories removed: {}\n\
            files skipped: {}\n\
            symlinks skipped: {}\n\
            directories skipped: {}\n",
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
#[instrument(skip(prog_track))]
pub async fn rm(
    prog_track: &'static progress::Progress,
    path: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    // check filter for top-level path (files, directories, and symlinks)
    if let Some(ref filter) = settings.filter {
        let path_name = path.file_name().map(std::path::Path::new);
        if let Some(name) = path_name {
            let path_metadata = tokio::fs::symlink_metadata(path)
                .await
                .with_context(|| format!("failed reading metadata from {:?}", &path))
                .map_err(|err| Error::new(err, Default::default()))?;
            let is_dir = path_metadata.is_dir();
            let result = filter.should_include_root_item(name, is_dir);
            match result {
                crate::filter::FilterResult::Included => {}
                result => {
                    if let Some(mode) = settings.dry_run {
                        let entry_type = if path_metadata.is_dir() {
                            "directory"
                        } else if path_metadata.file_type().is_symlink() {
                            "symlink"
                        } else {
                            "file"
                        };
                        report_dry_run_skip(path, &result, mode, entry_type);
                    }
                    // return summary with skipped count
                    let skipped_summary = if path_metadata.is_dir() {
                        Summary {
                            directories_skipped: 1,
                            ..Default::default()
                        }
                    } else if path_metadata.file_type().is_symlink() {
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
    rm_internal(prog_track, path, path, settings).await
}
#[instrument(skip(prog_track))]
#[async_recursion]
async fn rm_internal(
    prog_track: &'static progress::Progress,
    path: &std::path::Path,
    source_root: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    let _ops_guard = prog_track.ops.guard();
    tracing::debug!("read path metadata");
    let src_metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &path))
        .map_err(|err| Error::new(err, Default::default()))?;
    if !src_metadata.is_dir() {
        tracing::debug!("not a directory, just remove");
        // handle dry-run mode for files/symlinks
        if settings.dry_run.is_some() {
            let entry_type = if src_metadata.file_type().is_symlink() {
                "symlink"
            } else {
                "file"
            };
            report_dry_run_rm(path, entry_type);
            return Ok(Summary {
                files_removed: if src_metadata.file_type().is_symlink() {
                    0
                } else {
                    1
                },
                symlinks_removed: if src_metadata.file_type().is_symlink() {
                    1
                } else {
                    0
                },
                ..Default::default()
            });
        }
        tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("failed removing {:?}", &path))
            .map_err(|err| Error::new(err, Default::default()))?;
        if src_metadata.file_type().is_symlink() {
            prog_track.symlinks_removed.inc();
            return Ok(Summary {
                symlinks_removed: 1,
                ..Default::default()
            });
        }
        prog_track.files_removed.inc();
        return Ok(Summary {
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
    let mut success = true;
    let mut skipped_files = 0;
    let mut skipped_symlinks = 0;
    let mut skipped_dirs = 0;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &path))
        .map_err(|err| Error::new(err, Default::default()))?
    {
        // it's better to await the token here so that we throttle the syscalls generated by the
        // DirEntry call. the ops-throttle will never cause a deadlock (unlike max-open-files limit)
        // so it's safe to do here.
        throttle::get_ops_token().await;
        let entry_path = entry.path();
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
            // increment skipped counters - will be added to rm_summary below
            if entry_is_dir {
                skipped_dirs += 1;
            } else if entry_is_symlink {
                skipped_symlinks += 1;
            } else {
                skipped_files += 1;
            }
            continue;
        }
        let settings = settings.clone();
        let source_root = source_root.to_owned();
        let do_rm =
            || async move { rm_internal(prog_track, &entry_path, &source_root, &settings).await };
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
        match res.map_err(|err| Error::new(err.into(), Default::default()))? {
            Ok(summary) => rm_summary = rm_summary + summary,
            Err(error) => {
                tracing::error!("remove: {:?} failed with: {:#}", path, &error);
                rm_summary = rm_summary + error.summary;
                if settings.fail_early {
                    return Err(Error::new(error.source, rm_summary));
                }
                success = false;
            }
        }
    }
    if !success {
        return Err(Error::new(anyhow!("rm: {:?} failed!", &path), rm_summary));
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
    // handle dry-run mode for directories.
    // `traversed_only` catches dirs only entered to search for include pattern matches.
    // `anything_skipped` catches dirs that would still have content after partial removal
    // (applies to both include and exclude filters).
    // the real-mode path below only needs `traversed_only` because the subsequent `remove_dir`
    // call handles the non-empty case via ENOTEMPTY.
    if settings.dry_run.is_some() {
        if traversed_only || anything_skipped {
            tracing::debug!(
                "dry-run: directory {:?} would not be removed (removed={}, skipped={})",
                &path,
                anything_removed,
                anything_skipped
            );
        } else {
            report_dry_run_rm(path, "dir");
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
    // when filtering is active, directories may not be empty because we only removed
    // matching files (includes) or skipped excluded files; use remove_dir (not remove_dir_all)
    // so non-empty directories fail gracefully with ENOTEMPTY
    match tokio::fs::remove_dir(path).await {
        Ok(()) => {
            prog_track.directories_removed.inc();
            rm_summary.directories_removed += 1;
        }
        Err(err) if settings.filter.is_some() => {
            // with filtering, it's expected that directories may not be empty because we only
            // removed matching files; raw_os_error 39 is ENOTEMPTY on Linux
            if err.kind() == std::io::ErrorKind::DirectoryNotEmpty || err.raw_os_error() == Some(39)
            {
                tracing::debug!(
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
    use crate::testutils;
    use tracing_test::traced_test;

    lazy_static! {
        static ref PROGRESS: progress::Progress = progress::Progress::new();
    }

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
                },
            )
            .await?;
            // should only remove files matching bar/*.txt pattern (bar/1.txt, bar/2.txt, bar/3.txt)
            assert_eq!(
                summary.files_removed, 3,
                "should remove 3 files matching bar/*.txt"
            );
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
        async fn test_dry_run_include_directly_matched_empty_dir_reported(
        ) -> Result<(), anyhow::Error> {
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
}
