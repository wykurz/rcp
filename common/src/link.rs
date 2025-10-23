use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use std::os::linux::fs::MetadataExt as LinuxMetadataExt;
use tracing::instrument;

use crate::copy;
use crate::copy::{Settings as CopySettings, Summary as CopySummary};
use crate::filecmp;
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
/// tracing::error!("operation failed: {:#}", &error);  // ✅ Shows full chain
/// tracing::error!("operation failed: {}", &error);    // ❌ Loses root cause
/// ```
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
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

#[derive(Debug, Copy, Clone)]
pub struct Settings {
    pub copy_settings: CopySettings,
    pub update_compare: filecmp::MetadataCmpSettings,
    pub update_exclusive: bool,
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
                },
            )
            .await
            .map_err(|err| {
                let rm_summary = err.summary;
                link_summary.copy_summary.rm_summary = rm_summary;
                Error::new(anyhow::Error::msg(err), link_summary)
            })?;
            link_summary.copy_summary.rm_summary = rm_summary;
            tokio::fs::hard_link(src, dst)
                .await
                .map_err(|err| Error::new(anyhow::Error::msg(err), link_summary))?;
        }
    }
    prog_track.hard_links_created.inc();
    link_summary.hard_links_created = 1;
    Ok(link_summary)
}

#[instrument(skip(prog_track))]
#[async_recursion]
pub async fn link(
    prog_track: &'static progress::Progress,
    cwd: &std::path::Path,
    src: &std::path::Path,
    dst: &std::path::Path,
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
    let copy_summary = {
        if let Err(error) = tokio::fs::create_dir(dst).await {
            assert!(!is_fresh, "unexpected error creating directory: {:?}", &dst);
            if settings.copy_settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists
            {
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
                        },
                    )
                    .await
                    .map_err(|err| {
                        let rm_summary = err.summary;
                        copy_summary.rm_summary = rm_summary;
                        Error::new(
                            anyhow::Error::msg(err),
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
                                anyhow::Error::msg(err),
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
        }
    };
    let mut link_summary = Summary {
        copy_summary,
        ..Default::default()
    };
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
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
        processed_files.insert(entry_name.to_owned());
        let dst_path = dst.join(entry_name);
        let update_path = update.as_ref().map(|s| s.join(entry_name));
        let settings = *settings;
        let do_link = || async move {
            link(
                prog_track,
                &cwd_path,
                &entry_path,
                &dst_path,
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
            let settings = *settings;
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
                    success = false;
                }
            },
            Err(error) => {
                if settings.copy_settings.fail_early {
                    return Err(Error::new(anyhow::Error::msg(error), link_summary));
                }
            }
        }
    }
    if !success {
        return Err(Error::new(
            anyhow!("link: {:?} {:?} -> {:?} failed!", src, update, dst),
            link_summary,
        ))?;
    }
    tracing::debug!("set 'dst' directory metadata");
    let preserve_metadata = if let Some(update_metadata) = update_metadata_opt.as_ref() {
        update_metadata
    } else {
        &src_metadata
    };
    preserve::set_dir_metadata(&RLINK_PRESERVE_SETTINGS, preserve_metadata, dst)
        .await
        .map_err(|err| Error::new(err, link_summary))?;
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
            },
            update_compare: filecmp::MetadataCmpSettings {
                size: true,
                mtime: true,
                ..Default::default()
            },
            update_exclusive: false,
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
                &rm::Settings { fail_early: false },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &rm::Settings { fail_early: false },
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
                &rm::Settings { fail_early: false },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &rm::Settings { fail_early: false },
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
                &rm::Settings { fail_early: false },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &bar_path.join("2.txt"),
                    &rm::Settings { fail_early: false },
                )
                .await?
                + rm::rm(
                    &PROGRESS,
                    &bar_path.join("3.txt"),
                    &rm::Settings { fail_early: false },
                )
                .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz"),
                    &rm::Settings { fail_early: false },
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
                &rm::Settings { fail_early: false },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &bar_path.join("2.txt"),
                    &rm::Settings { fail_early: false },
                )
                .await?
                + rm::rm(
                    &PROGRESS,
                    &bar_path.join("3.txt"),
                    &rm::Settings { fail_early: false },
                )
                .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz"),
                    &rm::Settings { fail_early: false },
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
}
