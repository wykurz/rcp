use anyhow::{Context, Result};
use async_recursion::async_recursion;
use std::os::linux::fs::MetadataExt as LinuxMetadataExt;
use std::os::unix::prelude::PermissionsExt;
use tracing::{event, instrument, Level};

use crate::copy;
use crate::filecmp;
use crate::progress;
use crate::rm;
use crate::CopySettings;

#[derive(Debug, Copy, Clone)]
pub struct LinkSettings {
    pub copy_settings: CopySettings,
    pub update_compare: filecmp::MetadataCmpSettings,
}

#[derive(Copy, Clone, Default)]
pub struct LinkSummary {
    pub hard_links_created: usize,
    pub hard_links_unchanged: usize,
    pub copy_summary: copy::CopySummary,
}

impl std::ops::Add for LinkSummary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            hard_links_created: self.hard_links_created + other.hard_links_created,
            hard_links_unchanged: self.hard_links_unchanged + other.hard_links_unchanged,
            copy_summary: self.copy_summary + other.copy_summary,
        }
    }
}

impl std::fmt::Display for LinkSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}hard-links created: {}\nhard links unchanged: {}",
            &self.copy_summary, self.hard_links_created, self.hard_links_unchanged
        )
    }
}

fn is_hard_link(md1: &std::fs::Metadata, md2: &std::fs::Metadata) -> bool {
    copy::is_file_type_same(md1, md2)
        && md2.st_dev() == md1.st_dev()
        && md2.st_ino() == md1.st_ino()
}

#[instrument]
async fn hard_link_helper(
    prog_track: &'static progress::TlsProgress,
    src: &std::path::Path,
    src_metadata: &std::fs::Metadata,
    dst: &std::path::Path,
    settings: &LinkSettings,
) -> Result<LinkSummary> {
    if let Err(error) = tokio::fs::hard_link(src, dst).await {
        if settings.copy_settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists {
            event!(
                Level::DEBUG,
                "'dst' already exists, check if we need to update"
            );
            let dst_metadata = tokio::fs::symlink_metadata(dst).await?;
            if is_hard_link(src_metadata, &dst_metadata) {
                event!(Level::DEBUG, "no change, leaving file as is");
                return Ok(LinkSummary {
                    hard_links_unchanged: 1,
                    ..Default::default()
                });
            }
            event!(
                Level::DEBUG,
                "'dst' file type changed, removing and hard-linking"
            );
            rm::rm(
                prog_track,
                dst,
                &rm::Settings {
                    fail_early: settings.copy_settings.fail_early,
                },
            )
            .await?;
            tokio::fs::hard_link(src, dst).await?;
        }
    }
    Ok(LinkSummary {
        hard_links_created: 1,
        ..Default::default()
    })
}

#[instrument(skip(prog_track))]
#[async_recursion]
pub async fn link(
    prog_track: &'static progress::TlsProgress,
    cwd: &std::path::Path,
    src: &std::path::Path,
    dst: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &LinkSettings,
) -> Result<LinkSummary> {
    let _guard = prog_track.guard();
    event!(Level::DEBUG, "reading source metadata");
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))?;
    let update_metadata_opt = match update {
        Some(update) => {
            event!(Level::DEBUG, "reading 'update' metadata");
            let update_metadata_res = tokio::fs::symlink_metadata(update).await;
            match update_metadata_res {
                Ok(update_metadata) => Some(update_metadata),
                Err(error) => {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        None
                    } else {
                        return Err(error).with_context(|| {
                            format!("failed reading metadata from {:?}", &update)
                        });
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
            event!(
                Level::DEBUG,
                "link: file type of {:?} ({:?}) and {:?} ({:?}) differs - copying from update",
                src,
                src_metadata.file_type(),
                update,
                update_metadata.file_type()
            );
            let copy_summary =
                copy::copy(prog_track, cwd, update, dst, &settings.copy_settings).await?;
            return Ok(LinkSummary {
                copy_summary,
                ..Default::default()
            });
        }
        if update_metadata.is_file() {
            // check if the file is unchanged and if so hard-link, otherwise copy from the updated one
            if filecmp::metadata_equal(&settings.update_compare, &src_metadata, update_metadata) {
                event!(Level::DEBUG, "no change, hard link 'src'");
                return hard_link_helper(prog_track, src, &src_metadata, dst, settings).await;
            } else {
                event!(
                    Level::DEBUG,
                    "link: {:?} metadata has changed, copying from {:?}",
                    src,
                    update
                );
                return Ok(LinkSummary {
                    copy_summary: copy::copy_file(prog_track, update, dst, &settings.copy_settings)
                        .await?,
                    ..Default::default()
                });
            }
        }
        if update_metadata.is_symlink() {
            event!(Level::DEBUG, "'update' is a symlink so just symlink that");
            // use "copy" function to handle the overwrite logic
            let copy_summary =
                copy::copy(prog_track, cwd, update, dst, &settings.copy_settings).await?;
            return Ok(LinkSummary {
                copy_summary,
                ..Default::default()
            });
        }
    } else {
        // update hasn't been specified, if this is a file just hard-link the source or symlink if it's a symlink
        event!(Level::DEBUG, "no 'update' specified");
        if src_metadata.is_file() {
            return hard_link_helper(prog_track, src, &src_metadata, dst, settings).await;
        }
        if src_metadata.is_symlink() {
            event!(Level::DEBUG, "'src' is a symlink so just symlink that");
            // use "copy" function to handle the overwrite logic
            let copy_summary =
                copy::copy(prog_track, cwd, src, dst, &settings.copy_settings).await?;
            return Ok(LinkSummary {
                copy_summary,
                ..Default::default()
            });
        }
    }
    assert!(src_metadata.is_dir());
    assert!(update_metadata_opt.is_none() || update_metadata_opt.as_ref().unwrap().is_dir());
    event!(Level::DEBUG, "process contents of 'src' directory");
    let mut src_entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {:?} for reading", src))?;
    let copy_summary = {
        if let Err(error) = tokio::fs::create_dir(dst).await {
            if settings.copy_settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists
            {
                // check if the destination is a directory - if so, leave it
                //
                // N.B. the permissions may prevent us from writing to it but the alternative is to open up the directory
                // while we're writing to it which isn't safe
                let dst_metadata = tokio::fs::metadata(dst)
                    .await
                    .with_context(|| format!("failed reading metadata from {:?}", &dst))?;
                if dst_metadata.is_dir() {
                    event!(Level::DEBUG, "'dst' is a directory, leaving it as is");
                    copy::CopySummary {
                        directories_unchanged: 1,
                        ..Default::default()
                    }
                } else {
                    event!(
                        Level::DEBUG,
                        "'dst' is not a directory, removing and creating a new one"
                    );
                    let rm_summary = rm::rm(
                        prog_track,
                        dst,
                        &rm::Settings {
                            fail_early: settings.copy_settings.fail_early,
                        },
                    )
                    .await
                    .with_context(|| format!("cannot remove conflicting path {:?}", dst))?;
                    tokio::fs::create_dir(dst)
                        .await
                        .with_context(|| format!("cannot create directory {:?}", dst))?;
                    copy::CopySummary {
                        rm_summary,
                        directories_created: 1,
                        ..Default::default()
                    }
                }
            } else {
                return Err(error).with_context(|| format!("cannot create directory {:?}", dst));
            }
        } else {
            // new directory created, no conflicts
            copy::CopySummary {
                directories_created: 1,
                ..Default::default()
            }
        }
    };
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
    // create a set of all the files we already processed
    let mut processed_files = std::collections::HashSet::new();
    // iterate through src entries and recursively call "link" on each one
    while let Some(src_entry) = src_entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &src))?
    {
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
            )
            .await
        };
        join_set.spawn(do_link());
    }
    // only process update if the path was provided and the directory is present
    if update_metadata_opt.is_some() {
        let update = update.as_ref().unwrap();
        event!(Level::DEBUG, "process contents of 'update' directory");
        let mut update_entries = tokio::fs::read_dir(update)
            .await
            .with_context(|| format!("cannot open directory {:?} for reading", &update))?;
        // iterate through update entries and for each one that's not present in src call "copy"
        while let Some(update_entry) = update_entries
            .next_entry()
            .await
            .with_context(|| format!("failed traversing directory {:?}", &update))?
        {
            let cwd_path = cwd.to_owned();
            let entry_path = update_entry.path();
            let entry_name = entry_path.file_name().unwrap();
            if processed_files.contains(entry_name) {
                // we already must have considered this file, skip it
                continue;
            }
            event!(Level::DEBUG, "found a new entry in the 'update' directory");
            let dst_path = dst.join(entry_name);
            let update_path = update.join(entry_name);
            let settings = *settings;
            let do_copy = || async move {
                let copy_summary = copy::copy(
                    prog_track,
                    &cwd_path,
                    &update_path,
                    &dst_path,
                    &settings.copy_settings,
                )
                .await?;
                Ok(LinkSummary {
                    copy_summary,
                    ..Default::default()
                })
            };
            join_set.spawn(do_copy());
        }
    }
    let mut link_summary = LinkSummary {
        copy_summary,
        ..Default::default()
    };
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => link_summary = link_summary + summary,
            Err(error) => {
                event!(
                    Level::ERROR,
                    "link: {:?} {:?} -> {:?} failed with: {}",
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
        }
    }
    if !success {
        return Err(anyhow::anyhow!(
            "link: {:?} {:?} -> {:?} failed!",
            src,
            update,
            dst
        ));
    }
    event!(Level::DEBUG, "set 'dst' directory metadata");
    let preserve_metadata = if let Some(update_metadata) = update_metadata_opt.as_ref() {
        update_metadata
    } else {
        &src_metadata
    };
    let permissions = if settings.copy_settings.preserve {
        preserve_metadata.permissions()
    } else {
        // remove sticky bit, setuid and setgid from permissions to mimic behavior of cp
        std::fs::Permissions::from_mode(preserve_metadata.permissions().mode() & 0o0777)
    };
    tokio::fs::set_permissions(dst, permissions.clone())
        .await
        .with_context(|| format!("cannot set {:?} permissions to {:?}", &dst, &permissions))?;
    if settings.copy_settings.preserve {
        copy::set_owner_and_time(dst, preserve_metadata).await?;
    }
    Ok(link_summary)
}

#[cfg(test)]
mod link_tests {
    use crate::testutils;
    use tracing_test::traced_test;

    use super::*;

    lazy_static! {
        static ref PROGRESS: progress::TlsProgress = progress::TlsProgress::new();
    }

    fn common_settings(preserve: bool, dereference: bool, overwrite: bool) -> LinkSettings {
        LinkSettings {
            copy_settings: copy::CopySettings {
                preserve,
                read_buffer: 10,
                dereference,
                fail_early: false,
                overwrite,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
            },
            update_compare: filecmp::MetadataCmpSettings {
                size: true,
                mtime: true,
                ..Default::default()
            },
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_basic_link() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &None,
            &common_settings(true, false, false),
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
    async fn test_basic_link_update() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Some(test_path.join("foo")),
            &common_settings(true, false, false),
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
    async fn test_basic_link_empty_src() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        tokio::fs::create_dir(tmp_dir.join("baz")).await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path,
            &test_path.join("baz"), // empty source
            &test_path.join("bar"),
            &Some(test_path.join("foo")),
            &common_settings(true, false, false),
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

    pub async fn setup_update_dir(tmp_dir: &std::path::PathBuf) -> Result<()> {
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
    async fn test_link_update() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        setup_update_dir(&tmp_dir).await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Some(test_path.join("update")),
            &common_settings(true, false, false),
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

    async fn setup_test_dir_and_link() -> Result<std::path::PathBuf> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &None,
            &common_settings(true, false, false),
        )
        .await?;
        assert_eq!(summary.hard_links_created, 5);
        assert_eq!(summary.copy_summary.symlinks_created, 2);
        assert_eq!(summary.copy_summary.directories_created, 3);
        Ok(tmp_dir)
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_overwrite_basic() -> Result<()> {
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
            &output_path,
            &None,
            &common_settings(true, false, true), // overwrite!
        )
        .await?;
        assert_eq!(summary.hard_links_created, 3);
        assert_eq!(summary.copy_summary.symlinks_created, 1);
        assert_eq!(summary.copy_summary.directories_created, 1);
        testutils::check_dirs_identical(
            &tmp_dir.join("foo"),
            &output_path,
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_link_update_overwrite_basic() -> Result<()> {
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
            &output_path,
            &Some(tmp_dir.join("update")),
            &common_settings(true, false, true), // overwrite!
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
    async fn test_link_overwrite_hardlink_file() -> Result<()> {
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
            &output_path,
            &None,
            &common_settings(true, false, true), // overwrite!
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
}
