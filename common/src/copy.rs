use std::os::unix::fs::MetadataExt;

use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use tracing::{event, instrument, Level};

use crate::filecmp;
use crate::preserve;
use crate::progress;
use crate::rm;
use crate::rm::{Settings as RmSettings, Summary as RmSummary};

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct Error {
    #[source]
    pub source: anyhow::Error,
    pub summary: Summary,
}

impl Error {
    pub fn new(source: anyhow::Error, summary: Summary) -> Self {
        Error { source, summary }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Settings {
    pub dereference: bool,
    pub fail_early: bool,
    pub overwrite: bool,
    pub overwrite_compare: filecmp::MetadataCmpSettings,
    pub chunk_size: u64,
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
    let _open_file_guard = throttle::open_file_permit().await;
    event!(
        Level::DEBUG,
        "opening 'src' for reading and 'dst' for writing"
    );
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))
        .map_err(|err| Error::new(err, Default::default()))?;
    if settings.chunk_size > 0 {
        let tokens = 1 + (std::cmp::max(1, src_metadata.size()) - 1) / settings.chunk_size;
        if tokens > u32::MAX as u64 {
            event!(
                Level::ERROR,
                "chunk size: {} is too small to limit throughput for files this big, file: {:?}, size: {}",
                settings.chunk_size, &src, src_metadata.size(),
            );
        } else {
            throttle::get_iops_tokens(tokens as u32).await;
        }
    }
    let mut rm_summary = RmSummary::default();
    if !is_fresh && dst.exists() {
        if settings.overwrite {
            event!(Level::DEBUG, "file exists, check if it's identical");
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
                event!(Level::DEBUG, "file is identical, skipping");
                prog_track.files_unchanged.inc();
                return Ok(Summary {
                    files_unchanged: 1,
                    ..Default::default()
                });
            }
            event!(Level::INFO, "file is different, removing existing file");
            // note tokio::fs::overwrite cannot handle this path being e.g. a directory
            rm_summary = rm::rm(
                prog_track,
                dst,
                &RmSettings {
                    fail_early: settings.fail_early,
                },
            )
            .await
            .map_err(|err| {
                let rm_summary = err.summary;
                let copy_summary = Summary {
                    rm_summary,
                    ..Default::default()
                };
                Error::new(anyhow::Error::msg(err), copy_summary)
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
    event!(Level::DEBUG, "copying data");
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
    event!(Level::DEBUG, "setting permissions");
    preserve::set_file_metadata(preserve, &src_metadata, dst)
        .await
        .map_err(|err| Error::new(err, copy_summary))?;
    // we mark files as "copied" only after all metadata is set as well
    copy_summary.bytes_copied += src_metadata.len();
    copy_summary.files_copied += 1;
    Ok(copy_summary)
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Summary {
    pub bytes_copied: u64,
    pub files_copied: usize,
    pub symlinks_created: usize,
    pub directories_created: usize,
    pub files_unchanged: usize,
    pub symlinks_unchanged: usize,
    pub directories_unchanged: usize,
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
            {}",
            bytesize::ByteSize(self.bytes_copied),
            self.files_copied,
            self.symlinks_created,
            self.directories_created,
            self.files_unchanged,
            self.symlinks_unchanged,
            self.directories_unchanged,
            &self.rm_summary,
        )
    }
}

#[instrument(skip(prog_track))]
#[async_recursion]
pub async fn copy(
    prog_track: &'static progress::Progress,
    cwd: &std::path::Path,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
    preserve: &preserve::Settings,
    mut is_fresh: bool,
) -> Result<Summary, Error> {
    throttle::get_ops_token().await;
    let _ops_guard = prog_track.ops.guard();
    event!(Level::DEBUG, "reading source metadata");
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))
        .map_err(|err| Error::new(err, Default::default()))?;
    if settings.dereference && src_metadata.is_symlink() {
        let link = tokio::fs::read_link(&src)
            .await
            .with_context(|| format!("failed reading src symlink {:?}", &src))
            .map_err(|err| Error::new(err, Default::default()))?;
        let abs_link = if link.is_relative() {
            cwd.join(link)
        } else {
            link
        };
        let new_cwd = abs_link
            .parent()
            .with_context(|| {
                format!(
                    "the source symlink {:?} does not have a parent directory",
                    &src
                )
            })
            .unwrap();
        return copy(
            prog_track, new_cwd, &abs_link, dst, settings, preserve, is_fresh,
        )
        .await;
    }
    if src_metadata.is_file() {
        return copy_file(prog_track, src, dst, settings, preserve, is_fresh).await;
    }
    if src_metadata.is_symlink() {
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
                        event!(
                            Level::DEBUG,
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
                                event!(Level::DEBUG, "'dst' metadata is different, updating");
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
                        event!(Level::DEBUG, "symlink already exists, skipping");
                        prog_track.symlinks_unchanged.inc();
                        return Ok(Summary {
                            symlinks_unchanged: 1,
                            ..Default::default()
                        });
                    }
                    event!(
                        Level::DEBUG,
                        "'dst' is a symlink but points to a different path, updating"
                    );
                } else {
                    event!(Level::INFO, "'dst' is not a symlink, updating");
                }
                rm_summary = rm::rm(
                    prog_track,
                    dst,
                    &RmSettings {
                        fail_early: settings.fail_early,
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
    event!(Level::DEBUG, "process contents of 'src' directory");
    let mut entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {src:?} for reading"))
        .map_err(|err| Error::new(err, Default::default()))?;
    let mut copy_summary = {
        if let Err(error) = tokio::fs::create_dir(dst).await {
            assert!(!is_fresh, "unexpected error creating directory: {:?}", &dst);
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
                    event!(Level::DEBUG, "'dst' is a directory, leaving it as is");
                    prog_track.directories_unchanged.inc();
                    Summary {
                        directories_unchanged: 1,
                        ..Default::default()
                    }
                } else {
                    event!(
                        Level::INFO,
                        "'dst' is not a directory, removing and creating a new one"
                    );
                    let rm_summary = rm::rm(
                        prog_track,
                        dst,
                        &RmSettings {
                            fail_early: settings.fail_early,
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
                            Error::new(anyhow::Error::msg(err), copy_summary)
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
                event!(Level::ERROR, "{:?}", &error);
                return Err(Error::new(
                    anyhow!("cannot create directory {:?}", dst),
                    Default::default(),
                ));
            }
        } else {
            // new directory created, anything copied into dst may assume they don't need to check for conflicts
            is_fresh = true;
            prog_track.directories_created.inc();
            Summary {
                directories_created: 1,
                ..Default::default()
            }
        }
    };
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing src directory {:?}", &src))
        .map_err(|err| Error::new(err, copy_summary))?
    {
        let cwd_path = src.to_owned();
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        let settings = *settings;
        let preserve = *preserve;
        let do_copy = || async move {
            copy(
                prog_track,
                &cwd_path,
                &entry_path,
                &dst_path,
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
                    event!(
                        Level::ERROR,
                        "copy: {:?} -> {:?} failed with: {}",
                        src,
                        dst,
                        &error
                    );
                    copy_summary = copy_summary + error.summary;
                    if settings.fail_early {
                        return Err(Error::new(error.source, copy_summary));
                    }
                    success = false;
                }
            },
            Err(error) => {
                if settings.fail_early {
                    return Err(Error::new(anyhow::Error::msg(error), copy_summary));
                }
            }
        }
    }
    if !success {
        return Err(Error::new(
            anyhow!("copy: {:?} -> {:?} failed!", src, dst),
            copy_summary,
        ))?;
    }
    event!(Level::DEBUG, "set 'dst' directory metadata");
    preserve::set_dir_metadata(preserve, &src_metadata, dst)
        .await
        .map_err(|err| Error::new(err, copy_summary))?;
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
            &test_path,
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
            &test_path,
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
            },
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await
        {
            Ok(_) => panic!("Expected the copy to error!"),
            Err(error) => {
                event!(Level::INFO, "{}", &error);
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
            &test_path,
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
            &test_path,
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
        for f in vec![src1, src2] {
            tokio::fs::set_permissions(f, std::fs::Permissions::from_mode(test_mode)).await?;
        }
        let summary = copy(
            &PROGRESS,
            &test_path,
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
        for f in vec![dst1, dst2] {
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
            &test_path,
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
            &test_path,
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
                &RmSettings { fail_early: false },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings { fail_early: false },
                )
                .await?;
            assert_eq!(summary.files_removed, 3);
            assert_eq!(summary.symlinks_removed, 1);
            assert_eq!(summary.directories_removed, 1);
        }
        let summary = copy(
            &PROGRESS,
            &tmp_dir,
            &tmp_dir.join("foo"),
            &output_path,
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
            &output_path,
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
                &RmSettings { fail_early: false },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz"),
                    &RmSettings { fail_early: false },
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
            &tmp_dir,
            &tmp_dir.join("foo"),
            &output_path,
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
            &output_path,
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
                &RmSettings { fail_early: false },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings { fail_early: false },
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
            &tmp_dir,
            &tmp_dir.join("foo"),
            &output_path,
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
            &output_path,
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
                &RmSettings { fail_early: false },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings { fail_early: false },
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
            &tmp_dir,
            &tmp_dir.join("foo"),
            &output_path,
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
            &output_path,
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
            &test_path,
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
            &tmp_dir,
            &tmp_dir.join("foo"),
            &output_path,
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
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await
        {
            Ok(_) => panic!("Expected the copy to error!"),
            Err(error) => {
                event!(Level::INFO, "{}", &error);
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
    async fn test_cp_dereference_dir() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        // symlink bar to bar-link
        tokio::fs::symlink("bar", &tmp_dir.join("foo").join("bar-link")).await?;
        // symlink bar-link to bar-link-link
        tokio::fs::symlink("bar-link", &tmp_dir.join("foo").join("bar-link-link")).await?;
        let summary = copy(
            &PROGRESS,
            &tmp_dir,
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
            .args(&["-r", "-L"])
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
}
