use std::os::unix::fs::MetadataExt;

use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use throttle::get_file_iops_tokens;
use tracing::instrument;

use crate::filecmp;
use crate::preserve;
use crate::progress;
use crate::rm;
use crate::rm::{Settings as RmSettings, Summary as RmSummary};

/// Error type for copy operations that preserves operation summary even on failure.
///
/// # Logging Convention
/// The Display implementation automatically shows the full error chain, so you can log it
/// with any format specifier:
/// ```ignore
/// tracing::error!("operation failed: {}", &error);   // ✅ Shows full chain
/// tracing::error!("operation failed: {:#}", &error); // ✅ Shows full chain
/// tracing::error!("operation failed: {:?}", &error); // ✅ Shows full chain
/// ```
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
    src: &std::path::Path,
    dst: &std::path::Path,
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
    tracing::debug!("process contents of 'src' directory");
    let mut entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {src:?} for reading"))
        .map_err(|err| Error::new(err, Default::default()))?;
    let mut copy_summary = {
        if let Err(error) = tokio::fs::create_dir(dst).await {
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
                tracing::error!("{:#}", &error);
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
        // it's better to await the token here so that we throttle the syscalls generated by the
        // DirEntry call. the ops-throttle will never cause a deadlock (unlike max-open-files limit)
        // so it's safe to do here.
        throttle::get_ops_token().await;
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        let settings = *settings;
        let preserve = *preserve;
        let do_copy = || async move {
            copy(
                prog_track,
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
                    tracing::error!("copy: {:?} -> {:?} failed with: {:#}", src, dst, &error);
                    copy_summary = copy_summary + error.summary;
                    if settings.fail_early {
                        return Err(Error::new(error.source, copy_summary));
                    }
                    success = false;
                }
            },
            Err(error) => {
                if settings.fail_early {
                    return Err(Error::new(error.into(), copy_summary));
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
    tracing::debug!("set 'dst' directory metadata");
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
    }
}
