use anyhow::{Context, Result};
use async_recursion::async_recursion;
use std::os::unix::fs::MetadataExt;
use std::os::unix::prelude::PermissionsExt;

use crate::progress;

#[derive(Debug, Clone)]
pub struct Settings {
    pub preserve: bool,
    pub read_buffer: usize,
    pub dereference: bool,
    pub fail_early: bool,
}

async fn set_owner_and_time(dst: &std::path::Path, metadata: &std::fs::Metadata) -> Result<()> {
    let dst = dst.to_owned();
    let metadata = metadata.to_owned();
    tokio::task::spawn_blocking(move || -> Result<()> {
        // set timestamps first - those are unlikely to fail
        let atime = nix::sys::time::TimeSpec::new(metadata.atime(), metadata.atime_nsec());
        let mtime = nix::sys::time::TimeSpec::new(metadata.mtime(), metadata.mtime_nsec());
        nix::sys::stat::utimensat(
            None,
            &dst,
            &atime,
            &mtime,
            nix::sys::stat::UtimensatFlags::NoFollowSymlink,
        )
        .with_context(|| format!("failed setting timestamps for {:?}", &dst))?;
        // set user and group - set those last, if those fail we at least have the timestamps set
        let uid = metadata.uid();
        let gid = metadata.gid();
        nix::unistd::fchownat(
            None,
            &dst,
            Some(uid.into()),
            Some(gid.into()),
            nix::unistd::FchownatFlags::NoFollowSymlink,
        )
        .with_context(|| {
            format!(
                "cannot set {:?} owner to {} and/or group id to {}",
                &dst, &uid, &gid
            )
        })
        .map_err(anyhow::Error::from)?;
        Ok(())
    })
    .await?
}

async fn copy_file(
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
) -> Result<()> {
    let mut reader = tokio::fs::File::open(src)
        .await
        .with_context(|| format!("cannot open {:?} for reading", src))?;
    let mut buf_reader = tokio::io::BufReader::with_capacity(settings.read_buffer, &mut reader);
    let mut writer = tokio::fs::File::create(dst)
        .await
        .with_context(|| format!("cannot open {:?} for writing", dst))?;
    tokio::io::copy_buf(&mut buf_reader, &mut writer)
        .await
        .with_context(|| format!("failed copying data to {:?}", &dst))?;
    let src_metadata = reader
        .metadata()
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))?;
    let permissions = if settings.preserve {
        src_metadata.permissions()
    } else {
        // remove sticky bit, setuid and setgid from permissions to mimic behavior of cp
        std::fs::Permissions::from_mode(src_metadata.permissions().mode() & 0o0777)
    };
    writer
        .set_permissions(permissions.clone())
        .await
        .with_context(|| format!("cannot set {:?} permissions to {:?}", &dst, &permissions))?;
    if settings.preserve {
        // modify the uid and gid of the file as well
        set_owner_and_time(dst, &src_metadata).await?;
    }
    Ok(())
}

#[derive(Copy, Clone, Default)]
pub struct CopySummary {
    pub files_copied: usize,
    pub symlinks_created: usize,
    pub directories_created: usize,
}

impl std::ops::Add for CopySummary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            files_copied: self.files_copied + other.files_copied,
            symlinks_created: self.symlinks_created + other.symlinks_created,
            directories_created: self.directories_created + other.directories_created,
        }
    }
}

impl std::fmt::Display for CopySummary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "files copied: {}\nsymlinks created: {}\ndirectories created: {}",
            self.files_copied, self.symlinks_created, self.directories_created
        )
    }
}

#[async_recursion]
pub async fn copy(
    prog_track: &'static progress::TlsProgress,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
) -> Result<CopySummary> {
    debug!("copy: {:?} -> {:?}", src, dst);
    let _guard = prog_track.guard();
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))?;
    if src_metadata.is_file() || (src_metadata.is_symlink() && settings.dereference) {
        copy_file(src, dst, settings).await?;
        return Ok(CopySummary {
            files_copied: 1,
            ..Default::default()
        });
    }
    if src_metadata.is_symlink() {
        let link = tokio::fs::read_link(src)
            .await
            .with_context(|| format!("failed reading symlink {:?}", &src))?;
        tokio::fs::symlink(link, dst)
            .await
            .with_context(|| format!("failed creating symlink {:?}", &dst))?;
        if settings.preserve {
            set_owner_and_time(dst, &src_metadata).await?;
        }
        return Ok(CopySummary {
            symlinks_created: 1,
            ..Default::default()
        });
    }
    assert!(src_metadata.is_dir());
    let mut entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {:?} for reading", src))?;
    tokio::fs::create_dir(dst)
        .await
        .with_context(|| format!("cannot create directory {:?}", dst))?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &src))?
    {
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        let settings = settings.clone();
        let do_copy = || async move { copy(prog_track, &entry_path, &dst_path, &settings).await };
        join_set.spawn(do_copy());
    }
    let mut copy_summary = CopySummary {
        directories_created: 1,
        ..Default::default()
    };
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => copy_summary = copy_summary + summary,
            Err(error) => {
                error!("copy: {:?} -> {:?} failed with: {}", src, dst, &error);
                if settings.fail_early {
                    return Err(error);
                }
                success = false;
            }
        }
    }
    if !success {
        return Err(anyhow::anyhow!("copy: {:?} -> {:?} failed!", src, dst));
    }
    let permissions = if settings.preserve {
        src_metadata.permissions()
    } else {
        // remove sticky bit, setuid and setgid from permissions to mimic behavior of cp
        std::fs::Permissions::from_mode(src_metadata.permissions().mode() & 0o0777)
    };
    tokio::fs::set_permissions(dst, permissions.clone())
        .await
        .with_context(|| format!("cannot set {:?} permissions to {:?}", &dst, &permissions))?;
    if settings.preserve {
        set_owner_and_time(dst, &src_metadata).await?;
    }
    debug!("copy: {:?} -> {:?} succeeded!", src, dst);
    Ok(copy_summary)
}

#[cfg(test)]
mod copy_tests {
    use crate::testutils;
    use anyhow::Context;
    use test_log::test;

    use super::*;

    lazy_static! {
        static ref PROGRESS: progress::TlsProgress = progress::TlsProgress::new();
    }

    #[tokio::test]
    async fn check_basic_copy() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                preserve: false,
                read_buffer: 10,
                dereference: false,
                fail_early: false,
            },
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

    async fn no_read_permission() -> Result<()> {
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
                preserve: false,
                read_buffer: 5,
                dereference: false,
                fail_early: false,
            },
        )
        .await
        {
            Ok(_) => panic!("Expected the copy to error!"),
            Err(error) => {
                info!("{}", &error);
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

    #[test(tokio::test)]
    async fn no_read_permission_1() -> Result<()> {
        no_read_permission().await
    }

    #[test(tokio::test)]
    async fn no_read_permission_2() -> Result<()> {
        no_read_permission().await
    }

    #[test(tokio::test)]
    async fn no_read_permission_10() -> Result<()> {
        no_read_permission().await
    }

    #[test(tokio::test)]
    async fn check_default_mode() -> Result<()> {
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
                preserve: false,
                read_buffer: 7,
                dereference: false,
                fail_early: false,
            },
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

    #[test(tokio::test)]
    async fn no_write_permission() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // directory - readable and non-executable
        let non_exec_dir = &test_path.join("foo").join("bogey");
        tokio::fs::create_dir(&non_exec_dir).await?;
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
                preserve: false,
                read_buffer: 8,
                dereference: false,
                fail_early: false,
            },
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

    #[test(tokio::test)]
    async fn dereference() -> Result<()> {
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
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                preserve: false,
                read_buffer: 10,
                dereference: true, // <- important!
                fail_early: false,
            },
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

    async fn cp_compare(cp_args: &[&str], rcp_settings: &Settings) -> Result<()> {
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
            if rcp_settings.preserve {
                testutils::FileEqualityCheck::Timestamp
            } else {
                testutils::FileEqualityCheck::Basic
            },
        )
        .await?;
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_cp_compat() -> Result<()> {
        cp_compare(
            &["-r"],
            &Settings {
                preserve: false,
                read_buffer: 100,
                dereference: false,
                fail_early: false,
            },
        )
        .await?;
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_cp_compat_preserve() -> Result<()> {
        cp_compare(
            &["-r", "-p"],
            &Settings {
                preserve: true,
                read_buffer: 100,
                dereference: false,
                fail_early: false,
            },
        )
        .await?;
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_cp_compat_dereference() -> Result<()> {
        cp_compare(
            &["-r", "-L"],
            &Settings {
                preserve: false,
                read_buffer: 100,
                dereference: true,
                fail_early: false,
            },
        )
        .await?;
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_cp_compat_preserve_and_dereference() -> Result<()> {
        cp_compare(
            &["-r", "-p", "-L"],
            &Settings {
                preserve: true,
                read_buffer: 100,
                dereference: true,
                fail_early: false,
            },
        )
        .await?;
        Ok(())
    }
}

fn is_file_type_same(md1: &std::fs::Metadata, md2: &std::fs::Metadata) -> bool {
    let ft1 = md1.file_type();
    let ft2 = md2.file_type();
    ft1.is_dir() == ft2.is_dir()
        && ft1.is_file() == ft2.is_file()
        && ft1.is_symlink() == ft2.is_symlink()
}

fn is_unchanged(md1: &std::fs::Metadata, md2: &std::fs::Metadata) -> bool {
    if md1.size() != md2.size()
        || md1.mtime() != md2.mtime()
        || md1.permissions() != md2.permissions()
        || md1.uid() != md2.uid()
        || md1.gid() != md2.gid()
    {
        return false;
    }
    // some filesystems do not support nanosecond precision, so we only compare nanoseconds if both files have them
    if md1.mtime_nsec() != 0 && md2.mtime_nsec() != 0 && md1.mtime_nsec() != md2.mtime_nsec() {
        return false;
    }
    true
}

#[derive(Copy, Clone, Default)]
pub struct LinkSummary {
    pub files_hard_linked: usize,
    pub copy_summary: CopySummary,
}

impl std::ops::Add for LinkSummary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            files_hard_linked: self.files_hard_linked + other.files_hard_linked,
            copy_summary: self.copy_summary + other.copy_summary,
        }
    }
}

impl std::fmt::Display for LinkSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "files hard-linked: {}\n{}",
            self.files_hard_linked, &self.copy_summary
        )
    }
}

#[async_recursion]
pub async fn link(
    prog_track: &'static progress::TlsProgress,
    src: &std::path::Path,
    dst: &std::path::Path,
    update: &Option<std::path::PathBuf>,
    settings: &Settings,
) -> Result<LinkSummary> {
    debug!("link: {:?} {:?} -> {:?}", src, update, dst);
    let _guard = prog_track.guard();
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))?;
    let update_metadata_opt = match update {
        Some(update) => {
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
        if !is_file_type_same(&src_metadata, update_metadata) {
            // file type changed, just copy the updated one
            debug!(
                "link: file type of {:?} ({:?}) and {:?} ({:?}) differs - copying from update",
                src,
                src_metadata.file_type(),
                update,
                update_metadata.file_type()
            );
            let copy_summary = copy(prog_track, update, dst, settings).await?;
            return Ok(LinkSummary {
                copy_summary,
                ..Default::default()
            });
        }
        if update_metadata.is_file() {
            // check if the file is unchanged and if so hard-link, otherwise copy from the updated one
            if is_unchanged(&src_metadata, update_metadata) {
                tokio::fs::hard_link(src, dst).await?;
                return Ok(LinkSummary {
                    files_hard_linked: 1,
                    ..Default::default()
                });
            } else {
                debug!(
                    "link: {:?} metadata has changed, copying from {:?}",
                    src, update
                );
                copy_file(update, dst, settings).await?;
                return Ok(LinkSummary {
                    copy_summary: CopySummary {
                        files_copied: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                });
            }
        }
        if update_metadata.is_symlink() {
            // just symlink the updated one, no need to check if it's changed
            let update_symlink = tokio::fs::read_link(update)
                .await
                .with_context(|| format!("failed reading symlink {:?}", &update))?;
            tokio::fs::symlink(update_symlink, dst)
                .await
                .with_context(|| format!("failed creating symlink {:?}", &dst))?;
            if settings.preserve {
                set_owner_and_time(dst, update_metadata).await?;
            }
            return Ok(LinkSummary {
                copy_summary: CopySummary {
                    symlinks_created: 1,
                    ..Default::default()
                },
                ..Default::default()
            });
        }
    } else {
        // update hasn't been specified, if this is a file just hard-link the source or symlink if it's a symlink
        if src_metadata.is_file() {
            tokio::fs::hard_link(src, dst).await?;
            return Ok(LinkSummary {
                files_hard_linked: 1,
                ..Default::default()
            });
        }
        if src_metadata.is_symlink() {
            let src_symlink = tokio::fs::read_link(src)
                .await
                .with_context(|| format!("failed reading symlink {:?}", &src))?;
            tokio::fs::symlink(src_symlink, dst)
                .await
                .with_context(|| format!("failed creating symlink {:?}", &dst))?;
            if settings.preserve {
                set_owner_and_time(dst, &src_metadata).await?;
            }
            return Ok(LinkSummary {
                copy_summary: CopySummary {
                    symlinks_created: 1,
                    ..Default::default()
                },
                ..Default::default()
            });
        }
    }
    assert!(src_metadata.is_dir());
    assert!(update_metadata_opt.is_none() || update_metadata_opt.as_ref().unwrap().is_dir());
    let mut src_entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {:?} for reading", src))?;
    tokio::fs::create_dir(dst)
        .await
        .with_context(|| format!("cannot create directory {:?}", dst))?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
    // iterate through src entries and recursively call "link" on each one
    while let Some(src_entry) = src_entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &src))?
    {
        let entry_path = src_entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        let update_path = update.as_ref().map(|s| s.join(entry_name));
        let settings = settings.clone();
        let do_link = || async move {
            link(prog_track, &entry_path, &dst_path, &update_path, &settings).await
        };
        join_set.spawn(do_link());
    }
    // only process update if it the path was provided and the directory is present
    if update_metadata_opt.is_some() {
        let update = update.as_ref().unwrap();
        let mut update_entries = tokio::fs::read_dir(update)
            .await
            .with_context(|| format!("cannot open directory {:?} for reading", &update))?;
        // iterate through update entries and for each one that's not present in src call "copy"
        while let Some(update_entry) = update_entries
            .next_entry()
            .await
            .with_context(|| format!("failed traversing directory {:?}", &update))?
        {
            let entry_path = update_entry.path();
            let entry_name = entry_path.file_name().unwrap();
            let src_path = src.join(entry_name);
            let dst_path = dst.join(entry_name);
            let update_path = update.join(entry_name);
            let settings = settings.clone();
            let do_copy = || async move {
                if tokio::fs::symlink_metadata(src_path).await.is_ok() {
                    // we already must have considered this file, skip it
                    return Ok(LinkSummary::default());
                }
                let copy_summary = copy(prog_track, &update_path, &dst_path, &settings).await?;
                Ok(LinkSummary {
                    copy_summary,
                    ..Default::default()
                })
            };
            join_set.spawn(do_copy());
        }
    }
    let mut link_summary = LinkSummary {
        copy_summary: CopySummary {
            directories_created: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => link_summary = link_summary + summary,
            Err(error) => {
                error!(
                    "link: {:?} {:?} -> {:?} failed with: {}",
                    src, update, dst, &error
                );
                if settings.fail_early {
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
    let preserve_metadata = if let Some(update_metadata) = update_metadata_opt.as_ref() {
        update_metadata
    } else {
        &src_metadata
    };
    let permissions = if settings.preserve {
        preserve_metadata.permissions()
    } else {
        // remove sticky bit, setuid and setgid from permissions to mimic behavior of cp
        std::fs::Permissions::from_mode(preserve_metadata.permissions().mode() & 0o0777)
    };
    tokio::fs::set_permissions(dst, permissions.clone())
        .await
        .with_context(|| format!("cannot set {:?} permissions to {:?}", &dst, &permissions))?;
    if settings.preserve {
        set_owner_and_time(dst, preserve_metadata).await?;
    }
    debug!("link: {:?} {:?} -> {:?} succeeded!", src, update, dst);
    Ok(link_summary)
}

#[cfg(test)]
mod link_tests {
    use crate::testutils;
    use test_log::test;

    use super::*;

    lazy_static! {
        static ref PROGRESS: progress::TlsProgress = progress::TlsProgress::new();
    }

    const COMMON_SETTINGS: Settings = Settings {
        preserve: true,
        read_buffer: 10,
        dereference: false,
        fail_early: false,
    };

    #[test(tokio::test)]
    async fn check_basic_link() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &None,
            &COMMON_SETTINGS,
        )
        .await?;
        assert_eq!(summary.files_hard_linked, 5);
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

    #[test(tokio::test)]
    async fn check_basic_link_update() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Some(test_path.join("foo")),
            &COMMON_SETTINGS,
        )
        .await?;
        assert_eq!(summary.files_hard_linked, 5);
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

    #[test(tokio::test)]
    async fn check_basic_link_empty_src() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        tokio::fs::create_dir(tmp_dir.join("baz")).await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path.join("baz"), // empty source
            &test_path.join("bar"),
            &Some(test_path.join("foo")),
            &COMMON_SETTINGS,
        )
        .await?;
        assert_eq!(summary.files_hard_linked, 0);
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

    #[test(tokio::test)]
    async fn check_link_update() -> Result<()> {
        let tmp_dir = testutils::setup_test_dir().await?;
        setup_update_dir(&tmp_dir).await?;
        let test_path = tmp_dir.as_path();
        let summary = link(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Some(test_path.join("update")),
            &COMMON_SETTINGS,
        )
        .await?;
        assert_eq!(summary.files_hard_linked, 2);
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
}
