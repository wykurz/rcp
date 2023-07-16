use anyhow::{Context, Result};
use async_recursion::async_recursion;
use std::os::unix::fs::MetadataExt;
use std::os::unix::prelude::PermissionsExt;
use std::vec;

use crate::progress;

#[derive(Debug, Clone)]
pub struct Settings {
    pub preserve: bool,
    pub read_buffer: usize,
    pub dereference: bool,
}

async fn set_uid_gid(dst: &std::path::Path, uid: u32, gid: u32) -> Result<()> {
    let dst_owned = dst.to_owned();
    tokio::task::spawn_blocking(move || -> Result<()> {
        nix::unistd::fchownat(
            None,
            &dst_owned,
            Some(uid.into()),
            Some(gid.into()),
            nix::unistd::FchownatFlags::NoFollowSymlink,
        )
        .map_err(anyhow::Error::from)?;
        Ok(())
    })
    .await
    .with_context(|| {
        format!(
            "rcp: cannot set {:?} owner to {} and/or group id to {}",
            &dst, &uid, &gid
        )
    })?
}

async fn copy_file(
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
) -> Result<()> {
    let mut reader = tokio::fs::File::open(src)
        .await
        .with_context(|| format!("rcp: cannot open {:?} for reading", src))?;
    let mut buf_reader = tokio::io::BufReader::with_capacity(settings.read_buffer, &mut reader);
    let mut writer = tokio::fs::File::create(dst)
        .await
        .with_context(|| format!("rcp: cannot open {:?} for writing", dst))?;
    tokio::io::copy_buf(&mut buf_reader, &mut writer)
        .await
        .with_context(|| format!("rcp: failed copying data to {:?}", &dst))?;
    let src_metadata = reader
        .metadata()
        .await
        .with_context(|| format!("rcp: failed reading metadata from {:?}", &src))?;
    // remove sticky bit, setuid and setgid from permissions to mimic behavior of cp
    let permissions = if settings.preserve {
        src_metadata.permissions()
    } else {
        std::fs::Permissions::from_mode(src_metadata.permissions().mode() & 0o0777)
    };
    writer
        .set_permissions(permissions.clone())
        .await
        .with_context(|| {
            format!(
                "rcp: cannot set {:?} permissions to {:?}",
                &dst, &permissions
            )
        })?;
    if settings.preserve {
        // modify the uid and gid of the file as well
        let src_uid = src_metadata.uid();
        let src_gid = src_metadata.gid();
        set_uid_gid(dst, src_uid, src_gid).await.with_context(|| {
            format!(
                "rcp: cannot set {:?} owner to {} and/or group id to {}",
                &dst,
                &src_metadata.uid(),
                &src_metadata.gid()
            )
        })?;
    }
    Ok(())
}

#[async_recursion]
pub async fn copy(
    prog_track: &'static progress::TlsProgress,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
) -> Result<()> {
    debug!("copy: {:?} -> {:?}", src, dst);
    let _guard = prog_track.guard();
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("rcp: failed reading metadata from {:?}", &src))?;
    if src_metadata.is_file() {
        return copy_file(src, dst, settings).await;
    } else if src_metadata.is_symlink() {
        let link = tokio::fs::read_link(src)
            .await
            .with_context(|| format!("rcp: failed reading symlink {:?}", &src))?;
        tokio::fs::symlink(link, dst)
            .await
            .with_context(|| format!("rcp: failed creating symlink {:?}", &dst))?;
        return Ok(());
    }
    assert!(src_metadata.is_dir());
    let mut entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("rcp: cannot open directory {:?} for reading", src))?;
    tokio::fs::create_dir(dst)
        .await
        .with_context(|| format!("rcp: cannot create directory {:?}", dst))?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut errors = vec![];
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("rcp: failed traversing directory {:?}", &dst))?
    {
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        let settings = settings.clone();
        let do_copy = || async move { copy(prog_track, &entry_path, &dst_path, &settings).await };
        join_set.spawn(do_copy());
    }
    while let Some(res) = join_set.join_next().await {
        if let Err(error) = res? {
            errors.push(error);
        }
    }
    if !errors.is_empty() {
        debug!("copy: {:?} -> {:?} failed with: {:?}", src, dst, &errors);
        return Err(anyhow::anyhow!("{:?}", &errors));
    }
    // remove sticky bit, setuid and setgid from permissions to mimic behavior of cp
    let permissions = if settings.preserve {
        src_metadata.permissions()
    } else {
        std::fs::Permissions::from_mode(src_metadata.permissions().mode() & 0o0777)
    };
    tokio::fs::set_permissions(dst, permissions.clone())
        .await
        .with_context(|| {
            format!(
                "rcp: cannot set {:?} permissions to {:?}",
                &dst, &permissions
            )
        })?;
    if settings.preserve {
        // modify the uid and gid of the file as well
        let src_uid = src_metadata.uid();
        let src_gid = src_metadata.gid();
        set_uid_gid(dst, src_uid, src_gid).await.with_context(|| {
            format!(
                "rcp: cannot set {:?} owner to {} and/or group id to {}",
                &dst,
                &src_metadata.uid(),
                &src_metadata.gid()
            )
        })?;
    }
    debug!("copy: {:?} -> {:?} succeeded!", src, dst);
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use test_log::test;

    use super::*;

    lazy_static! {
        static ref PROGRESS: progress::TlsProgress = progress::TlsProgress::new();
    }

    async fn create_temp_dir() -> Result<std::path::PathBuf> {
        let mut idx = 0;
        loop {
            let tmp_dir = std::env::temp_dir().join(format!("rcp_test{}", &idx));
            if let Err(error) = tokio::fs::create_dir(&tmp_dir).await {
                match error.kind() {
                    std::io::ErrorKind::AlreadyExists => {
                        idx += 1;
                    }
                    _ => return Err(error.into()),
                }
            } else {
                return Ok(tmp_dir);
            }
        }
    }

    async fn setup() -> Result<std::path::PathBuf> {
        // create a temporary directory
        let tmp_dir = create_temp_dir().await?;
        // foo
        // |- 0.txt
        // |- bar
        //    |- 1.txt
        //    |- 2.txt
        //    |- 3.txt
        // |- baz
        //    |- 4.txt
        //    |- 5.txt -> ../bar/2.txt
        //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
        let foo_path = tmp_dir.join("foo");
        tokio::fs::create_dir(&foo_path).await.unwrap();
        tokio::fs::write(foo_path.join("0.txt"), "0").await.unwrap();
        let bar_path = foo_path.join("bar");
        tokio::fs::create_dir(&bar_path).await.unwrap();
        tokio::fs::write(bar_path.join("1.txt"), "1").await.unwrap();
        tokio::fs::write(bar_path.join("2.txt"), "2").await.unwrap();
        tokio::fs::write(bar_path.join("3.txt"), "3").await.unwrap();
        let baz_path = foo_path.join("baz");
        tokio::fs::create_dir(&baz_path).await.unwrap();
        tokio::fs::write(baz_path.join("4.txt"), "4").await.unwrap();
        tokio::fs::symlink("../bar/2.txt", baz_path.join("5.txt"))
            .await
            .unwrap();
        tokio::fs::symlink(bar_path.join("3.txt"), baz_path.join("6.txt"))
            .await
            .unwrap();
        Ok(tmp_dir)
    }

    #[async_recursion]
    async fn check_dirs_identical(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        let mut src_entries = tokio::fs::read_dir(src).await?;
        while let Some(src_entry) = src_entries.next_entry().await? {
            let src_entry_path = src_entry.path();
            let src_entry_name = src_entry_path.file_name().unwrap();
            let dst_entry_path = dst.join(src_entry_name);
            let src_md = tokio::fs::symlink_metadata(&src_entry_path)
                .await
                .context(format!("Source file {:?} is missing!", &src_entry_path))?;
            let dst_md = tokio::fs::symlink_metadata(&dst_entry_path)
                .await
                .context(format!(
                    "Destination file {:?} is missing!",
                    &dst_entry_path
                ))?;
            assert_eq!(src_md.file_type(), dst_md.file_type());
            assert_eq!(src_md.permissions(), dst_md.permissions());
            if src_md.is_file() {
                let src_contents = tokio::fs::read_to_string(&src_entry_path).await?;
                let dst_contents = tokio::fs::read_to_string(&dst_entry_path).await?;
                assert_eq!(src_contents, dst_contents);
            } else if src_md.file_type().is_symlink() {
                let src_link = tokio::fs::read_link(&src_entry_path).await?;
                let dst_link = tokio::fs::read_link(&dst_entry_path).await?;
                assert_eq!(src_link, dst_link);
            } else {
                check_dirs_identical(&src_entry_path, &dst_entry_path).await?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn check_basic_copy() -> Result<()> {
        let tmp_dir = setup().await?;
        let test_path = tmp_dir.as_path();
        copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                preserve: false,
                read_buffer: 10,
                dereference: false,
            },
        )
        .await?;
        check_dirs_identical(&test_path.join("foo"), &test_path.join("bar")).await?;
        Ok(())
    }

    async fn no_read_permission() -> Result<()> {
        let tmp_dir = setup().await?;
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
        check_dirs_identical(&test_path.join("foo"), &test_path.join("bar")).await?;
        Ok(())
    }

    // parametrize the tests to run with different max_width values
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
        let tmp_dir = setup().await?;
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
        copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                preserve: false,
                read_buffer: 7,
                dereference: false,
            },
        )
        .await?;
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
        check_dirs_identical(&test_path.join("foo"), &test_path.join("bar")).await?;
        Ok(())
    }

    #[test(tokio::test)]
    async fn no_write_permission() -> Result<()> {
        let tmp_dir = setup().await?;
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
        copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                preserve: false,
                read_buffer: 8,
                dereference: false,
            },
        )
        .await?;
        check_dirs_identical(&test_path.join("foo"), &test_path.join("bar")).await?;
        Ok(())
    }
}
