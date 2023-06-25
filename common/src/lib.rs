#[macro_use]
extern crate log;
use std::vec;

use anyhow::{Context, Result};
use async_recursion::async_recursion;

async fn copy_file(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    let mut reader = tokio::fs::File::open(src)
        .await
        .with_context(|| format!("rcp: cannot open {:?} for reading", src))?;
    let mut writer = tokio::fs::File::create(dst)
        .await
        .with_context(|| format!("rcp: cannot open {:?} for writing", dst))?;
    tokio::io::copy(&mut reader, &mut writer).await?;
    Ok(())
}

#[async_recursion]
pub async fn copy(src: &std::path::Path, dst: &std::path::Path, max_width: usize) -> Result<()> {
    debug!("copy: {:?} -> {:?}", src, dst);
    assert!(max_width > 0);
    let metadata = tokio::fs::symlink_metadata(src).await?;
    if metadata.is_file() {
        return copy_file(src, dst).await;
    } else if metadata.is_symlink() {
        let link = tokio::fs::read_link(src).await?;
        tokio::fs::symlink(link, dst).await?;
        return Ok(());
    }
    assert!(metadata.is_dir());
    let mut entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("rcp: cannot open directory {:?} for reading", src))?;
    tokio::fs::create_dir(dst)
        .await
        .with_context(|| format!("rcp: cannot create directory {:?}", dst))?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut errors = vec![];
    while let Some(entry) = entries.next_entry().await? {
        if join_set.len() >= max_width {
            if let Err(error) = join_set
                .join_next()
                .await
                .expect("JoinSet must not be empty here!")?
            {
                errors.push(error);
            }
        }
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        let do_copy = || async move { copy(&entry_path, &dst_path, max_width).await };
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
    debug!("copy: {:?} -> {:?} succeeded!", src, dst);
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use std::os::unix::prelude::PermissionsExt;
    use test_log::test;

    use super::*;

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
        //    |- 5.txt
        //    |- 6.txt -> ../bar/3.txt
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
        copy(&test_path.join("foo"), &test_path.join("bar"), 1).await?;
        check_dirs_identical(&test_path.join("foo"), &test_path.join("bar")).await?;
        Ok(())
    }

    async fn no_read_permission(max_width: usize) -> Result<()> {
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
        match copy(&test_path.join("foo"), &test_path.join("bar"), max_width).await {
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
        no_read_permission(1).await
    }

    #[test(tokio::test)]
    async fn no_read_permission_2() -> Result<()> {
        no_read_permission(2).await
    }

    #[test(tokio::test)]
    async fn no_read_permission_10() -> Result<()> {
        no_read_permission(10).await
    }
}
