#[macro_use]
extern crate log;
use anyhow::Result;
use async_recursion::async_recursion;

async fn is_file(path: &std::path::Path) -> Result<bool> {
    let md = tokio::fs::metadata(path).await?;
    Ok(md.is_file())
}

async fn copy_file(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    let mut reader = match tokio::fs::File::open(src).await {
        Ok(reader) => reader,
        Err(error) => {
            // ignore permission denied on READ errors
            if error.kind() == std::io::ErrorKind::PermissionDenied {
                warn!(
                    "rcp: cannot open '{}' for reading: Permission denied",
                    src.display()
                );
                return Ok(());
            } else {
                return Err(error.into());
            }
        }
    };
    let mut writer = tokio::fs::File::create(dst).await?;
    tokio::io::copy(&mut reader, &mut writer).await?;
    Ok(())
}

#[async_recursion]
pub async fn copy(src: &std::path::Path, dst: &std::path::Path, max_width: usize) -> Result<()> {
    assert!(max_width > 0);
    if is_file(src).await? {
        return copy_file(src, dst).await;
    }
    let mut entries = match tokio::fs::read_dir(src).await {
        Ok(entries) => entries,
        Err(error) => {
            // ignore permission denied on READ errors
            if error.kind() == std::io::ErrorKind::PermissionDenied {
                warn!(
                    "rcp: cannot open '{}' for reading: Permission denied",
                    src.display()
                );
                return Ok(());
            } else {
                return Err(error.into());
            }
        }
    };
    tokio::fs::create_dir(dst).await?;
    let mut join_set = tokio::task::JoinSet::new();
    while let Some(entry) = entries.next_entry().await? {
        if join_set.len() >= max_width {
            join_set
                .join_next()
                .await
                .expect("JoinSet must not be empty here!")??;
        }
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        let do_copy = || async move { copy(&entry_path, &dst_path, max_width).await };
        join_set.spawn(do_copy());
    }
    while let Some(res) = join_set.join_next().await {
        res??
    }
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
        tokio::fs::write(baz_path.join("5.txt"), "5").await.unwrap();
        Ok(tmp_dir)
    }

    #[async_recursion]
    async fn check_dirs_identical(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        let mut src_entries = tokio::fs::read_dir(src).await?;
        while let Some(src_entry) = src_entries.next_entry().await? {
            let src_entry_path = src_entry.path();
            let src_entry_name = src_entry_path.file_name().unwrap();
            let dst_entry_path = dst.join(src_entry_name);
            let src_md = tokio::fs::metadata(&src_entry_path)
                .await
                .context(format!("Source file {:?} is missing!", &src_entry_path))?;
            let dst_md = tokio::fs::metadata(&dst_entry_path).await.context(format!(
                "Destination file {:?} is missing!",
                &dst_entry_path
            ))?;
            assert_eq!(src_md.is_file(), dst_md.is_file());
            if src_md.is_file() {
                let src_contents = tokio::fs::read_to_string(&src_entry_path).await?;
                let dst_contents = tokio::fs::read_to_string(&dst_entry_path).await?;
                assert_eq!(src_contents, dst_contents);
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

    #[test(tokio::test)]
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
        if copy(&test_path.join("foo"), &test_path.join("bar"), 1)
            .await
            .is_ok()
        {
            panic!("Expected the copy to error!");
        }
        // make source directory same as what we expect destination to be
        for fpath in &filepaths {
            tokio::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o700)).await?;
            if tokio::fs::metadata(fpath).await?.is_file() {
                tokio::fs::remove_file(fpath).await?;
            } else {
                tokio::fs::remove_dir_all(fpath).await?;
            }
        }
        check_dirs_identical(&test_path.join("foo"), &test_path.join("bar")).await?;
        Ok(())
    }
}
