#![allow(dead_code)]

use anyhow::{Context, Result};
use async_recursion::async_recursion;
use std::os::unix::fs::MetadataExt;

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

pub async fn setup_test_dir() -> Result<std::path::PathBuf> {
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
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    Ok(tmp_dir)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileEqualityCheck {
    Basic,
    Timestamp,
    HardLink,
}

#[async_recursion]
pub async fn check_dirs_identical(
    src: &std::path::Path,
    dst: &std::path::Path,
    file_eqality_check: FileEqualityCheck,
) -> Result<()> {
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
        // compare file type and content
        assert_eq!(src_md.file_type(), dst_md.file_type());
        if src_md.is_file() {
            if file_eqality_check == FileEqualityCheck::HardLink {
                assert_eq!(src_md.ino(), dst_md.ino());
            } else {
                let src_contents = tokio::fs::read_to_string(&src_entry_path).await?;
                let dst_contents = tokio::fs::read_to_string(&dst_entry_path).await?;
                assert_eq!(src_contents, dst_contents);
            }
        } else if src_md.file_type().is_symlink() {
            let src_link = tokio::fs::read_link(&src_entry_path).await?;
            let dst_link = tokio::fs::read_link(&dst_entry_path).await?;
            assert_eq!(src_link, dst_link);
        } else {
            check_dirs_identical(&src_entry_path, &dst_entry_path, file_eqality_check).await?;
        }
        // compare permissions
        assert_eq!(src_md.permissions(), dst_md.permissions());
        if file_eqality_check != FileEqualityCheck::Timestamp {
            continue;
        }
        // compare timestamps
        // NOTE: skip comparing "atime" - we read the file few times when comparing agaisnt "cp"
        assert_eq!(
            src_md.mtime_nsec(),
            dst_md.mtime_nsec(),
            "mtime doesn't match for {:?} {:?}",
            src_entry_path,
            dst_entry_path
        );
    }
    Ok(())
}
