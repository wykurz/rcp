use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use tracing::instrument;

use crate::progress;

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

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    pub files_created: usize,
    pub directories_created: usize,
    pub bytes_written: u64,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            files_created: self.files_created + other.files_created,
            directories_created: self.directories_created + other.directories_created,
            bytes_written: self.bytes_written + other.bytes_written,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "files created: {}\n\
            directories created: {}\n\
            bytes written: {}",
            self.files_created,
            self.directories_created,
            bytesize::ByteSize(self.bytes_written)
        )
    }
}

#[instrument(skip(prog_track))]
pub async fn write_file(
    prog_track: &'static progress::Progress,
    path: std::path::PathBuf,
    mut filesize: usize,
    bufsize: usize,
    chunk_size: u64,
) -> Result<Summary, Error> {
    use rand::Rng;
    use tokio::io::AsyncWriteExt;
    let _permit = throttle::open_file_permit().await;
    throttle::get_file_iops_tokens(chunk_size, filesize as u64).await;
    let _ops_guard = prog_track.ops.guard();
    let original_filesize = filesize;
    let mut bytes = vec![0u8; bufsize];
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .await
        .with_context(|| format!("Error opening {:?}", &path))
        .map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))?;
    while filesize > 0 {
        {
            // make sure rng falls out of scope before await
            let mut rng = rand::thread_rng();
            rng.fill(&mut bytes[..]);
        }
        let writesize = std::cmp::min(filesize, bufsize) as usize;
        file.write_all(&bytes[..writesize])
            .await
            .with_context(|| format!("Error writing to {:?}", &path))
            .map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))?;
        filesize -= writesize;
    }
    prog_track.files_copied.inc();
    prog_track.bytes_copied.add(original_filesize as u64);
    Ok(Summary {
        files_created: 1,
        bytes_written: original_filesize as u64,
        ..Default::default()
    })
}

#[async_recursion]
#[instrument(skip(prog_track))]
pub async fn filegen(
    prog_track: &'static progress::Progress,
    root: &std::path::Path,
    dirwidth: &[usize],
    numfiles: usize,
    filesize: usize,
    writebuf: usize,
    chunk_size: u64,
) -> Result<Summary, Error> {
    let numdirs = *dirwidth.first().unwrap_or(&0);
    let mut join_set = tokio::task::JoinSet::new();
    // generate directories and recurse into them
    for i in 0..numdirs {
        let path = root.join(format!("dir{i}"));
        let dirwidth = dirwidth[1..].to_owned();
        let recurse = || async move {
            tokio::fs::create_dir(&path)
                .await
                .with_context(|| format!("Error creating directory {:?}", &path))
                .map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))?;
            prog_track.directories_created.inc();
            let dir_summary = Summary {
                directories_created: 1,
                ..Default::default()
            };
            let recurse_summary = filegen(
                prog_track, &path, &dirwidth, numfiles, filesize, writebuf, chunk_size,
            )
            .await?;
            Ok(dir_summary + recurse_summary)
        };
        join_set.spawn(recurse());
    }
    // generate files
    for i in 0..numfiles {
        // it's better to await the token here so that we throttle how many tasks we spawn. the
        // ops-throttle will never cause a deadlock (unlike max-open-files limit) so it's safe to
        // do here.
        throttle::get_ops_token().await;
        let path = root.join(format!("file{i}"));
        join_set.spawn(write_file(prog_track, path, filesize, writebuf, chunk_size));
    }
    let mut success = true;
    let mut filegen_summary = Summary::default();
    while let Some(res) = join_set.join_next().await {
        match res.map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))? {
            Ok(summary) => filegen_summary = filegen_summary + summary,
            Err(error) => {
                tracing::error!("filegen: {:?} failed with: {:?}", root, &error);
                filegen_summary = filegen_summary + error.summary;
                success = false;
            }
        }
    }
    if !success {
        return Err(Error::new(
            anyhow!("filegen: {:?} failed!", &root),
            filegen_summary,
        ));
    }
    Ok(filegen_summary)
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
    async fn test_basic_filegen() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate 2 subdirectories with 3 files per directory (including root)
        let summary = filegen(
            &PROGRESS,
            test_path,
            &[2],
            3,
            100,
            50, // buffer size
            0,  // chunk_size
        )
        .await?;
        // verify summary
        // files: 3 (in root) + 3 (in dir0) + 3 (in dir1) = 9 files
        // directories: 2 (dir0, dir1)
        // bytes: 100 bytes × 9 files = 900 bytes
        assert_eq!(summary.files_created, 9);
        assert_eq!(summary.directories_created, 2);
        assert_eq!(summary.bytes_written, 900);
        // verify files were actually created
        assert!(test_path.join("file0").exists()); // root level files
        assert!(test_path.join("dir0").join("file0").exists());
        assert!(test_path.join("dir0").join("file1").exists());
        assert!(test_path.join("dir0").join("file2").exists());
        assert!(test_path.join("dir1").join("file0").exists());
        assert!(test_path.join("dir1").join("file1").exists());
        assert!(test_path.join("dir1").join("file2").exists());
        // verify file sizes
        let metadata = tokio::fs::metadata(test_path.join("dir0").join("file0")).await?;
        assert_eq!(metadata.len(), 100);
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_nested_filegen() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate nested structure: 2 top-level dirs, each with 3 subdirs, 4 files per dir, 50 bytes each
        let summary = filegen(
            &PROGRESS,
            test_path,
            &[2, 3],
            4,
            50,
            25, // buffer size
            0,  // chunk_size
        )
        .await?;
        // calculate expected values:
        // directories: 2 top-level + (2 × 3) subdirs = 8 total
        // files: 4 (in root) + 4×2 (in dir0, dir1) + 4×2×3 (in all leaf dirs) = 4 + 8 + 24 = 36 files
        // bytes: 50 bytes × 36 files = 1800 bytes
        assert_eq!(summary.files_created, 36);
        assert_eq!(summary.directories_created, 8);
        assert_eq!(summary.bytes_written, 1800);
        // spot check some files exist
        assert!(test_path.join("file0").exists()); // root files
        assert!(test_path.join("dir0").join("file0").exists()); // top-level dir files
        assert!(test_path.join("dir0").join("dir0").join("file0").exists());
        assert!(test_path.join("dir0").join("dir2").join("file3").exists());
        assert!(test_path.join("dir1").join("dir1").join("file2").exists());
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_deeply_nested_filegen() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate 3 levels: 2,2,2 with 2 files each, 10 bytes per file
        let summary = filegen(
            &PROGRESS,
            test_path,
            &[2, 2, 2],
            2,
            10,
            10, // buffer size
            0,  // chunk_size
        )
        .await?;
        // directories: 2 + (2×2) + (2×2×2) = 2 + 4 + 8 = 14 dirs
        // files: 2 (root) + 2×2 (level 1) + 2×2×2 (level 2) + 2×2×2×2 (level 3) = 2 + 4 + 8 + 16 = 30 files
        // bytes: 10 bytes × 30 files = 300 bytes
        assert_eq!(summary.files_created, 30);
        assert_eq!(summary.directories_created, 14);
        assert_eq!(summary.bytes_written, 300);
        // verify deep nesting works
        assert!(test_path.join("file0").exists()); // root files
        assert!(test_path
            .join("dir0")
            .join("dir0")
            .join("dir0")
            .join("file0")
            .exists());
        assert!(test_path
            .join("dir1")
            .join("dir1")
            .join("dir1")
            .join("file1")
            .exists());
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_single_file() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate just files, no directories
        let summary = filegen(
            &PROGRESS,
            test_path,
            &[], // no subdirectories
            5,   // 5 files
            200, // 200 bytes each
            100, // buffer size
            0,   // chunk_size
        )
        .await?;
        assert_eq!(summary.files_created, 5);
        assert_eq!(summary.directories_created, 0);
        assert_eq!(summary.bytes_written, 1000); // 200 × 5
        for i in 0..5 {
            // verify files
            let file_path = test_path.join(format!("file{}", i));
            assert!(file_path.exists());
            let metadata = tokio::fs::metadata(&file_path).await?;
            assert_eq!(metadata.len(), 200);
        }
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_zero_files() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate only directories, no files
        let summary = filegen(
            &PROGRESS,
            test_path,
            &[3, 2], // 3 top-level, each with 2 subdirs
            0,       // 0 files
            100,
            50,
            0,
        )
        .await?;
        // directories: 3 + (3×2) = 9 dirs
        assert_eq!(summary.files_created, 0);
        assert_eq!(summary.directories_created, 9);
        assert_eq!(summary.bytes_written, 0);
        // verify directories exist but no files
        assert!(test_path.join("dir0").join("dir0").exists());
        assert!(test_path.join("dir2").join("dir1").exists());
        assert!(!test_path.join("dir0").join("file0").exists());
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }
}
