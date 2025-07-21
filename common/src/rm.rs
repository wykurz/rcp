use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use std::os::unix::fs::PermissionsExt;
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
    fn new(source: anyhow::Error, summary: Summary) -> Self {
        Error { source, summary }
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub fail_early: bool,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Summary {
    pub files_removed: usize,
    pub symlinks_removed: usize,
    pub directories_removed: usize,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            files_removed: self.files_removed + other.files_removed,
            symlinks_removed: self.symlinks_removed + other.symlinks_removed,
            directories_removed: self.directories_removed + other.directories_removed,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "files removed: {}\n\
            symlinks removed: {}\n\
            directories removed: {}\n",
            self.files_removed, self.symlinks_removed, self.directories_removed
        )
    }
}

#[instrument(skip(prog_track))]
#[async_recursion]
pub async fn rm(
    prog_track: &'static progress::Progress,
    path: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    throttle::get_ops_token().await;
    let _ops_guard = prog_track.ops.guard();
    tracing::debug!("read path metadata");
    let src_metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &path))
        .map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))?;
    if !src_metadata.is_dir() {
        tracing::debug!("not a directory, just remove");
        tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("failed removing {:?}", &path))
            .map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))?;
        if src_metadata.file_type().is_symlink() {
            prog_track.symlinks_removed.inc();
            return Ok(Summary {
                symlinks_removed: 1,
                ..Default::default()
            });
        }
        prog_track.files_removed.inc();
        return Ok(Summary {
            files_removed: 1,
            ..Default::default()
        });
    }
    tracing::debug!("remove contents of the directory first");
    if src_metadata.permissions().readonly() {
        tracing::debug!("directory is read-only - change the permissions");
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))
            .await
            .with_context(|| {
                format!(
                    "failed to make '{:?}' directory readable and writeable",
                    &path
                )
            })
            .map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))?;
    }
    let mut entries = tokio::fs::read_dir(path)
        .await
        .map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &path))
        .map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))?
    {
        let entry_path = entry.path();
        let settings = settings.clone();
        let do_rm = || async move { rm(prog_track, &entry_path, &settings).await };
        join_set.spawn(do_rm());
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(entries);
    let mut rm_summary = Summary {
        directories_removed: 0,
        ..Default::default()
    };
    while let Some(res) = join_set.join_next().await {
        match res.map_err(|err| Error::new(anyhow::Error::msg(err), Default::default()))? {
            Ok(summary) => rm_summary = rm_summary + summary,
            Err(error) => {
                tracing::error!("remove: {:?} failed with: {:?}", path, &error);
                rm_summary = rm_summary + error.summary;
                if settings.fail_early {
                    return Err(Error::new(error.source, rm_summary));
                }
                success = false;
            }
        }
    }
    if !success {
        return Err(Error::new(anyhow!("rm: {:?} failed!", &path), rm_summary));
    }
    tracing::debug!("finally remove the empty directory");
    tokio::fs::remove_dir(path)
        .await
        .with_context(|| format!("failed removing directory {:?}", &path))
        .map_err(|err| Error::new(anyhow::Error::msg(err), rm_summary))?;
    prog_track.directories_removed.inc();
    rm_summary.directories_removed += 1;
    Ok(rm_summary)
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
    async fn no_write_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let filepaths = vec![
            test_path.join("foo").join("0.txt"),
            test_path.join("foo").join("bar").join("2.txt"),
            test_path.join("foo").join("baz").join("4.txt"),
            test_path.join("foo").join("baz"),
        ];
        for fpath in &filepaths {
            // change file permissions to not readable and not writable
            tokio::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o555)).await?;
        }
        let summary = rm(
            &PROGRESS,
            &test_path.join("foo"),
            &Settings { fail_early: false },
        )
        .await?;
        assert!(!test_path.join("foo").exists());
        assert_eq!(summary.files_removed, 5);
        assert_eq!(summary.symlinks_removed, 2);
        assert_eq!(summary.directories_removed, 3);
        Ok(())
    }
}
