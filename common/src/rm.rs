use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use std::os::unix::fs::PermissionsExt;
use tracing::{event, instrument, Level};

use crate::progress;
use crate::throttle;

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct RmError {
    #[source]
    pub source: anyhow::Error,
    pub summary: RmSummary,
}

impl RmError {
    fn new(source: anyhow::Error, summary: RmSummary) -> Self {
        RmError { source, summary }
    }
}

#[derive(Debug, Clone)]
pub struct RmSettings {
    pub fail_early: bool,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct RmSummary {
    pub files_removed: usize,
    pub symlinks_removed: usize,
    pub directories_removed: usize,
}

impl std::ops::Add for RmSummary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            files_removed: self.files_removed + other.files_removed,
            symlinks_removed: self.symlinks_removed + other.symlinks_removed,
            directories_removed: self.directories_removed + other.directories_removed,
        }
    }
}

impl std::fmt::Display for RmSummary {
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
    settings: &RmSettings,
) -> Result<RmSummary, RmError> {
    throttle::get_token().await;
    let _ops_guard = prog_track.ops.guard();
    event!(Level::DEBUG, "read path metadata");
    let src_metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &path))
        .map_err(|err| RmError::new(anyhow::Error::msg(err), Default::default()))?;
    if !src_metadata.is_dir() {
        event!(Level::DEBUG, "not a directory, just remove");
        tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("failed removing {:?}", &path))
            .map_err(|err| RmError::new(anyhow::Error::msg(err), Default::default()))?;
        if src_metadata.file_type().is_symlink() {
            prog_track.symlinks_removed.inc();
            return Ok(RmSummary {
                symlinks_removed: 1,
                ..Default::default()
            });
        }
        prog_track.files_removed.inc();
        return Ok(RmSummary {
            files_removed: 1,
            ..Default::default()
        });
    }
    event!(Level::DEBUG, "remove contents of the directory first");
    if src_metadata.permissions().readonly() {
        event!(
            Level::DEBUG,
            "directory is read-only - change the permissions"
        );
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))
            .await
            .with_context(|| {
                format!(
                    "failed to make '{:?}' directory readable and writeable",
                    &path
                )
            })
            .map_err(|err| RmError::new(anyhow::Error::msg(err), Default::default()))?;
    }
    let mut entries = tokio::fs::read_dir(path)
        .await
        .map_err(|err| RmError::new(anyhow::Error::msg(err), Default::default()))?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &path))
        .map_err(|err| RmError::new(anyhow::Error::msg(err), Default::default()))?
    {
        let entry_path = entry.path();
        let settings = settings.clone();
        let do_rm = || async move { rm(prog_track, &entry_path, &settings).await };
        join_set.spawn(do_rm());
    }
    let mut rm_summary = RmSummary {
        directories_removed: 0,
        ..Default::default()
    };
    while let Some(res) = join_set.join_next().await {
        match res.map_err(|err| RmError::new(anyhow::Error::msg(err), Default::default()))? {
            Ok(summary) => rm_summary = rm_summary + summary,
            Err(error) => {
                event!(Level::ERROR, "remove: {:?} failed with: {}", path, &error);
                rm_summary = rm_summary + error.summary;
                if settings.fail_early {
                    return Err(RmError::new(error.source, rm_summary));
                }
                success = false;
            }
        }
    }
    if !success {
        return Err(RmError::new(anyhow!("rm: {:?} failed!", &path), rm_summary));
    }
    event!(Level::DEBUG, "finally remove the empty directory");
    tokio::fs::remove_dir(path)
        .await
        .with_context(|| format!("failed removing directory {:?}", &path))
        .map_err(|err| RmError::new(anyhow::Error::msg(err), rm_summary))?;
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
            &RmSettings { fail_early: false },
        )
        .await?;
        assert!(!test_path.join("foo").exists());
        assert_eq!(summary.files_removed, 5);
        assert_eq!(summary.symlinks_removed, 2);
        assert_eq!(summary.directories_removed, 3);
        Ok(())
    }
}
