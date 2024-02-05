use anyhow::{Context, Result};
use async_recursion::async_recursion;
use std::os::unix::fs::PermissionsExt;

use crate::progress;

#[derive(Debug, Clone)]
pub struct Settings {
    pub fail_early: bool,
}

#[derive(Copy, Clone, Default)]
pub struct RmSummary {
    pub files_removed: usize,
    pub directories_removed: usize,
}

impl std::ops::Add for RmSummary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            files_removed: self.files_removed + other.files_removed,
            directories_removed: self.directories_removed + other.directories_removed,
        }
    }
}

impl std::fmt::Display for RmSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "files removed: {}\ndirectories removed: {}",
            self.files_removed, self.directories_removed
        )
    }
}

#[async_recursion]
pub async fn rm(
    prog_track: &'static progress::TlsProgress,
    path: &std::path::Path,
    settings: &Settings,
) -> Result<RmSummary> {
    debug!("remove: {:?}", path);
    let _guard = prog_track.guard();
    let src_metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &path))?;
    if !src_metadata.is_dir() {
        tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("failed removing {:?}", &path))?;
        return Ok(RmSummary {
            files_removed: 1,
            ..Default::default()
        });
    }
    if src_metadata.permissions().readonly() {
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))
            .await
            .with_context(|| {
                format!(
                    "while removing non-empty directory with no read permissions \
                    failed to modify to read permissions for {:?}",
                    &path
                )
            })?;
    }
    let mut entries = tokio::fs::read_dir(path).await?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &path))?
    {
        let entry_path = entry.path();
        let settings = settings.clone();
        let do_rm = || async move { rm(prog_track, &entry_path, &settings).await };
        join_set.spawn(do_rm());
    }
    let mut rm_summary = RmSummary {
        directories_removed: 1,
        ..Default::default()
    };
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => rm_summary = rm_summary + summary,
            Err(error) => {
                error!("remove: {:?} failed with: {}", path, &error);
                if settings.fail_early {
                    return Err(error);
                }
                success = false;
            }
        }
    }
    if !success {
        return Err(anyhow::anyhow!("rm: {:?} failed!", &path));
    }
    tokio::fs::remove_dir(path)
        .await
        .with_context(|| format!("failed removing directory {:?}", &path))?;
    debug!("remove: {:?} succeeded!", path);
    Ok(rm_summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils;
    use test_log::test;

    lazy_static! {
        static ref PROGRESS: progress::TlsProgress = progress::TlsProgress::new();
    }

    #[test(tokio::test)]
    async fn no_write_permission() -> Result<()> {
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
        assert_eq!(summary.files_removed, 7); // we cound symlinks (there are 2) as files
        assert_eq!(summary.directories_removed, 3);
        Ok(())
    }
}
