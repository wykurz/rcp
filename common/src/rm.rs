use anyhow::{Context, Result};
use async_recursion::async_recursion;
use std::os::unix::fs::PermissionsExt;

use crate::progress;

#[derive(Debug, Clone)]
pub struct Settings {
    pub fail_early: bool,
}

#[async_recursion]
pub async fn rm(
    prog_track: &'static progress::TlsProgress,
    path: &std::path::Path,
    settings: &Settings,
) -> Result<()> {
    debug!("remove: {:?}", path);
    let _guard = prog_track.guard();
    let src_metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("rrm: failed reading metadata from {:?}", &path))?;
    if !src_metadata.is_dir() {
        return tokio::fs::remove_file(path)
            .await
            .with_context(|| format!("rrm: failed removing {:?}", &path));
    }
    if src_metadata.permissions().readonly() {
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o777))
            .await
            .with_context(|| {
                format!(
                    "rrm: while removing non-empty directory with no read permissions \
                    failed to modify to read permissions for {:?}",
                    &path
                )
            })?;
    }
    let mut entries = tokio::fs::read_dir(path).await?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut errors = vec![];
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("rrm: failed traversing directory {:?}", &path))?
    {
        let entry_path = entry.path();
        let settings = settings.clone();
        let do_rm = || async move { rm(prog_track, &entry_path, &settings).await };
        join_set.spawn(do_rm());
    }
    while let Some(res) = join_set.join_next().await {
        if let Err(error) = res? {
            errors.push(error);
        }
    }
    if !errors.is_empty() {
        debug!("remove: {:?} failed with: {:?}", path, &errors);
        return Err(anyhow::anyhow!("{:?}", &errors));
    }
    tokio::fs::remove_dir(path)
        .await
        .with_context(|| format!("rrm: failed removing directory {:?}", &path))?;
    debug!("remove: {:?} succeeded!", path);
    Ok(())
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
        rm(
            &PROGRESS,
            &test_path.join("foo"),
            &Settings { fail_early: false },
        )
        .await?;
        assert!(!test_path.join("foo").exists());
        Ok(())
    }
}
