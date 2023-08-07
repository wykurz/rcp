use anyhow::{Context,  Result};
use async_recursion::async_recursion;

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
    let mut entries = tokio::fs::read_dir(path)
        .await
        .with_context(|| format!("rrm: cannot open directory {:?} for reading", path))?;
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
