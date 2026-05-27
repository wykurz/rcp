//! rsync-style `--delete` (mirror) support: remove destination entries that
//! have no counterpart in the source directory.

use crate::copy::DeleteSettings;
use crate::progress;

/// Remove entries in `dst` whose names are not in `keep` (the source entry
/// names that passed the filter for this directory).
///
/// `relative_dir` is this directory's path relative to the source root, used to
/// match destination entries against `filter` for exclude-protection. Excluded
/// destination entries are protected (kept) unless `delete_settings.delete_excluded`
/// is set. Honors `dry_run` (reports without removing, via [`crate::rm::rm`]).
#[allow(clippy::too_many_arguments)]
pub async fn prune_extraneous(
    prog_track: &'static progress::Progress,
    dst: &std::path::Path,
    relative_dir: &std::path::Path,
    keep: &std::collections::HashSet<std::ffi::OsString>,
    filter: Option<&crate::filter::FilterSettings>,
    delete_settings: &DeleteSettings,
    fail_early: bool,
    dry_run: Option<crate::config::DryRunMode>,
) -> Result<crate::rm::Summary, crate::rm::Error> {
    let mut summary = crate::rm::Summary::default();
    // Destination root: `dst` with this directory's source-relative path stripped. Removed
    // descendants are matched against the filter relative to this root, so their full (mirror)
    // relative paths are used — making path/anchored excludes like `cache/*.log` protect
    // descendants correctly, not just simple basename patterns.
    let mut dest_root = dst;
    for _ in relative_dir.components() {
        dest_root = dest_root.parent().unwrap_or(dest_root);
    }
    // In --dry-run the create-or-overwrite step is skipped, so `dst` may still be a file,
    // symlink, or even a symlink-to-directory at this point. `read_dir` follows symlinks, so
    // without a `symlink_metadata` pre-check it would walk the symlink's target and preview
    // deletions OUTSIDE the destination tree. In a real run this can't happen — upstream
    // create_dir/overwrite guarantees `dst` is a real directory by the time prune runs — so
    // skip the extra stat there to keep the hot path cheap.
    if dry_run.is_some() {
        match tokio::fs::symlink_metadata(dst).await {
            Ok(meta) if meta.file_type().is_dir() => { /* real directory: fall through */ }
            Ok(_) => {
                // not a real directory (file, symlink, special) — nothing to prune
                return Ok(summary);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(summary);
            }
            Err(err) => {
                return Err(crate::rm::Error::new(
                    anyhow::Error::new(err)
                        .context(format!("cannot stat destination {dst:?} for delete scan")),
                    summary,
                ));
            }
        }
    }
    let mut entries = match tokio::fs::read_dir(dst).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // destination directory absent (e.g. dry-run never created it): nothing to prune
            return Ok(summary);
        }
        Err(err)
            if err.kind() == std::io::ErrorKind::NotADirectory
                || err.raw_os_error() == Some(20) =>
        {
            // destination counterpart is not a directory: e.g. `--dry-run --delete` where the real
            // run would replace a file/symlink at this path with a directory before pruning, but
            // dry-run skips that overwrite. There is nothing to prune. (In dry-run we already
            // handled the non-dir case via symlink_metadata above; this arm remains as defense in
            // depth against a race between that stat and the read_dir below.)
            return Ok(summary);
        }
        Err(err) => {
            return Err(crate::rm::Error::new(
                anyhow::Error::new(err)
                    .context(format!("cannot open destination {dst:?} for delete scan")),
                summary,
            ));
        }
    };
    let errors = crate::error_collector::ErrorCollector::default();
    loop {
        let (entry, entry_file_type) = match crate::walk::next_entry_probed(
            &mut entries,
            congestion::Side::Destination,
            || format!("failed scanning destination directory {dst:?} for deletion"),
        )
        .await
        {
            Ok(Some(value)) => value,
            Ok(None) => break,
            Err(err) => {
                errors.push(err);
                break;
            }
        };
        let name = entry.file_name();
        if keep.contains(&name) {
            continue;
        }
        let is_dir = entry_file_type.as_ref().is_some_and(|ft| ft.is_dir());
        // exclude-protection: keep destination entries the filter would exclude,
        // unless --delete-excluded was requested.
        if !delete_settings.delete_excluded
            && let Some(filter) = filter
        {
            let entry_relative = relative_dir.join(&name);
            if !matches!(
                filter.should_include(&entry_relative, is_dir),
                crate::filter::FilterResult::Included
            ) {
                tracing::debug!("protecting excluded destination entry {:?}", entry.path());
                continue;
            }
        }
        // Protect excluded descendants when removing an extraneous directory: rm::rm applies
        // the filter recursively (skipping excluded entries), so an extra dir containing e.g.
        // `*.log` files keeps them and survives non-empty — upholding the documented
        // "excluded files are protected by default" guarantee (and matching rsync). With
        // --delete-excluded we pass no filter so the whole subtree is removed. (rm matches
        // relative to the entry being removed, so simple patterns protect by basename.)
        let rm_settings = crate::rm::Settings {
            fail_early,
            filter: if delete_settings.delete_excluded {
                None
            } else {
                filter.cloned()
            },
            time_filter: None,
            dry_run,
        };
        match crate::rm::rm_with_filter_root(prog_track, &entry.path(), dest_root, &rm_settings)
            .await
        {
            Ok(rm_summary) => {
                summary = summary + rm_summary;
            }
            Err(err) => {
                summary = summary + err.summary;
                if fail_early {
                    return Err(crate::rm::Error::new(err.source, summary));
                }
                errors.push(err.source);
            }
        }
    }
    if let Some(err) = errors.into_error() {
        return Err(crate::rm::Error::new(err, summary));
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tracing_test::traced_test;

    static PROGRESS: std::sync::LazyLock<progress::Progress> =
        std::sync::LazyLock::new(progress::Progress::new);

    fn delete_settings(delete_excluded: bool) -> DeleteSettings {
        DeleteSettings { delete_excluded }
    }

    #[tokio::test]
    #[traced_test]
    async fn removes_entries_not_in_keep_set() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let dst = tmp.path().join("dst");
        tokio::fs::create_dir(&dst).await?;
        tokio::fs::write(dst.join("keep.txt"), b"x").await?;
        tokio::fs::write(dst.join("extra.txt"), b"x").await?;
        tokio::fs::create_dir(dst.join("extra_dir")).await?;
        tokio::fs::write(dst.join("extra_dir").join("nested.txt"), b"x").await?;

        let mut keep = HashSet::new();
        keep.insert(std::ffi::OsString::from("keep.txt"));

        let summary = prune_extraneous(
            &PROGRESS,
            &dst,
            std::path::Path::new(""),
            &keep,
            None,
            &delete_settings(false),
            false,
            None,
        )
        .await
        .map_err(|e| e.source)?;

        assert_eq!(summary.files_removed, 2); // extra.txt + extra_dir/nested.txt
        assert_eq!(summary.directories_removed, 1); // extra_dir
        assert!(dst.join("keep.txt").exists());
        assert!(!dst.join("extra.txt").exists());
        assert!(!dst.join("extra_dir").exists());
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn protects_excluded_entries_unless_delete_excluded() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let dst = tmp.path().join("dst");
        tokio::fs::create_dir(&dst).await?;
        tokio::fs::write(dst.join("data.bin"), b"x").await?; // extra, not excluded
        tokio::fs::write(dst.join("note.log"), b"x").await?; // extra, excluded by *.log

        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("*.log")?;
        let keep = HashSet::new(); // both are extraneous

        // default: *.log is protected, data.bin is removed
        let summary = prune_extraneous(
            &PROGRESS,
            &dst,
            std::path::Path::new(""),
            &keep,
            Some(&filter),
            &delete_settings(false),
            false,
            None,
        )
        .await
        .map_err(|e| e.source)?;
        assert_eq!(summary.files_removed, 1);
        assert!(!dst.join("data.bin").exists());
        assert!(
            dst.join("note.log").exists(),
            "*.log must be protected by default"
        );

        // with delete_excluded: note.log is also removed
        let summary = prune_extraneous(
            &PROGRESS,
            &dst,
            std::path::Path::new(""),
            &keep,
            Some(&filter),
            &delete_settings(true),
            false,
            None,
        )
        .await
        .map_err(|e| e.source)?;
        assert_eq!(summary.files_removed, 1);
        assert!(!dst.join("note.log").exists());
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn protects_excluded_descendants_of_extraneous_dir() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let dst = tmp.path().join("dst");
        tokio::fs::create_dir(&dst).await?;
        // an extraneous directory (no source counterpart) with an excluded and a normal file
        tokio::fs::create_dir(dst.join("extra_dir")).await?;
        tokio::fs::write(dst.join("extra_dir").join("keep.log"), b"x").await?; // excluded by *.log
        tokio::fs::write(dst.join("extra_dir").join("gone.txt"), b"x").await?; // not excluded

        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("*.log")?;
        let keep = HashSet::new(); // extra_dir is extraneous

        // default --delete: the excluded descendant is protected, so the dir survives non-empty
        let summary = prune_extraneous(
            &PROGRESS,
            &dst,
            std::path::Path::new(""),
            &keep,
            Some(&filter),
            &delete_settings(false),
            false,
            None,
        )
        .await
        .map_err(|e| e.source)?;
        assert_eq!(summary.files_removed, 1); // gone.txt
        assert!(!dst.join("extra_dir").join("gone.txt").exists());
        assert!(
            dst.join("extra_dir").join("keep.log").exists(),
            "excluded descendant of an extraneous dir must be protected"
        );

        // --delete-excluded: the whole extraneous directory is removed
        let summary = prune_extraneous(
            &PROGRESS,
            &dst,
            std::path::Path::new(""),
            &keep,
            Some(&filter),
            &delete_settings(true),
            false,
            None,
        )
        .await
        .map_err(|e| e.source)?;
        assert_eq!(summary.files_removed, 1); // keep.log
        assert!(!dst.join("extra_dir").exists());
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn protects_path_excluded_descendants_of_extraneous_dir() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let dst = tmp.path().join("dst");
        tokio::fs::create_dir(&dst).await?;
        // an extraneous directory whose descendants are targeted by a PATH-based exclude
        tokio::fs::create_dir(dst.join("cache")).await?;
        tokio::fs::write(dst.join("cache").join("foo.log"), b"x").await?; // matches cache/*.log -> protected
        tokio::fs::write(dst.join("cache").join("data.txt"), b"x").await?; // not matched -> removed

        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("cache/*.log")?;
        let keep = HashSet::new();

        let summary = prune_extraneous(
            &PROGRESS,
            &dst,
            std::path::Path::new(""),
            &keep,
            Some(&filter),
            &delete_settings(false),
            false,
            None,
        )
        .await
        .map_err(|e| e.source)?;

        assert_eq!(summary.files_removed, 1); // data.txt
        assert!(!dst.join("cache").join("data.txt").exists());
        assert!(
            dst.join("cache").join("foo.log").exists(),
            "path-based exclude must protect the descendant of an extraneous dir"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn dry_run_does_not_follow_dst_symlink_to_directory() -> anyhow::Result<()> {
        // In a real --delete run, the create-or-overwrite step replaces any non-directory
        // destination (including a symlink) before prune runs. In --dry-run that overwrite
        // is skipped, so prune_extraneous can be called with a dst that is still a symlink
        // pointing to a directory. `tokio::fs::read_dir` follows symlinks, so without a
        // pre-check it would walk the symlink's target and previews deletions OUTSIDE the
        // destination tree.
        let tmp = tempfile::tempdir()?;
        let dst_parent = tmp.path().join("dst_parent");
        let outside = tmp.path().join("outside"); // outside the destination tree
        tokio::fs::create_dir(&dst_parent).await?;
        tokio::fs::create_dir(&outside).await?;
        tokio::fs::write(outside.join("precious.txt"), b"keep me").await?;
        // dst is a symlink-to-directory living under the parent we'd prune.
        let dst = dst_parent.join("link_dir");
        std::os::unix::fs::symlink(&outside, &dst)?;

        // keep set empty: anything `read_dir(dst)` returns would look extraneous.
        let keep = HashSet::new();
        let summary = prune_extraneous(
            &PROGRESS,
            &dst,
            std::path::Path::new(""),
            &keep,
            None,
            &delete_settings(false),
            false,
            Some(crate::config::DryRunMode::Brief),
        )
        .await
        .map_err(|e| e.source)?;
        assert_eq!(
            summary.files_removed, 0,
            "dry-run must not preview deletions reached by following a dst symlink"
        );
        assert_eq!(summary.directories_removed, 0);
        // dry-run never deletes, but assert anyway as a belt-and-braces guard.
        assert!(outside.join("precious.txt").exists());
        Ok(())
    }
}
