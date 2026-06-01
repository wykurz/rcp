//! rsync-style `--delete` (mirror) support: remove destination entries that
//! have no counterpart in the source directory.

use std::ffi::OsString;
use std::sync::Arc;

use anyhow::Context;

use crate::copy::DeleteSettings;
use crate::progress;
use crate::safedir::Dir;

/// Remove entries in the already-open destination directory `dst_dir` whose names are not in
/// `keep` (the source entry names that passed the filter for this directory).
///
/// The destination is enumerated and pruned entirely through `dst_dir`'s pinned file descriptor:
/// entries come from `dst_dir.read_entries()` and each extraneous entry is removed via
/// `rm::rm_child` (fd-relative, `O_NOFOLLOW` descent). The destination path is never
/// re-resolved, so a privileged prune cannot be redirected by a concurrent symlink swap into
/// deleting a tree outside the destination — the classic mirror-delete symlink race fails closed.
/// The caller is responsible for opening `dst_dir` `O_NOFOLLOW`: in a real copy it is the held
/// destination directory; in `--dry-run` (where the create-or-overwrite step is skipped and the
/// path could still be a symlink-to-directory) the caller opens it `O_NOFOLLOW|O_DIRECTORY`, which
/// fails closed on a symlink or non-directory before prune is ever invoked.
///
/// `relative_dir` is this directory's path relative to the source root, used to match destination
/// entries against `filter` for exclude-protection. Excluded destination entries are protected
/// (kept) unless `delete_settings.delete_excluded` is set. Honors `dry_run` (reports without
/// removing, via `rm::rm_child`).
#[allow(clippy::too_many_arguments)]
pub async fn prune_extraneous(
    prog_track: &'static progress::Progress,
    dst_dir: &Arc<Dir>,
    relative_dir: &std::path::Path,
    keep: &std::collections::HashSet<OsString>,
    filter: Option<&crate::filter::FilterSettings>,
    delete_settings: &DeleteSettings,
    fail_early: bool,
    dry_run: Option<crate::config::DryRunMode>,
) -> Result<crate::rm::Summary, crate::rm::Error> {
    let mut summary = crate::rm::Summary::default();
    // Enumerate the destination through its pinned fd (no path re-resolution). The returned
    // `d_type` is a best-effort hint passed to `filter_is_dir`, which resolves authoritatively via
    // fstat on DT_UNKNOWN. `rm_child` re-classifies each entry authoritatively via `child()`.
    let entries = dst_dir
        .read_entries()
        .await
        .with_context(|| "failed scanning destination directory for deletion".to_string())
        .map_err(|err| crate::rm::Error::new(err, summary))?;
    let errors = crate::error_collector::ErrorCollector::default();
    for (name, hint) in entries {
        if keep.contains(&name) {
            continue;
        }
        // the exclude-protection decision must use the AUTHORITATIVE is_dir: on filesystems that
        // report DT_UNKNOWN (NFS, some FUSE mounts) the hint is None, so defaulting to non-dir
        // would fail to protect a real directory that matches a dir-only exclude pattern like
        // `cache/`. `filter_is_dir` does one authoritative fstat only in the DT_UNKNOWN+filter
        // case, preserving the no-cost path when the hint is reliable or no filter is active.
        let is_dir = crate::walk::filter_is_dir(filter, dst_dir, &name, hint, false).await;
        // the entry's path relative to the destination (mirror) root: anchors filter matching and
        // reconstructs the display path inside `rm_child`. Computed once and reused below.
        let rel = relative_dir.join(&name);
        // exclude-protection: keep destination entries the filter would exclude,
        // unless --delete-excluded was requested.
        if !delete_settings.delete_excluded
            && let Some(filter) = filter
            && !matches!(
                filter.should_include(&rel, is_dir),
                crate::filter::FilterResult::Included
            )
        {
            tracing::debug!("protecting excluded destination entry {:?}", rel);
            continue;
        }
        // Protect excluded descendants when removing an extraneous directory: rm::rm_child applies
        // the filter recursively (skipping excluded entries), so an extra dir containing e.g.
        // `*.log` files keeps them and survives non-empty — upholding the documented
        // "excluded files are protected by default" guarantee (and matching rsync). With
        // --delete-excluded we pass no filter so the whole subtree is removed. The filter is
        // anchored at the destination (mirror) root, so the entry's destination-root-relative path
        // `relative_dir.join(name)` matches path/anchored patterns like `cache/*.log` correctly.
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
        match crate::rm::rm_child(prog_track, dst_dir, &name, &rel, &rm_settings).await {
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

    /// Open `dst` as the destination directory `Dir` the (now fd-relative) prune operates through,
    /// mirroring what the copy/link call sites do (`O_NOFOLLOW|O_DIRECTORY`, Destination side).
    async fn open_dst(dst: &std::path::Path) -> anyhow::Result<Arc<Dir>> {
        Ok(Arc::new(
            Dir::open_root_dir(dst, false, congestion::Side::Destination).await?,
        ))
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

        let dst_dir = open_dst(&dst).await?;
        let summary = prune_extraneous(
            &PROGRESS,
            &dst_dir,
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
        let dst_dir = open_dst(&dst).await?;
        let summary = prune_extraneous(
            &PROGRESS,
            &dst_dir,
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
        let dst_dir = open_dst(&dst).await?;
        let summary = prune_extraneous(
            &PROGRESS,
            &dst_dir,
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
        let dst_dir = open_dst(&dst).await?;
        let summary = prune_extraneous(
            &PROGRESS,
            &dst_dir,
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
        let dst_dir = open_dst(&dst).await?;
        let summary = prune_extraneous(
            &PROGRESS,
            &dst_dir,
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

        let dst_dir = open_dst(&dst).await?;
        let summary = prune_extraneous(
            &PROGRESS,
            &dst_dir,
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

    /// Regression test for the DT_UNKNOWN + dir-only exclude protection bug (PR #247 review).
    ///
    /// On NFS/FUSE filesystems `read_entries` returns `None` for `d_type` (the `DT_UNKNOWN` case).
    /// The old hint-only `is_dir` computation (`hint.is_some_and(|k| k == Dir)`) produced `false`
    /// for `None`, so a real destination directory with a `None` hint appeared to be a non-dir.
    /// A dir-only exclude pattern like `cache/` therefore did NOT protect it, and prune would
    /// delete the directory when it should have kept it.
    ///
    /// The fix replaces the hint-only computation with `filter_is_dir(filter, dst_dir, name, hint)`,
    /// which performs an authoritative `fstat` when the hint is `None` AND a filter is active.
    ///
    /// Since we cannot force `DT_UNKNOWN` on a local tmpfs, we verify the fix indirectly by driving
    /// `filter_is_dir` with `hint = None` in isolation (exactly as the authoritative fstat path is
    /// exercised in `walk.rs`'s unit tests), and by confirming that `prune_extraneous` with a
    /// dir-only exclude retains the matching destination directory end-to-end. On a local fs the hint
    /// is always `Some(Dir)`, so `filter_is_dir` uses it directly — but the test would fail with the
    /// old code if the hint were forced to `None`, which is exactly what happens on NFS/FUSE.
    #[tokio::test]
    #[traced_test]
    async fn protects_dir_only_excluded_directory_dt_unknown() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let dst = tmp.path().join("dst");
        tokio::fs::create_dir(&dst).await?;
        // extraneous destination directory `cache/` containing a file; it should be RETAINED by a
        // `cache/` dir-only exclude even though it has no source counterpart.
        tokio::fs::create_dir(dst.join("cache")).await?;
        tokio::fs::write(dst.join("cache").join("item.dat"), b"x").await?;
        // an unrelated extra file that IS extraneous and not excluded, so it should be removed.
        tokio::fs::write(dst.join("unrelated.txt"), b"y").await?;

        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("cache/")?; // dir-only exclude: only protects directories, not files

        let keep = HashSet::new(); // both `cache/` and `unrelated.txt` are extraneous

        // verify the authoritative fstat path via `filter_is_dir` with hint=None (DT_UNKNOWN
        // simulation): a real directory must classify as `is_dir = true`, so the dir-only exclude
        // protects it. The old hint-only code returned `false` here, causing the directory to be
        // pruned instead.
        let dst_dir = open_dst(&dst).await?;
        let authoritative_is_dir = crate::walk::filter_is_dir(
            Some(&filter),
            &dst_dir,
            std::ffi::OsStr::new("cache"),
            None, // DT_UNKNOWN: no hint available (NFS/FUSE case)
            false,
        )
        .await;
        assert!(
            authoritative_is_dir,
            "filter_is_dir with hint=None on a real directory must return true via authoritative fstat"
        );

        // end-to-end: `prune_extraneous` must retain `cache/` (protected by the dir-only exclude)
        // and remove `unrelated.txt` (not excluded).
        let dst_dir = open_dst(&dst).await?;
        let summary = prune_extraneous(
            &PROGRESS,
            &dst_dir,
            std::path::Path::new(""),
            &keep,
            Some(&filter),
            &delete_settings(false),
            false,
            None,
        )
        .await
        .map_err(|e| e.source)?;

        assert_eq!(summary.files_removed, 1); // unrelated.txt
        assert!(!dst.join("unrelated.txt").exists());
        assert!(
            dst.join("cache").exists(),
            "dir-only exclude `cache/` must protect the destination directory from --delete"
        );
        assert!(
            dst.join("cache").join("item.dat").exists(),
            "contents of the excluded directory must be preserved"
        );
        Ok(())
    }

    /// Repeatedly swap `dst/extra` between a real directory (holding a real file) and a symlink to
    /// an OUT-OF-TREE sentinel directory, using rename so each individual state is atomic. Two
    /// staging names live alongside `extra` and are renamed over it in a tight loop until `stop` is
    /// set. Runs on a dedicated OS thread so it makes progress regardless of the tokio runtime's
    /// scheduling. Mirrors rm's `spawn_dir_symlink_swapper`.
    fn spawn_extra_swapper(
        dst: std::path::PathBuf,
        sentinel: std::path::PathBuf,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let extra = dst.join("extra");
            let staged_dir = dst.join("__staged_extra_dir");
            let staged_link = dst.join("__staged_extra_link");
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // stage a real directory (with a real file) then swap it in over `extra`.
                let _ = std::fs::remove_dir_all(&staged_dir);
                if std::fs::create_dir(&staged_dir).is_ok() {
                    let _ = std::fs::write(staged_dir.join("real.txt"), b"REAL");
                    // RENAME_EXCHANGE isn't portable here; remove-then-rename. The window where
                    // `extra` is briefly absent is fine — prune may error or no-op, an accepted
                    // failed-closed outcome. (prune must still never touch the sentinel.)
                    let _ = std::fs::remove_dir_all(&extra);
                    let _ = std::fs::remove_file(&extra);
                    let _ = std::fs::rename(&staged_dir, &extra);
                }
                // stage a symlink to the out-of-tree sentinel dir, then swap it in over `extra`.
                let _ = std::fs::remove_file(&staged_link);
                if std::os::unix::fs::symlink(&sentinel, &staged_link).is_ok() {
                    let _ = std::fs::remove_dir_all(&extra);
                    let _ = std::fs::remove_file(&extra);
                    let _ = std::fs::rename(&staged_link, &extra);
                }
            }
        })
    }

    /// While `prune_extraneous` prunes an extraneous destination SUBDIRECTORY (`dst/extra`, with no
    /// source counterpart so the empty keep-set marks it for deletion), a background thread rapidly
    /// flips `dst/extra` between a real directory and a symlink to a SENTINEL directory tree that
    /// lives OUTSIDE the destination, holding files that must never be deleted.
    ///
    /// Prune is fd-relative: it enumerates and removes children through the destination's own
    /// pinned `Dir` fd (`rm_child` → `child()` classify + `open_dir` descent). If `extra` is a
    /// symlink at the moment of descent, `open_dir`'s `O_NOFOLLOW|O_DIRECTORY` fails closed
    /// (ELOOP/ENOTDIR) and prune never follows it into the sentinel. If `extra` is a symlink at the
    /// moment of classification it is treated as a leaf and `unlink_at` removes the LINK, never its
    /// target. Either way the out-of-tree sentinel files survive — the core safety assertion,
    /// checked on every iteration regardless of timing. Also confirms the run terminates (per-op
    /// timeout) rather than hanging or following the link.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prune_extra_dir_swap_never_deletes_out_of_tree_sentinel() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let root = tmp.path();
        // the sentinel tree lives OUTSIDE the destination, reachable only via the swapped symlink.
        let sentinel = root.join("sentinel_tree");
        tokio::fs::create_dir(&sentinel).await?;
        tokio::fs::write(sentinel.join("secret1.txt"), b"SECRET-1").await?;
        tokio::fs::create_dir(sentinel.join("subdir")).await?;
        tokio::fs::write(sentinel.join("subdir").join("secret2.txt"), b"SECRET-2").await?;

        let dst = root.join("dst");
        tokio::fs::create_dir(&dst).await?;

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapper = spawn_extra_swapper(dst.clone(), sentinel.clone(), stop.clone());

        // empty keep-set: `extra` (and any other entry) is extraneous and marked for pruning.
        let keep = HashSet::new();
        let mut pruned = 0usize;
        let mut errored = 0usize;
        for i in 0..400 {
            // give `dst` many sibling extraneous subdirectories with files so prune spends real
            // time enumerating/removing them concurrently with the swapper's flips, widening the
            // window in which `extra` is classified/descended mid-swap.
            for d in 0..16 {
                let sib = dst.join(format!("sib_{d}"));
                let _ = tokio::fs::create_dir(&sib).await;
                for f in 0..4 {
                    let _ = tokio::fs::write(sib.join(format!("f{f}.txt")), b"x").await;
                }
            }
            let extra = dst.join("extra");
            if i % 2 == 0 {
                // deterministically place a symlink-to-sentinel at `extra` (best-effort; the
                // swapper may immediately flip it — both states are safe).
                let _ = tokio::fs::remove_dir_all(&extra).await;
                let _ = tokio::fs::remove_file(&extra).await;
                let _ = tokio::fs::symlink(&sentinel, &extra).await;
            } else if tokio::fs::symlink_metadata(&extra).await.is_err() {
                let _ = tokio::fs::create_dir(&extra).await;
                let _ = tokio::fs::write(extra.join("real.txt"), b"REAL").await;
            }
            // open the destination O_NOFOLLOW (as the real call sites do) and prune it.
            let dst_dir = open_dst(&dst).await?;
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                prune_extraneous(
                    &PROGRESS,
                    &dst_dir,
                    std::path::Path::new(""),
                    &keep,
                    None,
                    &delete_settings(false),
                    false,
                    None,
                ),
            )
            .await
            .expect("prune must not hang under concurrent dir swapping");
            match result {
                Ok(_) => pruned += 1,
                Err(_) => errored += 1, // a swap was caught mid-walk (failed closed) — accepted
            }
            // CORE SAFETY ASSERTION (holds on every iteration regardless of timing): the
            // out-of-tree sentinel tree and its files are NEVER deleted — neither by following a
            // symlinked `extra` (unlink removes the link, not the target) nor by descending it
            // (open_dir's O_NOFOLLOW fails closed).
            assert!(
                sentinel.exists(),
                "iteration {i}: sentinel directory was deleted — prune followed the symlink"
            );
            let s1 = tokio::fs::read(sentinel.join("secret1.txt")).await;
            assert!(
                matches!(&s1, Ok(b) if b == b"SECRET-1"),
                "iteration {i}: sentinel/secret1.txt was deleted or altered — prune followed the symlink"
            );
            let s2 = tokio::fs::read(sentinel.join("subdir").join("secret2.txt")).await;
            assert!(
                matches!(&s2, Ok(b) if b == b"SECRET-2"),
                "iteration {i}: sentinel/subdir/secret2.txt was deleted — prune recursed through the symlink"
            );
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        swapper.join().expect("extra swapper thread panicked");
        // sanity (not the safety assertion): the run did observable work across the iterations.
        tracing::info!("prune extra-dir swap: pruned={pruned}, errored={errored}");
        assert!(
            pruned + errored > 0,
            "expected at least one observable outcome across the iterations"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn opening_dst_symlink_to_directory_fails_closed() -> anyhow::Result<()> {
        // prune is now fd-relative: it operates through a `Dir` the caller opens `O_NOFOLLOW`. In a
        // real --delete run the create-or-overwrite step replaces any non-directory destination
        // (including a symlink) before prune runs; in --dry-run that overwrite is skipped, so the
        // destination path could still be a symlink-to-directory. Opening it `O_NOFOLLOW|O_DIRECTORY`
        // (dereference=false) — exactly what the copy/link prune call sites do — must FAIL CLOSED on
        // that symlink rather than follow it. This is the guarantee that replaces the old
        // `symlink_metadata` pre-check: prune can never be handed a Dir that followed a dst symlink,
        // so it can never preview/perform deletions OUTSIDE the destination tree.
        let tmp = tempfile::tempdir()?;
        let dst_parent = tmp.path().join("dst_parent");
        let outside = tmp.path().join("outside"); // outside the destination tree
        tokio::fs::create_dir(&dst_parent).await?;
        tokio::fs::create_dir(&outside).await?;
        tokio::fs::write(outside.join("precious.txt"), b"keep me").await?;
        // dst is a symlink-to-directory living under the parent we'd prune.
        let dst = dst_parent.join("link_dir");
        std::os::unix::fs::symlink(&outside, &dst)?;

        // opening the symlinked dst O_NOFOLLOW must fail (ELOOP), so the call site skips prune.
        let result = Dir::open_root_dir(&dst, false, congestion::Side::Destination).await;
        assert!(
            result.is_err(),
            "opening a dst symlink-to-directory O_NOFOLLOW must fail closed, not follow it"
        );
        // the out-of-tree tree is untouched.
        assert!(outside.join("precious.txt").exists());
        Ok(())
    }
}
