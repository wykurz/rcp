//! Integration tests for `rlink --delete` (local mirror).

use std::process::Command;

fn rlink_bin() -> &'static str {
    env!("CARGO_BIN_EXE_rlink")
}

#[test]
fn delete_mirrors_source_removing_extraneous() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("stale.txt"), b"stale").unwrap();

    // no trailing slash: `dst` IS the mirror target; --delete implies --overwrite
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    assert!(dst.join("a.txt").exists());
    assert!(!dst.join("stale.txt").exists());
}

#[test]
fn delete_with_update_keeps_src_and_update_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::write(src.join("b.txt"), b"b").unwrap();
    std::fs::create_dir(&upd).unwrap();
    std::fs::write(upd.join("c.txt"), b"c").unwrap(); // update-only
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("stale.txt"), b"stale").unwrap();

    // keep-set = src ∪ update = {a,b,c}; stale.txt has no counterpart -> removed.
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    assert!(dst.join("a.txt").exists());
    assert!(dst.join("b.txt").exists());
    assert!(dst.join("c.txt").exists());
    assert!(!dst.join("stale.txt").exists());
}

#[test]
fn delete_excluded_removes_excluded_update_only_name() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::create_dir(&upd).unwrap();
    std::fs::write(upd.join("keep.txt"), b"k").unwrap();
    std::fs::write(upd.join("drop.log"), b"d").unwrap(); // update-only, excluded by *.log
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("drop.log"), b"stale").unwrap(); // pre-existing dst entry with the excluded name

    // With --delete-excluded, an excluded update-only name must NOT protect the
    // pre-existing destination entry from deletion.
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--delete-excluded")
        .arg("--exclude")
        .arg("*.log")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    assert!(dst.join("a.txt").exists());
    assert!(dst.join("keep.txt").exists());
    assert!(
        !dst.join("drop.log").exists(),
        "excluded update-only name must not protect a dst entry under --delete-excluded"
    );
}

#[test]
fn delete_update_exclusive_mirrors_update_set() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap(); // source-only (absent from update)
    std::fs::write(src.join("b.txt"), b"b").unwrap(); // present in both
    std::fs::create_dir(&upd).unwrap();
    std::fs::write(upd.join("b.txt"), b"b").unwrap();
    std::fs::write(upd.join("c.txt"), b"c").unwrap(); // update-only
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("a.txt"), b"old-a").unwrap(); // source-only's dst counterpart
    std::fs::write(dst.join("stale.txt"), b"stale").unwrap();

    // --update-exclusive materializes only the update set {b, c} (a.txt is source-only and
    // skipped). With --delete the destination must mirror that set exactly: a.txt (source-only)
    // and stale.txt are pruned, matching `rsync --link-dest --delete`.
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--update-exclusive")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    assert!(dst.join("b.txt").exists());
    assert!(dst.join("c.txt").exists());
    assert!(
        !dst.join("a.txt").exists(),
        "source-only entry must be pruned under --update-exclusive (exact mirror of update set)"
    );
    assert!(!dst.join("stale.txt").exists());
}

#[test]
fn delete_skip_specials_protects_update_only_special() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&upd).unwrap();
    // an update-only special (FIFO) that --skip-specials will skip copying
    let mkfifo = Command::new("mkfifo")
        .arg(upd.join("pipe"))
        .status()
        .unwrap();
    assert!(mkfifo.success(), "mkfifo unavailable");
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("pipe"), "old").unwrap(); // counterpart of the skipped special

    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--skip-specials")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        dst.join("pipe").exists(),
        "destination counterpart of a skipped update-only special must not be pruned"
    );
}

#[test]
fn delete_update_path_exclude_protects_descendant() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir_all(upd.join("cache")).unwrap(); // an update-only directory
    std::fs::create_dir_all(dst.join("cache")).unwrap();
    std::fs::write(dst.join("cache").join("keep.log"), "x").unwrap(); // matches cache/*.log

    // The path-based exclude must protect dst/cache/keep.log even though pruning happens inside
    // the delegated copy of the update-only `cache` directory (its filter is rooted correctly).
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--exclude")
        .arg("cache/*.log")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        dst.join("cache").join("keep.log").exists(),
        "path-based exclude must protect a descendant of an update-only directory"
    );
}

#[test]
fn delete_update_path_include_keeps_matching_descendant() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir_all(upd.join("cache")).unwrap();
    std::fs::write(upd.join("cache").join("keep.txt"), "x").unwrap(); // matches include cache/*.txt
    std::fs::create_dir_all(dst.join("cache")).unwrap();
    std::fs::write(dst.join("cache").join("keep.txt"), "old").unwrap();

    // --include 'cache/*.txt': the update-only cache/keep.txt is in scope, so it must be copied
    // and NOT pruned. Regression: copy-side filtering used the delegated root (saw "keep.txt",
    // skipped it), then pruning used the correct root and deleted the included destination entry.
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--include")
        .arg("cache/*.txt")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        dst.join("cache").join("keep.txt").exists(),
        "an included update-only descendant must be kept, not pruned"
    );
}

#[test]
fn delete_type_change_update_protects_excluded_descendant() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("node"), b"a file").unwrap(); // src `node` is a FILE
    std::fs::create_dir(&upd).unwrap();
    std::fs::create_dir(upd.join("node")).unwrap(); // update `node` is a DIRECTORY (type change)
    std::fs::create_dir_all(dst.join("node")).unwrap();
    std::fs::write(dst.join("node").join("keep.log"), "x").unwrap(); // extraneous, matches node/*.log

    // src `node` (file) vs update `node` (dir) is a type change, so link delegates to copy from
    // the update directory. Under --delete, pruning inside that delegated subtree must evaluate the
    // path-anchored exclude at the correct root (`node/*.log`) and protect keep.log. Regression:
    // the type-change delegation passed an empty filter_base, so keep.log was tested as "keep.log"
    // (unmatched) and deleted.
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--exclude")
        .arg("node/*.log")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        dst.join("node").join("keep.log").exists(),
        "path-based exclude must protect a descendant inside a type-changed (file->dir) update delegation"
    );
}

#[test]
fn delete_does_not_prune_through_dst_symlink() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    let outside = tmp.path().join("outside"); // must NOT be touched
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("precious.txt"), b"keep me").unwrap();
    // dst is a symlink to an external directory
    symlink(&outside, &dst).unwrap();

    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    // pruning must NOT follow the destination symlink: the external target is untouched ...
    assert!(
        outside.join("precious.txt").exists(),
        "rlink --delete must not delete files through a destination symlink"
    );
    // ... and dst was replaced by a real directory mirroring src.
    assert!(!dst.symlink_metadata().unwrap().file_type().is_symlink());
    assert!(dst.join("a.txt").exists());
}

#[test]
fn delete_excluded_update_type_change_to_excluded_dir_prunes_dst() {
    // rlink --delete --delete-excluded --update where an update turns an included source FILE into
    // an excluded DIRECTORY (`node/`). Nothing is materialized at dst/node, so a stale pre-existing
    // dst/node must be pruned — the keep-set must reflect the materialized (update) type, not the
    // source file type.
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("node"), b"file").unwrap(); // src/node is a FILE
    std::fs::create_dir(&upd).unwrap();
    std::fs::create_dir(upd.join("node")).unwrap(); // update/node is a DIRECTORY (excluded by node/)
    std::fs::write(upd.join("node").join("inner.txt"), b"x").unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("node"), b"stale").unwrap(); // pre-existing dst/node

    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--delete-excluded")
        .arg("--update")
        .arg(&upd)
        .arg("--exclude")
        .arg("node/")
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    assert!(
        !dst.join("node").exists(),
        "stale dst/node must be pruned: its materialized (update) type is an excluded directory"
    );
}

#[test]
fn delete_path_include_materializes_nested_symlink() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir_all(src.join("dir")).unwrap();
    symlink("../target", src.join("dir").join("link")).unwrap();
    std::fs::create_dir_all(dst.join("dir")).unwrap();
    symlink("../stale", dst.join("dir").join("link")).unwrap();

    // --include 'dir/link' with rlink --delete. Regression: link_internal delegated symlink
    // materialization through copy::copy with an empty filter_base, so the inner filter re-
    // evaluated the include against the bare basename "link" (via should_include_root_item)
    // and skipped the copy. Meanwhile the outer loop had already inserted "link" into the
    // keep_set, so pruning protected the stale destination. Net: stale dst/dir/link survived
    // a mirror sync. Fix: pass the entry's logical path as filter_base so the inner check
    // uses nested semantics and matches `dir/link`.
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--include")
        .arg("dir/link")
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    let target = std::fs::read_link(dst.join("dir").join("link")).unwrap();
    assert_eq!(
        target,
        std::path::PathBuf::from("../target"),
        "the included nested symlink must be materialized at the destination, not left stale"
    );
}

#[test]
fn delete_path_include_materializes_nested_symlink_via_update() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir_all(src.join("dir")).unwrap();
    symlink("../from_src", src.join("dir").join("link")).unwrap();
    std::fs::create_dir_all(upd.join("dir")).unwrap();
    symlink("../from_upd", upd.join("dir").join("link")).unwrap();
    std::fs::create_dir_all(dst.join("dir")).unwrap();
    symlink("../stale", dst.join("dir").join("link")).unwrap();

    // Same regression as the no-update case, but on the matching-file-type update branch in
    // link_internal where both src and update entries are symlinks (the
    // `update_metadata.is_symlink()` arm): the delegation also passed an empty filter_base.
    // The update's symlink must materialize (target == "../from_upd"), not be left stale.
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--include")
        .arg("dir/link")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());

    let target = std::fs::read_link(dst.join("dir").join("link")).unwrap();
    assert_eq!(
        target,
        std::path::PathBuf::from("../from_upd"),
        "the update's nested symlink must be materialized at the destination, not left stale"
    );
}

#[test]
fn delete_skip_specials_with_excluded_update_protects_source_special() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let upd = tmp.path().join("upd");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    // source has a FIFO (special file) that --skip-specials will skip copying
    let mkfifo = Command::new("mkfifo")
        .arg(src.join("pipe"))
        .status()
        .unwrap();
    assert!(mkfifo.success(), "mkfifo unavailable");
    // update has a directory with the SAME name, excluded by the filter
    std::fs::create_dir(&upd).unwrap();
    std::fs::create_dir(upd.join("pipe")).unwrap();
    // pre-existing destination counterpart (e.g. from a previous mirror run)
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("pipe"), "old").unwrap();

    // Regression: the source loop inserted `pipe` into keep_set and then `continue`d on the
    // --skip-specials branch BEFORE adding to processed_files. The update loop then saw `pipe/`
    // filtered out and unconditionally did `keep_set.remove(entry_name)`, dropping the source
    // special's keep_set entry → prune deleted dst/pipe. With --skip-specials, the destination
    // counterpart of a skipped source special must be retained.
    let status = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--skip-specials")
        .arg("--exclude")
        .arg("pipe/")
        .arg("--update")
        .arg(&upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        dst.join("pipe").exists(),
        "destination counterpart of a --skip-specials'd source special must be retained when an excluded update entry shares the name"
    );
}

#[test]
fn delete_update_exclusive_missing_update_path_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let missing_upd = tmp.path().join("does_not_exist");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("src_only.txt"), b"x").unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(dst.join("stale.txt"), b"old").unwrap();

    // Regression: with --update-exclusive the materialized set is the update set. When the
    // update root was missing, link_internal hit the recursive early-return (line 254) before
    // the prune phase, so rlink reported success with an empty summary and left every stale
    // dst entry in place — a silent "mirror" that didn't mirror anything. We now reject the
    // missing update root at the public link() entry so a typo'd --update doesn't accidentally
    // preserve (or, with the other proposed option, wipe) the destination tree.
    let output = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--update-exclusive")
        .arg("--update")
        .arg(&missing_upd)
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "rlink --delete --update-exclusive with a missing --update path must fail; stdout/stderr were: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    // dst is left intact (we erred before any pruning, so the user can correct the typo and re-run)
    assert!(dst.join("stale.txt").exists());
}

#[test]
fn delete_missing_update_path_errors_even_without_exclusive() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let missing_upd = tmp.path().join("does_not_exist");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"x").unwrap();
    std::fs::create_dir(&dst).unwrap();
    // dst entry the missing update tree WOULD have protected (no counterpart in src).
    std::fs::write(dst.join("update_only.txt"), b"old").unwrap();

    // Regression: with --delete (no --update-exclusive) and a missing --update root, link_internal
    // sets `update_metadata_opt = None` and proceeds as if no --update was given. The source-only
    // keep_set then makes any "would have been in update" dst entry look extraneous, and the
    // --delete prune wipes it. A typo'd --update under --delete is destructive in the same way
    // the --update-exclusive case is, so we reject the missing root at the public link() entry.
    let output = Command::new(rlink_bin())
        .arg("--delete")
        .arg("--update")
        .arg(&missing_upd)
        .arg(&src)
        .arg(&dst)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "rlink --delete --update <missing> must fail; stdout/stderr were: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    // dst entries are left intact (we erred before any prune).
    assert!(dst.join("update_only.txt").exists());
}

#[test]
fn missing_update_path_without_delete_or_exclusive_falls_back() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let missing_upd = tmp.path().join("does_not_exist");
    let dst = tmp.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"x").unwrap();
    // intentionally do NOT pre-create dst — without --delete (which auto-overwrites) a pre-
    // existing dst is rejected by rlink's "destination exists" guard, which would mask the
    // behavior we're checking here.

    // Without --delete or --update-exclusive, a missing --update path falls back to "no update"
    // mode: rlink links from src as if --update was never specified. This is the long-standing
    // behavior. Lock it in so the new missing-update-root rejection doesn't accidentally widen
    // to reject the non-destructive case too.
    let status = Command::new(rlink_bin())
        .arg("--update")
        .arg(&missing_upd)
        .arg(&src)
        .arg(&dst)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(dst.join("a.txt").exists());
}
