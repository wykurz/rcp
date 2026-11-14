//! Local copy/link overlap behavior, including an opt-in isolated bind-mount regression.

use std::path::Path;

use common::{copy, filegen, link, preserve, progress};

#[path = "support/mounts.rs"]
mod mounts;
use mounts::BindMount;

static PROGRESS: std::sync::LazyLock<progress::Progress> =
    std::sync::LazyLock::new(progress::Progress::new);

struct RestoreMode {
    path: std::path::PathBuf,
    permissions: std::fs::Permissions,
}

impl RestoreMode {
    fn set(path: &Path, mode: u32) -> std::io::Result<Self> {
        use std::os::unix::fs::PermissionsExt as _;
        let permissions = std::fs::metadata(path)?.permissions();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
        Ok(Self {
            path: path.to_owned(),
            permissions,
        })
    }
}

impl Drop for RestoreMode {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.path, self.permissions.clone());
    }
}

fn copy_settings(preview: bool) -> copy::Settings {
    let mut filter = common::filter::FilterSettings::new();
    // bound the old implementation too; an overlap must still produce its own error
    filter.add_exclude("/*/*/*/*/").unwrap();
    copy::Settings {
        reflink: Default::default(),
        local_copy_handoff: Default::default(),
        dereference: false,
        fail_early: false,
        overwrite: true,
        overwrite_compare: Default::default(),
        overwrite_filter: None,
        ignore_existing: false,
        chunk_size: 0,
        skip_specials: false,
        remote_copy_buffer_size: 0,
        filter: Some(filter),
        dry_run: preview.then_some(common::config::DryRunMode::Brief),
        delete: Some(copy::DeleteSettings {
            delete_excluded: false,
        }),
    }
}

async fn run(
    use_link: bool,
    src: &Path,
    dst: &Path,
    update: Option<&Path>,
    preview: bool,
    exclusive: bool,
) -> anyhow::Result<()> {
    run_with_settings(
        use_link,
        src,
        dst,
        update,
        exclusive,
        copy_settings(preview),
    )
    .await
}

async fn run_with_settings(
    use_link: bool,
    src: &Path,
    dst: &Path,
    update: Option<&Path>,
    exclusive: bool,
    copy_settings: copy::Settings,
) -> anyhow::Result<()> {
    if use_link {
        let settings = link::Settings {
            filter: copy_settings.filter.clone(),
            dry_run: copy_settings.dry_run,
            copy_settings,
            update_compare: Default::default(),
            update_exclusive: exclusive,
            preserve: preserve::preserve_none(),
        };
        link::link(
            &PROGRESS,
            src.parent().unwrap(),
            src,
            dst,
            &update.map(Path::to_owned),
            &settings,
            false,
        )
        .await
        .map_err(|error| error.source)?;
    } else {
        copy::copy(
            &PROGRESS,
            src,
            dst,
            &copy_settings,
            &preserve::preserve_none(),
            false,
        )
        .await
        .map_err(|error| error.source)?;
    }
    Ok(())
}

async fn fixture(path: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(path)?;
    let mut config = filegen::FileGenConfig::new(path, vec![], 1, 32);
    config.leaf_files = true;
    filegen::filegen(&PROGRESS, &config).await?;
    Ok(())
}

fn assert_overlap(result: anyhow::Result<()>) {
    let error = result.expect_err("input/output overlap must be rejected");
    assert!(format!("{error:#}").contains("overlap"), "{error:#}");
}

#[test]
fn mount_fixture_rejects_an_unmatched_child_test() {
    let error = mounts::isolated("missing_mount_fixture")
        .expect_err("a child running no tests must not count as a successful mount regression");
    assert!(error.to_string().contains("did not select exactly"));
}

#[tokio::test]
async fn link_rejects_existing_destinations_in_update_before_pruning() -> anyhow::Result<()> {
    for preview in [false, true] {
        for exclusive in [false, true] {
            for suffix in ["", "dst", "sub/dst"] {
                let root = tempfile::tempdir()?;
                let src = root.path().join("src");
                let update = root.path().join("update");
                fixture(&src).await?;
                fixture(&update).await?;
                let dst = update.join(suffix);
                std::fs::create_dir_all(&dst)?;
                std::fs::write(dst.join("sentinel"), b"keep")?;
                assert_overlap(run(true, &src, &dst, Some(&update), preview, exclusive).await);
                assert_eq!(std::fs::read(dst.join("sentinel"))?, b"keep");
                assert_eq!(
                    std::fs::read_dir(&dst)?.count(),
                    if suffix.is_empty() { 2 } else { 1 }
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn copy_session_rechecks_each_destination_parent() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let first = root.path().join("first");
    let second = root.path().join("second");
    fixture(&first).await?;
    fixture(&second).await?;
    let session = copy::CopySession::default();
    session
        .copy(
            &PROGRESS,
            &first,
            &root.path().join("out"),
            &copy_settings(false),
            &preserve::preserve_none(),
            false,
        )
        .await
        .map_err(|error| error.source)?;
    let dst = first.join("dst");
    assert_overlap(
        session
            .copy(
                &PROGRESS,
                &second,
                &dst,
                &copy_settings(false),
                &preserve::preserve_none(),
                false,
            )
            .await
            .map(|_| ())
            .map_err(|error| error.source),
    );
    assert!(!dst.exists());
    Ok(())
}

#[tokio::test]
async fn ignored_non_directory_inside_input_is_skipped() -> anyhow::Result<()> {
    for use_link in [false, true] {
        for preview in [false, true] {
            let root = tempfile::tempdir()?;
            let src = root.path().join("src");
            fixture(&src).await?;
            let dst = src.join("existing");
            std::fs::write(&dst, b"sentinel")?;
            let mut settings = copy_settings(preview);
            settings.ignore_existing = true;
            settings.delete = None;
            let summary = if use_link {
                let settings = link::Settings {
                    filter: settings.filter.clone(),
                    dry_run: settings.dry_run,
                    copy_settings: settings,
                    update_compare: Default::default(),
                    update_exclusive: false,
                    preserve: preserve::preserve_none(),
                };
                let summary =
                    link::link(&PROGRESS, root.path(), &src, &dst, &None, &settings, false)
                        .await
                        .map_err(|error| error.source)?;
                assert_eq!(summary.hard_links_created, 0);
                summary.copy_summary
            } else {
                copy::copy(
                    &PROGRESS,
                    &src,
                    &dst,
                    &settings,
                    &preserve::preserve_none(),
                    false,
                )
                .await
                .map_err(|error| error.source)?
            };
            assert_eq!(summary.directories_unchanged, 1);
            assert_eq!(summary.directories_created, 0);
            assert_eq!(summary.files_copied, 0);
            assert_eq!(std::fs::read(&dst)?, b"sentinel");
            assert_eq!(std::fs::read_dir(&src)?.count(), 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn disjoint_outputs_still_work() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("src");
        let dst = root.path().join("dst");
        fixture(&src).await?;
        run(use_link, &src, &dst, None, false, false).await?;
        assert_eq!(
            std::fs::read(src.join("file0"))?,
            std::fs::read(dst.join("file0"))?
        );
    }
    Ok(())
}

#[tokio::test]
async fn previews_allow_missing_destination_parents() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("src");
        fixture(&src).await?;
        let dst = src.join("missing/parent/dst");
        run(use_link, &src, &dst, None, true, false).await?;
        let error = run(use_link, &src, &dst, None, false, false)
            .await
            .unwrap_err();
        assert!(
            error.chain().any(|cause| cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)),
            "{error:#}"
        );
        assert!(!src.join("missing").exists());
    }
    Ok(())
}

// this test always creates its own private namespace before any mount syscall
#[test]
#[ignore = "requires unprivileged user/mount namespaces; see docs/testing.md"]
fn bind_aliases_cannot_feed_output_back_into_input() -> anyhow::Result<()> {
    if !mounts::isolated("bind_aliases_cannot_feed_output_back_into_input")? {
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for use_link in [false, true] {
            for case in [
                "parent-alias",
                "reused-output",
                "input-alias",
                "cross-branch",
                "self-bind",
            ] {
                let root = tempfile::tempdir()?;
                let src = root.path().join("src");
                let out = root.path().join("out");
                fixture(&src).await?;
                std::fs::create_dir(&out)?;
                let mut mounts = Vec::new();
                let dst = match case {
                    "parent-alias" => {
                        std::fs::create_dir(src.join("sub"))?;
                        mounts.push(BindMount::new(&src.join("sub"), &out)?);
                        out.join("dst")
                    }
                    "reused-output" => {
                        mounts.push(BindMount::new(&src, &out)?);
                        out.clone()
                    }
                    "input-alias" => {
                        std::fs::create_dir(src.join("sub"))?;
                        mounts.push(BindMount::new(&out, &src.join("sub"))?);
                        out.clone()
                    }
                    "cross-branch" => {
                        for path in [src.join("A"), src.join("B/C"), out.join("A"), out.join("B")] {
                            std::fs::create_dir_all(path)?;
                        }
                        mounts.push(BindMount::new(&out.join("B"), &src.join("A"))?);
                        mounts.push(BindMount::new(&out.join("A"), &src.join("B/C"))?);
                        out.clone()
                    }
                    "self-bind" => {
                        std::fs::create_dir_all(src.join("sub/self"))?;
                        mounts.push(BindMount::new(&src.join("sub"), &src.join("sub/self"))?);
                        src.join("sub/self/dst")
                    }
                    _ => unreachable!(),
                };
                eprintln!("bind case {case}, link={use_link}");
                assert_overlap(run(use_link, &src, &dst, None, false, false).await);
                if case == "self-bind" {
                    assert!(!dst.exists(), "self-bind overlap created {dst:?}");
                }
                drop(mounts);
            }
        }
        for mismatch in [false, true] {
            let root = tempfile::tempdir()?;
            let src = root.path().join("src");
            let update = root.path().join("update");
            let out = root.path().join("out");
            fixture(&src).await?;
            std::fs::create_dir_all(update.join("node"))?;
            std::fs::create_dir(&out)?;
            if mismatch {
                std::fs::write(src.join("node"), b"source file")?;
            }
            let _mount = BindMount::new(&out, &update.join("node"))?;
            assert_overlap(run(true, &src, &out, Some(&update), false, false).await);
        }
        anyhow::Ok(())
    })
}

#[test]
#[ignore = "requires unprivileged user/mount namespaces; see docs/testing.md"]
fn nonrecursive_bind_source_can_copy_to_an_unrelated_mount() -> anyhow::Result<()> {
    if !mounts::isolated("nonrecursive_bind_source_can_copy_to_an_unrelated_mount")? {
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for different_device in [false, true] {
            for use_link in [false, true] {
                // hard links cannot cross filesystems; copy exercises both mount types
                if different_device && use_link {
                    continue;
                }
                let root = tempfile::tempdir()?;
                let source = root.path().join("source");
                let alias = root.path().join("alias");
                let unrelated = root.path().join("unrelated");
                if use_link {
                    // Linux also rejects hard links across bind mount instances of one device
                    std::fs::create_dir_all(source.join("empty"))?;
                } else {
                    fixture(&source).await?;
                }
                let mountpoint = source.join("backup");
                for path in [&alias, &unrelated, &mountpoint] {
                    std::fs::create_dir(path)?;
                }
                let _destination_mount = if different_device {
                    BindMount::tmpfs(&mountpoint)?
                } else {
                    BindMount::new(&unrelated, &mountpoint)?
                };
                // a nonrecursive bind sees the underlying empty backup directory, so output in
                // the original path's separate mount cannot feed back through this source alias
                let _source_mount = BindMount::new(&source, &alias)?;
                assert_eq!(std::fs::read_dir(alias.join("backup"))?.count(), 0);
                let dst = mountpoint.join("out");
                run(use_link, &alias, &dst, None, false, false).await?;
                if use_link {
                    assert!(dst.join("empty").is_dir());
                } else {
                    assert_eq!(
                        std::fs::read(source.join("file0"))?,
                        std::fs::read(dst.join("file0"))?
                    );
                }
                assert_eq!(std::fs::read_dir(alias.join("backup"))?.count(), 0);
            }
        }
        anyhow::Ok(())
    })
}

#[test]
#[ignore = "requires unprivileged user/mount namespaces; see docs/testing.md"]
fn preview_rejects_nested_output_aliases_with_or_without_delete() -> anyhow::Result<()> {
    if !mounts::isolated("preview_rejects_nested_output_aliases_with_or_without_delete")? {
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for use_link in [false, true] {
            for delete in [false, true] {
                let root = tempfile::tempdir()?;
                let src = root.path().join("src");
                let dst = root.path().join("dst");
                fixture(&src.join("node")).await?;
                std::fs::create_dir_all(dst.join("node"))?;
                let original = std::fs::read(src.join("node/file0"))?;
                let _mount = BindMount::new(&src.join("node"), &dst.join("node"))?;
                let mut settings = copy_settings(true);
                if !delete {
                    settings.delete = None;
                }
                assert_overlap(
                    run_with_settings(use_link, &src, &dst, None, false, settings).await,
                );
                assert_eq!(std::fs::read(src.join("node/file0"))?, original);
                assert_eq!(std::fs::read_dir(&src)?.count(), 1);
                assert_eq!(std::fs::read_dir(&dst)?.count(), 1);
            }
        }
        anyhow::Ok(())
    })
}

#[test]
#[ignore = "requires unprivileged user/mount namespaces; see docs/testing.md"]
fn preview_dereference_rejects_a_nested_output_alias() -> anyhow::Result<()> {
    if !mounts::isolated("preview_dereference_rejects_a_nested_output_alias")? {
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let root = tempfile::tempdir()?;
        let src = root.path().join("src");
        let dst = root.path().join("dst");
        let target = root.path().join("target");
        fixture(&target).await?;
        std::fs::create_dir(&src)?;
        std::fs::create_dir_all(dst.join("node"))?;
        std::os::unix::fs::symlink(&target, src.join("node"))?;
        let _mount = BindMount::new(&target, &dst.join("node"))?;
        let original = std::fs::read(target.join("file0"))?;
        let mut settings = copy_settings(true);
        settings.delete = None;
        settings.dereference = true;
        assert_overlap(run_with_settings(false, &src, &dst, None, false, settings).await);
        assert_eq!(std::fs::read(target.join("file0"))?, original);
        assert_eq!(std::fs::read_dir(&dst)?.count(), 1);
        anyhow::Ok(())
    })
}

#[tokio::test]
async fn plain_previews_need_no_destination_read_permission() -> anyhow::Result<()> {
    for use_link in [false, true] {
        for mode in [0o111, 0o000] {
            let root = tempfile::tempdir()?;
            let src = root.path().join("src");
            let dst = root.path().join("dst");
            fixture(&src.join("nested")).await?;
            std::fs::create_dir_all(dst.join("nested"))?;
            let restore = RestoreMode::set(&dst, mode)?;
            let mut settings = copy_settings(true);
            settings.delete = None;
            let result = run_with_settings(use_link, &src, &dst, None, false, settings).await;
            drop(restore);
            result?;
            assert_eq!(std::fs::read_dir(dst.join("nested"))?.count(), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn failed_admission_releases_its_input_history() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let first = root.path().join("first");
    let second = root.path().join("second");
    fixture(&first).await?;
    fixture(&second).await?;
    let occupied = root.path().join("occupied");
    std::fs::write(&occupied, b"keep")?;
    let mut settings = copy_settings(false);
    settings.overwrite = false;
    settings.delete = None;
    let session = copy::CopySession::default();
    assert!(
        session
            .copy(
                &PROGRESS,
                &first,
                &occupied,
                &settings,
                &preserve::preserve_none(),
                false
            )
            .await
            .is_err()
    );
    session
        .copy(
            &PROGRESS,
            &second,
            &first.join("out"),
            &settings,
            &preserve::preserve_none(),
            false,
        )
        .await
        .map_err(|error| error.source)?;
    assert!(first.join("out/file0").is_file());
    Ok(())
}

#[tokio::test]
async fn committed_history_names_the_conflicting_operand() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let first = root.path().join("earlier-source");
    let second = root.path().join("second");
    fixture(&first).await?;
    fixture(&second).await?;
    let session = copy::CopySession::default();
    let settings = copy_settings(false);
    session
        .copy(
            &PROGRESS,
            &first,
            &root.path().join("out"),
            &settings,
            &preserve::preserve_none(),
            false,
        )
        .await
        .map_err(|error| error.source)?;
    let error = session
        .copy(
            &PROGRESS,
            &second,
            &first.join("out"),
            &settings,
            &preserve::preserve_none(),
            false,
        )
        .await
        .unwrap_err();
    assert!(
        format!("{:#}", error.source).contains("another operand"),
        "{error:#}"
    );
    assert!(
        format!("{:#}", error.source).contains(first.to_str().unwrap()),
        "{error:#}"
    );
    Ok(())
}

#[tokio::test]
async fn shared_ancestry_diagnostic_is_independent_of_first_committer() -> anyhow::Result<()> {
    for preview in [false, true] {
        for first in [0, 1] {
            let root = tempfile::tempdir()?;
            let sources = [root.path().join("A"), root.path().join("B")];
            let container = root.path().join("dst");
            std::fs::create_dir(&container)?;
            let destinations = [container.join("A"), container.join("B")];
            for source in &sources {
                fixture(source).await?;
            }
            let session = copy::CopySession::default();
            let mut settings = copy_settings(preview);
            settings.dereference = true;
            settings.delete = None;
            settings.filter = None;
            for index in [first, 1 - first] {
                session
                    .copy(
                        &PROGRESS,
                        &sources[index],
                        &destinations[index],
                        &settings,
                        &preserve::preserve_none(),
                        false,
                    )
                    .await
                    .map_err(|error| error.source)?;
            }
            std::os::unix::fs::symlink("../dst", sources[1].join("link"))?;
            let error = session
                .copy(
                    &PROGRESS,
                    &sources[1],
                    &destinations[1],
                    &settings,
                    &preserve::preserve_none(),
                    false,
                )
                .await
                .expect_err("the linked destination container must be rejected");
            let message = format!("{:#}", error.source);
            assert!(message.contains("destination ancestry"), "{message}");
            assert!(message.contains("this operand"), "{message}");
            assert!(message.contains(&format!("{:?}", sources[1])), "{message}");
            assert!(
                message.contains(&format!("{:?}", destinations[1])),
                "{message}"
            );
            assert!(!message.contains(&format!("{:?}", sources[0])), "{message}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn same_operand_overlap_identifies_source_and_update_roles() -> anyhow::Result<()> {
    for preview in [false, true] {
        for role in ["source", "update", "both"] {
            let root = tempfile::tempdir()?;
            let source = root.path().join("source");
            fixture(&source).await?;
            let update = if role == "both" {
                source.clone()
            } else {
                let update = root.path().join("update");
                fixture(&update).await?;
                update
            };
            let destination = if role == "source" { &source } else { &update }.join("output");
            let error = run(true, &source, &destination, Some(&update), preview, false)
                .await
                .expect_err("a destination inside an input must be rejected");
            let message = format!("{error:#}");
            assert!(message.contains("overlap"), "{message}");
            let label = match role {
                "source" => "source directory",
                "update" => "update directory",
                "both" => "directory used as both source and update",
                _ => unreachable!(),
            };
            assert!(message.contains(label), "{message}");
            assert!(message.contains(&format!("{:?}", source)), "{message}");
            assert!(
                message.contains(&format!("(update {update:?})")),
                "{message}"
            );
            assert!(message.contains(&format!("{:?}", destination)), "{message}");
            assert!(!destination.exists());
        }
    }
    Ok(())
}

#[tokio::test]
async fn delegated_update_descendants_retain_their_input_role() -> anyhow::Result<()> {
    for preview in [false, true] {
        for replaces_source_file in [false, true] {
            let root = tempfile::tempdir()?;
            let source = root.path().join("source");
            let update = root.path().join("update");
            let destination = root.path().join("destination");
            fixture(&source).await?;
            fixture(&update.join("only/nested")).await?;
            fixture(&destination).await?;
            let original = std::fs::read(update.join("only/nested/file0"))?;
            if replaces_source_file {
                std::fs::write(source.join("only"), b"old source file")?;
            }
            std::os::unix::fs::symlink(&destination, update.join("only/nested/back"))?;
            let mut settings = copy_settings(preview);
            settings.dereference = true;
            settings.delete = None;
            let error =
                run_with_settings(true, &source, &destination, Some(&update), false, settings)
                    .await
                    .expect_err("an update descendant must not consume the destination");
            let message = format!("{error:#}");
            assert!(message.contains("update directory"), "{message}");
            assert!(
                message.contains(&format!("(update {update:?})")),
                "{message}"
            );
            assert_eq!(std::fs::read(update.join("only/nested/file0"))?, original);
            assert_eq!(
                std::fs::read_link(update.join("only/nested/back"))?,
                destination
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn another_operands_ancestry_names_the_destination() -> anyhow::Result<()> {
    for preview in [false, true] {
        let root = tempfile::tempdir()?;
        let first = root.path().join("first-source");
        fixture(&first).await?;
        let container = root.path().join("container");
        std::fs::create_dir(&container)?;
        let first_destination = container.join("output");
        let session = copy::CopySession::default();
        let settings = copy_settings(preview);
        session
            .copy(
                &PROGRESS,
                &first,
                &first_destination,
                &settings,
                &preserve::preserve_none(),
                false,
            )
            .await
            .map_err(|error| error.source)?;
        let error = session
            .copy(
                &PROGRESS,
                &container,
                &root.path().join("second-output"),
                &settings,
                &preserve::preserve_none(),
                false,
            )
            .await
            .expect_err("a recorded destination ancestor must not become an input");
        let message = format!("{:#}", error.source);
        assert!(message.contains("destination ancestry"), "{message}");
        assert!(
            message.contains(&format!("{first_destination:?}")),
            "{message}"
        );
        assert!(!message.contains(&format!("{first:?}")), "{message}");
    }
    Ok(())
}

#[tokio::test]
async fn delete_preview_reads_an_empty_directory_without_search_permission() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("src");
        let dst = root.path().join("dst");
        std::fs::create_dir(&src)?;
        std::fs::create_dir(&dst)?;
        let restore = RestoreMode::set(&dst, 0o444)?;
        let result = run(use_link, &src, &dst, None, true, false).await;
        drop(restore);
        result?;
    }
    Ok(())
}

#[test]
#[ignore = "requires unprivileged user/mount namespaces; see docs/testing.md"]
fn general_self_bind_is_detected_after_destination_creation() -> anyhow::Result<()> {
    if !mounts::isolated("general_self_bind_is_detected_after_destination_creation")? {
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for use_link in [false, true] {
            let root = tempfile::tempdir()?;
            let src = root.path().join("src");
            let sub = src.join("sub");
            std::fs::create_dir_all(&sub)?;
            let _mount = BindMount::new(&sub, &sub)?;
            let dst = sub.join("output");
            // unlike the equal-inode parent transition, this mount hides the source root from
            // bounded ancestry. Traversal discovers the alias only after creating the output.
            assert_overlap(run(use_link, &src, &dst, None, false, false).await);
            assert!(
                dst.is_dir(),
                "this regression records the documented late-detection boundary"
            );
        }
        anyhow::Ok(())
    })
}

#[tokio::test]
async fn preview_rejects_a_non_directory_operand_parent() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("src");
        fixture(&src).await?;
        let parent = root.path().join("file");
        std::fs::write(&parent, b"keep")?;
        let error = run(use_link, &src, &parent.join("dst"), None, true, false)
            .await
            .unwrap_err();
        assert!(
            error.chain().any(|cause| cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.raw_os_error() == Some(libc::ENOTDIR))),
            "{error:#}"
        );
    }
    Ok(())
}
