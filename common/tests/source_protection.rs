//! Ordinary input trees must survive destination pruning and replacement.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use common::{copy, filegen, link, preserve, progress};

static PROGRESS: std::sync::LazyLock<progress::Progress> =
    std::sync::LazyLock::new(progress::Progress::new);

struct RestoreMode(std::path::PathBuf);

impl RestoreMode {
    fn read_only(path: &Path) -> std::io::Result<Self> {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o500))?;
        Ok(Self(path.to_owned()))
    }
}

impl Drop for RestoreMode {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
    }
}

fn settings(delete: bool, preview: bool) -> copy::Settings {
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
        filter: None,
        dry_run: preview.then_some(common::config::DryRunMode::Brief),
        delete: delete.then_some(copy::DeleteSettings {
            delete_excluded: false,
        }),
    }
}

async fn fixture(path: &Path) -> anyhow::Result<Vec<u8>> {
    std::fs::create_dir_all(path)?;
    let mut config = filegen::FileGenConfig::new(path, vec![], 1, 32);
    config.leaf_files = true;
    filegen::filegen(&PROGRESS, &config).await?;
    Ok(std::fs::read(path.join("file0"))?)
}

async fn run(
    use_link: bool,
    src: &Path,
    dst: &Path,
    update: Option<&Path>,
    exclusive: bool,
    settings: copy::Settings,
) -> Result<copy::Summary, copy::Error> {
    if use_link {
        let settings = link::Settings {
            filter: settings.filter.clone(),
            dry_run: settings.dry_run,
            copy_settings: settings,
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
        .map(|summary| summary.copy_summary)
        .map_err(|error| copy::Error::new(error.source, error.summary.copy_summary))
    } else {
        copy::copy(
            &PROGRESS,
            src,
            dst,
            &settings,
            &preserve::preserve_none(),
            false,
        )
        .await
    }
}

fn assert_retained(path: &Path, payload: &[u8]) {
    assert_eq!(
        std::fs::read(path.join("file0")).expect("input payload must survive destination removal"),
        payload,
    );
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o7777,
        0o500,
        "removal must not relax the input directory's permissions",
    );
}

fn assert_protection_error(result: Result<copy::Summary, copy::Error>) -> copy::Summary {
    let error = result.expect_err("removing an input must be refused");
    let message = format!("{:#}", error.source);
    assert!(
        message.contains("overlap") || message.contains("input"),
        "expected input protection error, got {message}",
    );
    error.summary
}

async fn ancestor_prune(use_link: bool, preview: bool) -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let dst = root.path().join("destination");
    let src = dst.join("a/input");
    let payload = fixture(&src).await?;
    let _mode = RestoreMode::read_only(&src)?;
    let result = run(use_link, &src, &dst, None, false, settings(true, preview)).await;
    assert_retained(&src, &payload);
    let summary = assert_protection_error(result);
    assert_eq!(summary.rm_summary.files_removed, 0);
    assert_eq!(summary.rm_summary.directories_removed, 0);
    assert_eq!(summary.rm_summary.bytes_removed, 0);
    Ok(())
}

#[tokio::test]
async fn copy_delete_preserves_its_input() -> anyhow::Result<()> {
    ancestor_prune(false, false).await
}

#[tokio::test]
async fn link_delete_preserves_its_input() -> anyhow::Result<()> {
    ancestor_prune(true, false).await
}

#[tokio::test]
async fn copy_delete_preview_refuses_to_remove_its_input() -> anyhow::Result<()> {
    ancestor_prune(false, true).await
}

#[tokio::test]
async fn link_delete_preview_refuses_to_remove_its_input() -> anyhow::Result<()> {
    ancestor_prune(true, true).await
}

async fn ancestor_overwrite(use_link: bool, symlink: bool) -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let dst = root.path().join("destination");
    let src = dst.join("a/input");
    let payload = fixture(&src).await?;
    if symlink {
        std::os::unix::fs::symlink("absent", src.join("a"))?;
    } else {
        std::fs::write(src.join("a"), b"replacement")?;
    }
    let _mode = RestoreMode::read_only(&src)?;
    let result = run(use_link, &src, &dst, None, false, settings(false, false)).await;
    assert_retained(&src, &payload);
    assert_protection_error(result);
    assert!(
        dst.join("a").is_dir(),
        "the input's ancestor must remain a directory"
    );
    Ok(())
}

#[tokio::test]
async fn copy_file_overwrite_preserves_its_input() -> anyhow::Result<()> {
    ancestor_overwrite(false, false).await
}

#[tokio::test]
async fn copy_symlink_overwrite_preserves_its_input() -> anyhow::Result<()> {
    ancestor_overwrite(false, true).await
}

#[tokio::test]
async fn link_file_overwrite_preserves_its_input() -> anyhow::Result<()> {
    ancestor_overwrite(true, false).await
}

#[tokio::test]
async fn link_symlink_overwrite_preserves_its_input() -> anyhow::Result<()> {
    ancestor_overwrite(true, true).await
}

#[tokio::test]
async fn empty_directory_overwrite_does_not_change_its_permissions() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("source");
        let payload = fixture(&src).await?;
        let dst = root.path().join("destination");
        let replaced = dst.join("file0");
        std::fs::create_dir_all(&replaced)?;
        // retain the removed inode to detect an unnecessary chmod before rmdir
        let held = std::fs::File::open(&replaced)?;
        std::fs::set_permissions(&replaced, std::fs::Permissions::from_mode(0o000))?;
        let result = run(use_link, &src, &dst, None, false, settings(false, false)).await;
        let summary = result.map_err(|error| error.source)?;
        assert_eq!(std::fs::read(&replaced)?, payload);
        assert_eq!(summary.rm_summary.directories_removed, 1);
        assert_eq!(held.metadata()?.permissions().mode() & 0o7777, 0);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires passwordless sudo"]
async fn sudo_overwrite_replaces_an_unreadable_foreign_empty_directory() -> anyhow::Result<()> {
    if !std::process::Command::new("sudo")
        .args(["-n", "true"])
        .status()?
        .success()
    {
        eprintln!("Skipping test: passwordless sudo not available");
        return Ok(());
    }
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("source");
        let payload = fixture(&src).await?;
        let dst = root.path().join("destination");
        let replaced = dst.join("file0");
        std::fs::create_dir_all(&replaced)?;
        std::fs::set_permissions(&replaced, std::fs::Permissions::from_mode(0o700))?;
        assert!(
            std::process::Command::new("sudo")
                .args(["-n", "chown", "0:0"])
                .arg(&replaced)
                .status()?
                .success()
        );
        assert_eq!(
            std::fs::read_dir(&replaced).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied,
            "fixture must be unreadable to the copier",
        );
        let result = run(use_link, &src, &dst, None, false, settings(false, false)).await;
        // cleanup remains possible through the owned parent even if replacement failed
        if result.is_err() {
            std::fs::remove_dir(&replaced)?;
        }
        let summary = result.map_err(|error| error.source)?;
        assert_eq!(std::fs::read(&replaced)?, payload);
        assert_eq!(summary.rm_summary.directories_removed, 1);
    }
    Ok(())
}

async fn update_prune(exclusive: bool) -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let src = root.path().join("source");
    std::fs::create_dir(&src)?;
    let dst = root.path().join("destination");
    let update = dst.join("a/update");
    let payload = fixture(&update).await?;
    let _mode = RestoreMode::read_only(&update)?;
    let result = run(
        true,
        &src,
        &dst,
        Some(&update),
        exclusive,
        settings(true, false),
    )
    .await;
    assert_retained(&update, &payload);
    assert_protection_error(result);
    Ok(())
}

#[tokio::test]
async fn link_delete_preserves_its_update() -> anyhow::Result<()> {
    update_prune(false).await
}

#[tokio::test]
async fn exclusive_link_delete_preserves_its_update() -> anyhow::Result<()> {
    update_prune(true).await
}

#[tokio::test]
async fn destructive_copy_does_not_scan_excluded_inputs() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("source");
        let payload = fixture(&src).await?;
        let hidden = src.join("cache");
        let hidden_payload = fixture(&hidden).await?;
        let dst = root.path().join("destination");
        let _mode = RestoreMode(hidden.clone());
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o000))?;
        let mut settings = settings(true, false);
        let mut filter = common::filter::FilterSettings::new();
        filter.add_exclude("cache/")?;
        settings.filter = Some(filter);
        run(use_link, &src, &dst, None, false, settings)
            .await
            .map_err(|error| error.source)?;
        assert_eq!(std::fs::read(dst.join("file0"))?, payload);
        assert!(!dst.join("cache").exists());
        assert_eq!(std::fs::metadata(&hidden)?.permissions().mode() & 0o7777, 0);
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o700))?;
        assert_eq!(std::fs::read(hidden.join("file0"))?, hidden_payload);
    }
    Ok(())
}

#[tokio::test]
async fn protected_input_does_not_discard_safe_sibling_removal_counts() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let dst = root.path().join("destination");
        let src = dst.join("a/input");
        let payload = fixture(&src).await?;
        let _mode = RestoreMode::read_only(&src)?;
        let extra = dst.join("obsolete");
        fixture(&extra).await?;
        let result = run(use_link, &src, &dst, None, false, settings(true, false)).await;
        assert_retained(&src, &payload);
        let summary = assert_protection_error(result);
        assert!(!extra.exists());
        assert_eq!(summary.rm_summary.files_removed, 1);
        assert_eq!(summary.rm_summary.directories_removed, 1);
        assert_eq!(summary.rm_summary.bytes_removed, 32);
    }
    Ok(())
}

#[tokio::test]
async fn identical_input_leaf_is_left_unchanged() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let payload = fixture(root.path()).await?;
        let file = root.path().join("file0");
        let mut settings = settings(false, false);
        settings.overwrite_compare.size = true;
        let summary = run(use_link, &file, &file, None, false, settings)
            .await
            .map_err(|error| error.source)?;
        assert_eq!(std::fs::read(&file)?, payload);
        assert_eq!(summary.rm_summary.files_removed, 0);
    }
    Ok(())
}

#[tokio::test]
async fn delete_can_unlink_a_different_hardlink_to_an_input_file() -> anyhow::Result<()> {
    for use_link in [false, true] {
        let root = tempfile::tempdir()?;
        let src = root.path().join("source");
        let dst = root.path().join("destination");
        let payload = fixture(&src).await?;
        std::fs::create_dir(&dst)?;
        std::fs::hard_link(src.join("file0"), dst.join("obsolete"))?;
        let summary = run(use_link, &src, &dst, None, false, settings(true, false))
            .await
            .map_err(|error| error.source)?;
        assert_eq!(std::fs::read(src.join("file0"))?, payload);
        assert_eq!(std::fs::read(dst.join("file0"))?, payload);
        assert!(!dst.join("obsolete").exists());
        assert_eq!(summary.rm_summary.files_removed, 1);
        assert_eq!(summary.rm_summary.bytes_removed, 32);
    }
    Ok(())
}

#[tokio::test]
async fn failed_destructive_admission_releases_directory_protection_at_every_depth()
-> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let first = root.path().join("first");
    let second = root.path().join("second");
    fixture(&first).await?;
    fixture(&first.join("nested")).await?;
    fixture(&second).await?;
    fixture(&second.join("nested")).await?;
    for name in ["file0", "nested/file0"] {
        std::fs::write(second.join(name), vec![42; 64])?;
    }
    let session = copy::CopySession::default();
    let mut settings = settings(false, false);
    settings.overwrite_compare.size = true;
    session
        .copy(
            &PROGRESS,
            &first,
            &first.join("output"),
            &settings,
            &preserve::preserve_none(),
            false,
        )
        .await
        .expect_err("overlapping destination must fail before admission");
    session
        .copy(
            &PROGRESS,
            &second,
            &first,
            &settings,
            &preserve::preserve_none(),
            false,
        )
        .await
        .map_err(|error| error.source)?;
    for name in ["file0", "nested/file0"] {
        assert_eq!(std::fs::read(first.join(name))?, vec![42; 64]);
    }
    Ok(())
}

#[tokio::test]
async fn excluded_dereferenced_root_does_not_reserve_its_target() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let source = root.path().join("from/A");
    let payload = fixture(&source).await?;
    let target = root.path().join("Target");
    std::fs::create_dir(&target)?;
    let link = root.path().join("link");
    std::os::unix::fs::symlink(&target, &link)?;
    let mut settings = settings(false, false);
    settings.dereference = true;
    let mut filter = common::filter::FilterSettings::new();
    filter.add_exclude("Target/")?;
    settings.filter = Some(filter);
    let session = copy::CopySession::default();
    let pending = session
        .prepare(&link, &target.join("link"), &settings)
        .await?;
    session
        .copy(
            &PROGRESS,
            &source,
            &target.join("A"),
            &settings,
            &preserve::preserve_none(),
            false,
        )
        .await
        .map_err(|error| error.source)?;
    let summary = pending
        .copy(&PROGRESS, &preserve::preserve_none(), false)
        .await?;
    assert_eq!(summary.directories_skipped, 1);
    assert_eq!(std::fs::read(target.join("A/file0"))?, payload);
    assert!(!target.join("link").exists());
    Ok(())
}
