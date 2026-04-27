//! Integration test verifying that the per-syscall metadata probes wired into
//! `common::rm`, `common::copy`, and `common::link` actually emit samples
//! when a `SampleSink` is installed.
//!
//! Lives in its own test binary so the process-wide `SampleSink` global is
//! isolated from every other test in the `common` crate.

use common::{copy, link, preserve, progress, rm};
use congestion::testing::CollectingSink;

static PROGRESS: std::sync::LazyLock<progress::Progress> =
    std::sync::LazyLock::new(progress::Progress::new);

// serializes access to the process-wide SampleSink so these tests can't
// race when run under cargo test (which uses threads rather than processes).
static SINK_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn make_tempdir(label: &str) -> std::path::PathBuf {
    let mut idx = 0;
    loop {
        let candidate = std::env::temp_dir().join(format!("rcp_probe_{label}_{idx}"));
        match tokio::fs::create_dir(&candidate).await {
            Ok(()) => return candidate,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => idx += 1,
            Err(err) => panic!("create tempdir: {err:#}"),
        }
    }
}

/// Builds a tree with exactly 5 non-root entries: {a/, b/, a/1.txt, a/2.txt, b/3.txt}.
async fn make_small_tree(root: &std::path::Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(root.join("a")).await?;
    tokio::fs::create_dir_all(root.join("b")).await?;
    tokio::fs::write(root.join("a").join("1.txt"), b"1").await?;
    tokio::fs::write(root.join("a").join("2.txt"), b"2").await?;
    tokio::fs::write(root.join("b").join("3.txt"), b"3").await?;
    Ok(())
}

fn install_sink() -> std::sync::Arc<CollectingSink> {
    let sink = std::sync::Arc::new(CollectingSink::new());
    congestion::install_sample_sink(sink.clone());
    sink
}

fn default_copy_settings() -> copy::Settings {
    copy::Settings {
        dereference: false,
        fail_early: false,
        overwrite: false,
        overwrite_compare: Default::default(),
        overwrite_filter: None,
        ignore_existing: false,
        chunk_size: 0,
        skip_specials: false,
        remote_copy_buffer_size: 0,
        filter: None,
        dry_run: None,
    }
}

#[tokio::test]
async fn rm_emits_one_metadata_sample_per_tree_entry() {
    let _guard = SINK_GUARD.lock().await;
    let sink = install_sink();
    let tmp = make_tempdir("rm_samples").await;
    let root = tmp.join("tree");
    make_small_tree(&root).await.expect("create tree");
    let result = rm::rm(
        &PROGRESS,
        &root,
        &rm::Settings {
            fail_early: false,
            filter: None,
            dry_run: None,
            time_filter: None,
        },
    )
    .await
    .expect("rm succeeds");
    congestion::clear_sample_sink();
    assert_eq!(result.directories_removed, 3);
    assert_eq!(result.files_removed, 3);
    // Source-side metadata: 1 stat for the top-level path before walking.
    assert!(sink.metadata_count_for(congestion::Side::Source) >= 1);
    // Destination-side metadata: one probe per mutation — 3 remove_file
    // calls (the .txt files) + 3 remove_dir calls (a, b, root tree).
    assert_eq!(sink.metadata_count_for(congestion::Side::Destination), 6);
    // every sample should have a non-zero latency; an all-zero result would
    // mean the probe is bracketing something that isn't a syscall.
    for s in sink.metadata_samples().iter() {
        assert!(
            s.latency() > std::time::Duration::ZERO,
            "sample latency must be non-zero: {:?}",
            s,
        );
    }
}

#[tokio::test]
async fn copy_emits_one_metadata_sample_per_tree_entry() {
    let _guard = SINK_GUARD.lock().await;
    let sink = install_sink();
    let tmp = make_tempdir("copy_samples").await;
    let src = tmp.join("src");
    let dst = tmp.join("dst");
    make_small_tree(&src).await.expect("create src tree");
    copy::copy(
        &PROGRESS,
        &src,
        &dst,
        &default_copy_settings(),
        &preserve::preserve_all(),
        true,
    )
    .await
    .expect("copy succeeds");
    congestion::clear_sample_sink();
    // Source-side metadata: one symlink_metadata per copy_internal call —
    // 6 entries (root + a + b + 3 .txt files) = 6 src probes.
    assert_eq!(
        sink.metadata_count_for(congestion::Side::Source),
        6,
        "expected 6 src metadata probes (one symlink_metadata per entry)",
    );
    // Destination-side metadata for the 6 entries:
    //   - 3 dirs:  1 create_dir + 3 preserve (chown + chmod + utimens) = 4 each
    //   - 3 files: 4 preserve (chown + open + chmod + utimens)         = 4 each
    // tokio::fs::copy itself is unprobed (data-path), so 6 × 4 = 24 dst.
    assert_eq!(
        sink.metadata_count_for(congestion::Side::Destination),
        24,
        "expected 24 dst metadata probes (4 per entry × 6 entries)",
    );
}

/// End-to-end integration test for the auto-meta-throttle pipeline:
/// Probe -> RoutingSink -> ControlUnit<VegasController> -> Decision watch.
///
/// Doesn't drive throttle::set_max_ops_in_flight (that's validated in the
/// congestion unit tests). Asserts that the full pipeline flows: running
/// a real rm over a tree results in the controller actually consuming
/// metadata samples (visible via the snapshot's `samples_seen` counter).
#[tokio::test]
async fn auto_meta_pipeline_propagates_probes_to_controller() {
    let _guard = SINK_GUARD.lock().await;
    // Tap the metadata-destination channel: rm's per-file unlinks and
    // dir removals all hit the destination side, so its probes land
    // there. (The pipeline being exercised is the same regardless of
    // which channel we tap; pick the one with the most samples to keep
    // this assertion robust.)
    let mut builder = congestion::RoutingSinkBuilder::new();
    let metadata_rx = builder.metadata_receiver(congestion::Side::Destination);
    congestion::install_sample_sink(std::sync::Arc::new(builder.build()));
    let controller = congestion::VegasController::new(congestion::VegasConfig {
        initial_cwnd: 5,
        ..congestion::VegasConfig::default()
    });
    let (unit, decision_rx, mut snapshot_rx) = congestion::ControlUnit::new(
        "pipeline-test",
        controller,
        metadata_rx,
        std::time::Duration::from_millis(20),
    );
    let handle = unit.spawn();
    // give ControlUnit a moment to publish its initial decision
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let initial_decision = *decision_rx.borrow();
    assert_eq!(
        initial_decision.max_in_flight,
        Some(5),
        "ControlUnit must publish the initial cwnd on startup",
    );
    // baseline the snapshot sample count; the assertion below only
    // counts samples observed AFTER this point.
    let samples_before = snapshot_rx.borrow_and_update().samples_seen;
    // walk a tree; probes fire on each metadata syscall and flow through the sink.
    // small_tree under root yields 6 dst probes (3 remove_file + 3 remove_dir).
    let tmp = make_tempdir("pipeline").await;
    let root = tmp.join("tree");
    make_small_tree(&root).await.expect("create tree");
    rm::rm(
        &PROGRESS,
        &root,
        &rm::Settings {
            fail_early: false,
            filter: None,
            dry_run: None,
            time_filter: None,
        },
    )
    .await
    .expect("rm succeeds");
    // give the ControlUnit time to consume samples across a few ticks
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let final_snapshot = *snapshot_rx.borrow();
    let final_decision = *decision_rx.borrow();
    congestion::clear_sample_sink();
    handle.abort();
    // The controller must have ingested the samples that flowed through
    // the routing sink. With the small_tree fixture, rm produces 6 dst
    // probes (3 remove_file + 3 remove_dir); samples_seen is monotonic
    // and any positive delta proves probes -> sink -> control unit ->
    // controller all wired correctly.
    let samples_after = final_snapshot.samples_seen;
    assert!(
        samples_after > samples_before,
        "controller must consume samples — saw {} before rm and {} after",
        samples_before,
        samples_after,
    );
    assert!(
        final_decision.max_in_flight.is_some(),
        "pipeline must keep publishing concrete decisions",
    );
}

#[tokio::test]
async fn filegen_emits_metadata_samples_for_created_dirs_and_files() {
    // filegen creates a tree of dirs + files. Each subdirectory creation
    // (create_dir) and each file open emits one metadata probe. With
    // dirwidth=[2] and numfiles=2, filegen generates files at every
    // level (leaf_files defaults to false, i.e. not leaf-only):
    //   root: 2 files + 2 subdirs created
    //     dir0: 2 files (leaf)
    //     dir1: 2 files (leaf)
    // Totals: 2 create_dir + 6 open = 8 probes.
    let _guard = SINK_GUARD.lock().await;
    let sink = install_sink();
    let tmp = make_tempdir("filegen_samples").await;
    let root = tmp.join("gen");
    tokio::fs::create_dir(&root).await.expect("mkdir root");
    let config = common::filegen::FileGenConfig::new(root, vec![2], 2, 16);
    common::filegen::filegen(&PROGRESS, &config)
        .await
        .expect("filegen succeeds");
    congestion::clear_sample_sink();
    // filegen probes only the destination-side mutations (mkdir + open)
    // — there's no source tree. At least 8: 2 create_dir + 6 file opens.
    assert!(
        sink.metadata_count_for(congestion::Side::Destination) >= 8,
        "expected at least 8 dst metadata probes, got {}",
        sink.metadata_count_for(congestion::Side::Destination)
    );
}

#[tokio::test]
async fn link_update_path_emits_probes_for_update_tree() {
    // Pins the per-file metadata probes that fire when `link` processes
    // entries present in `update/` but not in `src/`. These end up on
    // the spawned `copy::copy` calls inside the update-walk segment of
    // `link_internal`; if that segment regressed and stopped invoking
    // copy (or invoked it without probes), the dst count would drop by
    // the 4 preserve probes per update file (× 3 = 12) and the src
    // count would drop by the per-file `symlink_metadata` (× 3 = 3).
    let _guard = SINK_GUARD.lock().await;
    let sink = install_sink();
    let tmp = make_tempdir("link_update_samples").await;
    let src = tmp.join("src");
    let update = tmp.join("update");
    let dst = tmp.join("dst");
    make_small_tree(&src).await.expect("create src tree");
    tokio::fs::create_dir(&update).await.expect("create update");
    tokio::fs::write(update.join("u1.txt"), b"u1")
        .await
        .expect("u1");
    tokio::fs::write(update.join("u2.txt"), b"u2")
        .await
        .expect("u2");
    tokio::fs::write(update.join("u3.txt"), b"u3")
        .await
        .expect("u3");
    link::link(
        &PROGRESS,
        tmp.as_path(),
        &src,
        &dst,
        &Some(update),
        &link::Settings {
            copy_settings: default_copy_settings(),
            update_compare: Default::default(),
            update_exclusive: false,
            filter: None,
            dry_run: None,
            preserve: preserve::preserve_all(),
        },
        true,
    )
    .await
    .expect("link succeeds");
    congestion::clear_sample_sink();
    // Source-side: 6 link_internal symlink_metadata(src) calls (root + a +
    // b + 3 .txt) + 1 top-level symlink_metadata(update) (succeeds, dirs
    // only at top level — recursive update lookups for src/{a,b,*.txt}
    // are NotFound, so their probes are discarded) + 3 copy_internal
    // symlink_metadata calls (one per u*.txt under update) = 10.
    assert_eq!(
        sink.metadata_count_for(congestion::Side::Source),
        10,
        "expected 10 src metadata probes — drops to 7 if the update-walk \
         path stops spawning copies for u*.txt",
    );
    // Destination-side: 15 from the small-tree link (see
    // link_emits_one_metadata_sample_per_tree_entry) + 12 for the 3
    // update-only files (4 preserve probes per file via copy_file) = 27.
    assert_eq!(
        sink.metadata_count_for(congestion::Side::Destination),
        27,
        "expected 27 dst metadata probes — drops to 15 if the update-walk \
         path stops spawning copies for u*.txt",
    );
}

#[tokio::test]
async fn link_emits_one_metadata_sample_per_tree_entry() {
    let _guard = SINK_GUARD.lock().await;
    let sink = install_sink();
    let tmp = make_tempdir("link_samples").await;
    let src = tmp.join("src");
    let dst = tmp.join("dst");
    make_small_tree(&src).await.expect("create src tree");
    link::link(
        &PROGRESS,
        tmp.as_path(),
        &src,
        &dst,
        &None,
        &link::Settings {
            copy_settings: default_copy_settings(),
            update_compare: Default::default(),
            update_exclusive: false,
            filter: None,
            dry_run: None,
            preserve: preserve::preserve_all(),
        },
        true,
    )
    .await
    .expect("link succeeds");
    congestion::clear_sample_sink();
    // Source-side metadata: one symlink_metadata per link_internal call —
    // 6 entries (root + a + b + 3 .txt files) = 6 src probes.
    assert_eq!(
        sink.metadata_count_for(congestion::Side::Source),
        6,
        "expected 6 src metadata probes (one symlink_metadata per entry)",
    );
    // Destination-side metadata for the 6 entries:
    //   - 3 dirs:  1 create_dir + 3 preserve (chown + chmod + utimens) = 4 each
    //   - 3 files: 1 hard_link (no preserve in hard_link_helper)       = 1 each
    // Total: 12 + 3 = 15 dst.
    assert_eq!(
        sink.metadata_count_for(congestion::Side::Destination),
        15,
        "expected 15 dst metadata probes (4 per dir × 3 + 1 per file × 3)",
    );
}
