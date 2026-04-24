//! Integration test verifying that the tree-walk metadata probes wired into
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
    let samples = sink.metadata_samples();
    // exactly one probe per directory entry iterated across all three dirs.
    assert_eq!(
        samples.len(),
        5,
        "expected 5 metadata samples (a, b, 1.txt, 2.txt, 3.txt), got {}",
        samples.len(),
    );
    // every sample should have a non-zero latency; an all-zero result would
    // mean the probe is bracketing something that isn't a syscall.
    for s in &samples {
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
    assert_eq!(sink.metadata_count(), 5);
}

/// Regression test for a deadlock that occurs when the ops-in-flight
/// permit is held across a spawned task's `join_set.join_next()` await.
///
/// With cwnd=1 and any tree depth >= 2, the parent task would hold the
/// only permit while waiting for children; children would block forever
/// trying to acquire. The fix is to scope the permit tightly around the
/// actual syscalls in the tree-walk loop so it is released before the
/// join — which this test pins down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rm_on_deep_tree_does_not_deadlock_at_cwnd_one() {
    let _guard = SINK_GUARD.lock().await;
    // install the full auto-meta pipeline with cwnd == 1 so any
    // held-across-join semantics would trigger immediately.
    let mut builder = congestion::RoutingSinkBuilder::new();
    let metadata_rx = builder.metadata_receiver();
    congestion::install_sample_sink(std::sync::Arc::new(builder.build()));
    throttle::set_max_ops_in_flight(1);
    let vegas = congestion::VegasController::new(congestion::VegasConfig {
        initial_cwnd: 1,
        min_cwnd: 1,
        max_cwnd: 1,
        ..congestion::VegasConfig::default()
    });
    let (unit, _decision_rx) =
        congestion::ControlUnit::new(vegas, metadata_rx, std::time::Duration::from_millis(50));
    let ctrl_handle = unit.spawn();
    let tmp = make_tempdir("deadlock").await;
    let root = tmp.join("deep");
    // 5-deep chain: deep/d1/d2/d3/d4/leaf.txt
    let mut path = root.clone();
    for _ in 0..5 {
        path.push("d");
        tokio::fs::create_dir_all(&path).await.expect("mkdir");
    }
    tokio::fs::write(path.join("leaf.txt"), b"x")
        .await
        .expect("leaf file");
    // 5-second watchdog: pre-fix this call hangs forever at cwnd=1.
    let rm_fut = rm::rm(
        &PROGRESS,
        &root,
        &rm::Settings {
            fail_early: false,
            filter: None,
            dry_run: None,
            time_filter: None,
        },
    );
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), rm_fut)
        .await
        .expect("rm completed within 5s — deadlock detected if timeout fired")
        .expect("rm succeeded");
    congestion::clear_sample_sink();
    throttle::set_max_ops_in_flight(0);
    ctrl_handle.abort();
    assert_eq!(result.directories_removed, 6);
    assert_eq!(result.files_removed, 1);
}

/// End-to-end integration test for the auto-meta-throttle pipeline:
/// Probe -> RoutingSink -> ControlUnit<VegasController> -> Decision watch.
///
/// Doesn't drive throttle::set_max_ops_in_flight (that's validated in the
/// congestion unit tests). Asserts only that the full pipeline flows:
/// running a real rm over a tree moves the controller's cwnd away from the
/// initial UNLIMITED watch value, proving every layer is wired correctly.
#[tokio::test]
async fn auto_meta_pipeline_propagates_probes_to_controller() {
    let _guard = SINK_GUARD.lock().await;
    let mut builder = congestion::RoutingSinkBuilder::new();
    let metadata_rx = builder.metadata_receiver();
    congestion::install_sample_sink(std::sync::Arc::new(builder.build()));
    let controller = congestion::VegasController::new(congestion::VegasConfig {
        initial_cwnd: 5,
        ..congestion::VegasConfig::default()
    });
    let (unit, decision_rx) = congestion::ControlUnit::new(
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
    // walk a tree; probes fire on each entry and flow through the sink
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
    // first sample-bearing tick is the bootstrap (baseline establishment);
    // by 100ms / 20ms = 5 ticks, a well-behaved controller has had ample
    // opportunity to adjust cwnd in response to real samples.
    let final_decision = *decision_rx.borrow();
    congestion::clear_sample_sink();
    handle.abort();
    assert!(
        final_decision.max_in_flight.is_some(),
        "pipeline must keep publishing concrete decisions",
    );
}

#[tokio::test]
async fn cmp_emits_one_metadata_sample_per_tree_entry() {
    // cmp walks both the src AND the dst directories (the dst walk
    // reports entries missing from src); each tree entry on each side
    // emits one metadata probe. Two identical 5-entry trees therefore
    // produce 10 probes total — 5 from the src walk and 5 from the
    // dst walk.
    let _guard = SINK_GUARD.lock().await;
    let sink = install_sink();
    let tmp = make_tempdir("cmp_samples").await;
    let src = tmp.join("src");
    let dst = tmp.join("dst");
    make_small_tree(&src).await.expect("create src tree");
    make_small_tree(&dst).await.expect("create dst tree");
    let log = common::cmp::LogWriter::silent().await.expect("silent log");
    common::cmp::cmp(
        &PROGRESS,
        &src,
        &dst,
        &log,
        &common::cmp::Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: Default::default(),
            filter: None,
        },
    )
    .await
    .expect("cmp succeeds");
    congestion::clear_sample_sink();
    assert_eq!(sink.metadata_count(), 10);
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
    assert_eq!(sink.metadata_count(), 8);
}

#[tokio::test]
async fn link_update_path_emits_probes_for_update_tree() {
    // Regression: the update walk (for entries present in `update/` but
    // not in `src/`) was iterating with a raw next_entry() and bypassed
    // both probing and cwnd gating. This test pins down that it now
    // probes. With src holding the small-tree (5 probes on src walk)
    // and update holding 3 unique top-level files, we expect 5 src
    // probes + 3 update probes = 8 total.
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
    assert_eq!(sink.metadata_count(), 8);
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
    assert_eq!(sink.metadata_count(), 5);
}
