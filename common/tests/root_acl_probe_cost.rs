//! What the source-root ACL notice costs, and that it is a per-PROCESS constant.
//!
//! This lives in its own integration binary on purpose. The probe behind the notice
//! (`safedir::warn_if_root_acl_unpreserved_at`) fires at most once per process, so measuring it
//! requires a process where it has not already been spent — and `probe_metadata.rs` deliberately
//! spends it up front so its per-entry counts describe the walk alone. Putting this test in there
//! would make one of the two order-dependent, which is exactly the failure this arrangement exists
//! to remove: under plain `cargo test` (what `nix build`'s `checkPhase` runs) every test in a
//! binary shares one process, so whichever ran first would pay and the rest would not.

use common::{copy, preserve, progress};
use congestion::testing::CollectingSink;

static PROGRESS: std::sync::LazyLock<progress::Progress> =
    std::sync::LazyLock::new(progress::Progress::new);

async fn make_tempdir(label: &str) -> std::path::PathBuf {
    let mut idx = 0;
    loop {
        let candidate = std::env::temp_dir().join(format!("rcp_acl_probe_{label}_{idx}"));
        match tokio::fs::create_dir(&candidate).await {
            Ok(()) => return candidate,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => idx += 1,
            Err(err) => panic!("create tempdir: {err:#}"),
        }
    }
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
        delete: None,
    }
}

/// Copy `src` to `dst` with a fresh sink installed and return the source-side metadata count.
async fn source_metadata_ops(src: &std::path::Path, dst: &std::path::Path) -> usize {
    let sink = std::sync::Arc::new(CollectingSink::new());
    congestion::install_sample_sink(sink.clone());
    copy::copy(
        &PROGRESS,
        src,
        dst,
        &default_copy_settings(),
        &preserve::preserve_all(),
        true,
    )
    .await
    .expect("copy succeeds");
    congestion::clear_sample_sink();
    sink.metadata_count_for(congestion::Side::Source)
}

/// The notice costs exactly 2 source-side metadata ops — `child()` to classify the root plus the
/// `listxattr` on it — and it costs them ONCE, not once per copy.
///
/// Both halves matter. If the count were higher the constant would be wrong; if the SECOND copy
/// also paid, the "free at any tree size" claim in `docs/acls.md` would be false in the worst way,
/// since the cost would then scale with the number of operands rather than staying flat.
///
/// The two copies are deliberately identical in shape and differ only in destination, so the
/// difference between them is the probe and nothing else. `preserve_all()` is `all`, which excludes
/// ACLs, so the probe is armed; a copy that preserved ACLs would skip it entirely.
#[tokio::test]
async fn root_acl_probe_costs_two_source_ops_once_per_process() {
    let tmp = make_tempdir("cost").await;
    let src = tmp.join("src");
    tokio::fs::create_dir_all(&src).await.expect("create src");
    tokio::fs::write(src.join("f.txt"), b"payload")
        .await
        .expect("write file");
    // first copy in this process: the probe is unspent, so this pays for it
    let first = source_metadata_ops(&src, &tmp.join("dst_first")).await;
    // same copy again: the probe is spent, so this is the walk alone
    let second = source_metadata_ops(&src, &tmp.join("dst_second")).await;
    assert_eq!(
        first.checked_sub(second),
        Some(2),
        "the source-root ACL probe should cost exactly 2 source metadata ops (child + listxattr); \
         first copy paid {first}, second paid {second}"
    );
    assert_eq!(
        source_metadata_ops(&src, &tmp.join("dst_third")).await,
        second,
        "a third copy in the same process paid more than the second, so the probe is no longer a \
         per-process constant and its cost now scales with the number of operands"
    );
}
