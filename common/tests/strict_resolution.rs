//! Tests that ARM the process-global strict operand resolution switch
//! (`--require-toctou-safe`).
//!
//! These live in their own integration-test binary — not in the lib's unit-test
//! mod — because the switch is deliberately one-way: once armed it stays armed
//! for the life of the process. Under cargo-nextest every test is its own
//! process, but the plain `cargo test` harness (used by the nix checkPhase)
//! runs a binary's tests as threads of ONE shared process, where arming would
//! leak into unrelated lib tests (e.g. the symlink-following `open_parent_dir`
//! test, which must observe default behavior). A separate integration binary
//! gives these tests their own process under both runners; within this binary
//! every test either arms the switch itself or accepts an already-armed state.

use common::toctou_check::{LinterAction, run_linter};

/// Once strict operand resolution is armed, the two operand opens refuse to
/// resolve through a symlink anywhere in the path (ELOOP), while symlink-free
/// paths still open normally.
#[tokio::test]
async fn strict_resolution_rejects_symlinked_prefix() -> anyhow::Result<()> {
    if !common::safedir::openat2_available() {
        // on pre-5.6 kernels the linter refuses --require-toctou-safe outright, so
        // strict opens can never be reached in production; nothing to test here
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    // canonicalize: TMPDIR itself may contain symlinked components (e.g. under
    // nix-shell), which strict resolution would — correctly — refuse.
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    tokio::fs::create_dir_all(tmp.join("real/sub")).await?;
    tokio::fs::write(tmp.join("real/a.txt"), b"x").await?;
    tokio::fs::symlink(tmp.join("real"), tmp.join("link")).await?;

    common::safedir::enable_strict_operand_resolution();

    // a symlink component anywhere in the operand path fails closed with ELOOP
    let err =
        common::safedir::Dir::open_root_dir(&tmp.join("link/sub"), false, common::Side::Source)
            .await
            .expect_err("strict resolution must refuse a symlinked prefix component");
    assert_eq!(
        err.raw_os_error(),
        Some(libc::ELOOP),
        "expected ELOOP, got: {err:?}"
    );
    let err = common::safedir::Dir::open_parent_dir(&tmp.join("link"), common::Side::Source)
        .await
        .expect_err("strict resolution must refuse a symlinked parent operand");
    assert_eq!(
        err.raw_os_error(),
        Some(libc::ELOOP),
        "expected ELOOP, got: {err:?}"
    );

    // symlink-free operand paths still open and stay fully functional
    let root =
        common::safedir::Dir::open_root_dir(&tmp.join("real"), false, common::Side::Source).await?;
    let (_file, _meta) = root.open_file_read(std::ffi::OsStr::new("a.txt")).await?;
    let parent = common::safedir::Dir::open_parent_dir(&tmp.join("real"), common::Side::Source)
        .await?
        .into_tree();
    parent.open_dir(std::ffi::OsStr::new("sub")).await?;
    Ok(())
}

/// `strict_probe_dst_kind` decomposes the path so an INTERMEDIATE-prefix symlink
/// fails closed (ELOOP), while a final component that is merely a symlink is
/// reported as existing (`Some(Symlink)`, not followed) and a genuinely absent
/// entry is `Ok(None)` — never conflated.
#[tokio::test]
async fn strict_probe_separates_intermediate_from_final() -> anyhow::Result<()> {
    use common::safedir::strict_probe_dst_kind;
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2)");
        return Ok(());
    }
    let tmp = tempfile::tempdir()?;
    let tmp = tokio::fs::canonicalize(tmp.path()).await?;
    tokio::fs::create_dir_all(tmp.join("real/dir")).await?;
    tokio::fs::write(tmp.join("real/file.txt"), b"x").await?;
    tokio::fs::symlink(tmp.join("real"), tmp.join("prefixlink")).await?; // intermediate symlink
    tokio::fs::symlink(tmp.join("real/dir"), tmp.join("real/finallink")).await?; // final symlink

    common::safedir::enable_strict_operand_resolution();

    // intermediate-prefix symlink → ELOOP (fail closed)
    let err = strict_probe_dst_kind(&tmp.join("prefixlink/file.txt"), common::Side::Destination)
        .await
        .expect_err("intermediate-prefix symlink must fail closed");
    assert_eq!(err.raw_os_error(), Some(libc::ELOOP), "got: {err:?}");

    // a real final file classifies as a file
    assert_eq!(
        strict_probe_dst_kind(&tmp.join("real/file.txt"), common::Side::Destination).await?,
        Some(common::walk::EntryKind::File)
    );

    // a FINAL-component symlink: exists (not followed), classified Symlink — NOT ELOOP
    assert_eq!(
        strict_probe_dst_kind(&tmp.join("real/finallink"), common::Side::Destination).await?,
        Some(common::walk::EntryKind::Symlink)
    );

    // a genuinely absent entry (real parent) → Ok(None), not an error
    assert_eq!(
        strict_probe_dst_kind(&tmp.join("real/absent"), common::Side::Destination).await?,
        None
    );
    Ok(())
}

/// The linter arms strict operand resolution when `--require-toctou-safe`
/// proceeds with well-formed operands.
#[test]
fn require_mode_arms_strict_resolution_on_proceed() {
    // under nextest this test owns its process, so the switch starts unarmed and
    // this proves the off→on transition; under a shared-process `cargo test` a
    // sibling test in this binary may have armed it already, which the one-way
    // switch makes indistinguishable from proceeding — so no precondition assert.
    let operands = vec![
        std::path::PathBuf::from("/ok/src"),
        std::path::PathBuf::from("/ok/dst"),
    ];
    match run_linter(false, false, true, &operands) {
        LinterAction::Proceed => {
            assert!(
                common::safedir::strict_operand_resolution(),
                "linter Proceed in require mode must arm strict operand resolution"
            );
        }
        LinterAction::Exit { output, code } => {
            // on kernels without openat2 the refusal is the correct outcome
            assert!(
                !common::safedir::openat2_available(),
                "good operands must proceed on openat2-capable kernels, got: {output}"
            );
            assert_eq!(code, 1);
            assert!(output.contains("openat2"), "got: {output}");
        }
    }
}

/// rcpd passes no operands (they arrive via the master, already validated);
/// an empty operand list must proceed — and still arm strict resolution.
#[test]
fn require_mode_with_no_operands_proceeds() {
    match run_linter(false, false, true, &[]) {
        LinterAction::Proceed => {
            assert!(
                common::safedir::strict_operand_resolution(),
                "linter Proceed in require mode must arm strict operand resolution"
            );
        }
        LinterAction::Exit { output, code } => {
            // on kernels without openat2 the refusal is the correct outcome
            assert!(
                !common::safedir::openat2_available(),
                "empty operand list must proceed on openat2-capable kernels, got: {output}"
            );
            assert_eq!(code, 1);
            assert!(output.contains("openat2"), "got: {output}");
        }
    }
}
