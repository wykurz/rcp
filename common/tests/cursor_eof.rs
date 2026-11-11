//! The ops throttle is process-global, so this test is alone in its integration binary.
//! Its exhausted rate limit cannot affect other tests, including in Nix's shared-process runner.

use futures::FutureExt as _;

#[tokio::test]
async fn known_eof_returns_without_an_ops_token() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    common::filegen::write_file(common::get_progress(), tmp.path().join("entry"), 1, 1, 0)
        .await
        .map_err(|error| error.source)?;
    let root =
        common::safedir::Dir::open_root_dir(tmp.path(), false, congestion::Side::Source).await?;
    let mut completed = root.entries().await?;
    let mut unread = root.entries().await?;
    let limit = std::num::NonZeroUsize::new(64).unwrap();
    assert_eq!(completed.next_batch(limit).await?.len(), 1);
    throttle::init_ops_tokens(1);
    throttle::get_ops_token().await;
    assert!(throttle::get_ops_token().now_or_never().is_none());
    for _ in 0..3 {
        let result = completed.next_batch(limit).now_or_never();
        assert!(
            matches!(result, Some(Ok(batch)) if batch.is_empty()),
            "known EOF must return without consuming another ops token"
        );
    }
    assert!(
        unread.next_batch(limit).now_or_never().is_none(),
        "a fresh read must still wait for an ops token"
    );
    throttle::disable_ops_throttle();
    // cancelling before rate admission must leave the unread cursor usable
    assert_eq!(unread.next_batch(limit).await?.len(), 1);
    Ok(())
}
