//! Operation callbacks receive only successfully configured endpoint admission.
//!
//! Runtime setup installs a process-global tracing subscriber. Each setup scenario has its own
//! integration binary so it also runs independently under libtest.

#[test]
fn operation_observes_the_capacity_installed_in_both_leaf_pools() {
    let one = std::num::NonZeroUsize::new(1).unwrap();
    let result = common::run(
        None,
        common::OutputConfig::default(),
        common::RuntimeConfig {
            max_workers: 1,
            max_blocking_threads: 1,
        },
        common::ThrottleConfig {
            files_in_flight: common::ResolvedFilesInFlight::explicit(one),
            ..Default::default()
        },
        common::TracingConfig::default(),
        |admission| async move {
            let common::EndpointAdmission::Configured {
                soft_limit,
                leaf_capacity,
            } = admission
            else {
                panic!("operation did not receive configured admission");
            };
            assert!(soft_limit.is_some());
            assert_eq!(leaf_capacity, one);
            let open_file = throttle::open_file_permit().await;
            let pending_meta = throttle::pending_meta_permit().await;
            let another_open = throttle::open_file_permit();
            let another_meta = throttle::pending_meta_permit();
            tokio::pin!(another_open, another_meta);
            assert!(futures::poll!(&mut another_open).is_pending());
            assert!(futures::poll!(&mut another_meta).is_pending());
            drop((open_file, pending_meta));
            let _admitted = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                tokio::join!(another_open, another_meta)
            })
            .await
            .unwrap();
            Ok::<_, anyhow::Error>("configured")
        },
    );
    assert_eq!(result, Some("configured"));
}
