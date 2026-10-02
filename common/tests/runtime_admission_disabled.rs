//! Disabling runtime setup preserves existing endpoint admission.

#[test]
fn disabled_runtime_setup_preserves_existing_admission() {
    throttle::set_admission_limits(std::num::NonZeroUsize::new(1));
    let result = common::run(
        None,
        common::OutputConfig::default(),
        common::RuntimeConfig {
            max_workers: 1,
            max_blocking_threads: 1,
        },
        common::ThrottleConfig {
            apply_files_in_flight: false,
            ..Default::default()
        },
        common::TracingConfig::default(),
        |admission| async move {
            assert_eq!(admission, common::EndpointAdmission::Disabled);
            let open_file = throttle::open_file_permit().await;
            let pending_meta = throttle::pending_meta_permit().await;
            let another_open = throttle::open_file_permit();
            let another_meta = throttle::pending_meta_permit();
            tokio::pin!(another_open, another_meta);
            assert!(futures::poll!(&mut another_open).is_pending());
            assert!(futures::poll!(&mut another_meta).is_pending());
            drop((open_file, pending_meta));
            Ok::<_, anyhow::Error>("disabled")
        },
    );
    assert_eq!(result, Some("disabled"));
}
