//! Runtime setup failures do not invoke the operation callback.

#[test]
fn failed_startup_never_invokes_the_operation_callback() {
    let mut called = false;
    let result = common::run(
        None,
        common::OutputConfig::default(),
        common::RuntimeConfig::default(),
        common::ThrottleConfig {
            iops_throttle: 1,
            chunk_size: 0,
            ..Default::default()
        },
        common::TracingConfig::default(),
        |_admission| {
            called = true;
            async { Ok::<_, anyhow::Error>("must not run") }
        },
    );
    assert!(result.is_none());
    assert!(!called);
}
