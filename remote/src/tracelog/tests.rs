use super::*;

#[test]
fn test_rcpd_type_display() {
    assert_eq!(format!("{}", RcpdType::Source), "source");
    assert_eq!(format!("{}", RcpdType::Destination), "destination");
}

#[test]
fn test_rcpd_type_debug() {
    assert_eq!(format!("{:?}", RcpdType::Source), "Source");
    assert_eq!(format!("{:?}", RcpdType::Destination), "Destination");
}

#[test]
fn dispatch_message_updates_progress_snapshot() {
    let progress = common::progress::SerializableProgress {
        files_copied: 7,
        bytes_copied: 4096,
        ..Default::default()
    };
    dispatch_message(
        common::remote_tracing::TracingMessage::Progress(progress),
        RcpdType::Destination,
    );
    let snapshot = get_latest_progress_snapshot();
    assert_eq!(snapshot[RcpdType::Destination].files_copied, 7);
    assert_eq!(snapshot[RcpdType::Destination].bytes_copied, 4096);
}
