// Re-export RcpdType from common to avoid duplication
use chrono::TimeZone;
pub use common::RcpdType;

lazy_static::lazy_static! {
    // static storage for the latest progress from each rcpd process
    static ref PROGRESS_MAP: std::sync::Mutex<enum_map::EnumMap<RcpdType, common::SerializableProgress>> =
        std::sync::Mutex::new(enum_map::EnumMap::default());
}

/// Get the latest progress snapshot from all rcpd processes
pub fn get_latest_progress_snapshot() -> enum_map::EnumMap<RcpdType, common::SerializableProgress> {
    PROGRESS_MAP.lock().unwrap().clone()
}

/// Sends tracing messages from rcpd to the master process over the TCP stream.
///
/// CANCEL SAFETY: both branches are cancel-safe:
/// - `receiver.recv()`: tokio mpsc channel recv is cancel-safe
/// - `cancellation_token.cancelled()`: cancel-safe (just polls a flag)
pub async fn run_sender<W: tokio::io::AsyncWrite + Unpin + Send>(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
    mut send_stream: crate::streams::SendStream<W>,
    cancellation_token: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    while let Some(msg) = tokio::select! {
        msg = receiver.recv() => msg,
        _ = cancellation_token.cancelled() => {
            println!("Remote tracing sender done");
            return Ok(());
        }
    } {
        if let Err(e) = send_stream.send_batch_message(&msg).await {
            eprintln!("Failed to send tracing message: {e}");
        }
    }
    println!("Remote tracing sender done, no more messages to send");
    Ok(())
}

pub async fn run_receiver<R: tokio::io::AsyncRead + Unpin + Send>(
    mut recv_stream: crate::streams::RecvStream<R>,
    rcpd_type: RcpdType,
) -> anyhow::Result<()> {
    while let Some(tracing_message) = recv_stream
        .recv_object::<common::remote_tracing::TracingMessage>()
        .await?
    {
        match tracing_message {
            common::remote_tracing::TracingMessage::Log {
                timestamp,
                level,
                target,
                message,
            } => {
                let log_level = match level.as_str() {
                    "ERROR" => tracing::Level::ERROR,
                    "WARN" => tracing::Level::WARN,
                    "INFO" => tracing::Level::INFO,
                    "DEBUG" => tracing::Level::DEBUG,
                    "TRACE" => tracing::Level::TRACE,
                    _ => tracing::Level::INFO,
                };
                let remote_target = format!("remote::{rcpd_type}::{target}");
                let timestamp_str = match timestamp.duration_since(std::time::UNIX_EPOCH) {
                    Ok(duration) => {
                        let datetime = chrono::Local
                            .timestamp_opt(duration.as_secs() as i64, duration.subsec_nanos());
                        match datetime.single() {
                            Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            None => format!("{timestamp:?}"),
                        }
                    }
                    Err(_) => format!("{timestamp:?}"),
                };
                match log_level {
                    tracing::Level::ERROR => {
                        tracing::error!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, message)
                    }
                    tracing::Level::WARN => {
                        tracing::warn!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, message)
                    }
                    tracing::Level::INFO => {
                        tracing::info!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, message)
                    }
                    tracing::Level::DEBUG => {
                        tracing::debug!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, message)
                    }
                    tracing::Level::TRACE => {
                        tracing::trace!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, message)
                    }
                }
            }
            common::remote_tracing::TracingMessage::Progress(progress) => {
                tracing::debug!(target: "remote", "Received progress update from {} rcpd: {:?}", rcpd_type, progress);
                PROGRESS_MAP.lock().unwrap()[rcpd_type] = progress;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
}
