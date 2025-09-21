#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, enum_map::Enum)]
pub enum RcpdType {
    Source,
    Destination,
}

impl std::fmt::Display for RcpdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RcpdType::Source => write!(f, "source"),
            RcpdType::Destination => write!(f, "destination"),
        }
    }
}

lazy_static::lazy_static! {
    // static storage for the latest progress from each rcpd process
    static ref PROGRESS_MAP: std::sync::Mutex<enum_map::EnumMap<RcpdType, Option<common::SerializableProgress>>> =
        std::sync::Mutex::new(enum_map::EnumMap::default());
}

/// Get the latest progress snapshot from all rcpd processes
pub fn get_latest_progress_snapshot(
) -> enum_map::EnumMap<RcpdType, Option<common::SerializableProgress>> {
    PROGRESS_MAP.lock().unwrap().clone()
}

pub async fn run_sender(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
    mut send_stream: crate::streams::SendStream,
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

pub async fn run_receiver(
    mut recv_stream: crate::streams::RecvStream,
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
                        let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(
                            duration.as_secs() as i64,
                            duration.subsec_nanos(),
                        );
                        match datetime {
                            Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
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
                tracing::debug!(target: "remote", "Received progress update from {} rcpd", rcpd_type);
                PROGRESS_MAP.lock().unwrap()[rcpd_type] = Some(progress.clone());
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
