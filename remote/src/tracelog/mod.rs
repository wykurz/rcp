#[derive(Debug, Clone, Copy)]
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
    // Storage for the latest progress update from this rcpd process
    let mut _latest_progress: Option<common::SerializableProgress> = None;

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
                let remote_target = format!("remote::{}::{target}", rcpd_type);
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
                // Store the latest progress update from this rcpd process
                _latest_progress = Some(progress.clone());
                tracing::debug!(target: "remote", "Received progress update from {} rcpd", rcpd_type);
                // TODO: Send the progress to the master process for aggregation
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
