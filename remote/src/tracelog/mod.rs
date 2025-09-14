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

pub async fn run_receiver(mut recv_stream: crate::streams::RecvStream) -> anyhow::Result<()> {
    while let Some(tracing_message) = recv_stream
        .recv_object::<common::remote_tracing::TracingMessage>()
        .await?
    {
        let level = match tracing_message.level.as_str() {
            "ERROR" => tracing::Level::ERROR,
            "WARN" => tracing::Level::WARN,
            "INFO" => tracing::Level::INFO,
            "DEBUG" => tracing::Level::DEBUG,
            "TRACE" => tracing::Level::TRACE,
            _ => tracing::Level::INFO,
        };
        let remote_target = format!("remote::{}", tracing_message.target);
        let timestamp_str = match tracing_message
            .timestamp
            .duration_since(std::time::UNIX_EPOCH)
        {
            Ok(duration) => {
                let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(
                    duration.as_secs() as i64,
                    duration.subsec_nanos(),
                );
                match datetime {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
                    None => format!("{:?}", tracing_message.timestamp),
                }
            }
            Err(_) => format!("{:?}", tracing_message.timestamp),
        };
        match level {
            tracing::Level::ERROR => {
                tracing::error!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, tracing_message.message)
            }
            tracing::Level::WARN => {
                tracing::warn!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, tracing_message.message)
            }
            tracing::Level::INFO => {
                tracing::info!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, tracing_message.message)
            }
            tracing::Level::DEBUG => {
                tracing::debug!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, tracing_message.message)
            }
            tracing::Level::TRACE => {
                tracing::trace!(target: "remote", "[{}] {}: {}", timestamp_str, remote_target, tracing_message.message)
            }
        }
    }
    Ok(())
}
