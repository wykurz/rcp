use chrono::TimeZone;
// re-export RcpdType from common to avoid duplication
pub use common::RcpdType;

// static storage for the latest progress from each rcpd process
static PROGRESS_MAP: std::sync::LazyLock<
    std::sync::Mutex<enum_map::EnumMap<RcpdType, common::SerializableProgress>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(enum_map::EnumMap::default()));

/// Get the latest progress snapshot from all rcpd processes
pub fn get_latest_progress_snapshot() -> enum_map::EnumMap<RcpdType, common::SerializableProgress> {
    PROGRESS_MAP.lock().unwrap().clone()
}

/// Dispatch one forwarded tracing or progress message received from a remote `rcpd`.
pub(crate) fn dispatch_message(
    tracing_message: common::remote_tracing::TracingMessage,
    rcpd_type: RcpdType,
) {
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
                // A forwarded NOTICE keeps its own target instead of the blanket `remote` one.
                // Re-emitting it as `remote` would hand it to the master's filter, which at the
                // default verbosity passes only `error` and `rcp::notice` — so the notice would
                // survive the rcpd's filter, cross the wire, and then be dropped one step from
                // the user. That is the exact gap the dedicated target exists to close, and it
                // is invisible unless a test runs a real remote copy. The `remote::<role>::`
                // provenance stays in the message text, as for every other forwarded line.
                tracing::Level::WARN if target == common::NOTICE_TARGET => {
                    tracing::warn!(target: common::NOTICE_TARGET, "[{}] {}: {}", timestamp_str, remote_target, message)
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

#[cfg(test)]
mod tests;
