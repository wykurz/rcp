//! Shared receiver cancellation and pre-teardown connection-error ownership.

/// One shutdown transition shared by directory claims, control work, and data workers.
#[derive(Clone, Default)]
pub(crate) struct ReceiverShutdown {
    inner: std::sync::Arc<ShutdownState>,
}

#[derive(Default)]
struct ShutdownState {
    cancelled: tokio_util::sync::CancellationToken,
    first_connect_error: std::sync::Mutex<Option<anyhow::Error>>,
}

impl ReceiverShutdown {
    /// Stop receiver work without depending on the tracker or control-stream locks.
    pub(crate) fn cancel(&self) {
        // cancellation and cause insertion share this lock: a recorder either commits before
        // teardown or observes cancellation. Keep the token private so no caller bypasses it.
        let _error = self
            .inner
            .first_connect_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.cancelled.cancel();
    }
    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.cancelled.is_cancelled()
    }
    pub(crate) async fn cancelled(&self) {
        self.inner.cancelled.cancelled().await;
    }
    /// Retain the first connection failure only while the receiver is still running.
    pub(crate) fn record_first_connect_error(&self, error: anyhow::Error) {
        let mut first = self
            .inner
            .first_connect_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.is_cancelled() && first.is_none() {
            *first = Some(error);
        }
    }
    pub(crate) fn take_first_connect_error(&self) -> Option<anyhow::Error> {
        self.inner
            .first_connect_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_preserves_the_first_connection_cause() {
        let shutdown = ReceiverShutdown::default();
        shutdown.record_first_connect_error(
            std::io::Error::from_raw_os_error(libc::ECONNREFUSED).into(),
        );
        shutdown
            .record_first_connect_error(std::io::Error::from_raw_os_error(libc::ETIMEDOUT).into());
        shutdown.cancel();
        shutdown
            .record_first_connect_error(std::io::Error::from_raw_os_error(libc::ECONNRESET).into());
        let cause = shutdown.take_first_connect_error().unwrap();
        assert_eq!(
            cause
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::ECONNREFUSED)
        );
        assert!(shutdown.take_first_connect_error().is_none());
    }

    #[test]
    fn cancellation_discards_late_connection_failures() {
        let shutdown = ReceiverShutdown::default();
        shutdown.cancel();
        shutdown.record_first_connect_error(
            std::io::Error::from_raw_os_error(libc::ECONNREFUSED).into(),
        );
        assert!(shutdown.take_first_connect_error().is_none());
    }
}
