//! Generic error type for tree-walking operations that preserves the partial
//! summary (work that completed before the error) alongside the error chain.
//!
//! Each operation (copy, link, rm, filegen) wraps this with a per-operation
//! `Summary` via a type alias.
//!
//! # Logging Convention
//! When logging this error, use `{:#}` or `{:?}` format to preserve the error chain:
//! ```ignore
//! tracing::error!("operation failed: {:#}", &error); // ✅ Shows full chain
//! tracing::error!("operation failed: {:?}", &error); // ✅ Shows full chain
//! ```
//! The Display implementation also shows the full chain, but workspace linting enforces `{:#}`
//! for consistency.

#[derive(Debug, thiserror::Error)]
#[error("{source:#}")]
pub struct OperationError<S> {
    #[source]
    pub source: anyhow::Error,
    pub summary: S,
}

impl<S> OperationError<S> {
    #[must_use]
    pub fn new(source: anyhow::Error, summary: S) -> Self {
        OperationError { source, summary }
    }
}
