//! Configuration types for runtime and execution settings

/// Runtime configuration for tokio and thread pools
#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeConfig {
    /// Number of worker threads (0 = number of CPU cores)
    pub max_workers: usize,
    /// Number of blocking threads (0 = tokio default of 512)
    pub max_blocking_threads: usize,
}

/// Throttling configuration for resource control
#[derive(Debug, Clone, Copy, Default)]
pub struct ThrottleConfig {
    /// Maximum number of open files (None = 80% of system limit)
    pub max_open_files: Option<usize>,
    /// Operations per second throttle (0 = no throttle)
    pub ops_throttle: usize,
    /// I/O operations per second throttle (0 = no throttle)
    pub iops_throttle: usize,
    /// Chunk size for I/O operations (bytes)
    pub chunk_size: u64,
}

impl ThrottleConfig {
    /// Validate configuration and return errors if invalid
    pub fn validate(&self) -> Result<(), String> {
        if self.iops_throttle > 0 && self.chunk_size == 0 {
            return Err("chunk_size must be specified when using iops_throttle".to_string());
        }
        Ok(())
    }
}

/// Output and logging configuration
#[derive(Debug, Clone, Copy, Default)]
pub struct OutputConfig {
    /// Suppress error output
    pub quiet: bool,
    /// Verbosity level: 0=ERROR, 1=INFO, 2=DEBUG, 3=TRACE
    pub verbose: u8,
    /// Print summary statistics at the end
    pub print_summary: bool,
}

/// Tracing configuration for debugging
#[derive(Debug, Default)]
pub struct TracingConfig {
    /// Remote tracing layer for distributed tracing
    pub remote_layer: Option<crate::remote_tracing::RemoteTracingLayer>,
    /// Debug log file path
    pub debug_log_file: Option<String>,
}
