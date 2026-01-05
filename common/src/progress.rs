use tracing::instrument;

/// Number of shards for the counter. More shards reduce contention but increase memory.
/// 64 shards × 128 bytes = 8KB per counter, which virtually eliminates contention.
const NUM_SHARDS: usize = 64;

/// Atomic counter padded to cache line size to prevent false sharing.
/// Each shard lives on its own cache line so concurrent updates from different
/// threads don't cause cache invalidation.
/// Uses 128B alignment to support both x86-64 (64B) and ARM (128B) cache lines.
#[repr(align(128))]
struct PaddedAtomicU64(std::sync::atomic::AtomicU64);

/// Global counter for assigning shard indices to threads.
/// Each thread gets a unique index (mod NUM_SHARDS) on first access.
static NEXT_SHARD_INDEX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

thread_local! {
    /// Per-thread shard index, assigned once on first access.
    /// Uses modulo to wrap around when more threads than shards.
    static MY_SHARD: usize =
        NEXT_SHARD_INDEX.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % NUM_SHARDS;
}

/// Sharded atomic counter optimized for concurrent access from multiple threads.
///
/// Uses cache-line-padded shards to prevent false sharing. Each thread is assigned
/// a shard index, so updates from different threads typically hit different cache lines.
///
/// This design handles interleaved access to multiple counters efficiently - unlike
/// a single-slot cache approach, there's no "cache thrashing" when alternating between
/// counters.
///
/// # Memory
///
/// Each counter uses NUM_SHARDS × 128 bytes = 8KB (with 64 shards).
/// This is larger than a simple AtomicU64 but virtually eliminates contention.
pub struct TlsCounter {
    shards: [PaddedAtomicU64; NUM_SHARDS],
}

impl TlsCounter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| PaddedAtomicU64(std::sync::atomic::AtomicU64::new(0))),
        }
    }

    pub fn add(&self, value: u64) {
        let shard = MY_SHARD.with(|&s| s);
        self.shards[shard]
            .0
            .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.add(1);
    }

    pub fn get(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.0.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    }
}

impl std::fmt::Debug for TlsCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsCounter")
            .field("value", &self.get())
            .finish()
    }
}

impl Default for TlsCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ProgressCounter {
    started: TlsCounter,
    finished: TlsCounter,
}

impl Default for ProgressCounter {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ProgressGuard<'a> {
    progress: &'a ProgressCounter,
}

impl<'a> ProgressGuard<'a> {
    pub fn new(progress: &'a ProgressCounter) -> Self {
        progress.started.inc();
        Self { progress }
    }
}

impl Drop for ProgressGuard<'_> {
    fn drop(&mut self) {
        self.progress.finished.inc();
    }
}

pub struct Status {
    pub started: u64,
    pub finished: u64,
}

impl ProgressCounter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            started: TlsCounter::new(),
            finished: TlsCounter::new(),
        }
    }

    pub fn guard(&self) -> ProgressGuard<'_> {
        ProgressGuard::new(self)
    }

    #[instrument]
    pub fn get(&self) -> Status {
        let mut status = Status {
            started: self.started.get(),
            finished: self.finished.get(),
        };
        if status.finished > status.started {
            tracing::debug!(
                "Progress inversion - started: {}, finished {}",
                status.started,
                status.finished
            );
            status.started = status.finished;
        }
        status
    }
}

pub struct Progress {
    pub ops: ProgressCounter,
    pub bytes_copied: TlsCounter,
    pub hard_links_created: TlsCounter,
    pub files_copied: TlsCounter,
    pub symlinks_created: TlsCounter,
    pub directories_created: TlsCounter,
    pub files_unchanged: TlsCounter,
    pub symlinks_unchanged: TlsCounter,
    pub directories_unchanged: TlsCounter,
    pub hard_links_unchanged: TlsCounter,
    pub files_removed: TlsCounter,
    pub symlinks_removed: TlsCounter,
    pub directories_removed: TlsCounter,
    start_time: std::time::Instant,
}

impl Progress {
    #[must_use]
    pub fn new() -> Self {
        Self {
            ops: Default::default(),
            bytes_copied: Default::default(),
            hard_links_created: Default::default(),
            files_copied: Default::default(),
            symlinks_created: Default::default(),
            directories_created: Default::default(),
            files_unchanged: Default::default(),
            symlinks_unchanged: Default::default(),
            directories_unchanged: Default::default(),
            hard_links_unchanged: Default::default(),
            files_removed: Default::default(),
            symlinks_removed: Default::default(),
            directories_removed: Default::default(),
            start_time: std::time::Instant::now(),
        }
    }

    pub fn get_duration(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }
}

impl Default for Progress {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SerializableProgress {
    pub ops_started: u64,
    pub ops_finished: u64,
    pub bytes_copied: u64,
    pub hard_links_created: u64,
    pub files_copied: u64,
    pub symlinks_created: u64,
    pub directories_created: u64,
    pub files_unchanged: u64,
    pub symlinks_unchanged: u64,
    pub directories_unchanged: u64,
    pub hard_links_unchanged: u64,
    pub files_removed: u64,
    pub symlinks_removed: u64,
    pub directories_removed: u64,
    pub current_time: std::time::SystemTime,
}

impl Default for SerializableProgress {
    fn default() -> Self {
        Self {
            ops_started: 0,
            ops_finished: 0,
            bytes_copied: 0,
            hard_links_created: 0,
            files_copied: 0,
            symlinks_created: 0,
            directories_created: 0,
            files_unchanged: 0,
            symlinks_unchanged: 0,
            directories_unchanged: 0,
            hard_links_unchanged: 0,
            files_removed: 0,
            symlinks_removed: 0,
            directories_removed: 0,
            current_time: std::time::SystemTime::now(),
        }
    }
}

impl From<&Progress> for SerializableProgress {
    /// Creates a `SerializableProgress` from a Progress, capturing the current time at the moment of conversion
    fn from(progress: &Progress) -> Self {
        Self {
            ops_started: progress.ops.started.get(),
            ops_finished: progress.ops.finished.get(),
            bytes_copied: progress.bytes_copied.get(),
            hard_links_created: progress.hard_links_created.get(),
            files_copied: progress.files_copied.get(),
            symlinks_created: progress.symlinks_created.get(),
            directories_created: progress.directories_created.get(),
            files_unchanged: progress.files_unchanged.get(),
            symlinks_unchanged: progress.symlinks_unchanged.get(),
            directories_unchanged: progress.directories_unchanged.get(),
            hard_links_unchanged: progress.hard_links_unchanged.get(),
            files_removed: progress.files_removed.get(),
            symlinks_removed: progress.symlinks_removed.get(),
            directories_removed: progress.directories_removed.get(),
            current_time: std::time::SystemTime::now(),
        }
    }
}

pub struct ProgressPrinter<'a> {
    progress: &'a Progress,
    last_ops: u64,
    last_bytes: u64,
    last_update: std::time::Instant,
}

impl<'a> ProgressPrinter<'a> {
    pub fn new(progress: &'a Progress) -> Self {
        Self {
            progress,
            last_ops: progress.ops.get().finished,
            last_bytes: progress.bytes_copied.get(),
            last_update: std::time::Instant::now(),
        }
    }

    pub fn print(&mut self) -> anyhow::Result<String> {
        let time_now = std::time::Instant::now();
        let ops = self.progress.ops.get();
        let total_duration_secs = self.progress.get_duration().as_secs_f64();
        let curr_duration_secs = (time_now - self.last_update).as_secs_f64();
        let average_ops_rate = ops.finished as f64 / total_duration_secs;
        let current_ops_rate = (ops.finished - self.last_ops) as f64 / curr_duration_secs;
        let bytes = self.progress.bytes_copied.get();
        let average_bytes_rate = bytes as f64 / total_duration_secs;
        let current_bytes_rate = (bytes - self.last_bytes) as f64 / curr_duration_secs;
        // update self
        self.last_ops = ops.finished;
        self.last_bytes = bytes;
        self.last_update = time_now;
        // nice to have: convert to a table
        Ok(format!(
            "---------------------\n\
            OPS:\n\
            pending: {:>10}\n\
            average: {:>10.2} items/s\n\
            current: {:>10.2} items/s\n\
            -----------------------\n\
            COPIED:\n\
            average: {:>10}/s\n\
            current: {:>10}/s\n\
            total:   {:>10}\n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}\n\
            hard-links:  {:>10}\n\
            -----------------------\n\
            UNCHANGED:\n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}\n\
            hard-links:  {:>10}\n\
            -----------------------\n\
            REMOVED:\n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}",
            ops.started - ops.finished, // pending
            average_ops_rate,
            current_ops_rate,
            // copy
            bytesize::ByteSize(average_bytes_rate as u64),
            bytesize::ByteSize(current_bytes_rate as u64),
            bytesize::ByteSize(self.progress.bytes_copied.get()),
            self.progress.files_copied.get(),
            self.progress.symlinks_created.get(),
            self.progress.directories_created.get(),
            self.progress.hard_links_created.get(),
            // unchanged
            self.progress.files_unchanged.get(),
            self.progress.symlinks_unchanged.get(),
            self.progress.directories_unchanged.get(),
            self.progress.hard_links_unchanged.get(),
            // remove
            self.progress.files_removed.get(),
            self.progress.symlinks_removed.get(),
            self.progress.directories_removed.get(),
        ))
    }
}

pub struct RcpdProgressPrinter {
    start_time: std::time::Instant,
    last_source_ops: u64,
    last_source_bytes: u64,
    last_source_files: u64,
    last_dest_ops: u64,
    last_dest_bytes: u64,
    last_update: std::time::Instant,
}

impl RcpdProgressPrinter {
    #[must_use]
    pub fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            start_time: now,
            last_source_ops: 0,
            last_source_bytes: 0,
            last_source_files: 0,
            last_dest_ops: 0,
            last_dest_bytes: 0,
            last_update: now,
        }
    }

    fn calculate_current_rate(&self, current: u64, last: u64, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            (current - last) as f64 / duration_secs
        } else {
            0.0
        }
    }

    fn calculate_average_rate(&self, total: u64, total_duration_secs: f64) -> f64 {
        if total_duration_secs > 0.0 {
            total as f64 / total_duration_secs
        } else {
            0.0
        }
    }

    pub fn print(
        &mut self,
        source_progress: &SerializableProgress,
        dest_progress: &SerializableProgress,
    ) -> anyhow::Result<String> {
        let time_now = std::time::Instant::now();
        let total_duration_secs = (time_now - self.start_time).as_secs_f64();
        let curr_duration_secs = (time_now - self.last_update).as_secs_f64();
        // source current rates
        let source_ops_rate_curr = self.calculate_current_rate(
            source_progress.ops_finished,
            self.last_source_ops,
            curr_duration_secs,
        );
        let source_bytes_rate_curr = self.calculate_current_rate(
            source_progress.bytes_copied,
            self.last_source_bytes,
            curr_duration_secs,
        );
        let source_files_rate_curr = self.calculate_current_rate(
            source_progress.files_copied,
            self.last_source_files,
            curr_duration_secs,
        );
        // source average rates
        let source_ops_rate_avg =
            self.calculate_average_rate(source_progress.ops_finished, total_duration_secs);
        let source_bytes_rate_avg =
            self.calculate_average_rate(source_progress.bytes_copied, total_duration_secs);
        let source_files_rate_avg =
            self.calculate_average_rate(source_progress.files_copied, total_duration_secs);
        // destination current rates
        let dest_ops_rate_curr = self.calculate_current_rate(
            dest_progress.ops_finished,
            self.last_dest_ops,
            curr_duration_secs,
        );
        let dest_bytes_rate_curr = self.calculate_current_rate(
            dest_progress.bytes_copied,
            self.last_dest_bytes,
            curr_duration_secs,
        );
        // destination average rates
        let dest_ops_rate_avg =
            self.calculate_average_rate(dest_progress.ops_finished, total_duration_secs);
        let dest_bytes_rate_avg =
            self.calculate_average_rate(dest_progress.bytes_copied, total_duration_secs);
        // update last values
        self.last_source_ops = source_progress.ops_finished;
        self.last_source_bytes = source_progress.bytes_copied;
        self.last_source_files = source_progress.files_copied;
        self.last_dest_ops = dest_progress.ops_finished;
        self.last_dest_bytes = dest_progress.bytes_copied;
        self.last_update = time_now;
        Ok(format!(
            "==== SOURCE =======\n\
            OPS:\n\
            pending: {:>10}\n\
            average: {:>10.2} items/s\n\
            current: {:>10.2} items/s\n\
            ---------------------\n\
            COPIED:\n\
            average: {:>10}/s\n\
            current: {:>10}/s\n\
            total:   {:>10}\n\
            files:       {:>10}\n\
            ---------------------\n\
            FILES:\n\
            average: {:>10.2} files/s\n\
            current: {:>10.2} files/s\n\
            ==== DESTINATION ====\n\
            OPS:\n\
            pending: {:>10}\n\
            average: {:>10.2} items/s\n\
            current: {:>10.2} items/s\n\
            ---------------------\n\
            COPIED:\n\
            average: {:>10}/s\n\
            current: {:>10}/s\n\
            total:   {:>10}\n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}\n\
            hard-links:  {:>10}\n\
            ---------------------\n\
            UNCHANGED:\n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}\n\
            hard-links:  {:>10}\n\
            ---------------------\n\
            REMOVED:\n\
            files:       {:>10}\n\
            symlinks:    {:>10}\n\
            directories: {:>10}",
            // source section
            source_progress.ops_started - source_progress.ops_finished, // pending
            source_ops_rate_avg,
            source_ops_rate_curr,
            bytesize::ByteSize(source_bytes_rate_avg as u64),
            bytesize::ByteSize(source_bytes_rate_curr as u64),
            bytesize::ByteSize(source_progress.bytes_copied),
            source_progress.files_copied,
            source_files_rate_avg,
            source_files_rate_curr,
            // destination section
            dest_progress.ops_started - dest_progress.ops_finished, // pending
            dest_ops_rate_avg,
            dest_ops_rate_curr,
            bytesize::ByteSize(dest_bytes_rate_avg as u64),
            bytesize::ByteSize(dest_bytes_rate_curr as u64),
            bytesize::ByteSize(dest_progress.bytes_copied),
            // destination detailed stats
            dest_progress.files_copied,
            dest_progress.symlinks_created,
            dest_progress.directories_created,
            dest_progress.hard_links_created,
            // unchanged
            dest_progress.files_unchanged,
            dest_progress.symlinks_unchanged,
            dest_progress.directories_unchanged,
            dest_progress.hard_links_unchanged,
            // removed
            dest_progress.files_removed,
            dest_progress.symlinks_removed,
            dest_progress.directories_removed,
        ))
    }
}

impl Default for RcpdProgressPrinter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_tracing::TracingMessage;
    use anyhow::Result;

    #[test]
    fn basic_counting() -> Result<()> {
        let tls_counter = TlsCounter::new();
        for _ in 0..10 {
            tls_counter.inc();
        }
        assert!(tls_counter.get() == 10);
        Ok(())
    }

    #[test]
    fn threaded_counting() -> Result<()> {
        let tls_counter = TlsCounter::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..10 {
                handles.push(scope.spawn(|| {
                    for _ in 0..100 {
                        tls_counter.inc();
                    }
                }));
            }
        });
        assert!(tls_counter.get() == 1000);
        Ok(())
    }

    #[test]
    fn basic_guard() -> Result<()> {
        let tls_progress = ProgressCounter::new();
        let _guard = tls_progress.guard();
        Ok(())
    }

    #[test]
    fn test_serializable_progress() -> Result<()> {
        let progress = Progress::new();

        // Add some test data
        progress.files_copied.inc();
        progress.bytes_copied.add(1024);
        progress.directories_created.add(2);

        // Test conversion to serializable format
        let serializable = SerializableProgress::from(&progress);
        assert_eq!(serializable.files_copied, 1);
        assert_eq!(serializable.bytes_copied, 1024);
        assert_eq!(serializable.directories_created, 2);

        // Test that we can create a TracingMessage with progress
        let _tracing_msg = TracingMessage::Progress(serializable);

        Ok(())
    }

    #[test]
    fn test_rcpd_progress_printer() -> Result<()> {
        let mut printer = RcpdProgressPrinter::new();

        // Create test progress data
        let source_progress = SerializableProgress {
            ops_started: 100,
            ops_finished: 80,
            bytes_copied: 1024,
            files_copied: 5,
            ..Default::default()
        };

        let dest_progress = SerializableProgress {
            ops_started: 80,
            ops_finished: 70,
            bytes_copied: 1024,
            files_copied: 8,
            symlinks_created: 2,
            directories_created: 1,
            ..Default::default()
        };

        // Test that print returns a formatted string
        let output = printer.print(&source_progress, &dest_progress)?;
        assert!(output.contains("SOURCE"));
        assert!(output.contains("DESTINATION"));
        assert!(output.contains("OPS:"));
        assert!(output.contains("pending:"));
        assert!(output.contains("20")); // source pending ops (100-80)
        assert!(output.contains("10")); // dest pending ops (80-70)
        let mut sections = output.split("==== DESTINATION ====");
        let source_section = sections.next().unwrap();
        let dest_section = sections.next().unwrap_or("");
        let source_files_line = source_section
            .lines()
            .find(|line| line.trim_start().starts_with("files:"))
            .expect("source files line missing");
        assert!(source_files_line.trim_start().ends_with("5"));
        assert!(!source_files_line.contains('.'));
        let dest_files_line = dest_section
            .lines()
            .find(|line| line.trim_start().starts_with("files:"))
            .expect("dest files line missing");
        assert!(dest_files_line.trim_start().ends_with("8"));
        assert!(!dest_files_line.contains('.'));

        Ok(())
    }

    #[test]
    fn interleaved_counter_access() -> Result<()> {
        // test that interleaved access to multiple counters works correctly
        // (this was problematic with the old single-slot cache design)
        let counter_a = TlsCounter::new();
        let counter_b = TlsCounter::new();
        let counter_c = TlsCounter::new();
        for i in 0..100 {
            counter_a.add(1);
            counter_b.add(2);
            counter_c.add(3);
            // verify intermediate values are correct
            if i % 10 == 0 {
                assert_eq!(counter_a.get(), i + 1);
                assert_eq!(counter_b.get(), (i + 1) * 2);
                assert_eq!(counter_c.get(), (i + 1) * 3);
            }
        }
        // verify final counts
        assert_eq!(counter_a.get(), 100);
        assert_eq!(counter_b.get(), 200);
        assert_eq!(counter_c.get(), 300);
        Ok(())
    }

    #[test]
    fn concurrent_multi_counter_access() -> Result<()> {
        // test concurrent access with multiple threads each using multiple counters
        let counter_a = std::sync::Arc::new(TlsCounter::new());
        let counter_b = std::sync::Arc::new(TlsCounter::new());
        const THREADS: usize = 4;
        const ITERATIONS: u64 = 1000;
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let ca = counter_a.clone();
                let cb = counter_b.clone();
                std::thread::spawn(move || {
                    for _ in 0..ITERATIONS {
                        ca.add(1);
                        cb.add(2);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // verify totals are correct (no lost increments)
        assert_eq!(counter_a.get(), THREADS as u64 * ITERATIONS);
        assert_eq!(counter_b.get(), THREADS as u64 * ITERATIONS * 2);
        Ok(())
    }

    #[test]
    fn repeated_counter_access() -> Result<()> {
        // test that repeated access to the same counter works correctly
        let counter = TlsCounter::new();
        for i in 1..=1000 {
            counter.add(1);
            assert_eq!(counter.get(), i);
        }
        Ok(())
    }

    #[test]
    fn sharding_distributes_across_threads() -> Result<()> {
        // test that different threads get assigned to different shards
        // and that all increments are correctly counted
        let counter = std::sync::Arc::new(TlsCounter::new());
        const THREADS: usize = 16;
        const ITERATIONS: u64 = 100;
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = counter.clone();
                std::thread::spawn(move || {
                    for _ in 0..ITERATIONS {
                        c.inc();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.get(), THREADS as u64 * ITERATIONS);
        Ok(())
    }

    #[test]
    fn sharding_handles_more_threads_than_shards() -> Result<()> {
        // test that shard assignment wraps correctly when threads > NUM_SHARDS
        let counter = std::sync::Arc::new(TlsCounter::new());
        const THREADS: usize = 128; // 2x NUM_SHARDS to force wrap-around
        const ITERATIONS: u64 = 100;
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = counter.clone();
                std::thread::spawn(move || {
                    for _ in 0..ITERATIONS {
                        c.inc();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.get(), THREADS as u64 * ITERATIONS);
        Ok(())
    }

    #[test]
    fn counter_independence() -> Result<()> {
        // test that multiple counters are completely independent
        let counters: Vec<_> = (0..10).map(|_| TlsCounter::new()).collect();
        for (i, counter) in counters.iter().enumerate() {
            counter.add((i + 1) as u64 * 100);
        }
        for (i, counter) in counters.iter().enumerate() {
            assert_eq!(counter.get(), (i + 1) as u64 * 100);
        }
        Ok(())
    }
}
