use anyhow::{Context, anyhow};
use async_recursion::async_recursion;
use tracing::instrument;

use crate::progress;

/// Error type for filegen operations. See [`crate::error::OperationError`] for
/// logging conventions and rationale.
pub type Error = crate::error::OperationError<Summary>;

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    pub files_created: usize,
    pub directories_created: usize,
    pub bytes_written: u64,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            files_created: self.files_created + other.files_created,
            directories_created: self.directories_created + other.directories_created,
            bytes_written: self.bytes_written + other.bytes_written,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "files created: {}\n\
            directories created: {}\n\
            bytes written: {}",
            self.files_created,
            self.directories_created,
            bytesize::ByteSize(self.bytes_written)
        )
    }
}

/// Configuration for file generation
#[derive(Debug, Clone)]
pub struct FileGenConfig {
    /// Root directory for file generation
    pub root: std::path::PathBuf,
    /// Directory width at each level
    pub dirwidth: Vec<usize>,
    /// Number of files to generate at each leaf
    pub numfiles: usize,
    /// Size of each file in bytes
    pub filesize: usize,
    /// Write buffer size in bytes
    pub writebuf: usize,
    /// Chunk size for I/O throttling
    pub chunk_size: u64,
    /// Whether to generate files at leaf directories only
    pub leaf_files: bool,
}

impl FileGenConfig {
    /// Create a new file generation configuration
    pub fn new(
        root: impl Into<std::path::PathBuf>,
        dirwidth: Vec<usize>,
        numfiles: usize,
        filesize: usize,
    ) -> Self {
        Self {
            root: root.into(),
            dirwidth,
            numfiles,
            filesize,
            writebuf: 1024 * 1024, // 1MB default
            chunk_size: 0,
            leaf_files: false,
        }
    }
}

fn allocate_write_buffer(path: &std::path::Path, bufsize: usize) -> Vec<u8> {
    #[cfg(test)]
    crate::testutils::record_blocking_path_allocation(path);
    #[cfg(not(test))]
    let _ = path;
    vec![0u8; bufsize]
}

#[instrument(skip(prog_track))]
pub async fn write_file(
    prog_track: &'static progress::Progress,
    path: std::path::PathBuf,
    mut filesize: usize,
    bufsize: usize,
    chunk_size: u64,
) -> Result<Summary, Error> {
    let open_file_guard = throttle::open_file_permit().await;
    let admission = open_file_guard.admission();
    crate::safedir::with_fd_admission(admission, async move {
        let _open_file_guard = open_file_guard;
        throttle::get_file_iops_tokens(chunk_size, filesize as u64).await;
        let _ops_guard = prog_track.ops.guard();
        let original_filesize = filesize;
        // The file open is the single metadata syscall in this path; wrap it
        // with the cwnd permit + probe so filegen participates in the same
        // adaptive control loop as copy/rm/link. Use the `_no_rate` variant
        // because filegen gates the ops-throttle at task-spawn time (see
        // `filegen` below) — going through the rate-gating helper here
        // would consume two tokens per file and halve the effective rate.
        let open_path = path.clone();
        let file = crate::safedir::run_metadata_probed_blocking_no_rate(
            congestion::Side::Destination,
            congestion::MetadataOp::OpenCreate,
            move || {
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(open_path)
            },
        )
        .await
        .with_context(|| format!("Error opening {:?}", &path))
        .map_err(|err| Error::new(err, Default::default()))?;
        let mut file = file;
        let mut bytes = None;
        while filesize > 0 {
            let writesize = std::cmp::min(filesize, bufsize);
            let write_path = path.clone();
            let chunk_output = crate::safedir::run_fd_admitted_blocking(move || {
                use std::io::Write as _;

                #[cfg(test)]
                let gate_visit = {
                    use std::os::fd::AsRawFd as _;
                    crate::testutils::wait_on_blocking_path_gate(&write_path, file.as_raw_fd())
                };
                // rebind the captured owner after the visit so unwinding drops the fd before the
                // completion witness. the tuple below preserves that order when a completed output
                // is abandoned.
                let mut file = file;
                let mut bytes =
                    bytes.unwrap_or_else(|| allocate_write_buffer(&write_path, bufsize));
                // rng state is thread-local and never crosses a blocking boundary.
                rand::fill(&mut bytes[..]);
                let result = file
                    .write_all(&bytes[..writesize])
                    .with_context(|| format!("Error writing to {:?}", &write_path));
                #[cfg(test)]
                {
                    Ok((result, file, gate_visit, bytes))
                }
                #[cfg(not(test))]
                {
                    Ok((result, file, bytes))
                }
            })
            .await
            .with_context(|| format!("Error running file writer for {:?}", &path))
            .map_err(|err| Error::new(err, Default::default()))?;
            #[cfg(test)]
            let (write_result, returned_file, _gate_visit, returned_bytes) = chunk_output;
            #[cfg(not(test))]
            let (write_result, returned_file, returned_bytes) = chunk_output;
            file = returned_file;
            bytes = Some(returned_bytes);
            write_result.map_err(|err| Error::new(err, Default::default()))?;
            filesize -= writesize;
            prog_track.bytes_copied.add(writesize as u64);
        }
        let flush_path = path.clone();
        let (flush_result, file) = crate::safedir::run_fd_admitted_blocking(move || {
            use std::io::Write as _;

            let mut file = file;
            let result = file
                .flush()
                .with_context(|| format!("Error flushing {:?}", &flush_path));
            Ok((result, file))
        })
        .await
        .with_context(|| format!("Error running file writer for {:?}", &path))
        .map_err(|err| Error::new(err, Default::default()))?;
        flush_result.map_err(|err| Error::new(err, Default::default()))?;
        drop(file);
        prog_track.files_copied.inc();
        Ok(Summary {
            files_created: 1,
            bytes_written: original_filesize as u64,
            ..Default::default()
        })
    })
    .await
}

#[async_recursion]
#[instrument(skip(prog_track))]
pub async fn filegen(
    prog_track: &'static progress::Progress,
    config: &FileGenConfig,
) -> Result<Summary, Error> {
    let FileGenConfig {
        root,
        dirwidth,
        numfiles,
        filesize,
        writebuf,
        chunk_size,
        leaf_files,
    } = config;
    let numdirs = *dirwidth.first().unwrap_or(&0);
    let mut join_set = tokio::task::JoinSet::new();
    // generate directories and recurse into them
    for i in 0..numdirs {
        let path = root.join(format!("dir{i}"));
        let next_dirwidth = dirwidth[1..].to_vec();
        let recurse_config = FileGenConfig {
            root: path.clone(),
            dirwidth: next_dirwidth,
            numfiles: *numfiles,
            filesize: *filesize,
            writebuf: *writebuf,
            chunk_size: *chunk_size,
            leaf_files: *leaf_files,
        };
        let recurse = || async move {
            // Bracket the create_dir metadata syscall with the cwnd permit
            // + probe so filegen participates in the same adaptive control
            // loop as copy/rm/link.
            crate::walk::run_metadata_probed(
                congestion::Side::Destination,
                congestion::MetadataOp::MkDir,
                tokio::fs::create_dir(&path),
            )
            .await
            .with_context(|| format!("Error creating directory {:?}", &path))
            .map_err(|err| Error::new(err, Default::default()))?;
            prog_track.directories_created.inc();
            let dir_summary = Summary {
                directories_created: 1,
                ..Default::default()
            };
            let recurse_summary = filegen(prog_track, &recurse_config).await?;
            Ok(dir_summary + recurse_summary)
        };
        join_set.spawn(recurse());
    }
    // generate files (only if we're not in leaf_files mode, or if we are a leaf directory)
    // a directory is a leaf when dirwidth is empty (no more subdirectories to create)
    let is_leaf = dirwidth.is_empty();
    let should_generate_files = !leaf_files || is_leaf;
    if should_generate_files {
        for i in 0..*numfiles {
            // await the replenished rate token before spawn so an enabled ops throttle paces task
            // creation at the configured operation rate
            throttle::get_ops_token().await;
            let path = root.join(format!("file{i}"));
            join_set.spawn(write_file(
                prog_track,
                path,
                *filesize,
                *writebuf,
                *chunk_size,
            ));
        }
    }
    let mut success = true;
    let mut last_error: Option<anyhow::Error> = None;
    let mut filegen_summary = Summary::default();
    while let Some(res) = join_set.join_next().await {
        match res.map_err(|err| Error::new(err.into(), Default::default()))? {
            Ok(summary) => filegen_summary = filegen_summary + summary,
            Err(error) => {
                tracing::error!("filegen: {:?} failed with: {:#}", root, &error);
                filegen_summary = filegen_summary + error.summary;
                if last_error.is_none() {
                    last_error = Some(error.source);
                }
                success = false;
            }
        }
    }
    if !success {
        let error = if let Some(error) = last_error {
            error.context(format!("filegen: {:?} failed!", &root))
        } else {
            anyhow!("filegen: {:?} failed!", &root)
        };
        return Err(Error::new(error, filegen_summary));
    }
    Ok(filegen_summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils;
    use std::os::unix::fs::PermissionsExt;
    use tracing_test::traced_test;

    static PROGRESS: std::sync::LazyLock<progress::Progress> =
        std::sync::LazyLock::new(progress::Progress::new);

    #[tokio::test]
    #[traced_test]
    async fn test_basic_filegen() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate 2 subdirectories with 3 files per directory (including root)
        let config = FileGenConfig {
            root: test_path.to_path_buf(),
            dirwidth: vec![2],
            numfiles: 3,
            filesize: 100,
            writebuf: 50,
            chunk_size: 0,
            leaf_files: false,
        };
        let summary = filegen(&PROGRESS, &config).await?;
        // verify summary
        // files: 3 (in root) + 3 (in dir0) + 3 (in dir1) = 9 files
        // directories: 2 (dir0, dir1)
        // bytes: 100 bytes × 9 files = 900 bytes
        assert_eq!(summary.files_created, 9);
        assert_eq!(summary.directories_created, 2);
        assert_eq!(summary.bytes_written, 900);
        // verify files were actually created
        assert!(test_path.join("file0").exists()); // root level files
        assert!(test_path.join("dir0").join("file0").exists());
        assert!(test_path.join("dir0").join("file1").exists());
        assert!(test_path.join("dir0").join("file2").exists());
        assert!(test_path.join("dir1").join("file0").exists());
        assert!(test_path.join("dir1").join("file1").exists());
        assert!(test_path.join("dir1").join("file2").exists());
        // verify file sizes
        let metadata = tokio::fs::metadata(test_path.join("dir0").join("file0")).await?;
        assert_eq!(metadata.len(), 100);
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_nested_filegen() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate nested structure: 2 top-level dirs, each with 3 subdirs, 4 files per dir, 50 bytes each
        let config = FileGenConfig {
            root: test_path.to_path_buf(),
            dirwidth: vec![2, 3],
            numfiles: 4,
            filesize: 50,
            writebuf: 25,
            chunk_size: 0,
            leaf_files: false,
        };
        let summary = filegen(&PROGRESS, &config).await?;
        // calculate expected values:
        // directories: 2 top-level + (2 × 3) subdirs = 8 total
        // files: 4 (in root) + 4×2 (in dir0, dir1) + 4×2×3 (in all leaf dirs) = 4 + 8 + 24 = 36 files
        // bytes: 50 bytes × 36 files = 1800 bytes
        assert_eq!(summary.files_created, 36);
        assert_eq!(summary.directories_created, 8);
        assert_eq!(summary.bytes_written, 1800);
        // spot check some files exist
        assert!(test_path.join("file0").exists()); // root files
        assert!(test_path.join("dir0").join("file0").exists()); // top-level dir files
        assert!(test_path.join("dir0").join("dir0").join("file0").exists());
        assert!(test_path.join("dir0").join("dir2").join("file3").exists());
        assert!(test_path.join("dir1").join("dir1").join("file2").exists());
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_deeply_nested_filegen() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate 3 levels: 2,2,2 with 2 files each, 10 bytes per file
        let config = FileGenConfig {
            root: test_path.to_path_buf(),
            dirwidth: vec![2, 2, 2],
            numfiles: 2,
            filesize: 10,
            writebuf: 10,
            chunk_size: 0,
            leaf_files: false,
        };
        let summary = filegen(&PROGRESS, &config).await?;
        // directories: 2 + (2×2) + (2×2×2) = 2 + 4 + 8 = 14 dirs
        // files: 2 (root) + 2×2 (level 1) + 2×2×2 (level 2) + 2×2×2×2 (level 3) = 2 + 4 + 8 + 16 = 30 files
        // bytes: 10 bytes × 30 files = 300 bytes
        assert_eq!(summary.files_created, 30);
        assert_eq!(summary.directories_created, 14);
        assert_eq!(summary.bytes_written, 300);
        // verify deep nesting works
        assert!(test_path.join("file0").exists()); // root files
        assert!(
            test_path
                .join("dir0")
                .join("dir0")
                .join("dir0")
                .join("file0")
                .exists()
        );
        assert!(
            test_path
                .join("dir1")
                .join("dir1")
                .join("dir1")
                .join("file1")
                .exists()
        );
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_single_file() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate just files, no directories
        let config = FileGenConfig {
            root: test_path.to_path_buf(),
            dirwidth: vec![],
            numfiles: 5,
            filesize: 200,
            writebuf: 100,
            chunk_size: 0,
            leaf_files: false,
        };
        let summary = filegen(&PROGRESS, &config).await?;
        assert_eq!(summary.files_created, 5);
        assert_eq!(summary.directories_created, 0);
        assert_eq!(summary.bytes_written, 1000); // 200 × 5
        for i in 0..5 {
            // verify files
            let file_path = test_path.join(format!("file{i}"));
            assert!(file_path.exists());
            let metadata = tokio::fs::metadata(&file_path).await?;
            assert_eq!(metadata.len(), 200);
        }
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_zero_files() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate only directories, no files
        let config = FileGenConfig {
            root: test_path.to_path_buf(),
            dirwidth: vec![3, 2],
            numfiles: 0,
            filesize: 100,
            writebuf: 50,
            chunk_size: 0,
            leaf_files: false,
        };
        let summary = filegen(&PROGRESS, &config).await?;
        // directories: 3 + (3×2) = 9 dirs
        assert_eq!(summary.files_created, 0);
        assert_eq!(summary.directories_created, 9);
        assert_eq!(summary.bytes_written, 0);
        // verify directories exist but no files
        assert!(test_path.join("dir0").join("dir0").exists());
        assert!(test_path.join("dir2").join("dir1").exists());
        assert!(!test_path.join("dir0").join("file0").exists());
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_leaf_files_only() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // generate with leaf_files=true, meaning files only in deepest directories
        let config = FileGenConfig {
            root: test_path.to_path_buf(),
            dirwidth: vec![2, 3],
            numfiles: 4,
            filesize: 50,
            writebuf: 25,
            chunk_size: 0,
            leaf_files: true,
        };
        let summary = filegen(&PROGRESS, &config).await?;
        // directories: 2 top-level + (2 × 3) subdirs = 8 total
        // files: ONLY in leaf dirs (6 leaf dirs) × 4 files each = 24 files
        // bytes: 50 bytes × 24 files = 1200 bytes
        assert_eq!(summary.files_created, 24);
        assert_eq!(summary.directories_created, 8);
        assert_eq!(summary.bytes_written, 1200);
        // verify NO files in root or intermediate directories
        assert!(!test_path.join("file0").exists()); // no root files
        assert!(!test_path.join("dir0").join("file0").exists()); // no intermediate files
        assert!(!test_path.join("dir1").join("file0").exists());
        // verify files ONLY in leaf directories
        assert!(test_path.join("dir0").join("dir0").join("file0").exists());
        assert!(test_path.join("dir0").join("dir0").join("file3").exists());
        assert!(test_path.join("dir0").join("dir2").join("file0").exists());
        assert!(test_path.join("dir1").join("dir1").join("file0").exists());
        // cleanup
        tokio::fs::remove_dir_all(test_path).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_permission_error_includes_root_cause() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let root = tmp_dir.join("readonly");
        tokio::fs::create_dir(&root).await?;
        tokio::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o555)).await?;

        let config = FileGenConfig {
            root: root.clone(),
            dirwidth: Vec::new(),
            numfiles: 1,
            filesize: 10,
            writebuf: 10,
            chunk_size: 0,
            leaf_files: false,
        };
        let result = filegen(&PROGRESS, &config).await;

        // restore permissions to allow cleanup
        tokio::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).await?;

        assert!(
            result.is_err(),
            "filegen inside read-only directory should fail"
        );
        let err = result.unwrap_err();
        let err_msg = format!("{:#}", err.source);
        assert!(
            err_msg.to_lowercase().contains("permission denied") || err_msg.contains("EACCES"),
            "Error message must include permission denied text. Got: {}",
            err_msg
        );
        Ok(())
    }

    mod max_open_files_tests {
        use super::*;

        static CANCELLATION_PROGRESS: std::sync::LazyLock<progress::Progress> =
            std::sync::LazyLock::new(progress::Progress::new);
        static REUSE_PROGRESS: std::sync::LazyLock<progress::Progress> =
            std::sync::LazyLock::new(progress::Progress::new);

        #[tokio::test(flavor = "current_thread")]
        async fn write_file_reuses_one_blocking_buffer_across_chunks() -> anyhow::Result<()> {
            let root = testutils::create_temp_dir().await?;
            let path = root.join("reused-buffer");
            let bufsize = 4096;
            let filesize = bufsize * 3;
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let gate = testutils::BlockingPathGate::install(path.clone());
            gate.release_all();
            let timeout = std::time::Duration::from_secs(20);
            let runtime_thread = std::thread::current().id();

            let write_result = match admission
                .run_with_timeout(
                    timeout,
                    write_file(&REUSE_PROGRESS, path.clone(), filesize, bufsize, 0),
                )
                .await
            {
                Ok(Ok(summary)) => Ok(summary),
                Ok(Err(error)) => Err(error.source),
                Err(error) => Err(anyhow::Error::new(error)
                    .context("multi-chunk write_file did not finish in time")),
            };
            let hit_count = gate.hit_count();
            let allocation_count = gate.allocation_count();
            let allocation_thread = gate.allocation_thread();
            let final_len = tokio::fs::metadata(&path).await.map(|meta| meta.len());
            drop(gate);
            drop(admission);
            let cleanup_result = tokio::fs::remove_dir_all(root).await;

            let summary = write_result?;
            let final_len = final_len?;
            cleanup_result?;
            assert_eq!(hit_count, 3, "write_file did not submit one job per chunk");
            assert_eq!(
                allocation_count, 1,
                "write_file allocated a new buffer instead of reusing the returned one"
            );
            assert_ne!(
                allocation_thread,
                Some(runtime_thread),
                "filegen allocated its user-sized buffer on the async runtime thread"
            );
            assert_eq!(summary.files_created, 1);
            assert_eq!(summary.bytes_written, filesize as u64);
            assert_eq!(final_len, filesize as u64);
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn cancelled_write_file_stops_after_one_buffer_and_retains_capacity()
        -> anyhow::Result<()> {
            let root = testutils::create_temp_dir().await?;
            let path = root.join("cancelled-file");
            let bufsize = 4096;
            let filesize = bufsize * 3;
            let gate = testutils::BlockingPathGate::install(path.clone());
            let admission = testutils::AdmissionLimit::new().await;
            admission.set_max_open_files(1);
            let timeout = std::time::Duration::from_secs(20);
            let runtime_thread = std::thread::current().id();
            let bytes_before = CANCELLATION_PROGRESS.bytes_copied.get();
            let files_before = CANCELLATION_PROGRESS.files_copied.get();
            let task = tokio::spawn(write_file(
                &CANCELLATION_PROGRESS,
                path.clone(),
                filesize,
                bufsize,
                0,
            ));
            let observations =
                testutils::cancel_at_blocking_path(admission, gate, task, timeout, |_| {
                    (
                        CANCELLATION_PROGRESS.bytes_copied.get(),
                        CANCELLATION_PROGRESS.files_copied.get(),
                    )
                })
                .await;
            let final_len = tokio::fs::metadata(&path).await.map(|meta| meta.len());
            let final_bytes = CANCELLATION_PROGRESS.bytes_copied.get();
            let final_files = CANCELLATION_PROGRESS.files_copied.get();
            let cleanup_result = tokio::fs::remove_dir_all(root).await;

            let (observations, (bytes_at_cancellation, files_at_cancellation)) = observations?;
            let final_len = final_len?;
            cleanup_result?;

            assert!(observations.waiter_was_cancelled);
            assert!(
                observations.admission_was_retained_while_work_gated,
                "cancelling write_file released capacity while its fd-owning job was live"
            );
            assert!(
                observations.fd_was_open_while_work_gated,
                "write_file closed its fd while the job was gated"
            );
            assert_eq!(observations.hit_count_before_release, 1);
            assert_eq!(
                observations.allocation_count_before_release, 0,
                "filegen allocated its user-sized buffer before the admitted work gate released"
            );
            assert_eq!(bytes_at_cancellation, bytes_before);
            assert_eq!(files_at_cancellation, files_before);
            assert!(
                observations.fd_was_closed_at_output_drop_start,
                "write_file reached its output-drop boundary before its destination fd closed"
            );
            assert!(
                observations.admission_was_retained_at_output_drop_start,
                "write_file released capacity before its abandoned output dropped"
            );
            assert_eq!(
                observations.final_hit_count, 1,
                "cancellation submitted another buffer"
            );
            assert_eq!(
                observations.final_allocation_count, 1,
                "filegen did not allocate exactly one reusable buffer in admitted blocking work"
            );
            assert_ne!(
                observations.allocation_thread,
                Some(runtime_thread),
                "filegen allocated its user-sized buffer on the async runtime thread"
            );
            assert_eq!(
                final_len, bufsize as u64,
                "cancellation must finish at most the already-submitted buffer"
            );
            assert_eq!(
                final_bytes, bytes_before,
                "an abandoned buffer must not advance byte progress"
            );
            assert_eq!(
                final_files, files_before,
                "a cancelled file must not be counted complete"
            );
            Ok(())
        }
    }
}
