//! Tracks directory completion state during remote copy operations.
//!
//! # Overview
//!
//! The `DirectoryTracker` manages the lifecycle of directory copy operations in the
//! destination process. It tracks:
//! - Pending directories waiting for files
//! - Failed directories whose descendants should be skipped
//! - Stored metadata to apply when directories complete
//! - Overall completion state for sending `DestinationDone`
//!
//! # Protocol Flow
//!
//! 1. Source sends `Directory` messages during traversal (with metadata)
//! 2. Destination creates directories, stores metadata, sends `DirectoryCreated`
//! 3. Source sends files with `dir_total_files` count
//! 4. When all files received, destination applies stored metadata
//! 5. When all directories complete and structure is done, send `DestinationDone`
//!
//! # Failed Directory Handling
//!
//! When a directory fails to be created:
//! - It's added to `failed_directories`
//! - No `DirectoryCreated` is sent (source won't send files)
//! - Descendant directories/symlinks are skipped via `has_failed_ancestor()`
//! - The directory is immediately considered complete (no files expected)
//!
//! # File Count Tracking
//!
//! The file count is deferred - we don't know how many files to expect until
//! the first `File`, `FileSkipped`, or `DirectoryEmpty` message arrives for
//! that directory. This handles cases where directory contents change during copy.

use anyhow::Context;

/// State for a single directory waiting for files.
#[derive(Debug)]
struct DirectoryState {
    /// Total files expected (None until first file-related message)
    files_expected: Option<usize>,
    /// Files still remaining
    files_remaining: usize,
}

/// Tracks directory entry counts and completion state for remote copy operations.
pub struct DirectoryTracker {
    /// Directories waiting for files (files_expected unknown or files_remaining > 0)
    pending_directories: std::collections::HashMap<std::path::PathBuf, DirectoryState>,
    /// Directories that failed to create - their descendants are skipped
    failed_directories: std::collections::HashSet<std::path::PathBuf>,
    /// Directories that we created (vs reused existing) - used for empty dir cleanup
    created_directories: std::collections::HashSet<std::path::PathBuf>,
    /// Stored metadata for each directory (applied when complete)
    metadata: std::collections::HashMap<std::path::PathBuf, remote::protocol::Metadata>,
    /// Have we received DirStructureComplete?
    structure_complete: bool,
    /// Is the root item complete?
    root_complete: bool,
    /// Path of the root directory (if root is a directory)
    root_directory: Option<std::path::PathBuf>,
    /// Have we already sent DestinationDone?
    done_sent: bool,
    /// Control stream for sending DirectoryCreated
    control_send_stream: remote::streams::BoxedSharedSendStream,
    /// Preserve settings for applying metadata
    preserve: common::preserve::Settings,
}

impl DirectoryTracker {
    pub fn new(
        control_send_stream: remote::streams::BoxedSharedSendStream,
        preserve: common::preserve::Settings,
    ) -> Self {
        Self {
            pending_directories: std::collections::HashMap::new(),
            failed_directories: std::collections::HashSet::new(),
            created_directories: std::collections::HashSet::new(),
            metadata: std::collections::HashMap::new(),
            structure_complete: false,
            root_complete: false,
            root_directory: None,
            done_sent: false,
            control_send_stream,
            preserve,
        }
    }
    /// Check if any ancestor of the given path is a failed directory.
    pub fn has_failed_ancestor(&self, path: &std::path::Path) -> bool {
        let mut current = path;
        while let Some(parent) = current.parent() {
            if self.failed_directories.contains(parent) {
                return true;
            }
            current = parent;
        }
        false
    }
    /// Add a successfully created directory to tracking.
    /// Sends `DirectoryCreated` to source.
    ///
    /// # Arguments
    /// * `was_created` - true if we created this directory, false if it already existed
    pub async fn add_directory(
        &mut self,
        src: &std::path::Path,
        dst: &std::path::Path,
        metadata: remote::protocol::Metadata,
        is_root: bool,
        was_created: bool,
    ) -> anyhow::Result<()> {
        // store metadata for later application
        self.metadata.insert(dst.to_path_buf(), metadata);
        // track root directory path
        if is_root {
            self.root_directory = Some(dst.to_path_buf());
        }
        // track whether we created this directory (vs reusing existing)
        if was_created {
            self.created_directories.insert(dst.to_path_buf());
        }
        // add ALL directories to pending (we don't know file count yet)
        // root directories are also tracked here - when they complete, we set root_complete
        self.pending_directories.insert(
            dst.to_path_buf(),
            DirectoryState {
                files_expected: None,
                files_remaining: 0,
            },
        );
        // send DirectoryCreated to trigger file sending
        let confirmation = remote::protocol::SrcDst {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let message = remote::protocol::DestinationMessage::DirectoryCreated(confirmation);
        {
            let mut stream = self.control_send_stream.lock().await;
            stream.send_control_message(&message).await?;
        }
        tracing::info!("Sent DirectoryCreated: {:?} -> {:?}", src, dst);
        Ok(())
    }
    /// Mark a directory as failed (creation error).
    /// Does NOT send DirectoryCreated - source won't send files.
    pub fn mark_directory_failed(&mut self, dst: &std::path::Path) {
        self.failed_directories.insert(dst.to_path_buf());
        tracing::info!("Directory marked as failed: {:?}", dst);
    }
    /// Update file count for a directory and decrement remaining.
    /// Called when receiving File or FileSkipped message.
    /// Returns true if directory is now complete.
    pub async fn process_file(
        &mut self,
        dst_dir: &std::path::Path,
        dir_total_files: usize,
    ) -> anyhow::Result<bool> {
        let state = self
            .pending_directories
            .get_mut(dst_dir)
            .ok_or_else(|| anyhow::anyhow!("directory {:?} not being tracked", dst_dir))?;
        // set expected count on first file
        if state.files_expected.is_none() {
            state.files_expected = Some(dir_total_files);
            state.files_remaining = dir_total_files;
            tracing::debug!(
                "Directory {:?} expecting {} files",
                dst_dir,
                dir_total_files
            );
        }
        // sanity check
        if state.files_expected != Some(dir_total_files) {
            tracing::warn!(
                "Directory {:?} file count mismatch: expected {:?}, got {}",
                dst_dir,
                state.files_expected,
                dir_total_files
            );
        }
        // decrement
        if state.files_remaining == 0 {
            anyhow::bail!(
                "directory {:?} already complete, received extra file",
                dst_dir
            );
        }
        state.files_remaining -= 1;
        tracing::debug!(
            "Directory {:?} files remaining: {}",
            dst_dir,
            state.files_remaining
        );
        // check completion
        if state.files_remaining == 0 {
            // directory has files, so it's not empty - keep_if_empty is irrelevant
            self.complete_directory(dst_dir, false).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Mark a directory as empty (no files).
    /// Called when receiving DirectoryEmpty message.
    /// `keep_if_empty` indicates whether to keep the directory even though it has no files
    /// (e.g., because it directly matches an include pattern or contains symlinks).
    pub async fn mark_directory_empty(
        &mut self,
        dst: &std::path::Path,
        keep_if_empty: bool,
    ) -> anyhow::Result<()> {
        let state = self
            .pending_directories
            .get_mut(dst)
            .ok_or_else(|| anyhow::anyhow!("directory {:?} not being tracked", dst))?;
        state.files_expected = Some(0);
        state.files_remaining = 0;
        tracing::debug!(
            "Directory {:?} is empty (keep_if_empty={})",
            dst,
            keep_if_empty
        );
        self.complete_directory(dst, keep_if_empty).await
    }
    /// Complete a directory: apply metadata and remove from pending.
    ///
    /// Note: `keep_if_empty` is accepted but currently unused. Empty directory cleanup
    /// is intentionally not performed in the remote path because directory completion
    /// is file-count based and does not wait for child directories to finish. A parent
    /// with no direct files completes as soon as it receives `DirectoryEmpty`, even if
    /// descendants are still in progress. Removing the parent at that point could race
    /// with the source sending `Directory(child)` for a descendant, causing the child
    /// creation to fail with `NotFound`. Empty directory cleanup for remote copy will
    /// be implemented once directory completion is deferred until all descendants finish.
    /// See `docs/remote_protocol.md` for details.
    async fn complete_directory(
        &mut self,
        dst: &std::path::Path,
        _keep_if_empty: bool,
    ) -> anyhow::Result<()> {
        // check if this is the root directory
        let is_root = self.root_directory.as_deref() == Some(dst);
        // remove from pending
        let state = self.pending_directories.remove(dst);
        if state.is_none() {
            tracing::warn!("directory {:?} was not in pending when completing", dst);
        }
        // check if we created this directory (vs reused existing)
        let was_created = self.created_directories.remove(dst);
        // increment counter now (if we created it)
        if was_created {
            common::get_progress().directories_created.inc();
        }
        // apply stored metadata
        if let Some(metadata) = self.metadata.remove(dst) {
            common::preserve::set_dir_metadata(&self.preserve, &metadata, dst)
                .await
                .with_context(|| format!("failed to set metadata on directory {:?}", dst))?;
            tracing::info!("Directory complete, metadata applied: {:?}", dst);
        } else {
            tracing::warn!("No stored metadata for directory {:?}", dst);
        }
        // if this was the root directory, mark root as complete
        if is_root {
            self.set_root_complete();
        }
        Ok(())
    }
    /// Mark the root item as complete.
    pub fn set_root_complete(&mut self) {
        self.root_complete = true;
        tracing::info!("Root item complete");
    }
    /// Mark the directory structure as complete (DirStructureComplete received).
    ///
    /// If `has_root_item` is false (dry-run mode or filtered root), this also
    /// sets root_complete to allow graceful shutdown since no root messages will follow.
    pub fn set_structure_complete(&mut self, has_root_item: bool) {
        self.structure_complete = true;
        // if source indicates no root item will be sent, mark root as complete
        // this happens in dry-run mode or when the root item is filtered out
        if !has_root_item {
            tracing::info!("No root item to receive, marking root as complete");
            self.root_complete = true;
        }
        tracing::info!("Directory structure complete");
    }
    /// Check if we're done and can send DestinationDone.
    pub fn is_done(&self) -> bool {
        self.structure_complete && self.pending_directories.is_empty() && self.root_complete
    }
    /// Send DestinationDone and close the send stream.
    /// Returns true if DestinationDone was sent, false if already sent.
    pub async fn send_destination_done(&mut self) -> anyhow::Result<bool> {
        if self.done_sent {
            tracing::debug!("DestinationDone already sent, skipping");
            return Ok(false);
        }
        self.done_sent = true;
        let mut stream = self.control_send_stream.lock().await;
        stream
            .send_control_message(&remote::protocol::DestinationMessage::DestinationDone)
            .await?;
        stream.close().await?;
        tracing::info!("Sent DestinationDone, closed send stream");
        Ok(true)
    }
    /// Close the send stream without sending DestinationDone.
    /// Used for error cleanup to ensure TLS streams are properly shut down.
    pub async fn close_stream(&mut self) {
        let mut stream = self.control_send_stream.lock().await;
        if let Err(e) = stream.close().await {
            tracing::debug!("Error closing stream during cleanup: {:#}", e);
        }
        tracing::debug!("Control send stream closed for cleanup");
    }
}

pub type SharedDirectoryTracker = std::sync::Arc<tokio::sync::Mutex<DirectoryTracker>>;

pub fn make_shared(
    control_send_stream: remote::streams::BoxedSharedSendStream,
    preserve: common::preserve::Settings,
) -> SharedDirectoryTracker {
    std::sync::Arc::new(tokio::sync::Mutex::new(DirectoryTracker::new(
        control_send_stream,
        preserve,
    )))
}
