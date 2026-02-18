//! Tracks directory completion state during remote copy operations.
//!
//! # Overview
//!
//! The `DirectoryTracker` manages the lifecycle of directory copy operations in the
//! destination process. It tracks:
//! - Pending directories waiting for all child entries to be processed
//! - Failed directories whose descendants should be skipped
//! - Stored metadata to apply when directories complete
//! - Overall completion state for sending `DestinationDone`
//!
//! # Protocol Flow
//!
//! 1. Source pre-reads directory children and sends `Directory` with entry counts
//! 2. Destination creates directories, stores metadata, sends `DirectoryCreated`
//! 3. Source sends files; destination also processes child directories and symlinks
//! 4. When all entries processed, destination applies stored metadata
//! 5. When all directories complete and structure is done, send `DestinationDone`
//!
//! # Unified Entry Counting
//!
//! Every child entry (file, directory, or symlink) counts toward the parent's
//! `entries_expected`. This ensures a parent directory only completes after all
//! its children are done, preventing premature metadata application.
//!
//! # Failed Directory Handling
//!
//! When a directory fails to be created:
//! - It's added to `failed_directories`
//! - No `DirectoryCreated` is sent (source won't send files)
//! - Descendant directories/symlinks are skipped via `has_failed_ancestor()`
//! - Skipped entries still call `process_child_entry()` on the parent

use anyhow::Context;

/// State for a single directory waiting for child entries.
#[derive(Debug)]
struct DirectoryState {
    /// total child entries expected (files + directories + symlinks)
    entries_expected: usize,
    /// child entries processed so far
    entries_processed: usize,
    /// whether to keep this directory if it ends up empty
    keep_if_empty: bool,
}

/// Tracks directory entry counts and completion state for remote copy operations.
pub struct DirectoryTracker {
    /// directories waiting for entries (entries_processed < entries_expected)
    pending_directories: std::collections::HashMap<std::path::PathBuf, DirectoryState>,
    /// directories that failed to create - their descendants are skipped
    failed_directories: std::collections::HashSet<std::path::PathBuf>,
    /// directories that we created (vs reused existing) - used for empty dir cleanup
    created_directories: std::collections::HashSet<std::path::PathBuf>,
    /// stored metadata for each directory (applied when complete)
    metadata: std::collections::HashMap<std::path::PathBuf, remote::protocol::Metadata>,
    /// have we received DirStructureComplete?
    structure_complete: bool,
    /// is the root item complete?
    root_complete: bool,
    /// path of the root directory (if root is a directory)
    root_directory: Option<std::path::PathBuf>,
    /// have we already sent DestinationDone?
    done_sent: bool,
    /// control stream for sending DirectoryCreated
    control_send_stream: remote::streams::BoxedSharedSendStream,
    /// preserve settings for applying metadata
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
    /// Sends `DirectoryCreated` to source with the `file_count`.
    /// If `entry_count` is 0, the directory completes immediately.
    ///
    /// # Arguments
    /// * `was_created` - true if we created this directory, false if it already existed
    /// * `entry_count` - total child entries (files + dirs + symlinks)
    /// * `file_count` - number of child files, echoed back to source
    /// * `keep_if_empty` - whether to keep this directory if empty
    #[allow(clippy::too_many_arguments)]
    pub async fn add_directory(
        &mut self,
        src: &std::path::Path,
        dst: &std::path::Path,
        metadata: remote::protocol::Metadata,
        is_root: bool,
        was_created: bool,
        entry_count: usize,
        file_count: usize,
        keep_if_empty: bool,
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
        // add to pending with known entry count
        self.pending_directories.insert(
            dst.to_path_buf(),
            DirectoryState {
                entries_expected: entry_count,
                entries_processed: 0,
                keep_if_empty,
            },
        );
        // send DirectoryCreated with file_count to trigger file sending
        let message = remote::protocol::DestinationMessage::DirectoryCreated {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            file_count,
        };
        {
            let mut stream = self.control_send_stream.lock().await;
            stream.send_control_message(&message).await?;
        }
        tracing::info!(
            "Sent DirectoryCreated: {:?} -> {:?} (entries={}, files={})",
            src,
            dst,
            entry_count,
            file_count
        );
        // if entry_count is 0, directory is immediately complete
        if entry_count == 0 {
            self.complete_directory(dst).await?;
        }
        Ok(())
    }
    /// Mark a directory as failed (creation error).
    /// Does NOT send DirectoryCreated - source won't send files.
    pub fn mark_directory_failed(&mut self, dst: &std::path::Path) {
        self.failed_directories.insert(dst.to_path_buf());
        tracing::info!("Directory marked as failed: {:?}", dst);
    }
    /// Process a file entry for a directory (File or FileSkipped).
    /// Increments entries_processed and checks completion.
    /// Returns true if directory is now complete.
    pub async fn process_file(&mut self, dst_dir: &std::path::Path) -> anyhow::Result<bool> {
        let state = self
            .pending_directories
            .get_mut(dst_dir)
            .ok_or_else(|| anyhow::anyhow!("directory {:?} not being tracked", dst_dir))?;
        state.entries_processed += 1;
        tracing::debug!(
            "Directory {:?} entries processed: {}/{}",
            dst_dir,
            state.entries_processed,
            state.entries_expected
        );
        // check completion
        if state.entries_processed >= state.entries_expected {
            self.complete_directory(dst_dir).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Process a non-file child entry (directory or symlink) for the parent.
    /// Increments the parent's entries_processed and checks completion.
    /// No-op if parent is not in `pending_directories` (e.g., failed parent).
    pub async fn process_child_entry(
        &mut self,
        parent_dst: &std::path::Path,
    ) -> anyhow::Result<()> {
        // no-op if parent is not tracked (e.g., failed directory or root item)
        let Some(state) = self.pending_directories.get_mut(parent_dst) else {
            return Ok(());
        };
        state.entries_processed += 1;
        tracing::debug!(
            "Directory {:?} entries processed: {}/{} (child entry)",
            parent_dst,
            state.entries_processed,
            state.entries_expected
        );
        // check completion
        if state.entries_processed >= state.entries_expected {
            self.complete_directory(parent_dst).await?;
        }
        Ok(())
    }
    /// Complete a directory: apply metadata and remove from pending.
    /// Uses `keep_if_empty` from the directory state to decide whether to remove
    /// empty directories that were only created for traversal purposes.
    async fn complete_directory(&mut self, dst: &std::path::Path) -> anyhow::Result<()> {
        // check if this is the root directory
        let is_root = self.root_directory.as_deref() == Some(dst);
        // remove from pending
        let state = self.pending_directories.remove(dst);
        let keep_if_empty = state.as_ref().is_none_or(|s| s.keep_if_empty);
        if state.is_none() {
            tracing::warn!("directory {:?} was not in pending when completing", dst);
        }
        // check if we created this directory (vs reused existing)
        let was_created = self.created_directories.remove(dst);
        // handle empty directory cleanup for directories we created
        if was_created && !keep_if_empty {
            // try to remove if empty (best effort - may fail if not empty due to races)
            match tokio::fs::remove_dir(dst).await {
                Ok(()) => {
                    tracing::info!("Removed empty directory: {:?}", dst);
                    // don't apply metadata or increment counter for removed directories
                    self.metadata.remove(dst);
                    if is_root {
                        self.set_root_complete();
                    }
                    return Ok(());
                }
                Err(e) => {
                    // not empty or other error - keep it and proceed normally
                    tracing::debug!(
                        "Could not remove empty directory {:?} (keeping): {:#}",
                        dst,
                        e
                    );
                }
            }
        }
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
