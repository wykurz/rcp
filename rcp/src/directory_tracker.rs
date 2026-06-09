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
//! 5. Child directories notify their parent upon completion (not creation),
//!    propagating bottom-up so parents only complete after all children finish
//! 6. When all directories complete and structure is done, send `DestinationDone`
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
//! - A `DirectorySkipped` nack is sent instead of `DirectoryCreated`, so the source releases the
//!   directory's held fd and sends no files for it (exactly one of `DirectoryCreated`/
//!   `DirectorySkipped` is sent per `Directory`)
//! - Descendant directories/symlinks are skipped via `has_failed_ancestor()`
//! - Skipped entries still call `process_child_entry()` on the parent
//!
//! # Directory fd-map (TOCTOU-safe destination writes)
//!
//! Every successfully created/reused directory also has its open [`Dir`] handle
//! (an `O_NOFOLLOW|O_DIRECTORY` fd) stored in the tracker, keyed by its destination
//! path. Because directories are created **top-down** (a parent's `DirectoryCreated`
//! precedes any message for its children), a parent's `Dir` is always present in the
//! tracker before its child files/dirs/symlinks are processed. All destination writes
//! are then fd-relative on the held parent `Dir`: file/symlink/subdirectory creation,
//! overwrite removal, directory-metadata application, and empty-directory cleanup. A
//! privileged `rcpd` destination therefore cannot be redirected by a concurrent
//! symlink swap of an intermediate destination directory into writing/creating/
//! deleting outside the destination tree — the `openat`/`mkdirat`/`unlinkat`/… resolve
//! relative to the pinned fd, never re-walking the path from the root.
//!
//! The map holds `Arc<Dir>`: callers clone the Arc out under the tracker lock, release
//! the lock, then perform the (possibly slow) fd syscall — the lock is never held
//! across a syscall, and the cloned Arc keeps the fd alive for the operation even if
//! the directory completes and is dropped from the map concurrently.

use common::safedir::Dir;
use std::sync::Arc;

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
    /// open `Dir` fd for each tracked directory, keyed by destination path. All
    /// destination writes for a directory's children resolve relative to the parent's
    /// fd held here (see the module-level "Directory fd-map" docs). Dropped when the
    /// directory completes.
    dirs: std::collections::HashMap<std::path::PathBuf, Arc<Dir>>,
    /// open `Dir` fd for the root directory's PARENT (the trusted user-specified
    /// destination parent, opened once via `open_parent_dir`). Held so the root
    /// directory's own empty-directory cleanup can `rmdir_at` it through a pinned
    /// parent fd, since the root's parent is itself never a tracked directory.
    root_parent_dir: Option<Arc<Dir>>,
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
    /// whether to fail immediately on errors
    fail_early: bool,
    /// collects errors for final reporting
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
}

impl DirectoryTracker {
    pub fn new(
        control_send_stream: remote::streams::BoxedSharedSendStream,
        preserve: common::preserve::Settings,
        fail_early: bool,
        error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    ) -> Self {
        Self {
            pending_directories: std::collections::HashMap::new(),
            failed_directories: std::collections::HashSet::new(),
            created_directories: std::collections::HashSet::new(),
            dirs: std::collections::HashMap::new(),
            root_parent_dir: None,
            metadata: std::collections::HashMap::new(),
            structure_complete: false,
            root_complete: false,
            root_directory: None,
            done_sent: false,
            control_send_stream,
            preserve,
            fail_early,
            error_collector,
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
    /// Send `DirectorySkipped` to the source for a `Directory` message the
    /// destination did NOT create (create failed, ancestor failed, or
    /// `--ignore-existing` skipped a non-directory). The source releases the
    /// matching held directory fd from its fd-map; no files are requested for a
    /// skipped directory. This balances the one-response-per-`Directory`-message
    /// contract that keeps the source's dir-fd budget effective and deadlock-free.
    ///
    /// Skipped directories are never inserted into `pending_directories`, so this
    /// does not affect `DestinationDone`/done-detection accounting.
    pub async fn send_directory_skipped(
        &self,
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> anyhow::Result<()> {
        let message = remote::protocol::DestinationMessage::DirectorySkipped {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let mut stream = self.control_send_stream.lock().await;
        stream.send_control_message(&message).await?;
        tracing::debug!("Sent DirectorySkipped: {:?} -> {:?}", src, dst);
        Ok(())
    }
    /// Look up a tracked directory's held `Arc<Dir>` by destination path.
    ///
    /// The returned Arc is a clone (a refcount bump under the tracker lock); the
    /// caller releases the lock and then performs the fd-relative syscall, so the
    /// lock is never held across a syscall and the fd stays alive for the operation
    /// even if the directory completes and is dropped from the map meanwhile.
    pub fn get_dir(&self, dst: &std::path::Path) -> Option<Arc<Dir>> {
        self.dirs.get(dst).cloned()
    }
    /// The root directory's PARENT `Dir`, if it has been opened.
    ///
    /// This is the trusted user-specified destination parent (opened via
    /// `open_parent_dir`), used to create the root directory itself and to `rmdir_at`
    /// it during empty-directory cleanup.
    pub fn root_parent_dir(&self) -> Option<Arc<Dir>> {
        self.root_parent_dir.clone()
    }
    /// Record the root directory's PARENT `Dir` (opened once via `open_parent_dir`).
    pub fn set_root_parent_dir(&mut self, dir: Arc<Dir>) {
        self.root_parent_dir = Some(dir);
    }
    /// Add a successfully created directory to tracking.
    /// Sends `DirectoryCreated` to source (the Pass-2 trigger; no count is echoed —
    /// the source retains its own authoritative Pass-1 file count).
    /// If `entry_count` is 0, the directory completes immediately.
    ///
    /// # Arguments
    /// * `dir` - the open `Dir` fd for this directory (stored in the fd-map so its
    ///   children's writes resolve relative to it)
    /// * `was_created` - true if we created this directory, false if it already existed
    /// * `entry_count` - total child entries (files + dirs + symlinks)
    /// * `keep_if_empty` - whether to keep this directory if empty
    /// * `existing` - pre-existing destination entries to include in the `DirectoryCreated` manifest
    #[allow(clippy::too_many_arguments)]
    pub async fn add_directory(
        &mut self,
        src: &std::path::Path,
        dst: &std::path::Path,
        dir: Arc<Dir>,
        metadata: remote::protocol::Metadata,
        is_root: bool,
        was_created: bool,
        entry_count: usize,
        keep_if_empty: bool,
        existing: Vec<remote::protocol::ExistingEntry>,
    ) -> anyhow::Result<()> {
        // store metadata for later application
        self.metadata.insert(dst.to_path_buf(), metadata);
        // store the open dir fd so children resolve relative to it (fd-map).
        self.dirs.insert(dst.to_path_buf(), dir);
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
        // send DirectoryCreated to trigger file sending (Pass-2 trigger only; the
        // source retains the authoritative Pass-1 file count, so none is echoed).
        let message = remote::protocol::DestinationMessage::DirectoryCreated {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            existing,
        };
        {
            let mut stream = self.control_send_stream.lock().await;
            stream.send_control_message(&message).await?;
        }
        tracing::info!(
            "Sent DirectoryCreated: {:?} -> {:?} (entries={})",
            src,
            dst,
            entry_count
        );
        // if entry_count is 0, directory is immediately complete
        if entry_count == 0 {
            self.complete_directory(dst).await?;
        }
        Ok(())
    }
    /// Mark a directory as failed (creation error). Records the failure only; the caller sends a
    /// `DirectorySkipped` nack (not `DirectoryCreated`), so the source won't send files for it.
    pub fn mark_directory_failed(&mut self, dst: &std::path::Path) {
        self.failed_directories.insert(dst.to_path_buf());
        tracing::info!("Directory marked as failed: {:?}", dst);
    }
    /// Process a file entry for a directory (File or FileSkipped).
    /// Increments entries_processed and checks completion.
    /// Returns true if directory is now complete.
    pub async fn process_file(&mut self, dst_dir: &std::path::Path) -> anyhow::Result<bool> {
        // no-op if the parent is not tracked (e.g. a FAILED directory whose counted
        // children are still being accounted via `FileSkipped`, or the root). Nothing
        // waits on an untracked directory's completion, so tolerating it is safe and
        // mirrors `process_child_entry` — without this, a `FileSkipped` (or `File`)
        // received under a failed ancestor would abort the whole destination.
        let Some(state) = self.pending_directories.get_mut(dst_dir) else {
            return Ok(false);
        };
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
    /// Complete a directory and propagate completion upward to parents.
    ///
    /// After completing a directory (applying metadata or removing it if empty),
    /// notifies the parent that this child is done. If the parent's entries are
    /// now all processed, completes the parent too, and so on up the tree.
    /// This ensures parent directories only complete after all children finish,
    /// so empty-directory cleanup decisions are correct.
    async fn complete_directory(&mut self, dst: &std::path::Path) -> anyhow::Result<()> {
        let mut current = dst.to_path_buf();
        loop {
            let is_root = self.root_directory.as_deref() == Some(&current);
            self.complete_directory_single(&current, is_root).await?;
            if is_root {
                break;
            }
            // notify parent that this child directory is complete
            let Some(parent) = current.parent() else {
                break;
            };
            let Some(state) = self.pending_directories.get_mut(parent) else {
                break;
            };
            state.entries_processed += 1;
            tracing::debug!(
                "Directory {:?} entries processed: {}/{} (child directory completed)",
                parent,
                state.entries_processed,
                state.entries_expected
            );
            if state.entries_processed < state.entries_expected {
                break; // parent not complete yet
            }
            // parent is now complete, continue loop to complete it
            current = parent.to_path_buf();
        }
        Ok(())
    }
    /// Complete a single directory: apply metadata and remove from pending.
    /// Uses `keep_if_empty` from the directory state to decide whether to remove
    /// empty directories that were only created for traversal purposes.
    ///
    /// All filesystem operations are fd-relative on held `Dir` handles: the empty-
    /// directory cleanup `rmdir_at`s the directory through its PARENT's pinned fd
    /// (the parent is still tracked when a child completes, since completion is
    /// bottom-up), and metadata is applied through the directory's OWN pinned fd via
    /// `set_dir_metadata_fd`. The directory's `Dir` is dropped from the fd-map on
    /// completion. Neither the parent fd nor the own fd is re-resolved by path, so a
    /// concurrent symlink swap of the destination path cannot redirect the cleanup or
    /// metadata application outside the destination tree.
    async fn complete_directory_single(
        &mut self,
        dst: &std::path::Path,
        is_root: bool,
    ) -> anyhow::Result<()> {
        // remove from pending
        let state = self.pending_directories.remove(dst);
        let keep_if_empty = state.as_ref().is_none_or(|s| s.keep_if_empty);
        if state.is_none() {
            tracing::warn!("directory {:?} was not in pending when completing", dst);
        }
        // drop this directory's own fd from the fd-map: it is completing, no more
        // children will be created under it. The own fd is kept locally below for the
        // metadata application (the clone keeps it alive even though it's now out of
        // the map).
        let own_dir = self.dirs.remove(dst);
        // resolve the PARENT's held Dir (and this entry's name) for fd-relative
        // empty-dir cleanup. For a nested directory the parent is still tracked
        // (bottom-up completion); for the root directory the parent is the trusted
        // root_parent_dir opened via open_parent_dir.
        let parent_dir = if is_root {
            self.root_parent_dir.clone()
        } else {
            dst.parent().and_then(|p| self.dirs.get(p).cloned())
        };
        let entry_name = dst.file_name();
        // check if we created this directory (vs reused existing)
        let was_created = self.created_directories.remove(dst);
        // handle empty directory cleanup for directories we created
        if was_created && !keep_if_empty {
            // try to remove if empty (best effort - may fail if not empty due to races).
            // fd-relative rmdir_at on the parent fd: never re-resolves dst by path, so a
            // swapped intermediate dir cannot redirect the removal. ENOTEMPTY (the common
            // "directory has content" case) is handled by keeping the directory below.
            match (parent_dir.as_ref(), entry_name) {
                (Some(parent), Some(name)) => match parent.rmdir_at(name).await {
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
                },
                _ => {
                    // parent fd missing (shouldn't happen: parent is tracked until the
                    // child completes) — keep the directory rather than fall back to a
                    // path-based removal that could be redirected.
                    tracing::warn!(
                        "No parent fd for empty-directory cleanup of {:?}; keeping it",
                        dst
                    );
                }
            }
        }
        // increment counter now (if we created it)
        if was_created {
            common::get_progress().directories_created.inc();
        }
        // apply stored metadata through the directory's OWN held fd (fd-relative).
        if let Some(metadata) = self.metadata.remove(dst) {
            match own_dir.as_ref() {
                Some(dir) => {
                    match common::safedir::set_dir_metadata_fd(&self.preserve, &metadata, dir).await
                    {
                        Ok(()) => {
                            tracing::info!("Directory complete, metadata applied: {:?}", dst);
                        }
                        Err(e) => {
                            let err = anyhow::Error::new(e)
                                .context(format!("failed to set metadata on directory {:?}", dst));
                            tracing::error!("{:#}", err);
                            if self.fail_early {
                                return Err(err);
                            }
                            self.error_collector.push(err);
                        }
                    }
                }
                None => {
                    // no held fd for this directory (shouldn't happen for a tracked
                    // directory) — fail closed rather than re-resolve dst by path.
                    let err = anyhow::anyhow!(
                        "no held directory fd for {:?} when applying metadata",
                        dst
                    );
                    tracing::error!("{:#}", err);
                    if self.fail_early {
                        return Err(err);
                    }
                    self.error_collector.push(err);
                }
            }
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
    fail_early: bool,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
) -> SharedDirectoryTracker {
    std::sync::Arc::new(tokio::sync::Mutex::new(DirectoryTracker::new(
        control_send_stream,
        preserve,
        fail_early,
        error_collector,
    )))
}
