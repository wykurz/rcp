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
//! 4. When all entries are processed AND the directory's announce (`DirectoryCreated`) has
//!    flushed, destination applies stored metadata — see [`DirectoryTracker::mark_announced`]
//!    for why completion must wait for the announce
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
    /// has this directory's announce — manifest chunks, then `DirectoryCreated` — reached the
    /// wire? Completion is gated on it (see [`DirectoryTracker::mark_announced`]): entries can all
    /// be processed first (symlink/subdirectory-only content arrives in Pass 1, not gated on the
    /// trigger), and completing then would let `DestinationDone` overtake the announce and close
    /// the control send stream it still needs
    announced: bool,
    /// whether to keep this directory if it ends up empty
    keep_if_empty: bool,
    /// what the lockdown must resolve at completion (original owner + original default ACL state),
    /// `Some` iff this reused directory was locked down under strict operand resolution (see
    /// [`common::safedir::lockdown_reused_dir`]); `None` for a freshly created directory, which must
    /// never be restore-chowned and has no destination ACL state to restore (strict creation also
    /// strips inherited ACLs)
    reused_lock: Option<common::safedir::ReusedDirLock>,
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
    /// has teardown been initiated (the control send stream closed)? A data worker uses this to tell
    /// a benign end-of-transfer close (initiated by US, tearing down) from a mid-transfer truncation.
    closing: bool,
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
            closing: false,
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
    /// Register a successfully resolved directory for tracking: fd-map, stored metadata, pending
    /// entry count, root/created bookkeeping, and — for a strict-mode locked reused directory —
    /// the lockdown to undo at completion.
    ///
    /// Registration is deliberately SEPARATE from announcing `DirectoryCreated` (see
    /// `announce_directory_created` in `destination.rs`): it must land before the control receive
    /// loop processes the next message — children resolve their parent through the fd-map, and
    /// the source's Pass-1 `Directory` messages for children do not wait for this directory's
    /// trigger — while the announce may follow later, from a per-directory task, once the
    /// overwrite manifest has been built. No directory completes here, whatever its entry count:
    /// completion follows the LATER of its last processed entry and its announce (see
    /// [`Self::mark_announced`]).
    ///
    /// # Arguments
    /// * `dir` - the open `Dir` fd for this directory (stored in the fd-map so its
    ///   children's writes resolve relative to it)
    /// * `was_created` - true if we created this directory, false if it already existed
    /// * `entry_count` - total child entries (files + dirs + symlinks)
    /// * `keep_if_empty` - whether to keep this directory if empty
    /// * `reused_lock` - what to resolve at completion (original owner + original default ACL
    ///   state) for a strict-mode locked reused directory (`None` for fresh dirs and the default
    ///   path)
    #[allow(clippy::too_many_arguments)]
    pub fn register_directory(
        &mut self,
        dst: &std::path::Path,
        dir: Arc<Dir>,
        metadata: remote::protocol::Metadata,
        is_root: bool,
        was_created: bool,
        entry_count: usize,
        keep_if_empty: bool,
        reused_lock: Option<common::safedir::ReusedDirLock>,
    ) {
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
        // add to pending with known entry count; not announced yet — the announce follows
        // (inline, or from a per-directory manifest task) and is what unlocks completion
        self.pending_directories.insert(
            dst.to_path_buf(),
            DirectoryState {
                entries_expected: entry_count,
                entries_processed: 0,
                announced: false,
                keep_if_empty,
                reused_lock,
            },
        );
        tracing::debug!(
            "Registered directory {:?} (entries={}, created={})",
            dst,
            entry_count,
            was_created
        );
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
        // check completion (gated on the announce having flushed — see mark_announced)
        if state.entries_processed >= state.entries_expected && state.announced {
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
        // check completion (gated on the announce having flushed — see mark_announced)
        if state.entries_processed >= state.entries_expected && state.announced {
            self.complete_directory(parent_dst).await?;
        }
        Ok(())
    }
    /// Record that this directory's announce — its manifest chunks, then `DirectoryCreated` — is
    /// on the wire, and complete the directory if its children already finished while the
    /// announce was pending (always the case for a 0-entry directory, which nothing else will
    /// complete).
    ///
    /// Completion is gated on the announce (`DirectoryState::announced`) because entries do not
    /// wait for it: a directory whose children are all symlinks/subdirectories has every entry
    /// processed straight off Pass-1 messages, which can all land while the directory's manifest
    /// is still being built. Completing it then would let `DestinationDone` win the race against
    /// its own announce — closing the control send stream the announce task still needs (failing
    /// a healthy copy with a broken pipe) and violating the one-response-per-`Directory` contract
    /// (docs/remote_protocol.md §2.2). No-op for an untracked directory, mirroring
    /// `process_child_entry`.
    pub async fn mark_announced(&mut self, dst: &std::path::Path) -> anyhow::Result<()> {
        let Some(state) = self.pending_directories.get_mut(dst) else {
            return Ok(());
        };
        state.announced = true;
        if state.entries_processed >= state.entries_expected {
            self.complete_directory(dst).await?;
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
    ///
    /// Callers have already established the completion conditions — all entries processed AND the
    /// announce on the wire; the upward walk re-checks both for each parent it reaches.
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
            if state.entries_processed < state.entries_expected || !state.announced {
                break; // parent not complete yet (still counting, or its announce has not flushed)
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
        // what the lockdown must undo before/while applying metadata, for a strict-mode locked
        // reused dir. Moved out of `state` rather than copied: it carries the directory's original
        // ACL bytes.
        let reused_lock = state.and_then(|s| s.reused_lock);
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
                    // for a reused directory locked down under strict mode, resolve the snapshotted
                    // default ACL and keep the uid at the copier until publishing either the original
                    // or source uid directly; no intermediate owner differs from the final owner.
                    // none for fresh dirs.
                    // the source's access AND default ACLs travel in the stored wire metadata; a
                    // `Captured` all-`None` value means the source had none and the destination's
                    // must be CLEARED (see the same note in `destination.rs`). `Unknown` means the
                    // source could not READ them (a committed directory that failed to open — its
                    // copy is already recorded as failed) or capture was off: this directory's ACL
                    // preservation is disabled for the one call, so the destination's ACLs are left
                    // alone and a locked reused directory gets its ORIGINAL default ACL back
                    // instead of an authoritative clear of state the source never observed.
                    let acls = metadata.captured_acls();
                    let preserve_for_entry = if acls.is_none() {
                        let mut p = self.preserve;
                        p.dir.acl = false;
                        p
                    } else {
                        self.preserve
                    };
                    let apply_result = common::safedir::set_reused_dir_metadata_fd(
                        &preserve_for_entry,
                        &metadata,
                        acls.as_ref(),
                        reused_lock,
                        dir,
                    )
                    .await;
                    match apply_result {
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
    /// Whether teardown has been initiated (the control stream is being/has been closed by us). A
    /// data worker uses this together with [`Self::is_done`] to distinguish a benign end-of-transfer
    /// close from a mid-transfer truncation.
    pub fn is_closing(&self) -> bool {
        self.closing
    }
    /// Whether `DestinationDone` has been sent — i.e. the copy reached completion and the send
    /// stream carried its final message. After the control receive loop exits, this distinguishes
    /// normal completion (drain the announce tasks) from a source-initiated teardown (abort them
    /// — see `process_control_stream`).
    pub fn destination_done_sent(&self) -> bool {
        self.done_sent
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
        // mark teardown as initiated BEFORE the close, so a data worker that observes the resulting
        // connection close (once the source tears down in response) classifies it as benign.
        self.closing = true;
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

#[cfg(test)]
mod tests {
    use super::*;
    // a sink writer discards every control message, so the completion state machine can be driven
    // without a real connection.
    fn mock_stream() -> remote::streams::BoxedSharedSendStream {
        let writer: remote::streams::BoxedWrite = Box::new(tokio::io::sink());
        std::sync::Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            writer,
        )))
    }
    fn new_tracker() -> DirectoryTracker {
        DirectoryTracker::new(
            mock_stream(),
            common::preserve::preserve_none(),
            false,
            std::sync::Arc::new(common::error_collector::ErrorCollector::default()),
        )
    }
    fn meta() -> remote::protocol::Metadata {
        remote::protocol::Metadata {
            mode: 0o755,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            atime_nsec: 0,
            mtime_nsec: 0,
            acls: remote::protocol::WireAcls::Unknown,
        }
    }
    async fn open_dir(path: &std::path::Path) -> Arc<Dir> {
        Arc::new(
            common::safedir::Dir::open_root_dir(path, false, common::Side::Destination)
                .await
                .unwrap(),
        )
    }
    #[test]
    fn has_failed_ancestor_walks_up_to_a_failed_directory() {
        let mut t = new_tracker();
        t.mark_directory_failed(std::path::Path::new("/dst/a/b"));
        assert!(t.has_failed_ancestor(std::path::Path::new("/dst/a/b/c")));
        assert!(t.has_failed_ancestor(std::path::Path::new("/dst/a/b/c/d")));
        assert!(
            !t.has_failed_ancestor(std::path::Path::new("/dst/a/b")),
            "the failed directory is not its own ancestor"
        );
        assert!(
            !t.has_failed_ancestor(std::path::Path::new("/dst/a/x")),
            "a sibling subtree is unaffected"
        );
    }
    #[tokio::test]
    async fn is_done_requires_structure_root_complete_and_no_pending() {
        let mut t = new_tracker();
        assert!(!t.is_done(), "a fresh tracker is not done");
        t.set_structure_complete(true); // has_root_item=true: does NOT auto-complete the root
        assert!(
            !t.is_done(),
            "structure complete but the root item is still pending"
        );
        t.set_root_complete();
        assert!(
            t.is_done(),
            "structure + root complete with no pending dirs is done"
        );
    }
    #[tokio::test]
    async fn structure_complete_with_no_root_item_is_immediately_done() {
        // dry-run / filtered-root: no root messages will follow, so completion is immediate.
        let mut t = new_tracker();
        t.set_structure_complete(false);
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn send_destination_done_guards_against_double_send() {
        let mut t = new_tracker();
        assert!(
            t.send_destination_done().await.unwrap(),
            "the first send returns true"
        );
        assert!(
            !t.send_destination_done().await.unwrap(),
            "a second send is a no-op and returns false"
        );
    }
    #[tokio::test]
    async fn process_on_untracked_directory_is_a_noop() {
        // a File/FileSkipped/child entry received under a failed (untracked) ancestor must be
        // tolerated rather than aborting the destination.
        let mut t = new_tracker();
        let untracked = std::path::Path::new("/not/tracked");
        assert!(!t.process_file(untracked).await.unwrap());
        t.process_child_entry(untracked).await.unwrap();
    }
    #[tokio::test]
    async fn child_completion_propagates_to_parent_bottom_up() {
        let tmp = tempfile::tempdir().unwrap();
        let parent_path = tmp.path().join("parent");
        let child_path = parent_path.join("child");
        std::fs::create_dir_all(&child_path).unwrap();
        let parent_dir = open_dir(&parent_path).await;
        let child_dir = open_dir(&child_path).await;
        let mut t = new_tracker();
        // the root parent expects exactly one child entry (the subdirectory).
        t.register_directory(&parent_path, parent_dir, meta(), true, true, 1, true, None);
        t.mark_announced(&parent_path).await.unwrap();
        assert!(!t.is_done(), "the parent still awaits its child");
        // the child has no entries: its announce completes it, which notifies the parent and
        // completes it too.
        t.register_directory(&child_path, child_dir, meta(), false, true, 0, true, None);
        t.mark_announced(&child_path).await.unwrap();
        t.set_structure_complete(true);
        assert!(
            t.is_done(),
            "completing the child must propagate bottom-up and complete the root parent"
        );
    }
    #[tokio::test]
    async fn completion_waits_for_the_announce() {
        // the DestinationDone-overtakes-DirectoryCreated race: a reused directory's children can
        // ALL be processed straight off Pass-1 messages (symlinks/subdirectories) while its
        // manifest is still being built — the directory must NOT complete, and DestinationDone
        // must not become sendable, until its announce has flushed.
        let tmp = tempfile::tempdir().unwrap();
        let root_path = tmp.path().join("root");
        std::fs::create_dir(&root_path).unwrap();
        let root_dir = open_dir(&root_path).await;
        let mut t = new_tracker();
        // reused root whose single entry (a symlink) arrives before the announce
        t.register_directory(&root_path, root_dir, meta(), true, false, 1, true, None);
        t.set_structure_complete(true);
        t.process_child_entry(&root_path).await.unwrap();
        assert!(
            !t.is_done(),
            "all entries processed, but the announce has not flushed — completing now would \
             let DestinationDone overtake DirectoryCreated"
        );
        t.mark_announced(&root_path).await.unwrap();
        assert!(
            t.is_done(),
            "the announce completes the already-fully-counted directory"
        );
    }
    #[tokio::test]
    async fn parent_completion_waits_for_the_parents_own_announce() {
        // the announce gate must also hold during bottom-up propagation: a completing child fills
        // the parent's count, but the parent stays pending until its OWN announce flushes.
        let tmp = tempfile::tempdir().unwrap();
        let parent_path = tmp.path().join("parent");
        let child_path = parent_path.join("child");
        std::fs::create_dir_all(&child_path).unwrap();
        let parent_dir = open_dir(&parent_path).await;
        let child_dir = open_dir(&child_path).await;
        let mut t = new_tracker();
        t.register_directory(&parent_path, parent_dir, meta(), true, false, 1, true, None);
        t.register_directory(&child_path, child_dir, meta(), false, true, 0, true, None);
        t.set_structure_complete(true);
        t.mark_announced(&child_path).await.unwrap();
        assert!(
            !t.is_done(),
            "the child completed and filled the parent's count, but the parent's announce has \
             not flushed"
        );
        t.mark_announced(&parent_path).await.unwrap();
        assert!(
            t.is_done(),
            "the parent's announce completes it once its entries are already counted"
        );
    }
    #[tokio::test]
    async fn empty_created_directory_is_removed_when_not_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let root_path = tmp.path().join("root");
        std::fs::create_dir(&root_path).unwrap();
        let mut t = new_tracker();
        // the root's parent fd (the trusted destination parent) backs the fd-relative rmdir.
        t.set_root_parent_dir(open_dir(tmp.path()).await);
        let root_dir = open_dir(&root_path).await;
        // created, empty (entry_count=0), keep_if_empty=false → removed via the parent's pinned
        // fd once its announce completes it.
        t.register_directory(&root_path, root_dir, meta(), true, true, 0, false, None);
        t.mark_announced(&root_path).await.unwrap();
        assert!(
            !root_path.exists(),
            "an empty created directory with keep_if_empty=false must be removed"
        );
    }
}
