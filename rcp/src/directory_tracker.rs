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
//! A Begin retains the destination directory descriptor and metadata. Finalization requires
//! Ready to have flushed, End to have sealed the child count, and every child to have finished.
//! Child directories contribute once after their own finalization. Rejected subtrees retain only
//! protocol bookkeeping; their descendants never contribute to an unrelated parent.
//!
//! All child writes resolve through held parent descriptors. Metadata uses the held directory
//! descriptor, while empty-directory cleanup acts by name through its held parent.

use common::safedir::Dir;
use std::sync::Arc;

/// State for a single directory waiting for child entries.
#[derive(Debug)]
struct DirectoryState {
    /// The final child count is available only after End.
    discovery: DiscoveryState,
    /// Completed direct-child obligations.
    entries_processed: usize,
    /// Direct-child directory names that entered finalization, retained only until this parent
    /// completes or discovery ends. They reject repeated Begins even if finalization failed.
    completed_directories: std::collections::HashSet<std::ffi::OsString>,
    /// Ready and its preceding manifest chunks have flushed.
    announced: bool,
    /// whether to keep this directory if it ends up empty
    keep_if_empty: bool,
    /// what the lockdown must undo at completion (original owner + original ACLs), `Some` iff this
    /// reused directory was locked down under strict operand resolution (see
    /// [`common::safedir::lockdown_reused_dir`]); `None` for a freshly created directory, which must
    /// never be restore-chowned and whose inherited ACLs were stripped outright at creation
    reused_lock: Option<common::safedir::ReusedDirLock>,
}

#[derive(Debug)]
enum DiscoveryState {
    Discovering,
    Sealed { expected: usize },
}
impl DirectoryState {
    fn ready_to_finalize(&self) -> bool {
        self.announced
            && matches!(self.discovery, DiscoveryState::Sealed { expected } if self.entries_processed == expected)
    }
    fn record_child(&mut self) -> anyhow::Result<()> {
        let processed = self
            .entries_processed
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("directory child count overflow"))?;
        if let DiscoveryState::Sealed { expected } = self.discovery {
            anyhow::ensure!(
                processed <= expected,
                "directory child count exceeds sealed count"
            );
        }
        self.entries_processed = processed;
        Ok(())
    }
}
#[derive(Debug)]
struct RejectedDirectory {
    sealed: bool,
}

/// An admitted Begin whose root claim precedes destination filesystem work.
/// Registration or rejection consumes its original destination identity.
#[must_use]
pub(super) struct DirectoryAdmission {
    dst: std::path::PathBuf,
    is_root: bool,
}

/// Tracks directory entry counts and completion state for remote copy operations.
pub struct DirectoryTracker {
    /// Directories waiting for End, Ready, or child completion.
    pending_directories: std::collections::HashMap<std::path::PathBuf, DirectoryState>,
    /// Exact rejected Begins and End bookkeeping, retained through discovery even after their
    /// accepted parent completes. Each descendant Begin still requires its own End.
    rejected_directories: std::collections::HashMap<std::path::PathBuf, RejectedDirectory>,
    /// Minimal rejected subtree roots, retained for late file outcomes after discovery.
    failed_subtrees: std::collections::HashSet<std::path::PathBuf>,
    root_observed: bool,
    /// directories that we created (vs reused existing) - used for empty dir cleanup
    created_directories: std::collections::HashSet<std::path::PathBuf>,
    /// open `Dir` fd for each tracked directory, keyed by destination path. All
    /// destination writes for a directory's children resolve relative to the parent's
    /// fd held here. Dropped when the
    /// directory completes.
    dirs: std::collections::HashMap<std::path::PathBuf, Arc<Dir>>,
    /// open `Dir` fd for the root directory's PARENT (the trusted user-specified
    /// destination parent, opened once via `open_parent_dir`). Held so the root
    /// directory's own empty-directory cleanup can `rmdir_at` it through a pinned
    /// parent fd, since the root's parent is itself never a tracked directory.
    root_parent_dir: Option<Arc<Dir>>,
    /// stored metadata for each directory (applied when complete)
    metadata: std::collections::HashMap<std::path::PathBuf, remote::protocol::Metadata>,
    /// have we received DiscoveryComplete?
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
    /// control stream for sending DirectoryReady
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
            rejected_directories: std::collections::HashMap::new(),
            failed_subtrees: std::collections::HashSet::new(),
            root_observed: false,
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
            if self.failed_subtrees.contains(parent) {
                return true;
            }
            current = parent;
        }
        false
    }
    /// Send the response for a rejected Begin. Its End remains required for discovery completion.
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
    /// Retain an accepted Begin's descriptor, metadata, and reused-directory lockdown.
    /// Children may arrive before Ready; finalization also waits for End and all children.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn register_directory(
        &mut self,
        admission: DirectoryAdmission,
        dir: Arc<Dir>,
        metadata: remote::protocol::Metadata,
        was_created: bool,
        keep_if_empty: bool,
        reused_lock: Option<common::safedir::ReusedDirLock>,
    ) -> anyhow::Result<()> {
        let DirectoryAdmission { dst, is_root } = admission;
        let dst = dst.as_path();
        self.validate_directory_begin(dst, is_root)?;
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
        // retain the unsealed count until End; Ready independently gates completion
        self.pending_directories.insert(
            dst.to_path_buf(),
            DirectoryState {
                discovery: DiscoveryState::Discovering,
                entries_processed: 0,
                completed_directories: std::collections::HashSet::new(),
                announced: false,
                keep_if_empty,
                reused_lock,
            },
        );
        Ok(())
    }
    /// Reject structural messages after the discovery marker.
    pub fn ensure_discovering(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.structure_complete,
            "structural message after DiscoveryComplete"
        );
        Ok(())
    }
    /// Admit a Begin and claim the root before destination filesystem work starts.
    pub(super) fn admit_directory(
        &mut self,
        dst: &std::path::Path,
        is_root: bool,
    ) -> anyhow::Result<DirectoryAdmission> {
        self.validate_directory_begin(dst, is_root)?;
        if is_root {
            self.observe_root()?;
        }
        Ok(DirectoryAdmission {
            dst: dst.to_path_buf(),
            is_root,
        })
    }
    /// Validate discovery state, directory uniqueness, and parent membership.
    fn validate_directory_begin(&self, dst: &std::path::Path, is_root: bool) -> anyhow::Result<()> {
        self.ensure_discovering()?;
        anyhow::ensure!(
            !self.pending_directories.contains_key(dst)
                && !self.rejected_directories.contains_key(dst),
            "duplicate DirectoryBegin for {dst:?}"
        );
        if !is_root {
            let parent = dst
                .parent()
                .ok_or_else(|| anyhow::anyhow!("directory has no parent"))?;
            if let Some(state) = self.pending_directories.get(parent) {
                let name = dst
                    .file_name()
                    .ok_or_else(|| anyhow::anyhow!("directory has no name"))?;
                anyhow::ensure!(
                    !state.completed_directories.contains(name),
                    "duplicate DirectoryBegin for {dst:?}"
                );
            } else {
                anyhow::ensure!(
                    self.in_rejected_subtree(parent),
                    "DirectoryBegin has unknown or completed parent {parent:?}"
                );
            }
        }
        Ok(())
    }
    /// Record a root header before starting its filesystem work.
    pub fn observe_root(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.root_observed, "duplicate root item");
        anyhow::ensure!(
            !(self.structure_complete && self.root_complete),
            "root item after DiscoveryComplete(false)"
        );
        self.root_observed = true;
        Ok(())
    }
    fn in_rejected_subtree(&self, dst: &std::path::Path) -> bool {
        self.failed_subtrees.contains(dst) || self.has_failed_ancestor(dst)
    }
    /// Record a rejected Begin and settle its parent slot exactly once.
    pub(super) async fn reject_directory(
        &mut self,
        admission: DirectoryAdmission,
    ) -> anyhow::Result<()> {
        let DirectoryAdmission { dst, is_root } = admission;
        let dst = dst.as_path();
        self.validate_directory_begin(dst, is_root)?;
        // parent membership is checked before admission, so an already rejected ancestor covers
        // this entire subtree. No scan or removal of previously registered descendants is needed.
        if !self.has_failed_ancestor(dst) {
            self.failed_subtrees.insert(dst.to_path_buf());
        }
        self.rejected_directories
            .insert(dst.to_path_buf(), RejectedDirectory { sealed: false });
        if is_root {
            self.set_root_complete();
        } else if let Some(parent) = dst.parent() {
            self.process_child_entry(parent).await?;
        }
        Ok(())
    }
    /// Seal an accepted or rejected Begin with its final admitted-child count.
    pub async fn seal_directory(
        &mut self,
        dst: &std::path::Path,
        expected: usize,
    ) -> anyhow::Result<()> {
        self.ensure_discovering()?;
        if let Some(state) = self.rejected_directories.get_mut(dst) {
            anyhow::ensure!(!state.sealed, "duplicate DirectoryEnd for {dst:?}");
            state.sealed = true;
            return Ok(());
        }
        let state = self.pending_directories.get_mut(dst).ok_or_else(|| {
            anyhow::anyhow!("DirectoryEnd for unknown or completed directory {dst:?}")
        })?;
        anyhow::ensure!(
            matches!(state.discovery, DiscoveryState::Discovering),
            "duplicate DirectoryEnd for {dst:?}"
        );
        anyhow::ensure!(
            state.entries_processed <= expected,
            "DirectoryEnd count below completed children for {dst:?}"
        );
        state.discovery = DiscoveryState::Sealed { expected };
        if state.ready_to_finalize() {
            self.complete_directory(dst).await?;
        }
        Ok(())
    }
    /// Record one terminal child event, ignoring only known rejected-subtree traffic.
    pub async fn process_file(&mut self, dst: &std::path::Path) -> anyhow::Result<bool> {
        if self.in_rejected_subtree(dst) {
            return Ok(false);
        }
        let state = self.pending_directories.get_mut(dst).ok_or_else(|| {
            anyhow::anyhow!("child outcome for unknown or completed directory {dst:?}")
        })?;
        state.record_child()?;
        if state.ready_to_finalize() {
            self.complete_directory(dst).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Record a symlink or rejected child directory's terminal outcome.
    pub async fn process_child_entry(&mut self, dst: &std::path::Path) -> anyhow::Result<()> {
        self.process_file(dst).await?;
        Ok(())
    }
    /// Record Ready after releasing the send lock and evaluate the finalization gate.
    pub async fn mark_announced(&mut self, dst: &std::path::Path) -> anyhow::Result<()> {
        let state = self
            .pending_directories
            .get_mut(dst)
            .ok_or_else(|| anyhow::anyhow!("Ready for unknown or completed directory {dst:?}"))?;
        anyhow::ensure!(!state.announced, "duplicate DirectoryReady for {dst:?}");
        state.announced = true;
        if state.ready_to_finalize() {
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
    /// The upward walk evaluates the same three finalization gates for every parent.
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
            let state = self.pending_directories.get_mut(parent).ok_or_else(|| {
                anyhow::anyhow!("completed child has no pending parent {parent:?}")
            })?;
            state.record_child()?;
            if !state.ready_to_finalize() {
                break;
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
        if !is_root && !self.structure_complete {
            let parent = dst
                .parent()
                .ok_or_else(|| anyhow::anyhow!("finalizing directory has no parent"))?;
            let name = dst
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("finalizing directory has no name"))?;
            let parent_state = self.pending_directories.get_mut(parent).ok_or_else(|| {
                anyhow::anyhow!("finalizing child has no pending parent {parent:?}")
            })?;
            // reserve the identity before removing pending state or awaiting filesystem work.
            // failed metadata must not make a duplicate Begin admissible during fatal teardown.
            parent_state
                .completed_directories
                .insert(name.to_os_string());
        }
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
                (Some(parent), Some(name)) => {
                    match common::timing_scope!(trace, "destination.directory.finalize.prune")
                        .measure(parent.rmdir_at(name))
                        .await
                    {
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
                    // for a reused directory locked down under strict mode, put back the ACLs the
                    // lockdown stripped and restore the original owner component-wise, then apply
                    // source metadata (see set_reused_dir_metadata_fd — no transient window hands the
                    // directory to a hostile prior owner); None for fresh dirs.
                    // The source's access AND default ACLs travel in the stored wire metadata; a
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
                    let apply_result =
                        common::timing_scope!(trace, "destination.directory.finalize.metadata")
                            .measure(common::safedir::set_reused_dir_metadata_fd(
                                &preserve_for_entry,
                                &metadata,
                                acls.as_ref(),
                                reused_lock,
                                dir,
                            ))
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
        self.root_observed = true;
        self.root_complete = true;
        tracing::info!("Root item complete");
    }
    /// Validate discovery completion without waiting for Ready or file payloads.
    pub async fn finish_discovery(&mut self, has_root_item: bool) -> anyhow::Result<()> {
        self.ensure_discovering()?;
        anyhow::ensure!(
            has_root_item || !self.root_observed,
            "DiscoveryComplete(false) after an observed root"
        );
        anyhow::ensure!(
            self.pending_directories
                .values()
                .all(|state| matches!(state.discovery, DiscoveryState::Sealed { .. })),
            "DiscoveryComplete before accepted directory End"
        );
        anyhow::ensure!(
            self.rejected_directories.values().all(|state| state.sealed),
            "DiscoveryComplete before rejected directory End"
        );
        self.structure_complete = true;
        // structural traffic is now rejected globally. Release the allocations too, while keeping
        // only the minimal rejected prefixes needed by independently arriving file outcomes.
        self.rejected_directories = std::collections::HashMap::new();
        for state in self.pending_directories.values_mut() {
            state.completed_directories = std::collections::HashSet::new();
        }
        if !has_root_item {
            self.root_complete = true;
        }
        Ok(())
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

/// Share directory completion state while measuring only time spent acquiring its mutex.
#[derive(Clone)]
pub(super) struct SharedDirectoryTracker(Arc<tokio::sync::Mutex<DirectoryTracker>>);

impl SharedDirectoryTracker {
    pub(super) fn new(
        control_send_stream: remote::streams::BoxedSharedSendStream,
        preserve: common::preserve::Settings,
        fail_early: bool,
        error_collector: Arc<common::error_collector::ErrorCollector>,
    ) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(DirectoryTracker::new(
            control_send_stream,
            preserve,
            fail_early,
            error_collector,
        ))))
    }
    pub(super) async fn lock(&self) -> tokio::sync::MutexGuard<'_, DirectoryTracker> {
        common::timing_scope!(trace, "destination.tracker.wait")
            .measure(self.0.lock())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use tracing::instrument::WithSubscriber as _;

    #[derive(Clone, Default)]
    struct TimingObservation(Arc<std::sync::Mutex<Vec<ObservedScope>>>);
    struct ObservedScope {
        name: &'static str,
        finished: Option<bool>,
    }
    impl TimingObservation {
        fn counts(&self, name: &str) -> (usize, usize, usize) {
            let scopes = self.0.lock().unwrap();
            let matching: Vec<_> = scopes.iter().filter(|scope| scope.name == name).collect();
            (
                matching.len(),
                matching
                    .iter()
                    .filter(|scope| scope.finished == Some(true))
                    .count(),
                matching
                    .iter()
                    .filter(|scope| scope.finished == Some(false))
                    .count(),
            )
        }
    }
    impl tracing::Subscriber for TimingObservation {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "rcp::timing"
        }
        fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::Id {
            let mut scopes = self.0.lock().unwrap();
            scopes.push(ObservedScope {
                name: attributes.metadata().name(),
                finished: None,
            });
            tracing::Id::from_u64(scopes.len() as u64)
        }
        fn record(&self, id: &tracing::Id, values: &tracing::span::Record<'_>) {
            struct Completion(Option<bool>);
            impl tracing::field::Visit for Completion {
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
                fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                    if field.name() == "timing_finished" {
                        self.0 = Some(value);
                    }
                }
            }
            let mut completion = Completion(None);
            values.record(&mut completion);
            if let Some(finished) = completion.0 {
                self.0.lock().unwrap()[id.into_u64() as usize - 1].finished = Some(finished);
            }
        }
        fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::Id) {}
        fn exit(&self, _: &tracing::Id) {}
    }

    #[tokio::test]
    async fn tracker_wait_finishes_when_the_guard_is_acquired() {
        let tracker = SharedDirectoryTracker::new(
            mock_stream(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        let held = tracker.lock().await;
        let observed = TimingObservation::default();
        async {
            let waiting = tracker.lock();
            tokio::pin!(waiting);
            assert!(waiting.as_mut().now_or_never().is_none());
            assert_eq!(observed.counts("destination.tracker.wait"), (1, 0, 0));
            drop(held);
            let mut acquired = waiting.await;
            assert_eq!(observed.counts("destination.tracker.wait"), (1, 1, 0));
            acquired.observe_root().unwrap();
            assert_eq!(
                observed.counts("destination.tracker.wait"),
                (1, 1, 0),
                "holding the returned guard must not extend the wait scope"
            );
        }
        .with_subscriber(observed.clone())
        .await;
        assert!(tracker.lock().await.root_observed);
    }

    #[tokio::test]
    async fn cancelled_tracker_wait_is_interrupted_without_acquiring() {
        let tracker = SharedDirectoryTracker::new(
            mock_stream(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        let held = tracker.lock().await;
        let observed = TimingObservation::default();
        async {
            let mut waiting = Box::pin(tracker.lock());
            assert!(waiting.as_mut().now_or_never().is_none());
            drop(waiting);
        }
        .with_subscriber(observed.clone())
        .await;
        assert_eq!(observed.counts("destination.tracker.wait"), (1, 0, 1));
        drop(held);
        tracker.lock().await.observe_root().unwrap();
    }

    #[tokio::test]
    async fn child_completion_times_each_directory_in_the_ancestor_cascade() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let mut tracker = new_tracker();
        register(&mut tracker, tmp.path(), true).await;
        tracker.mark_announced(tmp.path()).await.unwrap();
        tracker.seal_directory(tmp.path(), 1).await.unwrap();
        register(&mut tracker, &child, false).await;
        tracker.mark_announced(&child).await.unwrap();
        let observed = TimingObservation::default();
        tracker
            .seal_directory(&child, 0)
            .with_subscriber(observed.clone())
            .await
            .unwrap();
        tracker.finish_discovery(true).await.unwrap();
        assert!(tracker.is_done());
        assert!(tracker.get_dir(tmp.path()).is_none());
        assert!(tracker.get_dir(&child).is_none());
        assert_eq!(
            observed.counts("destination.directory.finalize.metadata"),
            (2, 2, 0)
        );
    }

    #[tokio::test]
    async fn pruned_directory_times_removal_without_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("traversal-only");
        std::fs::create_dir(&child).unwrap();
        let mut tracker = new_tracker();
        register(&mut tracker, tmp.path(), true).await;
        let admission = tracker.admit_directory(&child, false).unwrap();
        tracker
            .register_directory(admission, open_dir(&child).await, meta(), true, false, None)
            .unwrap();
        tracker.mark_announced(&child).await.unwrap();
        let observed = TimingObservation::default();
        tracker
            .seal_directory(&child, 0)
            .with_subscriber(observed.clone())
            .await
            .unwrap();
        assert!(!child.exists());
        assert_eq!(tracker.pending_directories[tmp.path()].entries_processed, 1);
        assert_eq!(
            observed.counts("destination.directory.finalize.prune"),
            (1, 1, 0)
        );
        assert_eq!(
            observed.counts("destination.directory.finalize.metadata"),
            (0, 0, 0)
        );
    }

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
    fn retained_history(t: &DirectoryTracker) -> usize {
        t.pending_directories
            .values()
            .map(|state| state.completed_directories.len())
            .sum::<usize>()
            + t.rejected_directories.len()
            + t.failed_subtrees.len()
    }
    fn register_virtual(t: &mut DirectoryTracker, path: &std::path::Path, dir: &Arc<Dir>) {
        // these state-machine fixtures reuse one real descriptor: no metadata is preserved and
        // no directory is marked created, so finalization performs no path-dependent mutations.
        let admission = t.admit_directory(path, false).unwrap();
        t.register_directory(admission, dir.clone(), meta(), false, true, None)
            .unwrap();
    }
    #[tokio::test]
    async fn failed_finalization_rejects_duplicate_begin_without_settling_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let mut t = new_tracker();
        t.fail_early = true;
        t.preserve.dir.user_and_time.time = true;
        register(&mut t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        t.seal_directory(tmp.path(), 1).await.unwrap();
        let mut invalid_metadata = meta();
        // outside the kernel's nanosecond range and neither UTIME_NOW nor UTIME_OMIT.
        invalid_metadata.mtime_nsec = 1_000_000_000;
        let admission = t.admit_directory(&child, false).unwrap();
        t.register_directory(
            admission,
            open_dir(&child).await,
            invalid_metadata,
            false,
            true,
            None,
        )
        .unwrap();
        t.mark_announced(&child).await.unwrap();
        let observed = TimingObservation::default();
        let error = t
            .seal_directory(&child, 0)
            .with_subscriber(observed.clone())
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("Invalid argument"));
        assert_eq!(
            observed.counts("destination.directory.finalize.metadata"),
            (1, 1, 0)
        );
        assert!(!t.pending_directories.contains_key(&child));
        assert_eq!(t.pending_directories[tmp.path()].entries_processed, 0);
        assert!(t.admit_directory(&child, false).is_err());
        assert!(t.seal_directory(&child, 0).await.is_err());
        assert!(t.process_file(&child).await.is_err());
        assert!(!t.is_done());
    }
    #[tokio::test]
    async fn completed_subtrees_retain_only_names_under_pending_parents() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = open_dir(tmp.path()).await;
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        for group in 0..24 {
            let branch = tmp.path().join(format!("branch-{group}"));
            register_virtual(&mut t, &branch, &dir);
            t.mark_announced(&branch).await.unwrap();
            for leaf in 0..32 {
                let leaf = branch.join(format!("leaf-{leaf}"));
                register_virtual(&mut t, &leaf, &dir);
                t.mark_announced(&leaf).await.unwrap();
                t.seal_directory(&leaf, 0).await.unwrap();
                assert!(t.admit_directory(&leaf, false).is_err());
            }
            t.seal_directory(&branch, 32).await.unwrap();
            assert_eq!(retained_history(&t), group + 1);
            assert_eq!(t.pending_directories.len(), 1);
            assert!(t.admit_directory(&branch, false).is_err());
            let pruned_leaf = branch.join("leaf-0");
            assert!(t.admit_directory(&pruned_leaf, false).is_err());
            assert!(t.seal_directory(&pruned_leaf, 0).await.is_err());
            assert!(t.mark_announced(&pruned_leaf).await.is_err());
            assert!(t.process_file(&pruned_leaf).await.is_err());
        }
        t.seal_directory(tmp.path(), 24).await.unwrap();
        assert_eq!(retained_history(&t), 0);
        assert!(t.admit_directory(tmp.path(), true).is_err());
        t.finish_discovery(true).await.unwrap();
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn discovery_releases_success_history_before_late_files_complete() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = open_dir(tmp.path()).await;
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        let completed = tmp.path().join("completed");
        register_virtual(&mut t, &completed, &dir);
        t.mark_announced(&completed).await.unwrap();
        t.seal_directory(&completed, 0).await.unwrap();
        let pending = tmp.path().join("pending");
        register_virtual(&mut t, &pending, &dir);
        t.mark_announced(&pending).await.unwrap();
        t.seal_directory(&pending, 1).await.unwrap();
        t.seal_directory(tmp.path(), 3).await.unwrap();
        assert_eq!(retained_history(&t), 1);
        t.finish_discovery(true).await.unwrap();
        assert_eq!(retained_history(&t), 0);
        assert!(
            t.pending_directories
                .values()
                .all(|state| state.completed_directories.capacity() == 0)
        );
        assert!(t.admit_directory(&completed, false).is_err());
        assert!(t.seal_directory(&completed, 0).await.is_err());
        assert!(t.process_file(&completed).await.is_err());
        t.process_file(&pending).await.unwrap();
        assert_eq!(retained_history(&t), 0);
        assert!(!t.is_done());
        t.process_file(tmp.path()).await.unwrap();
        assert!(t.is_done());
        assert_eq!(retained_history(&t), 0);
    }
    #[tokio::test]
    async fn rejected_descendant_ends_survive_parent_completion_then_compact() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        t.seal_directory(tmp.path(), 1).await.unwrap();
        let rejected = tmp.path().join("rejected");
        reject(&mut t, &rejected, false).await.unwrap();
        assert!(t.pending_directories.is_empty());
        t.seal_directory(&rejected, 64).await.unwrap();
        for index in 0..64 {
            let descendant = rejected.join(format!("child-{index}"));
            reject(&mut t, &descendant, false).await.unwrap();
            assert!(t.finish_discovery(true).await.is_err());
            assert!(t.admit_directory(&descendant, false).is_err());
            t.seal_directory(&descendant, 8).await.unwrap();
            assert!(t.seal_directory(&descendant, 8).await.is_err());
        }
        assert!(t.admit_directory(&rejected, false).is_err());
        t.finish_discovery(true).await.unwrap();
        assert_eq!(retained_history(&t), 1);
        assert_eq!(t.rejected_directories.capacity(), 0);
        assert!(t.is_done());
        let late = rejected.join("child-0/late-file");
        assert!(t.has_failed_ancestor(&late));
        assert!(!t.process_file(&rejected).await.unwrap());
        t.process_child_entry(&late).await.unwrap();
        assert!(t.process_file(tmp.path()).await.is_err());
        assert!(t.process_file(&tmp.path().join("unknown")).await.is_err());
        assert!(t.admit_directory(&late, false).is_err());
        assert!(t.seal_directory(&rejected, 64).await.is_err());
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
    async fn register(t: &mut DirectoryTracker, path: &std::path::Path, root: bool) {
        let admission = t.admit_directory(path, root).unwrap();
        t.register_directory(admission, open_dir(path).await, meta(), false, true, None)
            .unwrap();
    }
    async fn reject(
        t: &mut DirectoryTracker,
        dst: &std::path::Path,
        root: bool,
    ) -> anyhow::Result<()> {
        let admission = t.admit_directory(dst, root)?;
        t.reject_directory(admission).await
    }
    #[tokio::test]
    async fn unsealed_empty_directory_keeps_its_descriptor() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(
            t.get_dir(tmp.path()).is_some(),
            "Ready alone must not finalize a directory without End"
        );
        assert!(t.finish_discovery(true).await.is_err());
        t.seal_directory(tmp.path(), 0).await.unwrap();
        t.finish_discovery(true).await.unwrap();
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn unknown_parent_completion_is_rejected() {
        let mut t = new_tracker();
        assert!(
            t.process_file(std::path::Path::new("/unknown"))
                .await
                .is_err()
        );
        assert!(
            t.process_child_entry(std::path::Path::new("/unknown"))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn metadata_waits_for_every_ordering_of_the_three_gates() {
        use std::os::unix::fs::PermissionsExt;
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let mut t = new_tracker();
            t.preserve = common::preserve::Settings::default();
            register(&mut t, tmp.path(), true).await;
            let observed = TimingObservation::default();
            for (index, gate) in order.into_iter().enumerate() {
                async {
                    match gate {
                        0 => t.mark_announced(tmp.path()).await.unwrap(),
                        1 => t.seal_directory(tmp.path(), 1).await.unwrap(),
                        2 => {
                            t.process_file(tmp.path()).await.unwrap();
                        }
                        _ => unreachable!(),
                    }
                }
                .with_subscriber(observed.clone())
                .await;
                assert_eq!(
                    observed.counts("destination.directory.finalize.metadata"),
                    if index == 2 { (1, 1, 0) } else { (0, 0, 0) },
                    "order {order:?}"
                );
                assert_eq!(
                    observed.counts("destination.directory.finalize.prune"),
                    (0, 0, 0)
                );
                let mode = std::fs::metadata(tmp.path()).unwrap().permissions().mode() & 0o777;
                assert_eq!(
                    mode,
                    if index == 2 { 0o755 } else { 0o700 },
                    "order {order:?}, gate {gate}"
                );
                assert_eq!(t.get_dir(tmp.path()).is_none(), index == 2);
            }
            t.finish_discovery(true).await.unwrap();
            assert!(t.is_done());
        }
    }
    #[tokio::test]
    async fn nested_directory_contributes_once_after_finalization() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.seal_directory(tmp.path(), 1).await.unwrap();
        register(&mut t, &child, false).await;
        t.seal_directory(&child, 0).await.unwrap();
        t.finish_discovery(true).await.unwrap();
        t.mark_announced(&child).await.unwrap();
        assert_eq!(t.pending_directories[tmp.path()].entries_processed, 1);
        assert!(!t.is_done());
        assert!(t.mark_announced(&child).await.is_err());
        assert_eq!(t.pending_directories[tmp.path()].entries_processed, 1);
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn rejected_child_settles_parent_once_and_still_requires_end() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("rejected");
        let descendant = child.join("descendant");
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.seal_directory(tmp.path(), 1).await.unwrap();
        t.mark_announced(tmp.path()).await.unwrap();
        reject(&mut t, &child, false).await.unwrap();
        assert!(t.get_dir(tmp.path()).is_none());
        reject(&mut t, &descendant, false).await.unwrap();
        t.process_file(&child).await.unwrap();
        t.process_child_entry(&descendant).await.unwrap();
        assert!(t.finish_discovery(true).await.is_err());
        t.seal_directory(&child, 5).await.unwrap();
        assert!(t.finish_discovery(true).await.is_err());
        t.seal_directory(&descendant, 8).await.unwrap();
        assert!(t.seal_directory(&child, 5).await.is_err());
        assert!(reject(&mut t, &child, false).await.is_err());
        t.finish_discovery(true).await.unwrap();
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn rejected_root_cannot_finish_discovery_without_end() {
        let mut t = new_tracker();
        let root = std::path::Path::new("/rejected");
        reject(&mut t, root, true).await.unwrap();
        assert!(t.finish_discovery(true).await.is_err());
        assert!(t.finish_discovery(false).await.is_err());
        t.seal_directory(root, 0).await.unwrap();
        t.finish_discovery(true).await.unwrap();
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn duplicate_and_unknown_directory_messages_are_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = new_tracker();
        let unknown = tmp.path().join("unknown");
        assert!(t.seal_directory(&unknown, 0).await.is_err());
        assert!(t.mark_announced(&unknown).await.is_err());
        register(&mut t, tmp.path(), true).await;
        assert!(t.admit_directory(tmp.path(), true).is_err());
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(t.mark_announced(tmp.path()).await.is_err());
        t.seal_directory(tmp.path(), 1).await.unwrap();
        assert!(t.seal_directory(tmp.path(), 1).await.is_err());
        t.process_file(tmp.path()).await.unwrap();
        assert!(t.seal_directory(tmp.path(), 1).await.is_err());
        assert!(t.process_file(tmp.path()).await.is_err());
    }
    #[tokio::test]
    async fn counts_reject_underreported_end_and_overflow_without_mutating() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.process_file(tmp.path()).await.unwrap();
        assert!(t.seal_directory(tmp.path(), 0).await.is_err());
        t.seal_directory(tmp.path(), 1).await.unwrap();
        assert!(t.process_file(tmp.path()).await.is_err());
        assert_eq!(t.pending_directories[tmp.path()].entries_processed, 1);
        let state = t.pending_directories.get_mut(tmp.path()).unwrap();
        state.discovery = DiscoveryState::Discovering;
        state.entries_processed = usize::MAX;
        assert!(t.process_file(tmp.path()).await.is_err());
        assert_eq!(
            t.pending_directories[tmp.path()].entries_processed,
            usize::MAX
        );
    }
    #[tokio::test]
    async fn discovery_can_finish_before_ready_and_final_file_completion() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.seal_directory(tmp.path(), 1).await.unwrap();
        t.finish_discovery(true).await.unwrap();
        assert!(!t.is_done());
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(!t.is_done());
        t.process_file(tmp.path()).await.unwrap();
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn discovery_marker_rejects_duplicates_late_structure_and_false_root_claim() {
        let tmp = tempfile::tempdir().unwrap();
        let mut t = new_tracker();
        register(&mut t, tmp.path(), true).await;
        t.seal_directory(tmp.path(), 0).await.unwrap();
        assert!(t.finish_discovery(false).await.is_err());
        t.finish_discovery(true).await.unwrap();
        assert!(t.finish_discovery(true).await.is_err());
        assert!(t.ensure_discovering().is_err());
        assert!(t.admit_directory(&tmp.path().join("late"), false).is_err());
        assert!(t.seal_directory(tmp.path(), 0).await.is_err());
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn admitted_root_directory_excludes_another_root_before_registration() {
        let mut t = new_tracker();
        let _admission = t
            .admit_directory(std::path::Path::new("/root"), true)
            .unwrap();
        assert!(
            t.observe_root().is_err(),
            "directory admission must reserve the root before filesystem work"
        );
    }
    #[tokio::test]
    async fn root_file_can_arrive_after_discovery() {
        let mut t = new_tracker();
        t.finish_discovery(true).await.unwrap();
        assert!(!t.is_done());
        t.observe_root().unwrap();
        assert!(t.observe_root().is_err());
        t.set_root_complete();
        assert!(t.is_done());
    }
    #[tokio::test]
    async fn empty_discovery_completes_once_and_rejects_late_root() {
        let mut t = new_tracker();
        t.finish_discovery(false).await.unwrap();
        assert!(t.is_done());
        assert!(t.observe_root().is_err());
        assert!(t.send_destination_done().await.unwrap());
        assert!(!t.send_destination_done().await.unwrap());
    }
    #[tokio::test]
    async fn empty_created_directory_is_removed_only_after_seal() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let mut t = new_tracker();
        t.set_root_parent_dir(open_dir(tmp.path()).await);
        let admission = t.admit_directory(&root, true).unwrap();
        t.register_directory(admission, open_dir(&root).await, meta(), true, false, None)
            .unwrap();
        t.mark_announced(&root).await.unwrap();
        assert!(root.exists());
        t.seal_directory(&root, 0).await.unwrap();
        assert!(!root.exists());
    }
}
