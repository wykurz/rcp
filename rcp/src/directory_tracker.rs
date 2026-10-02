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
//! A Begin reserves its identity before filesystem work. Secured creation publishes its held
//! descriptor and retains metadata and lockdown ownership without resetting early counts.
//! Finalization requires
//! Ready to have flushed, End to have sealed the child count, and every child to have finished.
//! Child directories contribute once after their own finalization. Rejected subtrees retain only
//! protocol bookkeeping; their descendants never contribute to an unrelated parent.
//!
//! All child writes resolve through held parent descriptors. Metadata uses the held directory
//! descriptor, while empty-directory cleanup acts by name through its held parent.

use crate::receiver_shutdown::ReceiverShutdown;
use anyhow::Context as _;
use common::safedir::Dir;
use std::sync::Arc;

/// An accepted directory, with resource ownership transferred out during finalization.
#[derive(Debug)]
// keep active records inline instead of adding an allocation to every directory lifetime
#[allow(clippy::large_enum_variant)]
enum DirectoryRecord {
    Pending(DirectoryState),
    Finalizing,
}
impl DirectoryRecord {
    fn pending(&self) -> Option<&DirectoryState> {
        match self {
            Self::Pending(state) => Some(state),
            Self::Finalizing => None,
        }
    }
    fn pending_mut(&mut self) -> Option<&mut DirectoryState> {
        match self {
            Self::Pending(state) => Some(state),
            Self::Finalizing => None,
        }
    }
}

/// State for a single directory waiting for creation, Ready, End, or child completion.
#[derive(Debug)]
struct DirectoryState {
    /// The final child count is available only after End.
    discovery: DiscoveryState,
    creation: tokio::sync::watch::Receiver<DirectoryCreation>,
    /// Completed direct-child obligations.
    entries_processed: usize,
    /// Direct-child directory names that entered finalization, retained only until this parent
    /// completes or discovery ends. They reject repeated Begins even if finalization failed.
    completed_directories: std::collections::HashSet<std::ffi::OsString>,
    phase: DirectoryPhase,
}

/// Secured resources become eligible for finalization only after Ready has flushed.
#[derive(Debug)]
enum DirectoryPhase {
    Preparing,
    Secured(DirectoryRegistration),
    Announced(DirectoryRegistration),
}

#[derive(Debug)]
enum DiscoveryState {
    Discovering,
    Sealed { expected: usize },
}
impl DirectoryState {
    fn ready_to_finalize(&self) -> bool {
        matches!(self.phase, DirectoryPhase::Announced(_))
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

/// Secured parent publication, independent of Ready and subtree completion.
#[derive(Clone, Debug)]
pub(super) enum DirectoryCreation {
    Pending,
    Created(Arc<Dir>),
    Rejected,
}

/// A sibling already owns teardown; this refusal must not compete with its primary failure.
#[derive(Debug, thiserror::Error)]
#[error("directory registration during teardown")]
pub(super) struct DirectoryRegistrationDuringTeardown;

/// An admitted Begin whose root claim precedes destination filesystem work.
/// Registration or rejection consumes its original destination identity.
#[must_use]
pub(super) struct DirectoryAdmission {
    dst: std::path::PathBuf,
    is_root: bool,
    publication: tokio::sync::watch::Sender<DirectoryCreation>,
    claim: FinalizationClaim,
}

/// Tracks directory entry counts and completion state for remote copy operations.
pub(super) struct DirectoryTracker {
    /// Accepted directories preparing, awaiting completion, or owned by a finalization job.
    directories: std::collections::HashMap<std::path::PathBuf, DirectoryRecord>,
    /// Exact rejected Begins and End bookkeeping, retained through discovery even after their
    /// accepted parent completes. Each descendant Begin still requires its own End.
    rejected_directories: std::collections::HashMap<std::path::PathBuf, RejectedDirectory>,
    /// Minimal rejected subtree roots, retained for late file outcomes after discovery.
    failed_subtrees: std::collections::HashSet<std::path::PathBuf>,
    #[cfg(test)]
    finalization_gate: Option<Arc<tokio::sync::Semaphore>>,
    root_observed: bool,
    /// Independent of rejected history, which DiscoveryComplete compacts before replies may flush.
    pending_acknowledgements: std::collections::HashSet<std::path::PathBuf>,
    /// open `Dir` fd for the root directory's PARENT (the trusted user-specified
    /// destination parent, opened once via `open_parent_dir`). Held so the root
    /// directory's own empty-directory cleanup can `rmdir_at` it through a pinned
    /// parent fd, since the root's parent is itself never a tracked directory.
    root_parent_dir: Option<Arc<Dir>>,
    /// have we received DiscoveryComplete?
    structure_complete: bool,
    /// is the root item complete?
    root_complete: bool,
    /// path of the root directory (if root is a directory)
    root_directory: Option<std::path::PathBuf>,
    /// have we already sent DestinationDone?
    done_phase: DonePhase,
    shutdown: ReceiverShutdown,
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
            directories: std::collections::HashMap::new(),
            rejected_directories: std::collections::HashMap::new(),
            failed_subtrees: std::collections::HashSet::new(),
            #[cfg(test)]
            finalization_gate: None,
            root_observed: false,
            pending_acknowledgements: std::collections::HashSet::new(),
            root_parent_dir: None,
            structure_complete: false,
            root_complete: false,
            root_directory: None,
            done_phase: DonePhase::Open,
            shutdown: ReceiverShutdown::default(),
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
    /// Look up a tracked directory's held `Arc<Dir>` by destination path.
    ///
    /// The returned Arc is a clone (a refcount bump under the tracker lock); the
    /// caller releases the lock and then performs the fd-relative syscall, so the
    /// lock is never held across a syscall and the fd stays alive for the operation
    /// even if the directory completes and is dropped from the map meanwhile.
    pub fn get_dir(&self, dst: &std::path::Path) -> Option<Arc<Dir>> {
        let state = self.directories.get(dst)?.pending()?;
        match &state.phase {
            DirectoryPhase::Preparing => None,
            DirectoryPhase::Secured(registration) | DirectoryPhase::Announced(registration) => {
                Some(registration.dir.clone())
            }
        }
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
    pub fn set_root_parent_dir(&mut self, dir: Arc<Dir>) -> Option<Arc<Dir>> {
        self.root_parent_dir.replace(dir)
    }
    /// Retain an accepted Begin's descriptor, metadata, and reused-directory lockdown.
    /// Children may arrive before Ready; finalization also waits for End and all children.
    fn register_directory(
        &mut self,
        admission: DirectoryAdmission,
        registration: &mut Option<DirectoryRegistration>,
    ) -> anyhow::Result<()> {
        let DirectoryAdmission {
            dst,
            publication,
            mut claim,
            ..
        } = admission;
        if self.is_closing() {
            return Err(DirectoryRegistrationDuringTeardown.into());
        }
        let state = self
            .directories
            .get_mut(&dst)
            .and_then(DirectoryRecord::pending_mut)
            .ok_or_else(|| anyhow::anyhow!("registration without admitted Begin {dst:?}"))?;
        anyhow::ensure!(
            matches!(state.phase, DirectoryPhase::Preparing),
            "directory registration after secured creation {dst:?}"
        );
        let registration = registration
            .take()
            .expect("validated registration resources");
        let dir = registration.dir.clone();
        state.phase = DirectoryPhase::Secured(registration);
        // the fd and unique rollback owner are installed before dependent work can proceed
        publication.send_replace(DirectoryCreation::Created(dir));
        claim.armed = false;
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
        let (publication, creation) = tokio::sync::watch::channel(DirectoryCreation::Pending);
        self.directories.insert(
            dst.to_path_buf(),
            DirectoryRecord::Pending(DirectoryState {
                discovery: DiscoveryState::Discovering,
                creation,
                entries_processed: 0,
                completed_directories: std::collections::HashSet::new(),
                phase: DirectoryPhase::Preparing,
            }),
        );
        self.pending_acknowledgements.insert(dst.to_path_buf());
        if is_root {
            self.root_directory = Some(dst.to_path_buf());
        }
        Ok(DirectoryAdmission {
            dst: dst.to_path_buf(),
            is_root,
            publication,
            claim: FinalizationClaim {
                shutdown: self.shutdown.clone(),
                armed: true,
            },
        })
    }
    /// Subscribe only to an already admitted parent; never resolve a pending parent by pathname.
    pub(super) fn parent_creation(
        &self,
        dst: &std::path::Path,
    ) -> anyhow::Result<tokio::sync::watch::Receiver<DirectoryCreation>> {
        let parent = dst
            .parent()
            .ok_or_else(|| anyhow::anyhow!("entry has no parent"))?;
        if self.in_rejected_subtree(parent) {
            return Ok(tokio::sync::watch::channel(DirectoryCreation::Rejected).1);
        }
        self.directories
            .get(parent)
            .and_then(DirectoryRecord::pending)
            .map(|state| state.creation.clone())
            .ok_or_else(|| anyhow::anyhow!("entry has unknown or completed parent {parent:?}"))
    }

    /// Validate discovery state, directory uniqueness, and parent membership.
    fn validate_directory_begin(&self, dst: &std::path::Path, is_root: bool) -> anyhow::Result<()> {
        self.ensure_discovering()?;
        anyhow::ensure!(
            !self.directories.contains_key(dst) && !self.rejected_directories.contains_key(dst),
            "duplicate DirectoryBegin for {dst:?}"
        );
        if !is_root {
            let parent = dst
                .parent()
                .ok_or_else(|| anyhow::anyhow!("directory has no parent"))?;
            if let Some(state) = self
                .directories
                .get(parent)
                .and_then(DirectoryRecord::pending)
            {
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
    fn reject_directory(
        &mut self,
        admission: DirectoryAdmission,
    ) -> anyhow::Result<Option<DirectoryFinalization>> {
        let DirectoryAdmission {
            dst,
            is_root,
            publication,
            mut claim,
        } = admission;
        let state = self
            .directories
            .get(&dst)
            .and_then(DirectoryRecord::pending)
            .ok_or_else(|| anyhow::anyhow!("rejection without admitted Begin {dst:?}"))?;
        anyhow::ensure!(
            matches!(state.phase, DirectoryPhase::Preparing),
            "directory rejection after secured creation {dst:?}"
        );
        let sealed = matches!(state.discovery, DiscoveryState::Sealed { .. });
        self.directories.remove(&dst);
        let dst = dst.as_path();
        if !self.has_failed_ancestor(dst) {
            self.failed_subtrees.insert(dst.to_path_buf());
        }
        if !self.structure_complete {
            self.rejected_directories
                .insert(dst.to_path_buf(), RejectedDirectory { sealed });
        }
        publication.send_replace(DirectoryCreation::Rejected);
        claim.armed = false;
        if is_root {
            self.set_root_complete();
        } else if let Some(parent) = dst.parent() {
            return self.process_child_entry(parent);
        }
        Ok(None)
    }
    /// Seal an accepted or rejected Begin with its final admitted-child count.
    fn seal_directory(
        &mut self,
        dst: &std::path::Path,
        expected: usize,
    ) -> anyhow::Result<Option<DirectoryFinalization>> {
        self.ensure_discovering()?;
        if let Some(state) = self.rejected_directories.get_mut(dst) {
            anyhow::ensure!(!state.sealed, "duplicate DirectoryEnd for {dst:?}");
            state.sealed = true;
            return Ok(None);
        }
        let state = self
            .directories
            .get_mut(dst)
            .and_then(DirectoryRecord::pending_mut)
            .ok_or_else(|| {
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
            return self.claim_finalization(dst);
        }
        Ok(None)
    }
    /// Record one terminal child event, ignoring only known rejected-subtree traffic.
    fn process_file(
        &mut self,
        dst: &std::path::Path,
    ) -> anyhow::Result<Option<DirectoryFinalization>> {
        if self.in_rejected_subtree(dst) {
            return Ok(None);
        }
        let state = self
            .directories
            .get_mut(dst)
            .and_then(DirectoryRecord::pending_mut)
            .ok_or_else(|| {
                anyhow::anyhow!("child outcome for unknown or completed directory {dst:?}")
            })?;
        state.record_child()?;
        if state.ready_to_finalize() {
            self.claim_finalization(dst)
        } else {
            Ok(None)
        }
    }
    /// Record a terminal child outcome.
    fn process_child_entry(
        &mut self,
        dst: &std::path::Path,
    ) -> anyhow::Result<Option<DirectoryFinalization>> {
        self.process_file(dst)
    }
    /// Record Ready after releasing the send lock and evaluate the finalization gate.
    fn mark_announced(
        &mut self,
        dst: &std::path::Path,
    ) -> anyhow::Result<Option<DirectoryFinalization>> {
        let state = self
            .directories
            .get_mut(dst)
            .and_then(DirectoryRecord::pending_mut)
            .ok_or_else(|| anyhow::anyhow!("Ready for unknown or completed directory {dst:?}"))?;
        match state.phase {
            DirectoryPhase::Preparing => anyhow::bail!("Ready before secured creation"),
            DirectoryPhase::Announced(_) => anyhow::bail!("duplicate DirectoryReady for {dst:?}"),
            DirectoryPhase::Secured(_) => {}
        }
        let DirectoryPhase::Secured(registration) =
            std::mem::replace(&mut state.phase, DirectoryPhase::Preparing)
        else {
            unreachable!("validated secured directory");
        };
        state.phase = DirectoryPhase::Announced(registration);
        self.pending_acknowledgements.remove(dst);
        if state.ready_to_finalize() {
            return self.claim_finalization(dst);
        }
        Ok(None)
    }
    /// Reserve finalization without advancing the parent obligation.
    fn claim_finalization(
        &mut self,
        dst: &std::path::Path,
    ) -> anyhow::Result<Option<DirectoryFinalization>> {
        if self.is_closing() {
            return Ok(None);
        }
        let is_root = self.root_directory.as_deref() == Some(dst);
        if !is_root && !self.structure_complete {
            let parent = dst
                .parent()
                .ok_or_else(|| anyhow::anyhow!("finalizing directory has no parent"))?;
            let name = dst
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("finalizing directory has no name"))?;
            let parent_state = self
                .directories
                .get_mut(parent)
                .and_then(DirectoryRecord::pending_mut)
                .ok_or_else(|| {
                    anyhow::anyhow!("finalizing child has no pending parent {parent:?}")
                })?;
            // reserve the identity before removing pending state or awaiting filesystem work.
            // failed metadata must not make a duplicate Begin admissible during fatal teardown.
            parent_state
                .completed_directories
                .insert(name.to_os_string());
        }
        let record = self
            .directories
            .get_mut(dst)
            .expect("claim requires pending directory");
        let DirectoryRecord::Pending(state) =
            std::mem::replace(record, DirectoryRecord::Finalizing)
        else {
            unreachable!("claim requires pending directory");
        };
        let DirectoryPhase::Announced(registration) = state.phase else {
            unreachable!("claim requires announced directory");
        };
        // finalizing records retain only identity. The creation receiver must release its
        // published descriptor alias along with the other pending bookkeeping.
        drop(state.creation);
        Ok(Some(DirectoryFinalization {
            #[cfg(test)]
            gate: self.finalization_gate.clone(),
            claim: FinalizationClaim {
                shutdown: self.shutdown.clone(),
                armed: true,
            },
            dst: dst.to_path_buf(),
            registration,
            parent_dir: if is_root {
                self.root_parent_dir.clone()
            } else {
                dst.parent().and_then(|parent| self.get_dir(parent))
            },
            preserve: self.preserve,
            fail_early: self.fail_early,
            error_collector: self.error_collector.clone(),
        }))
    }
    /// Commit successful finalization and claim any newly eligible parent.
    fn complete_finalization(
        &mut self,
        dst: &std::path::Path,
    ) -> anyhow::Result<Option<DirectoryFinalization>> {
        anyhow::ensure!(
            matches!(self.directories.get(dst), Some(DirectoryRecord::Finalizing)),
            "directory finalization committed twice"
        );
        self.directories.remove(dst);
        if self.root_directory.as_deref() == Some(dst) {
            self.set_root_complete();
            Ok(None)
        } else {
            let parent = dst
                .parent()
                .ok_or_else(|| anyhow::anyhow!("finalized directory has no parent"))?;
            self.process_child_entry(parent)
        }
    }
    /// Mark the root item as complete.
    pub fn set_root_complete(&mut self) {
        self.root_observed = true;
        self.root_complete = true;
    }
    /// Validate discovery completion without waiting for Ready or file payloads.
    pub fn finish_discovery(&mut self, has_root_item: bool) -> anyhow::Result<()> {
        self.ensure_discovering()?;
        anyhow::ensure!(
            has_root_item || !self.root_observed,
            "DiscoveryComplete(false) after an observed root"
        );
        anyhow::ensure!(
            self.directories
                .values()
                .filter_map(DirectoryRecord::pending)
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
        for state in self
            .directories
            .values_mut()
            .filter_map(DirectoryRecord::pending_mut)
        {
            state.completed_directories = std::collections::HashSet::new();
        }
        if !has_root_item {
            self.root_complete = true;
        }
        Ok(())
    }
    /// Whether tracker bookkeeping is complete; lifetime releases and Done remain separate gates.
    pub fn is_done(&self) -> bool {
        self.structure_complete
            && self.directories.is_empty()
            && self.root_complete
            && self.pending_acknowledgements.is_empty()
    }
    /// Whether teardown has been latched, independently of completed stream I/O.
    /// A data worker uses this with [`Self::is_done`] to distinguish teardown closure from truncation.
    pub fn is_closing(&self) -> bool {
        self.shutdown.is_cancelled()
    }
    /// Successful wire completion remains sticky through ordinary cleanup.
    pub fn destination_done_sent(&self) -> bool {
        self.done_phase == DonePhase::Sent
    }
    pub fn transfer_complete(&self) -> bool {
        self.is_done() && self.destination_done_sent()
    }
}

#[derive(Debug)]
struct DirectoryRegistration {
    dir: Arc<Dir>,
    metadata: remote::protocol::Metadata,
    was_created: bool,
    keep_if_empty: bool,
    /// The unique rollback owner for an existing directory locked down under strict resolution.
    /// Fresh directories have no prior ownership or default ACL to restore.
    reused_lock: Option<common::safedir::ReusedDirLock>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DonePhase {
    Open,
    Sending,
    Sent,
    Failed,
}

struct FinalizationClaim {
    shutdown: ReceiverShutdown,
    armed: bool,
}
impl Drop for FinalizationClaim {
    fn drop(&mut self) {
        if self.armed {
            self.shutdown.cancel();
        }
    }
}

/// Owns every filesystem resource until execution and bookkeeping have committed.
struct DirectoryFinalization {
    // drop the cancellation latch before any filesystem/ACL recovery resources
    claim: FinalizationClaim,
    #[cfg(test)]
    gate: Option<Arc<tokio::sync::Semaphore>>,
    dst: std::path::PathBuf,
    registration: DirectoryRegistration,
    parent_dir: Option<Arc<Dir>>,
    preserve: common::preserve::Settings,
    fail_early: bool,
    error_collector: Arc<common::error_collector::ErrorCollector>,
}
impl DirectoryFinalization {
    async fn execute(&mut self) -> anyhow::Result<()> {
        #[cfg(test)]
        if let Some(gate) = &self.gate {
            gate.acquire().await?.forget();
        }
        let dst = self.dst.as_path();
        let dir = &self.registration.dir;
        let metadata = &self.registration.metadata;
        let parent_dir = &self.parent_dir;
        let entry_name = dst.file_name();
        let was_created = self.registration.was_created;
        let keep_if_empty = self.registration.keep_if_empty;
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
        let apply_result = common::timing_scope!(trace, "destination.directory.finalize.metadata")
            .measure(common::safedir::set_reused_dir_metadata_fd(
                &preserve_for_entry,
                metadata,
                acls.as_ref(),
                self.registration.reused_lock.take(),
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
        Ok(())
    }
}

struct DoneSendClaim {
    state: Arc<std::sync::Mutex<DirectoryTracker>>,
    shutdown: ReceiverShutdown,
    armed: bool,
}
impl Drop for DoneSendClaim {
    fn drop(&mut self) {
        if self.armed {
            self.shutdown.cancel();
            // no state guard is held across await or while this claim is dropped
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.done_phase = DonePhase::Failed;
        }
    }
}

struct DoneWriter<'a> {
    // latch cancellation before releasing the writer, so cleanup cannot retry a partial frame
    claim: DoneSendClaim,
    stream: tokio::sync::MutexGuard<'a, remote::streams::BoxedSendStream>,
}

enum FinalizationOrigin {
    Control,
    Announce,
    Data,
}

/// Closure-confined bookkeeping; filesystem and control work is owned outside the mutex.
#[derive(Clone)]
pub(super) struct SharedDirectoryTracker {
    state: Arc<std::sync::Mutex<DirectoryTracker>>,
    shutdown: ReceiverShutdown,
    completion_ready: tokio_util::sync::CancellationToken,
}
impl SharedDirectoryTracker {
    pub(super) fn new(
        control_send_stream: remote::streams::BoxedSharedSendStream,
        preserve: common::preserve::Settings,
        fail_early: bool,
        error_collector: Arc<common::error_collector::ErrorCollector>,
    ) -> Self {
        let state =
            DirectoryTracker::new(control_send_stream, preserve, fail_early, error_collector);
        Self {
            shutdown: state.shutdown.clone(),
            state: Arc::new(std::sync::Mutex::new(state)),
            completion_ready: tokio_util::sync::CancellationToken::new(),
        }
    }
    pub(super) fn shutdown(&self) -> &ReceiverShutdown {
        &self.shutdown
    }
    pub(super) fn with_state<R>(&self, f: impl FnOnce(&mut DirectoryTracker) -> R) -> R {
        let scope = common::timing_scope!(trace, "destination.tracker.access");
        let (result, complete) = {
            let mut state = self.state.lock().expect("directory tracker poisoned");
            let result = f(&mut state);
            (result, state.is_done() && !state.is_closing())
        };
        if complete {
            self.completion_ready.cancel();
        }
        scope.finish();
        result
    }
    async fn finish_finalization(
        &self,
        mut work: Option<DirectoryFinalization>,
        origin: FinalizationOrigin,
    ) -> anyhow::Result<()> {
        if work.is_none() {
            return Ok(());
        }
        let scope = match origin {
            FinalizationOrigin::Control => {
                common::timing_scope!("destination.directory.finalize.control")
            }
            FinalizationOrigin::Announce => {
                common::timing_scope!("destination.directory.finalize.announce")
            }
            FinalizationOrigin::Data => {
                common::timing_scope!("destination.directory.finalize.data")
            }
        };
        scope
            .measure(async {
                while let Some(mut job) = work {
                    job.execute().await?;
                    work = self.with_state(|state| state.complete_finalization(&job.dst))?;
                    job.claim.armed = false;
                }
                Ok(())
            })
            .await
    }
    /// Record a data worker's terminal file outcome.
    pub(super) async fn process_file(&self, dst: &std::path::Path) -> anyhow::Result<()> {
        let work = self.with_state(|state| state.process_file(dst))?;
        self.finish_finalization(work, FinalizationOrigin::Data)
            .await
    }
    /// Record a control message's terminal child outcome.
    pub(super) async fn process_child_entry(&self, dst: &std::path::Path) -> anyhow::Result<()> {
        let work = self.with_state(|state| state.process_child_entry(dst))?;
        self.finish_finalization(work, FinalizationOrigin::Control)
            .await
    }
    pub(super) async fn seal_directory(
        &self,
        dst: &std::path::Path,
        expected: usize,
    ) -> anyhow::Result<()> {
        let work = self.with_state(|state| state.seal_directory(dst, expected))?;
        self.finish_finalization(work, FinalizationOrigin::Control)
            .await
    }
    pub(super) async fn mark_announced(&self, dst: &std::path::Path) -> anyhow::Result<()> {
        let work = self.with_state(|state| state.mark_announced(dst))?;
        self.finish_finalization(work, FinalizationOrigin::Announce)
            .await
    }
    pub(super) async fn reject_directory(
        &self,
        admission: DirectoryAdmission,
        preparation: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> anyhow::Result<()> {
        let work = self.with_state(|state| state.reject_directory(admission))?;
        drop(preparation);
        self.finish_finalization(work, FinalizationOrigin::Announce)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn register_directory(
        &self,
        admission: DirectoryAdmission,
        dir: Arc<Dir>,
        metadata: remote::protocol::Metadata,
        was_created: bool,
        keep_if_empty: bool,
        reused_lock: Option<common::safedir::ReusedDirLock>,
    ) -> anyhow::Result<()> {
        let mut resources = Some(DirectoryRegistration {
            dir,
            metadata,
            was_created,
            keep_if_empty,
            reused_lock,
        });
        self.with_state(|state| state.register_directory(admission, &mut resources))
    }
    #[cfg(test)]
    pub(super) fn gate_finalization(&self, gate: Arc<tokio::sync::Semaphore>) {
        self.with_state(|state| state.finalization_gate = Some(gate));
    }
    pub(super) fn begin_close(&self) {
        self.shutdown.cancel();
    }
    pub(super) async fn close_stream(&self) {
        self.begin_close();
        let send = self.with_state(|state| state.control_send_stream.clone());
        let mut stream = send.lock().await;
        if self.with_state(|state| state.done_phase == DonePhase::Failed) {
            return;
        }
        if let Err(error) = stream.close().await {
            tracing::debug!("Error closing stream during cleanup: {error:#}");
        }
    }
    pub(super) async fn send_directory_skipped(
        &self,
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> anyhow::Result<()> {
        let send = self.with_state(|state| state.control_send_stream.clone());
        let message = remote::protocol::DestinationMessage::DirectorySkipped {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        send.lock()
            .await
            .send_control_message(&message)
            .await
            .context("Failed to send DirectorySkipped")?;
        self.with_state(|state| {
            anyhow::ensure!(
                state.pending_acknowledgements.remove(dst),
                "Skipped without unacknowledged Begin {dst:?}"
            );
            Ok(())
        })
    }
    /// The control receiver alone sends Done; workers only publish logical completion.
    pub(super) async fn send_destination_done(&self) -> anyhow::Result<bool> {
        let Some((send, claim)) = self.with_state(|state| {
            if state.done_phase != DonePhase::Open || state.is_closing() || !state.is_done() {
                return None;
            }
            state.done_phase = DonePhase::Sending;
            Some((
                state.control_send_stream.clone(),
                DoneSendClaim {
                    state: self.state.clone(),
                    shutdown: state.shutdown.clone(),
                    armed: true,
                },
            ))
        }) else {
            return Ok(false);
        };
        let mut writer = DoneWriter {
            claim,
            stream: send.lock().await,
        };
        writer
            .stream
            .send_control_message(&remote::protocol::DestinationMessage::DestinationDone)
            .await?;
        writer.stream.close().await?;
        self.with_state(|state| state.done_phase = DonePhase::Sent);
        writer.claim.armed = false;
        Ok(true)
    }
    pub(super) async fn wait_for_completion(&self) {
        self.completion_ready.cancelled().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use tracing::instrument::WithSubscriber as _;

    #[tokio::test]
    async fn begin_reserves_identity_before_creation() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = shared_tracker();
        let root = tracker
            .with_state(|state| state.admit_directory(tmp.path(), true))
            .unwrap();
        let child_path = tmp.path().join("child");
        let child = tracker
            .with_state(|state| state.admit_directory(&child_path, false))
            .expect("an admitted parent must allow child admission before creation");
        assert!(
            tracker
                .with_state(|state| state.admit_directory(&child_path, false))
                .is_err()
        );
        drop((root, child));
    }
    #[tokio::test]
    async fn premature_ready_leaves_the_directory_admission_usable() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = shared_tracker();
        let admission = tracker
            .with_state(|state| state.admit_directory(tmp.path(), true))
            .unwrap();
        assert!(tracker.mark_announced(tmp.path()).await.is_err());
        tracker
            .register_directory(
                admission,
                open_dir(tmp.path()).await,
                meta(),
                false,
                true,
                None,
            )
            .unwrap();
        tracker.seal_directory(tmp.path(), 0).await.unwrap();
        tracker.mark_announced(tmp.path()).await.unwrap();
        tracker.with_state(|state| {
            state.finish_discovery(true).unwrap();
            assert!(state.is_done());
        });
    }
    #[tokio::test]
    async fn claimed_finalization_releases_tracker_descriptor_aliases() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = shared_tracker();
        let admission = tracker
            .with_state(|state| state.admit_directory(tmp.path(), true))
            .unwrap();
        let dir = open_dir(tmp.path()).await;
        let held = Arc::downgrade(&dir);
        tracker
            .register_directory(admission, dir, meta(), false, true, None)
            .unwrap();
        tracker.mark_announced(tmp.path()).await.unwrap();
        let alias = tracker
            .with_state(|state| state.get_dir(tmp.path()))
            .unwrap();
        let job = tracker
            .with_state(|state| state.seal_directory(tmp.path(), 0))
            .unwrap()
            .unwrap();
        assert!(
            tracker
                .with_state(|state| state.get_dir(tmp.path()))
                .is_none()
        );
        let mut shutdown = Box::pin(tracker.shutdown().cancelled());
        assert!(shutdown.as_mut().now_or_never().is_none());
        drop(job);
        assert!(shutdown.as_mut().now_or_never().is_some());
        assert!(
            held.upgrade().is_some(),
            "an outstanding user still owns its descriptor"
        );
        drop(alias);
        assert!(
            held.upgrade().is_none(),
            "the finalizing record must not retain a descriptor"
        );
        assert!(tracker.with_state(|state| state.is_closing()));
    }
    #[tokio::test]
    async fn creation_after_discovery_preserves_early_child_counts() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = shared_tracker();
        let admission = tracker
            .with_state(|state| state.admit_directory(tmp.path(), true))
            .unwrap();
        tracker.process_child_entry(tmp.path()).await.unwrap();
        tracker.seal_directory(tmp.path(), 1).await.unwrap();
        tracker
            .with_state(|state| state.finish_discovery(true))
            .unwrap();
        assert!(!tracker.with_state(|state| state.is_done()));
        tracker
            .register_directory(
                admission,
                open_dir(tmp.path()).await,
                meta(),
                false,
                true,
                None,
            )
            .unwrap();
        assert!(!tracker.with_state(|state| state.is_done()));
        tracker.mark_announced(tmp.path()).await.unwrap();
        assert!(tracker.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn rejected_acknowledgement_gates_done_after_discovery_compacts_history() {
        let send = mock_stream();
        let tracker = SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        let root = std::path::Path::new("/root");
        let admission = tracker
            .with_state(|state| state.admit_directory(root, true))
            .unwrap();
        tracker.reject_directory(admission, None).await.unwrap();
        tracker.seal_directory(root, 0).await.unwrap();
        tracker
            .with_state(|state| state.finish_discovery(true))
            .unwrap();
        let held = send.lock().await;
        let mut reply = Box::pin(tracker.send_directory_skipped(root, root));
        assert!(reply.as_mut().now_or_never().is_none());
        assert!(
            !tracker.with_state(|state| state.is_done()),
            "an unflushed Skipped must block Done"
        );
        drop(held);
        reply.await.unwrap();
        assert!(tracker.send_destination_done().await.unwrap());
    }

    #[derive(Clone, Default)]
    struct TimingObservation {
        scopes: Arc<std::sync::Mutex<Vec<ObservedScope>>>,
        state: Option<Arc<std::sync::Mutex<DirectoryTracker>>>,
    }
    struct ObservedScope {
        name: &'static str,
        finished: Option<bool>,
    }
    impl TimingObservation {
        fn counts(&self, name: &str) -> (usize, usize, usize) {
            let scopes = self.scopes.lock().unwrap();
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
            if let Some(state) = &self.state {
                assert!(
                    state.try_lock().is_ok(),
                    "timing started while state is locked"
                );
            }
            let mut scopes = self.scopes.lock().unwrap();
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
                if let Some(state) = &self.state {
                    assert!(
                        state.try_lock().is_ok(),
                        "timing recorded while state is locked"
                    );
                }
                self.scopes.lock().unwrap()[id.into_u64() as usize - 1].finished = Some(finished);
            }
        }
        fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::Id) {}
        fn exit(&self, _: &tracing::Id) {}
    }

    #[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
    #[tokio::test]
    async fn cancelled_and_failed_reused_finalizers_restore_default_acl_on_the_held_inode() {
        use std::os::fd::AsFd as _;
        if !common::safedir::openat2_available() {
            return;
        }
        const TEST_NAME: &str = "directory_tracker::tests::cancelled_and_failed_reused_finalizers_restore_default_acl_on_the_held_inode";
        if crate::test_process::run_in_child(TEST_NAME) {
            return;
        }
        common::safedir::enable_strict_operand_resolution();
        let tmp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(tmp.path()).unwrap();
        let path = root.join("reused");
        std::fs::create_dir(&path).unwrap();
        let mut default_acl = 2u32.to_le_bytes().to_vec();
        for (tag, permission) in [(1u16, 7u16), (4, 5), (32, 0)] {
            default_acl.extend_from_slice(&tag.to_le_bytes());
            default_acl.extend_from_slice(&permission.to_le_bytes());
            default_acl.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        let fd = std::fs::File::open(&path).unwrap();
        common::safedir::apply_acls_fd(
            fd.as_fd(),
            common::Side::Destination,
            &common::safedir::Acls {
                access: None,
                default: Some(default_acl.clone()),
            },
            true,
        )
        .await
        .unwrap();
        let parent = open_dir(&root).await;
        let dir = open_dir(&path).await;
        let handle = parent.child(std::ffi::OsStr::new("reused")).await.unwrap();
        let lock = common::safedir::lockdown_reused_dir(&dir, &handle)
            .await
            .unwrap();
        assert!(lock.is_some());
        assert_eq!(dir.read_acls().await.unwrap().default, None);
        let tracker = shared_tracker();
        let admission = tracker
            .with_state(|state| state.admit_directory(&path, true))
            .unwrap();
        tracker
            .register_directory(admission, dir.clone(), meta(), false, true, lock)
            .unwrap();
        tracker.mark_announced(&path).await.unwrap();
        tracker.with_state(|state| {
            state.finalization_gate = Some(Arc::new(tokio::sync::Semaphore::new(0)))
        });
        let mut finalizing = Box::pin(tracker.seal_directory(&path, 0));
        assert!(finalizing.as_mut().now_or_never().is_none());
        std::fs::rename(&path, root.join("held-inode")).unwrap();
        std::fs::create_dir(&path).unwrap();
        drop(finalizing);
        assert_eq!(
            dir.read_acls().await.unwrap().default,
            Some(default_acl.clone())
        );
        assert_eq!(
            open_dir(&path).await.read_acls().await.unwrap().default,
            None
        );
        assert!(tracker.with_state(|state| state.is_closing()));
        assert!(!tracker.with_state(|state| state.is_done()));
        // a later strict finalization error also returns the original ACL through the held inode
        let held_path = root.join("held-inode");
        let handle = parent
            .child(std::ffi::OsStr::new("held-inode"))
            .await
            .unwrap();
        let lock = common::safedir::lockdown_reused_dir(&dir, &handle)
            .await
            .unwrap();
        let tracker = shared_tracker();
        tracker.with_state(|state| {
            state.fail_early = true;
            state.preserve.dir.user_and_time.time = true;
        });
        let admission = tracker
            .with_state(|state| state.admit_directory(&held_path, true))
            .unwrap();
        let mut invalid = meta();
        invalid.mtime_nsec = 1_000_000_000;
        tracker
            .register_directory(admission, dir.clone(), invalid, false, true, lock)
            .unwrap();
        tracker.mark_announced(&held_path).await.unwrap();
        let error = tracker.seal_directory(&held_path, 0).await.unwrap_err();
        assert!(format!("{error:#}").contains("Invalid argument"));
        assert_eq!(
            dir.read_acls().await.unwrap().default,
            Some(default_acl.clone())
        );
        assert!(tracker.with_state(|state| state.is_closing()));
        assert!(!tracker.with_state(|state| state.is_done()));
        // validation failure keeps ownership outside state access, including ACL rollback
        let handle = parent
            .child(std::ffi::OsStr::new("held-inode"))
            .await
            .unwrap();
        let lock = common::safedir::lockdown_reused_dir(&dir, &handle)
            .await
            .unwrap();
        let tracker = shared_tracker();
        let admission = tracker
            .with_state(|state| state.admit_directory(&held_path, true))
            .unwrap();
        tracker.begin_close();
        let mut registration = Some(DirectoryRegistration {
            dir: dir.clone(),
            metadata: meta(),
            was_created: false,
            keep_if_empty: true,
            reused_lock: lock,
        });
        assert!(
            tracker
                .with_state(|state| state.register_directory(admission, &mut registration))
                .is_err()
        );
        assert!(registration.is_some());
        assert_eq!(dir.read_acls().await.unwrap().default, None);
        drop(registration);
        assert_eq!(dir.read_acls().await.unwrap().default, Some(default_acl));
        crate::test_process::completed(TEST_NAME);
    }
    #[tokio::test]
    async fn collected_metadata_failure_commits_but_fail_early_retains_obligation() {
        for fail_early in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let tracker = shared_tracker();
            tracker.with_state(|state| {
                state.preserve.dir.user_and_time.time = true;
                state.fail_early = fail_early;
            });
            let admission = tracker
                .with_state(|state| state.admit_directory(tmp.path(), true))
                .unwrap();
            let mut invalid = meta();
            invalid.mtime_nsec = 1_000_000_000;
            tracker
                .register_directory(
                    admission,
                    open_dir(tmp.path()).await,
                    invalid,
                    false,
                    true,
                    None,
                )
                .unwrap();
            tracker.mark_announced(tmp.path()).await.unwrap();
            let observed = TimingObservation {
                state: Some(tracker.state.clone()),
                ..Default::default()
            };
            let result = tracker
                .seal_directory(tmp.path(), 0)
                .with_subscriber(observed.clone())
                .await;
            assert_eq!(result.is_err(), fail_early);
            assert_eq!(
                observed.counts("destination.directory.finalize.control"),
                (1, 1, 0)
            );
            if let Err(error) = result {
                assert!(format!("{error:#}").contains("Invalid argument"));
            }
            tracker.with_state(|state| {
                state.finish_discovery(true).unwrap();
                assert_eq!(state.is_done(), !fail_early);
                assert_eq!(state.is_closing(), fail_early);
                assert_eq!(state.error_collector.has_errors(), !fail_early);
            });
        }
    }
    #[tokio::test]
    async fn final_metadata_uses_the_held_inode_after_path_replacement() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("root");
        let moved = tmp.path().join("moved");
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let tracker = shared_tracker();
        tracker.with_state(|state| state.preserve = common::preserve::Settings::default());
        register_shared(&tracker, &path, true).await;
        tracker.mark_announced(&path).await.unwrap();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        tracker.with_state(|state| state.finalization_gate = Some(gate.clone()));
        let mut finalizing = Box::pin(tracker.seal_directory(&path, 0));
        assert!(finalizing.as_mut().now_or_never().is_none());
        std::fs::rename(&path, &moved).unwrap();
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        gate.add_permits(1);
        finalizing.await.unwrap();
        assert_eq!(
            std::fs::metadata(&moved).unwrap().permissions().mode() & 0o777,
            0o755
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    #[tokio::test]
    async fn incomplete_file_outcome_finishes_while_control_writer_is_busy() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = shared_tracker();
        register_shared(&tracker, tmp.path(), true).await;
        tracker.mark_announced(tmp.path()).await.unwrap();
        tracker.seal_directory(tmp.path(), 3).await.unwrap();
        tracker
            .with_state(|state| state.finish_discovery(true))
            .unwrap();
        let send = tracker.with_state(|state| state.control_send_stream.clone());
        let _held = send.lock().await;
        let observed = TimingObservation {
            state: Some(tracker.state.clone()),
            ..Default::default()
        };
        let result = async {
            tracker.process_file(tmp.path()).await.unwrap();
            tracker.process_child_entry(tmp.path()).await.unwrap();
        }
        .with_subscriber(observed.clone())
        .now_or_never();
        assert_eq!(
            result,
            Some(()),
            "ordinary file bookkeeping must complete synchronously"
        );
        assert_eq!(
            tracker.with_state(|state| state.directories[tmp.path()]
                .pending()
                .unwrap()
                .entries_processed),
            2
        );
        for origin in ["control", "announce", "data"] {
            assert_eq!(
                observed.counts(&format!("destination.directory.finalize.{origin}")),
                (0, 0, 0)
            );
        }
    }
    #[tokio::test]
    async fn cancelled_done_after_state_poison_latches_failure_without_panicking() {
        let send = mock_stream();
        let tracker = SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        tracker
            .with_state(|state| state.finish_discovery(false))
            .unwrap();
        let held = send.lock().await;
        let mut sender = Box::pin(tracker.send_destination_done());
        assert!(sender.as_mut().now_or_never().is_none());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || tracker.with_state(|_| panic!("injected state panic"))
            ))
            .is_err()
        );
        drop(sender);
        drop(held);
        let state = tracker
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.is_closing());
        assert!(state.done_phase == DonePhase::Failed);
        assert!(!state.transfer_complete());
    }
    fn shared_tracker() -> SharedDirectoryTracker {
        SharedDirectoryTracker::new(
            mock_stream(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        )
    }
    async fn register_shared(t: &SharedDirectoryTracker, path: &std::path::Path, root: bool) {
        let admission = t
            .with_state(|state| state.admit_directory(path, root))
            .unwrap();
        t.register_directory(admission, open_dir(path).await, meta(), false, true, None)
            .unwrap();
    }
    #[tokio::test]
    async fn blocked_finalizer_keeps_parent_pending_while_sibling_progresses() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("first");
        let sibling = tmp.path().join("sibling");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&sibling).unwrap();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let tracker = shared_tracker();
        tracker.with_state(|state| state.preserve = common::preserve::Settings::default());
        register_shared(&tracker, tmp.path(), true).await;
        register_shared(&tracker, &first, false).await;
        register_shared(&tracker, &sibling, false).await;
        tracker.mark_announced(tmp.path()).await.unwrap();
        tracker.seal_directory(tmp.path(), 2).await.unwrap();
        tracker.mark_announced(&first).await.unwrap();
        tracker.mark_announced(&sibling).await.unwrap();
        tracker.seal_directory(&sibling, 1).await.unwrap();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        tracker.with_state(|state| state.finalization_gate = Some(gate.clone()));
        let mut finishing = Box::pin(tracker.seal_directory(&first, 0));
        assert!(finishing.as_mut().now_or_never().is_none());
        tracker.with_state(|state| {
            state.finalization_gate = None;
            assert!(matches!(
                state.directories.get(&first),
                Some(DirectoryRecord::Finalizing)
            ));
            assert_eq!(
                state.directories[tmp.path()]
                    .pending()
                    .unwrap()
                    .entries_processed,
                0
            );
            assert!(state.get_dir(&sibling).is_some());
            assert!(state.admit_directory(&first, false).is_err());
            assert!(state.admit_directory(&first.join("late"), false).is_err());
            assert!(state.mark_announced(&first).is_err());
            assert!(state.seal_directory(&first, 0).is_err());
            assert!(state.process_file(&first).is_err());
            state.finish_discovery(true).unwrap();
        });
        tracker.process_file(&sibling).await.unwrap();
        tracker.with_state(|state| {
            assert_eq!(
                state.directories[tmp.path()]
                    .pending()
                    .unwrap()
                    .entries_processed,
                1
            );
            assert!(!state.is_done());
        });
        assert_eq!(
            std::fs::metadata(tmp.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        gate.add_permits(1);
        finishing.await.unwrap();
        assert_eq!(
            std::fs::metadata(tmp.path()).unwrap().permissions().mode() & 0o777,
            0o755
        );
        assert!(tracker.with_state(|state| state.is_done()));
        assert!(
            tracker
                .with_state(|state| state.complete_finalization(&first))
                .is_err()
        );
    }
    #[tokio::test]
    async fn cancelled_finalizer_latches_teardown_without_settling_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let tracker = shared_tracker();
        register_shared(&tracker, tmp.path(), true).await;
        register_shared(&tracker, &child, false).await;
        tracker.mark_announced(tmp.path()).await.unwrap();
        tracker.mark_announced(&child).await.unwrap();
        tracker.seal_directory(tmp.path(), 1).await.unwrap();
        tracker.with_state(|state| {
            state.finalization_gate = Some(Arc::new(tokio::sync::Semaphore::new(0)))
        });
        let observed = TimingObservation {
            state: Some(tracker.state.clone()),
            ..Default::default()
        };
        let mut finishing = Box::pin(
            tracker
                .seal_directory(&child, 0)
                .with_subscriber(observed.clone()),
        );
        assert!(finishing.as_mut().now_or_never().is_none());
        assert_eq!(
            observed.counts("destination.directory.finalize.control"),
            (1, 0, 0)
        );
        drop(finishing);
        assert_eq!(
            observed.counts("destination.directory.finalize.control"),
            (1, 0, 1)
        );
        tracker.with_state(|state| {
            state.finish_discovery(true).unwrap();
            assert!(state.is_closing());
            assert!(matches!(
                state.directories.get(&child),
                Some(DirectoryRecord::Finalizing)
            ));
            assert_eq!(
                state.directories[tmp.path()]
                    .pending()
                    .unwrap()
                    .entries_processed,
                0
            );
            assert!(!state.is_done());
        });
        assert!(!tracker.send_destination_done().await.unwrap());
    }
    #[tokio::test]
    async fn done_send_is_not_retried_while_in_progress_or_after_success() {
        let send = mock_stream();
        let tracker = SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        tracker
            .with_state(|state| state.finish_discovery(false))
            .unwrap();
        let held = send.lock().await;
        let mut first = Box::pin(tracker.send_destination_done());
        assert!(first.as_mut().now_or_never().is_none());
        assert!(!tracker.send_destination_done().await.unwrap());
        assert!(tracker.with_state(|state| state.is_done()));
        assert!(!tracker.with_state(|state| state.transfer_complete()));
        drop(held);
        assert!(first.await.unwrap());
        assert!(!tracker.send_destination_done().await.unwrap());
        tracker.close_stream().await;
        assert!(tracker.with_state(|state| state.transfer_complete()));
    }
    #[tokio::test]
    async fn logical_completion_notifies_existing_and_late_control_waiters() {
        let tracker = shared_tracker();
        let mut waiting = Box::pin(tracker.wait_for_completion());
        assert!(waiting.as_mut().now_or_never().is_none());
        tracker
            .with_state(|state| state.finish_discovery(false))
            .unwrap();
        assert!(waiting.as_mut().now_or_never().is_some());
        assert!(tracker.wait_for_completion().now_or_never().is_some());
        assert!(tracker.with_state(|state| state.is_done()));
        assert!(!tracker.with_state(|state| state.transfer_complete()));
    }
    #[tokio::test]
    async fn teardown_and_cancelled_done_cannot_retry_the_terminal_send() {
        let send = mock_stream();
        let tracker = SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        tracker
            .with_state(|state| state.finish_discovery(false))
            .unwrap();
        let held = send.lock().await;
        let mut first = Box::pin(tracker.send_destination_done());
        assert!(first.as_mut().now_or_never().is_none());
        let mut close = Box::pin(tracker.close_stream());
        assert!(close.as_mut().now_or_never().is_none());
        assert!(tracker.with_state(|state| state.is_closing()));
        drop(first);
        assert!(!tracker.send_destination_done().await.unwrap());
        drop(held);
        close.await;
        assert!(!tracker.with_state(|state| state.transfer_complete()));
    }
    #[tokio::test]
    async fn stalled_control_reply_leaves_state_accessible() {
        let stream = mock_stream();
        let tracker = SharedDirectoryTracker::new(
            stream.clone(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        let held = stream.lock().await;
        let mut sending = Box::pin(async {
            tracker
                .send_directory_skipped(std::path::Path::new("/src"), std::path::Path::new("/dst"))
                .await
        });
        assert!(sending.as_mut().now_or_never().is_none());
        assert!(
            tracker.state.try_lock().is_ok(),
            "control I/O must not hold tracker state"
        );
        drop(sending);
        drop(held);
    }

    #[tokio::test]
    async fn cancelled_done_is_not_sent() {
        use tokio::io::AsyncReadExt as _;
        let (writer, mut reader) = tokio::io::duplex(1);
        let stream = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(writer) as remote::streams::BoxedWrite,
        )));
        let tracker = SharedDirectoryTracker::new(
            stream,
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        tracker
            .with_state(|state| state.finish_discovery(false))
            .unwrap();
        let mut sending = Box::pin(tracker.send_destination_done());
        assert!(sending.as_mut().now_or_never().is_none());
        let mut byte = [0];
        assert!(
            reader.read_exact(&mut byte).now_or_never().unwrap().is_ok(),
            "the frame must have started before cancellation"
        );
        drop(sending);
        assert!(!tracker.with_state(|state| state.destination_done_sent()));
        assert!(!tracker.send_destination_done().await.unwrap());
        assert!(
            tracker.close_stream().now_or_never().is_some(),
            "cleanup must not retry flushing a cancelled partial Done"
        );
        assert!(reader.read_exact(&mut byte).now_or_never().is_none());
    }

    #[test]
    fn tracker_access_finishes_after_unlock() {
        let tracker = SharedDirectoryTracker::new(
            mock_stream(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        let observed = TimingObservation {
            state: Some(tracker.state.clone()),
            ..Default::default()
        };
        tracing::subscriber::with_default(observed.clone(), || {
            tracker.with_state(|state| state.observe_root()).unwrap();
        });
        assert_eq!(observed.counts("destination.tracker.access"), (1, 1, 0));
        assert!(tracker.with_state(|state| state.root_observed));
    }

    #[tokio::test]
    async fn finalization_scope_identifies_the_completing_event() {
        for (event, origin) in [
            ("ready", "announce"),
            ("end", "control"),
            ("data", "data"),
            ("child", "control"),
            ("rejected", "announce"),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let tracker = shared_tracker();
            register_shared(&tracker, tmp.path(), true).await;
            let observed = TimingObservation {
                state: Some(tracker.state.clone()),
                ..Default::default()
            };
            async {
                match event {
                    "ready" => {
                        tracker.seal_directory(tmp.path(), 0).await.unwrap();
                        tracker.mark_announced(tmp.path()).await.unwrap();
                    }
                    "end" => {
                        tracker.mark_announced(tmp.path()).await.unwrap();
                        tracker.seal_directory(tmp.path(), 0).await.unwrap();
                    }
                    _ => {
                        tracker.mark_announced(tmp.path()).await.unwrap();
                        tracker.seal_directory(tmp.path(), 1).await.unwrap();
                        match event {
                            "data" => tracker.process_file(tmp.path()).await.unwrap(),
                            "child" => tracker.process_child_entry(tmp.path()).await.unwrap(),
                            "rejected" => {
                                let child = tmp.path().join("rejected");
                                let admission = tracker
                                    .with_state(|state| state.admit_directory(&child, false))
                                    .unwrap();
                                tracker.reject_directory(admission, None).await.unwrap();
                                tracker
                                    .send_directory_skipped(&child, &child)
                                    .await
                                    .unwrap();
                                tracker.seal_directory(&child, 0).await.unwrap();
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
            .with_subscriber(observed.clone())
            .await;
            tracker.with_state(|state| {
                state.finish_discovery(true).unwrap();
                assert!(state.is_done(), "{event}");
            });
            for candidate in ["control", "announce", "data"] {
                let expected = if candidate == origin {
                    (1, 1, 0)
                } else {
                    (0, 0, 0)
                };
                assert_eq!(
                    observed.counts(&format!("destination.directory.finalize.{candidate}")),
                    expected,
                    "{event}"
                );
            }
        }
    }

    #[tokio::test]
    async fn child_completion_times_each_directory_in_the_ancestor_cascade() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let tracker = shared_tracker();
        register_shared(&tracker, tmp.path(), true).await;
        tracker.mark_announced(tmp.path()).await.unwrap();
        tracker.seal_directory(tmp.path(), 1).await.unwrap();
        register_shared(&tracker, &child, false).await;
        tracker.mark_announced(&child).await.unwrap();
        let observed = TimingObservation {
            state: Some(tracker.state.clone()),
            ..Default::default()
        };
        tracker
            .seal_directory(&child, 0)
            .with_subscriber(observed.clone())
            .await
            .unwrap();
        tracker.with_state(|state| {
            state.finish_discovery(true).unwrap();
            assert!(state.is_done());
            assert!(state.get_dir(tmp.path()).is_none());
            assert!(state.get_dir(&child).is_none());
        });
        assert_eq!(
            observed.counts("destination.directory.finalize.control"),
            (1, 1, 0)
        );
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
        let tracker = shared_tracker();
        register_shared(&tracker, tmp.path(), true).await;
        let admission = tracker
            .with_state(|state| state.admit_directory(&child, false))
            .unwrap();
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
        assert_eq!(
            tracker.with_state(|state| state.directories[tmp.path()]
                .pending()
                .unwrap()
                .entries_processed),
            1
        );
        assert_eq!(
            observed.counts("destination.directory.finalize.prune"),
            (1, 1, 0)
        );
        assert_eq!(
            observed.counts("destination.directory.finalize.metadata"),
            (0, 0, 0)
        );
    }

    #[tokio::test]
    async fn nonempty_traversal_directory_times_prune_attempt_and_metadata() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("traversal-only");
        std::fs::create_dir(&child).unwrap();
        std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o700)).unwrap();
        let retained = child.join("externally-created");
        std::fs::write(&retained, b"retained contents").unwrap();
        let tracker = shared_tracker();
        tracker.with_state(|state| state.preserve = common::preserve::Settings::default());
        register_shared(&tracker, tmp.path(), true).await;
        let admission = tracker
            .with_state(|state| state.admit_directory(&child, false))
            .unwrap();
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
        assert_eq!(std::fs::read(&retained).unwrap(), b"retained contents");
        assert_eq!(
            std::fs::metadata(&child).unwrap().permissions().mode() & 0o777,
            0o755,
            "metadata still applies after rmdir returns ENOTEMPTY"
        );
        assert!(tracker.with_state(|state| state.get_dir(&child)).is_none());
        assert_eq!(
            tracker.with_state(|state| state.directories[tmp.path()]
                .pending()
                .unwrap()
                .entries_processed),
            1
        );
        assert_eq!(
            observed.counts("destination.directory.finalize.prune"),
            (1, 1, 0),
            "an unsuccessful removal attempt finishes normally"
        );
        assert_eq!(
            observed.counts("destination.directory.finalize.metadata"),
            (1, 1, 0)
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
    fn retained_history(t: &DirectoryTracker) -> usize {
        t.directories
            .values()
            .filter_map(DirectoryRecord::pending)
            .map(|state| state.completed_directories.len())
            .sum::<usize>()
            + t.rejected_directories.len()
            + t.failed_subtrees.len()
    }
    fn register_virtual(t: &SharedDirectoryTracker, path: &std::path::Path, dir: &Arc<Dir>) {
        // these state-machine fixtures reuse one real descriptor: no metadata is preserved and
        // no directory is marked created, so finalization performs no path-dependent mutations.
        let admission = t
            .with_state(|state| state.admit_directory(path, false))
            .unwrap();
        t.register_directory(admission, dir.clone(), meta(), false, true, None)
            .unwrap();
    }
    #[tokio::test]
    async fn failed_finalization_rejects_duplicate_begin_without_settling_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let t = shared_tracker();
        t.with_state(|state| {
            state.fail_early = true;
            state.preserve.dir.user_and_time.time = true;
        });
        register_shared(&t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        t.seal_directory(tmp.path(), 1).await.unwrap();
        let mut invalid_metadata = meta();
        // outside the kernel's nanosecond range and neither UTIME_NOW nor UTIME_OMIT.
        invalid_metadata.mtime_nsec = 1_000_000_000;
        let admission = t
            .with_state(|state| state.admit_directory(&child, false))
            .unwrap();
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
        assert!(t.with_state(|state| matches!(
            state.directories.get(&child),
            Some(DirectoryRecord::Finalizing)
        )));
        assert_eq!(
            t.with_state(|state| state.directories[tmp.path()]
                .pending()
                .unwrap()
                .entries_processed),
            0
        );
        assert!(
            t.with_state(|state| state.admit_directory(&child, false))
                .is_err()
        );
        assert!(t.seal_directory(&child, 0).await.is_err());
        assert!(t.process_file(&child).await.is_err());
        assert!(!t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn completed_subtrees_retain_only_names_under_pending_parents() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = open_dir(tmp.path()).await;
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        for group in 0..24 {
            let branch = tmp.path().join(format!("branch-{group}"));
            register_virtual(&t, &branch, &dir);
            t.mark_announced(&branch).await.unwrap();
            for leaf in 0..32 {
                let leaf = branch.join(format!("leaf-{leaf}"));
                register_virtual(&t, &leaf, &dir);
                t.mark_announced(&leaf).await.unwrap();
                t.seal_directory(&leaf, 0).await.unwrap();
                assert!(
                    t.with_state(|state| state.admit_directory(&leaf, false))
                        .is_err()
                );
            }
            t.seal_directory(&branch, 32).await.unwrap();
            assert_eq!(t.with_state(|state| retained_history(state)), group + 1);
            assert_eq!(t.with_state(|state| state.directories.len()), 1);
            assert!(
                t.with_state(|state| state.admit_directory(&branch, false))
                    .is_err()
            );
            let pruned_leaf = branch.join("leaf-0");
            assert!(
                t.with_state(|state| state.admit_directory(&pruned_leaf, false))
                    .is_err()
            );
            assert!(t.seal_directory(&pruned_leaf, 0).await.is_err());
            assert!(t.mark_announced(&pruned_leaf).await.is_err());
            assert!(t.process_file(&pruned_leaf).await.is_err());
        }
        t.seal_directory(tmp.path(), 24).await.unwrap();
        assert_eq!(t.with_state(|state| retained_history(state)), 0);
        assert!(
            t.with_state(|state| state.admit_directory(tmp.path(), true))
                .is_err()
        );
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert!(t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn discovery_releases_success_history_before_late_files_complete() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = open_dir(tmp.path()).await;
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        let completed = tmp.path().join("completed");
        register_virtual(&t, &completed, &dir);
        t.mark_announced(&completed).await.unwrap();
        t.seal_directory(&completed, 0).await.unwrap();
        let pending = tmp.path().join("pending");
        register_virtual(&t, &pending, &dir);
        t.mark_announced(&pending).await.unwrap();
        t.seal_directory(&pending, 1).await.unwrap();
        t.seal_directory(tmp.path(), 3).await.unwrap();
        assert_eq!(t.with_state(|state| retained_history(state)), 1);
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert_eq!(t.with_state(|state| retained_history(state)), 0);
        assert!(t.with_state(|state| {
            state
                .directories
                .values()
                .filter_map(DirectoryRecord::pending)
                .all(|state| state.completed_directories.capacity() == 0)
        }));
        assert!(
            t.with_state(|state| state.admit_directory(&completed, false))
                .is_err()
        );
        assert!(t.seal_directory(&completed, 0).await.is_err());
        assert!(t.process_file(&completed).await.is_err());
        t.process_file(&pending).await.unwrap();
        assert_eq!(t.with_state(|state| retained_history(state)), 0);
        assert!(!t.with_state(|state| state.is_done()));
        t.process_file(tmp.path()).await.unwrap();
        assert!(t.with_state(|state| state.is_done()));
        assert_eq!(t.with_state(|state| retained_history(state)), 0);
    }
    #[tokio::test]
    async fn rejected_descendant_ends_survive_parent_completion_then_compact() {
        let tmp = tempfile::tempdir().unwrap();
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        t.seal_directory(tmp.path(), 1).await.unwrap();
        let rejected = tmp.path().join("rejected");
        reject(&t, &rejected, false).await.unwrap();
        assert!(t.with_state(|state| state.directories.is_empty()));
        t.seal_directory(&rejected, 64).await.unwrap();
        for index in 0..64 {
            let descendant = rejected.join(format!("child-{index}"));
            reject(&t, &descendant, false).await.unwrap();
            assert!(t.with_state(|state| state.finish_discovery(true)).is_err());
            assert!(
                t.with_state(|state| state.admit_directory(&descendant, false))
                    .is_err()
            );
            t.seal_directory(&descendant, 8).await.unwrap();
            assert!(t.seal_directory(&descendant, 8).await.is_err());
        }
        assert!(
            t.with_state(|state| state.admit_directory(&rejected, false))
                .is_err()
        );
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert_eq!(t.with_state(|state| retained_history(state)), 1);
        assert_eq!(
            t.with_state(|state| state.rejected_directories.capacity()),
            0
        );
        assert!(t.with_state(|state| state.is_done()));
        let late = rejected.join("child-0/late-file");
        assert!(t.with_state(|state| state.has_failed_ancestor(&late)));
        t.process_file(&rejected).await.unwrap();
        assert!(t.with_state(|state| state.is_done() && state.directories.is_empty()));
        assert_eq!(t.with_state(|state| retained_history(state)), 1);
        t.process_child_entry(&late).await.unwrap();
        assert!(t.process_file(tmp.path()).await.is_err());
        assert!(t.process_file(&tmp.path().join("unknown")).await.is_err());
        assert!(
            t.with_state(|state| state.admit_directory(&late, false))
                .is_err()
        );
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
    async fn reject(
        t: &SharedDirectoryTracker,
        dst: &std::path::Path,
        root: bool,
    ) -> anyhow::Result<()> {
        let admission = t.with_state(|state| state.admit_directory(dst, root))?;
        t.reject_directory(admission, None).await?;
        t.send_directory_skipped(dst, dst).await
    }
    #[tokio::test]
    async fn unsealed_empty_directory_keeps_its_descriptor() {
        let tmp = tempfile::tempdir().unwrap();
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(
            t.with_state(|state| state.get_dir(tmp.path())).is_some(),
            "Ready alone must not finalize a directory without End"
        );
        assert!(t.with_state(|state| state.finish_discovery(true)).is_err());
        t.seal_directory(tmp.path(), 0).await.unwrap();
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert!(t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn unknown_parent_completion_is_rejected() {
        let t = shared_tracker();
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
            let t = shared_tracker();
            t.with_state(|state| state.preserve = common::preserve::Settings::default());
            register_shared(&t, tmp.path(), true).await;
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
                assert_eq!(
                    t.with_state(|state| state.get_dir(tmp.path())).is_none(),
                    index == 2
                );
            }
            t.with_state(|state| state.finish_discovery(true)).unwrap();
            assert!(t.with_state(|state| state.is_done()));
        }
    }
    #[tokio::test]
    async fn nested_directory_contributes_once_after_finalization() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("child");
        std::fs::create_dir(&child).unwrap();
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.seal_directory(tmp.path(), 1).await.unwrap();
        register_shared(&t, &child, false).await;
        t.seal_directory(&child, 0).await.unwrap();
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        t.mark_announced(&child).await.unwrap();
        assert_eq!(
            t.with_state(|state| state.directories[tmp.path()]
                .pending()
                .unwrap()
                .entries_processed),
            1
        );
        assert!(!t.with_state(|state| state.is_done()));
        assert!(t.mark_announced(&child).await.is_err());
        assert_eq!(
            t.with_state(|state| state.directories[tmp.path()]
                .pending()
                .unwrap()
                .entries_processed),
            1
        );
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn rejected_child_settles_parent_once_and_still_requires_end() {
        let tmp = tempfile::tempdir().unwrap();
        let child = tmp.path().join("rejected");
        let descendant = child.join("descendant");
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.seal_directory(tmp.path(), 1).await.unwrap();
        t.mark_announced(tmp.path()).await.unwrap();
        reject(&t, &child, false).await.unwrap();
        assert!(t.with_state(|state| state.get_dir(tmp.path())).is_none());
        reject(&t, &descendant, false).await.unwrap();
        t.process_file(&child).await.unwrap();
        t.process_child_entry(&descendant).await.unwrap();
        assert!(t.with_state(|state| state.finish_discovery(true)).is_err());
        t.seal_directory(&child, 5).await.unwrap();
        assert!(t.with_state(|state| state.finish_discovery(true)).is_err());
        t.seal_directory(&descendant, 8).await.unwrap();
        assert!(t.seal_directory(&child, 5).await.is_err());
        assert!(reject(&t, &child, false).await.is_err());
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert!(t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn rejected_root_cannot_finish_discovery_without_end() {
        let t = shared_tracker();
        let root = std::path::Path::new("/rejected");
        reject(&t, root, true).await.unwrap();
        assert!(t.with_state(|state| state.finish_discovery(true)).is_err());
        assert!(t.with_state(|state| state.finish_discovery(false)).is_err());
        t.seal_directory(root, 0).await.unwrap();
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert!(t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn duplicate_and_unknown_directory_messages_are_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let t = shared_tracker();
        let unknown = tmp.path().join("unknown");
        assert!(t.seal_directory(&unknown, 0).await.is_err());
        assert!(t.mark_announced(&unknown).await.is_err());
        register_shared(&t, tmp.path(), true).await;
        assert!(
            t.with_state(|state| state.admit_directory(tmp.path(), true))
                .is_err()
        );
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
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.process_file(tmp.path()).await.unwrap();
        assert!(t.seal_directory(tmp.path(), 0).await.is_err());
        t.seal_directory(tmp.path(), 1).await.unwrap();
        assert!(t.process_file(tmp.path()).await.is_err());
        assert_eq!(
            t.with_state(|state| state.directories[tmp.path()]
                .pending()
                .unwrap()
                .entries_processed),
            1
        );
        t.with_state(|state| {
            let directory = state
                .directories
                .get_mut(tmp.path())
                .unwrap()
                .pending_mut()
                .unwrap();
            directory.discovery = DiscoveryState::Discovering;
            directory.entries_processed = usize::MAX;
        });
        assert!(t.process_file(tmp.path()).await.is_err());
        assert_eq!(
            t.with_state(|state| state.directories[tmp.path()]
                .pending()
                .unwrap()
                .entries_processed),
            usize::MAX
        );
    }
    #[tokio::test]
    async fn discovery_can_finish_before_ready_and_final_file_completion() {
        let tmp = tempfile::tempdir().unwrap();
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.seal_directory(tmp.path(), 1).await.unwrap();
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert!(!t.with_state(|state| state.is_done()));
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(!t.with_state(|state| state.is_done()));
        t.process_file(tmp.path()).await.unwrap();
        assert!(t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn discovery_marker_rejects_duplicates_late_structure_and_false_root_claim() {
        let tmp = tempfile::tempdir().unwrap();
        let t = shared_tracker();
        register_shared(&t, tmp.path(), true).await;
        t.seal_directory(tmp.path(), 0).await.unwrap();
        assert!(t.with_state(|state| state.finish_discovery(false)).is_err());
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert!(t.with_state(|state| state.finish_discovery(true)).is_err());
        assert!(t.with_state(|state| state.ensure_discovering()).is_err());
        assert!(
            t.with_state(|state| state.admit_directory(&tmp.path().join("late"), false))
                .is_err()
        );
        assert!(t.seal_directory(tmp.path(), 0).await.is_err());
        t.mark_announced(tmp.path()).await.unwrap();
        assert!(t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn admitted_root_directory_excludes_another_root_before_registration() {
        let t = shared_tracker();
        let _admission = t
            .with_state(|state| state.admit_directory(std::path::Path::new("/root"), true))
            .unwrap();
        assert!(
            t.with_state(|state| state.observe_root()).is_err(),
            "directory admission must reserve the root before filesystem work"
        );
    }
    #[tokio::test]
    async fn root_file_can_arrive_after_discovery() {
        let t = shared_tracker();
        t.with_state(|state| state.finish_discovery(true)).unwrap();
        assert!(!t.with_state(|state| state.is_done()));
        t.with_state(|state| state.observe_root()).unwrap();
        assert!(t.with_state(|state| state.observe_root()).is_err());
        t.with_state(|state| state.set_root_complete());
        assert!(t.with_state(|state| state.is_done()));
    }
    #[tokio::test]
    async fn empty_discovery_completes_once_and_rejects_late_root() {
        let t = shared_tracker();
        t.with_state(|state| state.finish_discovery(false)).unwrap();
        assert!(t.with_state(|state| state.is_done()));
        assert!(t.with_state(|state| state.observe_root()).is_err());
    }
    #[tokio::test]
    async fn empty_created_directory_is_removed_only_after_seal() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let t = shared_tracker();
        let parent = open_dir(tmp.path()).await;
        t.with_state(|state| state.set_root_parent_dir(parent));
        let admission = t
            .with_state(|state| state.admit_directory(&root, true))
            .unwrap();
        t.register_directory(admission, open_dir(&root).await, meta(), true, false, None)
            .unwrap();
        t.mark_announced(&root).await.unwrap();
        assert!(root.exists());
        t.seal_directory(&root, 0).await.unwrap();
        assert!(!root.exists());
    }
}
