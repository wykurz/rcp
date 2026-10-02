//! Source directory admission with one pipelined depth-first reserve lane.

use remote::protocol::{DirectoryClass, DirectoryLimits};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, thiserror::Error)]
#[error(
    "reserved directory depth exceeds the negotiated capacity of {capacity} held levels within one reserved subtree, not the total source path depth; reduce --max-connections or --max-files-in-flight, or configure a higher inherited hard file limit on both endpoints (ulimit -H -n / SSH session limits)"
)]
pub(super) struct ReservedDepthExhausted {
    capacity: usize,
}

pub(super) struct DirectoryBudget {
    pub(super) normal: Arc<Semaphore>,
    pub(super) reserve: Arc<Semaphore>,
    pub(super) lane: Arc<Semaphore>,
    reserve_capacity: usize,
}

pub(super) enum Scan {
    Normal(OwnedSemaphorePermit),
    Reserve {
        owner: Arc<ReservedScan>,
        depth: usize,
    },
}

#[derive(Debug)]
pub(super) struct ReservedScan {
    // active continuations retain the scan slot; directory lifetimes retain only the lane.
    _lane: Arc<OwnedSemaphorePermit>,
    _scan: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(super) struct DirectoryCredit {
    _permit: OwnedSemaphorePermit,
    _reserve: Option<Arc<OwnedSemaphorePermit>>,
}

pub(super) struct DirectoryAdmission {
    credit: Arc<DirectoryCredit>,
    scan: Option<Scan>,
}

impl DirectoryBudget {
    pub(super) fn new(normal: usize, reserve: usize) -> Self {
        assert!(normal > 0 && reserve > 0);
        Self {
            normal: Arc::new(Semaphore::new(normal)),
            reserve: Arc::new(Semaphore::new(reserve)),
            lane: Arc::new(Semaphore::new(1)),
            reserve_capacity: reserve,
        }
    }

    pub(super) fn for_endpoint(
        admission: common::EndpointAdmission,
        files_source: common::FilesInFlightSource,
        streams: usize,
        pending: usize,
        receiver: DirectoryLimits,
    ) -> anyhow::Result<(Self, usize, usize)> {
        let limits =
            DirectoryLimits::for_endpoint(admission, streams, pending)?.intersect(receiver);
        let normal = limits.normal.get();
        anyhow::ensure!(
            normal >= 2,
            "normal directory capacity must leave room for both a scan and a pending file parent"
        );
        let local = match admission {
            common::EndpointAdmission::Remote(resources) => resources.leaf_capacity.get(),
            common::EndpointAdmission::Disabled => streams,
            common::EndpointAdmission::Configured { .. } => {
                unreachable!("local admission cannot authorize remote discovery")
            }
        };
        let scans = local.min(normal / 2);
        let files = pending.min(normal - scans);
        if scans < streams || files < pending {
            match files_source {
                common::FilesInFlightSource::Automatic => tracing::info!(
                    "Directory capacity reduced source work: scans={streams}->{scans}, pending_files={pending}->{files}, normal_directories={normal}, reserved_depth={}",
                    limits.reserve
                ),
                common::FilesInFlightSource::Explicit
                | common::FilesInFlightSource::DeprecatedMaxOpenFiles => {
                    tracing::warn!(target: common::NOTICE_TARGET,
                        "Directory capacity reduced source work: scans={streams}->{scans}, pending_files={pending}->{files}, normal_directories={normal}, reserved_depth={}",
                        limits.reserve);
                }
            }
        }
        Ok((Self::new(normal, limits.reserve.get()), scans, files))
    }

    async fn reserve_level(
        &self,
        owner: Arc<ReservedScan>,
        depth: usize,
    ) -> anyhow::Result<DirectoryAdmission> {
        let permit = match self.reserve.clone().try_acquire_owned() {
            Err(error @ tokio::sync::TryAcquireError::Closed) => return Err(error.into()),
            _ if depth > self.reserve_capacity => {
                return Err(ReservedDepthExhausted {
                    capacity: self.reserve_capacity,
                }
                .into());
            }
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                // the inline traversal holds only depth - 1 ancestor credits. Other holders
                // have ended and their files/replies can finish without this scanner advancing.
                common::timing_scope!("source.directory.wait_release")
                    .measure(self.reserve.clone().acquire_owned())
                    .await?
            }
        };
        Ok(DirectoryAdmission {
            credit: Arc::new(DirectoryCredit {
                _permit: permit,
                _reserve: Some(owner._lane.clone()),
            }),
            scan: Some(Scan::Reserve { owner, depth }),
        })
    }

    pub(super) async fn admit(&self, scan: Scan) -> anyhow::Result<DirectoryAdmission> {
        match scan {
            Scan::Reserve { owner, depth } => self.reserve_level(owner, depth).await,
            Scan::Normal(scan) => {
                // a reserve owner keeps its existing scan slot; it never waits for workers
                // parked on normal directory credits to return one.
                tokio::select! {
                    biased;
                    credit = self.normal.clone().acquire_owned() => Ok(DirectoryAdmission {
                        credit: Arc::new(DirectoryCredit { _permit: credit?, _reserve: None }),
                        scan: Some(Scan::Normal(scan)),
                    }),
                    lane = self.lane.clone().acquire_owned() => self.reserve_level(
                        Arc::new(ReservedScan { _lane: Arc::new(lane?), _scan: scan }), 1,
                    ).await,
                }
            }
        }
    }
}

impl DirectoryAdmission {
    pub(super) fn credit(&self) -> Arc<DirectoryCredit> {
        self.credit.clone()
    }

    pub(super) fn class(&self) -> DirectoryClass {
        if self.sequential() {
            DirectoryClass::Reserve
        } else {
            DirectoryClass::Normal
        }
    }

    pub(super) fn sequential(&self) -> bool {
        matches!(self.scan, Some(Scan::Reserve { .. }))
    }

    pub(super) fn try_fork(&self, branches: &Arc<Semaphore>) -> anyhow::Result<Option<Scan>> {
        if self.sequential() {
            return Ok(None);
        }
        match branches.clone().try_acquire_owned() {
            Ok(permit) => Ok(Some(Scan::Normal(permit))),
            Err(tokio::sync::TryAcquireError::NoPermits) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn descend(&mut self) -> Scan {
        match self.scan.as_ref().expect("an active scan owns admission") {
            Scan::Reserve { owner, depth } => Scan::Reserve {
                owner: owner.clone(),
                depth: depth + 1,
            },
            Scan::Normal(_) => self.scan.take().unwrap(),
        }
    }

    pub(super) async fn resume(&mut self, branches: &Arc<Semaphore>) -> anyhow::Result<()> {
        if self.scan.is_none() {
            self.scan = Some(Scan::Normal(branches.clone().acquire_owned().await?));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;

    fn endpoint(soft: Option<u64>, streams: usize, pending: usize) -> common::EndpointAdmission {
        common::EndpointAdmission::Remote(
            common::RemoteResources::for_limits(
                soft.and_then(std::num::NonZeroU64::new),
                std::num::NonZeroUsize::new(streams).unwrap(),
                std::num::NonZeroUsize::new(streams).unwrap(),
                std::num::NonZeroUsize::new(pending).unwrap(),
            )
            .unwrap(),
        )
    }

    fn wide_limits() -> DirectoryLimits {
        DirectoryLimits {
            normal: std::num::NonZeroUsize::new(Semaphore::MAX_PERMITS).unwrap(),
            reserve: std::num::NonZeroUsize::new(Semaphore::MAX_PERMITS).unwrap(),
        }
    }

    #[test]
    fn automatic_source_capacity_reduction_is_info_but_explicit_requests_are_notices()
    -> anyhow::Result<()> {
        type Events = Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>;
        struct Capture(Events);
        impl tracing::Subscriber for Capture {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
                tracing::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
            fn event(&self, event: &tracing::Event<'_>) {
                let metadata = event.metadata();
                let source_target = module_path!().strip_suffix("::tests").unwrap();
                if metadata.target() != source_target && metadata.target() != common::NOTICE_TARGET
                {
                    return;
                }
                self.0
                    .lock()
                    .unwrap()
                    .push((*metadata.level(), metadata.target().to_owned()));
            }
            fn enter(&self, _: &tracing::Id) {}
            fn exit(&self, _: &tracing::Id) {}
        }
        for source in [
            common::FilesInFlightSource::Automatic,
            common::FilesInFlightSource::Explicit,
            common::FilesInFlightSource::DeprecatedMaxOpenFiles,
        ] {
            let events = Events::default();
            let (budget, scans, pending) =
                tracing::subscriber::with_default(Capture(events.clone()), || {
                    DirectoryBudget::for_endpoint(
                        endpoint(Some(1024), 40, 160),
                        source,
                        40,
                        160,
                        wide_limits(),
                    )
                })?;
            assert_eq!(budget.normal.available_permits(), 198);
            assert_eq!(budget.reserve.available_permits(), 198);
            assert_eq!((scans, pending), (40, 158));
            let events = events.lock().unwrap();
            assert_eq!(events.len(), 1, "one reduction should emit one event");
            let (level, target) = &events[0];
            match source {
                common::FilesInFlightSource::Automatic => {
                    assert_eq!(*level, tracing::Level::INFO);
                    assert_ne!(target, common::NOTICE_TARGET);
                }
                common::FilesInFlightSource::Explicit
                | common::FilesInFlightSource::DeprecatedMaxOpenFiles => {
                    assert_eq!(*level, tracing::Level::WARN);
                    assert_eq!(target, common::NOTICE_TARGET);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn reserved_depth_exhaustion_returns_an_error_without_waiting() -> anyhow::Result<()> {
        let budget = DirectoryBudget::new(1, 1);
        let scans = Arc::new(Semaphore::new(1));
        let mut root = budget
            .admit(Scan::Normal(scans.clone().acquire_owned().await?))
            .await?;
        let mut child = budget.admit(root.descend()).await?;
        assert!(child.sequential());
        let result = budget
            .admit(child.descend())
            .now_or_never()
            .expect("reserve depth exhaustion must not wait on its own ancestors");
        assert!(
            result.is_err(),
            "a second reserved level escaped its one-level allowance"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reserved_scan_returns_before_its_last_lifetime_releases() -> anyhow::Result<()> {
        let budget = DirectoryBudget::new(1, 2);
        let scans = Arc::new(Semaphore::new(1));
        let mut normal = budget
            .admit(Scan::Normal(scans.clone().acquire_owned().await?))
            .await?;
        let mut root = budget.admit(normal.descend()).await?;
        let child = budget.admit(root.descend()).await?;
        let peer_owner = root.credit();
        let local_owner = root.credit();
        drop(root);
        assert_eq!(
            scans.available_permits(),
            0,
            "an active reserved descendant still needs its guaranteed scan slot"
        );
        drop(child);
        assert_eq!(
            scans.available_permits(),
            1,
            "the last scan must return its slot before ended directory lifetimes release"
        );
        assert_eq!(budget.lane.available_permits(), 0);
        assert_eq!(budget.reserve.available_permits(), 1);

        let next = budget.admit(Scan::Normal(scans.clone().acquire_owned().await?));
        tokio::pin!(next);
        assert!(
            next.as_mut().now_or_never().is_none(),
            "returning the scan slot must not admit another reserved root"
        );
        drop(peer_owner);
        assert!(
            next.as_mut().now_or_never().is_none(),
            "a local descriptor alias must retain the reserve lane after peer release"
        );
        drop(local_owner);
        let next = next
            .now_or_never()
            .expect("the last lifetime release must admit the next reserved root")?;
        assert!(next.sequential());
        assert_eq!(budget.lane.available_permits(), 0);
        drop(next);
        assert_eq!(scans.available_permits(), 1);
        assert_eq!(budget.lane.available_permits(), 1);
        assert_eq!(budget.reserve.available_permits(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn reserved_sibling_waits_for_both_peer_and_local_lifetime_owners() -> anyhow::Result<()>
    {
        let budget = DirectoryBudget::new(1, 2);
        let scans = Arc::new(Semaphore::new(1));
        let mut normal = budget
            .admit(Scan::Normal(scans.acquire_owned().await?))
            .await?;
        let mut root = budget.admit(normal.descend()).await?;
        let previous = budget.admit(root.descend()).await?;
        let peer_owner = previous.credit();
        let local_owner = previous.credit();
        drop(previous);
        let next = budget.admit(root.descend());
        tokio::pin!(next);
        assert!(
            next.as_mut().now_or_never().is_none(),
            "an ended sibling must cause backpressure, not ancestor-depth exhaustion"
        );
        drop(peer_owner);
        assert!(
            next.as_mut().now_or_never().is_none(),
            "peer release must not discard a held local descriptor's credit"
        );
        drop(local_owner);
        let next = next
            .now_or_never()
            .expect("closing the last sibling owner must admit its successor")?;
        assert!(next.sequential());
        assert_eq!(budget.reserve.available_permits(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn closing_reserve_admission_wakes_a_sibling_credit_waiter() -> anyhow::Result<()> {
        let budget = DirectoryBudget::new(1, 2);
        let scans = Arc::new(Semaphore::new(1));
        let mut normal = budget
            .admit(Scan::Normal(scans.acquire_owned().await?))
            .await?;
        let mut root = budget.admit(normal.descend()).await?;
        let previous = budget.admit(root.descend()).await?;
        let held = previous.credit();
        drop(previous);
        let next = budget.admit(root.descend());
        tokio::pin!(next);
        assert!(
            next.as_mut().now_or_never().is_none(),
            "an ended sibling must wait while its lifetime is still owned"
        );
        budget.reserve.close();
        let error = match next
            .now_or_never()
            .expect("closed admission must wake its waiter")
        {
            Ok(_) => anyhow::bail!("closed reserve admitted another sibling"),
            Err(error) => error,
        };
        assert!(error.is::<tokio::sync::AcquireError>());
        assert!(!error.is::<ReservedDepthExhausted>());
        drop(held);
        Ok(())
    }

    #[tokio::test]
    async fn constrained_endpoint_leaves_scan_credits_after_all_pending_file_parents()
    -> anyhow::Result<()> {
        for (source_limit, destination_limit, streams) in [
            (1024, 1024, 100),
            (128, 1024, 20),
            (1024, 128, 20),
            (160, 160, 20),
        ] {
            let pending = streams * 4;
            let receiver = DirectoryLimits::for_endpoint(
                endpoint(Some(destination_limit), streams, pending),
                streams,
                pending,
            )?;
            let (budget, scans, pending) = DirectoryBudget::for_endpoint(
                endpoint(Some(source_limit), streams, pending),
                common::FilesInFlightSource::Automatic,
                streams,
                pending,
                receiver,
            )?;
            let slots = Arc::new(Semaphore::new(scans));
            let mut parents = Vec::new();
            for _ in 0..pending {
                let admission = budget
                    .admit(Scan::Normal(slots.clone().acquire_owned().await?))
                    .await?;
                assert!(!admission.sequential());
                parents.push(admission.credit());
            }
            let mut active = Vec::new();
            for _ in 0..scans {
                let admission = budget
                    .admit(Scan::Normal(slots.clone().acquire_owned().await?))
                    .await?;
                assert!(
                    !admission.sequential(),
                    "file parents forced all scans into reserve"
                );
                active.push(admission);
            }
            assert_eq!(budget.lane.available_permits(), 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn pending_files_do_not_force_scans_into_reserve_with_descriptor_headroom()
    -> anyhow::Result<()> {
        for soft in [1024, 1_048_576] {
            for streams in [4, 8] {
                let pending = streams * 4;
                let (budget, _, _) = DirectoryBudget::for_endpoint(
                    endpoint(Some(soft), streams, pending),
                    common::FilesInFlightSource::Automatic,
                    streams,
                    pending,
                    wide_limits(),
                )?;
                let scans = Arc::new(Semaphore::new(streams));
                let mut parents = Vec::new();
                for _ in 0..pending {
                    let admission = budget
                        .admit(Scan::Normal(scans.clone().acquire_owned().await?))
                        .await?;
                    parents.push(admission.credit());
                    drop(admission);
                }
                let mut active = Vec::new();
                for _ in 0..streams {
                    let admission = budget
                        .admit(Scan::Normal(scans.clone().acquire_owned().await?))
                        .await?;
                    assert!(
                        !admission.sequential(),
                        "{pending} pinned file parents forced reserve at soft={soft}, E={streams}"
                    );
                    active.push(admission);
                }
                assert_eq!(budget.lane.available_permits(), 1);
                drop(active);
                drop(parents);
                assert_eq!(scans.available_permits(), streams);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn eighty_distinct_file_parents_leave_normal_scan_headroom() -> anyhow::Result<()> {
        static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
            std::sync::LazyLock::new(common::progress::Progress::new);
        let fixture = tempfile::tempdir()?;
        let root = common::safedir::Dir::open_root_dir(fixture.path(), false, common::Side::Source)
            .await?;
        let (budget, _, _) = DirectoryBudget::for_endpoint(
            endpoint(Some(1024), 20, 80),
            common::FilesInFlightSource::Automatic,
            20,
            80,
            wide_limits(),
        )?;
        let scans = Arc::new(Semaphore::new(20));
        let mut parents = Vec::new();
        for index in 0..80 {
            let name = format!("parent-{index}");
            tokio::fs::create_dir(fixture.path().join(&name)).await?;
            common::filegen::write_file(
                &PROGRESS,
                fixture.path().join(&name).join("file"),
                1,
                1,
                0,
            )
            .await
            .map_err(|error| error.source)?;
            let admission = budget
                .admit(Scan::Normal(scans.clone().acquire_owned().await?))
                .await?;
            assert!(
                !admission.sequential(),
                "pending file parent {index} consumed reserve"
            );
            parents.push(
                root.open_dir_admitted(std::ffi::OsStr::new(&name), admission.credit())
                    .await?,
            );
            drop(admission);
        }
        let mut active = Vec::new();
        for _ in 0..20 {
            let admission = budget
                .admit(Scan::Normal(scans.clone().acquire_owned().await?))
                .await?;
            assert!(
                !admission.sequential(),
                "pending file parents starved normal scanning"
            );
            active.push(admission);
        }
        assert_eq!(budget.lane.available_permits(), 1);
        drop(active);
        assert_eq!(budget.normal.available_permits(), 143);
        drop(parents);
        assert_eq!(budget.normal.available_permits(), 223);
        assert_eq!(scans.available_permits(), 20);
        Ok(())
    }

    #[test]
    fn receiver_limits_bound_both_directory_classes() -> anyhow::Result<()> {
        let receiver = DirectoryLimits {
            normal: std::num::NonZeroUsize::new(3).unwrap(),
            reserve: std::num::NonZeroUsize::new(7).unwrap(),
        };
        let (budget, _, _) = DirectoryBudget::for_endpoint(
            endpoint(Some(1024), 20, 80),
            common::FilesInFlightSource::Automatic,
            20,
            80,
            receiver,
        )?;
        assert_eq!(budget.normal.available_permits(), 3);
        assert_eq!(budget.reserve.available_permits(), 7);
        Ok(())
    }

    #[test]
    fn missing_descriptor_headroom_keeps_finite_logical_work_capacity() {
        for admission in [common::EndpointAdmission::Disabled, endpoint(None, 20, 80)] {
            let (budget, _, _) = DirectoryBudget::for_endpoint(
                admission,
                common::FilesInFlightSource::Automatic,
                20,
                80,
                wide_limits(),
            )
            .unwrap();
            assert_eq!(budget.normal.available_permits(), 200);
            assert_eq!(budget.reserve.available_permits(), 200);
        }
    }

    #[tokio::test]
    async fn reserve_descendants_charge_each_level_without_reacquiring_the_lane()
    -> anyhow::Result<()> {
        let budget = DirectoryBudget::new(1, 2);
        let scans = Arc::new(Semaphore::new(1));
        let mut root = budget
            .admit(Scan::Normal(scans.clone().acquire_owned().await?))
            .await?;
        let held_root = root.credit();
        let mut child = budget.admit(root.descend()).await?;
        assert!(child.sequential());
        assert!(child.try_fork(&scans)?.is_none());
        let grandchild = budget.admit(child.descend()).await?;
        assert!(grandchild.sequential());
        assert_eq!(budget.normal.available_permits(), 0);
        assert_eq!(budget.reserve.available_permits(), 0);
        assert_eq!(scans.available_permits(), 0);
        let held_child = child.credit();
        drop(grandchild);
        assert_eq!(budget.reserve.available_permits(), 1);
        drop(child);
        assert_eq!(
            budget.lane.available_permits(),
            0,
            "a reserved descriptor alias still owns the lane"
        );
        drop(held_child);
        assert_eq!(budget.reserve.available_permits(), 2);
        assert_eq!(budget.lane.available_permits(), 1);
        root.resume(&scans).await?;
        drop(root);
        assert_eq!(
            budget.normal.available_permits(),
            0,
            "a parent alias still owns credit"
        );
        drop(held_root);
        assert_eq!(budget.normal.available_permits(), 1);
        assert_eq!(budget.reserve.available_permits(), 2);
        assert_eq!(scans.available_permits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn closing_both_directory_gates_wakes_a_saturated_worker() -> anyhow::Result<()> {
        let budget = DirectoryBudget::new(1, 1);
        let scans = Arc::new(Semaphore::new(3));
        let normal = budget
            .admit(Scan::Normal(scans.clone().acquire_owned().await?))
            .await?;
        let reserve = budget
            .admit(Scan::Normal(scans.clone().acquire_owned().await?))
            .await?;
        let waiting = budget.admit(Scan::Normal(scans.clone().acquire_owned().await?));
        tokio::pin!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
        budget.normal.close();
        budget.reserve.close();
        budget.lane.close();
        assert!(waiting.await.is_err());
        assert_eq!(scans.available_permits(), 1);
        drop(normal);
        drop(reserve);
        assert_eq!(scans.available_permits(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn inline_scan_can_release_its_ancestors_slot_while_descendants_remain()
    -> anyhow::Result<()> {
        let budget = DirectoryBudget::new(3, 3);
        let scans = Arc::new(Semaphore::new(2));
        let mut ancestor = budget
            .admit(Scan::Normal(scans.clone().acquire_owned().await?))
            .await?;
        let child = budget.admit(ancestor.descend()).await?;
        let fork = child
            .try_fork(&scans)?
            .expect("the second scan slot is free");
        let descendant = budget.admit(fork).await?;
        assert_eq!(scans.available_permits(), 0);
        drop(child);
        assert_eq!(
            scans.available_permits(),
            1,
            "EOF must release the moved slot even while a forked descendant remains"
        );
        ancestor.resume(&scans).await?;
        drop(ancestor);
        drop(descendant);
        assert_eq!(scans.available_permits(), 2);
        Ok(())
    }
}
