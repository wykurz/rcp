//! Source directory admission with one sequential lane for progress under descriptor pressure.

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct DirectoryBudget {
    pub(super) normal: Arc<Semaphore>,
    pub(super) reserve: Arc<Semaphore>,
}

pub(super) enum Scan {
    Normal(OwnedSemaphorePermit),
    Reserve(Arc<ReservedScan>),
}

pub(super) struct ReservedScan {
    credit: Arc<OwnedSemaphorePermit>,
    // retain one scan slot until the entire sequential subtree and its files finish. Reacquiring
    // it could deadlock against normal workers holding every slot while waiting for this reserve.
    _scan: OwnedSemaphorePermit,
}

pub(super) struct DirectoryAdmission {
    credit: Arc<OwnedSemaphorePermit>,
    scan: Option<Scan>,
}

impl DirectoryBudget {
    pub(super) fn new(groups: usize) -> Self {
        assert!(groups > 0);
        Self {
            normal: Arc::new(Semaphore::new(groups)),
            reserve: Arc::new(Semaphore::new(1)),
        }
    }

    pub(super) fn for_process(streams: usize) -> Self {
        let groups = match common::get_soft_open_file_limit() {
            Ok(soft) => directory_groups(soft, streams),
            Err(error) => {
                tracing::warn!(target: common::NOTICE_TARGET,
                    "Cannot query the source descriptor limit; using one normal directory group and a sequential reserve: {error:#}");
                1
            }
        };
        Self::new(groups)
    }

    pub(super) async fn admit(&self, scan: Scan) -> anyhow::Result<DirectoryAdmission> {
        match scan {
            Scan::Reserve(reserve) => Ok(DirectoryAdmission {
                credit: reserve.credit.clone(),
                scan: Some(Scan::Reserve(reserve)),
            }),
            Scan::Normal(scan) => {
                // the caller already owns scan admission. A reserve owner must never wait for a
                // scan slot held by normal workers parked on these two directory gates.
                tokio::select! {
                    biased;
                    credit = self.normal.clone().acquire_owned() => Ok(DirectoryAdmission {
                        credit: Arc::new(credit?),
                        scan: Some(Scan::Normal(scan)),
                    }),
                    credit = self.reserve.clone().acquire_owned() => {
                        let credit = Arc::new(credit?);
                        Ok(DirectoryAdmission {
                            credit: credit.clone(),
                            scan: Some(Scan::Reserve(Arc::new(ReservedScan {
                                credit,
                                _scan: scan,
                            }))),
                        })
                    }
                }
            }
        }
    }
}

pub(super) fn directory_groups(soft: u64, streams: usize) -> usize {
    let remaining = (soft / 5).saturating_sub(streams as u64).saturating_sub(32);
    // pending files can each pin a different group while scanners need additional groups.
    // reserve mode is for actual descriptor pressure, not exhausted file-task capacity. The
    // semaphore stores a counter; a large soft limit does not eagerly allocate directory state.
    usize::try_from((remaining / 2).max(1).min(Semaphore::MAX_PERMITS as u64))
        .expect("the semaphore capacity fits usize")
}

impl DirectoryAdmission {
    pub(super) fn credit(&self) -> Arc<OwnedSemaphorePermit> {
        self.credit.clone()
    }

    pub(super) fn sequential(&self) -> bool {
        matches!(self.scan, Some(Scan::Reserve(_)))
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
            Scan::Reserve(reserve) => Scan::Reserve(reserve.clone()),
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

    #[tokio::test]
    async fn pending_files_do_not_force_scans_into_reserve_with_descriptor_headroom()
    -> anyhow::Result<()> {
        for soft in [1024, 1_048_576] {
            for streams in [4, 8] {
                let pending = streams * 4;
                let budget = DirectoryBudget::new(directory_groups(soft, streams));
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
                assert_eq!(budget.reserve.available_permits(), 1);
                drop(active);
                drop(parents);
                assert_eq!(scans.available_permits(), streams);
            }
        }
        Ok(())
    }

    #[test]
    fn directory_capacity_reserves_leaf_socket_and_support_headroom() {
        for (soft, streams, expected) in [
            (1024, 4, 84),
            (1024, 8, 82),
            (1024, 64, 54),
            (1024, 100, 36),
            (64, 1, 1),
            (1_000_000, 32, 99_968),
            (0, 1, 1),
        ] {
            assert_eq!(directory_groups(soft, streams), expected);
        }
        let groups = directory_groups(u64::MAX, 4);
        let budget = DirectoryBudget::new(groups);
        assert_eq!(budget.normal.available_permits(), groups);
        assert!(groups <= Semaphore::MAX_PERMITS);
    }

    #[tokio::test]
    async fn reserve_descendants_progress_without_reacquiring_either_gate() -> anyhow::Result<()> {
        let budget = DirectoryBudget::new(1);
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
        drop(grandchild);
        drop(child);
        root.resume(&scans).await?;
        drop(root);
        assert_eq!(
            budget.normal.available_permits(),
            0,
            "a parent alias still owns credit"
        );
        drop(held_root);
        assert_eq!(budget.normal.available_permits(), 1);
        assert_eq!(budget.reserve.available_permits(), 1);
        assert_eq!(scans.available_permits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn closing_both_directory_gates_wakes_a_saturated_worker() -> anyhow::Result<()> {
        let budget = DirectoryBudget::new(1);
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
        let budget = DirectoryBudget::new(3);
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
