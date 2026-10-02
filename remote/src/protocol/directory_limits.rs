//! Directory lifetime limits shared by the source and destination.

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

/// Normal parallel traversal or the single reserved subtree with pipelined completion.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DirectoryClass {
    Normal,
    Reserve,
}

/// Maximum directory lifetimes in each traversal class.
///
/// The destination sends its limits before normal discovery. The source takes the smaller
/// capacity for each class across both endpoints. Ready does not return lifetime admission.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DirectoryLimits {
    pub normal: NonZeroUsize,
    pub reserve: NonZeroUsize,
}

impl DirectoryLimits {
    /// Use the remote plan installed during startup, or bounded logical limits for direct callers.
    pub fn for_endpoint(
        admission: common::EndpointAdmission,
        streams: usize,
        pending: usize,
    ) -> anyhow::Result<Self> {
        let resources = match admission {
            common::EndpointAdmission::Remote(resources) => resources,
            common::EndpointAdmission::Disabled => {
                let streams =
                    NonZeroUsize::new(streams).context("remote stream capacity is zero")?;
                let pending =
                    NonZeroUsize::new(pending).context("remote pending capacity is zero")?;
                common::RemoteResources::for_limits(None, streams, streams, pending)?
            }
            common::EndpointAdmission::Configured { .. } => {
                anyhow::bail!("remote directory traversal requires remote endpoint admission")
            }
        };
        Ok(Self {
            normal: resources.normal_directories,
            reserve: resources.reserved_directories,
        })
    }

    /// Take the per-class capacity supported by both endpoints.
    pub fn intersect(self, other: Self) -> Self {
        Self {
            normal: self.normal.min(other.normal),
            reserve: self.reserve.min(other.reserve),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_limits_leave_room_for_both_parallel_parents_and_deep_reserve() {
        let limits = DirectoryLimits::for_endpoint(
            common::EndpointAdmission::Remote(
                common::RemoteResources::for_limits(
                    std::num::NonZeroU64::new(1024),
                    NonZeroUsize::new(20).unwrap(),
                    NonZeroUsize::new(20).unwrap(),
                    NonZeroUsize::new(80).unwrap(),
                )
                .unwrap(),
            ),
            20,
            80,
        )
        .unwrap();
        assert!(
            limits.normal.get() >= 100,
            "80 pinned parents and 20 active scans"
        );
        assert!(
            limits.reserve.get() >= 80,
            "an 80-level reserved traversal must fit"
        );
        assert!(2 * (limits.normal.get() + limits.reserve.get()) + 80 + 20 + 32 <= 1024);
    }

    #[test]
    fn local_leaf_configuration_cannot_authorize_remote_directory_ownership() {
        let result = DirectoryLimits::for_endpoint(
            common::EndpointAdmission::Configured {
                soft_limit: std::num::NonZeroU64::new(64),
                leaf_capacity: NonZeroUsize::new(8).unwrap(),
            },
            8,
            32,
        );
        assert!(result.is_err());
    }

    #[test]
    fn disabled_admission_has_bounded_logical_capacity_without_a_depth_two_limit() {
        let limits =
            DirectoryLimits::for_endpoint(common::EndpointAdmission::Disabled, 20, 80).unwrap();
        assert_eq!(limits.normal.get(), 200);
        assert_eq!(limits.reserve.get(), 200);
    }

    #[test]
    fn installed_remote_directory_counts_are_authoritative() {
        let resources = common::RemoteResources::for_limits(
            std::num::NonZeroU64::new(128),
            NonZeroUsize::new(200).unwrap(),
            NonZeroUsize::new(20).unwrap(),
            NonZeroUsize::new(80).unwrap(),
        )
        .unwrap();
        let limits =
            DirectoryLimits::for_endpoint(common::EndpointAdmission::Remote(resources), 20, 80)
                .unwrap();
        assert_eq!(limits.normal.get(), 13);
        assert_eq!(limits.reserve.get(), 13);
    }

    #[test]
    fn endpoint_limits_preserve_normal_ancestor_capacity_at_low_file_concurrency() {
        for (streams, normal, reserve) in [(4, 243, 243), (8, 238, 238), (20, 223, 223)] {
            let resources = common::RemoteResources::for_limits(
                std::num::NonZeroU64::new(1024),
                NonZeroUsize::new(streams).unwrap(),
                NonZeroUsize::new(streams).unwrap(),
                NonZeroUsize::new(streams * 4).unwrap(),
            )
            .unwrap();
            let limits = DirectoryLimits::for_endpoint(
                common::EndpointAdmission::Remote(resources),
                streams,
                streams * 4,
            )
            .unwrap();
            assert_eq!(limits.normal.get(), normal);
            assert_eq!(limits.reserve.get(), reserve);
        }
    }

    #[test]
    fn each_endpoint_can_constrain_a_different_directory_class() {
        let source = DirectoryLimits {
            normal: NonZeroUsize::new(200).unwrap(),
            reserve: NonZeroUsize::new(80).unwrap(),
        };
        let destination = DirectoryLimits {
            normal: NonZeroUsize::new(100).unwrap(),
            reserve: NonZeroUsize::new(160).unwrap(),
        };
        let negotiated = source.intersect(destination);
        assert_eq!(negotiated.normal.get(), 100);
        assert_eq!(negotiated.reserve.get(), 80);
    }
}
