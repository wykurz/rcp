//! Receiver directory lifetime admission and descriptor-release notifications.

use anyhow::Context as _;
use remote::protocol::{DirectoryClass, DirectoryLimits, SrcDst};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct DirectoryLease {
    released: tokio::sync::mpsc::UnboundedSender<Released>,
    entry: Option<Released>,
}

impl Drop for DirectoryLease {
    fn drop(&mut self) {
        // safedir drops the held descriptor before its last lease reference. The lifetime
        // remains charged until this notification is flushed and its End has been received.
        if let Some(entry) = self.entry.take() {
            let _ = self.released.send(entry);
        }
    }
}

#[derive(Debug)]
pub(super) struct Released {
    pub(super) pair: SrcDst,
    class: DirectoryClass,
}

struct Active {
    src: PathBuf,
    class: DirectoryClass,
    ended: bool,
    released: bool,
}

pub(super) struct DirectoryLifetimes {
    limits: DirectoryLimits,
    active: HashMap<PathBuf, Active>,
    normal_count: usize,
    reserved_count: usize,
    reserved_root: Option<PathBuf>,
    released: tokio::sync::mpsc::UnboundedSender<Released>,
    received: tokio::sync::mpsc::UnboundedReceiver<Released>,
}

impl DirectoryLifetimes {
    pub(super) fn new(limits: DirectoryLimits) -> Self {
        let (released, received) = tokio::sync::mpsc::unbounded_channel();
        Self {
            limits,
            active: HashMap::new(),
            normal_count: 0,
            reserved_count: 0,
            reserved_root: None,
            released,
            received,
        }
    }

    /// Validate peer admission synchronously; the reader never waits for descriptor capacity.
    pub(super) fn admit(
        &mut self,
        src: &Path,
        dst: &Path,
        class: DirectoryClass,
    ) -> anyhow::Result<Arc<DirectoryLease>> {
        anyhow::ensure!(
            !self.active.contains_key(dst),
            "duplicate directory lifetime: {dst:?}"
        );
        match class {
            DirectoryClass::Normal => {
                anyhow::ensure!(
                    self.normal_count < self.limits.normal.get(),
                    "source exceeded normal directory lifetime capacity"
                );
                anyhow::ensure!(
                    self.reserved_root
                        .as_ref()
                        .is_none_or(|root| !dst.starts_with(root)),
                    "normal directory below a reserved traversal: {dst:?}"
                );
                self.normal_count += 1;
            }
            DirectoryClass::Reserve => {
                anyhow::ensure!(
                    self.reserved_count < self.limits.reserve.get(),
                    "source exceeded reserved directory lifetime capacity"
                );
                if self.reserved_count != 0 {
                    anyhow::ensure!(
                        dst.parent()
                            .and_then(|parent| self.active.get(parent))
                            .is_some_and(|parent| {
                                parent.class == DirectoryClass::Reserve && !parent.ended
                            }),
                        "concurrent reserved subtrees: {dst:?}"
                    );
                } else {
                    self.reserved_root = Some(dst.to_path_buf());
                }
                self.reserved_count += 1;
            }
        }
        self.active.insert(
            dst.to_path_buf(),
            Active {
                src: src.to_path_buf(),
                class,
                ended: false,
                released: false,
            },
        );
        Ok(Arc::new(DirectoryLease {
            released: self.released.clone(),
            entry: Some(Released {
                pair: SrcDst {
                    src: src.to_path_buf(),
                    dst: dst.to_path_buf(),
                },
                class,
            }),
        }))
    }

    pub(super) async fn next_release(&mut self) -> Released {
        self.received
            .recv()
            .await
            .expect("the directory lifetime owner retains a sender")
    }

    pub(super) fn try_next_release(&mut self) -> Option<Released> {
        self.received.try_recv().ok()
    }

    /// Record an End after tracker validation; rejected directories can have released already.
    pub(super) fn ended(&mut self, src: &Path, dst: &Path) -> anyhow::Result<()> {
        let active = self
            .active
            .get_mut(dst)
            .context("DirectoryEnd for unknown directory lifetime")?;
        anyhow::ensure!(active.src == src, "directory End identity mismatch");
        anyhow::ensure!(!active.ended, "duplicate directory lifetime End");
        active.ended = true;
        self.retire_completed(dst);
        Ok(())
    }

    /// Commit a flushed release acknowledgement, keeping peer-visible outstanding work bounded.
    pub(super) fn acknowledged(&mut self, released: &Released) -> anyhow::Result<()> {
        let active = self
            .active
            .get_mut(&released.pair.dst)
            .context("unknown directory release")?;
        anyhow::ensure!(
            active.src == released.pair.src && active.class == released.class,
            "directory release identity mismatch"
        );
        anyhow::ensure!(!active.released, "duplicate directory release");
        active.released = true;
        self.retire_completed(&released.pair.dst);
        Ok(())
    }

    fn retire_completed(&mut self, dst: &Path) {
        if !self
            .active
            .get(dst)
            .is_some_and(|active| active.ended && active.released)
        {
            return;
        }
        let active = self.active.remove(dst).expect("completed lifetime exists");
        match active.class {
            DirectoryClass::Normal => self.normal_count -= 1,
            DirectoryClass::Reserve => {
                self.reserved_count -= 1;
                // rejection can retire the root before its descendants. Keep the subtree's
                // identity until every outstanding lifetime has received both terminal events.
                if self.reserved_count == 0 {
                    self.reserved_root = None;
                }
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.active.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use std::num::NonZeroUsize;

    fn lifetimes() -> DirectoryLifetimes {
        DirectoryLifetimes::new(DirectoryLimits {
            normal: NonZeroUsize::new(1).unwrap(),
            reserve: NonZeroUsize::new(2).unwrap(),
        })
    }

    #[tokio::test]
    async fn released_rejected_directory_admits_its_already_submitted_child() {
        let mut lifetimes = DirectoryLifetimes::new(DirectoryLimits {
            normal: NonZeroUsize::MIN,
            reserve: NonZeroUsize::new(3).unwrap(),
        });
        let root = Path::new("/root");
        let rejected = root.join("rejected");
        let child = rejected.join("child");
        let _root = lifetimes
            .admit(root, root, DirectoryClass::Reserve)
            .unwrap();
        let rejected_lease = lifetimes
            .admit(&rejected, &rejected, DirectoryClass::Reserve)
            .unwrap();
        drop(rejected_lease);
        let released = lifetimes.next_release().await;
        lifetimes.acknowledged(&released).unwrap();
        // rejection can release descriptors before the source's queued child Begin and End
        lifetimes
            .admit(&child, &child, DirectoryClass::Reserve)
            .expect("a rejected parent remains in the reserved subtree until its End");
    }

    #[tokio::test]
    async fn aliases_and_unacknowledged_releases_each_prevent_new_admission() {
        let mut lifetimes = lifetimes();
        let path = Path::new("/one");
        let lease = lifetimes.admit(path, path, DirectoryClass::Normal).unwrap();
        lifetimes.ended(path, path).unwrap();
        let alias = lease.clone();
        drop(lease);
        assert!(lifetimes.next_release().now_or_never().is_none());
        assert!(
            lifetimes
                .admit(Path::new("/two"), Path::new("/two"), DirectoryClass::Normal)
                .is_err()
        );
        drop(alias);
        let released = lifetimes.next_release().await;
        assert!(
            lifetimes
                .admit(Path::new("/two"), Path::new("/two"), DirectoryClass::Normal)
                .is_err()
        );
        lifetimes.acknowledged(&released).unwrap();
        let next = lifetimes
            .admit(Path::new("/two"), Path::new("/two"), DirectoryClass::Normal)
            .unwrap();
        lifetimes
            .ended(Path::new("/two"), Path::new("/two"))
            .unwrap();
        drop(next);
        let released = lifetimes.next_release().await;
        lifetimes.acknowledged(&released).unwrap();
        assert!(lifetimes.is_empty());
    }

    #[tokio::test]
    async fn reserve_accepts_one_bounded_subtree_and_reuses_completed_lifetimes() {
        let mut lifetimes = lifetimes();
        let root = Path::new("/root");
        let child = Path::new("/root/child");
        let first = lifetimes
            .admit(root, root, DirectoryClass::Reserve)
            .unwrap();
        assert!(
            lifetimes
                .admit(
                    Path::new("/other"),
                    Path::new("/other"),
                    DirectoryClass::Reserve
                )
                .is_err()
        );
        assert!(
            lifetimes
                .admit(child, child, DirectoryClass::Normal)
                .is_err()
        );
        let second = lifetimes
            .admit(child, child, DirectoryClass::Reserve)
            .unwrap();
        let grandchild = Path::new("/root/child/deep");
        assert!(
            lifetimes
                .admit(grandchild, grandchild, DirectoryClass::Reserve)
                .is_err()
        );
        lifetimes.ended(child, child).unwrap();
        drop(second);
        let released = lifetimes.next_release().await;
        lifetimes.acknowledged(&released).unwrap();
        let sibling = Path::new("/root/sibling");
        let second = lifetimes
            .admit(sibling, sibling, DirectoryClass::Reserve)
            .unwrap();
        lifetimes.ended(sibling, sibling).unwrap();
        drop(second);
        let released = lifetimes.next_release().await;
        lifetimes.acknowledged(&released).unwrap();
        lifetimes.ended(root, root).unwrap();
        drop(first);
        let released = lifetimes.next_release().await;
        lifetimes.acknowledged(&released).unwrap();
        assert!(lifetimes.is_empty());
    }

    #[tokio::test]
    async fn rejected_reserved_parent_can_release_before_its_admitted_child() {
        let mut lifetimes = lifetimes();
        let root = Path::new("/root");
        let child = Path::new("/root/child");
        let parent = lifetimes
            .admit(root, root, DirectoryClass::Reserve)
            .unwrap();
        let descendant = lifetimes
            .admit(child, child, DirectoryClass::Reserve)
            .unwrap();
        drop(parent);
        let released = lifetimes.next_release().await;
        lifetimes.acknowledged(&released).unwrap();
        assert!(
            lifetimes
                .admit(
                    Path::new("/other"),
                    Path::new("/other"),
                    DirectoryClass::Reserve
                )
                .is_err()
        );
        lifetimes.ended(child, child).unwrap();
        lifetimes.ended(root, root).unwrap();
        assert!(
            lifetimes
                .admit(
                    Path::new("/root/normal"),
                    Path::new("/root/normal"),
                    DirectoryClass::Normal
                )
                .is_err(),
            "a retired root must retain its subtree identity while descendants remain"
        );
        drop(descendant);
        let released = lifetimes.next_release().await;
        lifetimes.acknowledged(&released).unwrap();
        assert!(lifetimes.is_empty());
        assert!(
            lifetimes
                .admit(
                    Path::new("/other"),
                    Path::new("/other"),
                    DirectoryClass::Reserve
                )
                .is_ok()
        );
    }

    #[tokio::test]
    async fn lifetimes_retire_only_after_end_and_release_in_either_order() {
        for class in [DirectoryClass::Normal, DirectoryClass::Reserve] {
            for end_first in [false, true] {
                let mut lifetimes = DirectoryLifetimes::new(DirectoryLimits {
                    normal: NonZeroUsize::MIN,
                    reserve: NonZeroUsize::MIN,
                });
                let path = Path::new("/one");
                let other = Path::new("/two");
                let lease = lifetimes.admit(path, path, class).unwrap();
                if end_first {
                    lifetimes.ended(path, path).unwrap();
                }
                drop(lease);
                let released = lifetimes.next_release().await;
                if !end_first {
                    lifetimes.acknowledged(&released).unwrap();
                }
                assert!(!lifetimes.is_empty());
                assert!(lifetimes.admit(other, other, class).is_err());
                assert!(lifetimes.admit(path, path, class).is_err());
                if end_first {
                    lifetimes.acknowledged(&released).unwrap();
                } else {
                    lifetimes.ended(path, path).unwrap();
                }
                assert!(lifetimes.is_empty());
                lifetimes.admit(other, other, class).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn invalid_terminal_events_do_not_release_directory_capacity() {
        for class in [DirectoryClass::Normal, DirectoryClass::Reserve] {
            let mut lifetimes = lifetimes();
            let src = Path::new("/source");
            let dst = Path::new("/destination");
            let other = Path::new("/other");
            let lease = lifetimes.admit(src, dst, class).unwrap();
            assert!(lifetimes.ended(other, dst).is_err());
            assert!(lifetimes.ended(src, other).is_err());
            lifetimes.ended(src, dst).unwrap();
            assert!(lifetimes.ended(src, dst).is_err());
            drop(lease);
            let released = lifetimes.next_release().await;
            for (source, destination, release_class) in [
                (other, dst, class),
                (src, other, class),
                (
                    src,
                    dst,
                    match class {
                        DirectoryClass::Normal => DirectoryClass::Reserve,
                        DirectoryClass::Reserve => DirectoryClass::Normal,
                    },
                ),
            ] {
                let invalid = Released {
                    pair: SrcDst {
                        src: source.into(),
                        dst: destination.into(),
                    },
                    class: release_class,
                };
                assert!(lifetimes.acknowledged(&invalid).is_err());
                assert!(!lifetimes.is_empty());
            }
            lifetimes.acknowledged(&released).unwrap();
            assert!(lifetimes.is_empty());
            assert!(lifetimes.ended(src, dst).is_err());
            assert!(lifetimes.acknowledged(&released).is_err());
        }
    }

    #[tokio::test]
    async fn duplicate_release_before_end_keeps_the_lifetime_charged() {
        for class in [DirectoryClass::Normal, DirectoryClass::Reserve] {
            let mut lifetimes = lifetimes();
            let path = Path::new("/root");
            let lease = lifetimes.admit(path, path, class).unwrap();
            drop(lease);
            let released = lifetimes.next_release().await;
            lifetimes.acknowledged(&released).unwrap();
            assert!(lifetimes.acknowledged(&released).is_err());
            assert!(!lifetimes.is_empty());
            lifetimes.ended(path, path).unwrap();
            assert!(lifetimes.is_empty());
        }
    }

    #[tokio::test]
    async fn reserved_siblings_pipeline_within_the_lifetime_limit() {
        let mut lifetimes = DirectoryLifetimes::new(DirectoryLimits {
            normal: NonZeroUsize::MIN,
            reserve: NonZeroUsize::new(3).unwrap(),
        });
        let root = Path::new("/root");
        let first = root.join("first");
        let second = root.join("second");
        let next = second.join("next");
        let _root = lifetimes
            .admit(root, root, DirectoryClass::Reserve)
            .unwrap();
        let first_lease = lifetimes
            .admit(&first, &first, DirectoryClass::Reserve)
            .unwrap();
        lifetimes.ended(&first, &first).unwrap();
        let _second = lifetimes
            .admit(&second, &second, DirectoryClass::Reserve)
            .expect("an ended sibling can retain descriptors while discovery continues");
        assert!(
            lifetimes
                .admit(&next, &next, DirectoryClass::Reserve)
                .is_err(),
            "ended siblings still count until their release flushes"
        );
        drop(first_lease);
        let released = lifetimes.next_release().await;
        lifetimes.acknowledged(&released).unwrap();
        lifetimes
            .admit(&next, &next, DirectoryClass::Reserve)
            .unwrap();
    }

    #[tokio::test]
    async fn reserved_children_require_an_active_unended_reserved_parent() {
        let mut lifetimes = DirectoryLifetimes::new(DirectoryLimits {
            normal: NonZeroUsize::MIN,
            reserve: NonZeroUsize::new(3).unwrap(),
        });
        let root = Path::new("/root");
        let normal = Path::new("/normal");
        let _root = lifetimes
            .admit(root, root, DirectoryClass::Reserve)
            .unwrap();
        let _normal = lifetimes
            .admit(normal, normal, DirectoryClass::Normal)
            .unwrap();
        for path in [Path::new("/unknown/child"), Path::new("/normal/child")] {
            assert!(
                lifetimes
                    .admit(path, path, DirectoryClass::Reserve)
                    .is_err()
            );
        }
        lifetimes.ended(root, root).unwrap();
        let child = root.join("child");
        assert!(
            lifetimes
                .admit(&child, &child, DirectoryClass::Reserve)
                .is_err(),
            "inline reserved discovery sends every child Begin before its parent's End"
        );
    }
}
