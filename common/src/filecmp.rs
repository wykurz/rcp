use std::os::unix::prelude::PermissionsExt;
use tracing::instrument;

#[derive(Copy, Clone, Debug, Default)]
pub struct MetadataCmpSettings {
    pub uid: bool,
    pub gid: bool,
    pub mode: bool,
    pub size: bool,
    pub mtime: bool,
    pub ctime: bool,
}

#[instrument]
pub fn metadata_equal<
    M1: crate::preserve::Metadata + std::fmt::Debug,
    M2: crate::preserve::Metadata + std::fmt::Debug,
>(
    settings: &MetadataCmpSettings,
    metadata1: &M1,
    metadata2: &M2,
) -> bool {
    if settings.uid && metadata1.uid() != metadata2.uid() {
        return false;
    }
    if settings.gid && metadata1.gid() != metadata2.gid() {
        return false;
    }
    if settings.mode && metadata1.permissions().mode() != metadata2.permissions().mode() {
        return false;
    }
    if settings.size && metadata1.size() != metadata2.size() {
        return false;
    }
    if settings.mtime {
        if metadata1.mtime() != metadata2.mtime() {
            return false;
        }
        // some filesystems do not support nanosecond precision, so we only compare nanoseconds if both files have them
        if metadata1.mtime_nsec() != 0
            && metadata2.mtime_nsec() != 0
            && metadata1.mtime_nsec() != metadata2.mtime_nsec()
        {
            return false;
        }
    }
    if settings.ctime {
        // ctime() returns 0 if not available (e.g., in protocol::Metadata)
        // only compare if both have ctime available
        if metadata1.ctime() != 0 && metadata2.ctime() != 0 {
            if metadata1.ctime() != metadata2.ctime() {
                return false;
            }
            if metadata1.ctime_nsec() != 0
                && metadata2.ctime_nsec() != 0
                && metadata1.ctime_nsec() != metadata2.ctime_nsec()
            {
                return false;
            }
        }
    }
    true
}

/// Returns true if dest mtime is strictly greater than src mtime (including nanoseconds).
///
/// Unlike [`metadata_equal`], this does not special-case zero nanoseconds for filesystems
/// without nanosecond precision. Zero nsec is compared literally, which is the safest
/// default for a directional check: when in doubt, we overwrite rather than skip.
#[instrument]
pub fn dest_is_newer<
    M1: crate::preserve::Metadata + std::fmt::Debug,
    M2: crate::preserve::Metadata + std::fmt::Debug,
>(
    src: &M1,
    dest: &M2,
) -> bool {
    (dest.mtime(), dest.mtime_nsec()) > (src.mtime(), src.mtime_nsec())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Debug)]
    struct FakeMeta {
        mtime: i64,
        mtime_nsec: i64,
    }
    impl crate::preserve::Metadata for FakeMeta {
        fn uid(&self) -> u32 {
            0
        }
        fn gid(&self) -> u32 {
            0
        }
        fn atime(&self) -> i64 {
            0
        }
        fn atime_nsec(&self) -> i64 {
            0
        }
        fn mtime(&self) -> i64 {
            self.mtime
        }
        fn mtime_nsec(&self) -> i64 {
            self.mtime_nsec
        }
        fn permissions(&self) -> std::fs::Permissions {
            std::os::unix::fs::PermissionsExt::from_mode(0o644)
        }
    }
    #[test]
    fn dest_newer_by_seconds() {
        let src = FakeMeta {
            mtime: 100,
            mtime_nsec: 0,
        };
        let dest = FakeMeta {
            mtime: 200,
            mtime_nsec: 0,
        };
        assert!(dest_is_newer(&src, &dest));
    }
    #[test]
    fn dest_older_by_seconds() {
        let src = FakeMeta {
            mtime: 200,
            mtime_nsec: 0,
        };
        let dest = FakeMeta {
            mtime: 100,
            mtime_nsec: 0,
        };
        assert!(!dest_is_newer(&src, &dest));
    }
    #[test]
    fn same_mtime_not_newer() {
        let src = FakeMeta {
            mtime: 100,
            mtime_nsec: 500,
        };
        let dest = FakeMeta {
            mtime: 100,
            mtime_nsec: 500,
        };
        assert!(!dest_is_newer(&src, &dest));
    }
    #[test]
    fn dest_newer_by_nsec() {
        let src = FakeMeta {
            mtime: 100,
            mtime_nsec: 500,
        };
        let dest = FakeMeta {
            mtime: 100,
            mtime_nsec: 600,
        };
        assert!(dest_is_newer(&src, &dest));
    }
    #[test]
    fn dest_older_by_nsec() {
        let src = FakeMeta {
            mtime: 100,
            mtime_nsec: 600,
        };
        let dest = FakeMeta {
            mtime: 100,
            mtime_nsec: 500,
        };
        assert!(!dest_is_newer(&src, &dest));
    }
}
