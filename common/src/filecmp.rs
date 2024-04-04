use std::os::unix::fs::MetadataExt;
use tracing::instrument;

#[derive(Copy, Clone, Debug, Default)]
pub struct MetadataCmpSettings {
    pub uid: bool,
    pub gid: bool,
    pub size: bool,
    pub mtime: bool,
    pub ctime: bool,
}

#[instrument]
pub fn metadata_equal(
    settings: &MetadataCmpSettings,
    metadata1: &std::fs::Metadata,
    metadata2: &std::fs::Metadata,
) -> bool {
    if settings.uid && metadata1.uid() != metadata2.uid() {
        return false;
    }
    if settings.gid && metadata1.gid() != metadata2.gid() {
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
    true
}
