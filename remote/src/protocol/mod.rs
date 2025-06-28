use serde::{Deserialize, Serialize};
use std::os::unix::prelude::PermissionsExt;

#[derive(Debug, Serialize, Deserialize)]
pub enum FsObject {
    Directory {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime_nsec: i64,
        ctime_nsec: i64,
    },
    // Implies files contents will be sent immediately after receiving this object
    File {
        path: std::path::PathBuf,
        size: u64,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime_nsec: i64,
        ctime_nsec: i64,
    },
    Symlink {
        path: std::path::PathBuf,
        target: std::path::PathBuf,
        uid: u32,
        gid: u32,
        mtime_nsec: i64,
        ctime_nsec: i64,
    },
}

impl common::preserve::Metadata for FsObject {
    fn uid(&self) -> u32 {
        match self {
            FsObject::Directory { uid, .. } => *uid,
            FsObject::File { uid, .. } => *uid,
            FsObject::Symlink { uid, .. } => *uid,
        }
    }
    fn gid(&self) -> u32 {
        match self {
            FsObject::Directory { gid, .. } => *gid,
            FsObject::File { gid, .. } => *gid,
            FsObject::Symlink { gid, .. } => *gid,
        }
    }
    fn atime(&self) -> i64 {
        self.mtime()
    }
    fn atime_nsec(&self) -> i64 {
        self.mtime_nsec()
    }
    fn mtime(&self) -> i64 {
        match self {
            FsObject::Directory { mtime_nsec, .. } => mtime_nsec / 1_000_000_000,
            FsObject::File { mtime_nsec, .. } => mtime_nsec / 1_000_000_000,
            FsObject::Symlink { mtime_nsec, .. } => mtime_nsec / 1_000_000_000,
        }
    }
    fn mtime_nsec(&self) -> i64 {
        match self {
            FsObject::Directory { mtime_nsec, .. } => mtime_nsec % 1_000_000_000,
            FsObject::File { mtime_nsec, .. } => mtime_nsec % 1_000_000_000,
            FsObject::Symlink { mtime_nsec, .. } => mtime_nsec % 1_000_000_000,
        }
    }
    fn permissions(&self) -> std::fs::Permissions {
        match self {
            FsObject::Directory { mode, .. } => std::fs::Permissions::from_mode(*mode),
            FsObject::File { mode, .. } => std::fs::Permissions::from_mode(*mode),
            FsObject::Symlink { .. } => std::fs::Permissions::from_mode(0o777), // symlinks don't have meaningful permissions
        }
    }
}

impl common::preserve::Metadata for &FsObject {
    fn uid(&self) -> u32 {
        (*self).uid()
    }
    fn gid(&self) -> u32 {
        (*self).gid()
    }
    fn atime(&self) -> i64 {
        (*self).atime()
    }
    fn atime_nsec(&self) -> i64 {
        (*self).atime_nsec()
    }
    fn mtime(&self) -> i64 {
        (*self).mtime()
    }
    fn mtime_nsec(&self) -> i64 {
        (*self).mtime_nsec()
    }
    fn permissions(&self) -> std::fs::Permissions {
        (*self).permissions()
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct RcpdConfig {
    pub fail_early: bool,
    pub max_workers: usize,
    pub max_blocking_threads: usize,
    pub max_open_files: Option<usize>,
    pub ops_throttle: usize,
    pub iops_throttle: usize,
    pub chunk_size: usize,
    pub tput_throttle: usize,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct SourceConfig {
    pub dereference: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DestinationConfig {
    pub overwrite: bool,
    pub overwrite_compare: String,
    pub preserve: bool,
    pub preserve_settings: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MasterHello {
    Source {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        source_config: SourceConfig,
        rcpd_config: RcpdConfig,
    },
    Destination {
        source_addr: std::net::SocketAddr,
        server_name: String,
        destination_config: DestinationConfig,
        rcpd_config: RcpdConfig,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceMasterHello {
    pub source_addr: std::net::SocketAddr,
    pub server_name: String,
}
