use serde::{Deserialize, Serialize};
use std::os::unix::prelude::PermissionsExt;

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,
    pub mtime: i64,
    pub atime_nsec: i64,
    pub mtime_nsec: i64,
}

impl common::preserve::Metadata for Metadata {
    fn uid(&self) -> u32 {
        self.uid
    }
    fn gid(&self) -> u32 {
        self.gid
    }
    fn atime(&self) -> i64 {
        self.atime
    }
    fn atime_nsec(&self) -> i64 {
        self.atime_nsec
    }
    fn mtime(&self) -> i64 {
        self.mtime
    }
    fn mtime_nsec(&self) -> i64 {
        self.mtime_nsec
    }
    fn permissions(&self) -> std::fs::Permissions {
        std::fs::Permissions::from_mode(self.mode)
    }
}

impl common::preserve::Metadata for &Metadata {
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

#[derive(Debug, Serialize, Deserialize)]
pub enum FsObject {
    DirStub {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        num_entries: usize,
    },
    Directory {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        metadata: Metadata,
        is_root: bool,
    },
    // Implies files contents will be sent immediately after receiving this object
    File {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        size: u64,
        metadata: Metadata,
        is_root: bool,
    },
    Symlink {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        target: std::path::PathBuf,
        metadata: Metadata,
        is_root: bool,
    },
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirectoryCreated {
    pub src: std::path::PathBuf,
    pub dst: std::path::PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirectoryComplete {
    pub src: std::path::PathBuf,
    pub dst: std::path::PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamHello {
    pub stream_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DirectoryMessage {
    Created(DirectoryCreated),
    Complete(DirectoryComplete),
}
