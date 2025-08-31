use serde::{Deserialize, Serialize};
use std::os::unix::fs::MetadataExt;
use std::os::unix::prelude::PermissionsExt;

#[derive(Debug, Deserialize, Serialize)]
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

impl From<&std::fs::Metadata> for Metadata {
    fn from(metadata: &std::fs::Metadata) -> Self {
        Metadata {
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            atime: metadata.atime(),
            mtime: metadata.mtime(),
            atime_nsec: metadata.atime_nsec(),
            mtime_nsec: metadata.mtime_nsec(),
        }
    }
}

// implies files contents will be sent immediately after receiving this object
#[derive(Debug, Deserialize, Serialize)]
pub struct File {
    pub src: std::path::PathBuf,
    pub dst: std::path::PathBuf,
    pub size: u64,
    pub metadata: Metadata,
    pub is_root: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum SourceMessage {
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
    // this message is useful to open the control stream when there are no directories
    DirStructureComplete,
    Symlink {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        target: std::path::PathBuf,
        metadata: Metadata,
        is_root: bool,
    },
    SourceDone, // must be the last message sent by the source
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SrcDst {
    pub src: std::path::PathBuf,
    pub dst: std::path::PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DestinationMessage {
    DirectoryCreated(SrcDst),
    DirectoryComplete(SrcDst),
    DestinationDone, // must be the last message sent by the destination
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RcpdConfig {
    pub verbose: u8,
    pub fail_early: bool,
    pub max_workers: usize,
    pub max_blocking_threads: usize,
    pub max_open_files: Option<usize>,
    pub ops_throttle: usize,
    pub iops_throttle: usize,
    pub chunk_size: usize,
    pub tput_throttle: usize,
    // common::copy::Settings
    pub dereference: bool,
    pub overwrite: bool,
    pub overwrite_compare: String,
}

impl RcpdConfig {
    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![
            format!("--max-workers={}", self.max_workers),
            format!("--max-blocking-threads={}", self.max_blocking_threads),
            format!("--ops-throttle={}", self.ops_throttle),
            format!("--iops-throttle={}", self.iops_throttle),
            format!("--chunk-size={}", self.chunk_size),
            format!("--tput-throttle={}", self.tput_throttle),
            format!("--overwrite-compare={}", self.overwrite_compare),
        ];
        if self.verbose > 0 {
            args.push(format!("-{}", "v".repeat(self.verbose as usize)));
        }
        if self.fail_early {
            args.push("--fail-early".to_string());
        }
        if let Some(v) = self.max_open_files {
            args.push(format!("--max-open-files={v}"));
        }
        if self.dereference {
            args.push("--dereference".to_string());
        }
        if self.overwrite {
            args.push("--overwrite".to_string());
        }
        args
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TracingHello {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MasterHello {
    Source {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
    },
    Destination {
        source_addr: std::net::SocketAddr,
        server_name: String,
        preserve: common::preserve::Settings,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceMasterHello {
    pub source_addr: std::net::SocketAddr,
    pub server_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RcpdGoodBye {}
