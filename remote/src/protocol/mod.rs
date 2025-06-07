use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum FsObject {
    Directory {
        path: std::path::PathBuf,
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
        source_config: SourceConfig,
        rcpd_config: RcpdConfig,
    },
    Destination {
        source_addr: std::net::SocketAddr,
        server_name: String,
        dst: std::path::PathBuf,
        destination_config: DestinationConfig,
        rcpd_config: RcpdConfig,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceMasterHello {
    pub source_addr: std::net::SocketAddr,
    pub server_name: String,
}
