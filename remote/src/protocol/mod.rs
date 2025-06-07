use serde::{Deserialize, Serialize};
use strum::EnumString;

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

#[derive(Copy, Clone, Debug, EnumString, Serialize, Deserialize)]
pub enum Side {
    Source,
    Destination,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceMasterHello {
    pub source_addr: std::net::SocketAddr,
    pub server_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MasterDestinationHello {
    pub source_addr: std::net::SocketAddr,
    pub server_name: String,
}