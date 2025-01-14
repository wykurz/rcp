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

impl FsObject {
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}
