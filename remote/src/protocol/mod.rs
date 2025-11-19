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

// wrapper that includes size for comparison purposes
#[derive(Debug)]
pub struct FileMetadata<'a> {
    pub metadata: &'a Metadata,
    pub size: u64,
}

impl<'a> common::preserve::Metadata for FileMetadata<'a> {
    fn uid(&self) -> u32 {
        self.metadata.uid()
    }
    fn gid(&self) -> u32 {
        self.metadata.gid()
    }
    fn atime(&self) -> i64 {
        self.metadata.atime()
    }
    fn atime_nsec(&self) -> i64 {
        self.metadata.atime_nsec()
    }
    fn mtime(&self) -> i64 {
        self.metadata.mtime()
    }
    fn mtime_nsec(&self) -> i64 {
        self.metadata.mtime_nsec()
    }
    fn permissions(&self) -> std::fs::Permissions {
        self.metadata.permissions()
    }
    fn size(&self) -> u64 {
        self.size
    }
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
    FileSkipped(SrcDst),    // file failed to send, decrement directory counter
    SymlinkSkipped(SrcDst), // symlink failed to send, decrement directory counter
    SourceDone,             // must be the last message sent by the source
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
    DirectoryFailed(SrcDst), // directory creation failed, source should skip its contents
    DestinationDone,         // must be the last message sent by the destination
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
    // common::copy::Settings
    pub dereference: bool,
    pub overwrite: bool,
    pub overwrite_compare: String,
    pub debug_log_prefix: Option<String>,
    pub quic_port_ranges: Option<String>,
    pub quic_idle_timeout_sec: u64,
    pub quic_keep_alive_interval_sec: u64,
    pub progress: bool,
    pub progress_delay: Option<String>,
    pub remote_copy_conn_timeout_sec: u64,
    /// SHA-256 fingerprint of the Master's TLS certificate (32 bytes)
    /// Used for certificate pinning when rcpd connects to Master
    pub master_cert_fingerprint: Vec<u8>,
}

impl RcpdConfig {
    pub fn to_args(&self) -> Vec<String> {
        let mut args = vec![
            format!("--max-workers={}", self.max_workers),
            format!("--max-blocking-threads={}", self.max_blocking_threads),
            format!("--ops-throttle={}", self.ops_throttle),
            format!("--iops-throttle={}", self.iops_throttle),
            format!("--chunk-size={}", self.chunk_size),
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
        if let Some(ref prefix) = self.debug_log_prefix {
            args.push(format!("--debug-log-prefix={prefix}"));
        }
        if let Some(ref ranges) = self.quic_port_ranges {
            args.push(format!("--quic-port-ranges={ranges}"));
        }
        args.push(format!(
            "--quic-idle-timeout-sec={}",
            self.quic_idle_timeout_sec
        ));
        args.push(format!(
            "--quic-keep-alive-interval-sec={}",
            self.quic_keep_alive_interval_sec
        ));
        if self.progress {
            args.push("--progress".to_string());
        }
        if let Some(ref delay) = self.progress_delay {
            args.push(format!("--progress-delay={delay}"));
        }
        args.push(format!(
            "--remote-copy-conn-timeout-sec={}",
            self.remote_copy_conn_timeout_sec
        ));
        // pass master cert fingerprint as hex-encoded string
        args.push(format!(
            "--master-cert-fingerprint={}",
            hex::encode(&self.master_cert_fingerprint)
        ));
        args
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum RcpdRole {
    Source,
    Destination,
}

impl std::fmt::Display for RcpdRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RcpdRole::Source => write!(f, "source"),
            RcpdRole::Destination => write!(f, "destination"),
        }
    }
}

impl std::str::FromStr for RcpdRole {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "source" => Ok(RcpdRole::Source),
            "destination" | "dest" => Ok(RcpdRole::Destination),
            _ => Err(anyhow::anyhow!("invalid role: {}", s)),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TracingHello {
    pub role: RcpdRole,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MasterHello {
    Source {
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
    },
    Destination {
        source_addr: std::net::SocketAddr,
        server_name: String,
        /// SHA-256 fingerprint of the source's TLS certificate (32 bytes)
        /// Used for certificate pinning to prevent MITM attacks
        source_cert_fingerprint: Vec<u8>,
        preserve: common::preserve::Settings,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceMasterHello {
    pub source_addr: std::net::SocketAddr,
    pub server_name: String,
    /// SHA-256 fingerprint of this source's TLS certificate (32 bytes)
    /// Used for certificate pinning to prevent MITM attacks
    pub cert_fingerprint: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum RcpdResult {
    Success {
        message: String,
        summary: common::copy::Summary,
    },
    Failure {
        error: String,
        summary: common::copy::Summary,
    },
}
