use anyhow::{anyhow, Context};

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

impl FsObject {
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

#[derive(std::fmt::Debug, std::clone::Clone)]
pub enum Side {
    Source,
    Destination {
        src_endpoint: std::net::SocketAddr,
        server_name: String,
    },
}

impl std::str::FromStr for Side {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('@').collect();
        match parts.as_slice() {
            ["source"] | ["src"] => Ok(Side::Source),
            ["destination", rest] | ["dst", rest] => {
                let addr_parts: Vec<&str> = rest.split(',').collect();
                match addr_parts.as_slice() {
                    [addr, server_name] => {
                        let endpoint = addr.parse().map_err(|e| {
                            anyhow::anyhow!("Invalid endpoint address '{}': {}", addr, e)
                        })?;
                        Ok(Side::Destination {
                            src_endpoint: endpoint,
                            server_name: server_name.to_string(),
                        })
                    }
                    _ => Err(anyhow::anyhow!(
                        "Destination format must include server name: 'destination@<host:port>,<server_name>'"
                    )),
                }
            }
            _ => Err(anyhow::anyhow!(
                "Invalid side format: must be 'source' ('src') or 'destination@<host:port>,<server_name>'"
            )),
        }
    }
}

async fn run_rcpd(host: &str, port: u16, side: &str) -> anyhow::Result<()> {
    // Create SSH session using openssh
    let session = openssh::Session::connect(
        format!("{}:{}", host, port),
        openssh::KnownHosts::Accept,
    )
    .await
    .context("Failed to establish SSH connection")?;

    // Run rcpd command remotely
    let child = session
        .command("rcpd")
        .arg("--side")
        .arg(side)
        .spawn()
        .await
        .context("Failed to spawn rcpd command")?;

    // Wait for command completion
    let status = child
        .wait()
        .await
        .context("Failed to wait for rcpd completion")?;

    if !status.success() {
        return Err(anyhow!("rcpd command failed on remote host"));
    }

    Ok(())
}
