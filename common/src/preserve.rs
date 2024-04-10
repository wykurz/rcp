use anyhow::{Context, Result};
use std::os::unix::fs::MetadataExt;
use std::os::unix::prelude::PermissionsExt;
use tracing::{event, instrument, Level};

#[derive(Copy, Clone, Debug, Default)]
pub struct UserAndTimeSettings {
    pub uid: bool,
    pub gid: bool,
    pub time: bool,
}

impl UserAndTimeSettings {
    pub fn any(&self) -> bool {
        self.uid || self.gid || self.time
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct FileSettings {
    pub user_and_time: UserAndTimeSettings,
    pub mode_mask: u32,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct DirSettings {
    pub user_and_time: UserAndTimeSettings,
    pub mode_mask: u32,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct SymlinkSettings {
    pub user_and_time: UserAndTimeSettings,
}

impl SymlinkSettings {
    pub fn any(&self) -> bool {
        self.user_and_time.any()
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct PreserveSettings {
    pub file: FileSettings,
    pub dir: DirSettings,
    pub symlink: SymlinkSettings,
}

#[instrument]
async fn set_owner_and_time(
    settings: &UserAndTimeSettings,
    path: &std::path::Path,
    metadata: &std::fs::Metadata,
) -> Result<()> {
    let settings = settings.to_owned();
    let dst = path.to_owned();
    let metadata = metadata.to_owned();
    tokio::task::spawn_blocking(move || -> Result<()> {
        // set timestamps first - those are unlikely to fail
        if settings.time {
            event!(Level::DEBUG, "setting timestamps");
            let atime = nix::sys::time::TimeSpec::new(metadata.atime(), metadata.atime_nsec());
            let mtime = nix::sys::time::TimeSpec::new(metadata.mtime(), metadata.mtime_nsec());
            nix::sys::stat::utimensat(
                None,
                &dst,
                &atime,
                &mtime,
                nix::sys::stat::UtimensatFlags::NoFollowSymlink,
            )
            .with_context(|| format!("failed setting timestamps for {:?}", &dst))?;
        }
        if settings.uid || settings.gid {
            // set user and group - set those last, if those fail we at least have the timestamps set
            event!(Level::DEBUG, "setting uid ang gid");
            let uid = if settings.uid {
                Some(metadata.uid().into())
            } else {
                None
            };
            let gid = if settings.gid {
                Some(metadata.gid().into())
            } else {
                None
            };
            nix::unistd::fchownat(
                None,
                &dst,
                uid,
                gid,
                nix::unistd::FchownatFlags::NoFollowSymlink,
            )
            .with_context(|| {
                format!(
                    "cannot set {:?} owner to {:?} and/or group id to {:?}",
                    &dst, &uid, &gid
                )
            })
            .map_err(anyhow::Error::from)?;
        }
        Ok(())
    })
    .await?
}

pub async fn set_file_permissions(
    settings: &PreserveSettings,
    metadata: &std::fs::Metadata,
    file: &tokio::fs::File,
    path: &std::path::Path,
) -> Result<()> {
    let permissions = if settings.file.mode_mask == 0o7777 {
        // special case for default preserve
        metadata.permissions()
    } else {
        std::fs::Permissions::from_mode(metadata.permissions().mode() & settings.file.mode_mask)
    };
    file.set_permissions(permissions.clone())
        .await
        .with_context(|| format!("cannot set {:?} permissions to {:?}", &path, &permissions))?;
    set_owner_and_time(&settings.file.user_and_time, path, metadata).await?;
    Ok(())
}

pub async fn set_dir_permissions(
    settings: &PreserveSettings,
    metadata: &std::fs::Metadata,
    path: &std::path::Path,
) -> Result<()> {
    let permissions = if settings.dir.mode_mask == 0o7777 {
        // special case for default preserve
        metadata.permissions()
    } else {
        std::fs::Permissions::from_mode(metadata.permissions().mode() & settings.dir.mode_mask)
    };
    tokio::fs::set_permissions(path, permissions.clone())
        .await
        .with_context(|| format!("cannot set {:?} permissions to {:?}", &path, &permissions))?;
    set_owner_and_time(&settings.dir.user_and_time, path, metadata).await?;
    Ok(())
}

pub async fn set_symlink_permissions(
    settings: &PreserveSettings,
    metadata: &std::fs::Metadata,
    path: &std::path::Path,
) -> Result<()> {
    // we don't set permissions for symlinks, only owner and time
    set_owner_and_time(&settings.file.user_and_time, path, metadata).await?;
    Ok(())
}

pub fn preserve_all() -> PreserveSettings {
    let user_and_time = UserAndTimeSettings {
        uid: true,
        gid: true,
        time: true,
    };

    PreserveSettings {
        file: FileSettings {
            user_and_time,
            mode_mask: 0o7777,
        },
        dir: DirSettings {
            user_and_time,
            mode_mask: 0o7777,
        },
        symlink: SymlinkSettings { user_and_time },
    }
}

pub fn preserve_default() -> PreserveSettings {
    let user_and_time = UserAndTimeSettings {
        uid: false,
        gid: false,
        time: false,
    };

    PreserveSettings {
        file: FileSettings {
            user_and_time,
            mode_mask: 0o0777, // remove sticky bit, setuid and setgid to mimic "cp" tool
        },
        dir: DirSettings {
            user_and_time,
            mode_mask: 0o0777,
        },
        symlink: SymlinkSettings { user_and_time },
    }
}
