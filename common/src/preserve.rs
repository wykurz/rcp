use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::os::unix::fs::MetadataExt;
use std::os::unix::prelude::PermissionsExt;
use tracing::instrument;

pub trait Metadata {
    fn uid(&self) -> u32;
    fn gid(&self) -> u32;
    fn atime(&self) -> i64;
    fn atime_nsec(&self) -> i64;
    fn mtime(&self) -> i64;
    fn mtime_nsec(&self) -> i64;
    fn permissions(&self) -> std::fs::Permissions;
}

impl Metadata for std::fs::Metadata {
    fn uid(&self) -> u32 {
        MetadataExt::uid(self)
    }
    fn gid(&self) -> u32 {
        MetadataExt::gid(self)
    }
    fn atime(&self) -> i64 {
        MetadataExt::atime(self)
    }
    fn atime_nsec(&self) -> i64 {
        MetadataExt::atime_nsec(self)
    }
    fn mtime(&self) -> i64 {
        MetadataExt::mtime(self)
    }
    fn mtime_nsec(&self) -> i64 {
        MetadataExt::mtime_nsec(self)
    }
    fn permissions(&self) -> std::fs::Permissions {
        self.permissions()
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize)]
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

pub type ModeMask = u32;

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct FileSettings {
    pub user_and_time: UserAndTimeSettings,
    pub mode_mask: ModeMask,
}

impl Default for FileSettings {
    fn default() -> Self {
        Self {
            user_and_time: UserAndTimeSettings::default(),
            mode_mask: 0o0777, // remove sticky bit, setuid and setgid to mimic "cp" tool
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct DirSettings {
    pub user_and_time: UserAndTimeSettings,
    pub mode_mask: ModeMask,
}

impl Default for DirSettings {
    fn default() -> Self {
        Self {
            user_and_time: UserAndTimeSettings::default(),
            mode_mask: 0o0777,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize)]
pub struct SymlinkSettings {
    pub user_and_time: UserAndTimeSettings,
}

impl SymlinkSettings {
    pub fn any(&self) -> bool {
        self.user_and_time.any()
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Settings {
    pub file: FileSettings,
    pub dir: DirSettings,
    pub symlink: SymlinkSettings,
}

#[instrument]
async fn set_owner_and_time<Meta: Metadata + std::fmt::Debug>(
    settings: &UserAndTimeSettings,
    path: &std::path::Path,
    metadata: &Meta,
) -> Result<()> {
    let settings = settings.to_owned();
    let dst = path.to_owned();
    let uid = metadata.uid();
    let gid = metadata.gid();
    let atime = metadata.atime();
    let atime_nsec = metadata.atime_nsec();
    let mtime = metadata.mtime();
    let mtime_nsec = metadata.mtime_nsec();
    tokio::task::spawn_blocking(move || -> Result<()> {
        if settings.uid || settings.gid {
            // set user and group
            tracing::debug!("setting uid ang gid");
            let uid_val = if settings.uid { Some(uid.into()) } else { None };
            let gid_val = if settings.gid { Some(gid.into()) } else { None };
            nix::unistd::fchownat(
                None,
                &dst,
                uid_val,
                gid_val,
                nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .with_context(|| {
                format!(
                    "cannot set {:?} owner to {:?} and/or group id to {:?}",
                    &dst, &uid_val, &gid_val
                )
            })?;
        }
        // set timestamps last - modifying other file metadata can change them
        if settings.time {
            tracing::debug!("setting timestamps");
            let atime_spec = nix::sys::time::TimeSpec::new(atime, atime_nsec);
            let mtime_spec = nix::sys::time::TimeSpec::new(mtime, mtime_nsec);
            nix::sys::stat::utimensat(
                None,
                &dst,
                &atime_spec,
                &mtime_spec,
                nix::sys::stat::UtimensatFlags::NoFollowSymlink,
            )
            .with_context(|| format!("failed setting timestamps for {:?}", &dst))?;
        }
        Ok(())
    })
    .await?
}

pub async fn set_file_metadata<Meta: Metadata + std::fmt::Debug>(
    settings: &Settings,
    metadata: &Meta,
    path: &std::path::Path,
) -> Result<()> {
    let permissions = if settings.file.mode_mask == 0o7777 {
        // special case for default preserve
        metadata.permissions()
    } else {
        std::fs::Permissions::from_mode(metadata.permissions().mode() & settings.file.mode_mask)
    };
    let file = tokio::fs::File::open(path).await?;
    file.set_permissions(permissions.clone())
        .await
        .with_context(|| format!("cannot set {:?} permissions to {:?}", &path, &permissions))?;
    // close the file we don't accidentally race and have permissions applied after the timestamps, which would modify them!
    drop(file);
    set_owner_and_time(&settings.file.user_and_time, path, metadata).await?;
    Ok(())
}

pub async fn set_dir_metadata<Meta: Metadata + std::fmt::Debug>(
    settings: &Settings,
    metadata: &Meta,
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

pub async fn set_symlink_metadata<Meta: Metadata + std::fmt::Debug>(
    settings: &Settings,
    metadata: &Meta,
    path: &std::path::Path,
) -> Result<()> {
    // we don't set permissions for symlinks, only owner and time
    set_owner_and_time(&settings.file.user_and_time, path, metadata).await?;
    Ok(())
}

pub fn preserve_all() -> Settings {
    let user_and_time = UserAndTimeSettings {
        uid: true,
        gid: true,
        time: true,
    };

    Settings {
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

pub fn preserve_default() -> Settings {
    Settings::default()
}
