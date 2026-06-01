use serde::{Deserialize, Serialize};
use std::os::unix::fs::MetadataExt;
use std::os::unix::prelude::PermissionsExt;

pub trait Metadata {
    fn uid(&self) -> u32;
    fn gid(&self) -> u32;
    fn atime(&self) -> i64;
    fn atime_nsec(&self) -> i64;
    fn mtime(&self) -> i64;
    fn mtime_nsec(&self) -> i64;
    fn permissions(&self) -> std::fs::Permissions;
    // ctime cannot be set manually, but we include it for comparison purposes
    // default implementation returns 0 to indicate ctime is not available (e.g., in protocol::Metadata)
    fn ctime(&self) -> i64 {
        0
    }
    fn ctime_nsec(&self) -> i64 {
        0
    }
    // size is not preserved (cannot be set), but included for comparison purposes
    // default implementation returns 0 to indicate size is not available or not applicable
    fn size(&self) -> u64 {
        0
    }
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
    fn ctime(&self) -> i64 {
        MetadataExt::ctime(self)
    }
    fn ctime_nsec(&self) -> i64 {
        MetadataExt::ctime_nsec(self)
    }
    fn size(&self) -> u64 {
        self.len()
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize)]
pub struct UserAndTimeSettings {
    pub uid: bool,
    pub gid: bool,
    pub time: bool,
}

impl UserAndTimeSettings {
    #[must_use]
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
    #[must_use]
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

/// Compute the file permission bits to apply, honoring the mode mask.
///
/// When `mode_mask == 0o7777` (the "preserve everything" case) the source mode
/// is returned verbatim, including setuid/setgid/sticky bits. Otherwise the mode
/// is masked with `mode_mask` (e.g. the default `0o0777` strips the special
/// bits, mimicking `cp`). The returned value is always confined to the
/// permission bits (`0o7777`); file-type bits are never included.
///
/// This is the single source of truth for the destination create-mode and the fd-based metadata
/// appliers in `crate::safedir`.
#[must_use]
pub fn masked_file_mode<Meta: Metadata>(settings: &FileSettings, metadata: &Meta) -> u32 {
    let mode = metadata.permissions().mode();
    if settings.mode_mask == 0o7777 {
        // special case for default preserve: keep all permission bits verbatim
        mode & 0o7777
    } else {
        mode & settings.mode_mask
    }
}

/// Compute the directory permission bits to apply, honoring the mode mask.
///
/// Mirrors [`masked_file_mode`] for directories. See that function for details.
#[must_use]
pub fn masked_dir_mode<Meta: Metadata>(settings: &DirSettings, metadata: &Meta) -> u32 {
    let mode = metadata.permissions().mode();
    if settings.mode_mask == 0o7777 {
        // special case for default preserve: keep all permission bits verbatim
        mode & 0o7777
    } else {
        mode & settings.mode_mask
    }
}

#[must_use]
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

#[must_use]
pub fn preserve_none() -> Settings {
    Settings::default()
}
