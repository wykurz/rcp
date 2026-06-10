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

/// Compute the permission bits to apply, honoring the mode mask.
///
/// When `mode_mask == 0o7777` (the "preserve everything" case) the source mode
/// is returned verbatim, including setuid/setgid/sticky bits. Otherwise the mode
/// is masked with `mode_mask` (e.g. the default `0o0777` strips the special
/// bits, mimicking `cp`). The returned value is always confined to the
/// permission bits (`0o7777`); file-type bits are never included.
///
/// This is the single source of truth for the destination create-mode and the fd-based metadata
/// appliers in `crate::safedir`. Files and directories share this logic; pass the relevant
/// `mode_mask` ([`FileSettings::mode_mask`] or [`DirSettings::mode_mask`]).
#[must_use]
pub fn masked_mode<Meta: Metadata>(mode_mask: ModeMask, metadata: &Meta) -> u32 {
    // confine to permission bits up front so a user-supplied mask that itself includes file-type
    // (S_IF*) bits can never leak them into the create-mode / chmod target (`--preserve` parses the
    // mask as an arbitrary octal, with no upper bound)
    let mode = metadata.permissions().mode() & 0o7777;
    if mode_mask == 0o7777 {
        // default preserve keeps all permission bits verbatim
        mode
    } else {
        mode & mode_mask
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    struct FakeMeta {
        mode: u32,
    }
    impl Metadata for FakeMeta {
        fn uid(&self) -> u32 {
            0
        }
        fn gid(&self) -> u32 {
            0
        }
        fn atime(&self) -> i64 {
            0
        }
        fn atime_nsec(&self) -> i64 {
            0
        }
        fn mtime(&self) -> i64 {
            0
        }
        fn mtime_nsec(&self) -> i64 {
            0
        }
        fn permissions(&self) -> std::fs::Permissions {
            std::fs::Permissions::from_mode(self.mode)
        }
    }
    #[test]
    fn default_mask_strips_setuid_setgid_sticky() {
        // default mode_mask is 0o0777, which must drop the special bits like `cp`
        let meta = FakeMeta { mode: 0o7755 };
        assert_eq!(masked_mode(0o0777, &meta), 0o0755);
    }
    #[test]
    fn full_mask_preserves_special_bits_verbatim() {
        // 0o7777 is the "preserve everything" sentinel: setuid/setgid/sticky survive
        let meta = FakeMeta { mode: 0o4755 };
        assert_eq!(masked_mode(0o7777, &meta), 0o4755);
    }
    #[test]
    fn masking_strips_file_type_bits() {
        // source mode carries S_IFREG (0o100000); only permission bits may be returned
        let meta = FakeMeta { mode: 0o100644 };
        assert_eq!(masked_mode(0o7777, &meta), 0o0644);
        assert_eq!(masked_mode(0o0777, &meta), 0o0644);
    }
    #[test]
    fn out_of_range_mask_cannot_leak_file_type_bits() {
        // --preserve parses the mask as an arbitrary octal u32 (no upper bound), so a mask that
        // itself includes S_IF* bits must still not leak them into the returned mode
        let meta = FakeMeta { mode: 0o100644 };
        assert_eq!(masked_mode(0o100777, &meta), 0o0644);
    }
    #[test]
    fn custom_mask_applies() {
        let meta = FakeMeta { mode: 0o0777 };
        assert_eq!(masked_mode(0o0700, &meta), 0o0700);
    }
    #[test]
    fn shipped_defaults_strip_special_bits_while_preserve_all_keeps_them() {
        // pin the settings the tools ship with so a default change can't silently weaken fidelity
        assert_eq!(FileSettings::default().mode_mask, 0o0777);
        assert_eq!(DirSettings::default().mode_mask, 0o0777);
        let all = preserve_all();
        assert_eq!(all.file.mode_mask, 0o7777);
        assert_eq!(all.dir.mode_mask, 0o7777);
    }
}
