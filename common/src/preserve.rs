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

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
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

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FileSettings {
    pub user_and_time: UserAndTimeSettings,
    pub mode_mask: ModeMask,
    /// Preserve the POSIX.1e access ACL (`system.posix_acl_access`).
    ///
    /// Opt-in, and deliberately not part of [`preserve_all`] — see the comment there.
    pub acl: bool,
}

impl Default for FileSettings {
    fn default() -> Self {
        Self {
            user_and_time: UserAndTimeSettings::default(),
            mode_mask: 0o0777, // remove sticky bit, setuid and setgid to mimic "cp" tool
            acl: false,
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DirSettings {
    pub user_and_time: UserAndTimeSettings,
    pub mode_mask: ModeMask,
    /// Preserve the POSIX.1e access *and* default ACLs (`system.posix_acl_access` and
    /// `system.posix_acl_default`).
    ///
    /// Opt-in, and deliberately not part of [`preserve_all`] — see the comment there.
    pub acl: bool,
}

impl Default for DirSettings {
    fn default() -> Self {
        Self {
            user_and_time: UserAndTimeSettings::default(),
            mode_mask: 0o0777,
            acl: false,
        }
    }
}

/// Symlink preserve settings.
///
/// There is deliberately no `acl` field: the kernel has no symlink ACL, so asking to preserve one
/// is a request that could only ever be silently ignored. The settings parser rejects `l:acl`
/// instead.
#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct SymlinkSettings {
    pub user_and_time: UserAndTimeSettings,
}

impl SymlinkSettings {
    #[must_use]
    pub fn any(&self) -> bool {
        self.user_and_time.any()
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Settings {
    pub file: FileSettings,
    pub dir: DirSettings,
    pub symlink: SymlinkSettings,
}

impl Settings {
    /// Turn ACL preservation on (or off) for files and directories.
    ///
    /// Symlinks are untouched — see [`SymlinkSettings`]. This is the single place that knows which
    /// object types can carry an ACL, so the `all+acl` preset, the `none+acl` preset and
    /// [`preserve_all_with_acls`] cannot drift apart.
    #[must_use]
    pub fn with_acl(mut self, acl: bool) -> Self {
        self.file.acl = acl;
        self.dir.acl = acl;
        self
    }

    /// Whether this run asks for any metadata fidelity beyond what the tools ship with.
    ///
    /// The shipped default reproduces the source's `rwx` bits like `cp` and nothing else — no uid,
    /// no gid, no timestamps, no setuid/setgid/sticky. A run that stays there is not making a claim
    /// about the destination's metadata, so advice about metadata it also does not carry (see
    /// [`crate::safedir::RootAclNotice`]) is noise. Asking for *anything* more — `--preserve`,
    /// `all`, a bare `f:uid`, a `7777` mode mask — is that claim, and the advice becomes worth
    /// printing.
    ///
    /// Compared as a whole value rather than field by field, so a future field is covered the
    /// moment it exists. It reads the RESOLVED settings, so `--preserve-settings=none` — and any
    /// spelling that lands back on the default, such as `f:0777 d:0777` — counts as "did not ask",
    /// which is what those settings mean.
    #[must_use]
    pub fn requests_preservation(&self) -> bool {
        *self != Self::default()
    }
}

/// Compute the permission bits to apply, honoring the mode mask.
///
/// When `mode_mask == 0o7777` (the "preserve everything" case) the source mode
/// is returned verbatim, including setuid/setgid/sticky bits. Otherwise the mode
/// is masked with `mode_mask` (e.g. the default `0o0777` strips the special
/// bits, mimicking `cp`). The returned value is always confined to the
/// permission bits (`0o7777`); file-type bits are never included.
///
/// This is the single source of truth for the mode the fd-based metadata appliers in
/// `crate::safedir` apply. It does NOT decide the mode a destination is *created* at: destinations
/// are always created owner-only ([`crate::safedir::DST_FILE_CREATE_MODE`] /
/// [`crate::safedir::DST_DIR_CREATE_MODE`]) and only widened to this value once their contents are
/// written. Files and directories share this logic; pass the relevant `mode_mask`
/// ([`FileSettings::mode_mask`] or [`DirSettings::mode_mask`]).
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

/// Preserve uid, gid, timestamps and the full mode (including setuid/setgid/sticky).
///
/// This does **not** preserve ACLs; use [`preserve_all_with_acls`], equivalent to the `all+acl`
/// settings string, for that.
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
            // `all` deliberately does NOT preserve ACLs, and this stays spelled out rather than
            // riding on `..Default::default()`. There is no bit in `stat` saying whether an entry
            // has an ACL, so merely finding out costs an extra syscall on every single entry -
            // roughly doubling the per-entry metadata cost of the flag most people reach for by
            // default. ACLs are therefore opt-in via `all+acl` / `preserve_all_with_acls`.
            acl: false,
        },
        dir: DirSettings {
            user_and_time,
            mode_mask: 0o7777,
            // see the file settings above: opt-in for the same per-entry cost reason (and
            // directories pay for two ACLs, access and default)
            acl: false,
        },
        symlink: SymlinkSettings { user_and_time },
    }
}

/// [`preserve_all`] plus POSIX ACLs on files and directories, equivalent to the `all+acl` settings
/// string.
///
/// The settings parser does not call this — it applies the `+acl` modifier generically to whichever
/// preset it saw. The two agree because both route through [`Settings::with_acl`].
#[must_use]
pub fn preserve_all_with_acls() -> Settings {
    preserve_all().with_acl(true)
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
    #[test]
    fn shipped_defaults_and_preserve_all_leave_acls_off() {
        // ACLs are opt-in because detecting one costs a syscall per entry; a silent flip here would
        // impose that cost on every default copy
        assert!(!FileSettings::default().acl);
        assert!(!DirSettings::default().acl);
        let all = preserve_all();
        assert!(!all.file.acl);
        assert!(!all.dir.acl);
        let none = preserve_none();
        assert!(!none.file.acl);
        assert!(!none.dir.acl);
    }
    #[test]
    fn preserve_all_with_acls_enables_acls_without_changing_anything_else() {
        // whole-value equality, so any extra field `with_acl` touches shows up here rather than
        // slipping past a hand-written list of assertions
        let mut expected = preserve_all();
        expected.file.acl = true;
        expected.dir.acl = true;
        assert_eq!(preserve_all_with_acls(), expected);
    }
    #[test]
    fn with_acl_touches_only_the_acl_fields() {
        let mut expected = preserve_none();
        expected.file.acl = true;
        expected.dir.acl = true;
        assert_eq!(preserve_none().with_acl(true), expected);
    }
    #[test]
    fn with_acl_is_idempotent_and_reversible() {
        assert_eq!(
            preserve_none().with_acl(true).with_acl(true),
            preserve_none().with_acl(true)
        );
        assert_eq!(preserve_all_with_acls().with_acl(false), preserve_all());
    }
    #[test]
    fn the_shipped_default_requests_no_preservation() {
        // `none` IS the shipped default, so both spellings of "I did not ask" agree
        assert!(!Settings::default().requests_preservation());
        assert!(!preserve_none().requests_preservation());
    }
    #[test]
    fn any_attribute_beyond_the_default_requests_preservation() {
        // one field at a time, so a predicate that happened to key off only `uid` (or only the
        // mode mask) fails here rather than in whatever consumes it
        for settings in [
            preserve_all(),
            preserve_all_with_acls(),
            preserve_none().with_acl(true),
            Settings {
                file: FileSettings {
                    user_and_time: UserAndTimeSettings {
                        uid: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
            Settings {
                dir: DirSettings {
                    user_and_time: UserAndTimeSettings {
                        time: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
            Settings {
                symlink: SymlinkSettings {
                    user_and_time: UserAndTimeSettings {
                        gid: true,
                        ..Default::default()
                    },
                },
                ..Default::default()
            },
            Settings {
                file: FileSettings {
                    mode_mask: 0o7777,
                    ..Default::default()
                },
                ..Default::default()
            },
        ] {
            assert!(
                settings.requests_preservation(),
                "{settings:?} asks for more than the shipped default but reads as 'did not ask'"
            );
        }
    }
    #[test]
    fn settings_that_spell_out_the_default_request_no_preservation() {
        // the predicate reads the RESOLVED value, so writing the default out longhand is the same
        // as not writing it at all — otherwise `f:0777 d:0777` would behave unlike a bare run
        let spelled_out = Settings {
            file: FileSettings {
                mode_mask: 0o0777,
                ..Default::default()
            },
            dir: DirSettings {
                mode_mask: 0o0777,
                ..Default::default()
            },
            symlink: SymlinkSettings::default(),
        };
        assert!(!spelled_out.requests_preservation());
    }
}
