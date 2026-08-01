//! Parsers for the CLI string-setting DSLs shared by the tools: `--preserve` /
//! `--metadata-compare` / `--compare`, plus the `--update` compare-vs-preserve validation. These
//! are pure `&str -> typed settings` functions with no dependency on the runtime/progress
//! machinery; they are re-exported from the crate root (`common::parse_*`).

use crate::cmp::{self, ObjType};
use crate::{filecmp, preserve};
use anyhow::{Context, anyhow};

pub fn parse_metadata_cmp_settings(
    settings: &str,
) -> Result<filecmp::MetadataCmpSettings, anyhow::Error> {
    let mut metadata_cmp_settings = filecmp::MetadataCmpSettings::default();
    for setting in settings.split(',') {
        match setting {
            "uid" => metadata_cmp_settings.uid = true,
            "gid" => metadata_cmp_settings.gid = true,
            "mode" => metadata_cmp_settings.mode = true,
            "size" => metadata_cmp_settings.size = true,
            "mtime" => metadata_cmp_settings.mtime = true,
            "ctime" => metadata_cmp_settings.ctime = true,
            _ => {
                return Err(anyhow!("Unknown metadata comparison setting: {}", setting));
            }
        }
    }
    Ok(metadata_cmp_settings)
}

/// The attributes parsed out of one `<type>:<attrs>` group of the per-type DSL.
struct TypeSettings {
    user_and_time: preserve::UserAndTimeSettings,
    /// `None` when the group named no octal mask, meaning "keep the type's default mask".
    mode_mask: Option<preserve::ModeMask>,
    acl: bool,
}

fn parse_type_settings(settings: &str) -> Result<TypeSettings, anyhow::Error> {
    let mut user_and_time = preserve::UserAndTimeSettings::default();
    let mut mode_mask = None;
    let mut acl = false;
    for setting in settings.split(',') {
        match setting {
            "uid" => user_and_time.uid = true,
            "gid" => user_and_time.gid = true,
            "time" => user_and_time.time = true,
            "acl" => acl = true,
            _ => {
                if let Ok(mask) = u32::from_str_radix(setting, 8) {
                    mode_mask = Some(mask);
                } else {
                    return Err(anyhow!("Unknown preserve attribute specified: {}", setting));
                }
            }
        }
    }
    Ok(TypeSettings {
        user_and_time,
        mode_mask,
        acl,
    })
}

/// Parse the `all` / `none` preset form, including `+`-separated modifiers such as `all+acl`.
///
/// Returns `Ok(None)` when `settings` is not a preset at all, which is decided *only* by its first
/// `+`-separated token: anything other than `all` or `none` there means the caller should fall
/// through to the per-type DSL, where `+` is an ordinary character with no special meaning.
fn parse_preset_settings(settings: &str) -> Result<Option<preserve::Settings>, anyhow::Error> {
    // the modifiers this loop accepts, named in the error so the two cannot drift
    const PRESET_MODIFIERS: &str = "acl";
    let mut tokens = settings.split('+');
    // `split` always yields at least one item
    let mut preset = match tokens.next().unwrap_or_default() {
        "all" => preserve::preserve_all(),
        "none" => preserve::preserve_none(),
        _ => return Ok(None),
    };
    for modifier in tokens {
        match modifier {
            // repeats are idempotent, deliberately: `all+acl+acl` is not worth an error
            "acl" => preset = preset.with_acl(true),
            _ => {
                // quoted, so a modifier that is empty or carries stray whitespace (`"all+acl "`
                // out of a config file) reads as the mistake it is rather than as nonsense
                return Err(anyhow!(
                    "Unknown preserve preset modifier: {modifier:?}, expected one of: {PRESET_MODIFIERS}"
                ));
            }
        }
    }
    Ok(Some(preset))
}

/// An ACL *is* the permission state - the kernel derives the mode's group bits from the ACL's
/// `MASK` entry - so asking to preserve an ACL while masking away some of the rwx bits is a
/// contradiction: the mask says "strip this", the ACL puts it straight back.
///
/// The rule is exactly `mask & 0o777 != 0o777`. Masks that strip only the *special* bits are
/// orthogonal to the ACL (which carries no setuid/setgid/sticky) and stay legal - that includes
/// both the shipped default `0o0777` and the `all` preset's `0o7777`.
fn check_acl_vs_mode_mask(settings: &preserve::Settings) -> Result<(), anyhow::Error> {
    for (obj_type, acl, mode_mask) in [
        ("file", settings.file.acl, settings.file.mode_mask),
        ("directory", settings.dir.acl, settings.dir.mode_mask),
    ] {
        if acl && mode_mask & 0o777 != 0o777 {
            return Err(anyhow!(
                "Preserve attribute `acl` conflicts with mode mask {mode_mask:04o} for type \
                 `{obj_type}`: an ACL carries the rwx permission bits, so a mask that narrows them \
                 would contradict it. Use a mask whose low 3 digits are 777 (e.g. 0777 or 7777)."
            ));
        }
    }
    Ok(())
}

/// Parse a `--preserve-settings` string.
///
/// Two grammars: the `all` / `none` presets with optional `+`-separated modifiers (`all+acl`), and
/// the per-type DSL (`f:uid,gid,time,acl,7777 d:uid,gid,time l:uid,gid,time`). A string is a preset
/// only when its first `+`-token is `all` or `none`; every other string is per-type DSL.
pub fn parse_preserve_settings(settings: &str) -> Result<preserve::Settings, anyhow::Error> {
    let preserve_settings = match parse_preset_settings(settings)? {
        Some(preset) => preset,
        None => parse_per_type_settings(settings)?,
    };
    check_acl_vs_mode_mask(&preserve_settings)?;
    Ok(preserve_settings)
}

fn parse_per_type_settings(settings: &str) -> Result<preserve::Settings, anyhow::Error> {
    let mut preserve_settings = preserve::Settings::default();
    for type_settings in settings.split_whitespace() {
        if let Some((obj_type, obj_settings)) = type_settings.split_once(':') {
            let parsed = parse_type_settings(obj_settings).context(format!(
                "parsing preserve settings: {obj_settings}, type: {obj_type}"
            ))?;
            match obj_type {
                "f" | "file" => {
                    preserve_settings.file = preserve::FileSettings::default();
                    preserve_settings.file.user_and_time = parsed.user_and_time;
                    preserve_settings.file.acl = parsed.acl;
                    if let Some(mode) = parsed.mode_mask {
                        preserve_settings.file.mode_mask = mode;
                    }
                }
                "d" | "dir" | "directory" => {
                    preserve_settings.dir = preserve::DirSettings::default();
                    preserve_settings.dir.user_and_time = parsed.user_and_time;
                    preserve_settings.dir.acl = parsed.acl;
                    if let Some(mode) = parsed.mode_mask {
                        preserve_settings.dir.mode_mask = mode;
                    }
                }
                "l" | "link" | "symlink" => {
                    // accepting `l:acl` and quietly doing nothing would be the same silent lie ACL
                    // preservation exists to remove, so it is an error instead
                    if parsed.acl {
                        return Err(anyhow!(
                            "Preserve attribute `acl` is not valid for type `{obj_type}`: symlinks \
                             cannot carry ACLs (the kernel has no symlink ACL)"
                        ));
                    }
                    preserve_settings.symlink = preserve::SymlinkSettings::default();
                    preserve_settings.symlink.user_and_time = parsed.user_and_time;
                }
                _ => {
                    return Err(anyhow!("Unknown object type: {}", obj_type));
                }
            }
        } else {
            return Err(anyhow!("Invalid preserve settings: {}", settings));
        }
    }
    Ok(preserve_settings)
}

/// Validates that every attribute checked by --update's comparison is actually being preserved.
/// Skips size (always preserved via content copy) and ctime (kernel-managed, cannot be set).
pub fn validate_update_compare_vs_preserve(
    update_compare: &filecmp::MetadataCmpSettings,
    preserve: &preserve::Settings,
) -> Result<(), String> {
    let mut missing = Vec::new();
    if update_compare.mtime && !preserve.file.user_and_time.time {
        missing.push("mtime");
    }
    if update_compare.uid && !preserve.file.user_and_time.uid {
        missing.push("uid");
    }
    if update_compare.gid && !preserve.file.user_and_time.gid {
        missing.push("gid");
    }
    // metadata_equal compares full mode (0o7777), so a partial mask is lossy
    if update_compare.mode && preserve.file.mode_mask != 0o7777 {
        missing.push("mode");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "--update compares [{}] but --preserve-settings does not preserve them. \
             Use --allow-lossy-update to override or adjust --preserve-settings.",
            missing.join(", ")
        ))
    }
}

pub fn parse_compare_settings(settings: &str) -> Result<cmp::ObjSettings, anyhow::Error> {
    let mut cmp_settings = cmp::ObjSettings::default();
    for type_settings in settings.split_whitespace() {
        if let Some((obj_type, obj_settings)) = type_settings.split_once(':') {
            let obj_cmp_settings = parse_metadata_cmp_settings(obj_settings).context(format!(
                "parsing compare settings: {obj_settings}, type: {obj_type}"
            ))?;
            let obj_type = match obj_type {
                "f" | "file" => ObjType::File,
                "d" | "dir" | "directory" => ObjType::Dir,
                "l" | "link" | "symlink" => ObjType::Symlink,
                "o" | "other" => ObjType::Other,
                _ => {
                    return Err(anyhow!("Unknown obj type: {}", obj_type));
                }
            };
            cmp_settings[obj_type] = obj_cmp_settings;
        } else {
            return Err(anyhow!("Invalid compare settings: {}", settings));
        }
    }
    Ok(cmp_settings)
}

#[cfg(test)]
mod parse_preserve_settings_tests {
    use super::*;
    #[test]
    fn preset_all_returns_preserve_all() {
        let settings = parse_preserve_settings("all").unwrap();
        let expected = preserve::preserve_all();
        assert_eq!(settings.file.mode_mask, expected.file.mode_mask);
        assert!(settings.file.user_and_time.uid);
        assert!(settings.file.user_and_time.gid);
        assert!(settings.file.user_and_time.time);
        assert_eq!(settings.dir.mode_mask, expected.dir.mode_mask);
        assert!(settings.dir.user_and_time.uid);
        assert!(settings.dir.user_and_time.gid);
        assert!(settings.dir.user_and_time.time);
        assert!(settings.symlink.user_and_time.uid);
        assert!(settings.symlink.user_and_time.gid);
        assert!(settings.symlink.user_and_time.time);
    }
    #[test]
    fn preset_none_returns_preserve_none() {
        let settings = parse_preserve_settings("none").unwrap();
        let expected = preserve::preserve_none();
        assert_eq!(settings.file.mode_mask, expected.file.mode_mask);
        assert!(!settings.file.user_and_time.uid);
        assert!(!settings.file.user_and_time.gid);
        assert!(!settings.file.user_and_time.time);
        assert_eq!(settings.dir.mode_mask, expected.dir.mode_mask);
        assert!(!settings.dir.user_and_time.uid);
        assert!(!settings.dir.user_and_time.gid);
        assert!(!settings.dir.user_and_time.time);
        assert!(!settings.symlink.user_and_time.uid);
        assert!(!settings.symlink.user_and_time.gid);
        assert!(!settings.symlink.user_and_time.time);
    }
    #[test]
    fn per_type_settings_still_work() {
        let settings = parse_preserve_settings("f:uid,time,0777 d:gid").unwrap();
        assert!(settings.file.user_and_time.uid);
        assert!(settings.file.user_and_time.time);
        assert!(!settings.file.user_and_time.gid);
        assert_eq!(settings.file.mode_mask, 0o777);
        assert!(!settings.dir.user_and_time.uid);
        assert!(settings.dir.user_and_time.gid);
        assert!(!settings.dir.user_and_time.time);
    }
    #[test]
    fn invalid_settings_returns_error() {
        assert!(parse_preserve_settings("invalid").is_err());
        assert!(parse_preserve_settings("f:unknown_attr").is_err());
    }
    #[test]
    fn preset_all_does_not_enable_acls() {
        // decision D2: ACLs cost a syscall per entry to even detect, so `all` must not opt in
        let settings = parse_preserve_settings("all").unwrap();
        assert!(!settings.file.acl);
        assert!(!settings.dir.acl);
    }
    #[test]
    fn preset_all_plus_acl_enables_acls_on_files_and_dirs() {
        let settings = parse_preserve_settings("all+acl").unwrap();
        assert!(settings.file.acl);
        assert!(settings.dir.acl);
        // symlinks carry no ACL at all, so `SymlinkSettings` has no field to set; the rest of the
        // `all` preset must still reach them
        assert!(settings.symlink.user_and_time.uid);
        assert!(settings.symlink.user_and_time.gid);
        assert!(settings.symlink.user_and_time.time);
        // and the modifier must not disturb the preset it modifies
        let expected = preserve::preserve_all_with_acls();
        assert_eq!(settings.file.mode_mask, expected.file.mode_mask);
        assert_eq!(settings.dir.mode_mask, expected.dir.mode_mask);
        assert!(settings.file.user_and_time.time);
        assert!(settings.dir.user_and_time.time);
    }
    #[test]
    fn preset_none_plus_acl_enables_only_acls() {
        // odd but coherent: copy ACLs without ownership or timestamps
        let settings = parse_preserve_settings("none+acl").unwrap();
        assert!(settings.file.acl);
        assert!(settings.dir.acl);
        assert!(!settings.file.user_and_time.any());
        assert!(!settings.dir.user_and_time.any());
        assert!(!settings.symlink.user_and_time.any());
        assert_eq!(
            settings.file.mode_mask,
            preserve::preserve_none().file.mode_mask
        );
    }
    #[test]
    fn repeated_preset_modifier_is_idempotent() {
        let settings = parse_preserve_settings("all+acl+acl").unwrap();
        assert!(settings.file.acl);
        assert!(settings.dir.acl);
    }
    #[test]
    fn unknown_preset_modifier_names_the_valid_set() {
        let err = parse_preserve_settings("all+xattr")
            .unwrap_err()
            .to_string();
        assert!(err.contains("xattr"), "{err}");
        assert!(err.contains("acl"), "{err}");
        assert!(parse_preserve_settings("none+bogus").is_err());
        // an empty modifier is not silently accepted either
        assert!(parse_preserve_settings("all+").is_err());
    }
    #[test]
    fn plus_stays_an_ordinary_character_in_the_per_type_dsl() {
        // `+` is only special when the FIRST token is a preset name; `u32::from_str_radix` accepts a
        // leading `+`, so this string parses as a plain mode mask today and must keep doing so
        let settings = parse_preserve_settings("f:+777").unwrap();
        assert_eq!(settings.file.mode_mask, 0o777);
        assert!(!settings.file.acl);
        // and a `+`-containing string that was a per-type-DSL error stays that error, rather than
        // being reinterpreted as a preset modifier
        let err = parse_preserve_settings("f:uid+d:gid")
            .unwrap_err()
            .to_string();
        assert!(!err.contains("preset modifier"), "{err}");
    }
    #[test]
    fn per_type_acl_attribute_parses() {
        let settings =
            parse_preserve_settings("f:uid,gid,time,acl,7777 d:uid,gid,time,acl,7777").unwrap();
        assert!(settings.file.acl);
        assert_eq!(settings.file.mode_mask, 0o7777);
        assert!(settings.file.user_and_time.uid);
        assert!(settings.dir.acl);
        assert_eq!(settings.dir.mode_mask, 0o7777);
        // a type not named keeps its default, ACLs off
        assert!(!settings.symlink.user_and_time.any());
    }
    #[test]
    fn acl_on_symlinks_is_rejected() {
        for spec in ["l:acl", "link:acl", "symlink:acl", "l:uid,acl"] {
            let err = parse_preserve_settings(spec).unwrap_err().to_string();
            assert!(err.contains("acl"), "{spec}: {err}");
            assert!(err.contains("symlink"), "{spec}: {err}");
        }
    }
    #[test]
    fn acl_with_a_mask_that_narrows_rwx_is_rejected() {
        for spec in [
            "f:acl,0700", // strips group and other
            "f:acl,0770", // strips other only
            "f:acl,7000", // strips all rwx
            "d:acl,0755", // dirs are checked too
            "f:acl,0777 d:acl,0700",
        ] {
            let err = parse_preserve_settings(spec).unwrap_err().to_string();
            assert!(err.contains("acl"), "{spec}: {err}");
            assert!(err.contains("mode mask"), "{spec}: {err}");
        }
    }
    #[test]
    fn acl_with_a_mask_that_only_strips_special_bits_is_accepted() {
        // the ACL carries no setuid/setgid/sticky, so those bits are orthogonal to it
        assert!(parse_preserve_settings("f:acl,7777").unwrap().file.acl);
        assert!(parse_preserve_settings("f:acl,0777").unwrap().file.acl);
        // bare `f:acl` inherits the shipped default mask 0o0777, which must also pass
        let bare = parse_preserve_settings("f:acl").unwrap();
        assert!(bare.file.acl);
        assert!(!bare.dir.acl);
        assert_eq!(bare.file.mode_mask, 0o0777);
        // as must both presets
        assert!(parse_preserve_settings("all+acl").is_ok());
        assert!(parse_preserve_settings("none+acl").is_ok());
    }
    #[test]
    fn per_type_acl_applies_only_to_the_named_type() {
        // `acl` leaking from one type to the other is decision D2's cost charged to a user who did
        // not ask for it: `f:acl` enabling directory ACLs spends 2 extra syscalls per directory
        let file_only = parse_preserve_settings("f:acl").unwrap();
        assert!(file_only.file.acl);
        assert!(!file_only.dir.acl);
        let dir_only = parse_preserve_settings("d:acl").unwrap();
        assert!(dir_only.dir.acl);
        assert!(!dir_only.file.acl);
        // and naming a type without `acl` alongside one with it must not pick it up
        let mixed = parse_preserve_settings("f:acl d:uid,gid").unwrap();
        assert!(mixed.file.acl);
        assert!(!mixed.dir.acl);
    }
    #[test]
    fn narrowing_mask_without_acl_is_still_accepted() {
        // the rejection is about the combination; masks alone are unconstrained
        assert_eq!(
            parse_preserve_settings("f:uid,0700")
                .unwrap()
                .file
                .mode_mask,
            0o700
        );
    }
}

#[cfg(test)]
mod validate_update_compare_vs_preserve_tests {
    use super::*;
    #[test]
    fn detects_mtime_mismatch() {
        let compare = filecmp::MetadataCmpSettings {
            mtime: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mtime"));
    }
    #[test]
    fn detects_uid_mismatch() {
        let compare = filecmp::MetadataCmpSettings {
            uid: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("uid"));
    }
    #[test]
    fn detects_gid_mismatch() {
        let compare = filecmp::MetadataCmpSettings {
            gid: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("gid"));
    }
    #[test]
    fn detects_mode_mismatch() {
        let compare = filecmp::MetadataCmpSettings {
            mode: true,
            ..Default::default()
        };
        let mut preserve = preserve::preserve_none();
        preserve.file.mode_mask = 0;
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mode"));
    }
    #[test]
    fn detects_multiple_mismatches() {
        let compare = filecmp::MetadataCmpSettings {
            mtime: true,
            uid: true,
            gid: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("mtime"));
        assert!(err.contains("uid"));
        assert!(err.contains("gid"));
    }
    #[test]
    fn passes_when_preserve_covers_all_compared_attrs() {
        let compare = filecmp::MetadataCmpSettings {
            mtime: true,
            uid: true,
            gid: true,
            mode: true,
            size: true,  // always preserved, should not cause error
            ctime: true, // kernel-managed, should not cause error
        };
        let preserve = preserve::preserve_all();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_ok());
    }
    #[test]
    fn fails_with_partial_mode_mask_when_mode_compared() {
        // default mode_mask is 0o0777 which drops setuid/setgid/sticky bits,
        // but metadata_equal compares full mode (0o7777) — so this is lossy
        let compare = filecmp::MetadataCmpSettings {
            mode: true,
            ..Default::default()
        };
        let preserve = preserve::preserve_none();
        let result = validate_update_compare_vs_preserve(&compare, &preserve);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mode"));
    }
}
