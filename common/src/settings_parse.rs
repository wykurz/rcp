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

fn parse_type_settings(
    settings: &str,
) -> Result<(preserve::UserAndTimeSettings, Option<preserve::ModeMask>), anyhow::Error> {
    let mut user_and_time = preserve::UserAndTimeSettings::default();
    let mut mode_mask = None;
    for setting in settings.split(',') {
        match setting {
            "uid" => user_and_time.uid = true,
            "gid" => user_and_time.gid = true,
            "time" => user_and_time.time = true,
            _ => {
                if let Ok(mask) = u32::from_str_radix(setting, 8) {
                    mode_mask = Some(mask);
                } else {
                    return Err(anyhow!("Unknown preserve attribute specified: {}", setting));
                }
            }
        }
    }
    Ok((user_and_time, mode_mask))
}

pub fn parse_preserve_settings(settings: &str) -> Result<preserve::Settings, anyhow::Error> {
    // handle presets
    match settings {
        "all" => return Ok(preserve::preserve_all()),
        "none" => return Ok(preserve::preserve_none()),
        _ => {}
    }
    let mut preserve_settings = preserve::Settings::default();
    for type_settings in settings.split_whitespace() {
        if let Some((obj_type, obj_settings)) = type_settings.split_once(':') {
            let (user_and_time_settings, mode_opt) = parse_type_settings(obj_settings).context(
                format!("parsing preserve settings: {obj_settings}, type: {obj_type}"),
            )?;
            match obj_type {
                "f" | "file" => {
                    preserve_settings.file = preserve::FileSettings::default();
                    preserve_settings.file.user_and_time = user_and_time_settings;
                    if let Some(mode) = mode_opt {
                        preserve_settings.file.mode_mask = mode;
                    }
                }
                "d" | "dir" | "directory" => {
                    preserve_settings.dir = preserve::DirSettings::default();
                    preserve_settings.dir.user_and_time = user_and_time_settings;
                    if let Some(mode) = mode_opt {
                        preserve_settings.dir.mode_mask = mode;
                    }
                }
                "l" | "link" | "symlink" => {
                    preserve_settings.symlink = preserve::SymlinkSettings::default();
                    preserve_settings.symlink.user_and_time = user_and_time_settings;
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
