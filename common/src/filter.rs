//! Pattern-based file filtering for include/exclude operations
//!
//! This module provides glob pattern matching for filtering files during copy, link, and remove operations.
//!
//! # Pattern Syntax
//!
//! - `*` matches anything except `/`
//! - `**` matches anything including `/` (crosses directories)
//! - `?` matches a single character (except `/`)
//! - `[...]` character classes
//! - Leading `/` anchors to source root
//! - Trailing `/` matches only directories
//!
//! # Examples
//!
//! ```
//! use common::filter::{FilterSettings, FilterResult};
//! use std::path::Path;
//!
//! let mut settings = FilterSettings::default();
//! settings.add_exclude("*.log").unwrap();
//! settings.add_exclude("target/").unwrap();
//!
//! // .log files are excluded
//! assert!(matches!(
//!     settings.should_include(Path::new("debug.log"), false),
//!     FilterResult::ExcludedByPattern(_)
//! ));
//!
//! // other files are included
//! assert!(matches!(
//!     settings.should_include(Path::new("main.rs"), false),
//!     FilterResult::Included
//! ));
//! ```

use anyhow::{anyhow, Context};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::path::Path;

/// A compiled filter pattern with metadata about its original form
#[derive(Debug, Clone)]
pub struct FilterPattern {
    /// original pattern string for dry-run explain output
    pub original: String,
    /// compiled glob matcher
    matcher: globset::GlobMatcher,
    /// pattern ends with / (matches only directories)
    pub dir_only: bool,
    /// pattern starts with / (anchored to root)
    pub anchored: bool,
}

impl FilterPattern {
    /// Parse a pattern string into a FilterPattern
    pub fn parse(pattern: &str) -> Result<Self, anyhow::Error> {
        if pattern.is_empty() {
            return Err(anyhow!("empty pattern is not allowed"));
        }
        let original = pattern.to_string();
        let dir_only = pattern.ends_with('/');
        let anchored = pattern.starts_with('/');
        // strip leading/trailing markers for glob compilation
        let pattern_str = pattern.trim_start_matches('/').trim_end_matches('/');
        if pattern_str.is_empty() {
            return Err(anyhow!(
                "pattern '{}' results in empty glob after stripping / markers",
                pattern
            ));
        }
        // build glob with appropriate settings
        let glob = globset::GlobBuilder::new(pattern_str)
            .literal_separator(true) // * doesn't match /
            .build()
            .with_context(|| format!("invalid glob pattern: {}", pattern))?;
        let matcher = glob.compile_matcher();
        Ok(Self {
            original,
            matcher,
            dir_only,
            anchored,
        })
    }
    /// Check if this pattern contains path separators (excluding leading/trailing / markers).
    /// Path patterns require full path matching, while simple patterns can match filenames.
    fn is_path_pattern(&self) -> bool {
        // strip leading / (anchored marker) and trailing / (dir-only marker)
        let core = self.original.trim_start_matches('/').trim_end_matches('/');
        core.contains('/')
    }
    /// Check if this pattern matches the given path
    pub fn matches(&self, relative_path: &Path, is_dir: bool) -> bool {
        // directory-only patterns only match directories
        if self.dir_only && !is_dir {
            return false;
        }
        if self.anchored {
            // anchored patterns match from the root only
            self.matcher.is_match(relative_path)
        } else {
            // non-anchored patterns can match any component or the full path
            // first try full path match
            if self.matcher.is_match(relative_path) {
                return true;
            }
            // for non-anchored patterns, also try matching against just the filename
            // unless it's a path pattern (in which case full path match is required)
            if !self.is_path_pattern() {
                if let Some(file_name) = relative_path.file_name() {
                    if self.matcher.is_match(Path::new(file_name)) {
                        return true;
                    }
                }
            }
            false
        }
    }
}

/// Result of checking whether a path should be included
#[derive(Debug, Clone)]
pub enum FilterResult {
    /// path should be processed
    Included,
    /// path was excluded because include patterns exist but none matched
    ExcludedByDefault,
    /// path was excluded by a specific pattern
    ExcludedByPattern(String),
}

/// Settings for filtering files based on include/exclude patterns
#[derive(Debug, Clone, Default)]
pub struct FilterSettings {
    /// patterns for files to include (if non-empty, only matching files are included)
    pub includes: Vec<FilterPattern>,
    /// patterns for files to exclude
    pub excludes: Vec<FilterPattern>,
}

impl FilterSettings {
    /// Create new empty filter settings
    pub fn new() -> Self {
        Self::default()
    }
    /// Add an include pattern
    pub fn add_include(&mut self, pattern: &str) -> Result<(), anyhow::Error> {
        self.includes.push(FilterPattern::parse(pattern)?);
        Ok(())
    }
    /// Add an exclude pattern
    pub fn add_exclude(&mut self, pattern: &str) -> Result<(), anyhow::Error> {
        self.excludes.push(FilterPattern::parse(pattern)?);
        Ok(())
    }
    /// Check if this filter has any patterns
    pub fn is_empty(&self) -> bool {
        self.includes.is_empty() && self.excludes.is_empty()
    }
    /// Determine if a root item (the source itself) should be included based on filter patterns.
    ///
    /// This is a specialized version of `should_include` for root items (the source directory
    /// or file being copied). Anchored patterns (starting with `/`) are skipped because they
    /// are meant to match paths INSIDE the source root, not the source root itself.
    ///
    /// For root files, only non-anchored simple patterns (no `/`) can directly match the name.
    /// For root directories, they're always traversed if include patterns exist (to find
    /// matching content inside).
    ///
    /// For example, pattern `/bar` on source `foo/` should match `foo/bar`, not filter out `foo`.
    pub fn should_include_root_item(&self, name: &Path, is_dir: bool) -> FilterResult {
        // check excludes first - only non-anchored, simple patterns apply to root items
        // (patterns with path separators like `bar/baz` match content inside, not the root)
        // note: trailing `/` is a dir-only marker, not a path separator
        for pattern in &self.excludes {
            if !pattern.anchored
                && !Self::is_path_pattern(&pattern.original)
                && pattern.matches(name, is_dir)
            {
                return FilterResult::ExcludedByPattern(pattern.original.clone());
            }
        }
        // if there are include patterns...
        if !self.includes.is_empty() {
            // for root files, check if any non-anchored simple pattern matches
            if !is_dir {
                for pattern in &self.includes {
                    if !pattern.anchored
                        && !Self::is_path_pattern(&pattern.original)
                        && pattern.matches(name, false)
                    {
                        return FilterResult::Included;
                    }
                }
                // no simple pattern matched the root file
                return FilterResult::ExcludedByDefault;
            }
            // for root directories, always traverse to find matching content inside
            // (both anchored patterns like /bar and path patterns like bar/*.txt
            // need us to traverse the directory to find matches)
            return FilterResult::Included;
        }
        // no includes and not excluded = included
        FilterResult::Included
    }
    /// Check if a pattern is a path pattern (contains `/` other than leading/trailing markers)
    fn is_path_pattern(original: &str) -> bool {
        let trimmed = original.trim_start_matches('/').trim_end_matches('/');
        trimmed.contains('/')
    }
    /// Determine if a path should be included based on filter patterns
    ///
    /// # Precedence
    /// - If only excludes: include everything except matches
    /// - If only includes: include only matches (exclude everything else by default)
    /// - If both: excludes take priority (excludes checked first, then includes)
    ///
    /// # Directory handling
    /// Directories are traversed when include patterns exist if they could potentially contain
    /// matching files. For non-anchored patterns (like `*.txt`), all directories are traversed.
    /// For anchored patterns (like `/bar`), only directories matching the pattern prefix are traversed.
    pub fn should_include(&self, relative_path: &Path, is_dir: bool) -> FilterResult {
        // check excludes first - if matched, path is excluded
        for pattern in &self.excludes {
            if pattern.matches(relative_path, is_dir) {
                return FilterResult::ExcludedByPattern(pattern.original.clone());
            }
        }
        // if there are include patterns, at least one must match
        if !self.includes.is_empty() {
            // first check if this path matches any include pattern
            for pattern in &self.includes {
                if pattern.matches(relative_path, is_dir) {
                    return FilterResult::Included;
                }
            }
            // for directories that don't directly match, check if they could contain matches
            if is_dir {
                for pattern in &self.includes {
                    if self.could_contain_matches(relative_path, pattern) {
                        return FilterResult::Included;
                    }
                }
            }
            return FilterResult::ExcludedByDefault;
        }
        // no includes specified and not excluded = included
        FilterResult::Included
    }
    /// Check if a directory could potentially contain files matching the pattern
    pub fn could_contain_matches(&self, dir_path: &Path, pattern: &FilterPattern) -> bool {
        // non-anchored simple patterns (no path separators) can match anywhere
        if !pattern.anchored && !pattern.is_path_pattern() {
            return true;
        }
        // extract the non-wildcard prefix from the pattern
        // e.g., "/src/**" -> "src", "src/foo/**/*.rs" -> "src/foo", "**/*.rs" -> ""
        let pattern_path = pattern
            .original
            .trim_start_matches('/')
            .trim_end_matches('/');
        let prefix = Self::extract_literal_prefix(pattern_path);
        let dir_str = dir_path.to_string_lossy();
        // if no literal prefix (pattern starts with wildcard like "**/*.rs"),
        // it can match anywhere
        if prefix.is_empty() {
            return true;
        }
        // empty dir_path (root) is always an ancestor of any prefix
        if dir_str.is_empty() {
            return true;
        }
        // check if dir_path could lead to matches:
        // 1. dir_path is an ancestor of prefix (e.g., "src" for prefix "src/foo")
        // 2. dir_path equals the prefix
        // 3. dir_path is a descendant of prefix (e.g., "src/foo/bar" for prefix "src")
        // case 1 & 2: prefix starts with dir_path
        if prefix.starts_with(&*dir_str) {
            let after_dir = &prefix[dir_str.len()..];
            // dir_path is ancestor if followed by '/' or is exact match
            if after_dir.is_empty() || after_dir.starts_with('/') {
                return true;
            }
        }
        // case 3: dir_path is descendant of prefix
        if let Some(after_prefix) = dir_str.strip_prefix(prefix) {
            if after_prefix.is_empty() || after_prefix.starts_with('/') {
                return true;
            }
        }
        false
    }
    /// Extract the literal (non-wildcard) prefix from a pattern.
    /// Returns the portion before any wildcard characters (*, ?, [), trimmed to complete path components.
    /// Examples:
    /// - "src/**" -> "src"
    /// - "src/foo/**/*.rs" -> "src/foo"
    /// - "**/*.rs" -> ""
    /// - "bar" -> "bar" (no wildcards = entire pattern is literal)
    /// - "bar/*.txt" -> "bar"
    /// - "*.txt" -> ""
    fn extract_literal_prefix(pattern: &str) -> &str {
        // find first wildcard character
        let wildcard_pos = pattern.find(['*', '?', '[']).unwrap_or(pattern.len());
        // if no wildcards, entire pattern is literal
        if wildcard_pos == pattern.len() {
            return pattern;
        }
        // if wildcard at start, no prefix
        if wildcard_pos == 0 {
            return "";
        }
        // find the last '/' before the wildcard to get a complete path component
        let prefix = &pattern[..wildcard_pos];
        match prefix.rfind('/') {
            Some(pos) => &pattern[..pos],
            None => {
                // no '/' before wildcard - pattern like "src*.txt" has no usable prefix
                ""
            }
        }
    }
    /// Parse filter settings from a file
    ///
    /// # File Format
    /// ```text
    /// # comments supported
    /// --include *.rs
    /// --include Cargo.toml
    /// --exclude target/
    /// --exclude *.log
    /// ```
    pub fn from_file(path: &Path) -> Result<Self, anyhow::Error> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read filter file: {:?}", path))?;
        Self::parse_content(&content)
    }
    /// Parse filter settings from a string (filter file format)
    pub fn parse_content(content: &str) -> Result<Self, anyhow::Error> {
        let mut settings = Self::new();
        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();
            // skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let line_num = line_num + 1; // 1-based for error messages
            if let Some(pattern) = line.strip_prefix("--include ") {
                let pattern = pattern.trim();
                settings
                    .add_include(pattern)
                    .with_context(|| format!("line {}: invalid include pattern", line_num))?;
            } else if let Some(pattern) = line.strip_prefix("--exclude ") {
                let pattern = pattern.trim();
                settings
                    .add_exclude(pattern)
                    .with_context(|| format!("line {}: invalid exclude pattern", line_num))?;
            } else {
                return Err(anyhow!(
                    "line {}: invalid syntax '{}', expected '--include PATTERN' or '--exclude PATTERN'",
                    line_num, line
                ));
            }
        }
        Ok(settings)
    }
}

/// Data transfer object for FilterSettings serialization.
/// Used for passing filter settings across process boundaries (e.g., to rcpd).
#[derive(Serialize, Deserialize)]
struct FilterSettingsDto {
    includes: Vec<String>,
    excludes: Vec<String>,
}

impl Serialize for FilterSettings {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let dto = FilterSettingsDto {
            includes: self.includes.iter().map(|p| p.original.clone()).collect(),
            excludes: self.excludes.iter().map(|p| p.original.clone()).collect(),
        };
        dto.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FilterSettings {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let dto = FilterSettingsDto::deserialize(deserializer)?;
        let mut settings = FilterSettings::new();
        for pattern in dto.includes {
            settings
                .add_include(&pattern)
                .map_err(serde::de::Error::custom)?;
        }
        for pattern in dto.excludes {
            settings
                .add_exclude(&pattern)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_pattern_basic_glob() {
        let pattern = FilterPattern::parse("*.rs").unwrap();
        assert!(pattern.matches(Path::new("foo.rs"), false));
        assert!(pattern.matches(Path::new("main.rs"), false));
        assert!(!pattern.matches(Path::new("foo.txt"), false));
        // simple patterns match against filename, so src/foo.rs matches via its filename
        assert!(pattern.matches(Path::new("src/foo.rs"), false));
    }
    #[test]
    fn test_pattern_double_star() {
        let pattern = FilterPattern::parse("**/*.rs").unwrap();
        assert!(pattern.matches(Path::new("src/foo.rs"), false));
        assert!(pattern.matches(Path::new("a/b/c/d.rs"), false));
        // ** can match zero segments, so **/*.rs matches foo.rs
        assert!(pattern.matches(Path::new("foo.rs"), false));
    }
    #[test]
    fn test_pattern_question_mark() {
        let pattern = FilterPattern::parse("file?.txt").unwrap();
        assert!(pattern.matches(Path::new("file1.txt"), false));
        assert!(pattern.matches(Path::new("fileA.txt"), false));
        assert!(!pattern.matches(Path::new("file12.txt"), false));
        assert!(!pattern.matches(Path::new("file.txt"), false));
    }
    #[test]
    fn test_pattern_character_class() {
        let pattern = FilterPattern::parse("[abc].txt").unwrap();
        assert!(pattern.matches(Path::new("a.txt"), false));
        assert!(pattern.matches(Path::new("b.txt"), false));
        assert!(pattern.matches(Path::new("c.txt"), false));
        assert!(!pattern.matches(Path::new("d.txt"), false));
    }
    #[test]
    fn test_pattern_anchored() {
        let pattern = FilterPattern::parse("/src").unwrap();
        assert!(pattern.anchored);
        // matches only at root level
        assert!(pattern.matches(Path::new("src"), true));
        assert!(!pattern.matches(Path::new("foo/src"), true));
    }
    #[test]
    fn test_pattern_dir_only() {
        let pattern = FilterPattern::parse("build/").unwrap();
        assert!(pattern.dir_only);
        // only matches directories
        assert!(pattern.matches(Path::new("build"), true));
        assert!(!pattern.matches(Path::new("build"), false)); // file named build
    }
    #[test]
    fn test_include_only_mode() {
        let mut settings = FilterSettings::new();
        settings.add_include("*.rs").unwrap();
        settings.add_include("Cargo.toml").unwrap();
        assert!(matches!(
            settings.should_include(Path::new("main.rs"), false),
            FilterResult::Included
        ));
        assert!(matches!(
            settings.should_include(Path::new("Cargo.toml"), false),
            FilterResult::Included
        ));
        assert!(matches!(
            settings.should_include(Path::new("README.md"), false),
            FilterResult::ExcludedByDefault
        ));
    }
    #[test]
    fn test_exclude_only_mode() {
        let mut settings = FilterSettings::new();
        settings.add_exclude("*.log").unwrap();
        settings.add_exclude("target/").unwrap();
        assert!(matches!(
            settings.should_include(Path::new("main.rs"), false),
            FilterResult::Included
        ));
        match settings.should_include(Path::new("debug.log"), false) {
            FilterResult::ExcludedByPattern(p) => assert_eq!(p, "*.log"),
            other => panic!("expected ExcludedByPattern, got {:?}", other),
        }
        match settings.should_include(Path::new("target"), true) {
            FilterResult::ExcludedByPattern(p) => assert_eq!(p, "target/"),
            other => panic!("expected ExcludedByPattern, got {:?}", other),
        }
    }
    #[test]
    fn test_include_then_exclude() {
        let mut settings = FilterSettings::new();
        settings.add_include("*.rs").unwrap();
        settings.add_exclude("test_*.rs").unwrap();
        // regular .rs files are included
        assert!(matches!(
            settings.should_include(Path::new("main.rs"), false),
            FilterResult::Included
        ));
        // test_*.rs files are excluded even though they match *.rs
        match settings.should_include(Path::new("test_foo.rs"), false) {
            FilterResult::ExcludedByPattern(p) => assert_eq!(p, "test_*.rs"),
            other => panic!("expected ExcludedByPattern, got {:?}", other),
        }
        // non-.rs files are excluded by default
        assert!(matches!(
            settings.should_include(Path::new("README.md"), false),
            FilterResult::ExcludedByDefault
        ));
    }
    #[test]
    fn test_filter_file_basic() {
        let content = r#"
# this is a comment
--include *.rs
--include Cargo.toml

--exclude target/
--exclude *.log
"#;
        let settings = FilterSettings::parse_content(content).unwrap();
        assert_eq!(settings.includes.len(), 2);
        assert_eq!(settings.excludes.len(), 2);
    }
    #[test]
    fn test_filter_file_comments() {
        let content = "# only comments\n# and empty lines\n\n";
        let settings = FilterSettings::parse_content(content).unwrap();
        assert!(settings.is_empty());
    }
    #[test]
    fn test_filter_file_syntax_error() {
        let content = "invalid line without prefix";
        let result = FilterSettings::parse_content(content);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("line 1"));
        assert!(err.contains("invalid syntax"));
    }
    #[test]
    fn test_empty_pattern_error() {
        let result = FilterPattern::parse("");
        assert!(result.is_err());
    }
    #[test]
    fn test_is_empty() {
        let empty = FilterSettings::new();
        assert!(empty.is_empty());
        let mut with_include = FilterSettings::new();
        with_include.add_include("*.rs").unwrap();
        assert!(!with_include.is_empty());
        let mut with_exclude = FilterSettings::new();
        with_exclude.add_exclude("*.log").unwrap();
        assert!(!with_exclude.is_empty());
    }
    #[test]
    fn test_filename_match_for_simple_patterns() {
        // simple patterns (no /) should match the filename anywhere in the path
        let pattern = FilterPattern::parse("*.rs").unwrap();
        assert!(pattern.matches(Path::new("foo.rs"), false));
        assert!(pattern.matches(Path::new("src/foo.rs"), false)); // matches filename
                                                                  // nested paths also match via filename
        assert!(pattern.matches(Path::new("a/b/c/foo.rs"), false));
    }
    #[test]
    fn test_path_pattern_requires_full_match() {
        // patterns with / require the full path to match
        let pattern = FilterPattern::parse("src/*.rs").unwrap();
        assert!(pattern.matches(Path::new("src/foo.rs"), false));
        assert!(!pattern.matches(Path::new("foo.rs"), false));
        assert!(!pattern.matches(Path::new("other/src/foo.rs"), false));
    }
    #[test]
    fn test_double_star_matches_nested_paths() {
        // **/*.rs should match files at any depth
        let pattern = FilterPattern::parse("**/*.rs").unwrap();
        assert!(pattern.matches(Path::new("foo.rs"), false));
        assert!(pattern.matches(Path::new("src/foo.rs"), false));
        assert!(pattern.matches(Path::new("src/lib/foo.rs"), false));
        assert!(pattern.matches(Path::new("a/b/c/d/e.rs"), false));
    }
    #[test]
    fn test_anchored_pattern_matches_only_at_root() {
        // /src should match only at root, not nested
        let pattern = FilterPattern::parse("/src").unwrap();
        assert!(pattern.matches(Path::new("src"), true));
        assert!(!pattern.matches(Path::new("foo/src"), true));
        assert!(!pattern.matches(Path::new("a/b/src"), true));
    }
    #[test]
    fn test_nested_directory_pattern() {
        // src/lib/ should match only that specific nested path
        let pattern = FilterPattern::parse("src/lib/").unwrap();
        assert!(pattern.matches(Path::new("src/lib"), true));
        assert!(!pattern.matches(Path::new("lib"), true));
        assert!(!pattern.matches(Path::new("other/src/lib"), true));
    }
    #[test]
    fn test_dir_only_simple_pattern_matches_at_any_level() {
        // target/ (dir-only) should match "target" at any level, like simple patterns
        // the trailing / is just a dir-only marker, not a path separator
        let pattern = FilterPattern::parse("target/").unwrap();
        assert!(pattern.dir_only);
        assert!(!pattern.anchored);
        // should match at root
        assert!(pattern.matches(Path::new("target"), true));
        // should match nested (filename matching)
        assert!(pattern.matches(Path::new("foo/target"), true));
        assert!(pattern.matches(Path::new("a/b/target"), true));
        // should NOT match files (dir-only)
        assert!(!pattern.matches(Path::new("target"), false));
        assert!(!pattern.matches(Path::new("foo/target"), false));
    }
    #[test]
    fn test_dir_only_pattern_could_contain_matches() {
        // target/ should allow traversal into any directory since it can match anywhere
        let mut settings = FilterSettings::new();
        settings.add_include("target/").unwrap();
        let pattern = &settings.includes[0];
        // should return true for any directory since target/ can match at any level
        assert!(settings.could_contain_matches(Path::new("foo"), pattern));
        assert!(settings.could_contain_matches(Path::new("a/b"), pattern));
        assert!(settings.could_contain_matches(Path::new("src"), pattern));
    }
    #[test]
    fn test_precedence_exclude_overrides_include() {
        // when both include and exclude match, exclude wins (excludes checked first)
        let mut settings = FilterSettings::new();
        settings.add_include("*.rs").unwrap();
        settings.add_exclude("test_*.rs").unwrap();
        // file matching both patterns should be excluded
        match settings.should_include(Path::new("test_main.rs"), false) {
            FilterResult::ExcludedByPattern(p) => assert_eq!(p, "test_*.rs"),
            other => panic!("expected ExcludedByPattern, got {:?}", other),
        }
        // file matching only include should be included
        assert!(matches!(
            settings.should_include(Path::new("main.rs"), false),
            FilterResult::Included
        ));
    }
    #[test]
    fn test_should_include_root_item_non_anchored_exclude() {
        // non-anchored exclude patterns should apply to root items
        let mut settings = FilterSettings::new();
        settings.add_exclude("*.log").unwrap();
        // root file matching exclude pattern is excluded
        match settings.should_include_root_item(Path::new("debug.log"), false) {
            FilterResult::ExcludedByPattern(p) => assert_eq!(p, "*.log"),
            other => panic!("expected ExcludedByPattern, got {:?}", other),
        }
        // root file not matching exclude pattern is included
        assert!(matches!(
            settings.should_include_root_item(Path::new("main.rs"), false),
            FilterResult::Included
        ));
    }
    #[test]
    fn test_should_include_root_item_anchored_exclude_skipped() {
        // anchored exclude patterns should NOT apply to root items
        let mut settings = FilterSettings::new();
        settings.add_exclude("/target/").unwrap();
        // root directory "target" should NOT be excluded (pattern is anchored)
        assert!(matches!(
            settings.should_include_root_item(Path::new("target"), true),
            FilterResult::Included
        ));
    }
    #[test]
    fn test_should_include_root_item_non_anchored_include() {
        // non-anchored include patterns should apply to root items
        let mut settings = FilterSettings::new();
        settings.add_include("*.rs").unwrap();
        // root file matching include pattern is included
        assert!(matches!(
            settings.should_include_root_item(Path::new("main.rs"), false),
            FilterResult::Included
        ));
        // root file not matching include pattern is excluded by default
        assert!(matches!(
            settings.should_include_root_item(Path::new("readme.md"), false),
            FilterResult::ExcludedByDefault
        ));
    }
    #[test]
    fn test_should_include_root_item_anchored_include_skipped() {
        // anchored include patterns should NOT apply to root items directly
        // but root directories should still be traversed if anchored patterns exist
        let mut settings = FilterSettings::new();
        settings.add_include("/bar").unwrap();
        // root directory "foo" should be included (so we can traverse to find /bar inside)
        assert!(matches!(
            settings.should_include_root_item(Path::new("foo"), true),
            FilterResult::Included
        ));
        // root file "baz" should be excluded by default (only anchored includes, none match)
        assert!(matches!(
            settings.should_include_root_item(Path::new("baz"), false),
            FilterResult::ExcludedByDefault
        ));
    }
    #[test]
    fn test_should_include_root_item_mixed_patterns() {
        // mix of anchored and non-anchored patterns
        let mut settings = FilterSettings::new();
        settings.add_include("*.rs").unwrap();
        settings.add_include("/bar").unwrap();
        settings.add_exclude("test_*.rs").unwrap();
        // root .rs file is included (non-anchored include)
        assert!(matches!(
            settings.should_include_root_item(Path::new("main.rs"), false),
            FilterResult::Included
        ));
        // root test_*.rs file is excluded (non-anchored exclude)
        match settings.should_include_root_item(Path::new("test_foo.rs"), false) {
            FilterResult::ExcludedByPattern(p) => assert_eq!(p, "test_*.rs"),
            other => panic!("expected ExcludedByPattern, got {:?}", other),
        }
        // root directory "foo" is included (to traverse for /bar)
        assert!(matches!(
            settings.should_include_root_item(Path::new("foo"), true),
            FilterResult::Included
        ));
    }
    #[test]
    fn test_could_contain_matches_anchored_double_star() {
        // /src/** should only match directories under src, not unrelated directories
        let mut settings = FilterSettings::new();
        settings.add_include("/src/**").unwrap();
        let pattern = &settings.includes[0];
        // should return true for ancestors of "src" (we need to traverse to get there)
        assert!(settings.could_contain_matches(Path::new(""), pattern));
        // should return true for "src" itself (it's the prefix)
        assert!(settings.could_contain_matches(Path::new("src"), pattern));
        // should return true for descendants of "src" (matches can be inside)
        assert!(settings.could_contain_matches(Path::new("src/foo"), pattern));
        assert!(settings.could_contain_matches(Path::new("src/foo/bar"), pattern));
        // should return FALSE for unrelated directories
        assert!(!settings.could_contain_matches(Path::new("build"), pattern));
        assert!(!settings.could_contain_matches(Path::new("target"), pattern));
        assert!(!settings.could_contain_matches(Path::new("build/src"), pattern));
    }
    #[test]
    fn test_could_contain_matches_non_anchored_double_star() {
        // **/*.rs should match anywhere (no prefix)
        let mut settings = FilterSettings::new();
        settings.add_include("**/*.rs").unwrap();
        let pattern = &settings.includes[0];
        // should return true for any directory since ** can match anywhere
        assert!(settings.could_contain_matches(Path::new("src"), pattern));
        assert!(settings.could_contain_matches(Path::new("build"), pattern));
        assert!(settings.could_contain_matches(Path::new("any/path"), pattern));
    }
    #[test]
    fn test_could_contain_matches_nested_prefix() {
        // /src/foo/** should have prefix "src/foo"
        let mut settings = FilterSettings::new();
        settings.add_include("/src/foo/**").unwrap();
        let pattern = &settings.includes[0];
        // ancestors of prefix
        assert!(settings.could_contain_matches(Path::new(""), pattern));
        assert!(settings.could_contain_matches(Path::new("src"), pattern));
        // the prefix itself
        assert!(settings.could_contain_matches(Path::new("src/foo"), pattern));
        // descendants of prefix
        assert!(settings.could_contain_matches(Path::new("src/foo/bar"), pattern));
        // unrelated directories
        assert!(!settings.could_contain_matches(Path::new("build"), pattern));
        assert!(!settings.could_contain_matches(Path::new("src/bar"), pattern));
    }
    #[test]
    fn test_extract_literal_prefix() {
        // test the helper function
        assert_eq!(FilterSettings::extract_literal_prefix("src/**"), "src");
        assert_eq!(
            FilterSettings::extract_literal_prefix("src/foo/**"),
            "src/foo"
        );
        assert_eq!(FilterSettings::extract_literal_prefix("**/*.rs"), "");
        assert_eq!(FilterSettings::extract_literal_prefix("*.rs"), "");
        assert_eq!(FilterSettings::extract_literal_prefix("src/*.rs"), "src");
        // no wildcards = entire pattern is literal
        assert_eq!(
            FilterSettings::extract_literal_prefix("src/foo/bar"),
            "src/foo/bar"
        );
        assert_eq!(FilterSettings::extract_literal_prefix("bar"), "bar");
        assert_eq!(FilterSettings::extract_literal_prefix("src[0-9]/*.rs"), "");
    }
}
