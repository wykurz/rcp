// version information for protocol compatibility checking

use serde::{Deserialize, Serialize};

/// Protocol version information
///
/// Contains version information for compatibility checking between rcp and rcpd.
/// The semantic version is used for compatibility checks, while git information
/// provides additional debugging context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProtocolVersion {
    /// Semantic version from Cargo.toml (e.g., "0.22.0")
    ///
    /// This is the primary version used for compatibility checking.
    pub semantic: String,

    /// Git describe output (e.g., "v0.21.1-7-g644da27")
    ///
    /// Optional. Provides detailed version information including:
    /// - Most recent tag
    /// - Number of commits since tag
    /// - Short commit hash
    /// - "-dirty" suffix if working tree has uncommitted changes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_describe: Option<String>,

    /// Full git commit hash
    ///
    /// Optional. Useful for exact build identification and debugging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_hash: Option<String>,
}

impl ProtocolVersion {
    /// Get the current protocol version
    ///
    /// Reads version information from environment variables set at compile time
    /// by build.rs. The semantic version is always available, while git information
    /// may be absent if the build was done without git available.
    pub fn current() -> Self {
        Self {
            semantic: env!("CARGO_PKG_VERSION").to_string(),
            git_describe: option_env!("RCP_GIT_DESCRIBE").map(String::from),
            git_hash: option_env!("RCP_GIT_HASH").map(String::from),
        }
    }

    /// Check if this version is compatible with another version
    ///
    /// Currently implements exact version matching: versions are compatible
    /// only if their semantic versions match exactly.
    ///
    /// # Examples
    ///
    /// ```
    /// use common::version::ProtocolVersion;
    ///
    /// let v1 = ProtocolVersion {
    ///     semantic: "0.22.0".to_string(),
    ///     git_describe: None,
    ///     git_hash: None,
    /// };
    ///
    /// let v2 = ProtocolVersion {
    ///     semantic: "0.22.0".to_string(),
    ///     git_describe: Some("v0.21.1-7-g644da27".to_string()),
    ///     git_hash: None,
    /// };
    ///
    /// assert!(v1.is_compatible_with(&v2));
    /// ```
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        // exact version match for now
        // in the future, we might allow minor version skew (e.g., 0.22.x compatible with 0.22.y)
        self.semantic == other.semantic
    }

    /// Get a human-readable version string
    ///
    /// Returns the semantic version, optionally including git describe information
    /// if available.
    ///
    /// # Examples
    ///
    /// ```
    /// use common::version::ProtocolVersion;
    ///
    /// let v = ProtocolVersion {
    ///     semantic: "0.22.0".to_string(),
    ///     git_describe: Some("v0.21.1-7-g644da27".to_string()),
    ///     git_hash: None,
    /// };
    ///
    /// assert_eq!(v.display(), "0.22.0 (v0.21.1-7-g644da27)");
    /// ```
    pub fn display(&self) -> String {
        if let Some(ref git_describe) = self.git_describe {
            format!("{} ({})", self.semantic, git_describe)
        } else {
            self.semantic.clone()
        }
    }

    /// Serialize to JSON string
    ///
    /// # Errors
    ///
    /// Returns an error if JSON serialization fails.
    pub fn to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(self)
            .map_err(|e| anyhow::anyhow!("failed to serialize version: {:#}", e))
    }

    /// Deserialize from JSON string
    ///
    /// # Errors
    ///
    /// Returns an error if JSON deserialization fails or the format is invalid.
    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| anyhow::anyhow!("failed to parse version JSON: {:#}", e))
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_version() {
        let version = ProtocolVersion::current();
        // semantic version should always be available
        assert!(!version.semantic.is_empty());
        // git info should be available when building from git repository
        // this test will catch if build.rs is not activated in Cargo.toml
        assert!(
            version.git_describe.is_some(),
            "git_describe should be populated by build.rs (check that build.rs is activated in Cargo.toml)"
        );
        assert!(
            version.git_hash.is_some(),
            "git_hash should be populated by build.rs (check that build.rs is activated in Cargo.toml)"
        );
    }

    #[test]
    fn test_exact_version_compatibility() {
        let v1 = ProtocolVersion {
            semantic: "0.22.0".to_string(),
            git_describe: None,
            git_hash: None,
        };

        let v2 = ProtocolVersion {
            semantic: "0.22.0".to_string(),
            git_describe: Some("v0.21.1-7-g644da27".to_string()),
            git_hash: Some("644da27".to_string()),
        };

        let v3 = ProtocolVersion {
            semantic: "0.21.0".to_string(),
            git_describe: None,
            git_hash: None,
        };

        // same semantic version should be compatible
        assert!(v1.is_compatible_with(&v2));
        assert!(v2.is_compatible_with(&v1));

        // different semantic versions should not be compatible
        assert!(!v1.is_compatible_with(&v3));
        assert!(!v3.is_compatible_with(&v1));
    }

    #[test]
    fn test_display() {
        let v1 = ProtocolVersion {
            semantic: "0.22.0".to_string(),
            git_describe: None,
            git_hash: None,
        };
        assert_eq!(v1.display(), "0.22.0");

        let v2 = ProtocolVersion {
            semantic: "0.22.0".to_string(),
            git_describe: Some("v0.21.1-7-g644da27".to_string()),
            git_hash: None,
        };
        assert_eq!(v2.display(), "0.22.0 (v0.21.1-7-g644da27)");
    }

    #[test]
    fn test_json_serialization() {
        let v = ProtocolVersion {
            semantic: "0.22.0".to_string(),
            git_describe: Some("v0.21.1-7-g644da27".to_string()),
            git_hash: Some("644da27abc".to_string()),
        };

        let json = v.to_json().unwrap();
        let parsed = ProtocolVersion::from_json(&json).unwrap();

        assert_eq!(v, parsed);
    }

    #[test]
    fn test_json_deserialization_without_git() {
        let json = r#"{"semantic":"0.22.0"}"#;
        let v = ProtocolVersion::from_json(json).unwrap();

        assert_eq!(v.semantic, "0.22.0");
        assert!(v.git_describe.is_none());
        assert!(v.git_hash.is_none());
    }
}
