use anyhow::Context;

#[derive(Debug, Clone)]
pub struct RemotePath {
    session: remote::SshSession,
    path: std::path::PathBuf,
    needs_remote_home: bool,
}

impl RemotePath {
    pub fn new(
        session: remote::SshSession,
        path: std::path::PathBuf,
        needs_remote_home: bool,
    ) -> anyhow::Result<Self> {
        if !needs_remote_home && !path.is_absolute() {
            return Err(anyhow::anyhow!("Path must be absolute: {}", path.display()));
        }
        Ok(Self {
            session,
            path,
            needs_remote_home,
        })
    }

    #[must_use]
    pub fn from_local(path: &std::path::Path) -> Self {
        Self {
            session: remote::SshSession::local(),
            path: path.to_path_buf(),
            needs_remote_home: false,
        }
    }

    #[must_use]
    pub fn session(&self) -> &remote::SshSession {
        &self.session
    }

    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    #[must_use]
    pub fn needs_remote_home(&self) -> bool {
        self.needs_remote_home
    }

    pub fn apply_remote_home(&mut self, home: &std::path::Path) {
        if self.needs_remote_home {
            let suffix = &self.path;
            self.path = home.join(suffix);
            self.needs_remote_home = false;
        }
    }
}

#[derive(Debug)]
pub enum PathType {
    Local(std::path::PathBuf),
    Remote(RemotePath),
}

impl PartialEq for PathType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PathType::Local(_), PathType::Local(_)) => true, // Local paths are always equal
            (PathType::Local(_), PathType::Remote(_)) => false,
            (PathType::Remote(_), PathType::Local(_)) => false,
            (PathType::Remote(remote1), PathType::Remote(remote2)) => {
                remote1.session() == remote2.session()
            }
        }
    }
}

impl Clone for PathType {
    fn clone(&self) -> Self {
        match self {
            PathType::Local(p) => PathType::Local(p.clone()),
            PathType::Remote(r) => PathType::Remote(r.clone()),
        }
    }
}

/// Gets the compiled regex for parsing remote paths (shared with `parse_path`)
fn get_remote_path_regex() -> &'static regex::Regex {
    use std::sync::OnceLock;
    static REGEX: OnceLock<regex::Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        // The regex matches: [user@]host[:port]:path
        // - user: optional, no @ allowed
        // - host: either [IPv6] or hostname (no colons, no brackets, no slashes)
        // - port: optional, digits only
        // - path: everything after the final colon
        // Note: we explicitly exclude '/' from hostname to prevent matching paths like
        // /tmp/file:with:colons as remote paths
        regex::Regex::new(
            r"^(?:(?P<user>[^@]+)@)?(?P<host>(?:\[[^\]]+\]|[^:/\[\]]+))(?::(?P<port>\d+))?:(?P<path>.+)$"
        ).unwrap()
    })
}

/// Checks if a hostname represents the local machine
fn is_localhost(host: &str) -> bool {
    let host_lower = host.to_lowercase();
    host_lower == "localhost" || host_lower == "127.0.0.1" || host_lower == "[::1]"
}

/// Checks if a path string has a localhost-like prefix (e.g., "localhost:/path").
/// This is used to detect when the user explicitly used localhost syntax but the
/// path was parsed as local due to default behavior.
#[must_use]
pub fn has_localhost_prefix(path: &str) -> bool {
    let re = get_remote_path_regex();
    if let Some(captures) = re.captures(path) {
        let host = captures.name("host").unwrap().as_str();
        let user = captures.name("user");
        let port = captures.name("port");
        // only return true if it's a bare localhost (no user/port)
        // because user@localhost or localhost:22 would be treated as remote anyway
        is_localhost(host) && user.is_none() && port.is_none()
    } else {
        false
    }
}

/// Splits a remote path string into (`host_prefix`, `path_part`) using the same logic as `parse_path`
/// For example: "user@host:22:/path/to/file" -> ("user@host:22:", "/path/to/file")
/// For local paths, returns (None, `original_path`)
fn split_remote_path(path_str: &str) -> (Option<String>, &str) {
    let re = get_remote_path_regex();
    if let Some(captures) = re.captures(path_str) {
        let path_part = captures.name("path").unwrap().as_str();
        // Reconstruct the host prefix part by finding where the path starts
        let path_start = path_str.len() - path_part.len();
        let host_prefix = &path_str[..path_start];
        (Some(host_prefix.to_string()), path_part)
    } else {
        (None, path_str)
    }
}

/// Extracts just the filesystem path part from a remote or local path string
/// For example: "user@host:22:/path/to/file" -> "/path/to/file"
/// For local paths, returns the original path
fn extract_filesystem_path(path_str: &str) -> &str {
    split_remote_path(path_str).1
}

/// Joins a filesystem path with a filename, handling both remote and local cases
/// For example: ("user@host:22:", "/path/", "file.txt") -> "user@host:22:/path/file.txt"
/// For local: (None, "/path/", "file.txt") -> "/path/file.txt"
fn join_path_with_filename(host_prefix: Option<String>, dir_path: &str, filename: &str) -> String {
    let fs_path = std::path::Path::new(dir_path);
    let joined = fs_path.join(filename);
    let joined_str = joined.to_string_lossy();
    if let Some(prefix) = host_prefix {
        format!("{prefix}{joined_str}")
    } else {
        joined_str.to_string()
    }
}

pub fn expand_local_home(path: &str) -> anyhow::Result<std::path::PathBuf> {
    if let Some(rest) = path.strip_prefix("~/") {
        let home = std::env::var("HOME")
            .map(std::path::PathBuf::from)
            .context("HOME environment variable is not set; required for '~/' expansion")?;
        return Ok(home.join(rest));
    } else if path == "~" {
        let home = std::env::var("HOME")
            .map(std::path::PathBuf::from)
            .context("HOME environment variable is not set; required for '~' expansion")?;
        return Ok(home);
    }
    Ok(std::path::PathBuf::from(path))
}

/// Internal path parsing with configurable localhost handling
fn parse_path_internal(path: &str, treat_localhost_as_local: bool) -> anyhow::Result<PathType> {
    let re = get_remote_path_regex();
    if let Some(captures) = re.captures(path) {
        // It matched the remote path pattern
        let user = captures.name("user").map(|m| m.as_str().to_string());
        let host = captures.name("host").unwrap().as_str().to_string();
        let port = captures
            .name("port")
            .and_then(|m| m.as_str().parse::<u16>().ok());
        let path_part = captures
            .name("path")
            .expect("Unable to extract file system path from provided remote path")
            .as_str();
        // if host is localhost (and no user/port), optionally treat as local path
        // this provides an escape hatch for paths with colons: localhost:/tmp/file:with:colons
        if treat_localhost_as_local && is_localhost(&host) && user.is_none() && port.is_none() {
            // expand tilde for local paths
            let local_path = if path_part == "~" || path_part == "~/" {
                expand_local_home("~")?
            } else if path_part.starts_with("~/") {
                expand_local_home(path_part)?
            } else {
                std::path::PathBuf::from(path_part)
            };
            return Ok(PathType::Local(local_path));
        }
        let (remote_path, needs_remote_home) = if path_part == "~" || path_part == "~/" {
            (std::path::PathBuf::new(), true)
        } else if path_part.starts_with("~/") {
            let suffix = path_part.trim_start_matches("~/");
            (std::path::PathBuf::from(suffix), true)
        } else {
            let remote_path = std::path::PathBuf::from(path_part);
            if remote_path.is_absolute() {
                (remote_path, false)
            } else {
                let cwd = std::env::current_dir().context("failed to read current directory")?;
                (cwd.join(remote_path), false)
            }
        };
        Ok(PathType::Remote(RemotePath::new(
            remote::SshSession { user, host, port },
            remote_path,
            needs_remote_home,
        )?))
    } else {
        // It's a local path
        Ok(PathType::Local(expand_local_home(path)?))
    }
}

/// Parses a path string into either a local or remote path type.
///
/// Remote path syntax: `[user@]host[:port]:path`
///
/// Special handling:
/// - Paths starting with `/` are always local (absolute paths)
/// - `localhost:/path` is treated as local path `/path` (escape hatch for paths with colons)
/// - `127.0.0.1:/path` and `[::1]:/path` are also treated as local
pub fn parse_path(path: &str) -> anyhow::Result<PathType> {
    parse_path_internal(path, true)
}

/// Parses a path string, treating localhost as a remote host.
///
/// This is used with `--force-remote` flag to force remote copy mode
/// even when both paths use localhost.
///
/// Remote path syntax: `[user@]host[:port]:path`
pub fn parse_path_force_remote(path: &str) -> anyhow::Result<PathType> {
    parse_path_internal(path, false)
}

/// Validates that destination path doesn't end with problematic patterns like . or ..
///
/// # Arguments
/// * `dst_path_str` - Destination path string to validate
///
/// # Returns
/// * `Ok(())` - If path is valid
/// * `Err(...)` - If path ends with . or .. with clear error message
pub fn validate_destination_path(dst_path_str: &str) -> anyhow::Result<()> {
    // Extract the filesystem path part for validation
    let path_part = extract_filesystem_path(dst_path_str);
    // Check the raw string for problematic endings, since Path::file_name() normalizes
    if path_part.ends_with("/.") {
        return Err(anyhow::anyhow!(
            "Destination path cannot end with '/.' (current directory).\n\
            If you want to copy into the current directory, use './' instead.\n\
            Example: 'rcp source.txt ./' copies source.txt into current directory as source.txt"
        ));
    } else if path_part.ends_with("/..") {
        return Err(anyhow::anyhow!(
            "Destination path cannot end with '/..' (parent directory).\n\
            If you want to copy into the parent directory, use '../' instead.\n\
            Example: 'rcp source.txt ../' copies source.txt into parent directory as source.txt"
        ));
    } else if path_part == "." {
        return Err(anyhow::anyhow!(
            "Destination path cannot be '.' (current directory).\n\
            If you want to copy into the current directory, use './' instead.\n\
            Example: 'rcp source.txt ./' copies source.txt into current directory as source.txt"
        ));
    } else if path_part == ".." {
        return Err(anyhow::anyhow!(
            "Destination path cannot be '..' (parent directory).\n\
            If you want to copy into the parent directory, use '../' instead.\n\
            Example: 'rcp source.txt ../' copies source.txt into parent directory as source.txt"
        ));
    }
    Ok(())
}

/// Resolves destination path handling trailing slash semantics for both local and remote paths.
///
/// This function implements the logic: "foo/bar -> baz/" becomes "foo/bar -> baz/bar"
/// i.e., when destination ends with '/', copy source INTO the directory.
///
/// # Arguments
/// * `src_path_str` - Source path as string (before parsing)
/// * `dst_path_str` - Destination path as string (before parsing)
///
/// # Returns
/// * `Ok(resolved_dst_path)` - Destination path with trailing slash logic applied
/// * `Err(...)` - If path resolution fails or invalid combination detected
pub fn resolve_destination_path(src_path_str: &str, dst_path_str: &str) -> anyhow::Result<String> {
    // validate destination path doesn't end with problematic patterns
    validate_destination_path(dst_path_str)?;
    if dst_path_str.ends_with('/') {
        // extract source file name to append to destination directory
        let actual_src_path = std::path::Path::new(extract_filesystem_path(src_path_str));
        let src_file_name = actual_src_path.file_name().ok_or_else(|| {
            anyhow::anyhow!("Source path {:?} does not have a basename", actual_src_path)
        })?;
        // construct destination: "baz/" + "bar" -> "baz/bar"
        let (host_prefix, dir_path) = split_remote_path(dst_path_str);
        let filename = src_file_name.to_string_lossy();
        Ok(join_path_with_filename(host_prefix, dir_path, &filename))
    } else {
        // no trailing slash - use destination as-is
        Ok(dst_path_str.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path_local() {
        match parse_path("/path/to/file").unwrap() {
            PathType::Local(path) => assert_eq!(path.to_str().unwrap(), "/path/to/file"),
            _ => panic!("Expected local path"),
        }
    }

    #[test]
    fn test_parse_path_remote_basic() {
        match parse_path("host:/path/to/file").unwrap() {
            PathType::Remote(remote_path) => {
                assert_eq!(remote_path.session().user, None);
                assert_eq!(remote_path.session().host, "host");
                assert_eq!(remote_path.session().port, None);
                assert_eq!(remote_path.path().to_str().unwrap(), "/path/to/file");
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_parse_path_remote_full() {
        match parse_path("user@host:22:/path/to/file").unwrap() {
            PathType::Remote(remote_path) => {
                assert_eq!(remote_path.session().user, Some("user".to_string()));
                assert_eq!(remote_path.session().host, "host");
                assert_eq!(remote_path.session().port, Some(22));
                assert_eq!(remote_path.path().to_str().unwrap(), "/path/to/file");
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_parse_path_ipv6() {
        match parse_path("[2001:db8::1]:/path/to/file").unwrap() {
            PathType::Remote(remote_path) => {
                assert_eq!(remote_path.session().user, None);
                assert_eq!(remote_path.session().host, "[2001:db8::1]");
                assert_eq!(remote_path.session().port, None);
                assert_eq!(remote_path.path().to_str().unwrap(), "/path/to/file");
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_parse_path_local_tilde_expands() {
        let tmp_home = tempfile::tempdir().unwrap();
        let original_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", tmp_home.path());
        match parse_path("~/docs/file.txt").unwrap() {
            PathType::Local(path) => {
                assert_eq!(path, tmp_home.path().join("docs/file.txt"));
            }
            _ => panic!("Expected local path"),
        }
        if let Some(prev) = original_home {
            std::env::set_var("HOME", prev);
        } else {
            std::env::remove_var("HOME");
        }
    }

    #[test]
    fn test_parse_path_remote_tilde_requires_resolution() {
        match parse_path("host:~/file.txt").unwrap() {
            PathType::Remote(remote_path) => {
                assert!(remote_path.needs_remote_home());
                assert_eq!(remote_path.path(), std::path::Path::new("file.txt"));
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_parse_path_remote_bare_tilde() {
        match parse_path("host:~").unwrap() {
            PathType::Remote(remote_path) => {
                assert!(remote_path.needs_remote_home());
                assert_eq!(remote_path.path(), std::path::Path::new(""));
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_parse_path_remote_tilde_dir() {
        match parse_path("host:~/").unwrap() {
            PathType::Remote(remote_path) => {
                assert!(remote_path.needs_remote_home());
                assert_eq!(remote_path.path(), std::path::Path::new(""));
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_parse_path_remote_relative_resolved_to_cwd() {
        let cwd = std::env::current_dir().unwrap();
        match parse_path("host:relative/path").unwrap() {
            PathType::Remote(remote_path) => {
                assert_eq!(remote_path.path(), &cwd.join("relative/path"));
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_remote_path_apply_home() {
        let session = remote::SshSession::local();
        let mut remote_path =
            RemotePath::new(session, std::path::PathBuf::from("file.txt"), true).unwrap();
        let home = std::path::Path::new("/home/tester");
        remote_path.apply_remote_home(home);
        assert_eq!(remote_path.path(), &home.join("file.txt"));
        assert!(!remote_path.needs_remote_home());
    }

    #[test]
    fn test_resolve_destination_path_local_with_trailing_slash() {
        let result = resolve_destination_path("/path/to/file.txt", "/dest/").unwrap();
        assert_eq!(result, "/dest/file.txt");
    }

    #[test]
    fn test_resolve_destination_path_local_without_trailing_slash() {
        let result = resolve_destination_path("/path/to/file.txt", "/dest/newname.txt").unwrap();
        assert_eq!(result, "/dest/newname.txt");
    }

    #[test]
    fn test_resolve_destination_path_remote_with_trailing_slash() {
        let result = resolve_destination_path("host:/path/to/file.txt", "/dest/").unwrap();
        assert_eq!(result, "/dest/file.txt");
    }

    #[test]
    fn test_resolve_destination_path_remote_without_trailing_slash() {
        let result =
            resolve_destination_path("host:/path/to/file.txt", "/dest/newname.txt").unwrap();
        assert_eq!(result, "/dest/newname.txt");
    }

    #[test]
    fn test_resolve_destination_path_remote_complex() {
        let result =
            resolve_destination_path("user@host:22:/home/user/docs/report.pdf", "host2:/backup/")
                .unwrap();
        assert_eq!(result, "host2:/backup/report.pdf");
    }

    #[test]
    fn test_validate_destination_path_dot_local() {
        let result = resolve_destination_path("/path/to/file.txt", "/dest/.");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot end with '/.'"));
        assert!(error.to_string().contains("use './' instead"));
    }

    #[test]
    fn test_validate_destination_path_double_dot_local() {
        let result = resolve_destination_path("/path/to/file.txt", "/dest/..");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot end with '/..'"));
        assert!(error.to_string().contains("use '../' instead"));
    }

    #[test]
    fn test_validate_destination_path_dot_remote() {
        let result = resolve_destination_path("host:/path/to/file.txt", "host2:/dest/.");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot end with '/.'"));
    }

    #[test]
    fn test_validate_destination_path_double_dot_remote() {
        let result = resolve_destination_path("host:/path/to/file.txt", "host2:/dest/..");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot end with '/..'"));
    }

    #[test]
    fn test_validate_destination_path_bare_dot() {
        let result = resolve_destination_path("/path/to/file.txt", ".");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be '.'"));
    }

    #[test]
    fn test_validate_destination_path_bare_double_dot() {
        let result = resolve_destination_path("/path/to/file.txt", "..");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be '..'"));
    }

    #[test]
    fn test_validate_destination_path_remote_bare_dot() {
        let result = resolve_destination_path("host:/path/to/file.txt", "host2:.");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be '.'"));
    }

    #[test]
    fn test_validate_destination_path_remote_bare_double_dot() {
        let result = resolve_destination_path("host:/path/to/file.txt", "host2:..");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be '..'"));
    }

    #[test]
    fn test_validate_destination_path_dot_with_slash_allowed() {
        // these should work fine because they end with '/' not '.'
        let result = resolve_destination_path("/path/to/file.txt", "./").unwrap();
        assert_eq!(result, "./file.txt");
        let result = resolve_destination_path("/path/to/file.txt", "../").unwrap();
        assert_eq!(result, "../file.txt");
    }

    #[test]
    fn test_validate_destination_path_normal_paths_allowed() {
        // normal paths should work fine
        let result = resolve_destination_path("/path/to/file.txt", "/dest/normal").unwrap();
        assert_eq!(result, "/dest/normal");
        let result = resolve_destination_path("/path/to/file.txt", "/dest.txt").unwrap();
        assert_eq!(result, "/dest.txt");
        // paths containing dots but not ending with them should work
        let result = resolve_destination_path("/path/to/file.txt", "/dest.backup/").unwrap();
        assert_eq!(result, "/dest.backup/file.txt");
    }

    #[test]
    fn test_resolve_destination_path_remote_with_complex_host() {
        // test with complex remote paths including ports and IPv6
        let result =
            resolve_destination_path("host:/path/to/file.txt", "user@host2:22:/backup/").unwrap();
        assert_eq!(result, "user@host2:22:/backup/file.txt");

        let result =
            resolve_destination_path("[::1]:/path/file.txt", "[2001:db8::1]:8080:/dest/").unwrap();
        assert_eq!(result, "[2001:db8::1]:8080:/dest/file.txt");
    }

    #[test]
    fn test_split_remote_path() {
        // test remote path splitting
        assert_eq!(
            split_remote_path("user@host:22:/path/file"),
            (Some("user@host:22:".to_string()), "/path/file")
        );
        assert_eq!(
            split_remote_path("host:/path/file"),
            (Some("host:".to_string()), "/path/file")
        );
        assert_eq!(
            split_remote_path("[::1]:8080:/path/file"),
            (Some("[::1]:8080:".to_string()), "/path/file")
        );

        // test local path
        assert_eq!(
            split_remote_path("/local/path/file"),
            (None, "/local/path/file")
        );
        assert_eq!(
            split_remote_path("relative/path/file"),
            (None, "relative/path/file")
        );
    }

    #[test]
    fn test_extract_filesystem_path() {
        // test remote paths
        assert_eq!(
            extract_filesystem_path("user@host:22:/path/file"),
            "/path/file"
        );
        assert_eq!(extract_filesystem_path("host:/path/file"), "/path/file");
        assert_eq!(
            extract_filesystem_path("[::1]:8080:/path/file"),
            "/path/file"
        );

        // test local paths
        assert_eq!(
            extract_filesystem_path("/local/path/file"),
            "/local/path/file"
        );
        assert_eq!(
            extract_filesystem_path("relative/path/file"),
            "relative/path/file"
        );
    }

    #[test]
    fn test_join_path_with_filename() {
        // test remote path joining
        assert_eq!(
            join_path_with_filename(Some("user@host:22:".to_string()), "/backup/", "file.txt"),
            "user@host:22:/backup/file.txt"
        );
        assert_eq!(
            join_path_with_filename(Some("[::1]:8080:".to_string()), "/dest/", "file.txt"),
            "[::1]:8080:/dest/file.txt"
        );

        // test local path joining
        assert_eq!(
            join_path_with_filename(None, "/backup/", "file.txt"),
            "/backup/file.txt"
        );
        assert_eq!(
            join_path_with_filename(None, "relative/", "file.txt"),
            "relative/file.txt"
        );
    }

    #[test]
    fn test_ipv6_edge_cases_consistency() {
        // Test that helper functions and parse_path handle IPv6 consistently
        // Note: [::1] without user/port is now treated as local (localhost),
        // while other IPv6 addresses and [::1] with user/port remain remote
        let remote_test_cases = [
            "[2001:db8::1]:/path/file",
            "[2001:db8::1]:8080:/path/file",
            "user@[::1]:/path/file",
            "user@[2001:db8::1]:22:/path/file",
        ];
        for case in remote_test_cases {
            // Test that split_remote_path works correctly
            let (prefix, _path_part) = split_remote_path(case);
            assert!(prefix.is_some(), "Should detect {case} as remote");
            // Test that extract_filesystem_path works correctly
            let fs_path = extract_filesystem_path(case);
            assert_eq!(
                fs_path, "/path/file",
                "Should extract filesystem path from {case}"
            );
            // Test that parse_path treats non-localhost IPv6 as remote
            match parse_path(case).unwrap() {
                PathType::Remote(remote) => {
                    assert_eq!(
                        remote.path().to_str().unwrap(),
                        "/path/file",
                        "parse_path should extract same filesystem path from {case}"
                    );
                }
                PathType::Local(_) => panic!("parse_path should detect {case} as remote"),
            }
            // Test that join_path_with_filename can reconstruct correctly
            if let (Some(host_prefix), dir_path) = split_remote_path(&case.replace("file", "")) {
                let reconstructed = join_path_with_filename(Some(host_prefix), dir_path, "file");
                assert_eq!(
                    reconstructed, case,
                    "Should be able to reconstruct {case} correctly"
                );
            }
        }
        // [::1] without user/port is now local (localhost loopback)
        match parse_path("[::1]:/path/file").unwrap() {
            PathType::Local(path) => {
                assert_eq!(path.to_str().unwrap(), "/path/file");
            }
            PathType::Remote(_) => panic!("[::1]:/path should be local"),
        }
    }

    #[test]
    fn test_paths_with_colons_are_local() {
        // paths with colons in filename should be treated as local when they start with /
        let test_cases = [
            "/tmp/test-2024-01-01T12:30:45.txt",
            "/tmp/file:with:multiple:colons",
            "/path/to/foo:bar",
        ];
        for case in test_cases {
            match parse_path(case).unwrap() {
                PathType::Local(path) => {
                    assert_eq!(
                        path.to_str().unwrap(),
                        case,
                        "Path with colons should be parsed as local: {case}"
                    );
                }
                PathType::Remote(_) => {
                    panic!("Path with colons should NOT be parsed as remote: {case}")
                }
            }
        }
    }

    #[test]
    fn test_localhost_prefix_is_local() {
        // localhost: prefix should be treated as local path (escape hatch for paths with colons)
        match parse_path("localhost:/tmp/file:with:colons").unwrap() {
            PathType::Local(path) => {
                assert_eq!(path.to_str().unwrap(), "/tmp/file:with:colons");
            }
            PathType::Remote(_) => {
                panic!("localhost:/path should be parsed as local");
            }
        }
        // also test 127.0.0.1 and [::1]
        match parse_path("127.0.0.1:/tmp/test").unwrap() {
            PathType::Local(path) => assert_eq!(path.to_str().unwrap(), "/tmp/test"),
            PathType::Remote(_) => panic!("127.0.0.1:/path should be local"),
        }
        match parse_path("[::1]:/tmp/test").unwrap() {
            PathType::Local(path) => assert_eq!(path.to_str().unwrap(), "/tmp/test"),
            PathType::Remote(_) => panic!("[::1]:/path should be local"),
        }
    }

    #[test]
    fn test_localhost_with_user_or_port_is_remote() {
        // localhost with user or port should still be remote (explicit SSH config)
        match parse_path("user@localhost:/tmp/test").unwrap() {
            PathType::Remote(remote) => {
                assert_eq!(remote.session().host, "localhost");
                assert_eq!(remote.session().user, Some("user".to_string()));
            }
            PathType::Local(_) => {
                panic!("user@localhost:/path should be remote");
            }
        }
        match parse_path("localhost:22:/tmp/test").unwrap() {
            PathType::Remote(remote) => {
                assert_eq!(remote.session().host, "localhost");
                assert_eq!(remote.session().port, Some(22));
            }
            PathType::Local(_) => {
                panic!("localhost:22:/path should be remote");
            }
        }
    }

    #[test]
    fn test_parse_path_force_remote_localhost() {
        // parse_path_force_remote should treat localhost as remote
        match parse_path_force_remote("localhost:/tmp/test").unwrap() {
            PathType::Remote(remote) => {
                assert_eq!(remote.session().host, "localhost");
                assert_eq!(remote.path().to_str().unwrap(), "/tmp/test");
            }
            PathType::Local(_) => {
                panic!("parse_path_force_remote should treat localhost as remote");
            }
        }
    }

    #[test]
    fn test_is_localhost_variants() {
        assert!(is_localhost("localhost"));
        assert!(is_localhost("LOCALHOST"));
        assert!(is_localhost("Localhost"));
        assert!(is_localhost("127.0.0.1"));
        assert!(is_localhost("[::1]"));
        assert!(!is_localhost("example.com"));
        assert!(!is_localhost("192.168.1.1"));
    }
}
