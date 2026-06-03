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

    pub fn from_local(path: &std::path::Path) -> anyhow::Result<Self> {
        let path = if path.is_relative() {
            std::path::absolute(path)
                .with_context(|| format!("failed to resolve relative path: {}", path.display()))?
        } else {
            path.to_path_buf()
        };
        Ok(Self {
            session: remote::SshSession::local(),
            path,
            needs_remote_home: false,
        })
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

    /// Returns this path with `name` appended; the session and `needs_remote_home` flag are
    /// preserved. Joining a name onto an absolute path stays absolute, and a
    /// `needs_remote_home` path is allowed to be relative, so the [`RemotePath::new`]
    /// invariant cannot be violated.
    #[must_use]
    pub fn joined(&self, name: &std::ffi::OsStr) -> RemotePath {
        RemotePath {
            path: self.path.join(name),
            ..self.clone()
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

pub fn expand_local_home(path: &str) -> anyhow::Result<std::path::PathBuf> {
    expand_home_with(path, || {
        std::env::var("HOME")
            .map(std::path::PathBuf::from)
            .context("HOME environment variable is not set; required for tilde expansion")
    })
}

fn expand_home_with(
    path: &str,
    home: impl FnOnce() -> anyhow::Result<std::path::PathBuf>,
) -> anyhow::Result<std::path::PathBuf> {
    if let Some(rest) = path.strip_prefix("~/") {
        return Ok(home()?.join(rest));
    }
    if path == "~" {
        return home();
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
                return Err(anyhow::anyhow!(
                    "Relative paths are not supported for remote hosts: {path_part}\n\
Remote paths must be absolute (e.g., host:/absolute/path) or use tilde expansion (e.g., host:~/path).\n\
If you intended a local path, omit the host prefix."
                ));
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

/// Resolve the final destination for one source, applying the trailing-slash ("copy INTO
/// directory") rule on the already-parsed destination.
///
/// A `dst_str` ending in `/` means "copy INTO this directory": the source basename is appended
/// to the parsed destination (`dst/<src_basename>`); otherwise the parsed destination is used
/// verbatim. Classification (local vs remote) comes exclusively from the parsed values — the
/// caller parses both with the active parse function, so `--force-remote` and the `localhost:`
/// escape hatch are honored by construction. `dst_str` is consulted only for validation and
/// the trailing-slash rule (parsing does not preserve a trailing slash). `dst` must be the
/// result of parsing this same `dst_str` — the function trusts `dst_str` for the slash rule
/// and `dst` for the value.
pub fn resolve_destination(
    src: &PathType,
    dst: &PathType,
    dst_str: &str,
) -> anyhow::Result<PathType> {
    // validate the destination string doesn't end with problematic patterns; rcp also
    // validates once up front for early error ordering — these are cheap suffix checks, so
    // the small redundancy keeps this function safe in isolation
    validate_destination_path(dst_str)?;
    if !dst_str.ends_with('/') {
        // no trailing slash - use destination as-is
        return Ok(dst.clone());
    }
    // derive the source basename to append, using the parser's classification
    let src_file_name = match src {
        PathType::Local(src_path) => {
            // Local source: use the SAME decomposition the copy operation uses
            // (`root_operand_basename` canonicalizes `.`/`..`/`dir/..`), so `dst/<name>` names
            // exactly the entry that gets created — `rcp . out/` -> `out/<cwd-name>`. The path
            // is already `~`-expanded and `localhost:`-stripped by the parser. A plain
            // `file_name()` returns None for those operands and would reject them here, before
            // `common::copy` could canonicalize the source.
            common::walk::root_operand_basename(src_path)
                .with_context(|| format!("resolving source operand {src_path:?}"))?
        }
        PathType::Remote(src_remote) => {
            // Remote source: the basename must be resolved on the remote host, so only a
            // lexical `file_name()` is available here. A remote operand with no basename — a
            // path ending in `..`, or a bare `host:~` (stored as an empty path until the
            // remote home is known) — would need remote resolution and is not supported.
            let src_path = src_remote.path();
            src_path
                .file_name()
                .map(std::ffi::OsStr::to_owned)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Remote source path {:?} does not have a basename that can be \
                         resolved locally (e.g. a bare `host:~` or a path ending in `..`); \
                         use a destination without a trailing slash to name the result \
                         explicitly",
                        src_path
                    )
                })?
        }
    };
    // append the basename on the parsed destination: "baz/" + "bar" -> "baz/bar"
    Ok(match dst {
        PathType::Local(dir) => PathType::Local(dir.join(&src_file_name)),
        PathType::Remote(dir) => PathType::Remote(dir.joined(&src_file_name)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unwrap a local PathType or panic with a useful message.
    fn expect_local(pt: PathType) -> std::path::PathBuf {
        match pt {
            PathType::Local(p) => p,
            PathType::Remote(r) => panic!("expected local path, got remote {r:?}"),
        }
    }

    /// Unwrap a remote PathType or panic with a useful message.
    fn expect_remote(pt: PathType) -> RemotePath {
        match pt {
            PathType::Remote(r) => r,
            PathType::Local(p) => panic!("expected remote path, got local {p:?}"),
        }
    }

    #[test]
    fn remote_path_joined_appends_and_preserves_session_and_home_flag() {
        // absolute remote path: joined name stays absolute, session fields preserved
        let remote = expect_remote(parse_path("user@host:22:/backup/").unwrap());
        let joined = remote.joined(std::ffi::OsStr::new("file.txt"));
        assert_eq!(joined.path(), std::path::Path::new("/backup/file.txt"));
        assert_eq!(joined.session(), remote.session());
        assert!(!joined.needs_remote_home());
        // bare remote tilde ("host:~/" parses to an empty path with needs_remote_home):
        // the flag is preserved and the joined path stays relative until home expansion
        let tilde = expect_remote(parse_path("host:~/").unwrap());
        let joined = tilde.joined(std::ffi::OsStr::new("file.txt"));
        assert!(joined.needs_remote_home());
        assert_eq!(joined.path(), std::path::Path::new("file.txt"));
    }

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
    fn test_expand_home_with_tilde_slash() {
        let home = std::path::PathBuf::from("/tmp/fake-home");
        let expanded = expand_home_with("~/docs/file.txt", || Ok(home.clone())).unwrap();
        assert_eq!(expanded, home.join("docs/file.txt"));
    }
    #[test]
    fn test_expand_home_with_bare_tilde() {
        let home = std::path::PathBuf::from("/tmp/fake-home");
        let expanded = expand_home_with("~", || Ok(home.clone())).unwrap();
        assert_eq!(expanded, home);
    }
    #[test]
    fn test_expand_home_with_non_tilde_path_does_not_read_home() {
        let expanded = expand_home_with("/abs/path", || panic!("home should not be read")).unwrap();
        assert_eq!(expanded, std::path::PathBuf::from("/abs/path"));
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
    fn test_parse_path_remote_relative_is_error() {
        let result = parse_path("host:relative/path");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Relative paths are not supported for remote hosts")
        );
    }

    #[test]
    fn test_from_local_resolves_relative_path() {
        let cwd = std::env::current_dir().unwrap();
        let remote = RemotePath::from_local(std::path::Path::new("relative/path")).unwrap();
        assert_eq!(remote.path(), &cwd.join("relative/path"));
        assert!(remote.path().is_absolute());
    }

    #[test]
    fn test_from_local_preserves_absolute_path() {
        let remote = RemotePath::from_local(std::path::Path::new("/absolute/path")).unwrap();
        assert_eq!(remote.path(), std::path::Path::new("/absolute/path"));
    }

    #[test]
    fn test_parse_path_force_remote_relative_is_error() {
        let result = parse_path_force_remote("localhost:relative/path");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Relative paths are not supported for remote hosts")
        );
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
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("/dest/").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/").unwrap());
        assert_eq!(result, std::path::Path::new("/dest/file.txt"));
    }

    #[test]
    fn test_resolve_destination_path_local_without_trailing_slash() {
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("/dest/newname.txt").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/newname.txt").unwrap());
        assert_eq!(result, std::path::Path::new("/dest/newname.txt"));
    }

    #[test]
    fn test_resolve_destination_path_local_dotdot_source_uses_canonical_basename() {
        // A local source with no lexical basename (`.`/`..`/`dir/..`) resolves through the same
        // canonicalization the copy uses, so a trailing-slash dst appends the REAL entry name
        // instead of failing. `<tmp>/sub/..` canonicalizes to `<tmp>`.
        let tmp = tempfile::tempdir().unwrap();
        let sub = tmp.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let src = parse_path(&format!("{}/sub/..", tmp.path().to_str().unwrap())).unwrap();
        let dst = parse_path("/dest/").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/").unwrap());
        let expected_name = tmp.path().file_name().unwrap().to_str().unwrap();
        assert_eq!(
            result,
            std::path::PathBuf::from(format!("/dest/{expected_name}"))
        );
    }

    #[test]
    fn test_resolve_destination_path_local_dotdot_source_into_remote_dest() {
        // A LOCAL `.`/`..` source copied INTO a remote trailing-slash directory: the basename is
        // resolved locally (canonicalize) and appended to the parsed remote destination.
        let tmp = tempfile::tempdir().unwrap();
        let sub = tmp.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let src = parse_path(&format!("{}/sub/..", tmp.path().to_str().unwrap())).unwrap();
        let dst = parse_path("host:/backup/").unwrap();
        let result = expect_remote(resolve_destination(&src, &dst, "host:/backup/").unwrap());
        let expected_name = tmp.path().file_name().unwrap().to_str().unwrap();
        assert_eq!(
            result.path(),
            std::path::Path::new(&format!("/backup/{expected_name}"))
        );
        assert_eq!(result.session().host, "host");
    }

    #[test]
    fn test_resolve_destination_path_localhost_dotdot_source_uses_canonical_basename() {
        // A `localhost:`-prefixed source parses as LOCAL (escape hatch), so its basename resolves
        // through the same canonicalization a plain local source uses, not lexically from the raw
        // string. `<tmp>/sub/..` canonicalizes to `<tmp>`, so a trailing-slash dst appends the
        // REAL entry name rather than failing on the missing lexical basename.
        let tmp = tempfile::tempdir().unwrap();
        let sub = tmp.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let src = parse_path(&format!(
            "localhost:{}/sub/..",
            tmp.path().to_str().unwrap()
        ))
        .unwrap();
        let dst = parse_path("/dest/").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/").unwrap());
        let expected_name = tmp.path().file_name().unwrap().to_str().unwrap();
        assert_eq!(
            result,
            std::path::PathBuf::from(format!("/dest/{expected_name}"))
        );
    }

    #[test]
    fn test_resolve_destination_path_remote_dotdot_source_is_unsupported() {
        // A remote source's basename must be resolved on the remote host; a remote `.`/`..`
        // operand (no lexical basename) is unsupported and errors rather than canonicalizing
        // against the LOCAL filesystem.
        let src = parse_path("host:/data/proj/..").unwrap();
        let dst = parse_path("/dest/").unwrap();
        let result = resolve_destination(&src, &dst, "/dest/");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("does not have a basename")
        );
    }

    #[test]
    fn test_resolve_destination_path_remote_with_trailing_slash() {
        let src = parse_path("host:/path/to/file.txt").unwrap();
        let dst = parse_path("/dest/").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/").unwrap());
        assert_eq!(result, std::path::Path::new("/dest/file.txt"));
    }

    #[test]
    fn test_resolve_destination_path_remote_without_trailing_slash() {
        let src = parse_path("host:/path/to/file.txt").unwrap();
        let dst = parse_path("/dest/newname.txt").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/newname.txt").unwrap());
        assert_eq!(result, std::path::Path::new("/dest/newname.txt"));
    }

    #[test]
    fn test_resolve_destination_path_remote_complex() {
        let src = parse_path("user@host:22:/home/user/docs/report.pdf").unwrap();
        let dst = parse_path("host2:/backup/").unwrap();
        let result = expect_remote(resolve_destination(&src, &dst, "host2:/backup/").unwrap());
        assert_eq!(result.path(), std::path::Path::new("/backup/report.pdf"));
        assert_eq!(result.session().host, "host2");
        assert_eq!(result.session().user, None);
        assert_eq!(result.session().port, None);
    }

    #[test]
    fn test_validate_destination_path_dot_local() {
        let result = validate_destination_path("/dest/.");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot end with '/.'"));
        assert!(error.to_string().contains("use './' instead"));
    }

    #[test]
    fn test_validate_destination_path_double_dot_local() {
        let result = validate_destination_path("/dest/..");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot end with '/..'"));
        assert!(error.to_string().contains("use '../' instead"));
    }

    #[test]
    fn test_validate_destination_path_dot_remote() {
        let result = validate_destination_path("host2:/dest/.");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot end with '/.'"));
    }

    #[test]
    fn test_validate_destination_path_double_dot_remote() {
        let result = validate_destination_path("host2:/dest/..");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot end with '/..'"));
    }

    #[test]
    fn test_validate_destination_path_bare_dot() {
        let result = validate_destination_path(".");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be '.'"));
    }

    #[test]
    fn test_validate_destination_path_bare_double_dot() {
        let result = validate_destination_path("..");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be '..'"));
    }

    #[test]
    fn test_validate_destination_path_remote_bare_dot() {
        let result = validate_destination_path("host2:.");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be '.'"));
    }

    #[test]
    fn test_validate_destination_path_remote_bare_double_dot() {
        let result = validate_destination_path("host2:..");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be '..'"));
    }

    #[test]
    fn test_validate_destination_path_dot_with_slash_allowed() {
        // these should work fine because they end with '/' not '.'
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("./").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "./").unwrap());
        assert_eq!(result, std::path::Path::new("./file.txt"));
        let dst = parse_path("../").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "../").unwrap());
        assert_eq!(result, std::path::Path::new("../file.txt"));
    }

    #[test]
    fn test_validate_destination_path_normal_paths_allowed() {
        // normal paths should work fine
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("/dest/normal").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/normal").unwrap());
        assert_eq!(result, std::path::Path::new("/dest/normal"));
        let dst = parse_path("/dest.txt").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest.txt").unwrap());
        assert_eq!(result, std::path::Path::new("/dest.txt"));
        // paths containing dots but not ending with them should work
        let dst = parse_path("/dest.backup/").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest.backup/").unwrap());
        assert_eq!(result, std::path::Path::new("/dest.backup/file.txt"));
    }

    #[test]
    fn test_resolve_destination_path_remote_with_complex_host() {
        // complex remote destinations including ports and IPv6
        let src = parse_path("host:/path/to/file.txt").unwrap();
        let dst = parse_path("user@host2:22:/backup/").unwrap();
        let result =
            expect_remote(resolve_destination(&src, &dst, "user@host2:22:/backup/").unwrap());
        assert_eq!(result.path(), std::path::Path::new("/backup/file.txt"));
        assert_eq!(result.session().user, Some("user".to_string()));
        assert_eq!(result.session().host, "host2");
        assert_eq!(result.session().port, Some(22));
        // note: bare [::1] parses as LOCAL (localhost escape hatch) — the source basename is
        // identical either way; the IPv6 destination keeps host and port
        let src = parse_path("[::1]:/path/file.txt").unwrap();
        let dst = parse_path("[2001:db8::1]:8080:/dest/").unwrap();
        let result =
            expect_remote(resolve_destination(&src, &dst, "[2001:db8::1]:8080:/dest/").unwrap());
        assert_eq!(result.path(), std::path::Path::new("/dest/file.txt"));
        assert_eq!(result.session().host, "[2001:db8::1]");
        assert_eq!(result.session().port, Some(8080));
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

    #[test]
    fn resolve_destination_no_trailing_slash_returns_dst_verbatim() {
        // I1: without a trailing slash the parsed destination is the final name, used verbatim
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("/dest/newname.txt").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/newname.txt").unwrap());
        assert_eq!(result, std::path::Path::new("/dest/newname.txt"));
    }

    #[test]
    fn resolve_destination_joins_basename_on_local_dest() {
        // I2: trailing-slash local destination gets the source basename appended; a
        // `localhost:` destination parses as local (escape hatch) and behaves identically.
        // (the `~/x/` local-destination leg needs a real HOME and is covered end-to-end by
        // tilde_tests::test_local_copy_with_tilde_source_and_destination)
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("/dest/").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "/dest/").unwrap());
        assert_eq!(result, std::path::Path::new("/dest/file.txt"));
        let dst = parse_path("localhost:/dest/").unwrap();
        let result = expect_local(resolve_destination(&src, &dst, "localhost:/dest/").unwrap());
        assert_eq!(result, std::path::Path::new("/dest/file.txt"));
    }

    #[test]
    fn resolve_destination_joins_basename_on_remote_dest_preserving_session() {
        // I3: trailing-slash remote destination joins the name and keeps user/host/port
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("user@host2:22:/backup/").unwrap();
        let result =
            expect_remote(resolve_destination(&src, &dst, "user@host2:22:/backup/").unwrap());
        assert_eq!(result.path(), std::path::Path::new("/backup/file.txt"));
        assert_eq!(result.session().user, Some("user".to_string()));
        assert_eq!(result.session().host, "host2");
        assert_eq!(result.session().port, Some(22));
    }

    #[test]
    fn resolve_destination_remote_tilde_dest_preserves_needs_remote_home() {
        // I4: "host:~/" parses to an empty remote path with needs_remote_home; joining the
        // basename preserves that bookkeeping so apply_remote_home later yields home/<name>
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("host:~/").unwrap();
        let mut result = expect_remote(resolve_destination(&src, &dst, "host:~/").unwrap());
        assert!(result.needs_remote_home());
        assert_eq!(result.path(), std::path::Path::new("file.txt"));
        result.apply_remote_home(std::path::Path::new("/home/user"));
        assert_eq!(result.path(), std::path::Path::new("/home/user/file.txt"));
    }

    #[test]
    fn resolve_destination_force_remote_localhost_dest_stays_remote() {
        // I5: classification follows the caller's parse function — with --force-remote
        // parsing, a localhost: destination resolves as remote (and a localhost: source
        // uses the lexical remote basename)
        let src = parse_path_force_remote("localhost:/src/file.txt").unwrap();
        let dst = parse_path_force_remote("localhost:/dest/").unwrap();
        let result = expect_remote(resolve_destination(&src, &dst, "localhost:/dest/").unwrap());
        assert_eq!(result.path(), std::path::Path::new("/dest/file.txt"));
        assert_eq!(result.session().host, "localhost");
    }

    #[test]
    fn resolve_destination_rejects_invalid_destination_string() {
        // the resolver is safe in isolation: it validates dst_str before touching the paths
        let src = parse_path("/path/to/file.txt").unwrap();
        let dst = parse_path("/dest/.").unwrap();
        let result = resolve_destination(&src, &dst, "/dest/.");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot end with '/.'")
        );
    }
}
