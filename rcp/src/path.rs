#[derive(Debug)]
pub struct RemotePath {
    session: remote::SshSession,
    path: std::path::PathBuf,
}

impl RemotePath {
    pub fn new(session: remote::SshSession, path: std::path::PathBuf) -> anyhow::Result<Self> {
        if !path.is_absolute() {
            return Err(anyhow::anyhow!("Path must be absolute: {}", path.display()));
        }
        Ok(Self { session, path })
    }

    pub fn from_local(path: &std::path::Path) -> Self {
        Self {
            session: remote::SshSession::local(),
            path: path.to_path_buf(),
        }
    }

    pub fn session(&self) -> &remote::SshSession {
        &self.session
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
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

pub fn parse_path(path: &str) -> PathType {
    // Regular expression for remote paths with named groups
    let re = regex::Regex::new(
        r"^(?:(?P<user>[^@]+)@)?(?P<host>(?:\[[^\]]+\]|[^:\[\]]+))(?::(?P<port>\d+))?:(?P<path>.+)$",
    )
    .unwrap();
    if let Some(captures) = re.captures(path) {
        // It's a remote path
        let user = captures.name("user").map(|m| m.as_str().to_string());
        let host = captures.name("host").unwrap().as_str().to_string();
        let port = captures
            .name("port")
            .and_then(|m| m.as_str().parse::<u16>().ok());
        let remote_path = captures
            .name("path")
            .expect("Unable to extract file system path from provided remote path")
            .as_str();
        let remote_path = if std::path::Path::new(remote_path).is_absolute() {
            std::path::Path::new(remote_path).to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("/"))
                .join(remote_path)
        };
        PathType::Remote(
            RemotePath::new(remote::SshSession { user, host, port }, remote_path).unwrap(), // parse_path assumes valid paths for now
        )
    } else {
        // It's a local path
        PathType::Local(path.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path_local() {
        match parse_path("/path/to/file") {
            PathType::Local(path) => assert_eq!(path.to_str().unwrap(), "/path/to/file"),
            _ => panic!("Expected local path"),
        }
    }

    #[test]
    fn test_parse_path_remote_basic() {
        match parse_path("host:/path/to/file") {
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
        match parse_path("user@host:22:/path/to/file") {
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
        match parse_path("[2001:db8::1]:/path/to/file") {
            PathType::Remote(remote_path) => {
                assert_eq!(remote_path.session().user, None);
                assert_eq!(remote_path.session().host, "[2001:db8::1]");
                assert_eq!(remote_path.session().port, None);
                assert_eq!(remote_path.path().to_str().unwrap(), "/path/to/file");
            }
            _ => panic!("Expected remote path"),
        }
    }
}
