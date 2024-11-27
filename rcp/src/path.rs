#[derive(Debug)]
pub struct RemotePath {
    pub session: remote::SshSession,
    // FIXME
    #[allow(dead_code)]
    pub path: String,
}

impl RemotePath {
    pub fn from_local(path: String) -> Self {
        Self {
            session: remote::SshSession::local(),
            path,
        }
    }
}

#[derive(Debug)]
pub enum PathType {
    Local(String),
    Remote(RemotePath),
}

impl PartialEq for PathType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PathType::Local(_), PathType::Local(_)) => true, // Local paths are always equal
            (PathType::Local(_), PathType::Remote(_)) => false,
            (PathType::Remote(_), PathType::Local(_)) => false,
            (PathType::Remote(remote1), PathType::Remote(remote2)) => {
                remote1.session == remote2.session
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
        let remote_path = captures.name("path").unwrap().as_str().to_string();
        PathType::Remote(RemotePath {
            session: remote::SshSession { user, host, port },
            path: remote_path,
        })
    } else {
        // It's a local path
        PathType::Local(path.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path_local() {
        match parse_path("/path/to/file") {
            PathType::Local(path) => assert_eq!(path, "/path/to/file"),
            _ => panic!("Expected local path"),
        }
    }

    #[test]
    fn test_parse_path_remote_basic() {
        match parse_path("host:/path/to/file") {
            PathType::Remote(RemotePath {
                session: remote::SshSession { user, host, port },
                path,
            }) => {
                assert_eq!(user, None);
                assert_eq!(host, "host");
                assert_eq!(port, None);
                assert_eq!(path, "/path/to/file");
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_parse_path_remote_full() {
        match parse_path("user@host:22:/path/to/file") {
            PathType::Remote(RemotePath {
                session: remote::SshSession { user, host, port },
                path,
            }) => {
                assert_eq!(user, Some("user".to_string()));
                assert_eq!(host, "host");
                assert_eq!(port, Some(22));
                assert_eq!(path, "/path/to/file");
            }
            _ => panic!("Expected remote path"),
        }
    }

    #[test]
    fn test_parse_path_ipv6() {
        match parse_path("[2001:db8::1]:/path/to/file") {
            PathType::Remote(RemotePath {
                session: remote::SshSession { user, host, port },
                path,
            }) => {
                assert_eq!(user, None);
                assert_eq!(host, "[2001:db8::1]");
                assert_eq!(port, None);
                assert_eq!(path, "/path/to/file");
            }
            _ => panic!("Expected remote path"),
        }
    }
}
