//! Facts about the host this process runs on.
//!
//! Every tool reads its own host. A remote copy involves up to three: the master, the source
//! `rcpd` and the destination `rcpd` each read theirs and tune themselves from it. A process never
//! assumes that a fact holds for a peer. Where two processes have to agree on a tunable, the
//! protocol carries the tunable, not the fact: the destination adopts the file limit the source
//! derived from its own CPU count.
//!
//! This module only observes. What a fact means for a tunable is decided where that tunable lives,
//! by a function that takes the fact as a plain value, so the decision can be tested against
//! made-up hosts:
//!
//! - [`Host::cpus`] sets the automatic `--max-files-in-flight`
//!   ([`crate::ResolvedFilesInFlight::automatic`]).
//! - [`Host::name`] tells whether an operand names this machine ([`crate::is_localhost`]) and
//!   labels trace and histogram files.
//!
//! [`current`] is the one place a process looks at its host, and it looks once.

/// What a process found out about the host it runs on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    /// The host's name, when the system reports one that is valid UTF-8.
    pub name: Option<String>,
    /// How many threads this process can run in parallel, when the system reports it. This
    /// honours CPU affinity and container quotas, so it can be less than the machine has.
    pub cpus: Option<std::num::NonZeroUsize>,
}

impl Host {
    /// Look at the running system. A fact the system does not report is `None`; looking never
    /// fails as a whole.
    fn detect() -> Self {
        Self {
            name: nix::unistd::gethostname()
                .ok()
                .and_then(|name| name.into_string().ok()),
            cpus: std::thread::available_parallelism().ok(),
        }
    }

    /// The host's name for labels that need some text: `unknown` when there is none.
    #[must_use]
    pub fn name_or_unknown(&self) -> &str {
        self.name.as_deref().unwrap_or("unknown")
    }
}

/// The host this process runs on, read on first use and kept for the life of the process.
#[must_use]
pub fn current() -> &'static Host {
    static CURRENT: std::sync::OnceLock<Host> = std::sync::OnceLock::new();
    CURRENT.get_or_init(Host::detect)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_reads_this_hosts_name_and_parallelism() {
        let host = Host::detect();
        let name = nix::unistd::gethostname()
            .unwrap()
            .into_string()
            .expect("the test host has a UTF-8 name");
        assert_eq!(host.name.as_deref(), Some(name.as_str()));
        assert_eq!(host.cpus, std::thread::available_parallelism().ok());
        assert!(host.cpus.is_some(), "Linux reports its parallelism");
    }

    #[test]
    fn current_is_read_once_and_matches_a_fresh_reading() {
        assert!(std::ptr::eq(current(), current()));
        assert_eq!(*current(), Host::detect());
    }

    #[test]
    fn an_operand_naming_this_host_is_local() {
        let name = nix::unistd::gethostname().unwrap().into_string().unwrap();
        assert!(crate::is_localhost(&name));
        for local in ["localhost", "127.0.0.1", "::1", "[::1]"] {
            assert!(crate::is_localhost(local), "{local}");
        }
        for remote in [
            format!("{name}.elsewhere"),
            format!("not-{name}"),
            String::new(),
        ] {
            assert!(!crate::is_localhost(&remote), "{remote:?}");
        }
    }

    #[test]
    fn a_host_without_a_name_is_labelled_unknown() {
        let host = Host {
            name: None,
            cpus: None,
        };
        assert_eq!(host.name_or_unknown(), "unknown");
        let host = Host {
            name: Some("node7".to_string()),
            cpus: None,
        };
        assert_eq!(host.name_or_unknown(), "node7");
    }
}
