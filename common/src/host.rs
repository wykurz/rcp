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
//! - [`Host::tcp_buffers`] decides which socket buffer sizes each end of a remote copy asks the
//!   kernel for (the `socket_buffers` module of the `remote` crate).
//!
//! [`current`] is the one place a process looks at its host, and it looks at each thing once.

/// What a process found out about the host it runs on.
#[derive(Debug)]
pub struct Host {
    /// The host's name, when the system reports one that is valid UTF-8.
    pub name: Option<String>,
    /// How many threads this process can run in parallel, when the system reports it. This
    /// honours CPU affinity and container quotas, so it can be less than the machine has.
    pub cpus: Option<std::num::NonZeroUsize>,
    /// Filled by the first call of [`Self::tcp_buffers`].
    tcp_buffers: std::sync::OnceLock<Result<TcpBufferLimits, String>>,
}

/// The kernel's limits for the two ways a TCP socket buffer gets its size.
///
/// A program can ask for a size (`SO_SNDBUF`, `SO_RCVBUF`). The kernel then grants at most the
/// request limit, without saying so, and never changes that buffer again. Or the program asks for
/// nothing: the kernel starts the buffer small and grows it as the connection needs, up to the
/// growth limit. The two limits are separate settings, and on many hosts the growth limit is the
/// far larger one, so asking can leave a socket with less than saying nothing would.
///
/// The growth limits are in the unit the kernel accounts buffers in, which is what `getsockopt`
/// reports. A granted request counts double there: see [`Self::send_buffer_for_request`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpBufferLimits {
    /// The most a program is granted when it asks for a send buffer (`net.core.wmem_max`, as a
    /// socket experiences it).
    pub send_request: u64,
    /// The most a program is granted when it asks for a receive buffer (`net.core.rmem_max`,
    /// likewise).
    pub receive_request: u64,
    /// How far the kernel grows a send buffer nobody asked for (last value of
    /// `net.ipv4.tcp_wmem`).
    pub send_growth: u64,
    /// How large a receive buffer nobody asked for can get: the last value of
    /// `net.ipv4.tcp_rmem`, or its middle value, the size every buffer starts with, when
    /// `net.ipv4.tcp_moderate_rcvbuf` switches the growth off.
    pub receive_growth: u64,
}

impl TcpBufferLimits {
    /// Where a running kernel publishes its growth limits. They belong to the network namespace
    /// of the reading process.
    const KERNEL_SETTINGS: &'static str = "/proc/sys/net";

    /// Find out the limits of the running kernel. Fails when the kernel refuses a socket to ask
    /// with, or when a growth setting is missing, unreadable or not a number.
    fn detect() -> std::io::Result<Self> {
        let probe = RequestProbe::open()?;
        let (send_growth, receive_growth) =
            Self::read_growth_from(std::path::Path::new(Self::KERNEL_SETTINGS))?;
        Ok(Self {
            send_request: probe.limit(libc::SO_SNDBUF)?,
            receive_request: probe.limit(libc::SO_RCVBUF)?,
            send_growth,
            receive_growth,
        })
    }

    /// Read the growth limits, for sending and for receiving, from the settings under `root`.
    /// Fails when a setting is missing, unreadable or not a number.
    fn read_growth_from(root: &std::path::Path) -> std::io::Result<(u64, u64)> {
        // one whitespace-separated value of one setting
        let value = |setting: &str, index: usize| -> std::io::Result<u64> {
            let invalid = |problem: String| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("kernel setting {setting}: {problem}"),
                )
            };
            let text = std::fs::read_to_string(root.join(setting)).map_err(|error| {
                std::io::Error::new(error.kind(), format!("kernel setting {setting}: {error}"))
            })?;
            let field = text
                .split_whitespace()
                .nth(index)
                .ok_or_else(|| invalid(format!("no value {} in {text:?}", index + 1)))?;
            field
                .parse()
                .map_err(|error| invalid(format!("{field:?} is not a size: {error}")))
        };
        let receive_buffers_grow = value("ipv4/tcp_moderate_rcvbuf", 0)? != 0;
        Ok((
            value("ipv4/tcp_wmem", 2)?,
            value("ipv4/tcp_rmem", if receive_buffers_grow { 2 } else { 1 })?,
        ))
    }

    /// The send buffer a request for `size` bytes yields, as the kernel accounts it: twice the
    /// granted size, the grant being cut to the request limit.
    #[must_use]
    pub fn send_buffer_for_request(&self, size: usize) -> u64 {
        Self::buffer_for_request(size, self.send_request)
    }

    /// The receive buffer a request for `size` bytes yields, as the kernel accounts it.
    #[must_use]
    pub fn receive_buffer_for_request(&self, size: usize) -> u64 {
        Self::buffer_for_request(size, self.receive_request)
    }

    fn buffer_for_request(size: usize, limit: u64) -> u64 {
        u64::try_from(size)
            .unwrap_or(u64::MAX)
            .min(limit)
            .saturating_mul(2)
    }
}

/// A socket for finding out the most the kernel grants a program that asks for a buffer size.
///
/// The settings behind these limits (`net.core.wmem_max`, `net.core.rmem_max`) are not visible
/// inside a network namespace on kernels before 6.12, so a process in a container could not read
/// them there, although they bind it all the same. A socket tells in every namespace: it asks
/// for the largest size there is and reads back what it got, which is twice the limit. It also
/// tells the truth where a setting exceeds what the kernel can grant at all (half the largest
/// 32-bit size).
///
/// The socket is not an rcp connection: it never connects, so none of the connection options
/// apply to it.
struct RequestProbe(std::os::fd::OwnedFd);

impl RequestProbe {
    fn open() -> std::io::Result<Self> {
        use std::os::fd::FromRawFd;
        // safety: plain socket(2); the result is checked before it is used
        let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        if raw < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // safety: `raw` is a descriptor this function just opened and nothing else owns
        Ok(Self(unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) }))
    }

    /// The request limit behind `option`, which is `SO_SNDBUF` or `SO_RCVBUF`.
    fn limit(&self, option: libc::c_int) -> std::io::Result<u64> {
        use std::os::fd::AsRawFd;
        let request: libc::c_int = libc::c_int::MAX;
        let mut granted: libc::c_int = 0;
        let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // safety: both calls pass pointers to live integers of the stated size
        let failed = unsafe {
            libc::setsockopt(
                self.0.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                (&raw const request).cast(),
                length,
            ) != 0
                || libc::getsockopt(
                    self.0.as_raw_fd(),
                    libc::SOL_SOCKET,
                    option,
                    (&raw mut granted).cast(),
                    &raw mut length,
                ) != 0
        };
        if failed {
            return Err(std::io::Error::last_os_error());
        }
        Ok(u64::try_from(granted).unwrap_or(0) / 2)
    }
}

impl Host {
    /// Look at the running system. A fact the system does not report is `None`; looking never
    /// fails as a whole. The TCP buffer limits are left for [`Self::tcp_buffers`] to find out.
    fn detect() -> Self {
        Self {
            name: nix::unistd::gethostname()
                .ok()
                .and_then(|name| name.into_string().ok()),
            cpus: std::thread::available_parallelism().ok(),
            tcp_buffers: std::sync::OnceLock::new(),
        }
    }

    /// The kernel's limits for TCP socket buffers, or why they could not be found out.
    ///
    /// Found out on the first call and kept. Not earlier, because finding out opens a socket,
    /// which a tool that never talks to the network has no business doing. The reason for a
    /// failure is kept as text for whoever acts on the limits to report.
    pub fn tcp_buffers(&self) -> Result<&TcpBufferLimits, &str> {
        self.tcp_buffers
            .get_or_init(|| TcpBufferLimits::detect().map_err(|error| format!("{error:#}")))
            .as_ref()
            .map_err(String::as_str)
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
        let fresh = Host::detect();
        assert_eq!(current().name, fresh.name);
        assert_eq!(current().cpus, fresh.cpus);
        assert_eq!(current().tcp_buffers(), fresh.tcp_buffers());
    }

    #[test]
    fn tcp_buffer_limits_are_found_out_on_first_use_and_kept() {
        let host = Host::detect();
        assert!(
            host.tcp_buffers.get().is_none(),
            "looking at the host must not open a socket"
        );
        let first = host.tcp_buffers();
        assert!(host.tcp_buffers.get().is_some());
        match (first, host.tcp_buffers()) {
            (Ok(first), Ok(second)) => assert!(std::ptr::eq(first, second)),
            (Err(first), Err(second)) => assert!(std::ptr::eq(first, second)),
            (first, second) => panic!("the answer changed: {first:?} then {second:?}"),
        }
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
            tcp_buffers: std::sync::OnceLock::new(),
        };
        assert_eq!(host.name_or_unknown(), "unknown");
        let host = Host {
            name: Some("node7".to_string()),
            cpus: None,
            tcp_buffers: std::sync::OnceLock::new(),
        };
        assert_eq!(host.name_or_unknown(), "node7");
    }

    /// Kernel settings as files, the way `/proc/sys/net` presents them.
    fn settings(values: &[(&str, &str)]) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for (name, value) in values {
            let path = root.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, value).unwrap();
        }
        root
    }

    /// The growth settings a kernel before 6.18 ships.
    const STOCK: [(&str, &str); 3] = [
        ("ipv4/tcp_wmem", "4096\t16384\t4194304\n"),
        ("ipv4/tcp_rmem", "4096\t131072\t6291456\n"),
        ("ipv4/tcp_moderate_rcvbuf", "1\n"),
    ];

    #[test]
    fn growth_limits_are_the_last_values_of_the_kernels_settings() {
        let root = settings(&STOCK);
        assert_eq!(
            TcpBufferLimits::read_growth_from(root.path()).unwrap(),
            (4_194_304, 6_291_456)
        );
    }

    #[test]
    fn a_receive_buffer_that_never_grows_stays_at_its_starting_size() {
        let mut values = STOCK;
        values[2] = ("ipv4/tcp_moderate_rcvbuf", "0\n");
        let root = settings(&values);
        assert_eq!(
            TcpBufferLimits::read_growth_from(root.path()).unwrap(),
            (4_194_304, 131_072),
            "send buffers grow regardless"
        );
    }

    #[test]
    fn settings_that_cannot_be_understood_are_an_error_naming_the_setting() {
        for (index, broken) in [
            (0, "ipv4/tcp_wmem"),
            (1, "ipv4/tcp_rmem"),
            (2, "ipv4/tcp_moderate_rcvbuf"),
        ] {
            // missing
            let mut values = STOCK.to_vec();
            values.remove(index);
            let root = settings(&values);
            let error = TcpBufferLimits::read_growth_from(root.path()).unwrap_err();
            assert!(format!("{error:#}").contains(broken), "{error:#}");
            // not a number
            let mut values = STOCK;
            values[index] = (broken, "many\n");
            let root = settings(&values);
            let error = TcpBufferLimits::read_growth_from(root.path()).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            assert!(format!("{error:#}").contains(broken), "{error:#}");
        }
        // too few values where three are expected
        let mut values = STOCK;
        values[1] = ("ipv4/tcp_rmem", "4096 131072\n");
        let root = settings(&values);
        let error = TcpBufferLimits::read_growth_from(root.path()).unwrap_err();
        assert!(format!("{error:#}").contains("ipv4/tcp_rmem"), "{error:#}");
    }

    /// One number from a kernel setting, read without anything this module provides.
    fn kernel_setting(path: &str, index: usize) -> Option<u64> {
        std::fs::read_to_string(path)
            .ok()?
            .split_whitespace()
            .nth(index)?
            .parse()
            .ok()
    }

    #[test]
    fn a_socket_tells_the_request_limits_the_kernel_settings_state() {
        let probe = RequestProbe::open().unwrap();
        let (send, receive) = (
            probe.limit(libc::SO_SNDBUF).unwrap(),
            probe.limit(libc::SO_RCVBUF).unwrap(),
        );
        assert!(send > 0 && receive > 0);
        // where the settings can be read (not inside a network namespace on kernels before
        // 6.12), they say the same, up to the most the kernel can grant whatever the setting
        let grantable = (i32::MAX / 2) as u64;
        if let Some(stated) = kernel_setting("/proc/sys/net/core/wmem_max", 0) {
            assert_eq!(send, stated.min(grantable));
        }
        if let Some(stated) = kernel_setting("/proc/sys/net/core/rmem_max", 0) {
            assert_eq!(receive, stated.min(grantable));
        }
        let again = RequestProbe::open().unwrap();
        assert_eq!(
            (
                probe.limit(libc::SO_SNDBUF).unwrap(),
                again.limit(libc::SO_RCVBUF).unwrap()
            ),
            (send, receive),
            "asking again, of the same socket or another, changes nothing"
        );
    }

    #[test]
    fn a_granted_request_counts_double_and_is_cut_to_the_request_limit() {
        let limits = TcpBufferLimits {
            send_request: 4 << 20,
            receive_request: 212_992,
            send_growth: 0,
            receive_growth: 0,
        };
        // within the limit: all of it, twice
        assert_eq!(limits.send_buffer_for_request(2 << 20), 4 << 20);
        assert_eq!(limits.send_buffer_for_request(4 << 20), 8 << 20);
        assert_eq!(limits.receive_buffer_for_request(100_000), 200_000);
        // beyond it: the limit, twice
        assert_eq!(limits.send_buffer_for_request(16 << 20), 8 << 20);
        assert_eq!(limits.receive_buffer_for_request(16 << 20), 425_984);
        assert_eq!(limits.receive_buffer_for_request(usize::MAX), 425_984);
    }

    #[test]
    fn detect_finds_the_tcp_buffer_limits_of_the_running_kernel() {
        let host = Host::detect();
        let found = host.tcp_buffers().copied();
        // the growth settings as the kernel publishes them, read here by their literal paths so
        // that a wrong path in this module cannot make both sides agree on nothing
        let send_growth = kernel_setting("/proc/sys/net/ipv4/tcp_wmem", 2);
        let receive_growth = kernel_setting("/proc/sys/net/ipv4/tcp_rmem", 2);
        let grows = kernel_setting("/proc/sys/net/ipv4/tcp_moderate_rcvbuf", 0);
        match (send_growth, receive_growth, grows) {
            (Some(send_growth), Some(receive_growth), Some(grows)) => {
                let limits = found.expect("readable settings must be found");
                assert_eq!(limits.send_growth, send_growth);
                if grows != 0 {
                    assert_eq!(limits.receive_growth, receive_growth);
                } else {
                    assert_eq!(
                        Some(limits.receive_growth),
                        kernel_setting("/proc/sys/net/ipv4/tcp_rmem", 1)
                    );
                }
                let probe = RequestProbe::open().unwrap();
                assert_eq!(limits.send_request, probe.limit(libc::SO_SNDBUF).unwrap());
                assert_eq!(
                    limits.receive_request,
                    probe.limit(libc::SO_RCVBUF).unwrap()
                );
            }
            // a sandbox without these settings: the reason is kept
            _ => assert!(!found.unwrap_err().is_empty()),
        }
    }
}
