use std::path::Path;

pub fn isolated(test_name: &str) -> anyhow::Result<bool> {
    let namespace = std::fs::read_link("/proc/self/ns/mnt")?;
    let marker = format!("RCP_MOUNT_TEST_ENTERED:{test_name}");
    let Ok(parent_namespace) = std::env::var("RCP_TEST_PARENT_MOUNT_NS") else {
        let executable = std::env::current_exe()?;
        let listing = std::process::Command::new(&executable)
            .args(["--ignored", "--exact", test_name, "--list"])
            .output()?;
        let listed = String::from_utf8_lossy(&listing.stdout);
        anyhow::ensure!(
            listing.status.success()
                && listed
                    .lines()
                    .filter(|line| line.ends_with(": test"))
                    .collect::<Vec<_>>()
                    == [format!("{test_name}: test")],
            "private mount regression did not select exactly {test_name:?}: {listing:?}"
        );
        // hosted CI uses its passwordless sudo instead of relying on unprivileged user namespaces
        let use_sudo = std::env::var_os("RCP_TEST_MOUNT_SUDO").is_some();
        let mut command = if use_sudo {
            let mut command = std::process::Command::new("sudo");
            command.args(["-n", "--preserve-env=RCP_TEST_PARENT_MOUNT_NS", "unshare"]);
            command
        } else {
            let mut command = std::process::Command::new("unshare");
            command.args(["--user", "--map-root-user"]);
            command
        };
        let output = command
            .args(["--mount", "--propagation", "private"])
            .arg(executable)
            .args(["--ignored", "--exact", test_name, "--nocapture"])
            .env("RCP_TEST_PARENT_MOUNT_NS", namespace)
            .output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::ensure!(
            output.status.success() && stderr.lines().any(|line| line == marker),
            "private mount regression failed or did not enter its fixture: {}\n{stdout}{stderr}",
            output.status
        );
        return Ok(false);
    };
    anyhow::ensure!(
        namespace != Path::new(&parent_namespace),
        "refusing mounts in the parent namespace"
    );
    eprintln!("{marker}");
    Ok(true)
}

pub struct BindMount(std::ffi::CString);

impl BindMount {
    pub fn new(source: &Path, destination: &Path) -> std::io::Result<Self> {
        use std::os::unix::ffi::OsStrExt;
        let source = std::ffi::CString::new(source.as_os_str().as_bytes())?;
        let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())?;
        // SAFETY: both paths are live NUL-terminated strings; MS_BIND ignores type and data
        let result = unsafe {
            libc::mount(
                source.as_ptr(),
                destination.as_ptr(),
                std::ptr::null(),
                libc::MS_BIND,
                std::ptr::null(),
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self(destination))
    }

    // this helper module is also compiled by CLI tests that need only bind mounts
    #[allow(dead_code)]
    pub fn tmpfs(destination: &Path) -> std::io::Result<Self> {
        use std::os::unix::ffi::OsStrExt;
        let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())?;
        // SAFETY: all strings are live and NUL-terminated; tmpfs needs no source or mount data
        let result = unsafe {
            libc::mount(
                std::ptr::null(),
                destination.as_ptr(),
                c"tmpfs".as_ptr(),
                0,
                std::ptr::null(),
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self(destination))
    }
}

impl Drop for BindMount {
    fn drop(&mut self) {
        // SAFETY: the mount path remains a live NUL-terminated string
        unsafe {
            libc::umount2(self.0.as_ptr(), libc::MNT_DETACH);
        }
    }
}
