use std::os::unix::ffi::OsStrExt;

fn main() {
    // source archives have no git inputs; do not inherit an enclosing repository's identity
    println!("cargo:rerun-if-changed=build.rs");
    if !std::path::Path::new("../.git").exists() {
        return;
    }
    if let Some(describe) = git(&["describe", "--tags", "--long", "--always", "--dirty"]) {
        println!("cargo:rustc-env=RCP_GIT_DESCRIBE={describe}");
    }
    if let Some(hash) = git(&["rev-parse", "HEAD"]) {
        println!("cargo:rustc-env=RCP_GIT_HASH={hash}");
    }
    // a linked worktree has a private HEAD; refs and packed-refs live in its shared git directory
    for name in ["HEAD", "packed-refs", "shallow", "reftable"] {
        if let Some(path) = git_path(&["rev-parse", "--git-path", name]) {
            watch_existing(&path);
        }
    }
    // reftable worktrees have private HEAD tables as well as shared branch/tag tables
    if let Some(common_dir) = git_path(&["rev-parse", "--git-common-dir"]) {
        watch_existing(&common_dir.join("reftable"));
    }
    watch_ref("refs/tags");
    if let Some(head) = git_output(&["symbolic-ref", "--quiet", "--no-recurse", "HEAD"]) {
        let direct = std::str::from_utf8(&head).ok().map(str::trim);
        let resolved = git(&["symbolic-ref", "--quiet", "HEAD"]);
        match direct {
            Some(reference) if Some(reference) == resolved.as_deref() => watch_ref(reference),
            // unusual symbolic chains or non-UTF-8 ref names need a broader watch
            _ => watch_ref("refs"),
        }
    }
}

fn git_output(args: &[&str]) -> Option<Vec<u8>> {
    let output = std::process::Command::new("git")
        .current_dir("..")
        .args(["--git-dir=.git", "--work-tree=."])
        .args(args)
        .output()
        .ok()?;
    output.status.success().then_some(output.stdout)
}

fn git(args: &[&str]) -> Option<String> {
    let output = git_output(args)?;
    let text = String::from_utf8_lossy(&output);
    let value = text.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn git_path(args: &[&str]) -> Option<std::path::PathBuf> {
    let output = git_output(args)?;
    let path = output.strip_suffix(b"\n").unwrap_or(&output);
    // git paths are relative to its working directory; preserve spaces and non-UTF-8 bytes
    Some(std::path::Path::new("..").join(std::ffi::OsStr::from_bytes(path)))
}

fn watch_existing(path: &std::path::Path) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn watch_ref(name: &str) {
    // a packed ref may have no loose file yet. Watch its existing parent for creation instead:
    // watching a missing path makes cargo rerun the script and rebuild dependents on every build
    if let Some(path) = git_path(&["rev-parse", "--git-path", name])
        && let Some(existing) = path.ancestors().find(|candidate| candidate.exists())
    {
        println!("cargo:rerun-if-changed={}", existing.display());
    }
}
