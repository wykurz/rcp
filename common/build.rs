use std::process::Command;

fn main() {
    // Git directory is one level up from the package directory
    let git_dir = std::path::Path::new("..").join(".git");

    // Git describe (best effort, may fail without git)
    if let Ok(output) = Command::new("git")
        .current_dir("..")
        .args(["describe", "--tags", "--long", "--always", "--dirty"])
        .output()
    {
        if output.status.success() {
            let describe = String::from_utf8_lossy(&output.stdout);
            let describe = describe.trim();
            if !describe.is_empty() {
                println!("cargo:rustc-env=RCP_GIT_DESCRIBE={}", describe);
            }
        }
    }

    // Git hash (best effort, may fail without git)
    if let Ok(output) = Command::new("git")
        .current_dir("..")
        .args(["rev-parse", "HEAD"])
        .output()
    {
        if output.status.success() {
            let hash = String::from_utf8_lossy(&output.stdout);
            let hash = hash.trim();
            if !hash.is_empty() {
                println!("cargo:rustc-env=RCP_GIT_HASH={}", hash);
            }
        }
    }

    // Rerun if git state changes
    if git_dir.exists() {
        println!("cargo:rerun-if-changed=../.git/HEAD");
        println!("cargo:rerun-if-changed=../.git/refs");
    }
}
