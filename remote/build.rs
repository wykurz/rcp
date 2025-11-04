fn main() {
    // allow cfg(tokio_unstable) to pass cargo's check-cfg lint
    println!("cargo::rustc-check-cfg=cfg(tokio_unstable)");
}
