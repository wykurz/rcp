let
  rust_overlay = import (builtins.fetchTarball https://github.com/oxalica/rust-overlay/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [ rust_overlay ]; };
  myrust = nixpkgs.rust-bin.stable."1.85.0".default.override {
    extensions = [ "rust-analysis" "rust-src" ];
  };
in
  with nixpkgs;
  stdenv.mkDerivation {
    name = "rust-shell";
    buildInputs = [
      rust-analyzer
      myrust
      binutils
      # cargo-audit
      cargo-bloat
      # cargo-deb
      cargo-deny
      cargo-edit
      cargo-expand
      cargo-flamegraph
      cargo-generate
      cargo-outdated
      cargo-udeps
      gdb
      llvmPackages.bintools
      tokio-console
    ];
    RUST_SRC_PATH = "${myrust}/lib/rustlib/src/rust/src";
  }
