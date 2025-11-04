let
  rust_overlay = import (builtins.fetchTarball https://github.com/oxalica/rust-overlay/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [ rust_overlay ]; };
  myrust = nixpkgs.rust-bin.stable."1.90.0".default.override {
    extensions = [ "rust-analysis" "rust-src" ];
    targets = [ "x86_64-unknown-linux-musl" ];
  };
  muslTools =
    if nixpkgs.stdenv.isLinux then {
      gcc = nixpkgs.pkgsCross.musl64.buildPackages.gcc;
      binutils = nixpkgs.pkgsCross.musl64.buildPackages.binutils;
    } else null;
in
  with nixpkgs;
  stdenv.mkDerivation (
    let
      baseAttrs = {
        name = "rust-shell";
        buildInputs =
          [
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
            cargo-nextest
            cargo-outdated
            cargo-udeps
            gdb
            llvmPackages.bintools
            tokio-console
          ]
          ++ lib.optionals (muslTools != null) [
            muslTools.gcc
            muslTools.binutils
          ];
        RUST_SRC_PATH = "${myrust}/lib/rustlib/src/rust/src";
      };
      muslAttrs = if muslTools != null then {
        CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";
        CC_x86_64_unknown_linux_musl = "${muslTools.gcc}/bin/x86_64-unknown-linux-musl-gcc";
        AR_x86_64_unknown_linux_musl = "${muslTools.binutils}/bin/x86_64-unknown-linux-musl-ar";
        PKG_CONFIG_ALLOW_CROSS = "1";
      } else {};
    in
      baseAttrs // muslAttrs
  )
