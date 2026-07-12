let
  rust_overlay = import (builtins.fetchTarball https://github.com/oxalica/rust-overlay/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [ rust_overlay ]; };
  myrust = nixpkgs.rust-bin.stable."1.95.0".default.override {
    extensions = [ "rust-analysis" "rust-src" ];
    targets = [ "x86_64-unknown-linux-musl" ];
  };
  msrvToolchain = nixpkgs.rust-bin.stable."1.91.1".minimal.override {
    targets = [ "x86_64-unknown-linux-gnu" "x86_64-unknown-linux-musl" ];
  };
  msrvCheck = nixpkgs.writeShellScriptBin "msrv-check" ''
    exec ${msrvToolchain}/bin/cargo check --workspace --locked --all-targets --target x86_64-unknown-linux-gnu --target x86_64-unknown-linux-musl "$@"
  '';
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
            msrvCheck
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
            inferno
            just
            cargo-outdated
            cargo-udeps
            dprint
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
        CC_x86_64_unknown_linux_musl = "${muslTools.gcc}/bin/x86_64-unknown-linux-musl-gcc";
        AR_x86_64_unknown_linux_musl = "${muslTools.binutils}/bin/x86_64-unknown-linux-musl-ar";
        PKG_CONFIG_ALLOW_CROSS = "1";
      } else {};
    in
      baseAttrs // muslAttrs
  )
