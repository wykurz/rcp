let
  rust_overlay = import (builtins.fetchTarball https://github.com/oxalica/rust-overlay/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [ rust_overlay ]; };
  targetPlatform = import ./nix/target-platform.nix {
    pkgs = nixpkgs;
    system = nixpkgs.stdenv.hostPlatform.system;
  };
  myrust = nixpkgs.rust-bin.stable."1.95.0".default.override {
    extensions = [ "rust-analysis" "rust-src" ];
    targets = targetPlatform.rustTargets;
  };
  pythonWithPyYAML = nixpkgs.python3.withPackages (pythonPackages: [ pythonPackages.pyyaml ]);
  msrvToolchain = nixpkgs.rust-bin.stable."1.91.1".minimal.override {
    targets = [ "x86_64-unknown-linux-gnu" "x86_64-unknown-linux-musl" ];
  };
  # Pin the compiler, not just cargo. `cargo` resolves `rustc` from PATH, so without this the MSRV
  # cargo compiles with `myrust` (latest stable) and the check passes vacuously -- including
  # cargo's own `rust-version` resolve check, which reads the version off the rustc it finds. The
  # full rationale and the measurement live next to the flake's copy of this wrapper. The two stay
  # duplicated on purpose: they are independent entry points (flake vs plain `nix-shell`), and
  # scripts/check-rust-version.sh reads the toolchain pin out of each file separately.
  msrvCheck = nixpkgs.writeShellScriptBin "msrv-check" ''
    set -euo pipefail
    export RUSTC="${msrvToolchain}/bin/rustc"
    export RUSTDOC="${msrvToolchain}/bin/rustdoc"
    export PATH="${msrvToolchain}/bin:$PATH"

    pinned="$("$RUSTC" --version)"
    resolved="$(rustc --version)"
    if [ "$pinned" != "$resolved" ]; then
      echo "msrv-check: PATH resolves a different rustc than RUSTC pins," >&2
      echo "  RUSTC: $pinned" >&2
      echo "  PATH:  $resolved" >&2
      echo "cargo would compile with the PATH one and the check would not test the MSRV." >&2
      exit 1
    fi
    echo "msrv-check: using $pinned"

    export CARGO="${msrvToolchain}/bin/cargo"
    exec ${./scripts/cargo-host.sh} check --workspace --locked --all-targets --target x86_64-unknown-linux-gnu --target x86_64-unknown-linux-musl "$@"
  '';
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
            pythonWithPyYAML
            tokio-console
          ]
          ++ targetPlatform.buildTools;
        RUST_SRC_PATH = "${myrust}/lib/rustlib/src/rust/src";
        passthru.cargoTarget = targetPlatform.cargoTarget;
        shellHook = targetPlatform.cargoTargetShellHook;
      };
    in
      baseAttrs // targetPlatform.shellEnvironment
  )
