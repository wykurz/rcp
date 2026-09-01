{
  description = "RCP - Fast file operations in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    # Systems are enumerated explicitly rather than via `eachDefaultSystem`, which would also
    # claim x86_64-darwin: nixpkgs 26.11 dropped that platform, and merely importing nixpkgs for
    # it now throws, turning `nix flake check --all-systems` unconditionally red. Listing the
    # systems here makes an upstream platform removal a one-line edit instead of an eval failure
    # from a transitive input. Linux is what CI builds and tests; the Darwin outputs only have to
    # evaluate (the code is cfg-gated for non-Linux, with the hardened walk disabled there).
    # See https://nixos.org/manual/nixpkgs/unstable/release-notes#x86_64-darwin-26.11
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-linux"
      "aarch64-darwin"
    ] (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = pkgs.rust-bin.stable."1.95.0".default.override {
          extensions = [ "rustfmt" "clippy" "rust-src" ];
          targets = [ "x86_64-unknown-linux-musl" ];
        };

        depotPlatform = {
          "x86_64-linux" = {
            archive = "linux_amd64";
            hash = "sha256-V2/403jIp0Ygth2BNRPlKG8gWc3kWW46MezMIAWnRmk=";
          };
          "aarch64-linux" = {
            archive = "linux_arm64";
            hash = "sha256-q0VTWNPBgKPGAMmW3lA3J+lnCjRLSn/QvrVQ80nLvjw=";
          };
          "aarch64-darwin" = {
            archive = "darwin_arm64";
            hash = "sha256-rBNqwyK2Ch98IiDuGjvWnD6KllC2dasrLrvb+H7Lfrc=";
          };
        }.${system};

        depot = pkgs.stdenvNoCC.mkDerivation (finalAttrs: {
          pname = "depot";
          version = "2.102.7";

          src = pkgs.fetchurl {
            url = "https://github.com/depot/cli/releases/download/v${finalAttrs.version}/depot_${finalAttrs.version}_${depotPlatform.archive}.tar.gz";
            hash = depotPlatform.hash;
          };
          sourceRoot = ".";

          installPhase = ''
            runHook preInstall
            install -Dm755 bin/depot "$out/bin/depot"
            runHook postInstall
          '';
        });

        # MSRV toolchain — used only by the `msrv-check` wrapper (and CI's `msrv`
        # job) to verify the workspace still compiles on the minimum supported
        # Rust version. Kept separate from `rustToolchain` (latest stable) so
        # everyday dev work uses the newest compiler.
        msrvToolchain = pkgs.rust-bin.stable."1.91.1".minimal.override {
          targets = [ "x86_64-unknown-linux-gnu" "x86_64-unknown-linux-musl" ];
        };
        # `cargo` resolves `rustc` from PATH -- NOT from beside its own binary, the way a rustup
        # shim would. Running the MSRV cargo inside this devshell therefore used to compile with
        # the devshell's latest-stable rustc, which made the whole check vacuous: cargo derives
        # "the current Rust version" for its `rust-version` resolve check from the rustc it finds,
        # so a dependency needing 1.95 was accepted. Verified on the same tree -- enum-map 3.1.0
        # (requires 1.95) exited 0 with the devshell rustc on PATH and 101 with the MSRV one.
        #
        # So pin the compiler explicitly, three ways: RUSTC for cargo itself, RUSTDOC for doc
        # targets, and the PATH prefix for anything (build scripts, proc-macro tooling) that
        # resolves `rustc` by name. The assertion then fails loudly if a future edit drops one of
        # them, rather than silently going back to passing everything.
        msrvCheck = pkgs.writeShellScriptBin "msrv-check" ''
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

          exec ${msrvToolchain}/bin/cargo check --workspace --locked --all-targets --target x86_64-unknown-linux-gnu --target x86_64-unknown-linux-musl "$@"
        '';

        muslTools =
          if pkgs.stdenv.isLinux then {
            gcc = pkgs.pkgsCross.musl64.buildPackages.gcc;
            binutils = pkgs.pkgsCross.musl64.buildPackages.binutils;
          } else null;

        # Build inputs needed for the Rust project. Deliberately empty on every platform: nixpkgs
        # removed the `darwin.apple_sdk.frameworks.*` compatibility stubs this used to list for
        # Security/SystemConfiguration (evaluating them now throws, turning `nix flake check
        # --all-systems` unconditionally red) — the Darwin stdenv ships the SDK itself and this
        # project needs no framework beyond it. See the "Darwin legacy frameworks" section of the
        # nixpkgs manual. Kept as a binding (rather than deleted) so the package/dev-shell
        # definitions below keep one obvious place to add a real build input.
        buildInputs = [ ];

        nativeBuildInputs =
          [ rustToolchain pkgs.pkg-config ]
          ++ pkgs.lib.optionals (muslTools != null) [
            muslTools.gcc
            muslTools.binutils
          ];

        # nixpkgs adds exact-target rustflags such as frame pointers, so Cargo ignores
        # build.rustflags. A matching cfg target joins these required cfgs with the exact-target
        # flags instead of replacing them. Pass the same config to build and test so rustflags do
        # not invalidate otherwise reusable release units between the two phases.
        nixSandboxRustflags = ''target.'cfg(all())'.rustflags=["--cfg","tokio_unstable","--cfg","rcp_nix_sandbox"]'';
        nixSandboxCargoFlags = [ "--config" nixSandboxRustflags ];

        # Package builder for RCP tools with custom binary names
        mkRcpPackage = { packageName, binaryName, description }: pkgs.rustPlatform.buildRustPackage {
          pname = binaryName;
          version = "0.40.0";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          inherit buildInputs nativeBuildInputs;

          # Build and test only the specific package
          cargoBuildFlags = [ "-p" packageName ] ++ nixSandboxCargoFlags;
          cargoTestFlags = [ "-p" packageName ] ++ nixSandboxCargoFlags;

          checkFlags = [ "--test-threads=1" ];

          meta = with pkgs.lib; {
            description = description;
            homepage = "https://github.com/wykurz/rcp";
            license = licenses.mit;
            maintainers = [ ];
          };
        };

      in
      {
        packages = {
          default = self.packages.${system}.rcp;

          # Individual packages for each tool
          rcp = mkRcpPackage {
            packageName = "rcp-tools-rcp";
            binaryName = "rcp";
            description = "Fast file copy tool with remote support";
          };
          rrm = mkRcpPackage {
            packageName = "rcp-tools-rrm";
            binaryName = "rrm";
            description = "Fast file removal tool";
          };
          rchm = mkRcpPackage {
            packageName = "rcp-tools-rchm";
            binaryName = "rchm";
            description = "Fast recursive chmod/chgrp/chown tool";
          };
          rlink = mkRcpPackage {
            packageName = "rcp-tools-rlink";
            binaryName = "rlink";
            description = "Fast hard-linking tool";
          };
          rcmp = mkRcpPackage {
            packageName = "rcp-tools-rcmp";
            binaryName = "rcmp";
            description = "Fast file comparison tool";
          };
          filegen = mkRcpPackage {
            packageName = "rcp-tools-filegen";
            binaryName = "filegen";
            description = "File generation tool for testing";
          };

          # All tools in one package
          rcp-all = pkgs.rustPlatform.buildRustPackage {
            pname = "rcp-all";
            version = "0.40.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            inherit buildInputs nativeBuildInputs;

            cargoBuildFlags = nixSandboxCargoFlags;
            cargoTestFlags = nixSandboxCargoFlags;
            checkFlags = [ "--test-threads=1" ];

            meta = with pkgs.lib; {
              description = "Fast file operations tools suite";
              homepage = "https://github.com/wykurz/rcp";
              license = licenses.mit;
              maintainers = [ ];
            };
          };
        };

        devShells.default = pkgs.mkShell (
          {
            buildInputs =
              [
                rustToolchain
                msrvCheck
                pkgs.rust-analyzer

                # Development tools from the original default.nix
                pkgs.binutils
                pkgs.cargo-bloat
                pkgs.cargo-deny
                pkgs.cargo-edit
                pkgs.cargo-expand
                pkgs.cargo-flamegraph
                pkgs.cargo-generate
                pkgs.inferno
                pkgs.cargo-nextest
                pkgs.cargo-outdated
                pkgs.cargo-udeps
                pkgs.dprint
                pkgs.gdb
                pkgs.jq
                pkgs.just
                pkgs.llvmPackages.bintools
                pkgs.tokio-console
                depot

                # Additional useful tools
                pkgs.gh
                pkgs.pkg-config
              ]
              ++ buildInputs
              # Linux-only tools: neither package evaluates on Darwin, so listing them
              # unconditionally breaks `nix flake check --all-systems` at evaluation time.
              ++ pkgs.lib.optionals pkgs.stdenv.isLinux [
                # `getfacl`/`setfacl`, for reading a POSIX ACL by hand when debugging the ACL
                # tests. Nothing depends on it: the fixtures write the xattrs directly, precisely
                # so the suite needs no runtime tool.
                pkgs.acl
                # `all_does_not_pay_the_acl_probe` traces an rcp run to count its ACL-probe
                # syscalls: the point of making ACLs opt-in is that the default path costs
                # nothing, which no outcome-only check can show. `just test` fails without it.
                pkgs.strace
              ]
              ++ pkgs.lib.optionals (muslTools != null) [
                muslTools.gcc
                muslTools.binutils
              ];

            RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/src";

            # Environment variables for development
            shellHook = ''
              echo "RCP development environment"
              echo ""
              echo "Quick start:"
              echo "  just            - List all available commands"
              echo "  just lint       - Run all lints (fmt + clippy + error logging)"
              echo "  just test       - Run tests with nextest"
              echo "  just ci         - Run all CI checks locally"
              echo ""
              echo "Other commands:"
              echo "  just fmt        - Format code"
              echo "  just check      - Quick compilation check"
              echo "  just build      - Build all packages"
              echo "  just doc        - Check documentation"
              echo ""
              echo "Individual tools: rcp, rrm, rchm, rlink, rcmp, filegen"
              echo "Note: rcpd is included with rcp (rcp-tools-rcp package)"
              echo ""
              echo "Static musl target enabled by default (.cargo/config.toml):"
              echo "  cargo build     -> x86_64-unknown-linux-musl"
            '';
          }
          // (
            if muslTools != null then {
              CC_x86_64_unknown_linux_musl = "${muslTools.gcc}/bin/x86_64-unknown-linux-musl-gcc";
              AR_x86_64_unknown_linux_musl = "${muslTools.binutils}/bin/x86_64-unknown-linux-musl-ar";
              PKG_CONFIG_ALLOW_CROSS = "1";
            } else {}
          )
        );
      });
}
