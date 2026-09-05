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
    # RCP supports Linux on x86_64 and AArch64. Enumerate those systems explicitly so the flake
    # does not publish package or development-shell outputs for unsupported hosts.
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-linux"
    ] (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        targetPlatform = import ./nix/target-platform.nix {
          inherit pkgs system;
        };
        x86_64MuslPlatform = import ./nix/target-platform.nix {
          inherit pkgs;
          system = "x86_64-linux";
        };
        aarch64MuslPlatform = import ./nix/target-platform.nix {
          inherit pkgs;
          system = "aarch64-linux";
        };

        mkRustToolchain = platform: pkgs.rust-bin.stable."1.95.0".default.override {
          extensions = [ "rustfmt" "clippy" "rust-src" ];
          targets = platform.rustTargets;
        };
        rustToolchain = mkRustToolchain targetPlatform;
        # build package hooks from the target stdenv itself. Setting CARGO_BUILD_TARGET on a
        # native-GNU buildRustPackage is insufficient because the hooks append their own
        # --target from stdenv.hostPlatform.
        packageRustPlatform = targetPlatform.packagePkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };
        pythonWithPyYAML = pkgs.python3.withPackages (pythonPackages: [ pythonPackages.pyyaml ]);

        depotPlatform = {
          "x86_64-linux" = {
            archive = "linux_amd64";
            hash = "sha256-V2/403jIp0Ygth2BNRPlKG8gWc3kWW46MezMIAWnRmk=";
          };
          "aarch64-linux" = {
            archive = "linux_arm64";
            hash = "sha256-q0VTWNPBgKPGAMmW3lA3J+lnCjRLSn/QvrVQ80nLvjw=";
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

          export CARGO="${msrvToolchain}/bin/cargo"
          exec ${pkgs.bash}/bin/bash ${./scripts/cargo-host.sh} check --workspace --locked --all-targets --target x86_64-unknown-linux-gnu --target x86_64-unknown-linux-musl "$@"
        '';

        # Keep one obvious place to add shared package and development-shell inputs.
        buildInputs = [ ];

        nativeBuildInputs = [ targetPlatform.packagePkgs.buildPackages.pkg-config ];

        # keep a dependency-free package on the exact production package path. Its check output
        # is small enough for every native lint run to realize, while still exercising the real
        # build, check, install, linker, and pinned-toolchain behavior.
        abiSmokePlatform = {
          "x86_64-linux" = {
            rustArch = "x86_64";
            rustEnv = "musl";
            filePattern = "ELF 64-bit.*x86-64";
            loader = "ld-musl-x86_64.so.1";
          };
          "aarch64-linux" = {
            rustArch = "aarch64";
            rustEnv = "musl";
            filePattern = "ELF 64-bit.*ARM aarch64";
            loader = "ld-musl-aarch64.so.1";
          };
        }.${system};
        abiSmokeCargoToml = builtins.toFile "rcp-nix-package-abi-smoke-Cargo.toml" ''
          [package]
          name = "rcp-nix-package-abi-smoke"
          version = "0.1.0"
          edition = "2024"
          build = "build.rs"
        '';
        abiSmokeCargoLock = builtins.toFile "rcp-nix-package-abi-smoke-Cargo.lock" ''
          # This file is automatically @generated by Cargo.
          # It is not intended for manual editing.
          version = 4

          [[package]]
          name = "rcp-nix-package-abi-smoke"
          version = "0.1.0"
        '';
        abiSmokeBuildRs = builtins.toFile "rcp-nix-package-abi-smoke-build.rs" ''
          use std::{env, fs};

          fn main() {
              println!("cargo:rerun-if-env-changed=RCP_NIX_ABI_BUILD_MARKER");
              if let Ok(marker) = env::var("RCP_NIX_ABI_BUILD_MARKER") {
                  fs::write(marker, "build-hook-ran\n").expect("write build marker");
              }
          }
        '';
        abiSmokeMain = builtins.toFile "rcp-nix-package-abi-smoke-main.rs" ''
          fn target_env() -> &'static str {
              if cfg!(target_env = "musl") {
                  "musl"
              } else {
                  "unexpected"
              }
          }

          fn main() {
              println!(
                  "rcp-nix-package-abi-smoke:{}:{}",
                  std::env::consts::ARCH,
                  target_env()
              );
          }

          #[cfg(test)]
          mod tests {
              use std::{env, fs};

              #[test]
              fn records_target_check_execution() {
                  assert_eq!(env::consts::ARCH, env!("RCP_NIX_ABI_EXPECTED_ARCH"));
                  assert_eq!(super::target_env(), env!("RCP_NIX_ABI_EXPECTED_ENV"));
                  let marker = env::var("RCP_NIX_ABI_CHECK_MARKER")
                      .expect("RCP_NIX_ABI_CHECK_MARKER is set by preCheck");
                  fs::write(marker, "check-hook-ran\n").expect("write check marker");
              }
          }
        '';
        abiSmokeSource = pkgs.runCommand "rcp-nix-package-abi-smoke-source" { } ''
          mkdir -p "$out/src"
          cp ${abiSmokeCargoToml} "$out/Cargo.toml"
          cp ${abiSmokeCargoLock} "$out/Cargo.lock"
          cp ${abiSmokeBuildRs} "$out/build.rs"
          cp ${abiSmokeMain} "$out/src/main.rs"
        '';
        packageAbiSmoke = packageRustPlatform.buildRustPackage {
          pname = "rcp-nix-package-abi-smoke";
          version = "0.1.0";
          src = abiSmokeSource;
          cargoLock.lockFile = abiSmokeCargoLock;

          inherit nativeBuildInputs;

          RCP_NIX_ABI_EXPECTED_ARCH = abiSmokePlatform.rustArch;
          RCP_NIX_ABI_EXPECTED_ENV = abiSmokePlatform.rustEnv;

          preBuild = ''
            export RCP_NIX_ABI_BUILD_MARKER="$NIX_BUILD_TOP/rcp-nix-abi-build-marker"
            cargo_version="$(cargo --version)"
            rustc_version="$(rustc --version)"
            case "$cargo_version" in
              "cargo 1.95.0"*) ;;
              *) echo "unexpected Cargo provider: $cargo_version" >&2; exit 1 ;;
            esac
            case "$rustc_version" in
              "rustc 1.95.0"*) ;;
              *) echo "unexpected rustc provider: $rustc_version" >&2; exit 1 ;;
            esac
            printf 'cargo 1.95.0\n' > "$NIX_BUILD_TOP/rcp-nix-abi-cargo-version"
            printf 'rustc 1.95.0\n' > "$NIX_BUILD_TOP/rcp-nix-abi-rustc-version"
          '';
          postBuild = ''
            test "$(<"$RCP_NIX_ABI_BUILD_MARKER")" = build-hook-ran
            unset RCP_NIX_ABI_BUILD_MARKER
          '';
          preCheck = ''
            export RCP_NIX_ABI_CHECK_MARKER="$NIX_BUILD_TOP/rcp-nix-abi-check-marker"
          '';
          postCheck = ''
            test "$(<"$RCP_NIX_ABI_CHECK_MARKER")" = check-hook-ran
            unset RCP_NIX_ABI_CHECK_MARKER
          '';
          postInstall = ''
            marker_dir="$out/share/rcp-nix-package-abi-smoke"
            mkdir -p "$marker_dir"
            install -m444 "$NIX_BUILD_TOP/rcp-nix-abi-build-marker" "$marker_dir/build"
            install -m444 "$NIX_BUILD_TOP/rcp-nix-abi-check-marker" "$marker_dir/check"
            install -m444 "$NIX_BUILD_TOP/rcp-nix-abi-cargo-version" \
              "$marker_dir/cargo-version"
            install -m444 "$NIX_BUILD_TOP/rcp-nix-abi-rustc-version" \
              "$marker_dir/rustc-version"
            printf 'install-hook-ran\n' > "$marker_dir/install"
            test -x "$out/bin/rcp-nix-package-abi-smoke"
          '';
        };
        packageAbiSmokeCheck = pkgs.runCommand "rcp-nix-package-abi-smoke-check" {
          nativeBuildInputs = [ pkgs.file pkgs.binutils ];
          passthru.cargoTarget = targetPlatform.cargoTarget;
        } ''
          set -euo pipefail
          package=${packageAbiSmoke}
          marker_dir="$package/share/rcp-nix-package-abi-smoke"
          probe="$package/bin/rcp-nix-package-abi-smoke"

          test "$(<"$marker_dir/build")" = build-hook-ran
          test "$(<"$marker_dir/check")" = check-hook-ran
          test "$(<"$marker_dir/install")" = install-hook-ran
          test "$(<"$marker_dir/cargo-version")" = 'cargo 1.95.0'
          test "$(<"$marker_dir/rustc-version")" = 'rustc 1.95.0'

          file_output="$(file -b "$probe")"
          printf '%s\n' "$file_output" | grep -E '${abiSmokePlatform.filePattern}'
          output="$("$probe")"
          test "$output" = \
            'rcp-nix-package-abi-smoke:${abiSmokePlatform.rustArch}:${abiSmokePlatform.rustEnv}'
          interpreter="$(
            readelf -l "$probe" |
              sed -n 's/.*Requesting program interpreter: \(.*\)]/\1/p'
          )"
          test -n "$interpreter"
          case "$interpreter" in
            *musl*'/${abiSmokePlatform.loader}') ;;
            *) echo "unexpected ELF interpreter: $interpreter" >&2; exit 1 ;;
          esac

          mkdir -p "$out/markers"
          cp "$marker_dir/build" "$out/markers/build"
          cp "$marker_dir/check" "$out/markers/check"
          cp "$marker_dir/install" "$out/markers/install"
          cp "$marker_dir/cargo-version" "$out/markers/cargo-version"
          cp "$marker_dir/rustc-version" "$out/markers/rustc-version"
          {
            printf 'target=%s\n' '${targetPlatform.cargoTarget}'
            printf 'file=%s\n' "$file_output"
            printf 'interpreter=%s\n' "$interpreter"
            printf 'output=%s\n' "$output"
            printf 'binary-executed=yes\n'
          } > "$out/evidence"
        '';

        # nixpkgs adds exact-target rustflags such as frame pointers, so Cargo ignores
        # build.rustflags. A matching cfg target joins these required cfgs with the exact-target
        # flags instead of replacing them. Pass the same config to build and test so rustflags do
        # not invalidate otherwise reusable release units between the two phases.
        nixSandboxRustflags = ''target.'cfg(all())'.rustflags=["--cfg","tokio_unstable","--cfg","rcp_nix_sandbox"]'';
        nixSandboxCargoFlags = [ "--config" nixSandboxRustflags ];

        # Package builder for RCP tools with custom binary names
        mkRcpPackage = { packageName, binaryName, description }:
          packageRustPlatform.buildRustPackage {
            pname = binaryName;
            version = "0.41.0";
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

        mkDevShell = platform:
          let
            toolchain = mkRustToolchain platform;
          in
          pkgs.mkShell ({
            buildInputs =
              [
                toolchain
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
                pythonWithPyYAML
                pkgs.tokio-console
                depot

                # Additional useful tools
                pkgs.gh
                pkgs.pkg-config
              ]
              ++ buildInputs
              ++ [
                # `getfacl`/`setfacl`, for reading a POSIX ACL by hand when debugging the ACL
                # tests. Nothing depends on it: the fixtures write the xattrs directly, precisely
                # so the suite needs no runtime tool.
                pkgs.acl
                # `all_does_not_pay_the_acl_probe` traces an rcp run to count its ACL-probe
                # syscalls: the point of making ACLs opt-in is that the default path costs
                # nothing, which no outcome-only check can show. `just test` fails without it.
                pkgs.strace
              ]
              ++ platform.buildTools;

            RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/src";

            passthru.cargoTarget = platform.cargoTarget;

            shellHook = platform.cargoTargetShellHook + ''
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
              echo "Cargo build target: $CARGO_BUILD_TARGET"
            '';
          } // platform.shellEnvironment);

      in
      {
        checks.package-abi-smoke = packageAbiSmokeCheck;

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
          rcp-all = packageRustPlatform.buildRustPackage {
            pname = "rcp-all";
            version = "0.41.0";
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

        devShells = {
          default = mkDevShell targetPlatform;
          x86_64-musl = mkDevShell x86_64MuslPlatform;
          aarch64-musl = mkDevShell aarch64MuslPlatform;
        };
      });
}
