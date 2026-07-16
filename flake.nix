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
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = pkgs.rust-bin.stable."1.95.0".default.override {
          extensions = [ "rustfmt" "clippy" "rust-src" ];
          targets = [ "x86_64-unknown-linux-musl" ];
        };

        # MSRV toolchain — used only by the `msrv-check` wrapper (and CI's `msrv`
        # job) to verify the workspace still compiles on the minimum supported
        # Rust version. Kept separate from `rustToolchain` (latest stable) so
        # everyday dev work uses the newest compiler.
        msrvToolchain = pkgs.rust-bin.stable."1.91.1".minimal.override {
          targets = [ "x86_64-unknown-linux-gnu" "x86_64-unknown-linux-musl" ];
        };
        msrvCheck = pkgs.writeShellScriptBin "msrv-check" ''
          exec ${msrvToolchain}/bin/cargo check --workspace --locked --all-targets --target x86_64-unknown-linux-gnu --target x86_64-unknown-linux-musl "$@"
        '';

        muslTools =
          if pkgs.stdenv.isLinux then {
            gcc = pkgs.pkgsCross.musl64.buildPackages.gcc;
            binutils = pkgs.pkgsCross.musl64.buildPackages.binutils;
          } else null;

        # Build inputs needed for the Rust project
        buildInputs = with pkgs; lib.optionals stdenv.isDarwin [
          darwin.apple_sdk.frameworks.Security
          darwin.apple_sdk.frameworks.SystemConfiguration
        ];

        nativeBuildInputs =
          [ rustToolchain pkgs.pkg-config ]
          ++ pkgs.lib.optionals (muslTools != null) [
            muslTools.gcc
            muslTools.binutils
          ];

        # Tests that can't run in the Nix build sandbox -- they need setuid/chown
        # permissions, `getent`/NSS, git-derived version info, or network access.
        # Kept in sync with the nixpkgs package (pkgs/by-name/rc/rcp/package.nix);
        # everything else still runs, so a flake.lock bump that breaks the build or
        # the rest of the suite is still caught.
        sandboxSkippedTests = [
          # set setuid bits (3oXXX) on a test file, which the sandbox disallows
          "--skip=copy::copy_tests::check_default_mode"
          "--skip=test_weird_permissions"
          "--skip=test_edge_case_special_permissions"
          "--skip=test_default_strips_special_bits_on_directories"
          "--skip=test_default_strips_special_bits_on_files"
          "--skip=test_default_preserves_special_bits_on_directories"
          "--skip=test_preserve_all_preserves_special_bits_on_directories"
          "--skip=test_preserve_all_preserves_special_bits_on_files"
          "--skip=test_preserve_settings_dir_gid_time_7777"
          "--skip=test_preserve_settings_dir_7777_preserves_special_bits"
          "--skip=test_preserve_settings_file_7777_preserves_special_bits"
          "--skip=test_preserve_settings_none_strips_special_bits_on_directories"
          # expects overwrite behavior that doesn't work in a sandbox
          "--skip=test_overwrite_behavior"
          # need network access to determine the local IP address
          "--skip=test_remote"
          # expect version/git info that build.rs can't derive without git
          "--skip=version::tests::test_current_version"
          "--skip=test_protocol_version_has_git_info"
          "--skip=test_rcpd_protocol_version_has_git_info"
          # shell out to `getent` to resolve real user/group names
          "--skip=chmod::tests::getent_real_resolves_root"
          "--skip=chmod::tests::getent_real_option_like_name_fails_closed_no_injection"
          "--skip=rejects_unknown_group"
          # change ownership / set setuid/setgid bits (fchown / chmod / chgrp), which
          # the unprivileged sandbox build user isn't permitted to do (EPERM)
          "--skip=safedir::tests::set_dir_metadata_fd_applies"
          "--skip=safedir::tests::set_file_metadata_fd_ordering_preserves_setuid"
          "--skip=applies_per_type_modes_recursively"
          "--skip=group_change_preserves_setgid_across_chgrp"
          "--skip=preserves_setgid_through_mode_change"
          "--skip=no_setid_clears_bits_for_unchanged_owner_rule"
          "--skip=no_setid_clears_existing_bits_for_unrelated_mode"
          "--skip=no_setid_dry_run_reports_but_does_not_clear_bits"
          "--skip=no_setid_respects_filter_and_per_type_scope"
          "--skip=no_setid_retains_sticky_and_clears_setgid_on_directory"
        ];

        # Package builder for RCP tools with custom binary names
        mkRcpPackage = { packageName, binaryName, description }: pkgs.rustPlatform.buildRustPackage {
          pname = binaryName;
          version = "0.38.0";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          inherit buildInputs nativeBuildInputs;

          # Build and test only the specific package
          cargoBuildFlags = [ "-p" packageName ];
          cargoTestFlags = [ "-p" packageName ];

          # Run the package's tests, skipping the ones the sandbox can't support.
          checkFlags = sandboxSkippedTests;

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
            version = "0.38.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            inherit buildInputs nativeBuildInputs;

            # Build and test the whole workspace, skipping the sandbox-incompatible
            # tests (mirrors the nixpkgs package, which also builds the full workspace).
            checkFlags = sandboxSkippedTests;

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
                pkgs.just
                pkgs.llvmPackages.bintools
                pkgs.tokio-console

                # Additional useful tools
                pkgs.gh
                pkgs.pkg-config
              ]
              ++ buildInputs
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
