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
          "--skip=copy_creates_file_owner_only_until_contents_are_written"
          "--skip=preserves_setuid_file_mode_when_created_owner_only"
          "--skip=interrupted_copy_leaves_partial_file_owner_only"
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
          "--skip=safedir::tests::secure_as_copier_takes_ownership_restricts_mode_and_preserves_gid"
          "--skip=applies_per_type_modes_recursively"
          "--skip=group_change_preserves_setgid_across_chgrp"
          "--skip=preserves_setgid_through_mode_change"
          "--skip=no_setid_clears_bits_for_unchanged_owner_rule"
          "--skip=no_setid_clears_existing_bits_for_unrelated_mode"
          "--skip=no_setid_dry_run_reports_but_does_not_clear_bits"
          "--skip=no_setid_respects_filter_and_per_type_scope"
          "--skip=no_setid_retains_sticky_and_clears_setgid_on_directory"
          # ACL tests that additionally set setuid/setgid, for the same EPERM reason
          "--skip=safedir::tests::set_file_metadata_fd_applies_a_setuid_source_mode_and_its_acl"
          "--skip=safedir::tests::set_file_metadata_fd_keeps_the_file_owner_only_when_the_acl_fails"
          "--skip=safedir::tests::set_file_metadata_fd_clears_an_inherited_acl_and_still_applies_the_full_mode"
          "--skip=safedir::tests::set_reused_dir_metadata_fd_verifies_a_setgid_source_with_an_acl"
          "--skip=preserves_an_acl_alongside_a_setuid_mode"
          # pads an xattr NAME list past its buffer with `user.*` attributes, which tmpfs only
          # supports on Linux >= 6.6 -- on an older kernel with a tmpfs build dir it hits
          # EOPNOTSUPP. `just test` and the normal CI matrix still cover the ERANGE re-read paths.
          "--skip=safedir::tests::read_acls_fd_reads_an_acl_larger_than_the_stack_buffers"
          # traces rcp with strace(1), which is neither present in nor permitted by the sandbox
          "--skip=all_does_not_pay_the_acl_probe"
          "--skip=strict_mode_does_not_enable_acl_preservation"
          "--skip=strict_mode_strips_once_per_directory_not_per_file"
          "--skip=the_root_warning_costs_one_syscall_whatever_the_tree_size"
          # Every test that needs a real POSIX ACL. The nix sandbox cannot hold one: a
          # `setxattr("system.posix_acl_access")` inside it returns EOPNOTSUPP, so the fixtures --
          # which deliberately panic rather than skip, so a lost feature cannot pass unnoticed --
          # abort. THE CAUSE IS THE SANDBOX, NOT THE FILESYSTEM: tmpfs itself holds POSIX ACLs
          # fine (verified directly on /dev/shm), so a future reader who tests tmpfs, finds ACLs
          # working and concludes these skips are stale would be wrong. The restriction is nix's
          # sandbox (user namespaces refuse `system.*` xattrs); pointing the fixtures at a
          # different directory does not help. `just ci` -- debug + release + Docker -- runs every
          # one of these, so nothing here is unverified; only this packaging check skips them.
          # (The setuid group above is also ACL-related, but is listed there because it needs
          # setuid/setgid as well and would fail in the sandbox for that reason too.)
          "--skip=a_root_that_cannot_warn_does_not_spend_the_probe_budget"
          "--skip=acl_clears_the_destination_trees_inherited_acl"
          "--skip=all_without_acl_drops_the_source_acl"
          "--skip=an_aborted_strict_copy_does_not_destroy_the_reused_directorys_acl"
          "--skip=clears_an_acl_the_destination_tree_would_have_imposed"
          "--skip=default_leaves_the_destination_trees_inherited_acl"
          "--skip=directory_acls_apply_normally_because_rlink_creates_directories_fresh"
          "--skip=hard_linking_never_writes_an_acl_through_the_shared_inode"
          "--skip=lockdown_reused_dir_never_loses_the_default_acl_when_cancelled"
          "--skip=per_type_acl_applies_only_to_the_type_that_asked_for_it"
          "--skip=preserves_a_source_access_acl_on_a_file"
          "--skip=preserves_both_acls_on_a_directory"
          "--skip=quiet_suppresses_the_root_notice"
          "--skip=require_toctou_safe_contains_an_inherited_destination_acl"
          "--skip=safedir::tests::apply_acls_fd_installs_the_source_acl_and_clears_what_the_source_lacked"
          "--skip=safedir::tests::read_acls_fd_reads_a_directorys_default_acl_only_when_asked"
          "--skip=safedir::tests::read_acls_fd_round_trips_an_access_acl"
          "--skip=safedir::tests::set_dir_metadata_fd_installs_access_and_default_acls"
          "--skip=safedir::tests::set_dir_metadata_fd_keeps_the_dir_owner_only_when_the_default_acl_fails"
          "--skip=stays_silent_when_the_source_root_acl_is_preserved"
          "--skip=stays_silent_when_the_source_root_has_no_acl"
          "--skip=strict_mode_contains_and_restores_a_reused_directorys_acls"
          "--skip=strict_mode_prevents_inheritance_in_every_directory_it_creates"
          "--skip=strict_mode_prevents_the_destination_trees_inherited_acl"
          "--skip=strict_reuse_rlink_restores_a_reused_dirs_acls"
          "--skip=the_root_warning_consults_the_setting_for_the_roots_own_kind"
          "--skip=the_strict_mode_warning_says_the_flag_does_not_preserve_source_acls"
          "--skip=warns_when_the_source_root_carries_an_acl_that_is_not_preserved"
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
                # `getfacl`/`setfacl`, for reading a POSIX ACL by hand when debugging the ACL
                # tests. Nothing depends on it: the fixtures write the xattrs directly, precisely
                # so the suite needs no runtime tool.
                pkgs.acl
                # `all_does_not_pay_the_acl_probe` traces an rcp run to count its ACL-probe
                # syscalls: the point of making ACLs opt-in is that the default path costs
                # nothing, which no outcome-only check can show. `just test` fails without it.
                pkgs.strace
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
