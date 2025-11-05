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

        rustToolchain = pkgs.rust-bin.stable."1.90.0".default.override {
          extensions = [ "rustfmt" "clippy" "rust-src" ];
          targets = [ "x86_64-unknown-linux-musl" ];
        };

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

        # Package builder for RCP tools with custom binary names
        mkRcpPackage = { packageName, binaryName, description }: pkgs.rustPlatform.buildRustPackage {
          pname = binaryName;
          version = "0.22.0";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          inherit buildInputs nativeBuildInputs;

          # Build only the specific package
          cargoBuildFlags = [ "-p" packageName ];
          cargoTestFlags = [ "-p" packageName ];

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
            version = "0.22.0";
            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            inherit buildInputs nativeBuildInputs;

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
                pkgs.rust-analyzer

                # Development tools from the original default.nix
                pkgs.binutils
                pkgs.cargo-bloat
                pkgs.cargo-deny
                pkgs.cargo-edit
                pkgs.cargo-expand
                pkgs.cargo-flamegraph
                pkgs.cargo-generate
                pkgs.cargo-nextest
                pkgs.cargo-outdated
                pkgs.cargo-udeps
                pkgs.gdb
                pkgs.just
                pkgs.llvmPackages.bintools
                pkgs.tokio-console

                # Additional useful tools
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
              echo "Individual tools: rcp, rrm, rlink, rcmp, filegen"
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
