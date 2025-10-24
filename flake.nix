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

        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

        # Build inputs needed for the Rust project
        buildInputs = with pkgs; lib.optionals stdenv.isDarwin [
          darwin.apple_sdk.frameworks.Security
          darwin.apple_sdk.frameworks.SystemConfiguration
        ];

        nativeBuildInputs = with pkgs; [
          rustToolchain
          pkg-config
        ];

        # Package builder for RCP tools with custom binary names
        mkRcpPackage = { packageName, binaryName, description }: pkgs.rustPlatform.buildRustPackage {
          pname = binaryName;
          version = "0.21.1";
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
            version = "0.21.1";
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

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            rustToolchain
            rust-analyzer

            # Development tools from the original default.nix
            binutils
            cargo-bloat
            cargo-deny
            cargo-edit
            cargo-expand
            cargo-flamegraph
            cargo-generate
            cargo-nextest
            cargo-outdated
            cargo-udeps
            gdb
            just
            llvmPackages.bintools
            tokio-console

            # Additional useful tools
            pkg-config
          ] ++ buildInputs;

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
          '';
        };
      });
}
