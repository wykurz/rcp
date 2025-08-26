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
        
        rustToolchain = pkgs.rust-bin.stable."1.88.0".default.override {
          extensions = [ "rust-analysis" "rust-src" ];
        };

        # Build inputs needed for the Rust project
        buildInputs = with pkgs; lib.optionals stdenv.isDarwin [
          darwin.apple_sdk.frameworks.Security
          darwin.apple_sdk.frameworks.SystemConfiguration
        ];

        nativeBuildInputs = with pkgs; [
          rustToolchain
          pkg-config
        ];

        # Common package builder for all RCP tools
        mkRcpPackage = name: pkgs.rustPlatform.buildRustPackage {
          pname = name;
          version = "0.1.0";
          src = ./.;
          
          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          inherit buildInputs nativeBuildInputs;

          # Build only the specific binary
          buildAndTestSubdir = name;
          cargoBuildFlags = [ "-p" name ];
          cargoTestFlags = [ "-p" name ];

          meta = with pkgs.lib; {
            description = "Fast file operations tool";
            homepage = "https://github.com/mateusz/rcp";
            license = licenses.mit;
            maintainers = [ ];
          };
        };

      in
      {
        packages = {
          default = self.packages.${system}.rcp;
          
          # Individual packages for each tool
          rcp = mkRcpPackage "rcp";
          rrm = mkRcpPackage "rrm";
          rlink = mkRcpPackage "rlink";
          rcmp = mkRcpPackage "rcmp";
          rcpd = mkRcpPackage "rcpd";
          filegen = mkRcpPackage "filegen";
          
          # All tools in one package
          rcp-all = pkgs.rustPlatform.buildRustPackage {
            pname = "rcp-all";
            version = "0.1.0";
            src = ./.;
            
            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            inherit buildInputs nativeBuildInputs;

            meta = with pkgs.lib; {
              description = "Fast file operations tools suite";
              homepage = "https://github.com/mateusz/rcp";
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
            llvmPackages.bintools
            tokio-console
            
            # Additional useful tools
            pkg-config
          ] ++ buildInputs;

          RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/src";
          
          # Environment variables for development
          shellHook = ''
            echo "RCP development environment"
            echo "Available commands:"
            echo "  cargo build          - Build all packages"
            echo "  cargo test           - Test all packages"
            echo "  cargo nextest run    - Test with better output (recommended)"
            echo "  cargo fmt            - Format code"
            echo "  cargo clippy         - Lint code"
            echo ""
            echo "Individual tools: rcp, rrm, rlink, rcmp, rcpd, filegen"
          '';
        };
      });
}