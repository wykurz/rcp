{ system, pkgs ? null }:

let
  platforms = {
    x86_64-linux = {
      cargoTarget = "x86_64-unknown-linux-musl";
      crossPackages = packages: packages.pkgsCross.musl64;
    };
    aarch64-linux = {
      cargoTarget = "aarch64-unknown-linux-musl";
      crossPackages = packages: packages.pkgsCross.aarch64-multiplatform-musl;
    };
  };

  platform =
    if builtins.hasAttr system platforms then
      builtins.getAttr system platforms
    else
      throw "unsupported RCP Nix system: ${system}";
  packagePkgs =
    if pkgs == null then null
    else platform.crossPackages pkgs;
  toolPackages =
    if packagePkgs != null then packagePkgs.buildPackages else null;
  toolchain =
    if toolPackages != null then {
      gcc = toolPackages.gcc;
      binutils = toolPackages.binutils;
    } else null;
  targetEnvSuffix = builtins.replaceStrings [ "-" ] [ "_" ] platform.cargoTarget;
  environment =
    {
      CARGO_BUILD_TARGET = platform.cargoTarget;
    }
    // (
      if toolchain != null then {
        "CC_${targetEnvSuffix}" = "${toolchain.gcc}/bin/${platform.cargoTarget}-gcc";
        "AR_${targetEnvSuffix}" = "${toolchain.binutils}/bin/${platform.cargoTarget}-ar";
        PKG_CONFIG_ALLOW_CROSS = "1";
      } else { }
    );
  shellEnvironment = builtins.removeAttrs environment [ "CARGO_BUILD_TARGET" ];
  cargoTargetShellHook = ''
    if [ -z "''${CARGO_BUILD_TARGET:-}" ]; then
      export CARGO_BUILD_TARGET=${platform.cargoTarget}
    fi
  '';
in
{
  inherit (platform) cargoTarget;
  inherit cargoTargetShellHook environment packagePkgs shellEnvironment;
  rustTargets = [ platform.cargoTarget ];
  buildTools = if toolchain != null then [ toolchain.gcc toolchain.binutils ] else [ ];
}
