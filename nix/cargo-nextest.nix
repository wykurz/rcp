{ cargo-nextest }:

# nix owns tool updates; omit the self-updater and its separately bundled WebPKI roots
cargo-nextest.overrideAttrs {
  cargoBuildNoDefaultFeatures = true;
  cargoCheckNoDefaultFeatures = true;
  cargoBuildFeatures = [ "default-no-update" ];
  cargoCheckFeatures = [ "default-no-update" ];
}
