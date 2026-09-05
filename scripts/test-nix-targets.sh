#!/bin/bash
# tests evaluated Nix targets and realized a tiny native package ABI smoke without workspace deps.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NIX_BIN="${NIX:-nix}"
EVALUATE_ONLY=no
if [ "$#" -gt 0 ]; then
    if [ "$#" -ne 1 ] || [ "$1" != --evaluate-only ]; then
        printf 'usage: %s [--evaluate-only]\n' "$0" >&2
        exit 2
    fi
    EVALUATE_ONLY=yes
fi

RCP_NIX_TEST_ROOT="$REPO_ROOT" "$NIX_BIN" eval --impure --raw --expr '
  let
    root = builtins.toPath (builtins.getEnv "RCP_NIX_TEST_ROOT");
    flake = builtins.getFlake (builtins.getEnv "RCP_NIX_TEST_ROOT");

    expectedTargets = {
      x86_64-linux = "x86_64-unknown-linux-musl";
      aarch64-linux = "aarch64-unknown-linux-musl";
    };
    expectedBuildPlatforms = {
      x86_64-linux = "x86_64-unknown-linux-gnu";
      aarch64-linux = "aarch64-unknown-linux-gnu";
    };
    expectedHostPlatforms = {
      x86_64-linux = "x86_64-unknown-linux-musl";
      aarch64-linux = "aarch64-unknown-linux-musl";
    };

    linuxSystems = [ "x86_64-linux" "aarch64-linux" ];
    supportedSystems = linuxSystems;
    unsupportedSystems = [ "x86_64-darwin" "aarch64-darwin" ];
    packageNames = [ "rcp" "rrm" "rchm" "rlink" "rcmp" "filegen" "rcp-all" ];

    fail = label: expected: actual:
      throw "${label}: expected ${builtins.toJSON expected}, got ${builtins.toJSON actual}";
    equals = label: expected: actual:
      if actual == expected then true else fail label expected actual;
    isTrue = label: actual: equals label true actual;

    nameOf = input: input.name or input.pname or "";
    namesOf = inputs: map nameOf inputs;
    hasPackage = packageName: inputs:
      builtins.any
        (name: builtins.match "${packageName}(-.*)?" name != null)
        (namesOf inputs);
    singleInputNamed = label: inputName: inputs:
      let matches = builtins.filter (input: nameOf input == inputName) inputs;
      in
        if builtins.length matches == 1 then builtins.head matches
        else throw "${label}: expected one ${inputName} input, got ${toString (builtins.length matches)}";
    singleInputMatching = label: description: predicate: inputs:
      let matches = builtins.filter predicate inputs;
      in
        if builtins.length matches == 1 then builtins.head matches
        else throw "${label}: expected one ${description} input, got ${toString (builtins.length matches)}";
    compilerProviderNamesOf = inputs:
      builtins.filter
        (name:
          builtins.match "rust-default-[0-9].*" name != null
          || builtins.match "rustc-wrapper-[0-9].*" name != null
          || builtins.match "cargo-[0-9].*" name != null
          || builtins.match "auditable-(rust-default|cargo)-[0-9].*" name != null)
        (namesOf inputs);
    isPinnedCompilerProvider = name:
      name == "rust-default-1.95.0"
      || name == "auditable-rust-default-1.95.0";
    stringLines = value:
      builtins.filter builtins.isString (builtins.split "\n" value);
    hookHasTargetLinker = target: hook:
      builtins.any
        (line:
          builtins.match
            ".*CARGO_TARGET_[A-Z0-9_]+_LINKER=/nix/store/[^/]*-${target}-gcc-wrapper-[^/]*/bin/${target}-(cc|gcc).*"
            line != null)
        (stringLines hook.setEnv);
    muslToolsOf = inputs:
      builtins.filter
        (name: builtins.match ".*-unknown-linux-musl-(gcc|binutils)-wrapper-.*" name != null)
        (namesOf inputs);
    matchingToolInputs = target: kind: inputs:
      builtins.filter
        (input: builtins.match "${target}-${kind}-wrapper-.*" (nameOf input) != null)
        inputs;
    targetEnvNamesOf = attrs:
      builtins.filter
        (name: builtins.match "(CC|AR)_.*_unknown_linux_musl" name != null)
        (builtins.attrNames attrs);
    targetEnvironmentOf = attrs:
      builtins.listToAttrs (map
        (name: { inherit name; value = builtins.getAttr name attrs; })
        (targetEnvNamesOf attrs));
    expectedTargetEnvironment = target: inputs:
      let
        suffix = builtins.replaceStrings [ "-" ] [ "_" ] target;
        compiler = builtins.head (matchingToolInputs target "gcc" inputs);
        binutils = builtins.head (matchingToolInputs target "binutils" inputs);
      in
        {
          "CC_${suffix}" = "${compiler}/bin/${target}-gcc";
          "AR_${suffix}" = "${binutils}/bin/${target}-ar";
        };

    checkToolPair = label: target: inputs:
      let
        tools = muslToolsOf inputs;
        targetTools = builtins.filter
          (name: builtins.match "${target}-(gcc|binutils)-wrapper-.*" name != null)
          tools;
        compilers = builtins.filter
          (name: builtins.match "${target}-gcc-wrapper-.*" name != null)
          tools;
        binutils = builtins.filter
          (name: builtins.match "${target}-binutils-wrapper-.*" name != null)
          tools;
      in
        [
          (equals "${label} musl tool count" 2 (builtins.length tools))
          (equals "${label} matching musl tool count" 2 (builtins.length targetTools))
          (equals "${label} compiler count" 1 (builtins.length compilers))
          (equals "${label} binutils count" 1 (builtins.length binutils))
        ];

    checkNoGenericTargetTools = label: attrs: [
      (isTrue "${label} has no CC_FOR_TARGET" (!(builtins.hasAttr "CC_FOR_TARGET" attrs)))
      (isTrue "${label} has no AR_FOR_TARGET" (!(builtins.hasAttr "AR_FOR_TARGET" attrs)))
    ];

    checkCommonDevTools = label: inputs: [
      (isTrue "${label} includes depot" (hasPackage "depot" inputs))
      (isTrue "${label} includes jq" (hasPackage "jq" inputs))
    ];
    checkYamlValidatorTool = label: inputs: [
      (isTrue "${label} includes Python with PyYAML"
        (builtins.any
          (name: builtins.match "python3-.*-env" name != null)
          (namesOf inputs)))
    ];
    checkLinuxShell = system: shellName: target:
      let
        shell = flake.devShells.${system}.${shellName};
        label = "devShells.${system}.${shellName}";
      in
        [
          (equals "${label} Cargo target default" target (shell.cargoTarget or null))
          (isTrue "${label} does not unconditionally export Cargo target"
            (!(builtins.hasAttr "CARGO_BUILD_TARGET" shell)))
          (equals "${label} target-specific C environment"
            (expectedTargetEnvironment target shell.buildInputs)
            (targetEnvironmentOf shell))
        ]
        ++ checkToolPair label target shell.buildInputs
        ++ checkCommonDevTools label shell.buildInputs
        ++ checkYamlValidatorTool label shell.buildInputs
        ++ checkNoGenericTargetTools label shell;

    checkLinuxPackage = system: packageName:
      let
        package = flake.packages.${system}.${packageName};
        label = "packages.${system}.${packageName}";
        target = expectedTargets.${system};
        buildHook = singleInputNamed label "cargo-build-hook.sh" package.nativeBuildInputs;
        checkHook = singleInputNamed label "cargo-check-hook.sh" package.nativeBuildInputs;
        installHook = singleInputNamed label "cargo-install-hook.sh" package.nativeBuildInputs;
        pkgConfig = singleInputMatching label "pkg-config wrapper"
          (input: builtins.match ".*pkg-config-wrapper-.*" (nameOf input) != null)
          package.nativeBuildInputs;
        compilerProviders = compilerProviderNamesOf package.nativeBuildInputs;
      in
        [
          (equals "${label} build platform" expectedBuildPlatforms.${system}
            package.stdenv.buildPlatform.config)
          (equals "${label} host platform" expectedHostPlatforms.${system}
            package.stdenv.hostPlatform.config)
          (equals "${label} stdenv Rust target" target
            package.stdenv.hostPlatform.rust.rustcTarget)
          (equals "${label} Cargo build-hook target" target buildHook.rustcTargetSpec)
          (equals "${label} Cargo check-hook target" target checkHook.rustcTargetSpec)
          (equals "${label} Cargo install-hook target directory" target
            installHook.targetSubdirectory)
          (isTrue "${label} Cargo build hook uses the matching target linker"
            (hookHasTargetLinker target buildHook))
          (isTrue "${label} Cargo check hook uses the matching target linker"
            (hookHasTargetLinker target checkHook))
          (isTrue "${label} build platform can execute host binaries"
            (package.stdenv.buildPlatform.canExecute package.stdenv.hostPlatform))
          (equals "${label} checks stay enabled" true package.doCheck)
          (equals "${label} pkg-config target" target
            pkgConfig.stdenv.targetPlatform.config)
          (equals "${label} pkg-config host is build-runnable" expectedBuildPlatforms.${system}
            pkgConfig.stdenv.hostPlatform.config)
          (isTrue "${label} has a pinned Rust provider" (compilerProviders != [ ]))
          (isTrue "${label} uses only the pinned Rust provider"
            (builtins.all isPinnedCompilerProvider compilerProviders))
          (equals "${label} has no redundant explicit musl toolchain" 0
            (builtins.length (muslToolsOf package.nativeBuildInputs)))
        ]
        ++ checkNoGenericTargetTools label package;

    checkMappingTarget = system:
      let mapped = import (root + "/nix/target-platform.nix") { inherit system; };
      in equals "target-platform mapping for ${system}" expectedTargets.${system}
        mapped.cargoTarget;

    checkPackageAbiSmoke = system:
      let
        smoke = flake.checks.${system}.package-abi-smoke;
        label = "checks.${system}.package-abi-smoke";
      in
        [
          (equals "${label} is a derivation" "derivation" (smoke.type or null))
          (equals "${label} target" expectedTargets.${system} smoke.passthru.cargoTarget)
        ];

    checkUnsupportedSystem = system:
      let
        mapped = builtins.tryEval (
          (import (root + "/nix/target-platform.nix") { inherit system; }).cargoTarget
        );
      in
        [
          (equals "target-platform rejects ${system}" false mapped.success)
          (isTrue "packages omit ${system}" (!(builtins.hasAttr system flake.packages)))
          (isTrue "devShells omit ${system}" (!(builtins.hasAttr system flake.devShells)))
          (isTrue "checks omit ${system}" (!(builtins.hasAttr system flake.checks)))
        ];

    legacy = import (root + "/default.nix");
    legacySystem = builtins.currentSystem;
    legacyTarget = expectedTargets.${legacySystem};
    legacyChecks =
      [
        (equals "default.nix Cargo target default" legacyTarget
          (legacy.cargoTarget or null))
        (isTrue "default.nix does not unconditionally export Cargo target"
          (!(builtins.hasAttr "CARGO_BUILD_TARGET" legacy)))
        (equals "default.nix target-specific C environment"
          (expectedTargetEnvironment legacyTarget legacy.buildInputs)
          (targetEnvironmentOf legacy))
      ]
      ++ checkToolPair "default.nix" legacyTarget legacy.buildInputs
      ++ checkYamlValidatorTool "default.nix" legacy.buildInputs
      ++ checkNoGenericTargetTools "default.nix" legacy;

    checks =
      # Keep an existing wrong behavior first so RED proves the test observes evaluated output,
      # rather than failing only because the new shared mapping file does not exist yet.
      [
        (equals "devShells.x86_64-linux.default Cargo target default"
          expectedTargets.x86_64-linux
          (flake.devShells.x86_64-linux.default.cargoTarget or null))
      ]
      ++ builtins.concatMap
        (system: checkLinuxShell system "default" expectedTargets.${system})
        linuxSystems
      ++ builtins.concatMap
        (system: checkLinuxShell system "x86_64-musl" expectedTargets.x86_64-linux)
        linuxSystems
      ++ builtins.concatMap
        (system: checkLinuxShell system "aarch64-musl" expectedTargets.aarch64-linux)
        linuxSystems
      ++ builtins.concatMap
        (system: builtins.concatMap (checkLinuxPackage system) packageNames)
        linuxSystems
      ++ map checkMappingTarget (builtins.attrNames expectedTargets)
      ++ builtins.concatMap checkPackageAbiSmoke supportedSystems
      ++ builtins.concatMap checkUnsupportedSystem unsupportedSystems
      ++ legacyChecks;
  in
    builtins.deepSeq checks "Nix target behavior tests passed\n"
'

if [ "$EVALUATE_ONLY" = yes ]; then
    exit 0
fi

check_realized_linux_shell() { # $1 = shell name, $2 = expected target
    local shell_name="$1"
    local expected_target="$2"

    "$NIX_BIN" develop "$REPO_ROOT#$shell_name" --ignore-env --command bash -c '
        set -euo pipefail
        label="$1"
        target="$2"

        fail() {
            printf "realized %s: %s\n" "$label" "$1" >&2
            exit 1
        }

        [[ "$CARGO_BUILD_TARGET" == "$target" ]] ||
            fail "CARGO_BUILD_TARGET expected $target, got $CARGO_BUILD_TARGET"
        [[ "$CC_FOR_TARGET" == "$target-gcc" ]] ||
            fail "CC_FOR_TARGET expected $target-gcc, got $CC_FOR_TARGET"
        [[ "$AR_FOR_TARGET" == "$target-ar" ]] ||
            fail "AR_FOR_TARGET expected $target-ar, got $AR_FOR_TARGET"

        resolved_cc="$(command -v -- "$CC_FOR_TARGET")" ||
            fail "CC_FOR_TARGET does not resolve: $CC_FOR_TARGET"
        resolved_ar="$(command -v -- "$AR_FOR_TARGET")" ||
            fail "AR_FOR_TARGET does not resolve: $AR_FOR_TARGET"
        [[ "$resolved_cc" == */bin/"$target-gcc" ]] ||
            fail "CC_FOR_TARGET resolves to mismatched $resolved_cc"
        [[ "$resolved_ar" == */bin/"$target-ar" ]] ||
            fail "AR_FOR_TARGET resolves to mismatched $resolved_ar"
        python3 -c '"'"'import yaml'"'"' ||
            fail "Python cannot import PyYAML"
    ' bash "$shell_name" "$expected_target"
}

check_inherited_cargo_target() { # $1 = shell name, $2 = caller-selected target
    local shell_name="$1"
    local selected_target="$2"

    CARGO_BUILD_TARGET="$selected_target" \
        "$NIX_BIN" develop "$REPO_ROOT#$shell_name" --command bash -c '
            set -euo pipefail
            expected="$1"
            if [ "$CARGO_BUILD_TARGET" != "$expected" ]; then
                printf "inherited CARGO_BUILD_TARGET expected %s, got %s\n" \
                    "$expected" "$CARGO_BUILD_TARGET" >&2
                exit 1
            fi
        ' bash "$selected_target"
}

check_native_package_abi_smoke() { # $1 = system, $2 = expected target
    local system="$1"
    local expected_target="$2"
    local result

    result="$(
        "$NIX_BIN" build "$REPO_ROOT#checks.$system.package-abi-smoke" \
            --no-link --print-out-paths
    )"

    [[ "$(<"$result/markers/build")" == "build-hook-ran" ]]
    [[ "$(<"$result/markers/check")" == "check-hook-ran" ]]
    [[ "$(<"$result/markers/install")" == "install-hook-ran" ]]
    grep -Fqx 'cargo 1.95.0' "$result/markers/cargo-version"
    grep -Fqx 'rustc 1.95.0' "$result/markers/rustc-version"
    grep -Fqx "target=$expected_target" "$result/evidence"
    grep -Fqx 'binary-executed=yes' "$result/evidence"
}

current_system="$("$NIX_BIN" eval --impure --raw --expr builtins.currentSystem)"
case "$current_system" in
    x86_64-linux)
        host_target=x86_64-unknown-linux-musl
        host_gnu_target=x86_64-unknown-linux-gnu
        ;;
    aarch64-linux)
        host_target=aarch64-unknown-linux-musl
        host_gnu_target=aarch64-unknown-linux-gnu
        ;;
    *)
        printf 'unsupported system for realized Nix shell test\n' >&2
        exit 1
        ;;
esac

check_native_package_abi_smoke "$current_system" "$host_target"
check_realized_linux_shell default "$host_target"
check_inherited_cargo_target default "$host_gnu_target"

printf 'Realized Nix shell target tests passed\n'
