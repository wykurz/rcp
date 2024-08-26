rcp tools
---------

This repo contains tools to efficiently copy, remove and link large filesets.

[![Build status](https://github.com/wykurz/rcp/actions/workflows/rust.yml/badge.svg)](https://github.com/wykurz/rcp/actions)

installation
============

nixpkgs
-------

All tools are available via nixpkgs under `rcp` package name.

The following command will install all the tools on your system:

```shell
nix-env -iA nixpkgs.rcp
```

debian / rhel
-------------

Starting with release `v0.10.1` .deb and .rpm packages are available as part of each release.

general controls
================

## error handling

- `rcp` tools will log non-terminal errors and continue
- to fail immediately on any error use the `--fail-early` flag

## terminal output

**Log messages**
- sent to `stdout`
- by default only errors are logged
- verbosity controlled using `-v`/`-vv`/`-vvv` for INFO/DEBUG/TRACE and `-q`/`--quiet` to disable

**Progress**
- sent to `stderr` (both `ProgressBar` and `TextUpdates`)
- by default disabled
- enabled using `-p`/`--progress` with optional `--progress-type=...` override

**Summary**
- sent to `stdout`
- by default disabled
- enabled using `--summary`

## overwrite

`rcp` tools will not-overwrite pre-existing data unless used with the `--overwrite` flag.


tracing and tokio-console
=========================

The `rcp` tools now use the `tracing` crate for logging and support sending data to the `tokio-console` subscriber.

## enabling

To enable the `console-subscriber` you need to set the environment variable `RCP_TOKIO_TRACING_CONSOLE_ENABLED=1` (or `true` with any case).

## sever port

By default port `6669` is used (`tokio-console` default) but this can be changed by setting `RCP_TOKIO_TRACING_CONSOLE_SERVER_PORT=1234`.

## retention time

The trace events are retained for 60s. This can be modified by setting `RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS=120`.

rcp
===
`rcp` is a tool for copying files similar to `cp` but generally MUCH faster when dealing with a large number of files.

Inspired by tools like `dsync`(1) and `pcp`(2).

1) https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html
2) https://github.com/wtsi-ssg/pcp

```
Usage: cargo run [OPTIONS] [ARGS]...

Arguments:
  [ARGS]...  Arguments for the binary or example to run

Options:
      --message-format <FMT>  Error format
  -v, --verbose...            Use verbose output (-vv very verbose/build.rs output)
  -q, --quiet                 Do not print cargo log messages
      --color <WHEN>          Coloring: auto, always, never
      --config <KEY=VALUE>    Override a configuration value
  -Z <FLAG>                   Unstable (nightly-only) flags to Cargo, see 'cargo -Z help' for details
  -h, --help                  Print help

Package Selection:
  -p, --package [<SPEC>]  Package with the target to run

Target Selection:
      --bin [<NAME>]      Name of the bin target to run
      --example [<NAME>]  Name of the example target to run

Feature Selection:
  -F, --features <FEATURES>  Space or comma separated list of features to activate
      --all-features         Activate all available features
      --no-default-features  Do not activate the `default` feature

Compilation Options:
  -j, --jobs <N>                Number of parallel jobs, defaults to # of CPUs.
      --keep-going              Do not abort the build as soon as there is an error
  -r, --release                 Build artifacts in release mode, with optimizations
      --profile <PROFILE-NAME>  Build artifacts with the specified profile
      --target [<TRIPLE>]       Build for the target triple
      --target-dir <DIRECTORY>  Directory for all generated artifacts
      --unit-graph              Output build graph in JSON (unstable)
      --timings[=<FMTS>]        Timing output formats (unstable) (comma separated): html, json

Manifest Options:
      --manifest-path <PATH>  Path to Cargo.toml
      --ignore-rust-version   Ignore `rust-version` specification in packages
      --locked                Assert that `Cargo.lock` will remain unchanged
      --offline               Run without accessing the network
      --frozen                Equivalent to specifying both --locked and --offline

Run `cargo help run` for more detailed information.
```

rrm
===
`rrm` is a simple tool for removing large numbers of files.

Note the basic usage is equivalent to `rm -rf`.

```
Usage: cargo run [OPTIONS] [ARGS]...

Arguments:
  [ARGS]...  Arguments for the binary or example to run

Options:
      --message-format <FMT>  Error format
  -v, --verbose...            Use verbose output (-vv very verbose/build.rs output)
  -q, --quiet                 Do not print cargo log messages
      --color <WHEN>          Coloring: auto, always, never
      --config <KEY=VALUE>    Override a configuration value
  -Z <FLAG>                   Unstable (nightly-only) flags to Cargo, see 'cargo -Z help' for details
  -h, --help                  Print help

Package Selection:
  -p, --package [<SPEC>]  Package with the target to run

Target Selection:
      --bin [<NAME>]      Name of the bin target to run
      --example [<NAME>]  Name of the example target to run

Feature Selection:
  -F, --features <FEATURES>  Space or comma separated list of features to activate
      --all-features         Activate all available features
      --no-default-features  Do not activate the `default` feature

Compilation Options:
  -j, --jobs <N>                Number of parallel jobs, defaults to # of CPUs.
      --keep-going              Do not abort the build as soon as there is an error
  -r, --release                 Build artifacts in release mode, with optimizations
      --profile <PROFILE-NAME>  Build artifacts with the specified profile
      --target [<TRIPLE>]       Build for the target triple
      --target-dir <DIRECTORY>  Directory for all generated artifacts
      --unit-graph              Output build graph in JSON (unstable)
      --timings[=<FMTS>]        Timing output formats (unstable) (comma separated): html, json

Manifest Options:
      --manifest-path <PATH>  Path to Cargo.toml
      --ignore-rust-version   Ignore `rust-version` specification in packages
      --locked                Assert that `Cargo.lock` will remain unchanged
      --offline               Run without accessing the network
      --frozen                Equivalent to specifying both --locked and --offline

Run `cargo help run` for more detailed information.
```

rlink
=====
`rlink` allows hard-linking large number of files.

A common pattern is to also provide `--update <path>` that overrides any paths in `src` to instead be copied over from
there.

```
Usage: cargo run [OPTIONS] [ARGS]...

Arguments:
  [ARGS]...  Arguments for the binary or example to run

Options:
      --message-format <FMT>  Error format
  -v, --verbose...            Use verbose output (-vv very verbose/build.rs output)
  -q, --quiet                 Do not print cargo log messages
      --color <WHEN>          Coloring: auto, always, never
      --config <KEY=VALUE>    Override a configuration value
  -Z <FLAG>                   Unstable (nightly-only) flags to Cargo, see 'cargo -Z help' for details
  -h, --help                  Print help

Package Selection:
  -p, --package [<SPEC>]  Package with the target to run

Target Selection:
      --bin [<NAME>]      Name of the bin target to run
      --example [<NAME>]  Name of the example target to run

Feature Selection:
  -F, --features <FEATURES>  Space or comma separated list of features to activate
      --all-features         Activate all available features
      --no-default-features  Do not activate the `default` feature

Compilation Options:
  -j, --jobs <N>                Number of parallel jobs, defaults to # of CPUs.
      --keep-going              Do not abort the build as soon as there is an error
  -r, --release                 Build artifacts in release mode, with optimizations
      --profile <PROFILE-NAME>  Build artifacts with the specified profile
      --target [<TRIPLE>]       Build for the target triple
      --target-dir <DIRECTORY>  Directory for all generated artifacts
      --unit-graph              Output build graph in JSON (unstable)
      --timings[=<FMTS>]        Timing output formats (unstable) (comma separated): html, json

Manifest Options:
      --manifest-path <PATH>  Path to Cargo.toml
      --ignore-rust-version   Ignore `rust-version` specification in packages
      --locked                Assert that `Cargo.lock` will remain unchanged
      --offline               Run without accessing the network
      --frozen                Equivalent to specifying both --locked and --offline

Run `cargo help run` for more detailed information.
```

rcmp
=====
`rcmp` is a tool for comparing large filesets.

Currently, it only supports comparing metadata (no content checking).

```
Usage: cargo run [OPTIONS] [ARGS]...

Arguments:
  [ARGS]...  Arguments for the binary or example to run

Options:
      --message-format <FMT>  Error format
  -v, --verbose...            Use verbose output (-vv very verbose/build.rs output)
  -q, --quiet                 Do not print cargo log messages
      --color <WHEN>          Coloring: auto, always, never
      --config <KEY=VALUE>    Override a configuration value
  -Z <FLAG>                   Unstable (nightly-only) flags to Cargo, see 'cargo -Z help' for details
  -h, --help                  Print help

Package Selection:
  -p, --package [<SPEC>]  Package with the target to run

Target Selection:
      --bin [<NAME>]      Name of the bin target to run
      --example [<NAME>]  Name of the example target to run

Feature Selection:
  -F, --features <FEATURES>  Space or comma separated list of features to activate
      --all-features         Activate all available features
      --no-default-features  Do not activate the `default` feature

Compilation Options:
  -j, --jobs <N>                Number of parallel jobs, defaults to # of CPUs.
      --keep-going              Do not abort the build as soon as there is an error
  -r, --release                 Build artifacts in release mode, with optimizations
      --profile <PROFILE-NAME>  Build artifacts with the specified profile
      --target [<TRIPLE>]       Build for the target triple
      --target-dir <DIRECTORY>  Directory for all generated artifacts
      --unit-graph              Output build graph in JSON (unstable)
      --timings[=<FMTS>]        Timing output formats (unstable) (comma separated): html, json

Manifest Options:
      --manifest-path <PATH>  Path to Cargo.toml
      --ignore-rust-version   Ignore `rust-version` specification in packages
      --locked                Assert that `Cargo.lock` will remain unchanged
      --offline               Run without accessing the network
      --frozen                Equivalent to specifying both --locked and --offline

Run `cargo help run` for more detailed information.
```