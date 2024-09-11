<picture><img src="assets/logo.svg" height="64"></picture>

RCP TOOLS
=========

This repo contains tools to efficiently copy, remove and link large filesets.

[![Build status](https://github.com/wykurz/rcp/actions/workflows/rust.yml/badge.svg)](https://github.com/wykurz/rcp/actions)

- `rcp` is a tool for copying files; similar to `cp` but generally MUCH faster when dealing with a large number of files.

    Inspired by tools like `dsync`(1) and `pcp`(2).

- `rrm` is a tool for removing files.

    Basic usage is equivalent to `rm -rf`.

- `rlink` allows hard-linking files.

    A common pattern is to also provide `--update <path>` that overrides any paths in `src` to instead be copied over from there.

- `rcmp` is a tool for comparing filesets.

    Currently, it only supports comparing metadata (no content checking).

    Returns error code 1 if there are differences, 2 if there were errors.

examples
========

### basic copy with progress-bar and summary at the end:
```fish
> rcp <foo> <bar> --progress --summary
```
Roughly equivalent to `cp -R --update=none <foo> <bar>`.

### copy while preserving metadata, overwrite/update destination if it already exists:
```fish
> rcp <foo> <bar> --preserve --progress --summary --overwrite
```
Roughly equivalent to: `cp -pR <foo> <bar>`.

### remove a path:
```fish
> rrm <bar> --progress --summary
```
Roughly equivalent to: `rm -rf <bar>`.

### hard-link contents of one path to another:
```fish
> rlink <foo> <bar> --progress --summary
```
Roughly equivalent to: `cp -p --link <foo> <bar>`.

### hard-link contents of `<foo>` to `<baz>` if they are identical to `<bar>`:
```fish
> rlink <foo> --update <bar> <baz> --update-exclusive --progress --summary
```
Using `--update-exclusive` means that if a file is present in `<foo>` but not in `<bar>` it will be ignored.
Roughly equivalent to: `rsync -a --link-dest=<foo> <bar> <baz>`.

### compare `<foo>` vs. `<bar>`:
```fish
> rcmp <foo> <bar> --progress --summary --log compare.log
```

installation
============

<picture>
<img src="https://github.com/NixOS/nixos-artwork/blob/master/logo/nix-snowflake-colours.svg" height="64">
</picture>

nixpkgs
-------

All tools are available via nixpkgs under [rcp](https://github.com/NixOS/nixpkgs/blob/nixos-unstable/pkgs/by-name/rc/rcp/package.nix) package name.

The following command will install all the tools on your system:

```fish
> nix-env -iA nixpkgs.rcp
```

debian / rhel
-------------

Starting with release `v0.10.1`, .deb and .rpm packages are available as part of each release.

general controls
================

## copy semantics

The copy semantics for RCP tools differ slightly from how e.g. the `cp` tool works. This is because of the ambiguity in the result of a `cp` operation that we wanted to avoid.

Specifically, the result of `cp foo/x bar/x` depends on `bar/x` being a directory. If so, the resulting path will be `bar/x/x` (which is usually undesired), otherwise it will be `bar/x`.

To avoid this confusion, RCP tools:
- will NOT overwrite data by default (use `--overwrite` to change)
- do assume that a path WITHOUT a trailing slash is the final name of the destination and
- path ending in slash is a directory into which we want to copy the sources (without renaming)

The following examples illustrate this (_those rules apply to both `rcp` and `rlink`_):

- `rcp A/B C/D` - copy `A/B` into `C/` and name it `D`; if `C/D` exists fail immediately
- `rcp A/B C/D/` - copy `B` into `D` WITHOUT renaming i.e., the resulting path will be `C/D/B`; if `C/B/D` exists fail immediately

Using `rcp` it's also possible to copy multiple sources into a single destination, but the destination MUST have a trailing slash (`/`):
- `rcp A B C D/` - copy `A`, `B` and `C` into `D` WITHOUT renaming i.e., the resulting paths will be `D/A`, `D/B` and `D/C`; if any of which exist fail immediately

## throttling

- set `--ops-throttle` to reduce the maximum number of operations per second
  - useful if you want to avoid interfering with other work on the storage / host

- set `--max-open-files` to reduce the maximum number of open files
  - RCP tools will automatically adjust the maximum based on the system limits however, this setting can be used if there are additional constraints

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

<picture>
<img src="https://github.com/tokio-rs/tracing/blob/master/assets/logo-type.png" height="64">
</picture>

The `rcp` tools now use the `tracing` crate for logging and support sending data to the `tokio-console` subscriber.

## enabling

To enable the `console-subscriber` you need to set the environment variable `RCP_TOKIO_TRACING_CONSOLE_ENABLED=1` (or `true` with any case).

## sever port

By default port `6669` is used (`tokio-console` default) but this can be changed by setting `RCP_TOKIO_TRACING_CONSOLE_SERVER_PORT=1234`.

## retention time

The trace events are retained for 60s. This can be modified by setting `RCP_TOKIO_TRACING_CONSOLE_RETENTION_SECONDS=120`.

references
==========
1) https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html
2) https://github.com/wtsi-ssg/pcp