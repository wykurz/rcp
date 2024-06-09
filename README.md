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
Inspired by tools like [dsync](https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html) and
[pcp](https://github.com/wtsi-ssg/pcp).

```
USAGE:
    rcp [FLAGS] [OPTIONS] [paths]...

FLAGS:
    -L, --dereference
            Always follow symbolic links in source

    -e, --fail-early
            Exit on first error

    -h, --help
            Prints help information

    -o, --overwrite
            Overwrite existing files/directories

    -p, --preserve
            Preserve additional file attributes: file owner, group, setuid, setgid, mtime and atime

        --progress
            Show progress

    -q, --quiet
            Quiet mode, don't report errors

        --summary
            Print summary at the end

    -V, --version
            Prints version information

    -v, --verbose
            Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))


OPTIONS:
        --max-blocking-threads <max-blocking-threads>
            Number of blocking worker threads, 0 means Tokio runtime default (512) [default: 0]

        --max-workers <max-workers>
            Number of worker threads, 0 means number of cores [default: 0]

        --overwrite-compare <overwrite-compare>
            Comma separated list of file attributes to compare when when deciding if files are "identical", used with
            --overwrite flag. Options are: uid, gid, size, mtime, ctime [default: size,mtime]
        --preserve-settings <preserve-settings>
            Specify exactly what attributes to preserve.

            If specified, the "preserve" flag is ignored.

            The format is: "<type1>:<attributes1> <type2>:<attributes2> ..." Where <type> is one of: "f" (file), "d"
            (directory), "l" (symlink) And <attributes> is a comma separated list of: "uid", "gid", "time", <mode mask>
            Where <mode mask> is a 4 digit octal number

            Example: "f:uid,gid,time,0777 d:uid,gid,time,0777 l:uid,gid,time"


ARGS:
    <paths>...
            Source path(s) and destination path
```

rrm
===

`rrm` is a simple tool for removing large numbers of files. Note the basic usage is equivalent to `rm -rf`.

```
USAGE:
    rrm [FLAGS] [OPTIONS] [paths]...

FLAGS:
    -e, --fail-early    Exit on first error
    -h, --help          Prints help information
        --progress      Show progress
    -q, --quiet         Quiet mode, don't report errors
        --summary       Print summary at the end
    -V, --version       Prints version information
    -v, --verbose       Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))

OPTIONS:
        --max-blocking-threads <max-blocking-threads>
            Number of blocking worker threads, 0 means Tokio runtime default (512) [default: 0]

        --max-workers <max-workers>                      Number of worker threads, 0 means number of cores [default: 0]

ARGS:
    <paths>...    Source path(s) and destination path
```

rlink
=====

`rlink` allows hard-linking large number of files. A common pattern is to also provide `--update <path>` that overrides any paths in `src` to instead be copied over from there.

```
USAGE:
    rlink [FLAGS] [OPTIONS] <src> <dst>

FLAGS:
    -e, --fail-early    Exit on first error
    -h, --help          Prints help information
    -o, --overwrite     Overwrite existing files/directories
        --progress      Show progress
    -q, --quiet         Quiet mode, don't report errors
        --summary       Print summary at the end
    -V, --version       Prints version information
    -v, --verbose       Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))

OPTIONS:
        --max-blocking-threads <max-blocking-threads>
            Number of blocking worker threads, 0 means Tokio runtime default (512) [default: 0]

        --max-workers <max-workers>                      Number of worker threads, 0 means number of cores [default: 0]
        --overwrite-compare <overwrite-compare>
            Comma separated list of file attributes to compare when when deciding if files are "identical", used with
            --overwrite flag. Options are: uid, gid, size, mtime, ctime [default: size,mtime]
        --update <update>                                Directory with updated contents of `link`
        --update-compare <update-compare>
            Same as overwrite-compare, but for deciding if we can hard-link or if we need to copy a file from the update
            directory. Used with --update flag [default: size,mtime]

ARGS:
    <src>    Directory with contents we want to update into `dst`
    <dst>    Directory where we put either a hard-link of a file from `link` if it was unchanged, or a copy of a
             file from `new` if it's been modified
```

rcmp
=====

`rcmp` is a tool for comparing large filesets. Currently, it only supports comparing metadata (no content checking).

```
USAGE:
    rcmp [FLAGS] [OPTIONS] <log> <src> <dst>

FLAGS:
    -m, --exit-early
            Exit on first mismatch

    -e, --fail-early
            Exit on first error

    -h, --help
            Prints help information

        --progress
            Show progress

    -q, --quiet
            Quiet mode, don't report errors

        --summary
            Print summary at the end

    -V, --version
            Prints version information

    -v, --verbose
            Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))


OPTIONS:
        --max-blocking-threads <max-blocking-threads>
            Number of blocking worker threads, 0 means Tokio runtime default (512) [default: 0]

        --max-workers <max-workers>
            Number of worker threads, 0 means number of cores [default: 0]

        --metadata-compare <metadata-compare>
            Attributes to compare when when deciding if objects are "identical". Options are: uid, gid, mode, size,
            mtime, ctime

            The format is: "<type1>:<attributes1> <type2>:<attributes2> ..." Where <type> is one of: "f" (file), "d"
            (directory), "l" (symlink) And <attributes> is a comma separated list of: uid, gid, size, mtime, ctime

            Example: "f:mtime,ctime,mode,size d:mtime,ctime,mode l:mtime,ctime,mode" [default: f:mtime,size d:mtime
            l:mtime]

ARGS:
    <log>
            File where we store comparison mismatch output

    <src>
            File or directory to compare

    <dst>
            File or directory to compare
```