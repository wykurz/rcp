rcp tools
---------

This repo contains tools to efficiently copy, remove and link large filesets.

[![Build status](https://github.com/wykurz/rcp/actions/workflows/rust.yml/badge.svg)](https://github.com/wykurz/rcp/actions)

rcp
===

`rcp` is a tool for copying files similar `cp` but generally MUCH faster when dealing with large number of files.
Inspired by tools like [dsync](https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html) and
[pcp](https://github.com/wtsi-ssg/pcp).

```
rcp 0.1.0

USAGE:
    rcp [FLAGS] [OPTIONS] [paths]...

FLAGS:
    -L, --dereference    Always follow symbolic links in source
    -e, --fail-early     Exit on first error
    -h, --help           Prints help information
    -o, --overwrite      Overwrite existing files/directories
    -p, --preserve       Preserve additional file attributes: file owner, group, setuid, setgid, mtime and atime
    -p, --progress       Show progress
    -q, --quiet          Quiet mode, don't report errors
    -V, --version        Prints version information
    -v, --verbose        Verbose level: -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))

OPTIONS:
        --max-workers <max-workers>    Number of worker threads, 0 means number of cores [default: 0]
        --read-buffer <read-buffer>    File copy read buffer size [default: 128KiB]

ARGS:
    <paths>...    Source path(s) and destination path
```

rrm
===

`rrm` is a simple tool to remove large numbers of files. Note the basic usage is equivalent to `rm -rf`.

```
rrm 0.1.0

USAGE:
    rrm [FLAGS] [OPTIONS] [paths]...

FLAGS:
    -e, --fail-early    Exit on first error
    -h, --help          Prints help information
    -p, --progress      Show progress
    -q, --quiet         Quiet mode, don't report errors
    -V, --version       Prints version information
    -v, --verbose       Verbose level: -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))

OPTIONS:
        --max-workers <max-workers>    Number of worker threads, 0 means number of cores [default: 0]

ARGS:
    <paths>...    Source path(s) and destination path
```

rlink
=====

`rlink` allows hard-linking large number of files. A common pattern is to also provide the `--update <path>` that overrides any paths in `src` to intead be copied over from there.

```
rlink 0.1.0

USAGE:
    rlink [FLAGS] [OPTIONS] <src> <dst>

FLAGS:
    -L, --dereference    Always follow symbolic links in source
    -e, --fail-early     Exit on first error
    -h, --help           Prints help information
    -p, --preserve       Preserve additional file attributes: file owner, group, setuid, setgid, mtime and atime
    -p, --progress       Show progress
    -q, --quiet          Quiet mode, don't report errors
    -V, --version        Prints version information
    -v, --verbose        Verbose level: -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))

OPTIONS:
        --max-workers <max-workers>    Number of worker threads, 0 means number of cores [default: 0]
        --read-buffer <read-buffer>    File copy read buffer size [default: 128KiB]
        --update <update>              Directory with updated contents of `link`

ARGS:
    <src>    Directory with contents we want to update into `dst`
    <dst>    Directory where we put either a hard-link of a file from `link` if it was unchanged, or a copy of a
             file from `new` if it's been modified
```