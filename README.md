rcp
---

`rcp` is a tool for copying files similar `cp` but generally MUCH faster when dealing with large number of files.
Inspired by tools like [dsync](https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html) and
[pcp](https://github.com/wtsi-ssg/pcp).

[![Build status](https://github.com/wykurz/rcp/actions/workflows/rust.yml/badge.svg)](https://github.com/wykurz/rcp/actions)

```
rcp 0.1.0

USAGE:
    rcp [FLAGS] [OPTIONS] [paths]...

FLAGS:
    -L, --dereference    Always follow symbolic links in source
    -h, --help           Prints help information
    -o, --overwrite      Overwrite existing files/directories
    -p, --preserve       Preserve additional file attributes: file owner, group, setuid, setgid, mtime and atime
    -p, --progress       Show progress
    -V, --version        Prints version information

OPTIONS:
        --max-workers <max-workers>    Number of worker threads, 0 means default (number of cores) [default: 0]
        --read-buffer <read-buffer>    File copy read buffer size [default: 128KiB]

ARGS:
    <paths>...    Source path(s) and destination path
```
