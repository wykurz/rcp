# RCP Copy Benchmark Results

Wall time is `copy_seconds`, measured around the copy command only. Destination sync and validation happen afterward and are excluded. Rows are successful latest attempts only; `n` is the number of completed iterations.

`cp` uses `--reflink=auto`, so supported filesystems may report extent-sharing results rather than physical-copy throughput. CPU and RSS come from GNU `time -v` around the locally launched command tree. CPU is cumulative for visible local children, but RSS is the largest observed per-process high-water mark, not a sum across concurrent processes or hosts. Consequently, `*-10x` RSS is not aggregate; `dcp` CPU/RSS primarily describes the local `mpirun` launcher rather than its MPI ranks; remote `rcp` describes only the local master CLI; and remote `rsync` excludes the receiver host.

Campaign policy: NFS 1K `cp-1x` and `rsync-1x` run one sample with a 12-hour cap; iterations 2 and 3 are intentionally omitted. A capped sample is reported as a right-censored lower bound, not as a successful wall time.

### Local / XFS / `linux-source`

| Variant | n | Wall min | Wall mean | Wall max | vs `rcp-default` | CPU user mean | CPU sys mean | GNU-time RSS max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-default` | 3 | 2.46 s | 2.48 s | 2.52 s | 1.00x | 6.46 s | 20.15 s | 162.7 MiB |
| `cp-1x` | 3 | 2.49 s | 2.50 s | 2.50 s | 1.01x | 0.11 s | 1.73 s | 3.0 MiB |
| `dcp-default` | 3 | 3.02 s | 3.14 s | 3.29 s | 1.27x | 0.01 s | 0.02 s | 8.7 MiB |
| `rsync-1x` | 3 | 6.24 s | 6.25 s | 6.27 s | 2.52x | 0.67 s | 3.25 s | 11.6 MiB |

### Remote / XFS / `linux-source`

| Variant | n | Wall min | Wall mean | Wall max | vs `rcp-default` | CPU user mean | CPU sys mean | GNU-time RSS max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-default` | 3 | 5.82 s | 5.94 s | 6.05 s | 1.00x | 0.06 s | 0.01 s | 10.6 MiB |
| `rsync-1x` | 3 | 8.65 s | 8.75 s | 8.82 s | 1.47x | 3.49 s | 1.45 s | 15.4 MiB |

## Failed Attempts

No failed attempts recorded.

