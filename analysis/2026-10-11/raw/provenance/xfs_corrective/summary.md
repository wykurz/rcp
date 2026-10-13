# RCP Copy Benchmark Results

Wall time is `copy_seconds`, measured around the copy command only. Destination sync and validation happen afterward and are excluded. Rows are successful latest attempts only; `n` is the number of completed iterations.

`cp` uses `--reflink=auto`, so supported filesystems may report extent-sharing results rather than physical-copy throughput. CPU and RSS come from GNU `time -v` around the locally launched command tree. CPU is cumulative for visible local children, but RSS is the largest observed per-process high-water mark, not a sum across concurrent processes or hosts. Consequently, `*-10x` RSS is not aggregate; `dcp` CPU/RSS primarily describes the local `mpirun` launcher rather than its MPI ranks; remote `rcp` describes only the local master CLI; and remote `rsync` excludes the receiver host.

Campaign policy: NFS 1K `cp-1x` and `rsync-1x` run one sample with a 12-hour cap; iterations 2 and 3 are intentionally omitted. A capped sample is reported as a right-censored lower bound, not as a successful wall time.

### Local / XFS / `10x100x1G`

| Variant | n | Wall min | Wall mean | Wall max | vs `rcp-default` | CPU user mean | CPU sys mean | GNU-time RSS max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-8` | 3 | 0.161 s | 0.162 s | 0.164 s | 0.88x | 0.07 s | 0.08 s | 9.9 MiB |
| `rcp-default` | 3 | 0.178 s | 0.184 s | 0.190 s | 1.00x | 0.07 s | 0.15 s | 12.2 MiB |
| `rcp-32` | 3 | 0.183 s | 0.189 s | 0.196 s | 1.03x | 0.08 s | 0.13 s | 8.3 MiB |
| `rcp-128` | 3 | 0.198 s | 0.201 s | 0.206 s | 1.09x | 0.07 s | 0.18 s | 11.4 MiB |

### Local / XFS / `10x1024x100M`

| Variant | n | Wall min | Wall mean | Wall max | vs `rcp-default` | CPU user mean | CPU sys mean | GNU-time RSS max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-8` | 3 | 0.323 s | 0.327 s | 0.330 s | 0.88x | 0.63 s | 0.87 s | 10.2 MiB |
| `rcp-32` | 3 | 0.362 s | 0.367 s | 0.371 s | 0.99x | 0.68 s | 1.44 s | 10.4 MiB |
| `rcp-default` | 3 | 0.360 s | 0.372 s | 0.383 s | 1.00x | 0.68 s | 1.21 s | 8.3 MiB |
| `rcp-128` | 3 | 0.370 s | 0.382 s | 0.392 s | 1.03x | 0.62 s | 1.57 s | 15.3 MiB |

### Local / XFS / `10x10x10x1024x1M`

| Variant | n | Wall min | Wall mean | Wall max | vs `rcp-default` | CPU user mean | CPU sys mean | GNU-time RSS max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-8` | 3 | 20.31 s | 20.40 s | 20.52 s | 0.93x | 62.99 s | 148.12 s | 87.2 MiB |
| `rcp-default` | 3 | 21.76 s | 22.00 s | 22.19 s | 1.00x | 68.20 s | 161.49 s | 92.7 MiB |
| `rcp-32` | 3 | 22.00 s | 22.27 s | 22.61 s | 1.01x | 69.72 s | 162.15 s | 88.0 MiB |
| `rcp-128` | 3 | 22.32 s | 22.73 s | 23.26 s | 1.03x | 60.01 s | 151.50 s | 91.5 MiB |

### Local / XFS / `10x10x10x10x1024x1K`

| Variant | n | Wall min | Wall mean | Wall max | vs `rcp-default` | CPU user mean | CPU sys mean | GNU-time RSS max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-8` | 3 | 3.53 min | 3.60 min | 3.70 min | 0.95x | 639.65 s | 1566.92 s | 743.7 MiB |
| `rcp-32` | 3 | 3.75 min | 3.77 min | 3.80 min | 1.00x | 744.87 s | 1787.03 s | 746.3 MiB |
| `rcp-default` | 3 | 3.75 min | 3.78 min | 3.81 min | 1.00x | 736.28 s | 1783.81 s | 752.0 MiB |
| `rcp-128` | 3 | 3.83 min | 3.84 min | 3.87 min | 1.02x | 655.61 s | 1653.58 s | 756.5 MiB |

## Failed Attempts

No failed attempts recorded.

